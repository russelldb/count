%%%-------------------------------------------------------------------
%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2013, Russell Brown
%%% @doc
%%% Common db functions
%%% @end
%%% Created : 23 Mar 2013 by Russell Brown <russelldb@basho.com>
%%%-------------------------------------------------------------------
-module(count_db).

-compile([export_all]).

-include("count.hrl").

 %% sext encoded key of {s, counter, Key::binary(), OpCount::non_neg_integer().
-type rollup_key() :: binary().
-type rollup_counter() :: {OpCount::non_neg_integer(),
                           count_pn_counter:counter()}.

%% ---------
%% Storage
%% ---------
start(Partition) ->
    Opts =  [{create_if_missing, true},
             {write_buffer_size, 1024*1024},
             {max_open_files, 20}],
    %% start hanoi
    {ok, DataDir} = get_data_dir(Partition),
    case eleveldb:open(DataDir, Opts) of
        {ok, Tree} ->
            {ok, DataDir, Tree};
        Error ->
            Error
    end.

get_data_dir(Partition) ->
    DataRoot = app_helper:get_env(count, data_root, "./data/count_hanoi"),
    PartitionRoot = filename:join(DataRoot, integer_to_list(Partition)),
    ok = filelib:ensure_dir(filename:join(PartitionRoot, ".dummy")),
    {ok, PartitionRoot}.

store(StorageState, Key, Value) ->
    eleveldb:write(StorageState, [{put, Key, Value}], []).

get(StorageState, Key) ->
    eleveldb:get(StorageState, Key, []).

make_key_range(Key, Min, Max) ->
    FromKey = counter_key(Key, Min),
    ToKey = counter_key(Key, Max),
    {FromKey, ToKey}.

counter_key(Key, OpCount) ->
    sext:encode({c, ?COUNTERS, Key, OpCount}).

%% Use the Opcount in the key
%% since we aysnchronously rollup
%% counters in the background
%% we don't want any races / overwriting
%% with older values
%% this just means there are no straight _gets_
%% always range folds.
%% Getting a counter folds over the range of
%% rolled up counters for key and taking the highest op count
%% (and deleting the rest?)
%% returns the rolled up counter,
%% and a key it should be stored under
checkpoint_key(Key, OpCount) ->
    sext:encode({s, ?COUNTERS, Key, OpCount}).

-spec get_checkpoint(term(), binary(), non_neg_integer()) ->
                            {rollup_key(), rollup_counter()}.
get_checkpoint(StorageState, Key, OpCount) ->
    CheckPointKey = checkpoint_key(Key, OpCount),
    FromKey = checkpoint_key(Key, 0),
    FoldFun = fun(K, V, {Keys, _Counter}) ->
                     {[{delete, K} | Keys],  binary_to_term(V)} end,
    Acc = {[], {0, count_pncounter:new()}},
    KR = {FromKey, CheckPointKey},
    %% @TODO level style fold fun, mkay, throw to break
    {CheckPointKey, fold(StorageState, FoldFun, Acc, KR)}.

get_counter(StorageState, Key, OpCount) ->
    {CheckPointKey, {KeysToDelete0, {CPOpCount, PNCounter0}}} = get_checkpoint(StorageState, Key, OpCount),
    KeysToDelete = remove_current_rollup_key(KeysToDelete0),
    KeyRange = make_key_range(Key, CPOpCount, OpCount),
    FoldFun = fun(K, V, {Keys, {Size, PNCount}}) ->
                      {VnodeId, Count} = binary_to_term(V),
                      Counter = case Count of
                                    N when N < 0 ->
                                        {Size+1, count_pncounter:update({decrement, N*-1}, VnodeId, PNCount)};
                                    P ->
                                        {Size+1, count_pncounter:update({increment, P}, VnodeId, PNCount)}
                                end,
                      {[{delete, K} | Keys], Counter}
              end,
    Acc = {KeysToDelete, {0, PNCounter0}},
    %% @TODO level style fold fun, throw to break
    {KeysToDelete2, {Size, PNCounter}} = fold(StorageState, FoldFun, Acc, KeyRange),
    {CheckPointKey, KeysToDelete2, Size, PNCounter}.

save_checkpoint(StorageState, CheckPointKey, {OpCount, PNCounter}) ->
    lager:info("Rollup of at ~p of ~p for ~p~n", [OpCount, count_pncounter:value(PNCounter),
                                                  sext:decode(CheckPointKey)]),
    store(StorageState, CheckPointKey, term_to_binary({OpCount, PNCounter})).

delete(StorageState, Keys) ->
    lager:info("Deleting, ~p keys", [length(Keys)]),
    ok = eleveldb:write(StorageState, Keys, []).

stop(StorageState) ->
    ok = eleveldb:close(StorageState).

remove_current_rollup_key([]) ->
    [];
remove_current_rollup_key([_H|T]) ->
    T.

%% Level db needs you to declare the limit of the fold
%% a KeyRange is just a {from key, to key} tuple
%% this makes key ranges that exclude the from key but include the to key.
fold_fun(Fun, {From, To}) ->
    fun({K, V}, Acc) when  From < K,
                           To >= K ->
            Fun(K, V, Acc);
       (_, Acc) ->
            throw({break, Acc})
    end.

fold(StorageState, Fun, Acc, {FirstKey, _}=KR) ->
    FoldFun = fold_fun(Fun, KR),
    try
        eleveldb:fold(StorageState, FoldFun, Acc, [{first_key, FirstKey}, {fill_cache, false}])
    catch
        {break, FinalAcc} ->
            FinalAcc
    end.
