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
-include_lib("hanoidb/include/hanoidb.hrl").

 %% sext encoded key of {s, counter, Key::binary(), OpCount::non_neg_integer().
-type rollup_key() :: binary().
-type rollup_counter() :: {OpCount::non_neg_integer(),
                           count_pn_counter:counter()}.

%% ---------
%% Storage
%% ---------
start(Partition) ->
    %% start hanoi
    {ok, DataDir} = get_data_dir(Partition),
    case application:start(hanoidb) of
        Good when Good =:= ok;
        Good =:= {error, {already_started, hanoidb}} ->
            open_data_dir(DataDir);
        Bad ->
            Bad
    end.

open_data_dir(DataDir) ->
    case hanoidb:open(DataDir, []) of
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
    hanoidb:transact(StorageState, [{put, Key, Value}]).

make_key_range(Key, Min, Max) ->
    FromKey = counter_key(Key, Min),
    ToKey = counter_key(Key, Max),
    #key_range{from_key= FromKey, from_inclusive=false, to_key= ToKey, to_inclusive=true}.

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
                     {[K | Keys],  binary_to_term(V)} end,
    Acc = {[], {0, count_pncounter:new()}},
    KR = #key_range{from_key= FromKey, from_inclusive=false,
                    to_key= CheckPointKey,
                    to_inclusive = true},
    {CheckPointKey, hanoidb:fold_range(StorageState, FoldFun, Acc, KR)}.

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
                      {[K | Keys], Counter}
              end,
    Acc = {KeysToDelete, {0, PNCounter0}},
    {KeysToDelete2, {Size, PNCounter}} = hanoidb:fold_range(StorageState, FoldFun, Acc, KeyRange),

    hanoidb:fold(StorageState, fun(K, V, _Acc) when K /= ?OP_COUNT_KEY -> lager:info("K ~p V ~p", [sext:decode(K), binary_to_term(V)]);
                                  (K, V, _Acc) -> lager:info("K ~p V ~p", [K, V]) end, ok),
    {CheckPointKey, KeysToDelete2, Size, PNCounter}.

save_checkpoint(StorageState, CheckPointKey, {OpCount, PNCounter}) ->
    lager:info("Rollup of at ~p of ~p for ~p~n", [OpCount, count_pncounter:value(PNCounter),
                                                  sext:decode(CheckPointKey)]),
    store(StorageState, CheckPointKey, term_to_binary({OpCount, PNCounter})).

delete(StorageState, Keys) ->
    Out = [sext:decode(K) || K <- Keys],
    lager:info("deleting  ~p", [Out]),
    Dels = [{delete, Key} || Key <- Keys],
    ok = hanoidb:transact(StorageState, Dels).

stop(StorageState) ->
    ok = hanoidb:close(StorageState).

remove_current_rollup_key([]) ->
    [];
remove_current_rollup_key([_H|T]) ->
    T.
