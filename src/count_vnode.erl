%%%-------------------------------------------------------------------
%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2013, Russell Brown
%%% @doc
%%% First pass at append only counter
%%%
%%% @end
%%% Created :  2 Mar 2013 by Russell Brown <russelldb@basho.com>
%%%-------------------------------------------------------------------
-module(count_vnode).
-behaviour(riak_core_vnode).
-include_lib("riak_core/include/riak_core_vnode.hrl").
-include_lib("hanoidb/include/hanoidb.hrl").
-include("count.hrl").

%% API
-export([increment/4, merge/4, get/3, repair/3]).

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).

-record(state, {data_dir :: file:filename(),
                handoff_target :: node(),
                node :: node(),
                op_count :: pos_integer(), %% monotonically increasing count of operations
                storage_state :: term(),
                vnode_id :: term(),
                partition :: partition()}).

-define(MASTER, count_vnode_master).
-define(OP_THRESHOLD, 1000).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Retrieves the state PN-Counter for the given key
-spec get(riak_core_apl:preflist2(), term(), term()) -> ok.
get(PrefList, Key, ReqId) ->
    riak_core_vnode_master:command(PrefList, {get, Key, ReqId}, {fsm, undefined, self()}, ?MASTER).

%% @doc Increments the counter for key by amount
-spec increment(partition(), term(), term(), pos_integer() | infinity) -> ok.
increment(IdxNode, Key, Amount, Timeout) ->
    riak_core_vnode_master:sync_command(IdxNode, {increment, Key, Amount}, ?MASTER, Timeout).

%% @doc Downstream application of increment
-spec merge(riak_core_apl:preflist2(),  term(), term(), term()) -> ok.
merge(PrefList, Key, Val, ReqId) ->
    riak_core_vnode_master:command(PrefList, {merge, Key, Val, ReqId}, {fsm, undefined, self()}, ?MASTER).

%% @doc Sends a read-repair of a value
-spec repair(riak_core_apl:preflist2(),  term(), term()) -> ok.
repair(PrefList, Key, PNCounter) ->
    riak_core_vnode_master:command(PrefList, {repair, Key, PNCounter, ignore}, ignore, ?MASTER).

init([Partition]) ->
    Node = node(),
    random:seed(erlang:now()),
    VnodeId = uuid:v4(),
    {ok, DataDir, StorageState} = start_storage(Partition),
    OpCount = get_op_count(StorageState),
    lager:info("Started ~p with op count ~p~n", [Partition, OpCount]),
    {ok, #state { data_dir = DataDir,
                  node = Node,
                  op_count = OpCount,
                  storage_state = StorageState,
                  vnode_id = VnodeId,
                  partition = Partition
                }}.

handle_command({increment, Key, Amount}, _Sender, State) ->
    #state{storage_state=StorageState, vnode_id=VnodeId, op_count=OpCount0} = State,
    OpCount = OpCount0+1,
    OpKey = counter_key(Key, OpCount),
    Val = term_to_binary({VnodeId, Amount}),
    store(StorageState, OpKey, Val),
    maybe_store_opcount(StorageState, OpCount),
    {reply, {ok, Val}, State#state{op_count=OpCount}};
handle_command({merge, Key, Val, ReqId}, _Sender, State) ->
    #state{storage_state=StorageState, op_count=OpCount0} = State,
    OpCount = OpCount0 + 1,
    OpKey = counter_key(Key, OpCount),
    store(StorageState, OpKey, Val),
    maybe_store_opcount(StorageState, OpCount),
    {reply, {ReqId, ok}, State#state{op_count=OpCount}};
handle_command({get, Key, ReqId}, _Sender, State) ->
    #state{storage_state=StorageState, op_count=OpCount, partition=Idx, node=Node} = State,
    %% Fold from Key, LowestPossibleOpCount
    %% (LPOC should be the last snapshotted opcount
    %% for that Key (where is that, hmmm?)
    %% but for now, use zero)
    %% To Key, CurrentOpCount)
    %% then turn all those elements
    %% into a PN-Counter and return it
    %% Checkpointed counter is stored on disk as a state based CRDT
    %% and an opcount, at Key, get that
    CheckPointKey = checkpoint_key(Key),
    {CPOpCount, PNCounter0} = get_checkpoint(StorageState, CheckPointKey),
    KeyRange = make_key_range(Key, CPOpCount, OpCount),
    FoldFun = fun(_K, V, {Size, PNCount}) ->
                      {VnodeId, Count} = binary_to_term(V),
                      case Count of
                          N when N < 0 ->
                              {Size+1, count_pncounter:update({decrement, N*-1}, VnodeId, PNCount)};
                          P ->
                              {Size+1, count_pncounter:update({increment, P}, VnodeId, PNCount)}
                      end
              end,
    Acc = {0, PNCounter0},
    {Size, PNCounter} = hanoidb:fold_range(StorageState, FoldFun, Acc, KeyRange),
    lager:info("read size was ~p~n", [Size]),
    Reply = case PNCounter of
                [{},{}] -> notfound; %% Empty PN-Counter, BAD, relies on knowledge of internal datastructure
                _ -> {ok, {count_pncounter, PNCounter}}
            end,
    %% Checkpoint the counter again
    case Size > 0 of
        true ->
            ok = save_checkpoint(StorageState, CheckPointKey, {OpCount, PNCounter});
        _ -> ok
    end,
    {reply, {ReqId, {{Idx, Node}, Reply}}, State};
handle_command({repair, Key, PNCounter}, _Sender, State) ->
    %% Not implemented yet
    lager:warn("Asked to merge ~p ~p~n", [Key, PNCounter]),
    {reply, ok, State};
handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command, Message}),
    {noreply, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(_ObjectName, _ObjectValue) ->
    <<>>.

is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.


terminate(_Reason, undefined) ->
    ok;
terminate(_Reason, State) ->
    #state{op_count = OpCount, storage_state = StorageState} = State,
    store_opcount(StorageState, OpCount),
    stop_storage(StorageState),
    ok.

%% ---------
%% Private
%% ---------

%% ---------
%% Storage
%% ---------
start_storage(Partition) ->
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

checkpoint_key(Key) ->
    sext:encode({s, ?COUNTERS, Key}).

get_checkpoint(StorageState, CheckPointKey) ->
    case hanoidb:get(StorageState, CheckPointKey) of
        {ok, Val} ->
            binary_to_term(Val);
        not_found ->
            {0, count_pncounter:new()};
        Error ->
            Error
    end.

save_checkpoint(StorageState, CheckPointKey, {OpCount, PNCounter}) ->
    %% @TODO shouldn't we then delete the keys we don't need?
    lager:info("Rollup of at ~p of ~p for ~p~n", [OpCount, count_pncounter:value(PNCounter), sext:decode(CheckPointKey)]),
    store(StorageState, CheckPointKey, term_to_binary({OpCount, PNCounter})).

store_opcount(StorageState, OpCount) ->
    lager:info("Op count checkpoint at ~p~n", [OpCount]),
    Key = op_count_key(),
    store(StorageState, Key, list_to_binary(integer_to_list(OpCount))).

stop_storage(StorageState) ->
    ok = hanoidb:close(StorageState).

%% -------
%% Vnode
%% -------
op_count_key() ->
    <<$o,$c>>.

maybe_store_opcount(StorageState, OpCount) when OpCount rem ?OP_THRESHOLD =:= 0 ->
    store_opcount(StorageState, OpCount);
maybe_store_opcount(_StorageState, _OpCount) ->
    ok.

get_op_count(StorageState) ->
    %% wtf here?
    %% get largest keys value?
    %% get checkpoint + 1000?
    case hanoidb:get(StorageState, op_count_key()) of
        {ok, Val0} ->
            Val = list_to_integer(binary_to_list(Val0)),
            Val + ?OP_THRESHOLD;
        _ ->
            %% No op-count found, ensure a monotonically increasing op count
            {Mega, Sec, Micro} = erlang:now(),
            (Mega * 1000000 + Sec) * 1000000 + Micro +?OP_THRESHOLD
    end.

