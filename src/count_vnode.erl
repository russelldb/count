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
                partition :: partition(),
                dirty_keys_tid :: ets:tid()}). %% Worst case could mean holding _all_ keys in
                                               %% memory, should bound size, evict by age / lru?

-define(MASTER, count_vnode_master).
-define(OP_THRESHOLD, 1000).
-define(ETS_OPTS, [ordered_set, private]).

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
    riak_core_vnode_master:command(PrefList, {repair, Key, PNCounter}, ignore, ?MASTER).

init([Partition]) ->
    %% Should scan all keys and build rollups in case a key too a lot
    %% of writes, but we crashed before the threshold, or it was never
    %% read, and remains unrolled up forever.
    Node = node(),
    random:seed(erlang:now()),
    VnodeId = uuid:v4(),
    {ok, DataDir, StorageState} = count_db:start(Partition),
    OpCount = get_op_count(StorageState),
    DirtyKeysTid = ets:new(?MODULE, ?ETS_OPTS),
    PoolSize = app_helper:get_env(count, worker_pool_size, 10), %% realy?
    %% pool size is sort of HOT key size, so a heuristic would be better
    {ok, #state { data_dir = DataDir,
                  node = Node,
                  op_count = OpCount,
                  storage_state = StorageState,
                  vnode_id = VnodeId,
                  partition = Partition,
                  dirty_keys_tid = DirtyKeysTid
                },
     [{pool, count_vnode_worker, PoolSize, []}]}.

handle_command({increment, Key, Amount}, Sender, State0) ->
    #state{storage_state=StorageState, vnode_id=VnodeId,
           op_count=OpCount0, dirty_keys_tid=DirtyKeysTid} = State0,
    OpCount = OpCount0+1,
    OpKey = count_db:counter_key(Key, OpCount),
    Val = term_to_binary({VnodeId, Amount}),
    count_db:store(StorageState, OpKey, Val),
    riak_core_vnode:reply(Sender, {ok, Val}),
    ets:insert_new(DirtyKeysTid, {Key, 0}),
    UpdateCount = ets:update_counter(DirtyKeysTid, Key, 1),
    State = State0#state{op_count=OpCount},
    Out = case UpdateCount >= ?OP_THRESHOLD of
              true ->
                  %% Should we somehow "lock" here, to preserve
                  %% the keys op count, but not try and  roll up
                  %% for some same period of time
                  %% for now, naively, just assume rollups work
                  ets:delete(DirtyKeysTid, Key),
                  {async, {rollup, Key, OpCount, StorageState}, ignore, State};
              false -> {noreply, State}
          end,
    maybe_store_opcount(StorageState, OpCount),
    Out;
handle_command({merge, Key, Val, ReqId}, Sender, State0) ->
    #state{storage_state=StorageState, op_count=OpCount0, dirty_keys_tid=DirtyKeysTid} = State0,
    OpCount = OpCount0 + 1,
    OpKey = count_db:counter_key(Key, OpCount),
    count_db:store(StorageState, OpKey, Val),
    riak_core_vnode:reply(Sender, {ReqId, ok}),
    ets:insert_new(DirtyKeysTid, {Key, 0}),
    UpdateCount =  ets:update_counter(DirtyKeysTid, Key, 1),
    State = State0#state{op_count=OpCount},
    Out = case UpdateCount >= ?OP_THRESHOLD of
              true ->
                  %% Should we somehow "lock" here, to preserve
                  %% the keys op count, but not try and  roll up
                  %% for some same period of time
                  %% for now, naively, just assume rollups work
                  ets:delete(DirtyKeysTid, Key),
                  {async, {rollup, Key, OpCount, StorageState}, ignore, State};
              false -> {noreply, State}
          end,
    maybe_store_opcount(StorageState, OpCount),
    Out;
handle_command({get, Key, ReqId}, Sender, State) ->
    #state{storage_state=StorageState, op_count=OpCount,
           partition=Idx, node=Node, dirty_keys_tid=DirtyKeysTid} = State,
    %% Fold from Key, LowestPossibleOpCount
    %% (LPOC should be the last snapshotted opcount)
    %% To Key, CurrentOpCount
    %% then turn all those elements
    %% into a PN-Counter and return it
    %% Checkpointed counter is stored on disk as {OpCount, a state based CRDT}
    {CheckPointKey, KeysToDelete, Size, PNCounter} = count_db:get_counter(StorageState, Key, OpCount),
    lager:info("read size was ~p~n", [Size]),
    Reply = case PNCounter of
                {[], []} -> notfound; %% Empty PN-Counter, BAD, relies on knowledge of internal datastructure
                _ -> {ok, {count_pncounter, PNCounter}}
            end,
    %% Checkpoint the counter again
    case Size > 0 of
        true ->
            ok = count_db:save_checkpoint(StorageState, CheckPointKey, {OpCount, PNCounter}),
            ets:delete(DirtyKeysTid, Key);
        _ -> ok
    end,
    riak_core_vnode:reply(Sender, {ReqId, {{Idx, Node}, Reply}}),
    Out = case KeysToDelete of
              [] ->
                  {noreply, State};
              _ -> {async, {delete, KeysToDelete, StorageState}, ignore, State}
          end,
    Out;
handle_command({repair, Key, {count_pncounter, PNCounter}}, Sender, State) ->
    lager:info("Getting read repaired at key ~p with val ~p~n", [Key, PNCounter]),
    #state{op_count=OpCount, storage_state=StorageState, dirty_keys_tid=DirtyKeysTid} = State,
    %% Read reair receives a state based PN-Counter, it seems the best thing to do
    %% is do a local get (merge with the rr counter, and save the rollup)
    {CheckPointKey, KeysToDelete, _Size, LocalCounter} = coount_db:get_counter(StorageState, Key, OpCount),
    Merged = count_pncounter:merge(PNCounter, LocalCounter),
    case count_pncounter:equal(Merged, LocalCounter) of
        false ->
            count_db:save_checkpoint(StorageState, CheckPointKey, {OpCount, PNCounter}),
            ets:delete(DirtyKeysTid, Key);
        true -> ok
    end,
    riak_core_vnode:reply(Sender, ok),
    Out = case KeysToDelete of
              [] ->
                  {noreply, State};
              _ -> {async, {delete, KeysToDelete, StorageState}, ignore, State}
          end,
    Out;
handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command, Message}),
    {noreply, State}.


%% @doc Handles commands while in the handoff state.
handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, Sender,
                       State=#state{storage_state=StorageState}) ->
    HandoffFoldFun = count_db:fold_and_rollup(StorageState, Fun, Acc0),
    {async, {handoff, HandoffFoldFun}, Sender, State};
handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State) ->
    #state{op_count=OpCount, storage_state=StorageState, dirty_keys_tid=DirtyKeysTid} = State,
    {Key, PNCounter} = binary_to_term(Data),
    %% Merge this, just like a repair
    {CheckPointKey, KeysToDelete, _Size, LocalCounter} = coount_db:get_counter(StorageState, Key, OpCount),
    Merged = count_pncounter:merge(PNCounter, LocalCounter),
    case count_pncounter:equal(Merged, LocalCounter) of
        false ->
            count_db:save_checkpoint(StorageState, CheckPointKey, {OpCount, PNCounter}),
            ets:delete(DirtyKeysTid, Key);
        true -> ok
    end,
    Out = case KeysToDelete of
              [] -> {reply, ok, State};
              _ -> {async, {delete, KeysToDelete, StorageState}, ignore, State}
          end,
    Out.


encode_handoff_item(ObjectName, ObjectValue) ->
    term_to_binary({ObjectName, ObjectValue}).

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
    count_db:stop(StorageState),
    ok.

%% ---------
%% Private
%% ---------

maybe_store_opcount(StorageState, OpCount) when OpCount rem ?OP_THRESHOLD =:= 0 ->
    store_opcount(StorageState, OpCount);
maybe_store_opcount(_StorageState, _OpCount) ->
    ok.

store_opcount(StorageState, OpCount) ->
    lager:info("Op count checkpoint at ~p~n", [OpCount]),
    count_db:store(StorageState, ?OP_COUNT_KEY, list_to_binary(integer_to_list(OpCount))).

get_op_count(StorageState) ->
    case count_db:get(StorageState, ?OP_COUNT_KEY) of
        {ok, Val0} ->
            Val = list_to_integer(binary_to_list(Val0)),
            Val + ?OP_THRESHOLD;
        _ ->
            %% No op-count found, ensure a monotonically increasing op count.
            %% Won't OP_THRESHOLD + 1 do? Either this
            %% vnode is new, has a checkpoint, or crashed before (during?)
            %% writing it's checkpoint
            {Mega, Sec, Micro} = erlang:now(),
            (Mega * 1000000 + Sec) * 1000000 + Micro +?OP_THRESHOLD
    end.

