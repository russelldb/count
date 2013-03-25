%%%-------------------------------------------------------------------
%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2013, Russell Brown
%%% @doc
%%% If a key has N updates against it, roll them up.
%%% An aysnc vnode worker for count_vnode.Started with some
%%% access to the backend. Will attempt to rollup latest writes to a key
%%% @end
%%% Created : 23 Mar 2013 by Russell Brown <russelldb@basho.com>
%%%-------------------------------------------------------------------
-module(count_rollup_worker).

-behaviour(gen_server).

%% API
-export([start_link/1, increment/4, read/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(ETS_OPTS, [ordered_set, private]).

-record(state, {partition :: non_neg_integer(),
                tid :: ets:tid(),
                storage_state :: term()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Partition) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Partition], []).

increment(Pid, Key, OpCount, OpThreshold) ->
    gen_server:cast(Pid, {incr, Key, OpCount, OpThreshold}).

read(Pid, Key) ->
    gen_server:cast(Pid, {read, Key}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Partition]) ->
    Tid = ets:new(?MODULE, ?ETS_OPTS),
    {ok, _DataDir, StorageState} = count_db:start(Partition),
    {ok, #state{partition = Partition, tid = Tid, storage_state=StorageState}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({incr, Key, OpCount, OpThreshold}, State) ->
    #state{tid=Tid, storage_state=StorageState} = State,
    ets:insert_new(Tid, Key, 0),
    UpdateCount =  ets:increment_counter(Tid, Key),
    case UpdateCount >= OpThreshold of
        true ->
            rollup(Key, OpCount, StorageState);
        false -> ok
    end,
    {noreply, State};
handle_cast({read, Key}, State) ->
    #state{tid=Tid} = State,
    ets:delete(Tid, Key),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
rollup(Key, OpCount, StorageState) ->
    %% Ideally, this should be a separate process, maybe from a bounded pool
    {CheckPointKey, Size, PNCounter} = count_db:get_counter(StorageState, Key, OpCount),
    case Size > 0 of
        true ->
            ok = count_db:save_checkpoint(StorageState, CheckPointKey, {OpCount, PNCounter}),
            count_db:read(RPid, Key);
        _ -> ok
    end.
