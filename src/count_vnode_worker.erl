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
-module(count_vnode_worker).

-behaviour(riak_core_vnode_worker).

-export([init_worker/3,
         handle_work/3]).

-include_lib("riak_core/include/riak_core_vnode.hrl").

-record(state, {index :: partition()}).

%% @TODO ask those that know, which is better,
%% start and keep a storage state open
%% or open per use.
init_worker(Index, [], _Props) ->
    {ok, #state{index=Index}}.

handle_work({rollup, Key, OpCount, StorageState}, _Sender, State) ->
    lager:info("Rolling up ~p", [Key]),
    KeysToDelete = rollup(Key, OpCount, StorageState),
    delete(KeysToDelete, StorageState),
    {noreply, State};
handle_work({delete, KeyList, StorageState}, _Sender, State) ->
    delete(KeyList, StorageState),
    {noreply, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
rollup(Key, OpCount, StorageState) ->
    {CheckPointKey, KeysToDelete,  Size, PNCounter} = count_db:get_counter(StorageState, Key, OpCount),
    case Size > 0 of
        true ->
            ok = count_db:save_checkpoint(StorageState, CheckPointKey, {OpCount, PNCounter});
        _ -> ok
    end,
    KeysToDelete.

delete(Keys, StorageState) ->
    count_db:delete(StorageState, Keys).


