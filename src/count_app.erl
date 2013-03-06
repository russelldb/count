-module(count_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case count_sup:start_link() of
        {ok, Pid} ->
            ok = riak_core:register([{vnode_module, count_vnode}]),
            ok = riak_core_ring_events:add_guarded_handler(count_ring_event_handler, []),
            ok = riak_core_node_watcher_events:add_guarded_handler(count_node_event_handler, []),
            ok = riak_core_node_watcher:service_up(count, self()),

            EntryRoute = {["count", "ping"], count_wm_ping, []},
            webmachine_router:add_route(EntryRoute),

            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
