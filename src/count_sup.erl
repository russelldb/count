-module(count_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->
    VMaster = { count_vnode_master,
                  {riak_core_vnode_master, start_link, [count_vnode]},
                  permanent, 5000, worker, [riak_core_vnode_master]},
    IncrSup = {count_incr_fsm_sup,
                    {count_incr_fsm_sup, start_link, []},
                    permanent, infinity, supervisor, [count_incr_fsm_sup]},

    GetSup = {count_get_fsm_sup,
                    {count_get_fsm_sup, start_link, []},
                    permanent, infinity, supervisor, [count_get_fsm_sup]},
    { ok,
        { {one_for_one, 5, 10},
          [VMaster, IncrSup, GetSup]}}.
