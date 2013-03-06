%% -------------------------------------------------------------------
%%
%% count_client: local client for count
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc Interface to count
-module(count_client).
-include("count.hrl").

-export([
         incr/2,
         get/1
        ]).

-define(REQ_TIMEOUT, 60000). %% default to a minute

%%%===================================================================
%%% API
%%%===================================================================

%% @doc increment counter at `Key' by `Amount'
%% note zero is noop.
incr(Key, Amount) when is_integer(Amount), Amount /= 0 ->
    ReqID = mk_reqid(),
    {ok, _} = count_incr_fsm_sup:start_incr_fsm(node(), [ReqID, self(), Key, Amount, ?REQ_TIMEOUT]),
    wait_for_reqid(ReqID, ?REQ_TIMEOUT);
incr(_Key, _Amount) ->
    ok.

get(Key) ->
    ReqID = mk_reqid(),
    {ok, _} = count_get_fsm_sup:start_get_fsm(node(), [ReqID, self(), Key, ?REQ_TIMEOUT]),
    wait_for_reqid(ReqID, ?REQ_TIMEOUT).

%%%===================================================================
%%% Internal Functions
%%%===================================================================
mk_reqid() -> erlang:phash2(erlang:now()).

wait_for_reqid(ReqID, Timeout0) ->
    %% give the fsm 100ms to reply before we cut it off
    Timeout = if is_integer(Timeout0) ->
            Timeout0 + 100;
                 true -> Timeout0
              end,
    receive
        {ReqID, Response} -> Response
    after Timeout ->
            %% Do we need to do something to drain messages on the queue,
            %% if the FSM replies after we timeout?
	    {error, timeout}
    end.
