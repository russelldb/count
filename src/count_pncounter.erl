%% -------------------------------------------------------------------
%%
%% count_pncounter: A convergent, replicated, state based PN counter
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

-module(count_pncounter).

%% API
-export([new/0, value/1, update/3, merge/2, equal/2]).

-opaque counter() :: {count_gcounter:counter(), count_gcounter:counter()}.

-export_type([counter/0]).

new() ->
    {count_gcounter:new(), count_gcounter:new()}.

value({Incr, Decr}) ->
    count_gcounter:value(Incr) -  count_gcounter:value(Decr).

update(increment, Actor, {Incr, Decr}) ->
    {count_gcounter:update(increment, Actor, Incr), Decr};
update({increment, By}, Actor, {Incr, Decr}) when is_integer(By), By > 0 ->
    {count_gcounter:update({increment, By}, Actor, Incr), Decr};
update(decrement, Actor, {Incr, Decr}) ->
    {Incr, count_gcounter:update(increment, Actor, Decr)};
update({decrement, By}, Actor, {Incr, Decr}) when is_integer(By), By > 0 ->
    {Incr, count_gcounter:update({increment, By}, Actor, Decr)}.

merge({Incr1, Decr1}, {Incr2, Decr2}) ->
    MergedIncr =  count_gcounter:merge(Incr1, Incr2),
    MergedDecr =  count_gcounter:merge(Decr1, Decr2),
    {MergedIncr, MergedDecr}.

equal({Incr1, Decr1}, {Incr2, Decr2}) ->
    count_gcounter:equal(Incr1, Incr2) andalso count_gcounter:equal(Decr1, Decr2).

