%% -------------------------------------------------------------------
%%
%% count_gcounter: A state based, grow only, convergent counter
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

%%% @doc
%%% a G-Counter CRDT, borrows liberally from argv0 and Justin Sheehy's vclock module
%%% @end

-module(count_gcounter).

-export([new/0, value/1, update/3, merge/2, equal/2]).

-opaque counter() :: [{Actor::term(), Count::non_neg_integer()}].

-export_type([counter/0]).

new() ->
    [].

value(GCnt) ->
    lists:sum([ Cnt || {_Act, Cnt} <- GCnt]).

update(increment, Actor, GCnt) ->
    increment_by(1, Actor, GCnt);
update({increment, Amount}, Actor, GCnt) when is_integer(Amount), Amount > 0 ->
    increment_by(Amount, Actor, GCnt).

merge(GCnt1, GCnt2) ->
    merge(GCnt1, GCnt2, []).

merge([], [], Acc) ->
    lists:reverse(Acc);
merge(LeftOver, [], Acc) ->
    lists:reverse(Acc, LeftOver);
merge([], LeftOver, Acc) ->
    lists:reverse(Acc, LeftOver);
merge([{Actor1, Cnt1}=AC1|Rest], Clock2, Acc) ->
    case lists:keytake(Actor1, 1, Clock2) of
        {value, {Actor1, Cnt2}, RestOfClock2} ->
            merge(Rest, RestOfClock2, [{Actor1, max(Cnt1, Cnt2)}|Acc]);
        false ->
            merge(Rest, Clock2, [AC1|Acc])
    end.

equal(VA,VB) ->
    lists:sort(VA) =:= lists:sort(VB).

%% priv
increment_by(Amount, Actor, GCnt) when is_integer(Amount), Amount > 0 ->
    {Ctr, NewGCnt} = case lists:keytake(Actor, 1, GCnt) of
                         false ->
                             {Amount, GCnt};
                         {value, {_N, C}, ModGCnt} ->
                             {C + Amount, ModGCnt}
                     end,
    [{Actor, Ctr}|NewGCnt].
