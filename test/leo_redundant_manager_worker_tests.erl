%%======================================================================
%%
%% Leo Redundant Manager
%%
%% Copyright (c) 2012-2013 Rakuten, Inc.
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
%%======================================================================
-module(leo_redundant_manager_worker_tests).
-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

redundant_manager_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [{with, [T]} || T <- [fun suite_/1
                          ]]}.

setup() ->
    catch ets:delete('leo_members'),
    catch ets:delete('leo_ring_cur'),
    catch ets:delete('leo_ring_prv'),

    leo_misc:init_env(),
    leo_misc:set_env(leo_redundant_manager, 'server_type', 'gateway'),
    {ok, Pid} = leo_redundant_manager_sup:start_link(gateway),
    leo_redundant_manager_api:set_options(
      [{n, 3},{r, 1}, {w ,2},{d, 2},{bit_of_ring, 128},{level_2, 0}]),
    leo_redundant_manager_api:attach('node_0@127.0.0.1'),
    leo_redundant_manager_api:attach('node_1@127.0.0.1'),
    leo_redundant_manager_api:attach('node_2@127.0.0.1'),
    leo_redundant_manager_api:attach('node_3@127.0.0.1'),
    leo_redundant_manager_api:attach('node_4@127.0.0.1'),
    leo_redundant_manager_api:create(),
    timer:sleep(1500),
    Pid.

teardown(Pid) ->
    timer:sleep(200),
    exit(Pid, normal),
    ok.

suite_(_) ->
    RingWorker1 = poolboy:checkout('ring_worker_pool'),
    Res0 = leo_redundant_manager_worker:first(RingWorker1, {ets, 'leo_ring_cur'}),

    Res1 = leo_redundant_manager_worker:next(
             RingWorker1, {ets, 'leo_ring_cur'}, 0),
    Res2 = leo_redundant_manager_worker:next(
             RingWorker1, {ets, 'leo_ring_cur'}, 243058967694854461280959919528606475),
    Res3 = leo_redundant_manager_worker:next(
             RingWorker1, {ets, 'leo_ring_cur'}, 340282366920938463463374607431768211456),

    Res4 = leo_redundant_manager_worker:lookup(
             RingWorker1, {ets, 'leo_ring_cur'}, 0),
    Res5 = leo_redundant_manager_worker:lookup(
             RingWorker1, {ets, 'leo_ring_cur'}, 1264314306571079495751037749109419166),
    Res6 = leo_redundant_manager_worker:lookup(
             RingWorker1, {ets, 'leo_ring_cur'}, 3088066518744027498382227205172020754),
    Res7 = leo_redundant_manager_worker:lookup(
             RingWorker1, {ets, 'leo_ring_cur'}, 4870818527799149765629747665733758595),
    Res8 = leo_redundant_manager_worker:lookup(
             RingWorker1, {ets, 'leo_ring_cur'}, 5257965865843856950061366315134191522),
    Res9 = leo_redundant_manager_worker:lookup(
             RingWorker1, {ets, 'leo_ring_cur'}, 340282366920938463463374607431768211456),

    Res10 = leo_redundant_manager_worker:last(RingWorker1, {ets, 'leo_ring_cur'}),
    Res11 = leo_redundant_manager_worker:prev(
              RingWorker1, {ets, 'leo_ring_cur'}, 5257965865843856950061366315134191522),
    Res12 = leo_redundant_manager_worker:prev(
              RingWorker1, {ets, 'leo_ring_cur'}, 0),
    Res13 = leo_redundant_manager_worker:prev(
              RingWorker1, {ets, 'leo_ring_cur'}, 340282366920938463463374607431768211456),
    Res14 = leo_redundant_manager_worker:prev(
              RingWorker1, {ets, 'leo_ring_cur'}, 5257965865843856950061366315134191523),

    ?assertEqual(243058967694854461280959919528606474, Res0),
    ?assertEqual(243058967694854461280959919528606474, Res1),
    ?assertEqual(Res0, Res1),
    ?assertEqual(643266634996242345494209403527351060, Res2),
    ?assertEqual('$end_of_table', Res3),
    ?assertEqual([], Res4),
    ?assertEqual('node_2@127.0.0.1', Res5),
    ?assertEqual('node_1@127.0.0.1', Res6),
    ?assertEqual('node_1@127.0.0.1', Res7),
    ?assertEqual('node_4@127.0.0.1', Res8),
    ?assertEqual([], Res9),
    ?assertEqual(340273463498854239912439946299330026060, Res10),
    ?assertEqual(4870818527799149765629747665733758595,   Res11),
    ?assertEqual('$end_of_table', Res12),
    ?assertEqual(329347436126708067719614132431830616781, Res13),
    ?assertEqual(5257965865843856950061366315134191522,   Res14),

    poolboy:checkin('ring_worker_pool', RingWorker1),
    ok.

-endif.
