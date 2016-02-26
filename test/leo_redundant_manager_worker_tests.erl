%%======================================================================
%%
%% Leo Redundant Manager
%%
%% Copyright (c) 2012-2016 Rakuten, Inc.
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

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(CLUSTER_ID, 'leofs_c1').


%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).


suite_test_() ->
    {setup,
     fun () ->
             setup()
     end,
     fun (Pid) ->
             teardown(Pid)
     end,
     [{"test all functions",
       {timeout, 300, fun suite/0}}
     ]}.

setup() ->
    [] = os:cmd("epmd -daemon"),
    {ok, Hostname} = inet:gethostname(),
    Me = list_to_atom("test_0@" ++ Hostname),
    net_kernel:start([Me, shortnames]),

    timer:sleep(500),
    application:start(crypto),
    application:start(mnesia),
    application:start(leo_redundant_manager),

    catch ets:delete(?MEMBER_TBL_CUR),
    catch ets:delete(?MEMBER_TBL_PREV),
    catch ets:delete(?RING_TBL_CUR),
    catch ets:delete(?RING_TBL_PREV),

    %% ok = leo_cluster_tbl_member:create_table(
    %%        'ram_copies', [node()], ?MEMBER_TBL_CUR),
    %% ok = leo_cluster_tbl_member:create_table(
    %%        'ram_copies', [node()], ?MEMBER_TBL_PREV),
    %% ok = leo_cluster_tbl_ring:create_table_current(ram_copies),
    %% ok = leo_cluster_tbl_ring:create_table_prev(ram_copies),

    catch ets:new(?MEMBER_TBL_CUR, [named_table, ordered_set,
                                    public, {read_concurrency, true}]),
    catch ets:new(?MEMBER_TBL_PREV,[named_table, ordered_set,
                                    public, {read_concurrency, true}]),
    catch ets:new(?RING_TBL_CUR, [named_table, ordered_set,
                                  public, {read_concurrency, true}]),
    catch ets:new(?RING_TBL_PREV,[named_table, ordered_set,
                                  public, {read_concurrency, true}]),
    CallbackFun = fun()-> ok end,
    Options = [{mq_dir, "work/mq-dir/"},
               {monitors, []},
               {membership_callback, CallbackFun},
               {system_conf, [{n, 3}, {w, 2}, {r ,1}, {d, 2}]}],
    ok = leo_redundant_manager_api:start(
           ?CLUSTER_ID, 'persistent_node', Options),

    Node_0 = list_to_atom("node_0@" ++ Hostname),
    Node_1 = list_to_atom("node_1@" ++ Hostname),
    Node_2 = list_to_atom("node_2@" ++ Hostname),
    Node_3 = list_to_atom("node_3@" ++ Hostname),
    Node_4 = list_to_atom("node_4@" ++ Hostname),
    Node_5 = list_to_atom("node_5@" ++ Hostname),
    Node_6 = list_to_atom("node_6@" ++ Hostname),
    Node_7 = list_to_atom("node_7@" ++ Hostname),
    leo_redundant_manager_api:attach(?CLUSTER_ID,
                                     #?MEMBER{id = {?CLUSTER_ID, Node_0},
                                              cluster_id = ?CLUSTER_ID,
                                              node = Node_0,
                                              alias= "node_0",
                                              state = ?STATE_ATTACHED}),
    leo_redundant_manager_api:attach(?CLUSTER_ID,
                                     #?MEMBER{id = {?CLUSTER_ID, Node_1},
                                              cluster_id = ?CLUSTER_ID,
                                              node = Node_1,
                                              alias= "node_1",
                                              state = ?STATE_ATTACHED}),
    leo_redundant_manager_api:attach(?CLUSTER_ID,
                                     #?MEMBER{id = {?CLUSTER_ID, Node_2},
                                              cluster_id = ?CLUSTER_ID,
                                              node = Node_2,
                                              alias= "node_2",
                                              state = ?STATE_ATTACHED}),
    leo_redundant_manager_api:attach(?CLUSTER_ID,
                                     #?MEMBER{id = {?CLUSTER_ID, Node_3},
                                              cluster_id = ?CLUSTER_ID,
                                              node = Node_3,
                                              alias= "node_3",
                                              state = ?STATE_ATTACHED}),
    leo_redundant_manager_api:attach(?CLUSTER_ID,
                                     #?MEMBER{id = {?CLUSTER_ID, Node_4},
                                              cluster_id = ?CLUSTER_ID,
                                              node = Node_4,
                                              alias= "node_4",
                                              state = ?STATE_ATTACHED}),
    leo_redundant_manager_api:attach(?CLUSTER_ID,
                                     #?MEMBER{id = {?CLUSTER_ID, Node_5},
                                              cluster_id = ?CLUSTER_ID,
                                              node = Node_5,
                                              alias= "node_5",
                                              state = ?STATE_ATTACHED}),
    leo_redundant_manager_api:attach(?CLUSTER_ID,
                                     #?MEMBER{id = {?CLUSTER_ID, Node_6},
                                              cluster_id = ?CLUSTER_ID,
                                              node = Node_6,
                                              alias= "node_6",
                                              state = ?STATE_ATTACHED}),
    leo_redundant_manager_api:attach(?CLUSTER_ID,
                                     #?MEMBER{id = {?CLUSTER_ID, Node_7},
                                              cluster_id = ?CLUSTER_ID,
                                              node = Node_7,
                                              alias= "node_7",
                                              state = ?STATE_ATTACHED}),
    {ok,_,_} = leo_redundant_manager_api:create(?CLUSTER_ID, ?VER_CUR),
    {ok,_,_} = leo_redundant_manager_api:create(?CLUSTER_ID, ?VER_PREV),
    timer:sleep(1000),
    ok.

teardown(_) ->
    net_kernel:stop(),
    application:stop(leo_redundant_manager),
    application:stop(mnesia),
    application:stop(crypto),

    os:cmd("rm -rf queue"),
    os:cmd("rm ring_*"),
    ok.

suite() ->
    ?debugVal("=== START - Get Redundancies ==="),
    {ok, #redundancies{vnode_id_to = VNodeIdTo1,
                       nodes = N0}} = leo_redundant_manager_worker:first(
                                        ?CLUSTER_ID, ?RING_TBL_CUR),
    {ok, #redundancies{vnode_id_to = VNodeIdTo2,
                       nodes = N0}} = leo_redundant_manager_worker:first(
                                        ?CLUSTER_ID, 'leo_ring_prv'),
    ?debugVal({VNodeIdTo1, VNodeIdTo2}),

    {ok, #redundancies{nodes = N1}} = leo_redundant_manager_worker:lookup(
                                        ?CLUSTER_ID, ?RING_TBL_CUR, 0),
    {ok, #redundancies{nodes = N2}} = leo_redundant_manager_worker:lookup(
                                        ?CLUSTER_ID, ?RING_TBL_CUR, 1264314306571079495751037749109419166),
    {ok, #redundancies{nodes = N3}} = leo_redundant_manager_worker:lookup(
                                        ?CLUSTER_ID, ?RING_TBL_CUR, 3088066518744027498382227205172020754),
    {ok, #redundancies{nodes = N4}} = leo_redundant_manager_worker:lookup(
                                        ?CLUSTER_ID, ?RING_TBL_CUR, 4870818527799149765629747665733758595),
    {ok, #redundancies{nodes = N5}} = leo_redundant_manager_worker:lookup(
                                        ?CLUSTER_ID, ?RING_TBL_CUR, 5257965865843856950061366315134191522),
    {ok, #redundancies{nodes = N6}} = leo_redundant_manager_worker:lookup(
                                        ?CLUSTER_ID, ?RING_TBL_CUR, 340282366920938463463374607431768211456),
    {ok, #redundancies{nodes = N7}} = leo_redundant_manager_worker:last(
                                        ?CLUSTER_ID, ?RING_TBL_CUR),

    ?assertEqual(3, length(N0)),
    ?assertEqual(3, length(N1)),
    ?assertEqual(3, length(N2)),
    ?assertEqual(3, length(N3)),
    ?assertEqual(3, length(N4)),
    ?assertEqual(3, length(N5)),
    ?assertEqual(3, length(N6)),
    ?assertEqual(3, length(N7)),

    St = leo_date:clock(),
    ok = check_redundancies(100000),
    End = leo_date:clock(),

    ?debugVal((End - St) / 1000),
    ?debugVal("=== END - Get Redundancies ==="),
    ok.


%% @private
check_redundancies(0) ->
    ok;
check_redundancies(Index) ->
    AddrId = leo_redundant_manager_chash:vnode_id(
               128, crypto:rand_bytes(64)),
    {ok, #redundancies{nodes = NodeL}} =
        leo_redundant_manager_worker:lookup(
          ?CLUSTER_ID, ?RING_TBL_CUR, AddrId),
    ?assertEqual(3, length(NodeL)),
    check_redundancies(Index - 1).

-endif.
