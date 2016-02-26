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
-module(leo_membership_cluster_local_tests).

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

-define(TEST_RING_HASH,   {1050503645, 1050503645}).
-define(TEST_MEMBER_HASH, 3430631340).
-define(CLUSTER_ID, 'leofs_c1').

membership_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [{with, [T]} || T <- [fun membership_manager_/1,
                           fun membership_storage_/1
                          ]]}.

setup() ->
    %% preparing network.
    [] = os:cmd("epmd -daemon"),
    {ok, Hostname} = inet:gethostname(),

    Me = list_to_atom("test_0@" ++ Hostname),
    net_kernel:start([Me, shortnames]),

    catch ets:delete_all_objects(?MEMBER_TBL_CUR),
    catch ets:delete_all_objects(?MEMBER_TBL_PREV),
    catch ets:delete_all_objects('leo_ring_cur'),
    catch ets:delete_all_objects('leo_ring_prv'),

    {ok, Node0} = slave:start_link(list_to_atom(Hostname), 'node_0'),
    {ok, Node1} = slave:start_link(list_to_atom(Hostname), 'node_1'),
    {ok, Node2} = slave:start_link(list_to_atom(Hostname), 'node_2'),
    {ok, Mgr0}  = slave:start_link(list_to_atom(Hostname), 'manager_master'),
    {ok, Mgr1}  = slave:start_link(list_to_atom(Hostname), 'manager_slave'),

    true = rpc:call(Node0, code, add_path, ["../deps/meck/ebin"]),
    true = rpc:call(Node1, code, add_path, ["../deps/meck/ebin"]),
    true = rpc:call(Node2, code, add_path, ["../deps/meck/ebin"]),
    true = rpc:call(Mgr0,  code, add_path, ["../deps/meck/ebin"]),
    true = rpc:call(Mgr1,  code, add_path, ["../deps/meck/ebin"]),
    timer:sleep(100),

    %% start applications
    leo_misc:init_env(),
    leo_misc:set_env(?APP, ?PROP_SERVER_TYPE, ?MONITOR_NODE),

    application:start(crypto),
    application:start(mnesia),
    application:start(leo_redundant_manager),

    leo_cluster_tbl_member:create_table(ram_copies, [node()], ?MEMBER_TBL_CUR),
    {Hostname, Mgr0, Mgr1, Node0, Node1, Node2}.

teardown({_, Mgr0, Mgr1, Node0, Node1, Node2}) ->
    application:stop(leo_redundant_manager),
    application:stop(mnesia),
    application:stop(crypto),
    meck:unload(),

    net_kernel:stop(),
    slave:stop(Mgr0),
    slave:stop(Mgr1),
    slave:stop(Node0),
    slave:stop(Node1),
    slave:stop(Node2),

    Path = filename:absname("") ++ "db",
    os:cmd("rm -rf " ++ Path),
    ok.

membership_manager_({Hostname, _, _, Node0, Node1, Node2}) ->
    ok = rpc:call(Node0, meck, new,    [leo_redundant_manager_api, [no_link, non_strict]]),
    ok = rpc:call(Node0, meck, expect, [leo_redundant_manager_api, checksum,
                                        fun(ring) ->
                                                {ok, ?TEST_RING_HASH};
                                           (member) ->
                                                {ok, ?TEST_MEMBER_HASH}
                                        end]),
    ok = rpc:call(Node1, meck, new,    [leo_redundant_manager_api, [no_link, non_strict]]),
    ok = rpc:call(Node1, meck, expect, [leo_redundant_manager_api, checksum,
                                        fun(ring) ->
                                                {ok, ?TEST_RING_HASH};
                                           (member) ->
                                                {ok, ?TEST_MEMBER_HASH}
                                        end]),
    ok = rpc:call(Node2, meck, new,    [leo_redundant_manager_api, [no_link, non_strict]]),
    ok = rpc:call(Node2, meck, expect, [leo_redundant_manager_api, checksum,
                                        fun(ring) ->
                                                {ok, []};
                                           (member) ->
                                                {ok, -1}
                                        end]),

    %% Path = filename:absname("") ++ "db/queue",
    {ok,_RefSup} = leo_redundant_manager_sup:start_link(),

    CallbackFun = fun()-> ok end,
    Options = [{mq_dir, "work/mq-dir/"},
               {monitors, ['manager_0@127.0.0.1', 'manager_1@127.0.0.1']},
               {membership_callback, CallbackFun},
               {system_conf, [{n, 3}, {w, 1}, {r ,1}, {d, 1}]}],
    ok = leo_redundant_manager_api:start(
           ?CLUSTER_ID, 'persistent_node', Options),

    Node_1 = list_to_atom("node_0@" ++ Hostname),
    Node_2 = list_to_atom("node_1@" ++ Hostname),
    Node_3 = list_to_atom("node_2@" ++ Hostname),

    leo_redundant_manager_api:attach(
      ?CLUSTER_ID, #?MEMBER{id = {?CLUSTER_ID, Node_1},
                            cluster_id = ?CLUSTER_ID,
                            node = Node_1,
                            ip = "127.0.0.1",
                            state = 'attached'
                           }),
    leo_redundant_manager_api:attach(
      ?CLUSTER_ID, #?MEMBER{id = {?CLUSTER_ID, Node_2},
                            cluster_id = ?CLUSTER_ID,
                            node = Node_2,
                            ip = "127.0.0.1",
                            state = 'attached'
                           }),
    leo_redundant_manager_api:attach(
      ?CLUSTER_ID, #?MEMBER{id = {?CLUSTER_ID, Node_3},
                            cluster_id = ?CLUSTER_ID,
                            node = Node_3,
                            ip = "127.0.0.1",
                            state = 'attached'
                           }),
    {ok,_Members,_Chksums} =
        leo_redundant_manager_api:create(?CLUSTER_ID, ?VER_CUR),
    ?debugVal({_Members,_Chksums}),
    timer:sleep(1500),
    ok.


membership_storage_({Hostname, Mgr0, Mgr1, Node0, Node1, Node2}) ->
    ok = rpc:call(Node0, meck, new,    [leo_redundant_manager_api, [no_link, non_strict]]),
    ok = rpc:call(Node0, meck, expect, [leo_redundant_manager_api, checksum,
                                        fun(ring) ->
                                                {ok, ?TEST_RING_HASH};
                                           (member) ->
                                                {ok, ?TEST_MEMBER_HASH}
                                        end]),
    ok = rpc:call(Node1, meck, new,    [leo_redundant_manager_api, [no_link, non_strict]]),
    ok = rpc:call(Node1, meck, expect, [leo_redundant_manager_api, checksum,
                                        fun(ring) ->
                                                {ok, ?TEST_RING_HASH};
                                           (member) ->
                                                {ok, ?TEST_MEMBER_HASH}
                                        end]),
    ok = rpc:call(Node2, meck, new,    [leo_redundant_manager_api, [no_link, non_strict]]),
    ok = rpc:call(Node2, meck, expect, [leo_redundant_manager_api, checksum,
                                        fun(ring) ->
                                                {ok, []};
                                           (member) ->
                                                {ok, -1}
                                        end]),

    ok = rpc:call(Mgr0, meck, new,    [leo_manager_api, [no_link, non_strict]]),
    ok = rpc:call(Mgr0, meck, expect, [leo_manager_api, synchronize,
                                       fun(_Type, _Node) ->
                                               ok
                                       end]),
    ok = rpc:call(Mgr1, meck, new,    [leo_manager_api, [no_link, non_strict]]),
    ok = rpc:call(Mgr1, meck, expect, [leo_manager_api, synchronize,
                                       fun(_Type, _Node) ->
                                               ok
                                       end]),

    CallbackFun = fun()-> ok end,
    Options = [{mq_dir, "db/queue/"},
               {monitors, [list_to_atom("manager_master@" ++ Hostname),
                           list_to_atom("manager_slave@" ++ Hostname)]},
               {membership_callback, CallbackFun},
               {system_conf, [{n, 3}, {w, 1}, {r ,1}, {d, 1}]}],
    ok = leo_redundant_manager_api:start(
           ?CLUSTER_ID, 'persistent_node', Options),

    Node_1 = list_to_atom("node_0@" ++ Hostname),
    Node_2 = list_to_atom("node_1@" ++ Hostname),
    Node_3 = list_to_atom("node_2@" ++ Hostname),

    leo_redundant_manager_api:attach(
      ?CLUSTER_ID, #?MEMBER{id = {?CLUSTER_ID, Node_1},
                            cluster_id = ?CLUSTER_ID,
                            node = Node_1,
                            ip = "127.0.0.1",
                            state = 'attached'
                           }),
    leo_redundant_manager_api:attach(
      ?CLUSTER_ID, #?MEMBER{id = {?CLUSTER_ID, Node_2},
                            cluster_id = ?CLUSTER_ID,
                            node = Node_2,
                            ip = "127.0.0.1",
                            state = 'attached'
                           }),
    leo_redundant_manager_api:attach(
      ?CLUSTER_ID, #?MEMBER{id = {?CLUSTER_ID, Node_3},
                            cluster_id = ?CLUSTER_ID,
                            node = Node_3,
                            ip = "127.0.0.1",
                            state = 'attached'
                           }),
    {ok,_Members,_Chksums} =
        leo_redundant_manager_api:create(?CLUSTER_ID, ?VER_CUR),
    ?debugVal({_Members,_Chksums}),
    timer:sleep(1500),

    %% History0 = rpc:call(Mgr0, meck, history, [leo_manager_api]),
    %% ?assertEqual(true, length(History0) > 0),
    History1 = rpc:call(Mgr1, meck, history, [leo_manager_api]),
    ?assertEqual([], History1),
    ok.

-endif.
