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
-module(leo_membership_mq_client_tests).

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").


%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

-define(NODEDOWN_NODE, 'nodedown_node').
-define(CLUSTER_ID, 'leofs_c1').

membership_mq_client_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [{with, [T]} || T <- [fun pubsub_manager_0_/1,
                           fun pubsub_manager_1_/1,
                           fun pubsub_storage_/1,
                           fun pubsub_gateway_0_/1,
                           fun pubsub_gateway_1_/1
                          ]]}.

setup() ->
    [] = os:cmd("epmd -daemon"),
    {ok, Hostname} = inet:gethostname(),

    Me = list_to_atom("test_0@" ++ Hostname),
    net_kernel:start([Me, shortnames]),

    Args = " -pa ../deps/*/ebin ",
    {ok, Node0} = slave:start_link(list_to_atom(Hostname), 'node_0', Args),
    {ok, Mgr0}  = slave:start_link(list_to_atom(Hostname), 'manager_master', Args),

    true = rpc:call(Node0, code, add_path, ["../deps/meck/ebin"]),
    true = rpc:call(Mgr0,  code, add_path, ["../deps/meck/ebin"]),

    S = os:cmd("pwd"),
    Path = string:substr(S, 1, length(S) -1) ++ "/db",

    application:start(crypto),
    application:start(mnesia),
    application:start(leo_redundant_manager),

    ok = leo_cluster_tbl_member:create_table('ram_copies', [node()], ?MEMBER_TBL_CUR),
    ok = leo_cluster_tbl_member:create_table('ram_copies', [node()], ?MEMBER_TBL_PREV),
    ok = leo_cluster_tbl_member:create_table(?MEMBER_TBL_CUR),
    ok = leo_cluster_tbl_member:create_table(?MEMBER_TBL_PREV),
    {Mgr0, Node0, Path}.

teardown({Mgr0, Node0, Path}) ->
    meck:unload(),

    application:stop(mnesia),
    application:stop(leo_mq),
    application:stop(leo_redundant_manager),

    net_kernel:stop(),
    slave:stop(Mgr0),
    slave:stop(Node0),

    os:cmd("rm -rf " ++ Path),

    timer:sleep(timer:seconds(1)),
    ok.

%% @doc publish
pubsub_manager_0_({Mgr0, _Node0, Path}) ->
    prepare(),
    CallbackFun = fun()-> ok end,
    Options = [{mq_dir, Path},
               {monitors, [Mgr0]},
               {membership_callback, CallbackFun},
               {system_conf, [{n, 3}, {w, 1}, {r ,1}, {d, 1}]}],
    ok = leo_redundant_manager_api:start(
           ?CLUSTER_ID, ?MONITOR_NODE, Options),

    ok = leo_membership_mq_client:publish(
           ?CLUSTER_ID, ?MONITOR_NODE,
           ?NODEDOWN_NODE, ?ERR_TYPE_NODE_DOWN),
    timer:sleep(1500),

    History0 = meck:history(leo_cluster_tbl_member),
    ?assertEqual(true, erlang:length(History0) > 0),
    ok.

pubsub_manager_1_({Mgr0, _, Path}) ->
    prepare(),

    CallbackFun = fun()-> ok end,
    Options = [{mq_dir, Path},
               {monitors, [Mgr0]},
               {membership_callback, CallbackFun},
               {system_conf, [{n, 3}, {w, 1}, {r ,1}, {d, 1}]}],
    ok = leo_redundant_manager_api:start(
           ?CLUSTER_ID, ?MONITOR_NODE, Options),

    ok = leo_membership_mq_client:publish(
           ?CLUSTER_ID, ?MONITOR_NODE,
           ?NODEDOWN_NODE, ?ERR_TYPE_NODE_DOWN),
    timer:sleep(1500),

    History0 = meck:history(leo_cluster_tbl_member),
    ?assertEqual(true, erlang:length(History0) > 0),
    ok.

pubsub_storage_({Mgr0,_, Path}) ->
    prepare(),

    ok = rpc:call(Mgr0, meck, new,    [leo_manager_api, [no_link, non_strict]]),
    ok = rpc:call(Mgr0, meck, expect, [leo_manager_api, notify,
                                       fun(_Type, _Node1, _Node2, _Error) ->
                                               ok
                                       end]),

    CallbackFun = fun()-> ok end,
    Options = [{mq_dir, Path},
               {monitors, [Mgr0]},
               {membership_callback, CallbackFun},
               {system_conf, [{n, 3}, {w, 1}, {r ,1}, {d, 1}]}],
    ok = leo_redundant_manager_api:start(
           ?CLUSTER_ID, ?PERSISTENT_NODE, Options),

    ok = leo_membership_mq_client:publish(
           ?CLUSTER_ID, ?PERSISTENT_NODE,
           ?NODEDOWN_NODE, ?ERR_TYPE_NODE_DOWN),
    timer:sleep(1500),

    History0 = meck:history(leo_cluster_tbl_member),
    ?assertEqual(true, erlang:length(History0) > 0),

    History1 = meck:history(leo_manager_api),
    ?assertEqual([], History1),
    ok.

pubsub_gateway_0_({Mgr0, _, Path}) ->
    prepare(),

    ok = rpc:call(Mgr0, meck, new,    [leo_manager_api, [no_link, non_strict]]),
    ok = rpc:call(Mgr0, meck, expect, [leo_manager_api, notify,
                                       fun(_Type, _Node1, _Node2, _Error) ->
                                               ok
                                       end]),
    CallbackFun = fun()-> ok end,
    Options = [{mq_dir, Path},
               {monitors, [Mgr0]},
               {membership_callback, CallbackFun},
               {system_conf, [{n, 3}, {w, 1}, {r ,1}, {d, 1}]}],
    ok = leo_redundant_manager_api:start(
           ?CLUSTER_ID, ?WORKER_NODE, Options),

    ok = leo_membership_mq_client:publish(
           ?CLUSTER_ID, ?WORKER_NODE,
           ?NODEDOWN_NODE, ?ERR_TYPE_NODE_DOWN),
    timer:sleep(1500),

    %% History0 = meck:history(leo_cluster_tbl_member),
    %% ?assertEqual(true, erlang:length(History0) > 0),
    History1 = meck:history(leo_manager_api),
    ?assertEqual([], History1),
    ok.

pubsub_gateway_1_({Mgr0, _, Path}) ->
    prepare(),

    ok = rpc:call(Mgr0, meck, new,    [leo_manager_api, [no_link, non_strict]]),
    ok = rpc:call(Mgr0, meck, expect, [leo_manager_api, notify,
                                       fun(_Type, _Node1, _Node2, _Error) ->
                                               ok
                                       end]),
    CallbackFun = fun()-> ok end,
    Options = [{mq_dir, Path},
               {monitors, [Mgr0]},
               {membership_callback, CallbackFun},
               {system_conf, [{n, 3}, {w, 1}, {r ,1}, {d, 1}]}],
    ok = leo_redundant_manager_api:start(
           ?CLUSTER_ID, ?WORKER_NODE, Options),

    ok = leo_membership_mq_client:publish(
           ?CLUSTER_ID, ?WORKER_NODE,
           ?NODEDOWN_NODE, ?ERR_TYPE_NODE_DOWN),
    timer:sleep(1500),

    %% History0 = meck:history(leo_cluster_tbl_member),
    %% ?assertEqual(true, erlang:length(History0) > 0),

    History1 = meck:history(leo_manager_api),
    ?assertEqual([], History1),
    ok.


prepare() ->
    meck:new(leo_cluster_tbl_member, [non_strict]),
    meck:expect(leo_cluster_tbl_member, lookup,
                fun(_ClusterId, Node) ->
                        {ok, #?MEMBER{node  = Node,
                                      state = ?STATE_STOP}}
                end),
    meck:expect(leo_cluster_tbl_member, find_by_cluster_id,
                fun(_ClusterId) ->
                        {ok, [#?MEMBER{node  = ?NODEDOWN_NODE,
                                       state = ?STATE_STOP}]}
                end),

    meck:new(leo_cluster_tbl_ring, [non_strict]),
    meck:expect(leo_cluster_tbl_ring, create_table_current,
                fun(_,_) ->
                        ok
                end),
    meck:expect(leo_cluster_tbl_ring, create_table_prev,
                fun(_,_) ->
                        ok
                end),

    meck:new(leo_manager_api, [non_strict]),
    meck:expect(leo_manager_api, notify, fun(_Type, _Node1, _Notify2, _Error) ->
                                                 ok
                                         end),
    ok.

-endif.
