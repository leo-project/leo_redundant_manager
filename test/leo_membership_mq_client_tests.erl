%%======================================================================
%%
%% Leo Redundant Manager
%%
%% Copyright (c) 2012-2014 Rakuten, Inc.
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
-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").


%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

-define(NODEDOWN_NODE, 'nodedown_node').

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

    catch leo_redundant_manager_sup:stop(),
    application:start(mnesia),
    {Mgr0, Node0, Path}.

teardown({Mgr0, Node0, Path}) ->
    meck:unload(),

    application:stop(mnesia),
    application:stop(leo_mq),
    application:stop(leo_backend_db),

    catch leo_redundant_manager_sup:stop(),
    application:stop(leo_redundant_manager),

    net_kernel:stop(),
    slave:stop(Mgr0),
    slave:stop(Node0),

    os:cmd("rm -rf " ++ Path),

    timer:sleep(timer:seconds(1)),
    ok.

%% @doc publish
%%
pubsub_manager_0_({Mgr0, _Node0, Path}) ->
    prepare(),
    leo_redundant_manager_sup:start_link(
      ?MONITOR_NODE, [Mgr0], Path),

    ok = leo_membership_mq_client:publish(
           ?MONITOR_NODE, ?NODEDOWN_NODE, ?ERR_TYPE_NODE_DOWN),
    timer:sleep(1500),

    History0 = meck:history(leo_cluster_tbl_member),
    ?assertEqual(true, erlang:length(History0) > 0),
    ok.

pubsub_manager_1_({Mgr0, _, Path}) ->
    prepare(),
    leo_redundant_manager_sup:start_link(
      ?MONITOR_NODE, [Mgr0], Path),

    ok = leo_membership_mq_client:publish(
           ?MONITOR_NODE, ?NODEDOWN_NODE, ?ERR_TYPE_NODE_DOWN),
    timer:sleep(1500),

    History0 = meck:history(leo_cluster_tbl_member),
    ?assertEqual(true, erlang:length(History0) > 0),
    ok.

pubsub_storage_({Mgr0, _, Path}) ->
    prepare(),

    ok = rpc:call(Mgr0, meck, new,    [leo_manager_api, [no_link, non_strict]]),
    ok = rpc:call(Mgr0, meck, expect, [leo_manager_api, notify,
                                       fun(_Type, _Node1, _Node2, _Error) ->
                                               ok
                                       end]),

    leo_redundant_manager_sup:start_link(
      ?PERSISTENT_NODE, [Mgr0], Path),
    ok = leo_membership_mq_client:publish(
           ?PERSISTENT_NODE, ?NODEDOWN_NODE, ?ERR_TYPE_NODE_DOWN),
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

    leo_redundant_manager_sup:start_link(
      ?WORKER_NODE, [Mgr0], Path),
    ok = leo_membership_mq_client:publish(
           ?WORKER_NODE, ?NODEDOWN_NODE, ?ERR_TYPE_NODE_DOWN),
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

    leo_redundant_manager_sup:start_link(
      ?WORKER_NODE, [Mgr0], Path),
    ok = leo_membership_mq_client:publish(
           ?WORKER_NODE, ?NODEDOWN_NODE, ?ERR_TYPE_NODE_DOWN),
    timer:sleep(1500),

    %% History0 = meck:history(leo_cluster_tbl_member),
    %% ?assertEqual(true, erlang:length(History0) > 0),

    History1 = meck:history(leo_manager_api),
    ?assertEqual([], History1),
    ok.


prepare() ->
    meck:new(leo_cluster_tbl_member, [non_strict]),
    meck:expect(leo_cluster_tbl_member, lookup,
                fun(Node) ->
                        {ok, #member{node  = Node,
                                     state = ?STATE_STOP}}
                end),
    meck:expect(leo_cluster_tbl_member, find_all,
                fun() ->
                        {ok, [#member{node  = ?NODEDOWN_NODE,
                                      state = ?STATE_STOP}]}
                end),
    meck:expect(leo_cluster_tbl_member, create_members,
                fun(_) ->
                        ok
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
