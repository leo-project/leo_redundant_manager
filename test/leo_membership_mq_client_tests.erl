%%======================================================================
%%
%% Leo Redundant Manager
%%
%% Copyright (c) 2012
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
%% ---------------------------------------------------------------------
%% Leo Redundant Manager - EUnit
%% @doc
%% @end
%%======================================================================
-module(leo_membership_mq_client_tests).
-author('yosuke hara').
-vsn('0.9.1').

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
    {ok, Node0} = slave:start_link(list_to_atom(Hostname), 'node_0'),
    {ok, Mgr0}  = slave:start_link(list_to_atom(Hostname), 'manager_master'),

    true = rpc:call(Node0, code, add_path, ["../deps/meck/ebin"]),
    true = rpc:call(Mgr0,  code, add_path, ["../deps/meck/ebin"]),

    S = os:cmd("pwd"),
    Path = string:substr(S, 1, length(S) -1) ++ "/db",

    application:start(mnesia),
    {Mgr0, Node0, Path}.

teardown({Mgr0, Node0, Path}) ->
    meck:unload(),

    application:stop(mnesia),
    application:stop(leo_mq),
    application:stop(leo_backend_db),
    application:stop(leo_redundant_manager),

    net_kernel:stop(),
    slave:stop(Mgr0),
    slave:stop(Node0),

    os:cmd("rm -rf " ++ Path),
    ok.

%% @doc publish
%%
pubsub_manager_0_({Mgr0, _Node0, Path}) ->
    prepare(),
    ok = leo_redundant_manager_api:start(manager, [Mgr0], Path),
    ok = leo_membership_mq_client:publish(manager, ?NODEDOWN_NODE, ?ERR_TYPE_NODE_DOWN),
    timer:sleep(2000),

    History0 = meck:history(leo_redundant_manager_mnesia),
    ?assertEqual(true, erlang:length(History0) > 0),

    History1 = meck:history(leo_manager_api),
    ?assertEqual(true, erlang:length(History1) > 0),
    ok.

pubsub_manager_1_({Mgr0, _, Path}) ->
    prepare(),
    ok = leo_redundant_manager_api:start(manager, [Mgr0], Path),
    ok = leo_membership_mq_client:publish(manager, ?NODEDOWN_NODE, ?ERR_TYPE_NODE_DOWN),
    timer:sleep(250),

    History0 = meck:history(leo_redundant_manager_mnesia),
    ?assertEqual(true, erlang:length(History0) > 0),

    History1 = meck:history(leo_manager_api),
    ?assertEqual(true, erlang:length(History1) > 0),
    ok.

pubsub_storage_({Mgr0, _, Path}) ->
    prepare(),

    ok = rpc:call(Mgr0, meck, new,    [leo_manager_api, [no_link]]),
    ok = rpc:call(Mgr0, meck, expect, [leo_manager_api, notify,
                                       fun(_Type, _Node, _Error) ->
                                               ok
                                       end]),

    ok = leo_redundant_manager_api:start(storage, [Mgr0], Path),
    ok = leo_membership_mq_client:publish(storage, ?NODEDOWN_NODE, ?ERR_TYPE_NODE_DOWN),
    timer:sleep(500),

    History0 = meck:history(leo_redundant_manager_mnesia),
    ?assertEqual(true, erlang:length(History0) > 0),

    History1 = meck:history(leo_manager_api),
    ?assertEqual([], History1),

    History2 = rpc:call(Mgr0, meck, history, [leo_manager_api]),
    ?assertEqual(true, erlang:length(History2) > 0),
    ok.

pubsub_gateway_0_({Mgr0, _, Path}) ->
    prepare(),

    ok = rpc:call(Mgr0, meck, new,    [leo_manager_api, [no_link]]),
    ok = rpc:call(Mgr0, meck, expect, [leo_manager_api, notify,
                                       fun(_Type, _Node, _Error) ->
                                               ok
                                       end]),

    ok = leo_redundant_manager_api:start(gateway, [Mgr0], Path),
    ok = leo_membership_mq_client:publish(gateway, ?NODEDOWN_NODE, ?ERR_TYPE_NODE_DOWN),
    timer:sleep(500),

    History0 = meck:history(leo_redundant_manager_mnesia),
    ?assertEqual(true, erlang:length(History0) > 0),

    History1 = meck:history(leo_manager_api),
    ?assertEqual([], History1),

    History2 = rpc:call(Mgr0, meck, history, [leo_manager_api]),
    ?assertEqual(true, erlang:length(History2) > 0),
    ok.

pubsub_gateway_1_({Mgr0, _, Path}) ->
    prepare(),

    ok = rpc:call(Mgr0, meck, new,    [leo_manager_api, [no_link]]),
    ok = rpc:call(Mgr0, meck, expect, [leo_manager_api, notify,
                                       fun(_Type, _Node, _Error) ->
                                               ok
                                       end]),

    ok = leo_redundant_manager_api:start(gateway, [Mgr0], Path),
    ok = leo_membership_mq_client:publish(gateway, ?NODEDOWN_NODE, ?ERR_TYPE_NODE_DOWN),
    timer:sleep(500),

    History0 = meck:history(leo_redundant_manager_mnesia),
    ?assertEqual(true, erlang:length(History0) > 0),

    History1 = meck:history(leo_manager_api),
    ?assertEqual([], History1),

    History2 = rpc:call(Mgr0, meck, history, [leo_manager_api]),
    ?assertEqual(true, erlang:length(History2) > 0),
    ok.


prepare() ->
    meck:new(leo_redundant_manager_mnesia),
    meck:expect(leo_redundant_manager_mnesia, get_member_by_node,
                fun(Node) ->
                        {ok, [#member{node  = Node,
                                      state = ?STATE_DOWNED}]}
                end),
    meck:expect(leo_redundant_manager_mnesia, get_members,
                fun() ->
                        {ok, [#member{node  = ?NODEDOWN_NODE,
                                      state = ?STATE_DOWNED}]}
                end),

    meck:new(leo_redundant_manager_table_ring),
    meck:expect(leo_redundant_manager_table_ring, create_ring_current,
                fun(_,_) ->
                        ok
                end),
    meck:expect(leo_redundant_manager_table_ring, create_ring_prev,
                fun(_,_) ->
                        ok
                end),

    meck:new(leo_manager_api),
    meck:expect(leo_manager_api, notify, fun(_Type, _Node, _Error) ->
                                                 ok
                                         end),
    ok.

-endif.

