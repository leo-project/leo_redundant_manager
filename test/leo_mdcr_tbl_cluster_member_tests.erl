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
-module(leo_mdcr_tbl_cluster_member_tests).
-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

-define(MEMBER1, #?CLUSTER_MEMBER{node = 'node_0@10.0.0.1',
                                  cluster_id = "cluster_11",
                                  alias = 'node_13075',
                                  ip = "10.0.0.1",
                                  state = 'running'
                                 }).
-define(MEMBER2, #?CLUSTER_MEMBER{node = 'node_1@10.0.0.2',
                                  cluster_id = "cluster_11",
                                  alias = 'node_13076',
                                  ip = "10.0.0.2",
                                  state = 'running'
                                 }).
-define(MEMBER3, #?CLUSTER_MEMBER{node = 'node_2@10.0.0.3',
                                  cluster_id = "cluster_11",
                                  alias = 'node_13077',
                                  ip = "10.0.0.3",
                                  state = 'suspend'
                                 }).
-define(MEMBER4, #?CLUSTER_MEMBER{node = 'node_3@10.0.0.2',
                                  cluster_id = "cluster_11",
                                  alias = 'node_13076',
                                  ip = "10.0.0.2",
                                  state = 'running'
                                 }).

-define(MEMBER5, #?CLUSTER_MEMBER{node = 'node_4@10.0.0.4',
                                  cluster_id = "cluster_12",
                                  alias = 'node_13077',
                                  ip = "10.0.0.4",
                                  state = 'running'
                                 }).
-define(MEMBER6, #?CLUSTER_MEMBER{node = 'node_5@10.0.0.5',
                                  cluster_id = "cluster_12",
                                  alias = 'node_13077',
                                  ip = "10.0.0.5",
                                  state = 'restarted'
                                 }).


table_cluster_test_() ->
    {timeout, 300,
     {foreach, fun setup/0, fun teardown/1,
      [{with, [T]} || T <- [fun suite_/1]]}}.


setup() ->
    ok.

teardown(_) ->
    ok.

suite_(_) ->
    application:start(mnesia),
    ok = leo_mdcr_tbl_cluster_member:create_table(ram_copies, [node()]),

    Res1 = leo_mdcr_tbl_cluster_member:all(),
    ?assertEqual(not_found, Res1),

    Res2 = leo_mdcr_tbl_cluster_member:update(?MEMBER1),
    Res3 = leo_mdcr_tbl_cluster_member:update(?MEMBER2),
    Res4 = leo_mdcr_tbl_cluster_member:update(?MEMBER3),
    Res5 = leo_mdcr_tbl_cluster_member:update(?MEMBER4),
    Res6 = leo_mdcr_tbl_cluster_member:update(?MEMBER5),
    Res7 = leo_mdcr_tbl_cluster_member:update(?MEMBER6),

    ?assertEqual(ok, Res2),
    ?assertEqual(ok, Res3),
    ?assertEqual(ok, Res4),
    ?assertEqual(ok, Res5),
    ?assertEqual(ok, Res6),
    ?assertEqual(ok, Res7),

    Res8 = leo_mdcr_tbl_cluster_member:get("cluster_12"),
    ?assertEqual({ok, [?MEMBER5,?MEMBER6]}, Res8),

    ?assertEqual(6, leo_mdcr_tbl_cluster_member:size()),
    {ok, Res9} = leo_mdcr_tbl_cluster_member:all(),
    ?assertEqual(6, length(Res9)),

    Res10 = leo_mdcr_tbl_cluster_member:delete("cluster_12"),
    ?assertEqual(ok, Res10),

    Res11 = leo_mdcr_tbl_cluster_member:get("cluster_12"),
    ?assertEqual(not_found, Res11),

    {ok, Res12} = leo_mdcr_tbl_cluster_member:all(),
    ?assertEqual(4, length(Res12)),

    {ok, Res13} = leo_mdcr_tbl_cluster_member:find_by_limit("cluster_11", 2),
    ?assertEqual(2, length(Res13)),

    {ok, Res14} = leo_mdcr_tbl_cluster_member:find_by_limit_with_rnd("cluster_11", 2),
    ?assertEqual(2, length(Res14)),
    {ok, Res15} = leo_mdcr_tbl_cluster_member:find_by_limit_with_rnd("cluster_11", 4),
    ?assertEqual(3, length(Res15)),

    {ok, Res16} = leo_mdcr_tbl_cluster_member:find_by_cluster_id("cluster_11"),
    ?assertEqual(4, length(Res16)),

    {ok, Res17} = leo_mdcr_tbl_cluster_member:checksum("cluster_11"),
    ?assertEqual(true, is_integer(Res17)),

    application:stop(mnesia),
    ok.

-endif.
