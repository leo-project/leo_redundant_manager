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
-module(leo_redundant_manager_table_cluster_tests).
-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

-define(CONF_1, #?SYSTEM_CONF{cluster_id = "cluster_11",
                              dc_id = "tokyo_1",
                              n = 3,
                              w = 2,
                              r = 1,
                              d = 1,
                              num_of_dc_replicas = 1,
                              num_of_rack_replicas = 1
                             }).
-define(CONF_2, #?SYSTEM_CONF{cluster_id = "cluster_12",
                              dc_id = "singapore_1",
                              n = 2,
                              w = 1,
                              r = 1,
                              d = 1,
                              num_of_dc_replicas = 1,
                              num_of_rack_replicas = 1
                             }).
-define(CONF_3, #?SYSTEM_CONF{cluster_id = "cluster_15",
                              dc_id = "europe_1",
                              n = 5,
                              w = 3,
                              r = 2,
                              d = 2,
                              num_of_dc_replicas = 1,
                              num_of_rack_replicas = 1
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
    {atomic,ok} = leo_redundant_manager_table_cluster:create_table(ram_copies, [node()]),

    Res1 = leo_redundant_manager_table_cluster:all(),
    ?assertEqual(not_found, Res1),

    Res2 = leo_redundant_manager_table_cluster:update(?CONF_1),
    Res3 = leo_redundant_manager_table_cluster:update(?CONF_2),
    Res4 = leo_redundant_manager_table_cluster:update(?CONF_3),
    ?assertEqual(ok, Res2),
    ?assertEqual(ok, Res3),
    ?assertEqual(ok, Res4),

    Res5 = leo_redundant_manager_table_cluster:get("cluster_12"),
    ?assertEqual({ok, ?CONF_2}, Res5),

    {ok, Res6} = leo_redundant_manager_table_cluster:all(),
    ?assertEqual(3, length(Res6)),

    Res7 = leo_redundant_manager_table_cluster:delete("cluster_12"),
    ?assertEqual(ok, Res7),

    Res8 = leo_redundant_manager_table_cluster:get("cluster_12"),
    ?assertEqual(not_found, Res8),

    {ok, Res9} = leo_redundant_manager_table_cluster:all(),
    ?assertEqual(2, length(Res9)),

    application:stop(mnesia),
    ok.

-endif.
