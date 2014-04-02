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
-module(leo_mdcr_tbl_cluster_mgr_tests).
-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

-define(MGR_1, #cluster_manager{node = 'manager_0@10.0.0.1',
                                cluster_id = "cluster_11"
                               }).
-define(MGR_2, #cluster_manager{node = 'manager_0@10.0.0.2',
                                cluster_id = "cluster_12"
                               }).
-define(MGR_3, #cluster_manager{node = 'manager_0@10.0.0.3',
                                cluster_id = "cluster_15"
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
    ok = leo_mdcr_tbl_cluster_mgr:create_table(ram_copies, [node()]),

    Res1 = leo_mdcr_tbl_cluster_mgr:all(),
    ?assertEqual(not_found, Res1),

    Res2 = leo_mdcr_tbl_cluster_mgr:update(?MGR_1),
    Res3 = leo_mdcr_tbl_cluster_mgr:update(?MGR_2),
    Res4 = leo_mdcr_tbl_cluster_mgr:update(?MGR_3),
    ?assertEqual(ok, Res2),
    ?assertEqual(ok, Res3),
    ?assertEqual(ok, Res4),

    ?assertEqual(3, leo_mdcr_tbl_cluster_mgr:size()),

    Res5 = leo_mdcr_tbl_cluster_mgr:get("cluster_12"),
    ?assertEqual({ok, [?MGR_2]}, Res5),

    {ok, Res6} = leo_mdcr_tbl_cluster_mgr:all(),
    ?assertEqual(3, length(Res6)),

    Res7 = leo_mdcr_tbl_cluster_mgr:delete("cluster_12"),
    ?assertEqual(ok, Res7),

    Res8 = leo_mdcr_tbl_cluster_mgr:get("cluster_12"),
    ?assertEqual(not_found, Res8),

    {ok, Res9} = leo_mdcr_tbl_cluster_mgr:all(),
    ?assertEqual(2, length(Res9)),

    application:stop(mnesia),
    ok.

-endif.
