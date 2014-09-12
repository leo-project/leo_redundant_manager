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
-module(leo_mdcr_tbl_cluster_stat_tests).
-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

-define(STAT_1,#?CLUSTER_STAT{cluster_id = "cluster_11",
                              state = ?STATE_RUNNING,
                              checksum = 12345,
                              updated_at = 139018453328000
                             }).
-define(STAT_2,#?CLUSTER_STAT{cluster_id = "cluster_12",
                              state = ?STATE_SUSPEND,
                              checksum = 23456,
                              updated_at = 139018453328000
                             }).
-define(STAT_3,#?CLUSTER_STAT{cluster_id = "cluster_15",
                              state = ?STATE_STOP,
                              checksum = 34567,
                              updated_at = 139018453328000
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
    ok = leo_mdcr_tbl_cluster_stat:create_table(ram_copies, [node()]),

    Res1 = leo_mdcr_tbl_cluster_stat:all(),
    ?assertEqual(not_found, Res1),

    Res2 = leo_mdcr_tbl_cluster_stat:update(?STAT_1),
    Res3 = leo_mdcr_tbl_cluster_stat:update(?STAT_2),
    Res4 = leo_mdcr_tbl_cluster_stat:update(?STAT_3),
    ?assertEqual(ok, Res2),
    ?assertEqual(ok, Res3),
    ?assertEqual(ok, Res4),

    ?assertEqual(3, leo_mdcr_tbl_cluster_stat:size()),

    Res5 = leo_mdcr_tbl_cluster_stat:get("cluster_12"),
    ?assertEqual({ok, ?STAT_2}, Res5),

    {ok, [?STAT_1]} = leo_mdcr_tbl_cluster_stat:find_by_state(?STATE_RUNNING),

    {ok, Res6} = leo_mdcr_tbl_cluster_stat:all(),
    ?assertEqual(3, length(Res6)),

    Res7 = leo_mdcr_tbl_cluster_stat:delete("cluster_12"),
    ?assertEqual(ok, Res7),

    Res8 = leo_mdcr_tbl_cluster_stat:get("cluster_12"),
    ?assertEqual(not_found, Res8),

    {ok, Res9} = leo_mdcr_tbl_cluster_stat:all(),
    ?assertEqual(2, length(Res9)),

    {ok, Res10} = leo_mdcr_tbl_cluster_stat:find_by_cluster_id("cluster_11"),
    ?debugVal(Res10),
    ?assertEqual(?STAT_1, Res10),

    {ok, Res11} = leo_mdcr_tbl_cluster_stat:checksum("cluster_11"),
    ?assertEqual(true, is_integer(Res11)),

    ok = leo_mdcr_tbl_cluster_stat:transform(),
    {ok, RetL} = leo_mdcr_tbl_cluster_stat:all(),
    lists:foreach(fun(#?CLUSTER_STAT{cluster_id = ClusterId}) ->
                          true = is_atom(ClusterId)
                  end, RetL),
    application:stop(mnesia),
    ok.

-endif.
