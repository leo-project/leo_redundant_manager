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
-module(leo_cluster_tbl_member_tests).

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

-define(ETS, 'ets').
-define(MNESIA, 'mnesia').

-define(CLUSTER_ID_1, 'leofs_cluster_1').
-define(CLUSTER_ID_2, 'leofs_cluster_2').

-define(NODE_0, 'node_0@127.0.0.1').
-define(NODE_1, 'node_1@127.0.0.1').
-define(NODE_2, 'node_2@127.0.0.1').
-define(NODE_3, 'node_3@127.0.0.1').
-define(NODE_4, 'node_4@127.0.0.1').

-define(RACK_1, "rack_1").
-define(RACK_2, "rack_2").

-define(MEMBER_0, #?MEMBER{id = {?CLUSTER_ID_1, ?NODE_0},
                           cluster_id = ?CLUSTER_ID_1,
                           node = ?NODE_0,
                           alias="node_0_a",
                           state = ?STATE_RUNNING,
                           grp_level_2 = ?RACK_1}).
-define(MEMBER_1, #?MEMBER{id = {?CLUSTER_ID_1, ?NODE_1},
                           node = ?NODE_1,
                           cluster_id = ?CLUSTER_ID_1,
                           alias="node_1_b",
                           state = ?STATE_SUSPEND,
                           grp_level_2 = ?RACK_1}).
-define(MEMBER_2, #?MEMBER{id = {?CLUSTER_ID_1, ?NODE_2},
                           node = ?NODE_2,
                           cluster_id = ?CLUSTER_ID_1,
                           alias="node_2_c",
                           state = ?STATE_RUNNING,
                           grp_level_2 = ?RACK_1}).
-define(MEMBER_3, #?MEMBER{id = {?CLUSTER_ID_1, ?NODE_3},
                           node = ?NODE_3,
                           cluster_id = ?CLUSTER_ID_1,
                           alias="node_3_d",
                           state = ?STATE_RUNNING,
                           grp_level_2 = ?RACK_2}).
-define(MEMBER_4, #?MEMBER{id = {?CLUSTER_ID_1, ?NODE_4},
                           node = ?NODE_4,
                           cluster_id = ?CLUSTER_ID_1,
                           alias="node_4_e",
                           state = ?STATE_STOP,
                           grp_level_2 = ?RACK_2}).

member_tbl_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [{with, [T]} || T <- [
                           fun suite_mnesia_/1,
                           fun suite_ets_/1
                          ]]}.

setup() ->
    ok.
teardown(_) ->
    ok.


suite_mnesia_(_) ->
    application:start(mnesia),
    ok = leo_cluster_tbl_member:create_table('ram_copies', [node()], ?MEMBER_TBL_CUR),
    ok = leo_cluster_tbl_member:create_table('ram_copies', [node()], ?MEMBER_TBL_PREV),
    ?debugVal(mnesia:table_info(?MEMBER_TBL_CUR,  all)),
    ?debugVal(mnesia:table_info(?MEMBER_TBL_PREV, all)),

    ok = inspect(?MNESIA),
    application:stop(mnesia),
    ok.

suite_ets_(_) ->
    ok = leo_cluster_tbl_member:create_table(?MEMBER_TBL_CUR),
    ok = leo_cluster_tbl_member:create_table(?MEMBER_TBL_PREV),
    ok = inspect(?ETS),
    ok.

inspect(TableType) ->
    ?debugVal(TableType),
    not_found = leo_cluster_tbl_member:find_by_cluster_id(
                  TableType, ?MEMBER_TBL_CUR, ?CLUSTER_ID_1),
    not_found = leo_cluster_tbl_member:lookup(
                  TableType, ?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?NODE_0),

    %% Insert records
    ok = leo_cluster_tbl_member:insert(TableType, ?MEMBER_TBL_CUR, ?MEMBER_0),
    ok = leo_cluster_tbl_member:insert(TableType, ?MEMBER_TBL_CUR, ?MEMBER_1),
    ok = leo_cluster_tbl_member:insert(TableType, ?MEMBER_TBL_CUR, ?MEMBER_2),
    ok = leo_cluster_tbl_member:insert(TableType, ?MEMBER_TBL_CUR, ?MEMBER_3),
    ok = leo_cluster_tbl_member:insert(TableType, ?MEMBER_TBL_CUR, ?MEMBER_4),

    %% Lookup a record
    {ok, Ret_1} = leo_cluster_tbl_member:lookup(
                    TableType, ?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?NODE_0),
    ?assertEqual(?MEMBER_0, Ret_1),
    {ok, Ret_2} = leo_cluster_tbl_member:lookup(
                    TableType, ?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?NODE_1),
    ?assertEqual(?MEMBER_1, Ret_2),
    {ok, Ret_3} = leo_cluster_tbl_member:lookup(
                    TableType, ?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?NODE_2),
    ?assertEqual(?MEMBER_2, Ret_3),
    {ok, Ret_4} = leo_cluster_tbl_member:lookup(
                    TableType, ?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?NODE_3),
    ?assertEqual(?MEMBER_3, Ret_4),
    {ok, Ret_5} = leo_cluster_tbl_member:lookup(
                    TableType, ?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?NODE_4),
    ?assertEqual(?MEMBER_4, Ret_5),

    {ok, RetL_1} = leo_cluster_tbl_member:find_by_cluster_id(
                     TableType, ?MEMBER_TBL_CUR, ?CLUSTER_ID_1),
    ?assertEqual(5, erlang:length(RetL_1)),

    {ok, RetL_2} = leo_cluster_tbl_member:find_by_status(
                     TableType, ?MEMBER_TBL_CUR,
                     ?CLUSTER_ID_1, ?STATE_RUNNING),
    ?assertEqual(3, erlang:length(RetL_2)),

    {ok, RetL_3} = leo_cluster_tbl_member:find_by_status(
                     TableType, ?MEMBER_TBL_CUR,
                     ?CLUSTER_ID_1, ?STATE_SUSPEND),
    ?assertEqual(1, erlang:length(RetL_3)),

    {ok, RetL_4} = leo_cluster_tbl_member:find_by_status(
                     TableType, ?MEMBER_TBL_CUR,
                     ?CLUSTER_ID_1, ?STATE_STOP),
    ?assertEqual(1, erlang:length(RetL_4)),


    {ok, Rack_1} = leo_cluster_tbl_member:find_by_level2(
                     TableType, ?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?RACK_1),
    {ok, Rack_2} = leo_cluster_tbl_member:find_by_level2(
                     TableType, ?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?RACK_2),
    ?assertEqual(3, length(Rack_1)),
    ?assertEqual(2, length(Rack_2)),

    Size_1 = leo_cluster_tbl_member:table_size(
               TableType, ?MEMBER_TBL_CUR, ?CLUSTER_ID_1),
    ?assertEqual(5, Size_1),

    ok = leo_cluster_tbl_member:delete(
           TableType, ?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?NODE_0),
    not_found = leo_cluster_tbl_member:lookup(
                  TableType, ?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?NODE_0),

    Size_2 = leo_cluster_tbl_member:table_size(
               TableType, ?MEMBER_TBL_CUR, ?CLUSTER_ID_1),
    ?assertEqual(4, Size_2),
    ok.
-endif.
