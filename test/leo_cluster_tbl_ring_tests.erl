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
-module(leo_cluster_tbl_ring_tests).

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).
cluster_tbl_ring_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [{with, [T]} || T <- [
                           fun suite_mnesia_/1,
                           fun suite_ets_/1
                          ]]}.

setup() ->
    catch ets:delete_all_objects(?MEMBER_TBL_CUR),
    catch ets:delete_all_objects(?MEMBER_TBL_PREV),
    ok.

teardown(_) ->
    ok.

-define(ClusterId_1, 'leofs_c1').
-define(ClusterId_2, 'leofs_c2').
-define(VNodeId_1, 987654320).
-define(VNodeId_2, 987654321).
-define(VNodeId_3, 987654322).
-define(VNodeId_4, 987654323).
-define(VNodeId_5, 987654324).
-define(Node_1, 'test_0@127.0.0.1').
-define(Node_2, 'test_1@127.0.0.1').
-define(Node_3, 'test_2@127.0.0.1').
-define(Node_4, 'test_3@127.0.0.1').
-define(Node_5, 'test_4@127.0.0.1').

-define(TblInfo_1, {?DB_MNESIA, leo_ring_cur}).
-define(TblInfo_2, {?DB_MNESIA, leo_ring_prev}).
-define(TblInfo_3, {?DB_ETS, leo_ring_cur}).
-define(TblInfo_4, {?DB_ETS, leo_ring_prev}).
-define(Clock, 1456189892357920).
-define(Ring_1, #ring_2{id = {?ClusterId_1, ?VNodeId_1},
                        cluster_id = ?ClusterId_1,
                        vnode_id = ?VNodeId_1,
                        node = ?Node_1,
                        clock = ?Clock}).
-define(Ring_2, #ring_2{id = {?ClusterId_1, ?VNodeId_2},
                        cluster_id = ?ClusterId_1,
                        vnode_id = ?VNodeId_2,
                        node = ?Node_2,
                        clock = ?Clock}).
-define(Ring_3, #ring_2{id = {?ClusterId_1, ?VNodeId_3},
                        cluster_id = ?ClusterId_1,
                        vnode_id = ?VNodeId_3,
                        node = ?Node_3,
                        clock = ?Clock}).
-define(Ring_4, #ring_2{id = {?ClusterId_2, ?VNodeId_4},
                        cluster_id = ?ClusterId_2,
                        vnode_id = ?VNodeId_4,
                        node = ?Node_4,
                        clock = ?Clock}).
-define(Ring_5, #ring_2{id = {?ClusterId_2, ?VNodeId_5},
                        cluster_id = ?ClusterId_2,
                        vnode_id = ?VNodeId_5,
                        node = ?Node_5,
                        clock = ?Clock}).

suite_mnesia_(_) ->
    %% Prepare
    application:start(mnesia),
    ok = leo_cluster_tbl_ring:create_table_current(ram_copies),
    ok = leo_cluster_tbl_ring:create_table_prev(ram_copies),
    %% Doing tests
    ok = commons(?TblInfo_1),

    application:stop(mnesia),
    ok.

suite_ets_(_) ->
    %% Prepare
    catch ets:new(?RING_TBL_CUR, [named_table, ordered_set,
                                  public, {read_concurrency, true}]),
    catch ets:new(?RING_TBL_PREV,[named_table, ordered_set,
                                  public, {read_concurrency, true}]),
    %% Doing tests
    ok = commons(?TblInfo_3),
    ok.

commons(TblInfo) ->
    ?debugVal(TblInfo),

    %% Put records
    ok = leo_cluster_tbl_ring:insert(TblInfo, ?Ring_1),
    ok = leo_cluster_tbl_ring:insert(TblInfo, ?Ring_2),
    ok = leo_cluster_tbl_ring:insert(TblInfo, ?Ring_3),
    ok = leo_cluster_tbl_ring:insert(TblInfo, ?Ring_4),
    ok = leo_cluster_tbl_ring:insert(TblInfo, ?Ring_5),

    %% Retrieve all of records
    {ok, RetL_1} = leo_cluster_tbl_ring:find_all(TblInfo),
    ?assertEqual(5, erlang:length(RetL_1)),

    %% Lookup a record by cluster-id and vnode-id
    Ret_1 = leo_cluster_tbl_ring:lookup(TblInfo, ?ClusterId_1, ?VNodeId_1),
    ?assertEqual(?Ring_1, Ret_1),
    Ret_2 = leo_cluster_tbl_ring:lookup(TblInfo, ?ClusterId_1, ?VNodeId_2),
    ?assertEqual(?Ring_2, Ret_2),
    Ret_3 = leo_cluster_tbl_ring:lookup(TblInfo, ?ClusterId_1, ?VNodeId_3),
    ?assertEqual(?Ring_3, Ret_3),
    Ret_4 = leo_cluster_tbl_ring:lookup(TblInfo, ?ClusterId_2, ?VNodeId_4),
    ?assertEqual(?Ring_4, Ret_4),
    Ret_5 = leo_cluster_tbl_ring:lookup(TblInfo, ?ClusterId_2, ?VNodeId_5),
    ?assertEqual(?Ring_5, Ret_5),

    not_found = leo_cluster_tbl_ring:lookup(TblInfo, ?ClusterId_2, ?VNodeId_2),
    not_found = leo_cluster_tbl_ring:lookup(TblInfo, ?ClusterId_1, ?VNodeId_5),

    %% Retrieve records by cluster-id
    {ok, RetL_11} = leo_cluster_tbl_ring:find_by_cluster_id(TblInfo, ?ClusterId_1),
    ?assertEqual(3, erlang:length(RetL_11)),
    {ok, RetL_12} = leo_cluster_tbl_ring:find_by_cluster_id(TblInfo, ?ClusterId_2),
    ?assertEqual(2, erlang:length(RetL_12)),

    %% Retrieve a first record
    ?VNodeId_1 = leo_cluster_tbl_ring:first(TblInfo, ?ClusterId_1),
    ?VNodeId_4 = leo_cluster_tbl_ring:first(TblInfo, ?ClusterId_2),

    %% Retrieve a next record
    ?VNodeId_2 = leo_cluster_tbl_ring:next(TblInfo, ?ClusterId_1, ?VNodeId_1),
    ?VNodeId_3 = leo_cluster_tbl_ring:next(TblInfo, ?ClusterId_1, ?VNodeId_2),
    '$end_of_table' = leo_cluster_tbl_ring:next(TblInfo, ?ClusterId_1, ?VNodeId_3),
    ?VNodeId_4 = leo_cluster_tbl_ring:next(TblInfo, ?ClusterId_2, ?VNodeId_3),
    ?VNodeId_5 = leo_cluster_tbl_ring:next(TblInfo, ?ClusterId_2, ?VNodeId_4),
    '$end_of_table' = leo_cluster_tbl_ring:next(TblInfo, ?ClusterId_2, ?VNodeId_5),

    %% Retrieve a last record
    ?VNodeId_3 = leo_cluster_tbl_ring:last(TblInfo, ?ClusterId_1),
    ?VNodeId_5 = leo_cluster_tbl_ring:last(TblInfo, ?ClusterId_2),

    %% Retrieve a previous record
    '$end_of_table' = leo_cluster_tbl_ring:prev(TblInfo, ?ClusterId_1, ?VNodeId_1),
    ?VNodeId_1 = leo_cluster_tbl_ring:prev(TblInfo, ?ClusterId_1, ?VNodeId_2),
    ?VNodeId_2 = leo_cluster_tbl_ring:prev(TblInfo, ?ClusterId_1, ?VNodeId_3),
    ?VNodeId_3 = leo_cluster_tbl_ring:prev(TblInfo, ?ClusterId_1, ?VNodeId_4),
    '$end_of_table' = leo_cluster_tbl_ring:prev(TblInfo, ?ClusterId_2, ?VNodeId_4),
    ?VNodeId_4 = leo_cluster_tbl_ring:prev(TblInfo, ?ClusterId_2, ?VNodeId_5),

    %% Size#1
    3 = leo_cluster_tbl_ring:size(TblInfo, ?ClusterId_1),
    2 = leo_cluster_tbl_ring:size(TblInfo, ?ClusterId_2),

    %% Delete a record
    ok = leo_cluster_tbl_ring:delete(TblInfo, ?ClusterId_1, ?VNodeId_1),
    not_found = leo_cluster_tbl_ring:lookup(TblInfo, ?ClusterId_1, ?VNodeId_1),

    %% Size#2
    2 = leo_cluster_tbl_ring:size(TblInfo, ?ClusterId_1),
    2 = leo_cluster_tbl_ring:size(TblInfo, ?ClusterId_2),

    %% Delete all of records
    ok = leo_cluster_tbl_ring:delete_all(TblInfo, ?ClusterId_1),
    ok = leo_cluster_tbl_ring:delete_all(TblInfo, ?ClusterId_2),
    not_found = leo_cluster_tbl_ring:lookup(TblInfo, ?ClusterId_1, ?VNodeId_2),
    not_found = leo_cluster_tbl_ring:lookup(TblInfo, ?ClusterId_2, ?VNodeId_4),

    %% Size#3
    0 = leo_cluster_tbl_ring:size(TblInfo, ?ClusterId_1),
    0 = leo_cluster_tbl_ring:size(TblInfo, ?ClusterId_2),
    ok.

-endif.
