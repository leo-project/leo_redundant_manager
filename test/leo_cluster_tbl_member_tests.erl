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
%% ---------------------------------------------------------------------
%% Leo Redundant Manager - EUnit
%% @doc
%% @end
%%======================================================================
-module(leo_cluster_tbl_member_tests).
-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

-define(ETS,    'ets').
-define(MNESIA, 'mnesia').

-define(NODE_0, "node_0").
-define(NODE_1, "node_1").
-define(NODE_2, "node_2").
-define(NODE_3, "node_3").
-define(NODE_4, "node_4").

-define(RACK_1, "rack_1").
-define(RACK_2, "rack_2").

-define(MEMBER_0, #member{node = ?NODE_0, alias="node_0_a", state = ?STATE_RUNNING, grp_level_2 = ?RACK_1}).
-define(MEMBER_1, #member{node = ?NODE_1, alias="node_1_b", state = ?STATE_SUSPEND, grp_level_2 = ?RACK_1}).
-define(MEMBER_2, #member{node = ?NODE_2, alias="node_2_c", state = ?STATE_RUNNING, grp_level_2 = ?RACK_1}).
-define(MEMBER_3, #member{node = ?NODE_3, alias="node_3_d", state = ?STATE_RUNNING, grp_level_2 = ?RACK_2}).
-define(MEMBER_4, #member{node = ?NODE_4, alias="node_4_e", state = ?STATE_STOP,    grp_level_2 = ?RACK_2}).

membership_test_() ->
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

suite_mnesia_(_) ->
    %% ok = application:start(mnesia),
    %% ok = leo_cluster_tbl_member:create_table('ram_copies', [node()], ?MEMBER_TBL_CUR),
    %% ok = leo_cluster_tbl_member:create_table('ram_copies', [node()], ?MEMBER_TBL_PREV),
    %% ?debugVal(mnesia:table_info(?MEMBER_TBL_CUR,  all)),
    %% ?debugVal(mnesia:table_info(?MEMBER_TBL_PREV, all)),
    %% ok = inspect(?MNESIA),
    %% _ = application:stop(mnesia),
    %% net_kernel:stop(),
    ok.

suite_ets_(_) ->
    ok = leo_cluster_tbl_member:create_table(?MEMBER_TBL_CUR),
    ok = leo_cluster_tbl_member:create_table(?MEMBER_TBL_PREV),
    ok = inspect(?ETS),
    ok.

inspect(TableType) ->
    not_found = leo_cluster_tbl_member:find_all(TableType, ?MEMBER_TBL_CUR),
    not_found = leo_cluster_tbl_member:lookup(TableType, ?MEMBER_TBL_CUR, ?NODE_0),

    ok = leo_cluster_tbl_member:insert(TableType, ?MEMBER_TBL_CUR, {?NODE_0, ?MEMBER_0}),
    ok = leo_cluster_tbl_member:insert(TableType, ?MEMBER_TBL_CUR, {?NODE_1, ?MEMBER_1}),
    ok = leo_cluster_tbl_member:insert(TableType, ?MEMBER_TBL_CUR, {?NODE_2, ?MEMBER_2}),
    ok = leo_cluster_tbl_member:insert(TableType, ?MEMBER_TBL_CUR, {?NODE_3, ?MEMBER_3}),
    ok = leo_cluster_tbl_member:insert(TableType, ?MEMBER_TBL_CUR, {?NODE_4, ?MEMBER_4}),

    {ok, Rack1} = leo_cluster_tbl_member:find_by_level2(?RACK_1),
    {ok, Rack2} = leo_cluster_tbl_member:find_by_level2(?RACK_2),
    ?assertEqual(3, length(Rack1)),
    ?assertEqual(2, length(Rack2)),

    Res0 = leo_cluster_tbl_member:table_size(TableType, ?MEMBER_TBL_CUR),
    ?assertEqual(5, Res0),

    Res1 = leo_cluster_tbl_member:lookup(TableType, ?MEMBER_TBL_CUR, ?NODE_0),
    ?assertEqual({ok, ?MEMBER_0}, Res1),
    Res2 = leo_cluster_tbl_member:lookup(TableType, ?MEMBER_TBL_CUR, ?NODE_1),
    ?assertEqual({ok, ?MEMBER_1}, Res2),
    Res3 = leo_cluster_tbl_member:lookup(TableType, ?MEMBER_TBL_CUR, ?NODE_2),
    ?assertEqual({ok, ?MEMBER_2}, Res3),
    Res4 = leo_cluster_tbl_member:lookup(TableType, ?MEMBER_TBL_CUR, ?NODE_3),
    ?assertEqual({ok, ?MEMBER_3}, Res4),
    Res5 = leo_cluster_tbl_member:lookup(TableType, ?MEMBER_TBL_CUR, ?NODE_4),
    ?assertEqual({ok, ?MEMBER_4}, Res5),

    {ok, Res6} = leo_cluster_tbl_member:find_all(TableType, ?MEMBER_TBL_CUR),
    ?assertEqual(5, length(Res6)),

    ok = leo_cluster_tbl_member:delete(TableType, ?MEMBER_TBL_CUR, ?NODE_0),
    not_found = leo_cluster_tbl_member:lookup(TableType, ?MEMBER_TBL_CUR, ?NODE_0),
    Res7 = leo_cluster_tbl_member:table_size(TableType, ?MEMBER_TBL_CUR),
    ?assertEqual(4, Res7),

    Ret8 = leo_cluster_tbl_member:tab2list(TableType, ?MEMBER_TBL_CUR),
    ?assertEqual(4, length(Ret8)),


    ok = leo_cluster_tbl_member:replace(
           TableType, ?MEMBER_TBL_CUR,
           [?MEMBER_1, ?MEMBER_2, ?MEMBER_3, ?MEMBER_4],
           [?MEMBER_3, ?MEMBER_4]),

    Ret9 = leo_cluster_tbl_member:tab2list(TableType, ?MEMBER_TBL_CUR),
    ?assertEqual(2, length(Ret9)),

    Res10 = leo_cluster_tbl_member:table_size(TableType, ?MEMBER_TBL_CUR),
    ?assertEqual(2, Res10),

    ok = leo_cluster_tbl_member:insert(TableType, ?MEMBER_TBL_CUR, {?NODE_0, ?MEMBER_0}),
    ok = leo_cluster_tbl_member:insert(TableType, ?MEMBER_TBL_CUR, {?NODE_0, ?MEMBER_0}),

    Res11 = leo_cluster_tbl_member:table_size(TableType, ?MEMBER_TBL_CUR),
    ?assertEqual(3, Res11),
    Ret12 = leo_cluster_tbl_member:tab2list(TableType, ?MEMBER_TBL_CUR),
    ?assertEqual(3, length(Ret12)),

    {ok, Ret13} = leo_cluster_tbl_member:find_by_status(TableType, ?MEMBER_TBL_CUR, ?STATE_RUNNING),
    ?assertEqual(2, length(Ret13)),

    Ret14 = leo_cluster_tbl_member:find_by_status(TableType, ?MEMBER_TBL_CUR, undefined),
    ?assertEqual(not_found, Ret14),


    {ok, Ret15} = leo_cluster_tbl_member:find_all(),
    lists:foreach(fun(#member{alias = Alias,
                              grp_level_2 = L2
                             }) ->
                          ?assertEqual(false, [] == Alias),
                          ?assertEqual(false, undefined == Alias),
                          ?assertEqual(false, [] == L2),
                          ?assertEqual(false, undefined == L2)
                  end, Ret15),

    %% TEST overwrite records - cur to prev
    not_found = leo_cluster_tbl_member:find_all(?MEMBER_TBL_PREV),
    ok = leo_cluster_tbl_member:overwrite(?MEMBER_TBL_CUR, ?MEMBER_TBL_PREV),

    {ok, Ret17} = leo_cluster_tbl_member:find_all(?MEMBER_TBL_PREV),
    ?assertEqual(3, length(Ret17)),
    ?assertEqual(Ret15, Ret17),

    %% migration test
    ok = meck:new(leo_redundant_manager_api, [no_link, non_strict]),
    ok = meck:expect(leo_redundant_manager_api, synchronize, fun(_,[]) ->
                                                                     ok
                                                             end),
    ok = meck:new(mnesia, [no_link, non_strict]),
    ok = meck:expect(mnesia, table_info, fun(_,_) ->
                                                 ok
                                         end),

    OldTable = 'leo_members',
    ok = leo_cluster_tbl_member:create_table(OldTable),
    ok = leo_cluster_tbl_member:delete_all(?MEMBER_TBL_CUR),
    ok = leo_cluster_tbl_member:delete_all(?MEMBER_TBL_PREV),

    ok = leo_cluster_tbl_member:insert(TableType, OldTable, {?NODE_0, ?MEMBER_0}),
    ok = leo_cluster_tbl_member:insert(TableType, OldTable, {?NODE_1, ?MEMBER_1}),
    ok = leo_cluster_tbl_member:insert(TableType, OldTable, {?NODE_2, ?MEMBER_2}),

    ok = leo_cluster_tbl_member:transform(),
    {ok, MembersOrg}  = leo_cluster_tbl_member:find_all(OldTable),
    {ok, MembersCur}  = leo_cluster_tbl_member:find_all(?MEMBER_TBL_CUR),
    {ok, MembersPrev} = leo_cluster_tbl_member:find_all(?MEMBER_TBL_PREV),
    ?assertEqual(length(MembersOrg), length(MembersCur )),
    ?assertEqual(length(MembersOrg), length(MembersPrev)),

    MembersOrgHash  = erlang:crc32(term_to_binary(lists:sort(MembersOrg))),
    MembersCurHash  = erlang:crc32(term_to_binary(lists:sort(MembersCur))),
    MembersPrevHash = erlang:crc32(term_to_binary(lists:sort(MembersPrev))),

    ?assertEqual(MembersOrgHash, MembersCurHash ),
    ?assertEqual(MembersOrgHash, MembersPrevHash),

    meck:unload(),
    ok.

-endif.

