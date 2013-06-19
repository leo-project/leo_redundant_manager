%%======================================================================
%%
%% Leo Redundant Manager
%%
%% Copyright (c) 2012-2013 Rakuten, Inc.
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
-module(leo_redundant_manager_table_member_tests).
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
     [{with, [T]} || T <- [fun suite_ets_/1
                          ]]}.

setup() ->
    catch ets:delete_all_objects('leo_members'),
    ok.

teardown(_) ->
    ok.

%% suite_mnesia_(_) ->
%%     _ =application:start(mnesia),
%%     _ = leo_redundant_manager_table_member:create_members('ram_copies', [node()]),
%%     ok = inspect(?MNESIA),
%%     _ = application:stop(mnesia),
%%     ok.

suite_ets_(_) ->
    ok = leo_redundant_manager_table_member:create_members(),
    ok = inspect(?ETS),
    ok.

inspect(Table) ->
    not_found = leo_redundant_manager_table_member:find_all(Table),
    not_found = leo_redundant_manager_table_member:lookup(Table, ?NODE_0),

    ok = leo_redundant_manager_table_member:insert(Table, {?NODE_0, ?MEMBER_0}),
    ok = leo_redundant_manager_table_member:insert(Table, {?NODE_1, ?MEMBER_1}),
    ok = leo_redundant_manager_table_member:insert(Table, {?NODE_2, ?MEMBER_2}),
    ok = leo_redundant_manager_table_member:insert(Table, {?NODE_3, ?MEMBER_3}),
    ok = leo_redundant_manager_table_member:insert(Table, {?NODE_4, ?MEMBER_4}),

    {ok, Rack1} = leo_redundant_manager_table_member:find_by_level2(?RACK_1),
    {ok, Rack2} = leo_redundant_manager_table_member:find_by_level2(?RACK_2),
    ?assertEqual(3, length(Rack1)),
    ?assertEqual(2, length(Rack2)),

    Res0 = leo_redundant_manager_table_member:size(Table),
    ?assertEqual(5, Res0),

    Res1 = leo_redundant_manager_table_member:lookup(Table, ?NODE_0),
    ?assertEqual({ok, ?MEMBER_0}, Res1),
    Res2 = leo_redundant_manager_table_member:lookup(Table, ?NODE_1),
    ?assertEqual({ok, ?MEMBER_1}, Res2),
    Res3 = leo_redundant_manager_table_member:lookup(Table, ?NODE_2),
    ?assertEqual({ok, ?MEMBER_2}, Res3),
    Res4 = leo_redundant_manager_table_member:lookup(Table, ?NODE_3),
    ?assertEqual({ok, ?MEMBER_3}, Res4),
    Res5 = leo_redundant_manager_table_member:lookup(Table, ?NODE_4),
    ?assertEqual({ok, ?MEMBER_4}, Res5),

    {ok, Res6} = leo_redundant_manager_table_member:find_all(Table),
    ?assertEqual(5, length(Res6)),

    ok = leo_redundant_manager_table_member:delete(Table, ?NODE_0),
    not_found = leo_redundant_manager_table_member:lookup(Table, ?NODE_0),
    Res7 = leo_redundant_manager_table_member:size(Table),
    ?assertEqual(4, Res7),

    Ret8 = leo_redundant_manager_table_member:tab2list(Table),
    ?assertEqual(4, length(Ret8)),


    ok = leo_redundant_manager_table_member:replace(
           Table,
           [?MEMBER_1, ?MEMBER_2, ?MEMBER_3, ?MEMBER_4],
           [?MEMBER_3, ?MEMBER_4]),

    Ret9 = leo_redundant_manager_table_member:tab2list(Table),
    ?assertEqual(2, length(Ret9)),

    Res10 = leo_redundant_manager_table_member:size(Table),
    ?assertEqual(2, Res10),

    ok = leo_redundant_manager_table_member:insert(Table, {?NODE_0, ?MEMBER_0}),
    ok = leo_redundant_manager_table_member:insert(Table, {?NODE_0, ?MEMBER_0}),

    Res11 = leo_redundant_manager_table_member:size(Table),
    ?assertEqual(3, Res11),
    Ret12 = leo_redundant_manager_table_member:tab2list(Table),
    ?assertEqual(3, length(Ret12)),

    {ok, Ret13} = leo_redundant_manager_table_member:find_by_status(Table, ?STATE_RUNNING),
    ?assertEqual(2, length(Ret13)),

    Ret14 = leo_redundant_manager_table_member:find_by_status(Table, undefined),
    ?assertEqual(not_found, Ret14),


    {ok, Ret15_2} = leo_redundant_manager_table_member:find_all(),
    lists:foreach(fun(#member{alias = Alias,
                              grp_level_2 = L2
                             }) ->
                          ?assertEqual(false, [] == Alias),
                          ?assertEqual(false, undefined == Alias),
                          ?assertEqual(false, [] == L2),
                          ?assertEqual(false, undefined == L2)
                  end, Ret15_2),
    ok.

-endif.

