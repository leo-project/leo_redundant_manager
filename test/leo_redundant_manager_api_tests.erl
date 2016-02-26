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
-module(leo_redundant_manager_api_tests).

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(CLUSTER_ID, 'leofs_c1').
-define(NUM_OF_RECURSIVE_CALLS, 1000).


%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

check_redundancies_test_() ->
    {setup,
     fun ( ) ->
             ?debugVal("### CHECK-REDUNDANCIES ###"),
             {_} = setup(),
             ok
     end,
     fun (_) ->
             teardown([]),
             timer:sleep(3000),
             ok
     end,
     [
      {"check redundancies",
       {timeout, 5000, fun check_redundancies/0}}
     ]}.

check_redundancies() ->
    {ok, Hostname} = inet:gethostname(),
    ok = prepare(Hostname, master),
    ok = inspect_1(Hostname, 500),
    ok.


%% @doc Test SUITE
%%
suite_1_test_() ->
    {setup,
     fun ( ) ->
             ?debugVal("### TEST-1 ###"),
             setup(),
             ok
     end,
     fun (_) ->
             teardown([]),
             ok
     end,
     [
      {"",{timeout, 5000, fun redundant_manager_0/0}}
     ]}.
suite_2_test_() ->
    {setup,
     fun ( ) ->
             ?debugVal("### TEST-2 ###"),
             setup(),
             ok
     end,
     fun (_) ->
             teardown([]),
             ok
     end,
     [
      {"",{timeout, 5000, fun redundant_manager_1/0}}
     ]}.
suite_3_test_() ->
    {setup,
     fun ( ) ->
             ?debugVal("### TEST-3 ###"),
             setup(),
             ok
     end,
     fun (_) ->
             teardown([]),
             ok
     end,
     [
      {"",{timeout, 5000, fun attach_1/0}}
     ]}.
suite_4_test_() ->
    {setup,
     fun ( ) ->
             ?debugVal("### TEST-4 ###"),
             setup(),
             ok end,
     fun (_) ->
             teardown([]),
             ok
     end,
     [
      {"",{timeout, 5000, fun attach_2/0}}
     ]}.
suite_5_test_() ->
    {setup,
     fun ( ) ->
             ?debugVal("### TEST-5 ###"),
             setup(),
             ok
     end,
     fun (_) ->
             teardown([]),
             ok
     end,
     [
      {"",{timeout, 5000, fun detach/0}}
     ]}.
suite_6_test_() ->
    {setup,
     fun ( ) ->
             ?debugVal("### TEST-6 ###"),
             setup(),
             ok end,
     fun (_) ->
             teardown([]),
             ok end,
     [
      {"",{timeout, 5000, fun rack_aware_1/0}}
     ]}.
suite_7_test_() ->
    {setup,
     fun ( ) ->
             ?debugVal("### TEST-7 ###"),
             setup(),
             ok
     end,
     fun (_) ->
             teardown([]),
             ok end,
     [
      {"",{timeout, 5000, fun rack_aware_2/0}}
     ]}.

setup() ->
    [] = os:cmd("epmd -daemon"),
    {ok, Hostname} = inet:gethostname(),
    Me = list_to_atom("test_0@" ++ Hostname),
    net_kernel:start([Me, shortnames]),

    timer:sleep(1000),
    application:start(crypto),
    application:start(mnesia),
    application:start(leo_redundant_manager),

    catch ets:delete(?MEMBER_TBL_CUR),
    catch ets:delete(?MEMBER_TBL_PREV),
    catch ets:delete(?RING_TBL_CUR),
    catch ets:delete(?RING_TBL_PREV),

    %% ok = leo_cluster_tbl_member:create_table(
    %%        'ram_copies', [node()], ?MEMBER_TBL_CUR),
    %% ok = leo_cluster_tbl_member:create_table(
    %%        'ram_copies', [node()], ?MEMBER_TBL_PREV),
    %% ok = leo_cluster_tbl_ring:create_table_current(ram_copies),
    %% ok = leo_cluster_tbl_ring:create_table_prev(ram_copies),

    catch ets:new(?MEMBER_TBL_CUR, [named_table, ordered_set,
                                    public, {read_concurrency, true}]),
    catch ets:new(?MEMBER_TBL_PREV,[named_table, ordered_set,
                                    public, {read_concurrency, true}]),
    catch ets:new(?RING_TBL_CUR, [named_table, ordered_set,
                                  public, {read_concurrency, true}]),
    catch ets:new(?RING_TBL_PREV,[named_table, ordered_set,
                                  public, {read_concurrency, true}]),
    {Hostname}.

teardown(_) ->
    net_kernel:stop(),
    application:stop(leo_redundant_manager),
    application:stop(mnesia),
    application:stop(crypto),

    os:cmd("rm -rf queue"),
    os:cmd("rm ring_*"),
    timer:sleep(3000),
    ok.

redundant_manager_0() ->
    {ok, Hostname} = inet:gethostname(),
    ok = prepare(Hostname, master),
    inspect_1(Hostname, 1000),
    ok.

redundant_manager_1() ->
    {ok, Hostname} = inet:gethostname(),
    ok = prepare(Hostname, storage),
    inspect_1(Hostname, 1000),
    ok.

attach_1() ->
    {ok, Hostname} = inet:gethostname(),
    ok = prepare(Hostname, gateway),
    {ok,_,_} = leo_redundant_manager_api:create(?CLUSTER_ID, ?VER_CUR),
    {ok,_,_} = leo_redundant_manager_api:create(?CLUSTER_ID, ?VER_PREV),
    timer:sleep(1000),

    {ok, {Chksum_0, Chksum_1}} = leo_redundant_manager_api:checksum(
                                   ?CLUSTER_ID, ?CHECKSUM_RING),
    ?assertEqual(true, (Chksum_0 > -1)),
    ?assertEqual(true, (Chksum_1 > -1)),

    Size_1 = leo_cluster_tbl_ring:size(
               {ets, ?RING_TBL_CUR}, ?CLUSTER_ID),
    ?assertEqual((8 * ?DEF_NUMBER_OF_VNODES), Size_1),
    timer:sleep(100),

    %% rebalance.attach
    AttachedNode = list_to_atom("node_8@" ++ Hostname),
    AttachedMember = #?MEMBER{id = {?CLUSTER_ID, AttachedNode},
                              cluster_id = ?CLUSTER_ID,
                              node = AttachedNode,
                              alias= "node_8x",
                              state = ?STATE_ATTACHED},
    ok = leo_redundant_manager_api:attach(?CLUSTER_ID, AttachedMember),
    leo_redundant_manager_api:dump(?CLUSTER_ID, ?CHECKSUM_RING),
    leo_redundant_manager_api:dump(?CLUSTER_ID, ?CHECKSUM_MEMBER),

    Size_2 = leo_cluster_tbl_ring:size({ets, ?RING_TBL_CUR}, ?CLUSTER_ID),
    ?assertEqual((9 * ?DEF_NUMBER_OF_VNODES), Size_2),

    %% execute
    timer:sleep(100),
    {ok, Res1} = leo_redundant_manager_api:rebalance(?CLUSTER_ID),
    ?assertEqual(true, Res1 =/= []),
    lists:foreach(fun(Item) ->
                          Src  = proplists:get_value('src',  Item),
                          Dest = proplists:get_value('dest', Item),
                          ?assertEqual(true, Src  /= Dest),
                          ?assertEqual(true, Src  /= AttachedNode),
                          ?assertEqual(true, Dest == AttachedNode)
                  end, Res1),
    {ok, MembersCur} =
        leo_cluster_tbl_member:find_by_cluster_id(?MEMBER_TBL_CUR, ?CLUSTER_ID),
    {ok, MembersPrev} =
        leo_cluster_tbl_member:find_by_cluster_id(?MEMBER_TBL_PREV, ?CLUSTER_ID),

    %% check
    {ok, {RingHashCur, RingHashPrev  }} =
        leo_redundant_manager_api:checksum(?CLUSTER_ID, ?CHECKSUM_RING),
    {ok, {MemberHashCur, MemberHashPrev}} = leo_redundant_manager_api:checksum(
                                              ?CLUSTER_ID, ?CHECKSUM_MEMBER),

    ?assertEqual(9, length(MembersCur)),
    ?assertEqual(8, length(MembersPrev)),
    ?assertNotEqual([], MembersPrev),
    ?assertNotEqual(-1, RingHashCur),
    ?assertNotEqual(-1, RingHashPrev),
    ?assertNotEqual(RingHashCur, RingHashPrev),
    ?assertNotEqual(-1, MemberHashCur),
    ?assertNotEqual(-1, MemberHashPrev),

    %% retrieve redundancies
    timer:sleep(500),
    attach_1_1(50),
    ok.

attach_1_1(0) ->
    ok;
attach_1_1(Index) ->
    Key = list_to_binary("key_" ++ integer_to_list(Index)),
    {ok, R1} = leo_redundant_manager_api:get_redundancies_by_key(
                 ?CLUSTER_ID, put, Key),
    {ok, R2} = leo_redundant_manager_api:get_redundancies_by_key(
                 ?CLUSTER_ID, get, Key),
    case (R1#redundancies.nodes == R2#redundancies.nodes) of
        true ->
            ok;
        false ->
            ?assertEqual(1, (length(R2#redundancies.nodes) -
                                 length(R1#redundancies.nodes)))
    end,
    attach_1_1(Index - 1).


attach_2() ->
    {ok, Hostname} = inet:gethostname(),
    ok = prepare(Hostname, gateway),
    {ok,_,_} = leo_redundant_manager_api:create(?CLUSTER_ID, ?VER_CUR),
    {ok,_,_} = leo_redundant_manager_api:create(?CLUSTER_ID, ?VER_PREV),
    timer:sleep(1000),

    %% rebalance.attach
    AttachedNodes = [list_to_atom("node_8@" ++ Hostname),
                     list_to_atom("node_9@" ++ Hostname),
                     list_to_atom("node_10@" ++ Hostname)],
    AttachedMembers = [ #?MEMBER{id = {?CLUSTER_ID, _N},
                                 cluster_id = ?CLUSTER_ID,
                                 node = _N,
                                 alias= atom_to_list(_N),
                                 state = ?STATE_ATTACHED}
                        || _N <- AttachedNodes ],
    lists:foreach(fun(_M) ->
                          ok = leo_redundant_manager_api:attach(?CLUSTER_ID,_M)
                  end, AttachedMembers),
    leo_redundant_manager_api:dump(?CLUSTER_ID, ?CHECKSUM_RING),
    leo_redundant_manager_api:dump(?CLUSTER_ID, ?CHECKSUM_MEMBER),

    RingSize = leo_cluster_tbl_ring:size(
                 {ets, ?RING_TBL_CUR}, ?CLUSTER_ID),
    ?assertEqual((11 * ?DEF_NUMBER_OF_VNODES), RingSize),

    %% execute
    timer:sleep(100),
    {ok, Res1} = leo_redundant_manager_api:rebalance(?CLUSTER_ID),
    ?assertEqual(true, Res1 =/= []),
    lists:foreach(fun(Item) ->
                          Src  = proplists:get_value('src',  Item),
                          Dest = proplists:get_value('dest', Item),
                          ?assertEqual(true,  Src =/= Dest),
                          ?assertEqual(false, lists:member(Src,  AttachedNodes)),
                          ?assertEqual(true,  lists:member(Dest, AttachedNodes))
                  end, Res1),

    %% check
    {ok, MembersCur } = leo_cluster_tbl_member:find_by_cluster_id(
                          ?MEMBER_TBL_CUR, ?CLUSTER_ID),
    {ok, MembersPrev} = leo_cluster_tbl_member:find_by_cluster_id(
                          ?MEMBER_TBL_PREV, ?CLUSTER_ID),
    {ok, {RingHashCur, RingHashPrev}} =
        leo_redundant_manager_api:checksum(
          ?CLUSTER_ID, ?CHECKSUM_RING),
    {ok, {MemberHashCur, MemberHashPrev}} =
        leo_redundant_manager_api:checksum(
          ?CLUSTER_ID, ?CHECKSUM_MEMBER),

    ?assertEqual(11, length(MembersCur)),
    ?assertEqual(8,  length(MembersPrev)),
    ?assertNotEqual([], MembersPrev),
    ?assertNotEqual(-1, RingHashCur),
    ?assertNotEqual(-1, RingHashPrev),
    ?assertNotEqual(RingHashCur, RingHashPrev),
    ?assertNotEqual(-1, MemberHashCur),
    ?assertNotEqual(-1, MemberHashPrev),

    %% retrieve redundancies
    timer:sleep(500),
    attach_2_1(50),
    ok.

attach_2_1(0) ->
    ok;
attach_2_1(Index) ->
    Key = list_to_binary("key_" ++ integer_to_list(Index)),
    {ok, R1} = leo_redundant_manager_api:get_redundancies_by_key(
                 ?CLUSTER_ID, put, Key),
    {ok, R2} = leo_redundant_manager_api:get_redundancies_by_key(
                 ?CLUSTER_ID, get, Key),
    case (R1#redundancies.nodes == R2#redundancies.nodes) of
        true ->
            void;
        false ->
            Difference = (length(R2#redundancies.nodes) -
                              length(R1#redundancies.nodes)),
            ?assertEqual(true, (Difference > 0 andalso Difference =< 3))
    end,
    attach_2_1(Index - 1).


detach() ->
    {ok, Hostname} = inet:gethostname(),
    ok = prepare(Hostname, gateway),
    {ok,_,_} = leo_redundant_manager_api:create(?CLUSTER_ID, ?VER_CUR),
    {ok,_,_} = leo_redundant_manager_api:create(?CLUSTER_ID, ?VER_PREV),
    timer:sleep(1000),

    %% 1. rebalance.detach
    DetachNode = list_to_atom("node_0@" ++ Hostname),
    ok = leo_redundant_manager_api:detach(?CLUSTER_ID, DetachNode),
    {ok, Res} = leo_redundant_manager_api:rebalance(?CLUSTER_ID),
    ?assertEqual(true, Res =/= []),

    lists:foreach(fun(Item) ->
                          Src  = proplists:get_value('src',  Item),
                          Dest = proplists:get_value('dest', Item),
                          ?assertEqual(true, Src  =/= Dest),
                          ?assertEqual(true, Src  =/= DetachNode),
                          ?assertEqual(true, Dest =/= DetachNode)
                  end, Res),
    {ok, MembersCur } = leo_cluster_tbl_member:find_by_cluster_id(
                          ?MEMBER_TBL_CUR, ?CLUSTER_ID),
    {ok, MembersPrev} = leo_cluster_tbl_member:find_by_cluster_id(
                          ?MEMBER_TBL_PREV, ?CLUSTER_ID),

    %% re-create previous-ring
    {ok, {RingHashCur, RingHashPrev}} =
        leo_redundant_manager_api:checksum(?CLUSTER_ID, ?CHECKSUM_RING),
    {ok, {MemberHashCur, MemberHashPrev}} =
        leo_redundant_manager_api:checksum(?CLUSTER_ID, ?CHECKSUM_MEMBER),

    ?assertEqual(7, length(MembersCur)),
    ?assertEqual(7, length(MembersPrev)),
    ?assertNotEqual(-1, RingHashCur),
    ?assertNotEqual(-1, RingHashPrev),
    ?assertNotEqual(-1, MemberHashCur),
    ?assertNotEqual(-1, MemberHashPrev),

    timer:sleep(500),
    detach_1_1(50),
    ok.

detach_1_1(0) ->
    ok;
detach_1_1(Index) ->
    Key = list_to_binary("key_" ++ integer_to_list(Index)),
    {ok, R1} = leo_redundant_manager_api:get_redundancies_by_key(
                 ?CLUSTER_ID, put, Key),
    {ok, R2} = leo_redundant_manager_api:get_redundancies_by_key(
                 ?CLUSTER_ID,get, Key),
    case (R1#redundancies.nodes == R2#redundancies.nodes) of
        true ->
            ok;
        false ->
            R1Nodes = [N || #redundant_node{node = N} <- R1#redundancies.nodes],
            R2Nodes = [N || #redundant_node{node = N} <- R2#redundancies.nodes],
            ?debugVal({R1Nodes, R2Nodes})
    end,
    detach_1_1(Index - 1).


%% @TODO
rack_aware_1() ->
    ok.

rack_aware_2() ->
    ok.


%% -------------------------------------------------------------------
%% INNER FUNCTION
%% -------------------------------------------------------------------
prepare(Hostname, ServerType) ->
    prepare(Hostname, ServerType, 8).

prepare(Hostname,_ServerType, NumOfNodes) ->
    CallbackFun = fun()-> ok end,
    Options = [{mq_dir, "work/mq-dir/"},
               {monitors, []},
               {membership_callback, CallbackFun},
               {system_conf, [{n, 3}, {w, 2}, {r ,1}, {d, 2}]}],
    ok = leo_redundant_manager_api:start(
           ?CLUSTER_ID, 'persistent_node', Options),

    Node_0 = list_to_atom("node_0@" ++ Hostname),
    Node_1 = list_to_atom("node_1@" ++ Hostname),
    Node_2 = list_to_atom("node_2@" ++ Hostname),
    Node_3 = list_to_atom("node_3@" ++ Hostname),
    Node_4 = list_to_atom("node_4@" ++ Hostname),
    Node_5 = list_to_atom("node_5@" ++ Hostname),
    Node_6 = list_to_atom("node_6@" ++ Hostname),
    Node_7 = list_to_atom("node_7@" ++ Hostname),

    leo_redundant_manager_api:attach(?CLUSTER_ID,
                                     #?MEMBER{id = {?CLUSTER_ID, Node_0},
                                              cluster_id = ?CLUSTER_ID,
                                              node = Node_0,
                                              alias= "node_0",
                                              state = ?STATE_ATTACHED}),
    leo_redundant_manager_api:attach(?CLUSTER_ID,
                                     #?MEMBER{id = {?CLUSTER_ID, Node_1},
                                              cluster_id = ?CLUSTER_ID,
                                              node = Node_1,
                                              alias= "node_1",
                                              state = ?STATE_ATTACHED}),
    leo_redundant_manager_api:attach(?CLUSTER_ID,
                                     #?MEMBER{id = {?CLUSTER_ID, Node_2},
                                              cluster_id = ?CLUSTER_ID,
                                              node = Node_2,
                                              alias= "node_2",
                                              state = ?STATE_ATTACHED}),
    case NumOfNodes of
        8 ->
            leo_redundant_manager_api:attach(?CLUSTER_ID,
                                             #?MEMBER{id = {?CLUSTER_ID, Node_3},
                                                      cluster_id = ?CLUSTER_ID,
                                                      node = Node_3,
                                                      alias= "node_3",
                                                      state = ?STATE_ATTACHED}),
            leo_redundant_manager_api:attach(?CLUSTER_ID,
                                             #?MEMBER{id = {?CLUSTER_ID, Node_4},
                                                      cluster_id = ?CLUSTER_ID,
                                                      node = Node_4,
                                                      alias= "node_4",
                                                      state = ?STATE_ATTACHED}),
            leo_redundant_manager_api:attach(?CLUSTER_ID,
                                             #?MEMBER{id = {?CLUSTER_ID, Node_5},
                                                      cluster_id = ?CLUSTER_ID,
                                                      node = Node_5,
                                                      alias= "node_5",
                                                      state = ?STATE_ATTACHED}),
            leo_redundant_manager_api:attach(?CLUSTER_ID,
                                             #?MEMBER{id = {?CLUSTER_ID, Node_6},
                                                      cluster_id = ?CLUSTER_ID,
                                                      node = Node_6,
                                                      alias= "node_6",
                                                      state = ?STATE_ATTACHED}),
            leo_redundant_manager_api:attach(?CLUSTER_ID,
                                             #?MEMBER{id = {?CLUSTER_ID, Node_7},
                                                      cluster_id = ?CLUSTER_ID,
                                                      node = Node_7,
                                                      alias= "node_7",
                                                      state = ?STATE_ATTACHED});
        _ ->
            void
    end,
    ok.


%% @private
inspect_1(Hostname, NumOfIteration) ->
    %% Check member-related functions
    {ok, Members_1} = leo_redundant_manager_api:get_members(?CLUSTER_ID),
    {ok, {MembersCur, MembersPrev}} =
        leo_redundant_manager_api:get_all_ver_members(?CLUSTER_ID),

    ?assertEqual(8, length(Members_1)),
    ?assertEqual(8, length(MembersCur)),
    ?assertEqual(8, length(MembersPrev)),
    ?assertEqual(true,  leo_redundant_manager_api:has_member(
                          ?CLUSTER_ID,
                          list_to_atom("node_3@" ++ Hostname))),
    ?assertEqual(false, leo_redundant_manager_api:has_member(
                          ?CLUSTER_ID,
                          list_to_atom("node_8@" ++ Hostname))),

    {ok, Member_5} = leo_redundant_manager_api:get_member_by_node(
                       ?CLUSTER_ID,
                       list_to_atom("node_5@" ++ Hostname)),
    ?assertEqual(list_to_atom("node_5@" ++ Hostname), Member_5#?MEMBER.node),

    ok = leo_redundant_manager_api:update_member_by_node(
           ?CLUSTER_ID,
           list_to_atom("node_7@" ++ Hostname),
           12345, 'suspend'),
    {ok, Member_7} = leo_redundant_manager_api:get_member_by_node(
                       ?CLUSTER_ID,
                       list_to_atom("node_7@" ++ Hostname)),
    ?assertEqual(12345, Member_7#?MEMBER.clock),
    ?assertEqual(?STATE_SUSPEND, Member_7#?MEMBER.state),

    %% Create routing-table > confirmations
    {ok, MembersL_1, Chksums} = leo_redundant_manager_api:create(?CLUSTER_ID),
    timer:sleep(3000),

    {ok, {Chksum_0, Chksum_1}} = leo_redundant_manager_api:checksum(
                                   ?CLUSTER_ID, ?CHECKSUM_RING),
    ?assertEqual(true, (Chksum_0 > -1)),
    ?assertEqual(true, (Chksum_1 > -1)),
    ?assertEqual(8, length(MembersL_1)),
    ?assertEqual(2, length(Chksums)),

    {ok, {Chksum_2, Chksum_3}} = leo_redundant_manager_api:checksum(
                                   ?CLUSTER_ID, ?CHECKSUM_RING),
    {ok, {Chksum_4, Chksum_5}} = leo_redundant_manager_api:checksum(
                                   ?CLUSTER_ID, ?CHECKSUM_MEMBER),
    ?assertEqual(true, (-1 =< Chksum_2)),
    ?assertEqual(true, (-1 =< Chksum_3)),
    ?assertEqual(true, (-1 =< Chksum_4)),
    ?assertEqual(true, (-1 =< Chksum_5)),

    ?assertEqual((8 * ?DEF_NUMBER_OF_VNODES),
                 leo_cluster_tbl_ring:size({ets, ?RING_TBL_CUR}, ?CLUSTER_ID)),
    ?assertEqual((8 * ?DEF_NUMBER_OF_VNODES),
                 leo_cluster_tbl_ring:size({ets, ?RING_TBL_PREV}, ?CLUSTER_ID)),
    timer:sleep(1000),

    %% Check redundant nodes
    {ok, #redundancies{nodes = N0}} =
        leo_redundant_manager_api:get_redundancies_by_addr_id(
          ?CLUSTER_ID, put, 0),
    {ok, #redundancies{nodes = N1}} =
        leo_redundant_manager_api:get_redundancies_by_addr_id(
          ?CLUSTER_ID, put, leo_math:power(2, 128)),
    ?assertEqual(3, length(N0)),
    ?assertEqual(3, length(N1)),

    ok = inspect_redundancies_1(NumOfIteration),
    ok = inspect_redundancies_2(NumOfIteration),

    Max = leo_math:power(2, ?MD5),
    {ok, #redundancies{id = Id,
                       vnode_id_to = VNodeId1,
                       nodes = Nodes1,
                       n = 3,
                       r = 1,
                       w = 2,
                       d = 2}} = leo_redundant_manager_api:get_redundancies_by_addr_id(
                                   ?CLUSTER_ID, put, Max + 1),
    ?assertEqual(true, (Id > VNodeId1)),
    ?assertEqual(3, length(Nodes1)),

    lists:foreach(fun(_) ->
                          Id2 = random:uniform(Max),
                          {ok, Res2} = leo_redundant_manager_api:range_of_vnodes(
                                         ?CLUSTER_ID, Id2),
                          inspect_2(Id2, Res2)
                  end, lists:seq(0, 300)),
    {ok, Res3} = leo_redundant_manager_api:range_of_vnodes(
                   ?CLUSTER_ID, 0),
    inspect_2(0, Res3),

    Max1 = leo_math:power(2,128) - 1,
    {ok, Res4} = leo_redundant_manager_api:range_of_vnodes(
                   ?CLUSTER_ID, Max1),
    ?assertEqual(2, length(Res4)),

    {ok, {_Options, Res6}} =
        leo_redundant_manager_api:collect_redundancies_by_key(
          ?CLUSTER_ID, <<"air_on_the_g_string_2">>, 6, 3),
    ?assertEqual(6, length(Res6)),

    {ok, Res7} =
        leo_redundant_manager_api:part_of_collect_redundancies_by_key(
          ?CLUSTER_ID, 3, <<"air_on_the_g_string_2\n3">>, 6, 3),
    Res7_1 = lists:nth(3, Res6),
    ?assertEqual(Res7, Res7_1),

    ok = leo_redundant_manager_api:dump(?CLUSTER_ID, work),
    ok.


%% @private
inspect_redundancies_1(0) ->
    ok;
inspect_redundancies_1(Counter) ->
    case leo_redundant_manager_api:get_redundancies_by_key(
           ?CLUSTER_ID, integer_to_list(Counter)) of
        {ok, #redundancies{id = _Id0,
                           vnode_id_to = _VNodeId0,
                           nodes = Nodes0,
                           n = 3,
                           r = 1,
                           w = 2,
                           d = 2}} ->
            lists:foreach(fun(A) ->
                                  Nodes0a = lists:delete(A, Nodes0),
                                  lists:foreach(fun(B) ->
                                                        ?assertEqual(false, (A == B))
                                                end, Nodes0a)
                          end, Nodes0),
            ?assertEqual(3, length(Nodes0));
        _Other ->
            ?debugVal(_Other)
    end,
    inspect_redundancies_1(Counter - 1).

inspect_redundancies_2(0) ->
    ok;
inspect_redundancies_2(Counter) ->
    Max = leo_math:power(2, ?MD5),
    Id  = random:uniform(Max),
    {ok, #redundancies{id = _Id0,
                       vnode_id_to = _VNodeId0,
                       nodes = Nodes0,
                       n = 3,
                       r = 1,
                       w = 2,
                       d = 2}
    } = leo_redundant_manager_api:get_redundancies_by_addr_id(
          ?CLUSTER_ID, put, Id),
    lists:foreach(fun(A) ->
                          Nodes0a = lists:delete(A, Nodes0),
                          lists:foreach(fun(B) ->
                                                ?assertEqual(false, (A == B))
                                        end, Nodes0a)
                  end, Nodes0),
    ?assertEqual(3, length(Nodes0)),
    inspect_redundancies_2(Counter - 1).

%% @private
inspect_2(Id, VNodes) ->
    Max = leo_math:power(2, 128),
    case length(VNodes) of
        1 ->
            [{From, To}] = VNodes,
            ?assertEqual(true, ((From =< Id) andalso (Id =< To)));
        2 ->
            [{From0, To0}, {From1, To1}] = VNodes,
            case (From1 =< Id andalso Id =< To1) of
                true ->
                    ?assertEqual(true, ((From0 =< Max) andalso (To0 =< Max))),
                    ?assertEqual(true, ((From1 =< Id ) andalso (Id =< To1)));
                false ->
                    ?assertEqual(true, ((From0 =< Max) andalso (To0 =< Max)))
            end
    end.
-endif.
