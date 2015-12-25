%%======================================================================
%%
%% Leo Redundant Manager
%%
%% Copyright (c) 2012-2015 Rakuten, Inc.
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
-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

check_redundancies_test_() ->
    {setup,
     fun ( ) ->
             ?debugVal("### CHECK-REDUNDANCIES.START ###"),
             {_} = setup(),
             ok
     end,
     fun (_) ->
             teardown([]),
             timer:sleep(3000),
             ?debugVal("### CHECK-REDUNDANCIES.END ###"),
             ok
     end,
     [
      {"check redundancies",
       {timeout, 5000, fun check_redundancies/0}}
     ]}.

check_redundancies() ->
    {ok, Hostname} = inet:gethostname(),
    ok = prepare(Hostname, master),
    ok = inspect_0(Hostname, 500),
    ok.


%% @doc Test SUITE
%%
suite_1_test_() ->
    {setup,
     fun ( ) -> setup(),      ok end,
     fun (_) -> teardown([]), ok end,
     [
      {"",{timeout, 5000, fun redundant_manager_0/0}}
     ]}.
suite_2_test_() ->
    {setup,
     fun ( ) -> setup(),      ok end,
     fun (_) -> teardown([]), ok end,
     [
      {"",{timeout, 5000, fun redundant_manager_1/0}}
     ]}.
suite_3_test_() ->
    {setup,
     fun ( ) -> setup(),      ok end,
     fun (_) -> teardown([]), ok end,
     [
      {"",{timeout, 5000, fun attach_1/0}}
     ]}.
suite_4_test_() ->
    {setup,
     fun ( ) -> setup(),      ok end,
     fun (_) -> teardown([]), ok end,
     [
      {"",{timeout, 5000, fun attach_2/0}}
     ]}.
suite_5_test_() ->
    {setup,
     fun ( ) -> setup(),      ok end,
     fun (_) -> teardown([]), ok end,
     [
      {"",{timeout, 5000, fun detach/0}}
     ]}.
suite_7_test_() ->
    {setup,
     fun ( ) -> setup(),      ok end,
     fun (_) -> teardown([]), ok end,
     [
      {"",{timeout, 5000, fun members_table/0}}
     ]}.
suite_8_test_() ->
    {setup,
     fun ( ) -> setup(),      ok end,
     fun (_) -> teardown([]), ok end,
     [
      {"",{timeout, 5000, fun rack_aware_1/0}}
     ]}.
suite_9_test_() ->
    {setup,
     fun ( ) -> setup(),      ok end,
     fun (_) -> teardown([]), ok end,
     [
      {"",{timeout, 5000, fun rack_aware_2/0}}
     ]}.

setup() ->
    [] = os:cmd("epmd -daemon"),
    {ok, Hostname} = inet:gethostname(),
    Me = list_to_atom("test_0@" ++ Hostname),
    net_kernel:start([Me, shortnames]),

    timer:sleep(1000),
    catch ets:delete_all_objects(?MEMBER_TBL_CUR),
    catch ets:delete_all_objects(?MEMBER_TBL_PREV),
    catch ets:delete_all_objects(?RING_TBL_CUR),
    catch ets:delete_all_objects(?RING_TBL_PREV),

    leo_misc:init_env(),
    leo_misc:set_env(?APP, ?PROP_SERVER_TYPE, ?MONITOR_NODE),
    leo_cluster_tbl_member:create_table(?MEMBER_TBL_CUR),
    leo_cluster_tbl_member:create_table(?MEMBER_TBL_PREV),
    {Hostname}.

teardown(_) ->
    net_kernel:stop(),
    catch application:stop(leo_mq),
    catch application:stop(leo_backend_db),
    catch leo_redundant_manager_sup:stop(),
    catch application:stop(leo_redundant_manager),

    os:cmd("rm -rf queue"),
    os:cmd("rm ring_*"),
    timer:sleep(3000),
    ok.

redundant_manager_0() ->
    {ok, Hostname} = inet:gethostname(),
    ok = prepare(Hostname, master),
    inspect_0(Hostname, 1000),
    ok.

redundant_manager_1() ->
    {ok, Hostname} = inet:gethostname(),
    ok = prepare(Hostname, storage),
    inspect_0(Hostname, 1000),
    ok.

attach_1() ->
    {ok, Hostname} = inet:gethostname(),
    ok = prepare(Hostname, gateway),
    {ok, _, _} = leo_redundant_manager_api:create(?VER_CUR),
    {ok, _, _} = leo_redundant_manager_api:create(?VER_PREV),
    timer:sleep(1000),

    {ok, {Chksum0, Chksum1}} = leo_redundant_manager_api:checksum(?CHECKSUM_RING),
    ?assertEqual(true, (Chksum0 > -1)),
    ?assertEqual(true, (Chksum1 > -1)),

    Size_1 = leo_cluster_tbl_ring:size({ets, ?RING_TBL_CUR}),
    ?assertEqual((8 * ?DEF_NUMBER_OF_VNODES), Size_1),
    timer:sleep(100),

    %% rebalance.attach
    AttachNode = list_to_atom("node_8@" ++ Hostname),
    ok = leo_redundant_manager_api:attach(AttachNode),
    leo_redundant_manager_api:dump(?CHECKSUM_RING),
    leo_redundant_manager_api:dump(?CHECKSUM_MEMBER),

    Size_2 = leo_cluster_tbl_ring:size({ets, ?RING_TBL_CUR}),
    ?assertEqual((9 * ?DEF_NUMBER_OF_VNODES), Size_2),

    %% execute
    timer:sleep(100),
    {ok, Res1} = leo_redundant_manager_api:rebalance(),
    ?assertEqual(true, Res1 =/= []),
    lists:foreach(fun(Item) ->
                          Src  = proplists:get_value('src',  Item),
                          Dest = proplists:get_value('dest', Item),
                          ?assertEqual(true, Src  /= Dest),
                          ?assertEqual(true, Src  /= AttachNode),
                          ?assertEqual(true, Dest == AttachNode)
                  end, Res1),
    {ok, MembersCur } = leo_cluster_tbl_member:find_all(?MEMBER_TBL_CUR),
    {ok, MembersPrev} = leo_cluster_tbl_member:find_all(?MEMBER_TBL_PREV),

    %% check
    {ok, {RingHashCur,   RingHashPrev  }} = leo_redundant_manager_api:checksum(ring),
    {ok, {MemberHashCur, MemberHashPrev}} = leo_redundant_manager_api:checksum(member),

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
    {ok, R1} = leo_redundant_manager_api:get_redundancies_by_key(put, Key),
    {ok, R2} = leo_redundant_manager_api:get_redundancies_by_key(get, Key),
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
    {ok, _, _} = leo_redundant_manager_api:create(?VER_CUR),
    {ok, _, _} = leo_redundant_manager_api:create(?VER_PREV),
    timer:sleep(1000),

    %% rebalance.attach
    AttachedNodes = [list_to_atom("node_8@"  ++ Hostname),
                     list_to_atom("node_9@"  ++ Hostname),
                     list_to_atom("node_10@" ++ Hostname)],
    lists:foreach(fun(_N) ->
                          ok = leo_redundant_manager_api:attach(_N)
                  end, AttachedNodes),
    leo_redundant_manager_api:dump(?CHECKSUM_RING),
    leo_redundant_manager_api:dump(?CHECKSUM_MEMBER),

    RingSize = leo_cluster_tbl_ring:size({ets, ?RING_TBL_CUR}),
    ?assertEqual((11 * ?DEF_NUMBER_OF_VNODES), RingSize),

    %% execute
    timer:sleep(100),
    {ok, Res1} = leo_redundant_manager_api:rebalance(),
    ?assertEqual(true, Res1 =/= []),
    lists:foreach(fun(Item) ->
                          Src  = proplists:get_value('src',  Item),
                          Dest = proplists:get_value('dest', Item),
                          ?assertEqual(true,  Src =/= Dest),
                          ?assertEqual(false, lists:member(Src,  AttachedNodes)),
                          ?assertEqual(true,  lists:member(Dest, AttachedNodes))
                  end, Res1),


    %% check
    {ok, MembersCur } = leo_cluster_tbl_member:find_all(?MEMBER_TBL_CUR),
    {ok, MembersPrev} = leo_cluster_tbl_member:find_all(?MEMBER_TBL_PREV),
    {ok, {RingHashCur,   RingHashPrev  }} = leo_redundant_manager_api:checksum(ring),
    {ok, {MemberHashCur, MemberHashPrev}} = leo_redundant_manager_api:checksum(member),

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
    {ok, R1} = leo_redundant_manager_api:get_redundancies_by_key(put, Key),
    {ok, R2} = leo_redundant_manager_api:get_redundancies_by_key(get, Key),
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
    {ok, _, _} = leo_redundant_manager_api:create(?VER_CUR),
    {ok, _, _} = leo_redundant_manager_api:create(?VER_PREV),
    timer:sleep(1000),

    %% 1. rebalance.detach
    DetachNode = list_to_atom("node_0@" ++ Hostname),
    ok = leo_redundant_manager_api:detach(DetachNode),
    {ok, Res} = leo_redundant_manager_api:rebalance(),
    ?assertEqual(true, Res =/= []),

    lists:foreach(fun(Item) ->
                          Src  = proplists:get_value('src',  Item),
                          Dest = proplists:get_value('dest', Item),
                          ?assertEqual(true, Src  =/= Dest),
                          ?assertEqual(true, Src  =/= DetachNode),
                          ?assertEqual(true, Dest =/= DetachNode)
                  end, Res),
    {ok, MembersCur } = leo_cluster_tbl_member:find_all(?MEMBER_TBL_CUR),
    {ok, MembersPrev} = leo_cluster_tbl_member:find_all(?MEMBER_TBL_PREV),

    %% re-create previous-ring
    {ok, {RingHashCur,   RingHashPrev  }} = leo_redundant_manager_api:checksum(ring),
    {ok, {MemberHashCur, MemberHashPrev}} = leo_redundant_manager_api:checksum(member),
    ?assertEqual(7, length(MembersCur)),
    ?assertEqual(7, length(MembersPrev)),
    %% ?assertEqual(RingHashCur, RingHashPrev),
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
    {ok, R1} = leo_redundant_manager_api:get_redundancies_by_key(put, Key),
    {ok, R2} = leo_redundant_manager_api:get_redundancies_by_key(get, Key),
    case (R1#redundancies.nodes == R2#redundancies.nodes) of
        true ->
            ok;
        false ->
            R1Nodes = [N || #redundant_node{node = N} <- R1#redundancies.nodes],
            R2Nodes = [N || #redundant_node{node = N} <- R2#redundancies.nodes],
            ?debugVal({R1Nodes, R2Nodes})
    end,
    detach_1_1(Index - 1).


-define(TEST_MEMBERS, [#member{node = 'node_0@127.0.0.1'},
                       #member{node = 'node_1@127.0.0.1'},
                       #member{node = 'node_2@127.0.0.1'}]).
members_table() ->
    %% create -> get -> not-found
    not_found = leo_cluster_tbl_member:find_all(),
    ?assertEqual(0, leo_cluster_tbl_member:table_size()),
    ?assertEqual(not_found, leo_cluster_tbl_member:lookup('node_0@127.0.0.1')),

    %% insert
    lists:foreach(fun(Item) ->
                          ?assertEqual(ok, leo_cluster_tbl_member:insert({Item#member.node, Item}))
                  end, ?TEST_MEMBERS),

    %% update
    ok = leo_cluster_tbl_member:insert({'node_1@127.0.0.1', #member{node  = 'node_1@127.0.0.1',
                                                                    clock = 12345,
                                                                    state = 'suspend'}}),

    %% get
    {ok, Members} = leo_cluster_tbl_member:find_all(),
    ?assertEqual(3, leo_cluster_tbl_member:table_size()),
    ?assertEqual(3, length(Members)),

    ?assertEqual({ok, lists:nth(1,?TEST_MEMBERS)},
                 leo_cluster_tbl_member:lookup('node_0@127.0.0.1')),

    %% delete
    #member{node = Node} = lists:nth(1, ?TEST_MEMBERS),
    leo_cluster_tbl_member:delete(Node),
    ?assertEqual(2, leo_cluster_tbl_member:table_size()),
    ok.

rack_aware_1() ->
    {ok, Hostname} = inet:gethostname(),
    catch ets:delete('leo_members'),
    catch ets:delete('leo_ring_cur'),
    catch ets:delete('leo_ring_prv'),

    {ok, _RefSup} = leo_redundant_manager_sup:start_link(master),
    leo_redundant_manager_api:set_options([{n, 3},
                                           {r, 1},
                                           {w ,2},
                                           {d, 2},
                                           {bit_of_ring, 128},
                                           {level_2, 1}
                                          ]),
    Node0  = list_to_atom("node_0@"  ++ Hostname),
    Node1  = list_to_atom("node_1@"  ++ Hostname),
    Node2  = list_to_atom("node_2@"  ++ Hostname),
    Node3  = list_to_atom("node_3@"  ++ Hostname),
    Node4  = list_to_atom("node_4@"  ++ Hostname),
    Node5  = list_to_atom("node_5@"  ++ Hostname),
    Node6  = list_to_atom("node_6@"  ++ Hostname),
    Node7  = list_to_atom("node_7@"  ++ Hostname),
    Node8  = list_to_atom("node_8@"  ++ Hostname),
    Node9  = list_to_atom("node_9@"  ++ Hostname),
    Node10 = list_to_atom("node_10@" ++ Hostname),
    Node11 = list_to_atom("node_11@" ++ Hostname),
    Node12 = list_to_atom("node_12@" ++ Hostname),
    Node13 = list_to_atom("node_13@" ++ Hostname),
    Node14 = list_to_atom("node_14@" ++ Hostname),
    Node15 = list_to_atom("node_15@" ++ Hostname),

    R1 = [Node0, Node1, Node2,  Node3,  Node4,  Node5,  Node6,  Node7],
    R2 = [Node8, Node9, Node10, Node11, Node12, Node13, Node14, Node15],

    leo_redundant_manager_api:attach(Node0, "R1"),
    leo_redundant_manager_api:attach(Node1, "R1"),
    leo_redundant_manager_api:attach(Node2, "R1"),
    leo_redundant_manager_api:attach(Node3, "R1"),
    leo_redundant_manager_api:attach(Node4, "R1"),
    leo_redundant_manager_api:attach(Node5, "R1"),
    leo_redundant_manager_api:attach(Node6, "R1"),
    leo_redundant_manager_api:attach(Node7, "R1"),

    leo_redundant_manager_api:attach(Node8,  "R2"),
    leo_redundant_manager_api:attach(Node9,  "R2"),
    leo_redundant_manager_api:attach(Node10, "R2"),
    leo_redundant_manager_api:attach(Node11, "R2"),
    leo_redundant_manager_api:attach(Node12, "R2"),
    leo_redundant_manager_api:attach(Node13, "R2"),
    leo_redundant_manager_api:attach(Node14, "R2"),
    leo_redundant_manager_api:attach(Node15, "R2"),

    {ok, _, _} = leo_redundant_manager_api:create(?VER_CUR),
    {ok, _, _} = leo_redundant_manager_api:create(?VER_PREV),
    timer:sleep(1000),

    ok = leo_redundant_manager_worker:force_sync(?RING_TBL_CUR),
    ok = leo_redundant_manager_worker:force_sync(?RING_TBL_PREV),
    ?debugVal(leo_redundant_manager_worker:checksum()),

    lists:foreach(
      fun(N) ->
              {ok, #redundancies{nodes = Nodes}} =
                  leo_redundant_manager_api:get_redundancies_by_key(
                    lists:append(["LEOFS_", integer_to_list(N)])),
              #redundant_node{node = N1} = lists:nth(1, Nodes),
              #redundant_node{node = N2} = lists:nth(2, Nodes),
              #redundant_node{node = N3} = lists:nth(3, Nodes),

              SumR1_1 = case lists:member(N1, R1) of
                            true  -> 1;
                            false -> 0
                        end,
              SumR1_2 = case lists:member(N2, R1) of
                            true  -> SumR1_1 + 1;
                            false -> SumR1_1
                        end,
              SumR1_3 = case lists:member(N3, R1) of
                            true  -> SumR1_2 + 1;
                            false -> SumR1_2
                        end,
              SumR2_1 = case lists:member(N1, R2) of
                            true  -> 1;
                            false -> 0
                        end,
              SumR2_2 = case lists:member(N2, R2) of
                            true  -> SumR2_1 + 1;
                            false -> SumR2_1
                        end,
              SumR2_3 = case lists:member(N3, R2) of
                            true  -> SumR2_2 + 1;
                            false -> SumR2_2
                        end,
              ?assertEqual(false, (SumR1_3 == 0 orelse SumR2_3 == 0))
      end, lists:seq(1, 300)),
    ok.

rack_aware_2() ->
    {ok, Hostname} = inet:gethostname(),
    catch ets:delete_all_objects('leo_members'),
    catch ets:delete_all_objects(?RING_TBL_CUR),
    catch ets:delete_all_objects(?RING_TBL_PREV),

    {ok, _RefSup} = leo_redundant_manager_sup:start_link(master),
    leo_redundant_manager_api:set_options([{n, 5},
                                           {r, 1},
                                           {w ,2},
                                           {d, 2},
                                           {bit_of_ring, 128},
                                           {level_2, 1}
                                          ]),
    Node0  = list_to_atom("node_0@"  ++ Hostname),
    Node1  = list_to_atom("node_1@"  ++ Hostname),
    Node2  = list_to_atom("node_2@"  ++ Hostname),
    Node3  = list_to_atom("node_3@"  ++ Hostname),
    Node4  = list_to_atom("node_4@"  ++ Hostname),
    Node5  = list_to_atom("node_5@"  ++ Hostname),
    Node6  = list_to_atom("node_6@"  ++ Hostname),
    Node7  = list_to_atom("node_7@"  ++ Hostname),
    Node8  = list_to_atom("node_8@"  ++ Hostname),
    Node9  = list_to_atom("node_9@"  ++ Hostname),
    Node10 = list_to_atom("node_10@" ++ Hostname),
    Node11 = list_to_atom("node_11@" ++ Hostname),
    Node12 = list_to_atom("node_12@" ++ Hostname),
    Node13 = list_to_atom("node_13@" ++ Hostname),
    Node14 = list_to_atom("node_14@" ++ Hostname),
    Node15 = list_to_atom("node_15@" ++ Hostname),

    leo_redundant_manager_api:attach(Node0, "R1"),
    leo_redundant_manager_api:attach(Node1, "R1"),
    leo_redundant_manager_api:attach(Node2, "R1"),
    leo_redundant_manager_api:attach(Node3, "R1"),
    leo_redundant_manager_api:attach(Node4, "R1"),
    leo_redundant_manager_api:attach(Node5, "R1"),
    leo_redundant_manager_api:attach(Node6, "R1"),
    leo_redundant_manager_api:attach(Node7, "R1"),

    leo_redundant_manager_api:attach(Node8,  "R2"),
    leo_redundant_manager_api:attach(Node9,  "R2"),
    leo_redundant_manager_api:attach(Node10, "R2"),
    leo_redundant_manager_api:attach(Node11, "R2"),
    leo_redundant_manager_api:attach(Node12, "R2"),
    leo_redundant_manager_api:attach(Node13, "R2"),
    leo_redundant_manager_api:attach(Node14, "R2"),
    leo_redundant_manager_api:attach(Node15, "R2"),

    {ok, _, _} = leo_redundant_manager_api:create(?VER_CUR),
    {ok, _, _} = leo_redundant_manager_api:create(?VER_PREV),
    timer:sleep(1000),

    ok = leo_redundant_manager_worker:force_sync(?RING_TBL_CUR),
    ok = leo_redundant_manager_worker:force_sync(?RING_TBL_PREV),
    ?debugVal(leo_redundant_manager_worker:checksum()),

    lists:foreach(
      fun(N) ->
              {ok, #redundancies{nodes = Nodes}} =
                  leo_redundant_manager_api:get_redundancies_by_key(
                    lists:append(["LEOFS_", integer_to_list(N)])),
              ?assertEqual(5, length(Nodes))
      end, lists:seq(1, 300)),
    ok.

%% -------------------------------------------------------------------
%% INNER FUNCTION
%% -------------------------------------------------------------------
prepare(Hostname, ServerType) ->
    prepare(Hostname, ServerType, 8).

prepare(Hostname, ServerType, NumOfNodes) ->
    catch ets:delete(?MEMBER_TBL_CUR),
    catch ets:delete(?MEMBER_TBL_PREV),
    catch ets:delete('leo_ring_cur'),
    catch ets:delete('leo_ring_prv'),

    {ok, _RefSup} = leo_redundant_manager_sup:start_link(ServerType),
    case ServerType of
        master ->
            leo_cluster_tbl_ring:create_table_current(ram_copies, [node()]),
            leo_cluster_tbl_ring:create_table_prev(ram_copies, [node()]);
        _ ->
            void
    end,

    leo_redundant_manager_api:set_options([{n, 3},
                                           {r, 1},
                                           {w ,2},
                                           {d, 2},
                                           {bit_of_ring, 128}]),

    leo_redundant_manager_api:attach(list_to_atom("node_0@" ++ Hostname)),
    leo_redundant_manager_api:attach(list_to_atom("node_1@" ++ Hostname)),
    leo_redundant_manager_api:attach(list_to_atom("node_2@" ++ Hostname)),
    case NumOfNodes of
        8 ->
            leo_redundant_manager_api:attach(list_to_atom("node_3@" ++ Hostname)),
            leo_redundant_manager_api:attach(list_to_atom("node_4@" ++ Hostname)),
            leo_redundant_manager_api:attach(list_to_atom("node_5@" ++ Hostname)),
            leo_redundant_manager_api:attach(list_to_atom("node_6@" ++ Hostname)),
            leo_redundant_manager_api:attach(list_to_atom("node_7@" ++ Hostname));
        _ ->
            void
    end,
    ok.


inspect_0(Hostname, NumOfIteration) ->
    %% member-related.
    {ok, Members0} = leo_redundant_manager_api:get_members(),
    {ok, {MembersCur, MembersPrev}} = leo_redundant_manager_api:get_all_ver_members(),

    ?assertEqual(8, length(Members0)),
    ?assertEqual(8, length(MembersCur)),
    ?assertEqual(8, length(MembersPrev)),
    ?assertEqual(true,  leo_redundant_manager_api:has_member(list_to_atom("node_3@" ++ Hostname))),
    ?assertEqual(false, leo_redundant_manager_api:has_member(list_to_atom("node_8@" ++ Hostname))),

    {ok, Node5} = leo_redundant_manager_api:get_member_by_node(list_to_atom("node_5@" ++ Hostname)),
    ?assertEqual(list_to_atom("node_5@" ++ Hostname), Node5#member.node),

    ok = leo_redundant_manager_api:update_member_by_node(list_to_atom("node_7@" ++ Hostname), 12345, 'suspend'),

    {ok, Node7} = leo_redundant_manager_api:get_member_by_node(list_to_atom("node_7@" ++ Hostname)),
    ?assertEqual(12345,     Node7#member.clock),
    ?assertEqual('suspend', Node7#member.state),

    %% create routing-table > confirmations.
    ?debugVal(leo_redundant_manager_api:get_options()),
    {ok, Members1, Chksums} = leo_redundant_manager_api:create(),
    timer:sleep(3000),

    {ok, {Chksum0, _Chksum1}} = leo_redundant_manager_api:checksum(?CHECKSUM_RING),
    ?debugVal({Chksum0, _Chksum1}),
    ?debugVal(leo_redundant_manager_worker:checksum()),
    %% ?assertEqual(true, (Chksum0 > -1)),

    ?assertEqual(8, length(Members1)),
    ?assertEqual(2, length(Chksums)),

    {ok, {Chksum2, Chksum3}} = leo_redundant_manager_api:checksum(?CHECKSUM_RING),
    {ok, {Chksum4, Chksum5}} = leo_redundant_manager_api:checksum(member),
    leo_misc:set_env(?APP, ?PROP_RING_HASH, Chksum2),

    ?assertEqual(true, (-1 =< Chksum2)),
    ?assertEqual(true, (-1 =< Chksum3)),
    ?assertEqual(true, (-1 =< Chksum4)),
    ?assertEqual(true, (-1 =< Chksum5)),

    ?assertEqual((8 * ?DEF_NUMBER_OF_VNODES), leo_cluster_tbl_ring:size({ets, ?RING_TBL_CUR} )),
    ?assertEqual((8 * ?DEF_NUMBER_OF_VNODES), leo_cluster_tbl_ring:size({ets, ?RING_TBL_PREV})),

    timer:sleep(1000),
    ?debugVal(leo_redundant_manager_worker:checksum()),

    ok = inspect_redundancies_1(NumOfIteration),
    ok = inspect_redundancies_2(NumOfIteration),

    {ok, #redundancies{nodes = N0}} = leo_redundant_manager_api:get_redundancies_by_addr_id(put, 0),
    {ok, #redundancies{nodes = N1}} = leo_redundant_manager_api:get_redundancies_by_addr_id(put, leo_math:power(2, 128)),
    ?assertEqual(3, length(N0)),
    ?assertEqual(3, length(N1)),

    Max = leo_math:power(2, ?MD5),
    {ok, #redundancies{id = Id,
                       vnode_id_to = VNodeId1,
                       nodes = Nodes1,
                       n = 3,
                       r = 1,
                       w = 2,
                       d = 2}} = leo_redundant_manager_api:get_redundancies_by_addr_id(put, Max + 1),
    ?assertEqual(true, (Id > VNodeId1)),
    ?assertEqual(3, length(Nodes1)),

    lists:foreach(fun(_) ->
                          Id2 = random:uniform(Max),
                          {ok, Res2} = leo_redundant_manager_api:range_of_vnodes(Id2),
                          inspect_1(Id2, Res2)
                  end, lists:seq(0, 300)),
    {ok, Res3} = leo_redundant_manager_api:range_of_vnodes(0),
    inspect_1(0, Res3),

    Max1 = leo_math:power(2,128) - 1,
    {ok, Res4} = leo_redundant_manager_api:range_of_vnodes(Max1),
    ?assertEqual(2, length(Res4)),

    {ok, {_Options, Res6}} =
        leo_redundant_manager_api:collect_redundancies_by_key(<<"air_on_the_g_string_2">>, 6, 3),
    ?debugVal(Res6),
    ?assertEqual(6, length(Res6)),

    {ok, Res7} =
        leo_redundant_manager_api:part_of_collect_redundancies_by_key(
          3, <<"air_on_the_g_string_2\n3">>, 6, 3),
    ?debugVal(Res7),
    Res7_1 = lists:nth(3, Res6),
    ?assertEqual(Res7, Res7_1),

    ok = leo_redundant_manager_api:dump(work),
    ok.


%% @private
inspect_redundancies_1(0) ->
    ok;
inspect_redundancies_1(Counter) ->
    case leo_redundant_manager_api:get_redundancies_by_key(integer_to_list(Counter)) of
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
    } = leo_redundant_manager_api:get_redundancies_by_addr_id(put, Id),
    lists:foreach(fun(A) ->
                          Nodes0a = lists:delete(A, Nodes0),
                          lists:foreach(fun(B) ->
                                                ?assertEqual(false, (A == B))
                                        end, Nodes0a)
                  end, Nodes0),
    ?assertEqual(3, length(Nodes0)),
    inspect_redundancies_2(Counter - 1).


%% @private
inspect_1(Id, VNodes) ->
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


redundant_test_() ->
    {timeout, 300, ?_assertEqual(ok, redundant(false))}.

redundant(false) ->
    ok;
redundant(true) ->
    %% prepare-1
    [] = os:cmd("epmd -daemon"),
    {ok, Hostname} = inet:gethostname(),

    leo_misc:init_env(),
    leo_misc:set_env(?APP, ?PROP_SERVER_TYPE, ?MONITOR_NODE),
    leo_cluster_tbl_member:create_table(?MEMBER_TBL_CUR),
    leo_cluster_tbl_member:create_table(?MEMBER_TBL_PREV),

    %% prepare-2
    {ok, _RefSup} = leo_redundant_manager_sup:start_link(master),
    leo_redundant_manager_api:set_options([{n, 3},
                                           {r, 1},
                                           {w ,2},
                                           {d, 2},
                                           {bit_of_ring, 128},
                                           {level_2, 0}
                                          ]),
    Node0  = list_to_atom("node_0@"  ++ Hostname),
    Node1  = list_to_atom("node_1@"  ++ Hostname),
    Node2  = list_to_atom("node_2@"  ++ Hostname),
    Node3  = list_to_atom("node_3@"  ++ Hostname),
    Node4  = list_to_atom("node_4@"  ++ Hostname),
    Node5  = list_to_atom("node_5@"  ++ Hostname),
    Node6  = list_to_atom("node_6@"  ++ Hostname),
    Node7  = list_to_atom("node_7@"  ++ Hostname),
    Members = [Node0, Node1, Node2, Node3,
               Node4, Node5, Node6, Node7],

    leo_redundant_manager_api:attach(Node0),
    leo_redundant_manager_api:attach(Node1),
    leo_redundant_manager_api:attach(Node2),
    leo_redundant_manager_api:attach(Node3),
    leo_redundant_manager_api:attach(Node4),
    leo_redundant_manager_api:attach(Node5),
    leo_redundant_manager_api:attach(Node6),
    leo_redundant_manager_api:attach(Node7),
    {ok, _, _} = leo_redundant_manager_api:create(?VER_CUR),
    {ok, _, _} = leo_redundant_manager_api:create(?VER_PREV),
    timer:sleep(1000),

    %% execute
    ok = redundant_1(Members,       0,  250000),
    ok = redundant_1(Members,  250001,  500000),
    ok = redundant_1(Members,  500001,  750000),
    ok = redundant_1(Members,  750001, 1000000),
    ok = redundant_1(Members, 1000001, 1250000),
    ok = redundant_1(Members, 1250001, 1500000),
    ok = redundant_1(Members, 1500001, 1750000),
    ok = redundant_1(Members, 1750001, 2000000),
    ok = redundant_1(Members, 2000001, 2250000),
    ok = redundant_1(Members, 2250001, 2500000),
    ok = redundant_1(Members, 2500001, 2750000),
    ok = redundant_1(Members, 2750001, 3000000),

    %% terminate
    timer:sleep(200),
    os:cmd("rm -rf queue"),
    os:cmd("rm ring_*"),
    ok.


redundant_1(_, End, End) ->
    ?debugVal({done, End}),
    ok;
redundant_1(Members, St, End) ->
    case leo_redundant_manager_api:get_redundancies_by_key(
           lists:append(["LEOFS_", integer_to_list(St)])) of
        {ok, #redundancies{nodes = Nodes}} ->
            ?assertEqual(3, length(Nodes)),
            lists:foreach(fun(#redundant_node{node = N}) ->
                                  ?assertEqual(true, lists:member(N, Members))
                          end, Nodes);
        _Error ->
            ?debugVal(_Error)
    end,
    redundant_1(Members, St+1, End).


redundant_manager_2_test_() ->
    {timeout, 60000, ?_assertEqual(ok, begin
                                           long_run_1()
                                       end)}.
redundant_manager_3_test_() ->
    {timeout, 60000, ?_assertEqual(ok, begin
                                           long_run_2()
                                       end)}.
redundant_manager_4_test_() ->
    {timeout, 60000, ?_assertEqual(ok, begin
                                           long_run_3()
                                       end)}.

redundant_manager_5_test_() ->
    {timeout, 60000, ?_assertEqual(ok, begin
                                           attach_to_detach()
                                       end)}.

redundant_manager_6_test_() ->
    {timeout, 60000, ?_assertEqual(ok, begin
                                           attach_to_attach()
                                       end)}.

redundant_manager_7_test_() ->
    {timeout, 60000, ?_assertEqual(ok, begin
                                           attach_and_detach()
                                       end)}.

redundant_manager_8_test_() ->
    {timeout, 60000, ?_assertEqual(ok, begin
                                           attach_and_attach()
                                       end)}.

redundant_manager_9_test_() ->
    {timeout, 60000, ?_assertEqual(ok, begin
                                           detach_after_attach_same_node()
                                       end)}.



%% -define(NUM_OF_RECURSIVE_CALLS, 100).
-define(NUM_OF_RECURSIVE_CALLS, 1000).

long_run_1() ->
    %% prepare
    {Hostname} = setup(),

    ok = prepare(Hostname, gateway),
    {ok, _, _} = leo_redundant_manager_api:create(?VER_CUR),
    {ok, _, _} = leo_redundant_manager_api:create(?VER_PREV),
    timer:sleep(1000),

    Size_1 = leo_cluster_tbl_ring:size({ets, ?RING_TBL_CUR}),
    ?assertEqual((8 * ?DEF_NUMBER_OF_VNODES), Size_1),
    timer:sleep(100),

    %% rebalance.attach
    AttachNode = list_to_atom("node_8@" ++ Hostname),
    ok = leo_redundant_manager_api:attach(AttachNode),

    timer:sleep(100),
    {ok, Res1} = leo_redundant_manager_api:rebalance(),
    ?assertEqual(true, Res1 =/= []),
    SrcNodes = lists:sort(
                 lists:foldl(fun(Item, Acc) ->
                                     Src = proplists:get_value('src',  Item),
                                     case lists:member(Src, Acc) of
                                         true  -> Acc;
                                         false -> [Src|Acc]
                                     end
                             end, [], Res1)),
    ?debugVal(SrcNodes),
    ?assertEqual(8, length(SrcNodes)),

    %% retrieve redundancies
    timer:sleep(1000),
    attach_1_1(?NUM_OF_RECURSIVE_CALLS),
    ok.

long_run_2() ->
    %% prepare
    {Hostname} = setup(),

    ok = prepare(Hostname, gateway),
    {ok, _, _} = leo_redundant_manager_api:create(?VER_CUR),
    {ok, _, _} = leo_redundant_manager_api:create(?VER_PREV),
    timer:sleep(1000),

    Size_1 = leo_cluster_tbl_ring:size({ets, ?RING_TBL_CUR}),
    ?assertEqual((8 * ?DEF_NUMBER_OF_VNODES), Size_1),
    timer:sleep(100),

    %% rebalance.attach
    AttachedNodes = [list_to_atom("node_8@"  ++ Hostname),
                     list_to_atom("node_9@"  ++ Hostname),
                     list_to_atom("node_10@" ++ Hostname)],
    lists:foreach(fun(_N) ->
                          ok = leo_redundant_manager_api:attach(_N)
                  end, AttachedNodes),

    timer:sleep(1000),
    {ok, Res1} = leo_redundant_manager_api:rebalance(),
    ?assertEqual(true, Res1 =/= []),
    SrcNodes = lists:sort(
                 lists:foldl(fun(Item, Acc) ->
                                     Src = proplists:get_value('src',  Item),
                                     case lists:member(Src, Acc) of
                                         true  -> Acc;
                                         false -> [Src|Acc]
                                     end
                             end, [], Res1)),
    ?debugVal(SrcNodes),
    ?assertEqual(8, length(SrcNodes)),

    %% retrieve redundancies
    timer:sleep(1000),
    attach_2_1(?NUM_OF_RECURSIVE_CALLS),
    ok.

long_run_3() ->
    %% prepare
    {Hostname} = setup(),

    ok = prepare(Hostname, gateway),
    {ok, _, _} = leo_redundant_manager_api:create(?VER_CUR),
    {ok, _, _} = leo_redundant_manager_api:create(?VER_PREV),
    timer:sleep(1000),

    %% rebalance.detach
    DetachNode = list_to_atom("node_0@" ++ Hostname),
    ok = leo_redundant_manager_api:detach(DetachNode),

    timer:sleep(100),
    {ok, Res1} = leo_redundant_manager_api:rebalance(),
    ?assertEqual(true, Res1 =/= []),
    SrcNodes = lists:sort(
                 lists:foldl(fun(Item, Acc) ->
                                     Src = proplists:get_value('src',  Item),
                                     case lists:member(Src, Acc) of
                                         true  -> Acc;
                                         false -> [Src|Acc]
                                     end
                             end, [], Res1)),
    ?debugVal(SrcNodes),
    ?assertEqual(7, length(SrcNodes)),

    %% retrieve redundancies
    timer:sleep(1000),
    detach_1_1(?NUM_OF_RECURSIVE_CALLS),
    ok.


attach_to_detach() ->
    %% prepare
    {Hostname} = setup(),

    ok = prepare(Hostname, gateway),
    {ok, _, _} = leo_redundant_manager_api:create(),
    timer:sleep(100),

    %% attach > rebalance
    AttachNode = list_to_atom("node_8@" ++ Hostname),
    ok = leo_redundant_manager_api:attach(AttachNode),
    {ok, Res1} = leo_redundant_manager_api:rebalance(),
    ?assertEqual(true, Res1 =/= []),

    ok = leo_redundant_manager_api:update_member_by_node(
           AttachNode, leo_date:clock(), ?STATE_RUNNING),

    {ok, {RingHashCur_1, RingHashPrev_1}} = leo_redundant_manager_api:checksum(ring),
    ?assertNotEqual(RingHashCur_1, RingHashPrev_1),

    {ok, _}= leo_redundant_manager_api:get_member_by_node(AttachNode),

    DetachNode = list_to_atom("node_0@" ++ Hostname),
    ok = leo_redundant_manager_api:detach(DetachNode),

    {ok, Res2} = leo_redundant_manager_api:rebalance(),
    ?assertEqual(true, Res2 =/= []),

    SrcNodes = lists:sort(
                 lists:foldl(fun(Item, Acc) ->
                                     Src = proplists:get_value('src',  Item),
                                     case lists:member(Src, Acc) of
                                         true  -> Acc;
                                         false -> [Src|Acc]
                                     end
                             end, [], Res2)),
    ?debugVal(SrcNodes),
    ?assertEqual(8, length(SrcNodes)),

    %% {ok, {RingHashCur_2, RingHashPrev_2}} = leo_redundant_manager_api:checksum(ring),
    %% ?assertEqual(RingHashCur_2, RingHashPrev_2),

    {error, not_found} = leo_redundant_manager_api:get_member_by_node(DetachNode),

    %% retrieve redundancies
    detach_1_1(?NUM_OF_RECURSIVE_CALLS),
    ok.

attach_to_attach() ->
    %% prepare
    os:cmd("rm -rf ./log/ring/*"),
    {Hostname} = setup(),

    ok = prepare(Hostname, gateway),
    {ok, _, _} = leo_redundant_manager_api:create(),
    timer:sleep(1000),

    %% attach#1 > rebalance
    AttachNode_1 = list_to_atom("node_8@" ++ Hostname),
    ok = leo_redundant_manager_api:attach(AttachNode_1),
    {ok, Res1} = leo_redundant_manager_api:rebalance(),
    ?assertEqual(true, Res1 =/= []),

    ok = leo_redundant_manager_api:update_member_by_node(
           AttachNode_1, leo_date:clock(), ?STATE_RUNNING),

    {ok, {RingHashCur_1, RingHashPrev_1}} = leo_redundant_manager_api:checksum(ring),
    ?assertNotEqual(RingHashCur_1, RingHashPrev_1),

    {ok, _}= leo_redundant_manager_api:get_member_by_node(AttachNode_1),
    timer:sleep(1000),

    %% attach#2 > rebalance
    AttachNode_2 = list_to_atom("node_9@" ++ Hostname),
    ok = leo_redundant_manager_api:attach(AttachNode_2),
    {ok, Res2} = leo_redundant_manager_api:rebalance(),
    ?assertEqual(true, Res2 =/= []),

    SrcNodes = lists:sort(
                 lists:foldl(fun(Item, Acc) ->
                                     Src  = proplists:get_value('src', Item),
                                     Dest = proplists:get_value('dest',Item),
                                     case Src of
                                         AttachNode_1 when Dest == AttachNode_2 ->
                                             %% ?debugVal({Src, Dest});
                                             ok;
                                         _ ->
                                             void
                                     end,

                                     case lists:member(Src, Acc) of
                                         true  -> Acc;
                                         false -> [Src|Acc]
                                     end
                             end, [], Res2)),
    ?debugVal(SrcNodes),
    ?assertEqual(9, length(SrcNodes)),

    ok = leo_redundant_manager_api:update_member_by_node(
           AttachNode_2, leo_date:clock(), ?STATE_RUNNING),

    {ok, {RingHashCur_2, RingHashPrev_2}} = leo_redundant_manager_api:checksum(ring),
    ?assertNotEqual(RingHashCur_2, RingHashPrev_2),

    {ok, _}= leo_redundant_manager_api:get_member_by_node(AttachNode_2),
    {ok, Members} = leo_redundant_manager_api:get_members(),
    ?assertEqual(10, length(Members)),

    RingSize = leo_cluster_tbl_ring:size({ets, ?RING_TBL_CUR}),
    ?assertEqual((10 * ?DEF_NUMBER_OF_VNODES), RingSize),

    %% retrieve redundancies
    timer:sleep(1000),
    attach_1_1(?NUM_OF_RECURSIVE_CALLS),
    ok.

attach_and_detach() ->
    %% prepare
    {Hostname} = setup(),

    ok = prepare(Hostname, gateway),
    {ok, _, _} = leo_redundant_manager_api:create(),
    timer:sleep(1000),

    %% attach and detach > rebalance
    AttachNode = list_to_atom("node_8@" ++ Hostname),
    DetachNode = list_to_atom("node_0@" ++ Hostname),

    ok = leo_redundant_manager_api:attach(AttachNode),
    ok = leo_redundant_manager_api:detach(DetachNode),

    {ok, Res1} = leo_redundant_manager_api:rebalance(),
    ?assertEqual(true, Res1 =/= []),

    SrcNodes = lists:sort(
                 lists:foldl(fun(Item, Acc) ->
                                     Src = proplists:get_value('src',  Item),
                                     case lists:member(Src, Acc) of
                                         true  -> Acc;
                                         false -> [Src|Acc]
                                     end
                             end, [], Res1)),
    ?debugVal(SrcNodes),
    ?assertEqual(7, length(SrcNodes)),

    lists:foreach(fun(Item) ->
                          Src  = proplists:get_value('src',  Item),
                          Dest = proplists:get_value('dest', Item),
                          ?assertEqual(true, Src  /= Dest),
                          ?assertEqual(true, Src  /= DetachNode),
                          ?assertEqual(true, Dest /= DetachNode),
                          ?assertEqual(true, Src  /= AttachNode),
                          ?assertEqual(true, Dest == AttachNode)
                  end, Res1),

    %% retrieve redundancies
    timer:sleep(500),
    ok = leo_redundant_manager_api:update_member_by_node(
           AttachNode, leo_date:clock(), ?STATE_RUNNING),

    timer:sleep(500),
    leo_redundant_manager_api:dump(both),
    detach_1_1(?NUM_OF_RECURSIVE_CALLS),
    ok.

attach_and_attach() ->
    %% prepare
    {Hostname} = setup(),

    ok = prepare(Hostname, gateway, 3),
    {ok, _, _} = leo_redundant_manager_api:create(),
    timer:sleep(1000),

    %% attach and detach > rebalance
    AttachNode_1 = list_to_atom("node_8@" ++ Hostname),
    AttachNode_2 = list_to_atom("node_9@" ++ Hostname),

    ok = leo_redundant_manager_api:attach(AttachNode_1),
    ok = leo_redundant_manager_api:attach(AttachNode_2),

    {ok, Res1} = leo_redundant_manager_api:rebalance(),
    ?assertEqual(true, Res1 =/= []),

    SrcNodes = lists:sort(
                 lists:foldl(fun(Item, Acc) ->
                                     Src  = proplists:get_value('src', Item),
                                     case lists:member(Src, Acc) of
                                         true  -> Acc;
                                         false -> [Src|Acc]
                                     end
                             end, [], Res1)),
    ?debugVal(SrcNodes),
    ?assertEqual(3, length(SrcNodes)),

    lists:foreach(fun(Item) ->
                          Src  = proplists:get_value('src',  Item),
                          Dest = proplists:get_value('dest', Item),
                          ?assertEqual(true, Src  /= Dest),
                          ?assertEqual(true, Src  /= AttachNode_1),
                          ?assertEqual(true, Src  /= AttachNode_2),
                          ?assertEqual(true, (Dest == AttachNode_1 orelse
                                              Dest == AttachNode_2))
                  end, Res1),

    %% retrieve redundancies
    timer:sleep(500),
    ok = leo_redundant_manager_api:update_member_by_node(
           AttachNode_1, leo_date:clock(), ?STATE_RUNNING),
    ok = leo_redundant_manager_api:update_member_by_node(
           AttachNode_2, leo_date:clock(), ?STATE_RUNNING),

    timer:sleep(500),
    leo_redundant_manager_api:dump(both),
    attach_2_1(?NUM_OF_RECURSIVE_CALLS),
    ok.


detach_after_attach_same_node() ->
    %% prepare
    {Hostname} = setup(),

    ok = prepare(Hostname, gateway),
    {ok, _, _} = leo_redundant_manager_api:create(),
    timer:sleep(1000),

    %% detach > rebalance
    DetachNode = list_to_atom("node_0@" ++ Hostname),
    ok = leo_redundant_manager_api:detach(DetachNode),
    {ok, Res2} = leo_redundant_manager_api:rebalance(),
    ?assertEqual(true, Res2 =/= []),

    %% attach > rebalance
    AttachNode = list_to_atom("node_0@" ++ Hostname),
    ok = leo_redundant_manager_api:attach(AttachNode),

    timer:sleep(5000),
    {ok, Res1} = leo_redundant_manager_api:rebalance(),
    ?assertEqual(true, Res1 =/= []),

    SrcNodes = lists:sort(
                 lists:foldl(fun(Item, Acc) ->
                                     Src = proplists:get_value('src',  Item),
                                     case lists:member(Src, Acc) of
                                         true  -> Acc;
                                         false -> [Src|Acc]
                                     end
                             end, [], Res1)),
    ?debugVal(SrcNodes),
    ?assertEqual(7, length(SrcNodes)),

    lists:foreach(fun(Item) ->
                          Src  = proplists:get_value('src',  Item),
                          Dest = proplists:get_value('dest', Item),
                          ?assertEqual(true, Src  /= Dest),
                          ?assertEqual(true, Src  /= AttachNode),
                          ?assertEqual(true, Dest == AttachNode)
                  end, Res1),

    ok = leo_redundant_manager_api:update_member_by_node(
           AttachNode, leo_date:clock(), ?STATE_RUNNING),

    {ok, {RingHashCur, RingHashPrev}} = leo_redundant_manager_api:checksum(ring),
    ?assertNotEqual(RingHashCur, RingHashPrev),

    %% retrieve redundancies
    timer:sleep(500),
    ok = leo_redundant_manager_api:update_member_by_node(
           AttachNode, leo_date:clock(), ?STATE_RUNNING),

    timer:sleep(500),
    attach_1_1(?NUM_OF_RECURSIVE_CALLS),
    ok.

-endif.
