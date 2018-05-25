%%======================================================================
%%
%% Leo Redundant Manager
%%
%% Copyright (c) 2012-2018 Rakuten, Inc.
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
-module(leo_redundant_manager_worker_tests).
-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

-define(TWO_REPLICAS, 2).
-define(THREE_REPLICAS, 3).

-define(TWO_RA, 2).
-define(THREE_RA, 3).

-define(RACK_1, "R1").
-define(RACK_2, "R2").
-define(RACK_3, "R3").
-define(RACK_4, "R4").

-define(CONSISTENCY_LEVEL_OF_BASIC, [{n, ?THREE_REPLICAS},
                                     {r, 1}, {w ,2},{d, 2},
                                     {bit_of_ring, 128},
                                     {level_2, 0}]).
-define(CONSISTENCY_LEVEL_OF_RA_1, [{n, ?TWO_REPLICAS},
                                    {r, 1}, {w ,1},{d, 1},
                                    {bit_of_ring, 128},
                                    {level_2, ?TWO_RA}]).
-define(CONSISTENCY_LEVEL_OF_RA_2, [{n, ?THREE_REPLICAS},
                                    {r, 1}, {w ,2},{d, 2},
                                    {bit_of_ring, 128},
                                    {level_2, ?TWO_RA}]).
-define(CONSISTENCY_LEVEL_OF_RA_3, [{n, ?THREE_REPLICAS},
                                    {r, 1}, {w ,2},{d, 2},
                                    {bit_of_ring, 128},
                                    {level_2, ?THREE_RA}]).
-define(NUM_OF_CHECK_REDUNDANCIES, 10000).


%% @doc Basic Test
suite_basic_test_() ->
    {setup,
     fun () ->
             setup_basic()
     end,
     fun (Pid) ->
             teardown(Pid)
     end,
     [{"test all functions",
       {timeout, 300, fun suite_basic/0}}
     ]}.

setup_basic() ->
    application:start(crypto),

    catch ets:delete_all_objects(?MEMBER_TBL_CUR),
    catch ets:delete_all_objects(?MEMBER_TBL_PREV),
    catch ets:delete_all_objects('leo_ring_cur'),
    catch ets:delete_all_objects('leo_ring_prv'),

    leo_misc:init_env(),
    leo_misc:set_env(leo_redundant_manager, 'server_type', ?WORKER_NODE),
    {ok, Pid} = leo_redundant_manager_sup:start_link(?WORKER_NODE),
    leo_redundant_manager_api:set_options(?CONSISTENCY_LEVEL_OF_BASIC),
    leo_redundant_manager_api:attach('node_0@127.0.0.1'),
    leo_redundant_manager_api:attach('node_1@127.0.0.1'),
    leo_redundant_manager_api:attach('node_2@127.0.0.1'),
    leo_redundant_manager_api:attach('node_3@127.0.0.1'),
    leo_redundant_manager_api:attach('node_4@127.0.0.1'),

    leo_redundant_manager_api:create(?VER_CUR),
    leo_redundant_manager_api:create(?VER_PREV),

    CurRows_2  = leo_cluster_tbl_ring:tab2list({?DB_ETS, 'leo_ring_cur'}),
    PrevRows_2 = leo_cluster_tbl_ring:tab2list({?DB_ETS, 'leo_ring_cur'}),
    ?assertEqual((?DEF_NUMBER_OF_VNODES * 5), length(CurRows_2)),
    ?assertEqual((?DEF_NUMBER_OF_VNODES * 5), length(PrevRows_2)),

    timer:sleep(1000),
    Pid.

teardown(Pid) ->
    timer:sleep(timer:seconds(1)),
    exit(Pid, normal),
    leo_redundant_manager_sup:stop(),
    application:stop(crypto),
    timer:sleep(timer:seconds(5)),
    ok.

suite_basic() ->
    ?debugVal("=== START - Basic Test ==="),
    ?debugVal(leo_redundant_manager_worker:checksum()),

    {ok, SystemConf} = leo_redundant_manager_api:get_options(),
    TotalReplicas = leo_misc:get_value('n', SystemConf),


    {ok, #redundancies{vnode_id_from = 0,
                       vnode_id_to   = VNodeIdTo1,
                       nodes = N0}} = leo_redundant_manager_worker:first('leo_ring_cur'),
    {ok, #redundancies{vnode_id_from = 0,
                       vnode_id_to   = VNodeIdTo2,
                       nodes = N0}} = leo_redundant_manager_worker:first('leo_ring_prv'),
    {ok, #redundancies{nodes = N1}} = leo_redundant_manager_worker:lookup(
                                        'leo_ring_cur', 0),
    {ok, #redundancies{nodes = N2}} = leo_redundant_manager_worker:lookup(
                                        'leo_ring_cur', 1264314306571079495751037749109419166),
    {ok, #redundancies{nodes = N3}} = leo_redundant_manager_worker:lookup(
                                        'leo_ring_cur', 3088066518744027498382227205172020754),
    {ok, #redundancies{nodes = N4}} = leo_redundant_manager_worker:lookup(
                                        'leo_ring_cur', 4870818527799149765629747665733758595),
    {ok, #redundancies{nodes = N5}} = leo_redundant_manager_worker:lookup(
                                        'leo_ring_cur', 5257965865843856950061366315134191522),
    {ok, #redundancies{nodes = N6}} = leo_redundant_manager_worker:lookup(
                                        'leo_ring_cur', 340282366920938463463374607431768211456),
    {ok, #redundancies{nodes = N7}} = leo_redundant_manager_worker:last('leo_ring_cur'),

    Size1 = collect_redundancies('leo_ring_cur', 1, VNodeIdTo1 + 1, []),
    Size2 = collect_redundancies('leo_ring_prv', 1, VNodeIdTo2 + 1, []),
    ?assertEqual((?DEF_NUMBER_OF_VNODES * 5), Size1),
    ?assertEqual((?DEF_NUMBER_OF_VNODES * 5), Size2),

    ?assertEqual(TotalReplicas, length(N0)),
    ?assertEqual(TotalReplicas, length(N1)),
    ?assertEqual(TotalReplicas, length(N2)),
    ?assertEqual(TotalReplicas, length(N3)),
    ?assertEqual(TotalReplicas, length(N4)),
    ?assertEqual(TotalReplicas, length(N5)),
    ?assertEqual(TotalReplicas, length(N6)),
    ?assertEqual(TotalReplicas, length(N7)),

    St = leo_date:clock(),
    ok = check_redundancies(?NUM_OF_CHECK_REDUNDANCIES),
    End = leo_date:clock(),

    ?debugVal((End - St) / 1000),
    ?debugVal("=== END ==="),
    ok.


%% @doc Rack-awareness Replication Test
%%      - Case 1: N=2, RA=2, Total Number of Racks:2
suite_rack_awareness_1_test_() ->
    {setup,
     fun () ->
             setup_rack_awareness_1()
     end,
     fun (Pid) ->
             teardown(Pid)
     end,
     [{"test rack-awareness functions",
       {timeout, 30000, fun suite_rack_awareness/0}}
     ]}.

setup_rack_awareness_1() ->
    application:start(crypto),

    catch ets:delete_all_objects(?MEMBER_TBL_CUR),
    catch ets:delete_all_objects(?MEMBER_TBL_PREV),
    catch ets:delete_all_objects('leo_ring_cur'),
    catch ets:delete_all_objects('leo_ring_prv'),

    leo_misc:init_env(),
    leo_misc:set_env(leo_redundant_manager, 'server_type', ?WORKER_NODE),
    {ok, Pid} = leo_redundant_manager_sup:start_link(?WORKER_NODE),
    leo_redundant_manager_api:set_options(?CONSISTENCY_LEVEL_OF_RA_1),
    leo_redundant_manager_api:attach('node_0@127.0.0.1', ?RACK_1),
    leo_redundant_manager_api:attach('node_1@127.0.0.1', ?RACK_1),
    leo_redundant_manager_api:attach('node_2@127.0.0.1', ?RACK_1),
    leo_redundant_manager_api:attach('node_3@127.0.0.1', ?RACK_2),
    leo_redundant_manager_api:attach('node_4@127.0.0.1', ?RACK_2),
    ?debugVal(leo_redundant_manager_api:get_options()),

    leo_redundant_manager_api:create(?VER_CUR),
    leo_redundant_manager_api:create(?VER_PREV),

    CurRows_2  = leo_cluster_tbl_ring:tab2list({?DB_ETS, 'leo_ring_cur'}),
    PrevRows_2 = leo_cluster_tbl_ring:tab2list({?DB_ETS, 'leo_ring_cur'}),
    ?assertEqual((?DEF_NUMBER_OF_VNODES * 5), length(CurRows_2)),
    ?assertEqual((?DEF_NUMBER_OF_VNODES * 5), length(PrevRows_2)),

    timer:sleep(1000),
    Pid.


%% @doc Rack-awareness Replication Test
%%      - Case 2: N=3, RA=2, Total Number of Racks:2
suite_rack_awareness_2_test_() ->
    {setup,
     fun () ->
             setup_rack_awareness_2()
     end,
     fun (Pid) ->
             teardown(Pid)
     end,
     [{"test rack-awareness functions",
       {timeout, 30000, fun suite_rack_awareness/0}}
     ]}.

setup_rack_awareness_2() ->
    application:start(crypto),

    catch ets:delete_all_objects(?MEMBER_TBL_CUR),
    catch ets:delete_all_objects(?MEMBER_TBL_PREV),
    catch ets:delete_all_objects('leo_ring_cur'),
    catch ets:delete_all_objects('leo_ring_prv'),

    leo_misc:init_env(),
    leo_misc:set_env(leo_redundant_manager, 'server_type', ?WORKER_NODE),
    {ok, Pid} = leo_redundant_manager_sup:start_link(?WORKER_NODE),
    leo_redundant_manager_api:set_options(?CONSISTENCY_LEVEL_OF_RA_2),
    leo_redundant_manager_api:attach('node_0@127.0.0.1', ?RACK_1),
    leo_redundant_manager_api:attach('node_1@127.0.0.1', ?RACK_1),
    leo_redundant_manager_api:attach('node_2@127.0.0.1', ?RACK_1),
    leo_redundant_manager_api:attach('node_3@127.0.0.1', ?RACK_2),
    leo_redundant_manager_api:attach('node_4@127.0.0.1', ?RACK_2),
    ?debugVal(leo_redundant_manager_api:get_options()),

    leo_redundant_manager_api:create(?VER_CUR),
    leo_redundant_manager_api:create(?VER_PREV),

    CurRows_2  = leo_cluster_tbl_ring:tab2list({?DB_ETS, 'leo_ring_cur'}),
    PrevRows_2 = leo_cluster_tbl_ring:tab2list({?DB_ETS, 'leo_ring_cur'}),
    ?assertEqual((?DEF_NUMBER_OF_VNODES * 5), length(CurRows_2)),
    ?assertEqual((?DEF_NUMBER_OF_VNODES * 5), length(PrevRows_2)),

    timer:sleep(1000),
    Pid.


%% @doc Rack-awareness Replication Test
%%      - Case 3: N=3, RA=2, Total Number of Racks:4
suite_rack_awareness_3_test_() ->
    {setup,
     fun () ->
             setup_rack_awareness_3()
     end,
     fun (Pid) ->
             teardown(Pid)
     end,
     [{"test rack-awareness functions",
       {timeout, 30000, fun suite_rack_awareness/0}}
     ]}.

setup_rack_awareness_3() ->
    application:start(crypto),

    catch ets:delete_all_objects(?MEMBER_TBL_CUR),
    catch ets:delete_all_objects(?MEMBER_TBL_PREV),
    catch ets:delete_all_objects('leo_ring_cur'),
    catch ets:delete_all_objects('leo_ring_prv'),

    leo_misc:init_env(),
    leo_misc:set_env(leo_redundant_manager, 'server_type', ?WORKER_NODE),
    {ok, Pid} = leo_redundant_manager_sup:start_link(?WORKER_NODE),
    leo_redundant_manager_api:set_options(?CONSISTENCY_LEVEL_OF_RA_2),
    leo_redundant_manager_api:attach('node_0@127.0.0.1', ?RACK_1),
    leo_redundant_manager_api:attach('node_1@127.0.0.1', ?RACK_2),
    leo_redundant_manager_api:attach('node_2@127.0.0.1', ?RACK_3),
    leo_redundant_manager_api:attach('node_3@127.0.0.1', ?RACK_4),
    leo_redundant_manager_api:attach('node_4@127.0.0.1', ?RACK_1),
    leo_redundant_manager_api:attach('node_5@127.0.0.1', ?RACK_2),
    leo_redundant_manager_api:attach('node_6@127.0.0.1', ?RACK_3),
    leo_redundant_manager_api:attach('node_7@127.0.0.1', ?RACK_4),
    leo_redundant_manager_api:attach('node_8@127.0.0.1', ?RACK_1),
    leo_redundant_manager_api:attach('node_9@127.0.0.1', ?RACK_2),
    leo_redundant_manager_api:attach('node_10@127.0.0.1', ?RACK_3),
    leo_redundant_manager_api:attach('node_11@127.0.0.1', ?RACK_4),
    leo_redundant_manager_api:attach('node_12@127.0.0.1', ?RACK_1),
    leo_redundant_manager_api:attach('node_13@127.0.0.1', ?RACK_2),
    leo_redundant_manager_api:attach('node_14@127.0.0.1', ?RACK_3),

    ?debugVal(leo_redundant_manager_api:get_options()),

    leo_redundant_manager_api:create(?VER_CUR),
    leo_redundant_manager_api:create(?VER_PREV),

    CurRows_2  = leo_cluster_tbl_ring:tab2list({?DB_ETS, 'leo_ring_cur'}),
    PrevRows_2 = leo_cluster_tbl_ring:tab2list({?DB_ETS, 'leo_ring_cur'}),
    ?assertEqual((?DEF_NUMBER_OF_VNODES * 15), length(CurRows_2)),
    ?assertEqual((?DEF_NUMBER_OF_VNODES * 15), length(PrevRows_2)),

    timer:sleep(1000),
    Pid.

%% @doc Rack-awareness Replication Test
%%      - Case 4: N=3, RA=2, Total Number of Racks:4
suite_rack_awareness_4_test_() ->
    {setup,
     fun () ->
             setup_rack_awareness_4()
     end,
     fun (Pid) ->
             teardown(Pid)
     end,
     [{"test rack-awareness functions",
       {timeout, 30000, fun suite_rack_awareness/0}}
     ]}.

setup_rack_awareness_4() ->
    application:start(crypto),

    catch ets:delete_all_objects(?MEMBER_TBL_CUR),
    catch ets:delete_all_objects(?MEMBER_TBL_PREV),
    catch ets:delete_all_objects('leo_ring_cur'),
    catch ets:delete_all_objects('leo_ring_prv'),

    leo_misc:init_env(),
    leo_misc:set_env(leo_redundant_manager, 'server_type', ?WORKER_NODE),
    {ok, Pid} = leo_redundant_manager_sup:start_link(?WORKER_NODE),
    leo_redundant_manager_api:set_options(?CONSISTENCY_LEVEL_OF_RA_3),
    leo_redundant_manager_api:attach('node_0@127.0.0.1', ?RACK_1),
    leo_redundant_manager_api:attach('node_1@127.0.0.1', ?RACK_2),
    leo_redundant_manager_api:attach('node_2@127.0.0.1', ?RACK_3),
    leo_redundant_manager_api:attach('node_3@127.0.0.1', ?RACK_4),
    leo_redundant_manager_api:attach('node_4@127.0.0.1', ?RACK_1),
    leo_redundant_manager_api:attach('node_5@127.0.0.1', ?RACK_2),
    leo_redundant_manager_api:attach('node_6@127.0.0.1', ?RACK_3),
    leo_redundant_manager_api:attach('node_7@127.0.0.1', ?RACK_4),
    leo_redundant_manager_api:attach('node_8@127.0.0.1', ?RACK_1),
    leo_redundant_manager_api:attach('node_9@127.0.0.1', ?RACK_2),
    leo_redundant_manager_api:attach('node_10@127.0.0.1', ?RACK_3),
    leo_redundant_manager_api:attach('node_11@127.0.0.1', ?RACK_4),
    leo_redundant_manager_api:attach('node_12@127.0.0.1', ?RACK_1),
    leo_redundant_manager_api:attach('node_13@127.0.0.1', ?RACK_2),
    leo_redundant_manager_api:attach('node_14@127.0.0.1', ?RACK_3),

    ?debugVal(leo_redundant_manager_api:get_options()),

    leo_redundant_manager_api:create(?VER_CUR),
    leo_redundant_manager_api:create(?VER_PREV),

    CurRows_2  = leo_cluster_tbl_ring:tab2list({?DB_ETS, 'leo_ring_cur'}),
    PrevRows_2 = leo_cluster_tbl_ring:tab2list({?DB_ETS, 'leo_ring_cur'}),
    ?assertEqual((?DEF_NUMBER_OF_VNODES * 15), length(CurRows_2)),
    ?assertEqual((?DEF_NUMBER_OF_VNODES * 15), length(PrevRows_2)),

    timer:sleep(1000),
    Pid.


%% @private
suite_rack_awareness() ->
    {ok, SystemConf} = leo_redundant_manager_api:get_options(),
    TotalReplicas = leo_misc:get_value('n', SystemConf),
    NumOfRA = leo_misc:get_value('level_2', SystemConf),

    ?debugVal("=== START - Rack Awareness Replication Test: ==="),
    ?debugVal(TotalReplicas),
    ?debugVal(NumOfRA),

    ?debugVal(leo_redundant_manager_worker:checksum()),

    {ok, #redundancies{vnode_id_from = 0,
                       vnode_id_to   = VNodeIdTo1,
                       nodes = N0}} = leo_redundant_manager_worker:first('leo_ring_cur'),
    {ok, #redundancies{vnode_id_from = 0,
                       vnode_id_to   = VNodeIdTo2,
                       nodes = N0}} = leo_redundant_manager_worker:first('leo_ring_prv'),
    {ok, #redundancies{nodes = N1}} = leo_redundant_manager_worker:lookup(
                                        'leo_ring_cur', 0),
    {ok, #redundancies{nodes = N2}} = leo_redundant_manager_worker:lookup(
                                        'leo_ring_cur', 1264314306571079495751037749109419166),
    {ok, #redundancies{nodes = N3}} = leo_redundant_manager_worker:lookup(
                                        'leo_ring_cur', 3088066518744027498382227205172020754),
    {ok, #redundancies{nodes = N4}} = leo_redundant_manager_worker:lookup(
                                        'leo_ring_cur', 4870818527799149765629747665733758595),
    {ok, #redundancies{nodes = N5}} = leo_redundant_manager_worker:lookup(
                                        'leo_ring_cur', 5257965865843856950061366315134191522),
    {ok, #redundancies{nodes = N6}} = leo_redundant_manager_worker:lookup(
                                        'leo_ring_cur', 340282366920938463463374607431768211456),
    {ok, #redundancies{nodes = N7}} = leo_redundant_manager_worker:last('leo_ring_cur'),

    ?assertEqual(TotalReplicas, length(N0)),
    ?assertEqual(TotalReplicas, length(N1)),
    ?assertEqual(TotalReplicas, length(N2)),
    ?assertEqual(TotalReplicas, length(N3)),
    ?assertEqual(TotalReplicas, length(N4)),
    ?assertEqual(TotalReplicas, length(N5)),
    ?assertEqual(TotalReplicas, length(N6)),
    ?assertEqual(TotalReplicas, length(N7)),

    {ok, MemberL} = leo_cluster_tbl_member:find_all(),
    MemberLen = length(MemberL),

    Size1 = collect_redundancies('leo_ring_cur', 1, VNodeIdTo1 + 1, []),
    Size2 = collect_redundancies('leo_ring_prv', 1, VNodeIdTo2 + 1, []),
    ?assertEqual((?DEF_NUMBER_OF_VNODES * MemberLen), Size1),
    ?assertEqual((?DEF_NUMBER_OF_VNODES * MemberLen), Size2),

    NodeAndRackL = [{N, R} || #member{node = N,
                                      grp_level_2 = R} <- MemberL],

    St = leo_date:clock(),
    ok = check_redundancies_for_rack_awareness(
           ?NUM_OF_CHECK_REDUNDANCIES, TotalReplicas, NumOfRA, NodeAndRackL),
    End = leo_date:clock(),
    ?debugVal((End - St) / 1000),

    ?debugVal("=== END ==="),
    ok.


%% @private
collect_redundancies(_Tbl,0,_To,Acc) ->
    length(Acc);
collect_redundancies( Tbl,_From, To, Acc) ->
    {ok, Ret} = leo_redundant_manager_worker:lookup(Tbl, To),
    From = Ret#redundancies.vnode_id_from,
    To1  = Ret#redundancies.vnode_id_to + 1,
    collect_redundancies(Tbl, From, To1, [Ret|Acc]).


%% @private
check_redundancies(0) ->
    ok;
check_redundancies(Index) ->
    AddrId = leo_redundant_manager_chash:vnode_id(128, crypto:strong_rand_bytes(64)),
    {ok, #redundancies{nodes = ReplicaNodeL}} =
        leo_redundant_manager_worker:lookup('leo_ring_cur', AddrId),
    ?assertEqual(3, length(ReplicaNodeL)),
    check_redundancies(Index - 1).

%% @private
check_redundancies_for_rack_awareness(0,_,_,_) ->
    ok;
check_redundancies_for_rack_awareness(
  Index, TotalNumOfReplicas, NumOfRackAwarenesses, NodeAndRackL) ->
    AddrId = leo_redundant_manager_chash:vnode_id(128, crypto:strong_rand_bytes(64)),
    {ok, #redundancies{nodes = ReplicaNodeL}} =
        leo_redundant_manager_worker:lookup('leo_ring_cur', AddrId),
    ?assertEqual(TotalNumOfReplicas, length(ReplicaNodeL)),

    RackDict = lists:foldl(
                 fun(#redundant_node{node = N}, D) ->
                         case lists:keyfind(N, 1, NodeAndRackL) of
                             false ->
                                 D;
                             {_, RackId} ->
                                 dict:append(RackId, N, D)
                         end
                 end, dict:new(), ReplicaNodeL),
    ?assertEqual(true, (NumOfRackAwarenesses =< length(dict:to_list(RackDict)))),

    check_redundancies_for_rack_awareness(
      Index - 1, TotalNumOfReplicas, NumOfRackAwarenesses, NodeAndRackL).

-endif.
