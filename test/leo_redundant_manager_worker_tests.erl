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
-module(leo_redundant_manager_worker_tests).
-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).


suite_test_() ->
    {setup,
     fun () ->
             setup()
     end,
     fun (Pid) ->
             teardown(Pid)
     end,
     [{"test all functions",
       {timeout, 300, fun suite/0}}
     ]}.

setup() ->
    application:start(crypto),

    catch ets:delete_all_objects(?MEMBER_TBL_CUR),
    catch ets:delete_all_objects(?MEMBER_TBL_PREV),
    catch ets:delete_all_objects('leo_ring_cur'),
    catch ets:delete_all_objects('leo_ring_prv'),

    leo_misc:init_env(),
    leo_misc:set_env(leo_redundant_manager, 'server_type', ?WORKER_NODE),
    {ok, Pid} = leo_redundant_manager_sup:start_link(?WORKER_NODE),
    leo_redundant_manager_api:set_options(
      [{n, 3},{r, 1}, {w ,2},{d, 2},{bit_of_ring, 128},{level_2, 0}]),
    leo_redundant_manager_api:attach('node_0@127.0.0.1'),
    leo_redundant_manager_api:attach('node_1@127.0.0.1'),
    leo_redundant_manager_api:attach('node_2@127.0.0.1'),
    leo_redundant_manager_api:attach('node_3@127.0.0.1'),
    leo_redundant_manager_api:attach('node_4@127.0.0.1'),

    ?debugVal(leo_redundant_manager_api:get_options()),

    leo_redundant_manager_api:create(?VER_CUR),
    leo_redundant_manager_api:create(?VER_PREV),

    CurRows_2  = leo_cluster_tbl_ring:tab2list({?DB_ETS, 'leo_ring_cur'}),
    PrevRows_2 = leo_cluster_tbl_ring:tab2list({?DB_ETS, 'leo_ring_cur'}),
    ?assertEqual((?DEF_NUMBER_OF_VNODES * 5), length(CurRows_2)),
    ?assertEqual((?DEF_NUMBER_OF_VNODES * 5), length(PrevRows_2)),

    timer:sleep(1000),
    Pid.

teardown(Pid) ->
    timer:sleep(100),
    exit(Pid, normal),
    application:stop(crypto),
    ok.

suite() ->
    ?debugVal("=== START - Get Redundancies ==="),
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

    Size1 = collect_redundancies('leo_ring_cur', 1, VNodeIdTo1 + 1, []),
    Size2 = collect_redundancies('leo_ring_prv', 1, VNodeIdTo2 + 1, []),
    ?assertEqual((?DEF_NUMBER_OF_VNODES * 5), Size1),
    ?assertEqual((?DEF_NUMBER_OF_VNODES * 5), Size2),

    ?assertEqual(3, length(N0)),
    ?assertEqual(3, length(N1)),
    ?assertEqual(3, length(N2)),
    ?assertEqual(3, length(N3)),
    ?assertEqual(3, length(N4)),
    ?assertEqual(3, length(N5)),
    ?assertEqual(3, length(N6)),
    ?assertEqual(3, length(N7)),

    St = leo_date:clock(),
    ok = check_redundancies(100000),
    End = leo_date:clock(),

    ?debugVal((End - St) / 1000),
    ?debugVal("=== END - Get Redundancies ==="),
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
    AddrId = leo_redundant_manager_chash:vnode_id(128, crypto:rand_bytes(64)),
    {ok, #redundancies{nodes = N8}} =
        leo_redundant_manager_worker:lookup('leo_ring_cur', AddrId),
    ?assertEqual(3, length(N8)),
    check_redundancies(Index - 1).

-endif.
