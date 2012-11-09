%%======================================================================
%%
%% Leo Redundant Manager
%%
%% Copyright (c) 2012
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
-module(leo_redundant_manager_api_tests).
-author('yosuke hara').

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

redundant_manager_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [{with, [T]} || T <- [fun redundant_manager_0_/1,
                           fun redundant_manager_1_/1,
                           fun attach_/1,
                           fun detach_/1,

                           fun members_table_/1,
                           fun synchronize_0_/1,
                           fun synchronize_1_/1,
                           fun synchronize_2_/1,
                           fun adjust_/1,
                           fun append_/1,
                           fun suspend_/1
                          ]]}.


setup() ->
    [] = os:cmd("epmd -daemon"),
    {ok, Hostname} = inet:gethostname(),

    application:set_env(?APP, ?PROP_SERVER_TYPE, ?SERVER_MANAGER),
    application:start(mnesia),
    leo_redundant_manager_table_member:create_members(ram_copies),
    mnesia:wait_for_tables([members], 30000),
    {Hostname}.

teardown(_) ->
    application:stop(mnesia),
    application:stop(leo_mq),
    application:stop(leo_backend_db),
    application:stop(leo_redundant_manager),
    %% meck:unload(),

    os:cmd("rm -rf queue"),
    os:cmd("rm ring_*"),
    ok.

redundant_manager_0_({Hostname}) ->
    ok = prepare(Hostname, master),
    inspect0(Hostname),
    ok.

redundant_manager_1_({Hostname}) ->
    ok = prepare(Hostname, storage),
    inspect0(Hostname),
    ok.

attach_({Hostname}) ->
    ok = prepare(Hostname, gateway),
    {ok, _, _} = leo_redundant_manager_api:create(),

    %% rebalance.attach
    AttachNode = list_to_atom("node_8@" ++ Hostname),
    ok = leo_redundant_manager_api:attach(AttachNode),
    leo_redundant_manager_api:dump(?CHECKSUM_RING),
    {ok, Res} = leo_redundant_manager_api:rebalance(),
    lists:foreach(fun(Item) ->
                          Src  = proplists:get_value('src',  Item),
                          Dest = proplists:get_value('dest', Item),
                          ?assertEqual(true, Src =/= Dest),
                          ?assertEqual(true, Src =/= AttachNode),
                          ?assertEqual(AttachNode, Dest)
                  end, Res),
    ok.

detach_({Hostname}) ->
    ok = prepare(Hostname, gateway),
    {ok, _, _} = leo_redundant_manager_api:create(),

    %% rebalance.detach
    DetachNode = list_to_atom("node_0@" ++ Hostname),
    ok = leo_redundant_manager_api:detach(DetachNode),
    leo_redundant_manager_api:dump(?CHECKSUM_RING),
    {ok, Res} = leo_redundant_manager_api:rebalance(),
    lists:foreach(fun(Item) ->
                          Src  = proplists:get_value('src',  Item),
                          Dest = proplists:get_value('dest', Item),
                          ?assertEqual(true, Src  =/= Dest),
                          ?assertEqual(true, Src  =/= DetachNode),
                          ?assertEqual(true, Dest =/= DetachNode)
                  end, Res),
    ok.


-define(TEST_MEMBERS, [#member{node = 'node_0@127.0.0.1'},
                       #member{node = 'node_1@127.0.0.1'},
                       #member{node = 'node_2@127.0.0.1'}]).
members_table_(_Arg) ->
    %% create -> get -> not-found.
    not_found = leo_redundant_manager_table_member:find_all(),

    ?assertEqual(0, leo_redundant_manager_table_member:size()),
    ?assertEqual(not_found, leo_redundant_manager_table_member:lookup('node_0@127.0.0.1')),

    %% insert.
    lists:foreach(fun(Item) ->
                          ?assertEqual(ok, leo_redundant_manager_table_member:insert({Item#member.node, Item}))
                  end, ?TEST_MEMBERS),

    %% update.
    ok = leo_redundant_manager_table_member:insert({'node_1@127.0.0.1', #member{node  = 'node_1@127.0.0.1',
                                                                                clock = 12345,
                                                                                state = 'suspend'}}),

    %% get.
    {ok, Members} = leo_redundant_manager_table_member:find_all(),
    ?assertEqual(3, leo_redundant_manager_table_member:size()),
    ?assertEqual(3, length(Members)),

    ?assertEqual({ok, lists:nth(1,?TEST_MEMBERS)},
                 leo_redundant_manager_table_member:lookup('node_0@127.0.0.1')),

    %% delete.
    #member{node = Node} = lists:nth(1, ?TEST_MEMBERS),
    leo_redundant_manager_table_member:delete(Node),
    ?assertEqual(2, leo_redundant_manager_table_member:size()),
    ok.


synchronize_0_(_Arg) ->
    ok = leo_redundant_manager_api:start(),
    Options = [{n, 3},
               {r, 1},
               {w ,2},
               {d, 2},
               {bit_of_ring, 128}],
    leo_redundant_manager_table_ring:create_ring_current(ram_copies, [node()]),
    leo_redundant_manager_table_ring:create_ring_prev(ram_copies, [node()]),

    {ok, NewMember, Checksums} = leo_redundant_manager_api:synchronize(?SYNC_MODE_BOTH, ?TEST_MEMBERS, Options),
    {CurRing,PrevRing} = proplists:get_value(?CHECKSUM_RING,   Checksums),
    MemberChksum       = proplists:get_value(?CHECKSUM_MEMBER, Checksums),

    ?assertEqual(3, length(NewMember)),
    ?assertEqual(true, (CurRing  > -1)),
    ?assertEqual(true, (PrevRing > -1)),
    ?assertEqual(true, (CurRing == PrevRing)),
    ?assertEqual(true, (MemberChksum > -1)),
    ok.

synchronize_1_({Hostname}) ->
    ok = prepare(Hostname, storage),
    {ok,_,_} = leo_redundant_manager_api:create(),
    {ok, _Members} = leo_redundant_manager_api:get_members(),

    {ok, {CurRingHash0, PrevRingHash0}} = leo_redundant_manager_api:checksum(?CHECKSUM_RING),
    {ok, MemberChksum0} = leo_redundant_manager_api:checksum(?CHECKSUM_MEMBER),

    ?assertEqual(true, (CurRingHash0  > -1)),
    ?assertEqual(true, (PrevRingHash0 > -1)),
    ?assertEqual(true, (CurRingHash0 == PrevRingHash0)),
    ?assertEqual(true, (MemberChksum0 > -1)),

    {ok, Ring} = leo_redundant_manager_api:get_ring(?SYNC_MODE_CUR_RING),
    {ok, {_,_}} = leo_redundant_manager_api:synchronize(?SYNC_MODE_CUR_RING, Ring),
    ok.

synchronize_2_({Hostname}) ->
    ok = prepare(Hostname, storage),
    {ok,_,_} = leo_redundant_manager_api:create(),
    {ok, _Members} = leo_redundant_manager_api:get_members(),

    {ok, Ring} = leo_redundant_manager_api:get_ring(?SYNC_MODE_PREV_RING),
    {ok, {_,_}} = leo_redundant_manager_api:synchronize(?SYNC_MODE_PREV_RING, Ring),
    ok.

adjust_({Hostname}) ->
    ok = prepare(Hostname, storage),
    {ok,_,_} = leo_redundant_manager_api:create(),

    {ok, Ring} = leo_redundant_manager_api:get_ring(?SYNC_MODE_CUR_RING),
    {VNodeId, _Node} = lists:nth(1, Ring),
    ok = leo_redundant_manager_api:adjust(VNodeId),
    ok.

append_({Hostname}) ->
    ok = prepare(Hostname, storage),
    {ok,_,_} = leo_redundant_manager_api:create(),

    Node = list_to_atom("node_0@" ++ Hostname),
    ok = leo_redundant_manager_api:append(?VER_CURRENT, 1, Node),
    ok = leo_redundant_manager_api:append(?VER_PREV,    2, Node),

    {ok, Ring0} = leo_redundant_manager_api:get_ring(?SYNC_MODE_CUR_RING),
    {ok, Ring1} = leo_redundant_manager_api:get_ring(?SYNC_MODE_PREV_RING),

    ?assertEqual({1, Node}, lists:keyfind(1, 1, Ring0)),
    ?assertEqual({2, Node}, lists:keyfind(2, 1, Ring1)),
    ok.

suspend_({Hostname}) ->
    ok = prepare(Hostname, storage),
    {ok,_,_} = leo_redundant_manager_api:create(),

    Node = list_to_atom("node_0@" ++ Hostname),
    ok = leo_redundant_manager_api:suspend(Node),
    {ok, Members} = leo_redundant_manager_api:get_members(),

    {member,_,_,suspend,_} = lists:keyfind(Node, 2, Members),
    ok.

%% -------------------------------------------------------------------
%% INNER FUNCTION
%% -------------------------------------------------------------------
prepare(Hostname, ServerType) ->
    case ServerType of
        master ->
            leo_redundant_manager_table_ring:create_ring_current(ram_copies, [node()]),
            leo_redundant_manager_table_ring:create_ring_prev(ram_copies, [node()]);
        _ ->
            void
    end,

    ok = leo_redundant_manager_api:start(ServerType),
    leo_redundant_manager_api:set_options([{n, 3},
                                           {r, 1},
                                           {w ,2},
                                           {d, 2},
                                           {bit_of_ring, 128}]),
    leo_redundant_manager_api:attach(list_to_atom("node_0@" ++ Hostname)),
    leo_redundant_manager_api:attach(list_to_atom("node_1@" ++ Hostname)),
    leo_redundant_manager_api:attach(list_to_atom("node_2@" ++ Hostname)),
    leo_redundant_manager_api:attach(list_to_atom("node_3@" ++ Hostname)),
    leo_redundant_manager_api:attach(list_to_atom("node_4@" ++ Hostname)),
    leo_redundant_manager_api:attach(list_to_atom("node_5@" ++ Hostname)),
    leo_redundant_manager_api:attach(list_to_atom("node_6@" ++ Hostname)),
    leo_redundant_manager_api:attach(list_to_atom("node_7@" ++ Hostname)),
    ?debugVal(leo_redundant_manager_table_member:size()),
    ok.


inspect0(Hostname) ->
    {ok, {Chksum0, _Chksum1}} = leo_redundant_manager_api:checksum(?CHECKSUM_RING),
    ?assertEqual(true, (Chksum0 > -1)),

    %% member-related.
    {ok, Members0} = leo_redundant_manager_api:get_members(),

    ?assertEqual(8, length(Members0)),
    ?assertEqual(true,  leo_redundant_manager_api:has_member(list_to_atom("node_3@" ++ Hostname))),
    ?assertEqual(false, leo_redundant_manager_api:has_member(list_to_atom("node_8@" ++ Hostname))),

    {ok, Node5} = leo_redundant_manager_api:get_member_by_node(list_to_atom("node_5@" ++ Hostname)),
    ?assertEqual(list_to_atom("node_5@" ++ Hostname), Node5#member.node),

    ok = leo_redundant_manager_api:update_member_by_node(list_to_atom("node_7@" ++ Hostname), 12345, 'suspend'),

    {ok, Node7} = leo_redundant_manager_api:get_member_by_node(list_to_atom("node_7@" ++ Hostname)),
    ?assertEqual(12345,     Node7#member.clock),
    ?assertEqual('suspend', Node7#member.state),

    %% create routing-table > confirmations.
    {ok, Members1, Chksums} = leo_redundant_manager_api:create(),
    ?assertEqual(8, length(Members1)),
    ?assertEqual(2, length(Chksums)),

    {ok, {Chksum2, Chksum3}} = leo_redundant_manager_api:checksum(?CHECKSUM_RING),
    {ok, Chksum4} = leo_redundant_manager_api:checksum(member),
    application:set_env(?APP, ?PROP_RING_HASH, Chksum2),

    ?assertEqual(true, (-1 =< Chksum2)),
    ?assertEqual(true, (-1 =< Chksum3)),
    ?assertEqual(true, (-1 =< Chksum4)),

    ?assertEqual(1024, leo_redundant_manager_table_ring:size({mnesia, ?CUR_RING_TABLE} )),
    ?assertEqual(1024, leo_redundant_manager_table_ring:size({mnesia, ?PREV_RING_TABLE})),

    Max = leo_math:power(2, ?MD5),
    lists:foreach(fun(Num) ->
                          {ok, #redundancies{id       = _Id0,
                                             vnode_id = _VNodeId0,
                                             nodes    = Nodes0,
                                             n = 3,
                                             r = 1,
                                             w = 2,
                                             d = 2}
                          } = leo_redundant_manager_api:get_redundancies_by_key(integer_to_list(Num)),
                          lists:foreach(fun(A) ->
                                                Nodes0a = lists:delete(A, Nodes0),
                                                lists:foreach(fun(B) ->
                                                                      ?assertEqual(false, (A == B))
                                                              end, Nodes0a)
                                        end, Nodes0),
                          ?assertEqual(3, length(Nodes0))
                  end, lists:seq(0, 300)),
    lists:foreach(fun(_) ->
                          Id = random:uniform(Max),
                          {ok, #redundancies{id       = _Id0,
                                             vnode_id = _VNodeId0,
                                             nodes    = Nodes0,
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
                          ?assertEqual(3, length(Nodes0))
                  end, lists:seq(0, 300)),

    {ok, #redundancies{id       = Id,
                       vnode_id = VNodeId1,
                       nodes    = Nodes1,
                       n = 3,
                       r = 1,
                       w = 2,
                       d = 2}} = leo_redundant_manager_api:get_redundancies_by_addr_id(put, Max + 1),
    ?assertEqual(true, (Id > VNodeId1)),
    ?assertEqual(3, length(Nodes1)),

    lists:foreach(fun(_) ->
                          Id2 = random:uniform(Max),
                          {ok, Res2} = leo_redundant_manager_api:range_of_vnodes(Id2),
                          inspect1(Id2, Res2)
                  end, lists:seq(0, 1000)),

    {ok, Res3} = leo_redundant_manager_api:range_of_vnodes(0),
    inspect1(0, Res3),
    ok.


inspect1(Id, VNodes) ->
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
