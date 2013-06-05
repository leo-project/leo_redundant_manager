%%======================================================================
%%
%% Leo Redundant Manager
%%
%% Copyright (c) 2012 Rakuten, Inc.
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
%% Leo Redundant Manager - Consistent Hashing
%% @doc
%% @end
%%======================================================================
-module(leo_redundant_manager_chash).

-author('yosuke hara').

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([add/2, append/3, adjust/3, remove/2,
         redundancies/5, range_of_vnodes/2, rebalance/4,
         checksum/1, vnode_id/1, vnode_id/2]).
-export([export/2, import/2]).

-record(rebalance, {n            :: pos_integer(),
                    level_2 = 0  :: pos_integer(),
                    members = [] :: list(),
                    src_tbl      :: ?CUR_RING_TABLE | ?PREV_RING_TABLE,
                    dest_tbl     :: ?CUR_RING_TABLE | ?PREV_RING_TABLE}).


%%====================================================================
%% API
%%====================================================================
%% @doc Add a node.
%%
-spec(add(atom(), #member{}) ->
             ok).
add(Table, Member) ->
    add(0, Table, Member).

add(N,_Table, #member{num_of_vnodes = NumOfVNodes}) when NumOfVNodes == N ->
    ok;
add(N, Table, #member{alias = Alias,
                      node  = Node} = Member) ->
    VNodeId = vnode_id(lists:append([Alias, "_", integer_to_list(N)])),
    true = leo_redundant_manager_table_ring:insert(Table, {VNodeId, Node}),
    add(N + 1, Table, Member).


%% @doc Append a node.
%%
-spec(append(atom(), integer(), atom()) ->
             ok).
append(Table, VNodeId, Node) ->
    true = leo_redundant_manager_table_ring:insert(Table, {VNodeId, Node}),
    ok.


%% @doc Adjust a vnode-id.
%%
-spec(adjust(atom(), atom(), integer()) ->
             ok).
adjust(CurRingTable, PrevRingTable, VNodeId) ->
    case leo_redundant_manager_table_ring:lookup(CurRingTable, VNodeId) of
        {error, _Cause} ->
            void;
        []   -> true = leo_redundant_manager_table_ring:delete(PrevRingTable, VNodeId);
        Node -> true = leo_redundant_manager_table_ring:insert(PrevRingTable, {VNodeId, Node})
    end,
    ok.


%% @doc Remove a node.
%%
-spec(remove(atom, #member{}) ->
             ok).
remove(Table, Member) ->
    remove(0, Table, Member).

remove(N,_Table, #member{num_of_vnodes = NumOfVNodes}) when NumOfVNodes == N ->
    ok;
remove(N, Table, #member{alias = Alias} = Member) ->
    VNodeId = vnode_id(lists:append([Alias, "_", integer_to_list(N)])),
    true = leo_redundant_manager_table_ring:delete(Table, VNodeId),
    remove(N + 1, Table, Member).


%% @doc get redundancies by key.
%%
-spec(redundancies(ring_table_info(), any(), pos_integer(), pos_integer(),list()) ->
             {ok, any()} | {error, any()}).
redundancies(_Table,_VNodeId, NumOfReplicas,_L2,_Members) when NumOfReplicas < 1;
                                                               NumOfReplicas > 3 ->
    {error, out_of_renge};
redundancies(_Table,_VNodeId, NumOfReplicas, L2,_Members) when (NumOfReplicas - L2) < 1 ->
    {error, invalid_level2};
redundancies(_Table,_VNodeId,_NumOfReplicas,_L2, [#member{node  = Node,
                                                          state = State}]) ->
        {ok, #redundancies{temp_nodes = [],
                           nodes      = [{Node, (State == ?STATE_RUNNING)}]}};
redundancies(Table, VNodeId0, NumOfReplicas, L2, Members) ->
    case leo_redundant_manager_table_ring:lookup(Table, VNodeId0) of
        {error, Cause} ->
            {error, Cause};
        [] ->
            case leo_redundant_manager_table_ring:next(Table, VNodeId0) of
                '$end_of_table' ->
                    case leo_redundant_manager_table_ring:first(Table) of
                        '$end_of_table' ->
                            {error, no_entry};
                        VNodeId1 ->
                            redundnacies_1(Table, VNodeId0, VNodeId1,
                                           NumOfReplicas, L2, Members)
                    end;
                VNodeId1 ->
                    redundnacies_1(Table, VNodeId0, VNodeId1,
                                   NumOfReplicas, L2, Members)
            end;
        Value ->
            redundnacies_1(Table, VNodeId0, VNodeId0, Members,
                           NumOfReplicas, L2, Value)
    end.

%% @private
redundnacies_1(Table, VNodeId_Org, VNodeId_Hop, NumOfReplicas, L2, Members) ->
    Value = leo_redundant_manager_table_ring:lookup(Table, VNodeId_Hop),
    redundnacies_1(Table, VNodeId_Org, VNodeId_Hop, NumOfReplicas, L2, Members, Value).

redundnacies_1(Table, VNodeId_Org, VNodeId_Hop, NumOfReplicas, L2, Members, Value) ->
    SetsL2 = sets:new(),
    {Node, State, SetsL2_1} = get_state(Members, Value, SetsL2),

    redundancies_2(Table, NumOfReplicas-1, L2, Members, VNodeId_Hop,
                   #redundancies{id           = VNodeId_Org,
                                 vnode_id     = VNodeId_Hop,
                                 temp_nodes   = [Value],
                                 temp_level_2 = SetsL2_1,
                                 nodes        = [{Node, State}]}).

%% @private
redundancies_2(_Table, 0,_L2,_Members,_VNodeId, #redundancies{nodes = Acc} = R) ->
    {ok, R#redundancies{temp_nodes = [],
                        nodes      = lists:reverse(Acc)}};
redundancies_2(_Table, _,_L2,_Members, -1,      #redundancies{nodes = Acc} = R) ->
    {ok, R#redundancies{temp_nodes = [],
                        nodes      = lists:reverse(Acc)}};

redundancies_2( Table, NumOfReplicas, L2, Members, VNodeId0, #redundancies{temp_nodes   = AccTempNode,
                                                                           temp_level_2 = AccLevel2,
                                                                           nodes        = AccNodes} = R) ->
    VNodeId2 = case leo_redundant_manager_table_ring:next(Table, VNodeId0) of
                   '$end_of_table' ->
                       case leo_redundant_manager_table_ring:first(Table) of
                           '$end_of_table' -> -1;
                           VNodeId1 ->
                               VNodeId1
                       end;
                   VNodeId1 ->
                       VNodeId1
               end,

    Value = leo_redundant_manager_table_ring:lookup(Table, VNodeId2),
    case lists:member(Value, AccTempNode) of
        true  ->
            redundancies_2(Table, NumOfReplicas, L2, Members, VNodeId2, R);
        false ->
            {Node, State, AccLevel2_1} = get_state(Members, Value, AccLevel2),
            AccNodesSize  = length(AccNodes),
            AccLevel2Size = sets:size(AccLevel2_1),

            case (L2 /= 0 andalso L2 == AccNodesSize) of
                true when AccLevel2Size < (L2+1) ->
                    redundancies_2(Table, NumOfReplicas, L2, Members, VNodeId2, R);
                _ ->
                    redundancies_2(Table, NumOfReplicas-1, L2, Members, VNodeId2,
                                   R#redundancies{temp_nodes   = [Value|AccTempNode],
                                                  temp_level_2 = AccLevel2_1,
                                                  nodes        = [{Node, State}|AccNodes]})
            end
    end.


%% @doc Do rebalance
%%
-spec(rebalance(tuple(), pos_integer(), pos_integer(), list()) ->
             {ok, list()} | {error, any()}).
rebalance(Tables, NumOfReplicas, L2, Members) ->
    %% {SrcTbl, DestTbl} = {?CUR_RING_TABLE, ?PREV_RING_TABLE},
    {SrcTbl, DestTbl} = Tables,
    Info = #rebalance{n        = NumOfReplicas,
                      level_2  = L2,
                      members  = Members,
                      src_tbl  = SrcTbl,
                      dest_tbl = DestTbl},
    Size = leo_redundant_manager_table_ring:size(SrcTbl),
    rebalance_1(Info, Size, 0, []).

rebalance_1(_Info, 0,   _AddrId, Acc) ->
    {ok, lists:reverse(Acc)};
rebalance_1( Info, Size, AddrId, Acc) ->
    #rebalance{n        = NumOfReplicas,
               level_2  = L2,
               members  = Members,
               src_tbl  = SrcTable,
               dest_tbl = DestTable} = Info,

    {ok, #redundancies{vnode_id = VNodeId0,
                       nodes    = Nodes0}} =
        redundancies(SrcTable,  AddrId, NumOfReplicas, L2, Members),
    {ok, #redundancies{nodes    = Nodes1}} =
        redundancies(DestTable, AddrId, NumOfReplicas, L2, Members),

    Res = lists:foldl(
            fun(N0, Acc0) ->
                    case lists:foldl(fun(N1,_Acc1) when N0 == N1 -> true;
                                        (N1, Acc1) when N0 /= N1 -> Acc1
                                     end, false, Nodes1) of
                        true  -> Acc0;
                        false -> [N0|Acc0]
                    end
            end, [], Nodes0),
    case Res of
        [] ->
            rebalance_1(Info, Size - 1, VNodeId0 + 1, Acc);
        [{DestNode, _}|_] ->
            SrcNode  = active_node(Members, Nodes1),
            rebalance_1(Info, Size - 1, VNodeId0 + 1,
                        [[{vnode_id, VNodeId0}, {src, SrcNode}, {dest, DestNode}]|Acc])
    end.


%% @doc Retrieve ring-checksum
%%
-spec(checksum(ring_table_info()) ->
             integer()).
checksum(Table) ->
    case catch leo_redundant_manager_table_ring:tab2list(Table) of
        {'EXIT', _Cause} ->
            {ok, -1};
        [] ->
            {ok, -1};
        List ->
            {ok, erlang:crc32(term_to_binary(List))}
    end.


%% @doc Retrieve virtual-node-id
%%
-spec(vnode_id(Key::any()) ->
             integer()).
vnode_id(Key) ->
    vnode_id(?MD5, Key).

vnode_id(?MD5, Key) ->
    leo_hex:raw_binary_to_integer(crypto:md5(Key));
vnode_id(_, _) ->
    {error, badarg}.


%% @doc Dump table to a file.
%%
-spec(export(atom(), string()) ->
             ok | {error, any()}).
export(Table, FileName) ->
    case leo_redundant_manager_table_ring:size(Table) of
        0 ->
            ok;
        _ ->
            List0 = leo_redundant_manager_table_ring:tab2list(Table),
            leo_file:file_unconsult(FileName, List0)
    end.


%% @doc Import from a file.
%%
-spec(import(atom(), string()) ->
             ok | {error, any()}).
import(Table, FileName) ->
    case (leo_redundant_manager_table_ring:size(Table) == 0) of
        true ->
            ok;
        false ->
            true = leo_redundant_manager_table_ring:delete_all_objects(Table),
            case file:consult(FileName) of
                {ok, List} ->
                    lists:foreach(fun({VNodeId, Node}) ->
                                          append(Table, VNodeId, Node)
                                  end, List);
                Error ->
                    Error
            end
    end.


%%====================================================================
%% Internal functions
%%====================================================================
%% @doc Retrieve a member from an argument.
%% @private
get_state([],_Node1,_) ->
    {null, false, undefined};
get_state([#member{node        = Node0,
                   state       = State,
                   grp_level_2 = L2}|_], Node1, SetL2) when Node0 == Node1  ->
    {Node0, (State == ?STATE_RUNNING), sets:add_element(L2, SetL2)};
get_state([#member{node = Node0}|T], Node1, SetL2) when Node0 /= Node1 ->
    get_state(T, Node1, SetL2).


%% @doc Retrieve range of vnodes.
%% @private
range_of_vnodes(Table, VNodeId0) ->
    Res = case leo_redundant_manager_table_ring:lookup(Table, VNodeId0) of
              {error, Cause} ->
                  {error, Cause};
              [] ->
                  case leo_redundant_manager_table_ring:next(Table, VNodeId0) of
                      '$end_of_table' ->
                          case leo_redundant_manager_table_ring:first(Table) of
                              '$end_of_table' ->
                                  {error, no_entry};
                              VNodeId1 ->
                                  {ok,  VNodeId1}
                          end;
                      VNodeId1 ->
                          {ok, VNodeId1}
                  end;
              _ValueId ->
                  {ok, VNodeId0}
          end,
    range_of_vnodes_1(Table, Res).

range_of_vnodes_1(_Table, {error, _} = Error) ->
    Error;
range_of_vnodes_1(Table, {ok, ToVNodeId}) ->
    Res = case leo_redundant_manager_table_ring:lookup(Table, ToVNodeId) of
              {error, Cause} ->
                  {error, Cause};
              [] ->
                  {error, no_entry};
              _Value ->
                  case leo_redundant_manager_table_ring:prev(Table, ToVNodeId) of
                      '$end_of_table' ->
                          case leo_redundant_manager_table_ring:last(Table) of
                              '$end_of_table' ->
                                  {error, no_entry};
                              VNodeId1 ->
                                  {ok, [{VNodeId1 + 1, leo_math:power(2, ?MD5)},
                                        {0, ToVNodeId}]}
                          end;
                      VNodeId1 ->
                          {ok, [{VNodeId1 + 1, ToVNodeId}]}
                  end
          end,
    Res.


%% @doc Retrieve active nodes.
%% @private
active_node(_Members, []) ->
    {error, no_entry};
active_node(Members, [{Node0, _}|T]) ->
    case lists:foldl(fun(#member{node = Node1, state = ?STATE_RUNNING}, []) when Node0 =:= Node1 ->
                             Node1;
                        (_Member, SoFar) ->
                             SoFar
                     end, [], Members) of
        [] ->
            active_node(Members, T);
        Res ->
            Res
    end.

