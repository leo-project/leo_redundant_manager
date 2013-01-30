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
         redundancies/4, range_of_vnodes/2, rebalance/3,
         checksum/1, vnode_id/1, vnode_id/2]).
-export([export/2, import/2]).

-record(rebalance, {n        :: integer(),
                    members  :: list(),
                    src_tbl  :: ?CUR_RING_TABLE | ?PREV_RING_TABLE,
                    dest_tbl :: ?CUR_RING_TABLE | ?PREV_RING_TABLE}).


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
add(N, Table, #member{node = Node} = Member) ->
    VNodeId = vnode_id([atom_to_list(Node) ++ integer_to_list(N)]),
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
remove(N, Table, #member{node = Node} = Member) ->
    VNodeId = vnode_id([atom_to_list(Node) ++ integer_to_list(N)]),
    true = leo_redundant_manager_table_ring:delete(Table, VNodeId),
    remove(N + 1, Table, Member).


%% @doc get redundancies by key.
%%
-spec(redundancies(ring_table_info(), any(), integer(), list()) ->
             {ok, any()} | {error, any()}).
redundancies(_Table,_VNodeId, NumOfReplicas,_Members) when NumOfReplicas < 1;
                                                           NumOfReplicas > 3 ->
    {error, out_of_renge};

redundancies(Table, VNodeId0, NumOfReplicas, Members) ->
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
                            Value = leo_redundant_manager_table_ring:lookup(Table, VNodeId1),
                            redundancies(next, Table, NumOfReplicas-1, Members, VNodeId1,
                                         #redundancies{id         = VNodeId0,
                                                       vnode_id   = VNodeId1,
                                                       temp_nodes = [Value],
                                                       nodes      = [get_member(Members, Value)]})
                    end;
                VNodeId1 ->
                    Value = leo_redundant_manager_table_ring:lookup(Table, VNodeId1),
                    redundancies(next, Table, NumOfReplicas-1, Members, VNodeId1,
                                 #redundancies{id         = VNodeId0,
                                               vnode_id   = VNodeId1,
                                               temp_nodes = [Value],
                                               nodes      = [get_member(Members, Value)]})
            end;
        Value ->
            redundancies(next, Table, NumOfReplicas-1, Members, VNodeId0,
                         #redundancies{id         = VNodeId0,
                                       vnode_id   = VNodeId0,
                                       temp_nodes = [Value],
                                       nodes      = [get_member(Members, Value)]})
    end.


redundancies(next, _Table, 0, _Members,_VNodeId, #redundancies{nodes = Acc} = R) ->
    {ok, R#redundancies{temp_nodes = [],
                        nodes      = lists:reverse(Acc)}};
redundancies(next, _Table, _, _Members, -1,      #redundancies{nodes = Acc} = R) ->
    {ok, R#redundancies{temp_nodes = [],
                        nodes      = lists:reverse(Acc)}};

redundancies(next, Table, NumOfReplicas, Members, VNodeId0, #redundancies{temp_nodes = Acc0,
                                                                          nodes      = Acc1} = R) ->
    VNodeId2 =
        case leo_redundant_manager_table_ring:next(Table, VNodeId0) of
            '$end_of_table' ->
                case leo_redundant_manager_table_ring:first(Table) of
                    '$end_of_table' -> -1;
                    VNodeId1        -> VNodeId1
                end;
            VNodeId1 ->
                VNodeId1
        end,

    Value = leo_redundant_manager_table_ring:lookup(Table, VNodeId2),
    case lists:member(Value, Acc0) of
        true  -> redundancies(next, Table, NumOfReplicas,   Members, VNodeId2, R);
        false -> redundancies(next, Table, NumOfReplicas-1, Members, VNodeId2,
                              R#redundancies{temp_nodes = [Value|Acc0],
                                             nodes      = [get_member(Members, Value)|Acc1]})
    end.


%% @doc Do rebalance
%%
-spec(rebalance(tuple(), integer(), list()) ->
             {ok, list()} | {error, any()}).
rebalance(Tables, NumOfReplicas, Members) ->
    %% {SrcTbl, DestTbl} = {?CUR_RING_TABLE, ?PREV_RING_TABLE},
    {SrcTbl, DestTbl} = Tables,
    Info = #rebalance{n        = NumOfReplicas,
                      members  = Members,
                      src_tbl  = SrcTbl,
                      dest_tbl = DestTbl},
    Size = leo_redundant_manager_table_ring:size(SrcTbl),
    rebalance(Info, Size, 0, []).

rebalance(_Info, 0,   _AddrId, Acc) ->
    {ok, lists:reverse(Acc)};
rebalance( Info, Size, AddrId, Acc) ->
    #rebalance{n        = NumOfReplicas,
               members  = Members,
               src_tbl  = SrcTable,
               dest_tbl = DestTable} = Info,

    {ok, #redundancies{vnode_id = VNodeId0,
                       nodes    = Nodes0}} = redundancies(SrcTable,  AddrId, NumOfReplicas, Members),
    {ok, #redundancies{nodes    = Nodes1}} = redundancies(DestTable, AddrId, NumOfReplicas, Members),

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
            rebalance(Info, Size - 1, VNodeId0 + 1, Acc);
        [{DestNode, _}|_] ->
            SrcNode  = active_node(Members, Nodes1),
            rebalance(Info, Size - 1, VNodeId0 + 1,
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
    leo_hex:binary_to_integer(crypto:md5(Key));
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
get_member([],_Node1) ->
    {null, false};
get_member([#member{node = Node0, state = State}|_], Node1) when Node0 == Node1  ->
    {Node0, (State /= ?STATE_SUSPEND  andalso
             State /= ?STATE_DETACHED andalso
             State /= ?STATE_STOP     andalso
             State /= ?STATE_RESTARTED)};
get_member([#member{node = Node0}|T], Node1) when Node0 /= Node1 ->
    get_member(T, Node1).


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

