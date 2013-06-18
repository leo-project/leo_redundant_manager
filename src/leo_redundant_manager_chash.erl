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
         redundancies/3, range_of_vnodes/2, rebalance/2,
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


%% %% @doc Retrieve redundancies by vnode-id.
%% %%
redundancies(ServerRef, {_,Table}, VNodeId) ->
    leo_redundant_manager_worker:lookup(ServerRef, Table, VNodeId).


%% @doc Do rebalance
%% @private
-spec(rebalance(tuple(), list()) ->
             {ok, list()} | {error, any()}).
rebalance(Tables, Members) ->
    %% {SrcTbl, DestTbl} = {?CUR_RING_TABLE, ?PREV_RING_TABLE},
    {SrcTbl, DestTbl} = Tables,
    Info = #rebalance{members  = Members,
                      src_tbl  = SrcTbl,
                      dest_tbl = DestTbl},
    Size = leo_redundant_manager_table_ring:size(SrcTbl),

    case catch poolboy:checkout(?RING_WORKER_POOL_NAME) of
        {'EXIT', Cause} ->
            {error, Cause};
        ServerRef ->
            {_, SrcTbl_1 } = SrcTbl,
            {_, DestTbl_1} = DestTbl,
            ok = leo_redundant_manager_worker:force_sync(ServerRef, SrcTbl_1),
            ok = leo_redundant_manager_worker:force_sync(ServerRef, DestTbl_1),

            Ret = rebalance_1(ServerRef, Info, Size, 0, []),
            _ = poolboy:checkin(?RING_WORKER_POOL_NAME, ServerRef),
            Ret
    end.

%% @private
rebalance_1(_ServerRef,_Info, 0,  _VNodeId, Acc) ->
    {ok, lists:reverse(Acc)};
rebalance_1(ServerRef, Info, Size, VNodeId, Acc) ->
    #rebalance{members  = Members,
               src_tbl  = {_, SrcTbl_1},
               dest_tbl = {_, DestTbl_1}} = Info,
    {ok, #redundancies{vnode_id_to = VNodeIdTo,
                       nodes = Nodes0}} =
        leo_redundant_manager_worker:lookup(ServerRef, SrcTbl_1,  VNodeId),
    {ok, #redundancies{nodes = Nodes1}} =
        leo_redundant_manager_worker:lookup(ServerRef, DestTbl_1, VNodeId),

    Res1 = lists:foldl(
             fun(N0, Acc0) ->
                     case lists:foldl(fun(N1,_Acc1) when N0 == N1 -> true;
                                         (N1, Acc1) when N0 /= N1 -> Acc1
                                      end, false, Nodes1) of
                         true  -> Acc0;
                         false -> [N0|Acc0]
                     end
             end, [], Nodes0),
    case Res1 of
        [] ->
            rebalance_1(ServerRef, Info, Size - 1, VNodeIdTo + 1, Acc);
        [{DestNode, _}|_] ->
            SrcNode  = active_node(Members, Nodes1),
            rebalance_1(ServerRef, Info, Size - 1, VNodeIdTo + 1,
                        [[{vnode_id, VNodeIdTo},
                          {src,  SrcNode},
                          {dest, DestNode}]|Acc])
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
%% @doc Retrieve range of vnodes.
%% @private
range_of_vnodes({_,Table}, VNodeId) ->
    case catch poolboy:checkout(?RING_WORKER_POOL_NAME) of
        {'EXIT', Cause} ->
            {error, Cause};
        ServerRef ->
            Res1 = range_of_vnodes_1(ServerRef, Table, VNodeId),
            _ = poolboy:checkin(?RING_WORKER_POOL_NAME, ServerRef),
            Res1
    end.

range_of_vnodes_1(ServerRef, Table, VNodeId) ->
    case leo_redundant_manager_worker:lookup(ServerRef, Table, VNodeId) of
        not_found ->
            {error, not_found};
        {ok, #redundancies{vnode_id_from = From,
                           vnode_id_to   = To}} ->
            case From of
                0 ->
                    case leo_redundant_manager_worker:last(ServerRef, Table) of
                        not_found ->
                            {ok, [{From, To}]};
                        {ok, #redundancies{vnode_id_to = LastId}} ->
                            {ok, [{From, To},
                                  {LastId + 1, leo_math:power(2, ?MD5)}]}
                    end;
                _ ->
                    {ok, [{From, To}]}
            end
    end.


%% @doc Retrieve active nodes.
%% @private
active_node(_Members, []) ->
    {error, no_entry};
active_node(Members, [{Node0, _}|T]) ->
    case lists:foldl(
           fun(#member{node  = Node1,
                       state = ?STATE_RUNNING}, []) when Node0 == Node1 ->
                   Node1;
              (_Member, SoFar) ->
                   SoFar
           end, [], Members) of
        [] ->
            active_node(Members, T);
        Res ->
            Res
    end.

