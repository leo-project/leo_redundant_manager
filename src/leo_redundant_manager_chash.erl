%%======================================================================
%%
%% Leo Redundant Manager
%%
%% Copyright (c) 2012-2013 Rakuten, Inc.
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

-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([add/2, remove/2,
         redundancies/3, range_of_vnodes/2, rebalance/1,
         checksum/1, vnode_id/1, vnode_id/2]).
-export([export/2]).


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
                      node  = Node,
                      clock = Clock} = Member) ->
    VNodeId = vnode_id(lists:append([Alias, "_", integer_to_list(N)])),
    true = leo_redundant_manager_table_ring:insert(Table, {VNodeId, Node, Clock}),
    add(N + 1, Table, Member).


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


%% @doc Retrieve redundancies by vnode-id.
%%
redundancies(ServerRef, {_,Table}, VNodeId) ->
    leo_redundant_manager_worker:lookup(ServerRef, Table, VNodeId).


%% @doc Execute rebalance
%% @private
-spec(rebalance(#rebalance{}) ->
             {ok, list()} | {error, any()}).
rebalance(RebalanceInfo) ->
    #rebalance{tbl_cur  = TblInfoCur,
               tbl_prev = TblInfoPrev} = RebalanceInfo,

    %% force sync worker's ring
    ServerRef = leo_redundant_manager_api:get_server_id(),
    {_, TblNameCur } = TblInfoCur,
    {_, TblNamePrev} = TblInfoPrev,

    ok = leo_redundant_manager_worker:force_sync(ServerRef, TblNameCur),
    ok = leo_redundant_manager_worker:force_sync(ServerRef, TblNamePrev),

    %% retrieve different node between current and previous ring
    rebalance_1(ServerRef, RebalanceInfo, 0, []).

%% @doc Retrieve diffrences between current-ring and prev-ring
%% case-1:
%%  cur-ring: |...---------E---|2^128, 0|---F---...
%% prev-ring: |...-----E-------|2^128, 0|------F...
%%
%% case-2
%%  cur-ring: |...---E-------- |2^128, 0|---F---...
%% prev-ring: |...------E------|2^128, 0|------F...
%%
%% @private
rebalance_1(ServerRef, RebalanceInfo, AddrId, Acc) ->
    #rebalance{tbl_cur      = TblInfoCur,
               members_cur  = MembersCur,
               members_prev = MembersPrev} = RebalanceInfo,
    {_, TblNameCur} = TblInfoCur,

    %% Judge whether it match which case
    CurLastVNodeId  = leo_redundant_manager_table_ring:last({mnesia, ?RING_TBL_CUR}),
    PrevLastVNodeId = leo_redundant_manager_table_ring:last({mnesia, ?RING_TBL_PREV}),

    {ok, #redundancies{vnode_id_to = PrevVNodeIdTo,
                       nodes = PrevNodes}} =
        leo_redundant_manager_worker:redundancies(
          ServerRef, ?ring_table(?SYNC_TARGET_RING_PREV), AddrId, MembersPrev),

    {VNodeIdTo, CurNodes} =
        case (CurLastVNodeId >= PrevLastVNodeId) of
            %% case-1:
            true ->
                {ok, #redundancies{vnode_id_to = CurVNodeIdTo,
                                   nodes = CurNodes_1}} =
                    leo_redundant_manager_worker:lookup(ServerRef, TblNameCur,  AddrId),
                {CurVNodeIdTo, CurNodes_1};
            %% case-2:
            false ->
                {ok, #redundancies{nodes = CurNodes_1}} =
                    leo_redundant_manager_worker:first(ServerRef, TblNameCur),
                {PrevVNodeIdTo, CurNodes_1}
        end,

    %% Retrieve deferences between current-ring and prev-ring
    IsEndOfAddrId = (VNodeIdTo < AddrId),
    Acc_1 = case lists:foldl(
                   fun(#redundant_node{node = N0}, SoFar_1) ->
                           case lists:foldl(
                                  fun(#redundant_node{node = N1},_SoFar_2) when N0 == N1 -> true;
                                     (#redundant_node{node = N1}, SoFar_2) when N0 /= N1 -> SoFar_2
                                  end, false, PrevNodes) of
                               true  -> SoFar_1;
                               false -> [N0|SoFar_1]
                           end
                   end, [], CurNodes) of
                [] ->
                    Acc;
                DestNodeList ->
                    %% Set one or plural target node(s)
                    SrcNode = active_node(MembersCur, PrevNodes),
                    VNodeIdTo_1 = case IsEndOfAddrId of
                                      true  -> leo_math:power(2, ?MD5);
                                      false -> VNodeIdTo
                                  end,
                    rebalance_1_1(VNodeIdTo_1, SrcNode, DestNodeList, Acc)
            end,

    case IsEndOfAddrId of
        true ->
            {ok, lists:reverse(Acc_1)};
        false  ->
            rebalance_1(ServerRef, RebalanceInfo, VNodeIdTo + 1, Acc_1)
    end.


%% @private
rebalance_1_1(_VNodeIdTo,_SrcNode, [], Acc) ->
    Acc;
rebalance_1_1(VNodeIdTo, SrcNode, [DestNode|Rest], Acc) ->
    Acc_1 = [[{vnode_id, VNodeIdTo},
              {src,  SrcNode},
              {dest, DestNode}]|Acc],
    rebalance_1_1(VNodeIdTo, SrcNode, Rest, Acc_1).


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
    leo_hex:raw_binary_to_integer(crypto:hash(md5, Key));
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


%%====================================================================
%% Internal functions
%%====================================================================
%% @doc Retrieve range of vnodes.
%% @private
range_of_vnodes({_,Table}, VNodeId) ->
    ServerRef = leo_redundant_manager_api:get_server_id(),
    range_of_vnodes_1(ServerRef, Table, VNodeId).

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
active_node(Members, [#redundant_node{node = Node_1}|T]) ->
    case lists:foldl(
           fun(#member{node  = Node_2,
                       state = ?STATE_RUNNING}, []) when Node_1 == Node_2 ->
                   Node_2;
              (_Member, SoFar) ->
                   SoFar
           end, [], Members) of
        [] ->
            active_node(Members, T);
        Res ->
            Res
    end.

