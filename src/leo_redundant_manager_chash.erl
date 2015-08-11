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
%% ---------------------------------------------------------------------
%% Leo Redundant Manager - Consistent Hashing
%%
%% @doc The consistent-hashing implementation
%% @reference https://github.com/leo-project/leo_redundant_manager/blob/master/src/leo_redundant_manager_chash.erl
%% @end
%%======================================================================
-module(leo_redundant_manager_chash).

-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([add/2, add_from_list/2,
         remove/2, remove_from_list/2,
         range_of_vnodes/2, rebalance/1,
         checksum/1, vnode_id/1, vnode_id/2]).
-export([export/2]).

-define(gen_child_name(_Alias,_N),
        lists:append([_Alias, "_", integer_to_list(_N)])).

%%====================================================================
%% API
%%====================================================================
%% @doc Add a node.
%%
-spec(add(TableInfo, Member) ->
             ok | {error, any()} when TableInfo::table_info(),
                                      Member::#member{}).
add(TableInfo, Member) ->
    {ok, List} = add_1(0, Member, []),
    leo_cluster_tbl_ring:bulk_insert(TableInfo, List).

%% @private
add_1(N, #member{num_of_vnodes = N}, Acc) ->
    {ok, Acc};
add_1(N, #member{alias = Alias,
                 node  = Node,
                 clock = Clock} = Member, Acc) ->
    VNodeId = vnode_id(?gen_child_name(Alias, N)),
    add_1(N + 1, Member, [{VNodeId, Node, Clock}|Acc]).


%% @doc Insert recods from the list
add_from_list(TableInfo, Members) ->
    {ok, List} = add_from_list_1(Members, []),
    leo_cluster_tbl_ring:bulk_insert(TableInfo, List).

%% @private
add_from_list_1([], Acc) ->
    {ok, Acc};
add_from_list_1([Member|Rest], Acc) ->
    {ok, Acc_1} = add_1(0, Member, Acc),
    add_from_list_1(Rest, Acc_1).


%% @doc Remove a node.
%%
-spec(remove(TableInfo, Member) ->
             ok | {error, any()} when TableInfo::table_info(),
                                      Member::#member{}).
remove(TableInfo, Member) ->
    {ok, List} = remove_1(0, Member, []),
    leo_cluster_tbl_ring:bulk_delete(TableInfo, List).

%% @private
remove_1(N, #member{num_of_vnodes = N}, Acc) ->
    {ok, Acc};
remove_1(N, #member{alias = Alias} = Member, Acc) ->
    VNodeId = vnode_id(?gen_child_name(Alias, N)),
    remove_1(N + 1, Member, [VNodeId|Acc]).


%% @doc Remove recods from the list
-spec(remove_from_list(TableInfo, Members) ->
             ok | {error, any()} when TableInfo::table_info(),
                                      Members::[#member{}]).
remove_from_list(TableInfo, Members) ->
    {ok, List} = remove_from_list_1(Members, []),
    leo_cluster_tbl_ring:bulk_delete(TableInfo, List).

%% @private
remove_from_list_1([], Acc) ->
    {ok, Acc};
remove_from_list_1([Member|Rest], Acc) ->
    {ok, Acc_1} = remove_1(0, Member, Acc),
    remove_from_list_1(Rest, Acc_1).


%% @doc Execute rebalance
%% @private
-spec(rebalance(RebalanceInfo) ->
             {ok, []} when RebalanceInfo::#rebalance{}).
rebalance(RebalanceInfo) ->
    #rebalance{tbl_cur  = TblInfoCur,
               tbl_prev = TblInfoPrev} = RebalanceInfo,

    %% force sync worker's ring
    {_, TblNameCur } = TblInfoCur,
    {_, TblNamePrev} = TblInfoPrev,

    ok = leo_redundant_manager_worker:force_sync(TblNameCur),
    ok = leo_redundant_manager_worker:force_sync(TblNamePrev),

    %% retrieve different node between current and previous ring
    rebalance_1(RebalanceInfo, 0, []).

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
rebalance_1(RebalanceInfo, AddrId, Acc) ->
    #rebalance{tbl_cur      = TblInfoCur,
               members_cur  = MembersCur,
               members_prev = MembersPrev} = RebalanceInfo,
    {_, TblNameCur} = TblInfoCur,

    %% Judge whether it match which case
    CurLastVNodeId  = leo_cluster_tbl_ring:last({mnesia, ?RING_TBL_CUR}),
    PrevLastVNodeId = leo_cluster_tbl_ring:last({mnesia, ?RING_TBL_PREV}),
    TblInfo = leo_redundant_manager_api:table_info(?VER_PREV),

    {ok, #redundancies{vnode_id_to = PrevVNodeIdTo,
                       nodes = PrevNodes}} =
        leo_redundant_manager_worker:redundancies(TblInfo, AddrId, MembersPrev),

    {VNodeIdTo, CurNodes} =
        case (PrevLastVNodeId > CurLastVNodeId andalso
              AddrId > CurLastVNodeId) of
            true ->
                %% case-2
                {ok, #redundancies{nodes = CurNodes_1}} =
                    leo_redundant_manager_worker:first(TblNameCur),
                {PrevVNodeIdTo, CurNodes_1};
            false ->
                %% case-1
                {ok, #redundancies{vnode_id_to = CurVNodeIdTo,
                                   nodes = CurNodes_1}} =
                    leo_redundant_manager_worker:lookup(TblNameCur,  AddrId),
                {CurVNodeIdTo, CurNodes_1}
        end,

    %% Retrieve deferences between current-ring and prev-ring
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
                    VNodeIdTo_1 = case (CurLastVNodeId < AddrId) of
                                      true  -> leo_math:power(2, ?MD5);
                                      false -> VNodeIdTo
                                  end,
                    rebalance_1_1(VNodeIdTo_1, SrcNode, DestNodeList, Acc)
            end,

    NewVNodeIdTo = VNodeIdTo + 1,
    case (erlang:max(CurLastVNodeId,PrevLastVNodeId) < NewVNodeIdTo) of
        true ->
            {ok, lists:reverse(Acc_1)};
        false  ->
            rebalance_1(RebalanceInfo, NewVNodeIdTo, Acc_1)
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
-spec(checksum(TableInfo) ->
             {ok, integer()} when TableInfo::table_info()).
checksum(TableInfo) ->
    {_, Tbl} = TableInfo,
    case catch leo_cluster_tbl_ring:tab2list(TableInfo) of
        {'EXIT', _Cause} ->
            {ok, -1};
        [] ->
            {ok, -1};
        _List when Tbl == ?RING_TBL_CUR ->
            {ok, {Checksum,_Old}} =leo_redundant_manager_worker:checksum(),
            {ok, Checksum};
        _List when Tbl == ?RING_TBL_PREV ->
            {ok, {_Now, Checksum}} =leo_redundant_manager_worker:checksum(),
            {ok, Checksum}
    end.


%% @doc Retrieve virtual-node-id
%%
-spec(vnode_id(Key) ->
             integer() when Key::any()).
vnode_id(Key) ->
    vnode_id(?MD5, Key).

vnode_id(?MD5, Key) ->
    leo_hex:raw_binary_to_integer(crypto:hash(md5, Key));
vnode_id(_, _) ->
    {error, badarg}.


%% @doc Dump table to a file.
%%
-spec(export(TableInfo, FileName) ->
             ok | {error, any()} when TableInfo::table_info(),
                                      FileName::string()).
export(TableInfo, FileName) ->
    case leo_cluster_tbl_ring:size(TableInfo) of
        0 ->
            ok;
        _ ->
            List0 = leo_cluster_tbl_ring:tab2list(TableInfo),
            leo_file:file_unconsult(FileName, List0)
    end.


%% @doc Retrieve range of vnodes.
%%
-spec(range_of_vnodes(TableInfo, VNodeId) ->
             {ok, [tuple()]} when TableInfo::table_info(),
                                  VNodeId::integer()).
range_of_vnodes({_,Table}, VNodeId) ->
    range_of_vnodes_1(Table, VNodeId).

range_of_vnodes_1(Table, VNodeId) ->
    case leo_redundant_manager_worker:lookup(Table, VNodeId) of
        not_found ->
            {error, not_found};
        {ok, #redundancies{vnode_id_from = From,
                           vnode_id_to   = To}} ->
            case From of
                0 ->
                    case leo_redundant_manager_worker:last(Table) of
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


%%====================================================================
%% Internal functions
%%====================================================================
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
