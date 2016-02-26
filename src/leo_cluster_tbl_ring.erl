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
%% @doc The cluster ring table's operation
%% @reference https://github.com/leo-project/leo_redundant_manager/blob/master/src/leo_cluster_tbl_ring.erl
%% @end
%%======================================================================
-module(leo_cluster_tbl_ring).

-include("leo_redundant_manager.hrl").
-include_lib("stdlib/include/qlc.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([create_table_current/1, create_table_current/2,
         create_table_prev/1, create_table_prev/2,
         lookup/3, find_all/1, find_by_cluster_id/2,
         insert/2, delete/3,
         first/2, last/2, prev/3, next/3,
         delete_all/2,
         size/2,
         overwrite/3
        ]).
-export([create_table_for_test/3]).


%% @doc create ring-current table
%%
-spec(create_table_current(Mode) ->
             ok when Mode::mnesia_copies()).
create_table_current(Mode) ->
    create_table_current(Mode, [erlang:node()]).

-spec(create_table_current(Mode, Nodes) ->
             ok when Mode::mnesia_copies(),
                     Nodes::[atom()]).
create_table_current(Mode, Nodes) ->
    case mnesia:create_table(
           ?RING_TBL_CUR,
           [{Mode, Nodes},
            {type, ordered_set},
            {record_name, ?RING},
            {attributes, record_info(fields, ?RING)}
           ]) of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
            {error, Reason}
    end.


%% @doc create ring-prev table
%%
-spec(create_table_prev(Mode) ->
             ok when Mode::mnesia_copies()).
create_table_prev(Mode) ->
    create_table_prev(Mode, [erlang:node()]).

-spec(create_table_prev(Mode, Nodes) ->
             ok when Mode::mnesia_copies(),
                     Nodes::[atom()]).
create_table_prev(Mode, Nodes) ->
    case mnesia:create_table(
           ?RING_TBL_PREV,
           [{Mode, Nodes},
            {type, ordered_set},
            {record_name, ?RING},
            {attributes, record_info(fields, ?RING)}
           ]) of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
            {error, Reason}
    end.


%% @doc create table for the test
%%
create_table_for_test(Mode, Nodes, Table) ->
    mnesia:create_table(
      Table,
      [{Mode, Nodes},
       {type, ordered_set},
       {record_name, ring},
       {attributes, record_info(fields, ring)}
      ]).


%% @doc Retrieve a record by key from the table
%%
-spec(lookup(TableInfo, ClusterId, VNodeId) ->
             #?RING{} |
             not_found |
             {error, any()} when TableInfo::table_info(),
                                 ClusterId::cluster_id(),
                                 VNodeId::integer()).
lookup({?DB_MNESIA, Table}, ClusterId, VNodeId) ->
    F = fun() ->
                Q = qlc:q([X || X <- mnesia:table(Table),
                                X#?RING.cluster_id == ClusterId,
                                X#?RING.vnode_id == VNodeId]),
                qlc:e(Q)
        end,
    case leo_mnesia:read(F) of
        {ok, [Ring|_]} ->
            Ring;
        Other ->
            Other
    end;
lookup({?DB_ETS, Table}, ClusterId, VNodeId) ->
    case catch ets:lookup(Table, {ClusterId, VNodeId}) of
        [{_,Ring}|_] ->
            Ring;
        [] ->
            not_found;
        {'EXIT', Cause} ->
            {error, Cause}
    end.


%% @doc Retrieve all of records
-spec(find_all(TableInfo) ->
             #?RING{} |
             not_found |
             {error, any()} when TableInfo::table_info()).
find_all({?DB_MNESIA, Table}) ->
    F = fun() ->
                Q1 = qlc:q([X || X <- mnesia:table(Table)]),
                Q2 = qlc:sort(Q1, [{order, ascending}]),
                qlc:e(Q2)
        end,
    leo_mnesia:read(F);
find_all({?DB_ETS, Table}) ->
    case catch ets:foldl(
                 fun({_, Ring}, Acc) ->
                         [Ring|Acc]
                 end, [], Table) of
        {'EXIT', Cause} ->
            {error, Cause};
        [] ->
            not_found;
        RetL ->
            {ok, lists:reverse(RetL)}
    end.


%% @doc Retrieve records by the cluster-id
-spec(find_by_cluster_id(TableInfo, ClusterId) ->
             #?RING{} |
             not_found |
             {error, any()} when TableInfo::table_info(),
                                 ClusterId::cluster_id()).
find_by_cluster_id({?DB_MNESIA, Table}, ClusterId) ->
    F = fun() ->
                Q1 = qlc:q([X || X <- mnesia:table(Table),
                                 X#?RING.cluster_id == ClusterId
                           ]),
                Q2 = qlc:sort(Q1, [{order, ascending}]),
                qlc:e(Q2)
        end,
    leo_mnesia:read(F);
find_by_cluster_id({?DB_ETS, Table}, ClusterId) ->
    case catch ets:foldl(
                 fun({_, #?RING{cluster_id = ClusterId_1} = Ring}, Acc)
                       when ClusterId_1 == ClusterId ->
                         [Ring|Acc];
                    (_, Acc) ->
                         Acc
                 end, [], Table) of
        {'EXIT', Cause} ->
            {error, Cause};
        [] ->
            not_found;
        RetL ->
            {ok, lists:reverse(RetL)}
    end.


%% @doc Insert a record into the table
%%
-spec(insert(TableInfo, Ring) ->
             ok | {error, any()} when TableInfo::table_info(),
                                      Ring::#ring_2{}).
insert({?DB_MNESIA, Table}, Ring) when is_record(Ring, ring_2) ->
    Fun = fun() ->
                  mnesia:write(Table, Ring, write)
          end,
    leo_mnesia:write(Fun);
insert({?DB_ETS, Table}, #?RING{cluster_id = ClusterId,
                                vnode_id = VNodeId} = Ring) ->
    true = ets:insert(Table, {{ClusterId, VNodeId}, Ring}),
    ok.


%% @doc Remove a record from the table
%%
-spec(delete(TableInfo, ClusterId, VNodeId) ->
             ok | {error, any()} when TableInfo::table_info(),
                                      ClusterId::cluster_id(),
                                      VNodeId::integer()).
delete({?DB_MNESIA, Table} = TableInfo, ClusterId, VNodeId) ->
    case lookup(TableInfo, ClusterId, VNodeId) of
        {error, Cause} ->
            {error, Cause};
        not_found ->
            ok;
        Ring ->
            Fun = fun() ->
                          mnesia:delete_object(Table, Ring, write)
                  end,
            leo_mnesia:delete(Fun)
    end;
delete({?DB_ETS, Table}, ClusterId, VNodeId) ->
    true = ets:delete(Table, {ClusterId, VNodeId}),
    ok.


%% @doc Retrieve a first record from the table
%%
-spec(first(TableInfo, ClusterId) ->
             integer() | '$end_of_table' when TableInfo::table_info(),
                                              ClusterId::cluster_id()).
first({?DB_MNESIA,_Table} = TblInfo, ClusterId) ->
    case find_by_cluster_id(TblInfo, ClusterId) of
        {ok, [#?RING{vnode_id = VNodeId}|_]} ->
            VNodeId;
        not_found ->
            '$end_of_table';
        Other ->
            Other
    end;
first({?DB_ETS, Table} = TblInfo, ClusterId) ->
    case ets:first(Table) of
        {ClusterId, VNodeId} ->
            VNodeId;
        {_,_} ->
            next(TblInfo, ClusterId, 0);
        Other ->
            Other
    end.


%% @doc Retrieve a last record from the table
%%
-spec(last(TableInfo, ClusterId) ->
             integer() | '$end_of_table' when TableInfo::table_info(),
                                              ClusterId::cluster_id()).
last({?DB_MNESIA,_Table} = TblInfo, ClusterId) ->
    case find_by_cluster_id(TblInfo, ClusterId) of
        {ok, RetL} ->
            [#?RING{vnode_id = VNodeId}|_] = lists:reverse(RetL),
            VNodeId;
        not_found ->
            '$end_of_table';
        Other ->
            Other
    end;
last({?DB_ETS, Table} = TblInfo, ClusterId) ->
    case ets:last(Table) of
        {ClusterId, VNodeId} ->
            VNodeId;
        {_, VNodeId} ->
            prev(TblInfo, ClusterId, VNodeId);
        Other ->
            Other
    end.


%% @doc Retrieve a previous record from the table
%%
-spec(prev(TableInfo, ClusterId, VNodeId) ->
             integer() | '$end_of_table' when TableInfo::table_info(),
                                              ClusterId::cluster_id(),
                                              VNodeId::integer()).
prev({?DB_MNESIA, Table}, ClusterId, VNodeId) ->
    F = fun() ->
                Q1 = qlc:q([X || X <- mnesia:table(Table),
                                 X#?RING.cluster_id == ClusterId,
                                 X#?RING.vnode_id < VNodeId]),
                Q2 = qlc:sort(Q1, [{order, descending}]),
                qlc:e(Q2)
        end,
    case leo_mnesia:read(F) of
        {ok, [#?RING{vnode_id = VNodeId_1}|_]} ->
            VNodeId_1;
        not_found ->
            '$end_of_table';
        Other ->
            Other
    end;
prev({?DB_ETS, Table}, ClusterId, VNodeId) ->
    case ets:prev(Table, {ClusterId, VNodeId}) of
        {ClusterId, VNodeId_1} ->
            VNodeId_1;
        {_, VNodeId_1} when is_integer(VNodeId_1) ->
            '$end_of_table';
        Other ->
            Other
    end.


%% @doc Retrieve a next record from the table
%%
-spec(next(TableInfo, ClusterId, VNodeId) ->
             integer() | '$end_of_table' when TableInfo::table_info(),
                                              ClusterId::cluster_id(),
                                              VNodeId::integer()).
next({?DB_MNESIA, Table}, ClusterId, VNodeId) ->
    F = fun() ->
                Q1 = qlc:q([X || X <- mnesia:table(Table),
                                 X#?RING.cluster_id == ClusterId,
                                 X#?RING.vnode_id > VNodeId]),
                Q2 = qlc:sort(Q1, [{order, ascending}]),
                qlc:e(Q2)
        end,
    case leo_mnesia:read(F) of
        {ok, [#?RING{vnode_id = VNodeId_1}|_]} ->
            VNodeId_1;
        not_found ->
            '$end_of_table';
        Other ->
            Other
    end;
next({?DB_ETS, Table}, ClusterId, VNodeId) ->
    case ets:next(Table, {ClusterId, VNodeId}) of
        {ClusterId, VNodeId_1} ->
            VNodeId_1;
        {_, VNodeId_1} when is_integer(VNodeId_1) ->
            '$end_of_table';
        Other ->
            Other
    end.


%% @doc Remove all objects from the table
%%
-spec(delete_all(TableInfo, ClusterId) ->
             ok when TableInfo::table_info(),
                     ClusterId::cluster_id()).
delete_all(TblInfo, ClusterId) ->
    case find_by_cluster_id(TblInfo, ClusterId) of
        {ok, RetL} ->
            delete_all_1(TblInfo, RetL);
        not_found ->
            ok;
        Other ->
            Other
    end.

%% @private
delete_all_1(_,[]) ->
    ok;
delete_all_1(TblInfo, [#?RING{cluster_id = ClusterId,
                              vnode_id = VNodeId}|Rest]) ->
    ok = delete(TblInfo, ClusterId, VNodeId),
    delete_all_1(TblInfo, Rest).


%% @doc Retrieve total of records
%%
-spec(size(TableInfo, ClusterId) ->
             integer() when TableInfo::table_info(),
                            ClusterId::cluster_id()).
size(TblInfo, ClusterId) ->
    case find_by_cluster_id(TblInfo, ClusterId) of
        {ok, RetL} ->
            erlang:length(RetL);
        not_found ->
            0;
        Other ->
            Other
    end.


%% @doc Overwrite current records by source records
%%
-spec(overwrite(TableInfo, TableInfo, ClusterId) ->
             ok | {error, any()} when TableInfo::table_info(),
                                      ClusterId::cluster_id()).
overwrite(SrcTableInfo, DestTableInfo, ClusterId) ->
    case find_by_cluster_id(SrcTableInfo, ClusterId) of
        {error, Cause} ->
            {error, Cause};
        [] ->
            delete_all(DestTableInfo, ClusterId);
        List ->
            Ret = case find_by_cluster_id(DestTableInfo, ClusterId) of

                      {error, Reason} ->
                          {error, Reason};
                      [] ->
                          ok;
                      _ ->
                          delete_all(DestTableInfo, ClusterId)
                  end,

            case  Ret of
                ok ->
                    overwrite_1(DestTableInfo, List);
                Error ->
                    Error
            end
    end.

%% @private
-spec(overwrite_1(TableInfo, Items) ->
             ok | {error, any()} when TableInfo::{?DB_MNESIA, atom()} |
                                                 {?DB_ETS, atom()},
                                      Items::[tuple()]|[#?RING{}]).
overwrite_1({?DB_MNESIA,_} = TableInfo,Items) ->
    case mnesia:transaction(
           fun() ->
                   overwrite_2(TableInfo, Items)
           end) of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
            {error, Reason}
    end;
overwrite_1({?DB_ETS,_} = TableInfo, Items) ->
    overwrite_2(TableInfo, Items).

%% @private
-spec(overwrite_2(TableInfo, Items) ->
             ok | {error, any()} when TableInfo::{?DB_MNESIA, atom()} |
                                                 {?DB_ETS, atom()},
                                      Items::[tuple()]|[#?RING{}]).
overwrite_2(_,[]) ->
    ok;
overwrite_2({?DB_MNESIA, Table} = TableInfo, [#?RING{} = Ring|Rest]) ->
    case mnesia:write(Table, Ring, write) of
        ok ->
            overwrite_2(TableInfo, Rest);
        _ ->
            mnesia:abort("Not inserted")
    end;
overwrite_2({?DB_MNESIA, Table} = TableInfo, [{VNodeId, Node, Clock}|Rest]) ->
    case mnesia:write(Table, #?RING{vnode_id = VNodeId,
                                    node     = Node,
                                    clock    = Clock}, write) of
        ok ->
            overwrite_2(TableInfo, Rest);
        _ ->
            mnesia:abort("Not inserted")
    end;
overwrite_2({?DB_ETS,_} = TableInfo, [#?RING{vnode_id = VNodeId,
                                             node     = Node,
                                             clock    = Clock}|Rest]) ->
    Ring = {VNodeId, Node, Clock},
    case insert(TableInfo, Ring) of
        ok ->
            overwrite_2(TableInfo, Rest);
        Error ->
            Error
    end;
overwrite_2({?DB_ETS,_} = TableInfo, [Ring|Rest]) ->
    case insert(TableInfo, Ring) of
        ok ->
            overwrite_2(TableInfo, Rest);
        Error ->
            Error
    end.
