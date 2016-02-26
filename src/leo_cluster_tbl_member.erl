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
%% @doc The cluster member table operation
%% @reference https://github.com/leo-project/leo_redundant_manager/blob/master/src/leo_cluster_tbl_member.erl
%% @end
%%======================================================================
-module(leo_cluster_tbl_member).

-include("leo_redundant_manager.hrl").
-include_lib("stdlib/include/qlc.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([create_table/1, create_table/3,
         lookup/2, lookup/3, lookup/4,

         find_by_cluster_id/1, find_by_cluster_id/2, find_by_cluster_id/3,
         find_by_status/2, find_by_status/3, find_by_status/4,
         find_by_level1/3, find_by_level1/4, find_by_level1/5,
         find_by_level2/2, find_by_level2/3, find_by_level2/4,

         find_by_alias/2, find_by_alias/3, find_by_alias/4,
         find_by_name/3, find_by_name/4,

         insert/1, insert/2, insert/3,
         bulk_insert/2,
         delete/2, delete/3, delete/4,
         delete_all/2, delete_all/3,
         replace/2, replace/3, replace/4,
         overwrite/3,

         table_size/1, table_size/2, table_size/3,
         %% tab2list/1, tab2list/2, tab2list/3,
         first/2, next/3
        ]).
-export([transform/0]).


-ifdef(TEST).
-define(table_type(_), ?DB_ETS).
-else.
-define(table_type(EnvClusterId),
        case leo_misc:get_env(?APP, ?id_red_type(EnvClusterId)) of
            {ok, Value} when Value == ?MONITOR_NODE ->
                ?DB_MNESIA;
            {ok, Value} when Value == ?PERSISTENT_NODE ->
                ?DB_ETS;
            {ok, Value} when Value == ?WORKER_NODE ->
                ?DB_ETS;
            _Error ->
                undefined
        end).
-endif.


%% @doc Create the member table.
-spec(create_table(Table) ->
             ok when Table::member_table()).
create_table(Table) ->
    catch ets:new(Table, [named_table, set, public, {read_concurrency, true}]),
    ok.

-spec(create_table(DiscCopy, Nodes, Table) ->
             ok when DiscCopy::mnesia_copies(),
                     Nodes::[atom()],
                     Table::member_table()).
create_table(DiscCopy, Nodes, Table) ->
    case mnesia:create_table(
           Table,
           [{DiscCopy, Nodes},
            {type, set},
            {record_name, ?MEMBER},
            {attributes, record_info(fields, ?MEMBER)}
           ]) of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
            {error, Reason}
    end.


%% @doc Retrieve a record by key from the table.
-spec(lookup(ClusterId, Node) ->
             {ok, #?MEMBER{}} |
             not_found |
             {error, any()} when ClusterId::cluster_id(),
                                 Node::atom()).
lookup(ClusterId, Node) ->
    lookup(?MEMBER_TBL_CUR, ClusterId, Node).

-spec(lookup(Table, ClusterId, Node) ->
             {ok, #?MEMBER{}} |
             not_found |
             {error, any()} when Table::atom(),
                                 ClusterId::cluster_id(),
                                 Node::atom()).
lookup(Table, ClusterId, Node) ->
    lookup(?table_type(ClusterId), Table, ClusterId, Node).

-spec(lookup(DBType, Table, ClusterId, Node) ->
             {ok, #?MEMBER{}} |
             not_found |
             {error, any()} when DBType::?DB_ETS|?DB_MNESIA,
                                 Table::atom(),
                                 ClusterId::cluster_id(),
                                 Node::atom()).
lookup(?DB_MNESIA, Table, ClusterId, Node) ->
    F = fun() ->
                Q = qlc:q([X || X <- mnesia:table(Table),
                                X#?MEMBER.cluster_id == ClusterId,
                                X#?MEMBER.node == Node]),
                qlc:e(Q)
        end,
    case leo_mnesia:read(F) of
        {ok, [H|_]} ->
            {ok, H};
        Other ->
            Other
    end;
lookup(?DB_ETS, Table, ClusterId, Node) ->
    case catch ets:lookup(Table, {ClusterId, Node}) of
        [{_,_}|_] = RetL ->
            case lists:foldl(
                   fun({_,#?MEMBER{cluster_id = CId} = Member}, Acc)
                         when ClusterId == CId ->
                           [Member|Acc];
                      (_,Acc) ->
                           Acc
                   end, [], RetL) of
                [] ->
                    not_found;
                RetL_1 ->
                    {ok, erlang:hd(RetL_1)}
            end;
        [] ->
            not_found;
        undefined ->
            {error, 'invalid_table_name'};
        {'EXIT', Cause} ->
            {error, Cause}
    end;
lookup(_,_,_,_) ->
    {error, invalid_db}.


%% @doc Retrieve all members from the table.
-spec(find_by_cluster_id(ClusterId) ->
             {ok, [#?MEMBER{}]} | not_found | {error, any()} when ClusterId::cluster_id()).
find_by_cluster_id(ClusterId) ->
    find_by_cluster_id(?MEMBER_TBL_CUR, ClusterId).

-spec(find_by_cluster_id(Table, ClusterId) ->
             {ok, [#?MEMBER{}]} |
             not_found |
             {error, any()} when Table::member_table(),
                                 ClusterId::cluster_id()).
find_by_cluster_id(Table, ClusterId) ->
    find_by_cluster_id(?table_type(ClusterId), Table, ClusterId).

-spec(find_by_cluster_id(DBType, Table, ClusterId) ->
             {ok, [#?MEMBER{}]} |
             not_found |
             {error, any()} when DBType::?DB_ETS|?DB_MNESIA,
                                 Table::member_table(),
                                 ClusterId::cluster_id()).
find_by_cluster_id(?DB_MNESIA, Table, ClusterId) ->
    F = fun() ->
                Q1 = qlc:q([X || X <- mnesia:table(Table),
                                 X#?MEMBER.cluster_id == ClusterId]),
                Q2 = qlc:sort(Q1, [{order, ascending}]),
                qlc:e(Q2)
        end,
    leo_mnesia:read(F);
find_by_cluster_id(?DB_ETS, Table, ClusterId) ->
    case catch ets:foldl(
                 fun({_, #?MEMBER{cluster_id = CId} = Member}, Acc)
                       when ClusterId == CId ->
                         [Member|Acc];
                    (_,Acc) ->
                         Acc
                 end, [], Table) of
        {'EXIT', Cause} ->
            {error, Cause};
        [] ->
            not_found;
        RetL ->
            {ok, lists:sort(RetL)}
    end;
find_by_cluster_id(_,_,_) ->
    {error, invalid_db}.


%% @doc Retrieve members by status
-spec(find_by_status(ClusterId, Status) ->
             {ok, [#?MEMBER{}]} |
             not_found |
             {error, any()} when ClusterId::cluster_id(),
                                 Status::atom()).
find_by_status(ClusterId, Status) ->
    find_by_status(?MEMBER_TBL_CUR, ClusterId, Status).

-spec(find_by_status(Table, ClusterId, Status) ->
             {ok, [#?MEMBER{}]} |
             not_found |
             {error, any()} when Table::member_table(),
                                 ClusterId::cluster_id(),
                                 Status::atom()).
find_by_status(Table, ClusterId, Status) ->
    find_by_status(?table_type(ClusterId), Table, ClusterId, Status).

-spec(find_by_status(DBType, Table, ClusterId, Status) ->
             {ok, [#?MEMBER{}]} |
             not_found |
             {error, any()} when DBType::?DB_ETS|?DB_MNESIA,
                                 Table::member_table(),
                                 ClusterId::cluster_id(),
                                 Status::atom()).
find_by_status(?DB_MNESIA, Table, ClusterId, Status) ->
    F = fun() ->
                Q = qlc:q([X || X <- mnesia:table(Table),
                                X#?MEMBER.cluster_id == ClusterId,
                                X#?MEMBER.state == Status]),
                qlc:e(Q)
        end,
    leo_mnesia:read(F);
find_by_status(?DB_ETS, Table, ClusterId, Status) ->
    case catch ets:foldl(
                 fun({_, #?MEMBER{cluster_id = ClusterId_1,
                                  state = Status_1} = Member}, Acc)
                       when ClusterId == ClusterId_1,
                            Status == Status_1 ->
                         [Member|Acc];
                    (_, Acc) ->
                         Acc
                 end, [], Table) of
        {'EXIT', Cause} ->
            {error, Cause};
        [] ->
            not_found;
        Ret ->
            {ok, lists:sort(Ret)}
    end;
find_by_status(_,_,_,_) ->
    {error, invalid_db}.


%% @doc Retrieve records by L1 and L2
-spec(find_by_level1(ClusterId, L1, L2) ->
             {ok, list()} |
             not_found |
             {error, any()} when ClusterId::cluster_id(),
                                 L1::atom(),
                                 L2::atom()).
find_by_level1(ClusterId, L1, L2) ->
    find_by_level1(?MEMBER_TBL_CUR, ClusterId, L1, L2).

-spec(find_by_level1(Table, ClusterId, L1, L2) ->
             {ok, list()} |
             not_found |
             {error, any()} when Table::member_table(),
                                 ClusterId::cluster_id(),
                                 L1::atom(),
                                 L2::atom()).
find_by_level1(Table, ClusterId, L1, L2) ->
    find_by_level1(?table_type(ClusterId), Table, ClusterId, L1, L2).

-spec(find_by_level1(DBType, Table, ClusterId, L1, L2) ->
             {ok, list()} |
             not_found |
             {error, any()} when DBType::?DB_ETS|?DB_MNESIA,
                                 Table::member_table(),
                                 ClusterId::cluster_id(),
                                 L1::atom(),
                                 L2::atom()).
find_by_level1(?DB_MNESIA, Table, ClusterId, L1, L2) ->
    F = fun() ->
                Q1 = qlc:q([X || X <- mnesia:table(Table),
                                 X#?MEMBER.cluster_id == ClusterId,
                                 X#?MEMBER.grp_level_1 == L1,
                                 X#?MEMBER.grp_level_2 == L2]),
                Q2 = qlc:sort(Q1, [{order, ascending}]),
                qlc:e(Q2)
        end,
    leo_mnesia:read(F);
find_by_level1(?DB_ETS, Table, ClusterId, L1, L2) ->
    case catch ets:foldl(
                 fun({_, #?MEMBER{cluster_id = ClusterId_1,
                                  grp_level_1 = L1_1,
                                  grp_level_2 = L2_1} = Member}, Acc)
                       when ClusterId == ClusterId_1,
                            L1 == L1_1,
                            L2 == L2_1 ->
                         [Member|Acc];
                    (_, Acc) ->
                         Acc
                 end, [], Table) of
        {'EXIT', Cause} ->
            {error, Cause};
        [] ->
            not_found;
        Ret ->
            {ok, lists:sort(Ret)}
    end;
find_by_level1(_,_,_,_,_) ->
    {error, invalid_db}.


%% @doc Retrieve records by L2
-spec(find_by_level2(ClusterId, L2) ->
             {ok, list()} |
             not_found |
             {error, any()} when ClusterId::cluster_id(),
                                 L2::atom()).
find_by_level2(ClusterId, L2) ->
    find_by_level2(?MEMBER_TBL_CUR, ClusterId, L2).

-spec(find_by_level2(Table, ClusterId, L2) ->
             {ok, list()} |
             not_found |
             {error, any()} when Table::member_table(),
                                 ClusterId::cluster_id(),
                                 L2::atom()).
find_by_level2(Table, ClusterId, L2) ->
    find_by_level2(?table_type(ClusterId), Table, ClusterId, L2).

-spec(find_by_level2(DBType, Table, ClusterId, L2) ->
             {ok, list()} |
             not_found |
             {error, any()} when DBType::?DB_ETS|?DB_MNESIA,
                                 Table::member_table(),
                                 ClusterId::cluster_id(),
                                 L2::atom()).
find_by_level2(?DB_MNESIA, Table, ClusterId, L2) ->
    F = fun() ->
                Q1 = qlc:q([X || X <- mnesia:table(Table),
                                 X#?MEMBER.cluster_id == ClusterId,
                                 X#?MEMBER.grp_level_2 == L2]),
                Q2 = qlc:sort(Q1, [{order, ascending}]),
                qlc:e(Q2)
        end,
    leo_mnesia:read(F);
find_by_level2(?DB_ETS, Table, ClusterId, L2) ->
    case catch ets:foldl(
                 fun({_, #?MEMBER{cluster_id = ClusterId_1,
                                  grp_level_2 = L2_1} = Member}, Acc)
                       when ClusterId == ClusterId_1,
                            L2 == L2_1 ->
                         [Member|Acc];
                    (_, Acc) ->
                         Acc
                 end, [], Table) of
        {'EXIT', Cause} ->
            {error, Cause};
        [] ->
            not_found;
        Ret ->
            {ok, lists:sort(Ret)}
    end;
find_by_level2(_,_,_,_) ->
    {error, invalid_db}.


%% @doc Retrieve records by alias
-spec(find_by_alias(ClusterId, Alias) ->
             {ok, list()} |
             not_found |
             {error, any()} when ClusterId::cluster_id(),
                                 Alias::string()).
find_by_alias(ClusterId, Alias) ->
    find_by_alias(?MEMBER_TBL_CUR, ClusterId, Alias).

-spec(find_by_alias(Table, ClusterId, Alias) ->
             {ok, list()} |
             not_found |
             {error, any()} when Table::atom(),
                                 ClusterId::cluster_id(),
                                 Alias::string()).
find_by_alias(Table, ClusterId, Alias) ->
    find_by_alias(?table_type(ClusterId), Table, ClusterId, Alias).

-spec(find_by_alias(DBType, Table, ClusterId, Alias) ->
             {ok, list()} |
             not_found |
             {error, any()} when DBType::?DB_MNESIA|?DB_ETS,
                                 Table::atom(),
                                 ClusterId::cluster_id(),
                                 Alias::string()).
find_by_alias(?DB_MNESIA, Table, ClusterId, Alias) ->
    F = fun() ->
                Q1 = qlc:q([X || X <- mnesia:table(Table),
                                 X#?MEMBER.cluster_id == ClusterId,
                                 X#?MEMBER.alias == Alias]),
                Q2 = qlc:sort(Q1, [{order, ascending}]),
                qlc:e(Q2)
        end,
    leo_mnesia:read(F);
find_by_alias(?DB_ETS, Table, ClusterId, Alias) ->
    case catch ets:foldl(
                 fun({_, #?MEMBER{cluster_id = ClusterId_1,
                                  alias = Alias_1} = Member}, Acc)
                       when ClusterId == ClusterId_1,
                            Alias == Alias_1 ->
                         [Member|Acc];
                    (_, Acc) ->
                         Acc
                 end, [], Table) of
        {'EXIT', Cause} ->
            {error, Cause};
        [] ->
            not_found;
        Ret ->
            {ok, lists:sort(Ret)}
    end;
find_by_alias(_,_,_,_) ->
    {error, invalid_db}.


%% @doc Retrieve records by name
-spec(find_by_name(Table, ClusterId, Name) ->
             {ok, list()} |
             not_found |
             {error, any()} when Table::atom(),
                                 ClusterId::cluster_id(),
                                 Name::atom()).
find_by_name(Table,_ClusterId, Name) ->
    find_by_name(?table_type(_ClusterId), Table, Name).

-spec(find_by_name(DBType, Table, ClusterId, Name) ->
             {ok, list()} |
             not_found |
             {error, any()} when DBType::?DB_MNESIA|?DB_ETS,
                                 Table::atom(),
                                 ClusterId::cluster_id(),
                                 Name::atom()).
find_by_name(?DB_MNESIA, Table, ClusterId, Name) ->
    F = fun() ->
                Q1 = qlc:q([X || X <- mnesia:table(Table),
                                 X#?MEMBER.cluster_id == ClusterId,
                                 X#?MEMBER.node == Name]),
                Q2 = qlc:sort(Q1, [{order, ascending}]),
                qlc:e(Q2)
        end,
    leo_mnesia:read(F);
find_by_name(?DB_ETS, Table, ClusterId, Name) ->
    case catch ets:foldl(
                 fun({_, #?MEMBER{cluster_id = ClusterId_1,
                                  node = Name_1} = Member}, Acc)
                       when ClusterId == ClusterId_1,
                            Name == Name_1 ->
                         [Member|Acc];
                    (_, Acc) ->
                         Acc
                 end, [], Table) of
        {'EXIT', Cause} ->
            {error, Cause};
        [] ->
            not_found;
        Ret ->
            {ok, lists:sort(Ret)}
    end;
find_by_name(_,_,_,_) ->
    {error, invalid_db}.


%% @doc Insert a record into the table.
-spec(insert(Member) ->
             ok | {error, any} when Member::#?MEMBER{}).
insert(Member) ->
    insert(?MEMBER_TBL_CUR, Member).

-spec(insert(Table, Member) ->
             ok | {error, any} when Table::member_table(),
                                    Member::#?MEMBER{}).
insert(Table, #?MEMBER{cluster_id = _ClusterId} = Member) ->
    insert(?table_type(_ClusterId), Table, Member).

-spec(insert(DBType, Table, Member) ->
             ok | {error, any} when DBType::?DB_ETS|?DB_MNESIA,
                                    Table::member_table(),
                                    Member::#?MEMBER{}).
insert(DBType, Table, #?MEMBER{cluster_id = ClusterId,
                               node = Node,
                               state = State,
                               clock = Clock} = Member) ->
    Ret = case lookup(DBType, Table, ClusterId, Node) of
              {ok, #?MEMBER{state = State,
                            clock = Clock_1}} when Clock > Clock_1 ->
                  ok;
              {ok, #?MEMBER{state = State,
                            clock = Clock_1}} when Clock =< Clock_1 ->
                  {error, ignore};
              {ok,_} ->
                  ok;
              not_found ->
                  ok;
              {error, Cause} ->
                  {error, Cause}
          end,
    insert_1(Ret, DBType, Table, Member).

%% @private
insert_1(ok, ?DB_MNESIA, Table, Member) ->
    Fun = fun() ->
                  mnesia:write(Table, Member, write)
          end,
    leo_mnesia:write(Fun);
insert_1(ok, ?DB_ETS, Table, Member) ->
    #?MEMBER{cluster_id = ClusterId,
             node = Node} = Member,
    case catch ets:insert(Table, {{ClusterId, Node}, Member}) of
        true ->
            ok;
        {'EXIT', Cause} ->
            {error, Cause}
    end;
insert_1({error, ignore},_,_,_) ->
    ok;
insert_1({error, Cause},_,_,_) ->
    {error, Cause}.


%% @doc Insert members at one time
bulk_insert(_,[]) ->
    ok;
bulk_insert(Tbl, [Member|Rest]) ->
    case insert(Tbl, Member) of
        ok ->
            bulk_insert(Tbl, Rest);
        Error ->
            Error
    end.


%% @doc Remove a record from the table.
-spec(delete(ClusterId, Node) ->
             ok | {error, any} when ClusterId::cluster_id(),
                                    Node::atom()).
delete(ClusterId, Node) ->
    delete(?MEMBER_TBL_CUR, ClusterId, Node).

-spec(delete(Table, ClusterId, Node) ->
             ok | {error, any} when Table::member_table(),
                                    ClusterId::cluster_id(),
                                    Node::atom()).
delete(Table, ClusterId, Node) ->
    delete(?table_type(ClusterId), Table, ClusterId, Node).

-spec(delete(DBType, Table, ClusterId, Node) ->
             ok | {error, any} when DBType::?DB_ETS|?DB_MNESIA,
                                    Table::member_table(),
                                    ClusterId::cluster_id(),
                                    Node::atom()).
delete(?DB_MNESIA, Table, ClusterId, Node) ->
    case lookup(?DB_MNESIA, Table, ClusterId, Node) of
        {ok, Member} ->
            Fun = fun() ->
                          mnesia:delete_object(Table, Member, write)
                  end,
            leo_mnesia:delete(Fun);
        Error ->
            Error
    end;
delete(?DB_ETS, Table, ClusterId, Node) ->
    case catch ets:delete(Table, {ClusterId, Node}) of
        true ->
            ok;
        {'EXIT', Cause} ->
            {error, Cause}
    end;
delete(_,_,_,_) ->
    {error, invalid_db}.


%% @doc Remove all records
-spec(delete_all(Table, ClusterId) ->
             ok | {error, any()} when Table::member_table(),
                                      ClusterId::cluster_id()).
delete_all(Table, ClusterId) ->
    delete_all(?table_type(ClusterId), Table, ClusterId).

-spec(delete_all(DBType, Table, ClusterId) ->
             ok | {error, any()} when DBType::?DB_ETS|?DB_MNESIA,
                                      Table::member_table(),
                                      ClusterId::cluster_id()).
delete_all(DBType, Table, ClusterId) ->
    case find_by_cluster_id(DBType, Table, ClusterId) of
        {ok, RetL} when  DBType == ?DB_MNESIA ->
            case mnesia:transaction(
                   fun() ->
                           case delete_all_1(RetL, DBType, Table) of
                               ok ->
                                   ok;
                               _ ->
                                   mnesia:abort("Could Not remove due to an error")
                           end
                   end) of
                {atomic, ok} ->
                    ok;
                {aborted, Reason} ->
                    {error, Reason}
            end;
        {ok, RetL} when  DBType == ?DB_ETS ->
            delete_all_1(RetL, DBType, Table);
        not_found ->
            ok;
        Error ->
            Error
    end.

%% @private
delete_all_1([],_,_) ->
    ok;
delete_all_1([#?MEMBER{} = Member|Rest], ?DB_MNESIA = DBType, Table) ->
    case mnesia:delete_object(Table, Member, write) of
        ok ->
            delete_all_1(Rest, DBType, Table);
        _ ->
            {error, transaction_abort}
    end;
delete_all_1([#?MEMBER{cluster_id = ClusterId,
                       node = Node}|Rest], ?DB_ETS = DBType, Table) ->
    ok = delete(DBType, Table, ClusterId, Node),
    delete_all_1(Rest, DBType, Table);
delete_all_1(_,_,_) ->
    ok.


%% @doc Replace members into the db.
-spec(replace(OldMembers, NewMembers) ->
             ok | {error, any()} when OldMembers::[#?MEMBER{}],
                                      NewMembers::[#?MEMBER{}]).
replace(OldMembers, NewMembers) ->
    replace(?MEMBER_TBL_CUR, OldMembers, NewMembers).

-spec(replace(Table, OldMembers, NewMembers) ->
             ok | {error, any()} when Table::member_table(),
                                      OldMembers::[#?MEMBER{}],
                                      NewMembers::[#?MEMBER{}]).
replace(Table, OldMembers, [#?MEMBER{cluster_id = _ClusterId}|_]= NewMembers) ->
    replace(?table_type(_ClusterId), Table, OldMembers, NewMembers).

-spec(replace(DBType, Table, OldMembers, NewMembers) ->
             ok | {error, any()} when DBType::?DB_ETS | ?DB_MNESIA,
                                      Table::member_table(),
                                      OldMembers::[#?MEMBER{}],
                                      NewMembers::[#?MEMBER{}]).
replace(?DB_MNESIA, Table, OldMembers, NewMembers) ->
    Fun = fun() ->
                  lists:foreach(fun(Item_1) ->
                                        mnesia:delete_object(Table, Item_1, write)
                                end, OldMembers),
                  lists:foreach(fun(Item_2) ->
                                        mnesia:write(Table, Item_2, write)
                                end, NewMembers)
          end,
    leo_mnesia:batch(Fun);
replace(DBType, Table, OldMembers, NewMembers) ->
    lists:foreach(fun(#?MEMBER{cluster_id = ClusterId,
                               node = Node}) ->
                          delete(DBType, Table, {ClusterId, Node})
                  end, OldMembers),
    lists:foreach(fun(Item) ->
                          insert(DBType, Table, Item)
                  end, NewMembers),
    ok.


%% @doc Overwrite current records by source records
-spec(overwrite(SrcTable, DestTable, ClusterId) ->
             ok | {error, any()} when SrcTable::member_table(),
                                      DestTable::member_table(),
                                      ClusterId::cluster_id()).
overwrite(SrcTable, DestTable, ClusterId) ->
    case find_by_cluster_id(SrcTable, ClusterId) of
        {error, Cause} ->
            {error, Cause};
        not_found ->
            delete_all(DestTable, ClusterId);
        {ok, Members} ->
            overwrite_1(?table_type(ClusterId),
                        DestTable, ClusterId, Members)
    end.

%% @private
overwrite_1(?DB_MNESIA = DB, Table, ClusterId, Members) ->
    case mnesia:transaction(
           fun() ->
                   overwrite_2(DB, Table, ClusterId, Members)
           end) of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
            {error, Reason}
    end;
overwrite_1(?DB_ETS = DB, Table, ClusterId, Members) ->
    overwrite_2(DB, Table, ClusterId, Members).

%% @private
overwrite_2(_,_,_,[]) ->
    ok;
overwrite_2(?DB_MNESIA = DB, Table,
            ClusterId, [#?MEMBER{node = Node} = Member|Rest]) ->
    case mnesia:delete(Table, Node, write) of
        ok ->
            case mnesia:write(
                   Table, Member#?MEMBER{cluster_id = ClusterId}, write) of
                ok ->
                    overwrite_2(DB, Table, ClusterId, Rest);
                _ ->
                    mnesia:abort("Not inserted")
            end;
        _ ->
            mnesia:abort("Not removed")
    end;
overwrite_2(?DB_ETS = DB, Table,
            ClusterId, [#?MEMBER{node = Node} = Member|Rest]) ->
    case delete(?DB_ETS, Table, ClusterId, Node) of
        ok ->
            case insert(?DB_ETS, Table, Member) of
                ok ->
                    overwrite_2(DB, Table, ClusterId, Rest);
                Error ->
                    Error
            end;
        Error ->
            Error
    end.


%% @doc Retrieve total of records.
-spec(table_size(ClusterId) ->
             integer() | {error, any()} when ClusterId::cluster_id()).
table_size(ClusterId) ->
    table_size(?MEMBER_TBL_CUR, ClusterId).

-spec(table_size(Table, ClusterId) ->
             integer() | {error, any()} when Table::atom(),
                                             ClusterId::cluster_id()).
table_size(Table, ClusterId) ->
    table_size(?table_type(ClusterId), Table, ClusterId).

-spec(table_size(DBType, Table, ClusterId) ->
             integer() | {error, any()} when DBType::?DB_ETS|?DB_MNESIA,
                                             Table::atom(),
                                             ClusterId::cluster_id()).
table_size(DBType, Table, ClusterId) ->
    case find_by_cluster_id(DBType, Table, ClusterId) of
        {ok, RetL} ->
            erlang:length(RetL);
        not_found ->
            0;
        Other ->
            Other
    end.


%% @doc Go to first record
-spec(first(Table, ClusterId) ->
             atom() | '$end_of_table' when Table::atom(),
                                           ClusterId::cluster_id()).
first(Table, ClusterId) ->
    first(?table_type(ClusterId), Table, ClusterId).

%% @private
-spec(first(DBType, Table, ClusterId) ->
             atom() | '$end_of_table' when DBType::?DB_MNESIA|?DB_ETS,
                                           ClusterId::cluster_id(),
                                           Table::atom()).
first(DBType, Table, ClusterId) ->
    case find_by_cluster_id(DBType, Table, ClusterId) of
        {ok, [#?MEMBER{node = Node}|_]} ->
            Node;
        not_found ->
            '$end_of_table';
        Other ->
            Other
    end.


%% @doc Go to next record
-spec(next(Table, ClusterId, Node) ->
             atom() | '$end_of_table' when Table::atom(),
                                           ClusterId::cluster_id(),
                                           Node::atom()).
next(Table, ClusterId, Node) ->
    next(?table_type(ClusterId), Table, ClusterId, Node).

%% @private
-spec(next(DBType, Table, ClusterId, Node) ->
             atom() | '$end_of_table' when DBType::?DB_MNESIA|?DB_ETS,
                                           Table::atom(),
                                           ClusterId::cluster_id(),
                                           Node::atom()).
next(?DB_MNESIA, Table, ClusterId, Node) ->
    F = fun() ->
                Q1 = qlc:q([X || X <- mnesia:table(Table),
                                 X#?MEMBER.cluster_id == ClusterId,
                                 X#?MEMBER.node > Node]),
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
next(?DB_ETS, Table, ClusterId, Node) ->
    case ets:next(Table, {ClusterId, Node}) of
        {ClusterId, Node_1} ->
            Node_1;
        {_, Node_1} when is_atom(Node_1) ->
            '$end_of_table';
        Other ->
            Other
    end.


%% @TODO
%% @doc Transform records
-spec(transform() ->
             ok | {error, any()}).
transform() ->
    ok.
