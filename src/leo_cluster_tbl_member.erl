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
%% @doc The cluster member table operation
%% @reference https://github.com/leo-project/leo_redundant_manager/blob/master/src/leo_cluster_tbl_member.erl
%% @end
%%======================================================================
-module(leo_cluster_tbl_member).
-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").
-include_lib("stdlib/include/qlc.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([create_table/1, create_table/3,
         lookup/1, lookup/2, lookup/3,
         find_all/0, find_all/1, find_all/2,
         find_by_status/1, find_by_status/2, find_by_status/3,
         find_by_level1/2, find_by_level1/3, find_by_level1/4,
         find_by_level2/1, find_by_level2/2, find_by_level2/3,
         find_by_alias/1, find_by_alias/2, find_by_alias/3,
         find_by_name/2, find_by_name/3,
         insert/1, insert/2, insert/3,
         delete/1, delete/2, delete/3, delete_all/1, delete_all/2,
         replace/2, replace/3, replace/4,
         overwrite/2,
         table_size/0, table_size/1, table_size/2,
         tab2list/0, tab2list/1, tab2list/2,
         first/1, next/2
        ]).
-export([transform/0, transform/1]).


-ifdef(TEST).
-define(table_type(), ?DB_ETS).
-else.
-define(table_type(), case leo_misc:get_env(?APP, ?PROP_SERVER_TYPE) of
                          {ok, Value} when Value == ?MONITOR_NODE -> ?DB_MNESIA;
                          {ok, Value} when Value == ?PERSISTENT_NODE -> ?DB_ETS;
                          {ok, Value} when Value == ?WORKER_NODE -> ?DB_ETS;
                          _Error ->
                              undefined
                      end).
-endif.


%% @doc Create the member table.
%%
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
            {record_name, member},
            {attributes, record_info(fields, member)},
            {user_properties,
             [{node,          varchar, primary},
              {alias,         varchar, false},
              {ip,            varchar, false},
              {port,          integer, false},
              {inet,          varchar, false},
              {clock,         integer, false},
              {num_of_vnodes, integer, false},
              {state,         varchar, false},
              {grp_level_1,   varchar, false},
              {grp_level_2,   varchar, false}
             ]}
           ]) of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
            {error, Reason}
    end.


%% @doc Retrieve a record by key from the table.
%%
-spec(lookup(Node) ->
             {ok, #member{}} |
             not_found |
             {error, any()} when Node::atom()).
lookup(Node) ->
    lookup(?MEMBER_TBL_CUR, Node).

-spec(lookup(Table, Node) ->
             {ok, #member{}} |
             not_found |
             {error, any()} when Table::atom(),
                                 Node::atom()).
lookup(Table, Node) ->
    lookup(?table_type(), Table, Node).

-spec(lookup(DBType, Table, Node) ->
             {ok, #member{}} |
             not_found |
             {error, any()} when DBType::?DB_ETS|?DB_MNESIA,
                                 Table::atom(),
                                 Node::atom()).
lookup(?DB_MNESIA, Table, Node) ->
    case catch mnesia:ets(fun ets:lookup/2, [Table, Node]) of
        [H|_T] ->
            {ok, H};
        [] ->
            not_found;
        undefined ->
            {error, 'invalid_table_name'};
        {'EXIT', Cause} ->
            {error, Cause}
    end;
lookup(?DB_ETS, Table, Node) ->
    case catch ets:lookup(Table, Node) of
        [{_, H}|_T] ->
            {ok, H};
        [] ->
            not_found;
        undefined ->
            {error, 'invalid_table_name'};
        {'EXIT', Cause} ->
            {error, Cause}
    end;
lookup(_,_,_) ->
    {error, invalid_db}.


%% @doc Retrieve all members from the table.
%%
-spec(find_all() ->
             {ok, [#member{}]} | not_found | {error, any()}).
find_all() ->
    find_all(?MEMBER_TBL_CUR).

-spec(find_all(Table) ->
             {ok, [#member{}]} |
             not_found |
             {error, any()} when Table::member_table()).
find_all(Table) ->
    find_all(?table_type(), Table).

-spec(find_all(DBType, Table) ->
             {ok, [#member{}]} |
             not_found |
             {error, any()} when DBType::?DB_ETS|?DB_MNESIA,
                                 Table::member_table()).
find_all(?DB_MNESIA, Table) ->
    F = fun() ->
                Q1 = qlc:q([X || X <- mnesia:table(Table)]),
                Q2 = qlc:sort(Q1, [{order, descending}]),
                qlc:e(Q2)
        end,
    leo_mnesia:read(F);
find_all(?DB_ETS, Table) ->
    case catch ets:foldl(
                 fun({_, Member}, Acc) ->
                         ordsets:add_element(Member, Acc)
                 end, [], Table) of
        {'EXIT', Cause} ->
            {error, Cause};
        [] ->
            not_found;
        Members ->
            {ok, Members}
    end;
find_all(_,_) ->
    {error, invalid_db}.


%% @doc Retrieve members by status
%%
-spec(find_by_status(Status) ->
             {ok, [#member{}]} |
             not_found |
             {error, any()} when Status::atom()).
find_by_status(Status) ->
    find_by_status(?MEMBER_TBL_CUR, Status).

-spec(find_by_status(Table, Status) ->
             {ok, [#member{}]} |
             not_found |
             {error, any()} when Table::member_table(),
                                 Status::atom()).
find_by_status(Table, Status) ->
    find_by_status(?table_type(), Table, Status).

-spec(find_by_status(DBType, Table, Status) ->
             {ok, [#member{}]} |
             not_found |
             {error, any()} when DBType::?DB_ETS|?DB_MNESIA,
                                 Table::member_table(),
                                 Status::atom()).
find_by_status(?DB_MNESIA, Table, St0) ->
    F = fun() ->
                Q = qlc:q([X || X <- mnesia:table(Table),
                                X#member.state == St0]),
                qlc:e(Q)
        end,
    leo_mnesia:read(F);
find_by_status(?DB_ETS, Table, St0) ->
    case catch ets:foldl(
                 fun({_, #member{state = St1} = Member}, Acc) when St0 == St1 ->
                         [Member|Acc];
                    (_, Acc) ->
                         Acc
                 end, [], Table) of
        {'EXIT', Cause} ->
            {error, Cause};
        [] ->
            not_found;
        Ret ->
            {ok, Ret}
    end;
find_by_status(_,_,_) ->
    {error, invalid_db}.


%% @doc Retrieve records by L1 and L2
%%
-spec(find_by_level1(L1, L2) ->
             {ok, list()} |
             not_found |
             {error, any()} when L1::atom(),
                                 L2::atom()).
find_by_level1(L1, L2) ->
    find_by_level1(?MEMBER_TBL_CUR, L1, L2).

-spec(find_by_level1(Table, L1, L2) ->
             {ok, list()} |
             not_found |
             {error, any()} when Table::member_table(),
                                 L1::atom(),
                                 L2::atom()).
find_by_level1(Table, L1, L2) ->
    find_by_level1(?table_type(), Table, L1, L2).

-spec(find_by_level1(DBType, Table, L1, L2) ->
             {ok, list()} |
             not_found |
             {error, any()} when DBType::?DB_ETS|?DB_MNESIA,
                                 Table::member_table(),
                                 L1::atom(),
                                 L2::atom()).
find_by_level1(?DB_MNESIA, Table, L1, L2) ->
    F = fun() ->
                Q = qlc:q([X || X <- mnesia:table(Table),
                                X#member.grp_level_1 == L1,
                                X#member.grp_level_2 == L2]),
                qlc:e(Q)
        end,
    leo_mnesia:read(F);
find_by_level1(?DB_ETS, Table, L1, L2) ->
    case catch ets:foldl(
                 fun({_, #member{grp_level_1 = L1_1,
                                 grp_level_2 = L2_1} = Member}, Acc) when L1 == L1_1,
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
            {ok, Ret}
    end;
find_by_level1(_,_,_,_) ->
    {error, invalid_db}.


%% @doc Retrieve records by L2
%%
-spec(find_by_level2(L2) ->
             {ok, list()} |
             not_found |
             {error, any()} when L2::atom()).
find_by_level2(L2) ->
    find_by_level2(?MEMBER_TBL_CUR, L2).

-spec(find_by_level2(Table, L2) ->
             {ok, list()} |
             not_found |
             {error, any()} when Table::member_table(),
                                 L2::atom()).
find_by_level2(Table, L2) ->
    find_by_level2(?table_type(), Table, L2).

-spec(find_by_level2(DBType, Table, L2) ->
             {ok, list()} |
             not_found |
             {error, any()} when DBType::?DB_ETS|?DB_MNESIA,
                                 Table::member_table(),
                                 L2::atom()).
find_by_level2(?DB_MNESIA, Table, L2) ->
    F = fun() ->
                Q = qlc:q([X || X <- mnesia:table(Table),
                                X#member.grp_level_2 == L2]),
                qlc:e(Q)
        end,
    leo_mnesia:read(F);
find_by_level2(?DB_ETS, Table, L2) ->
    case catch ets:foldl(
                 fun({_, #member{grp_level_2 = L2_1} = Member}, Acc) when L2 == L2_1 ->
                         [Member|Acc];
                    (_, Acc) ->
                         Acc
                 end, [], Table) of
        {'EXIT', Cause} ->
            {error, Cause};
        [] ->
            not_found;
        Ret ->
            {ok, Ret}
    end;
find_by_level2(_,_,_) ->
    {error, invalid_db}.


%% @doc Retrieve records by alias
%%
-spec(find_by_alias(Alias) ->
             {ok, list()} |
             not_found |
             {error, any()} when Alias::string()).
find_by_alias(Alias) ->
    find_by_alias(?MEMBER_TBL_CUR, Alias).

-spec(find_by_alias(Table, Alias) ->
             {ok, list()} |
             not_found |
             {error, any()} when Table::atom(),
                                 Alias::string()).
find_by_alias(Table, Alias) ->
    find_by_alias(?table_type(), Table, Alias).

-spec(find_by_alias(DBType, Table, Alias) ->
             {ok, list()} |
             not_found |
             {error, any()} when DBType::?DB_MNESIA|?DB_ETS,
                                 Table::atom(),
                                 Alias::string()).
find_by_alias(?DB_MNESIA, Table, Alias) ->
    F = fun() ->
                Q = qlc:q([X || X <- mnesia:table(Table),
                                X#member.alias == Alias]),
                qlc:e(Q)
        end,
    leo_mnesia:read(F);
find_by_alias(?DB_ETS, Table, Alias) ->
    case catch ets:foldl(
                 fun({_, #member{alias = Alias_1} = Member}, Acc) when Alias == Alias_1 ->
                         [Member|Acc];
                    (_, Acc) ->
                         Acc
                 end, [], Table) of
        {'EXIT', Cause} ->
            {error, Cause};
        [] ->
            not_found;
        Ret ->
            {ok, Ret}
    end;
find_by_alias(_,_,_) ->
    {error, invalid_db}.


%% @doc Retrieve records by name
%%
-spec(find_by_name(Table, Name) ->
             {ok, list()} |
             not_found |
             {error, any()} when Table::atom(),
                                 Name::atom()).
find_by_name(Table, Name) ->
    find_by_name(?table_type(), Table, Name).

-spec(find_by_name(DBType, Table, Name) ->
             {ok, list()} |
             not_found |
             {error, any()} when DBType::?DB_MNESIA|?DB_ETS,
                                 Table::atom(),
                                 Name::atom()).
find_by_name(?DB_MNESIA, Table, Name) ->
    F = fun() ->
                Q = qlc:q([X || X <- mnesia:table(Table),
                                X#member.node == Name]),
                qlc:e(Q)
        end,
    leo_mnesia:read(F);
find_by_name(?DB_ETS, Table, Name) ->
    case catch ets:foldl(
                 fun({_, #member{node = Name_1} = Member}, Acc) when Name == Name_1 ->
                         [Member|Acc];
                    (_, Acc) ->
                         Acc
                 end, [], Table) of
        {'EXIT', Cause} ->
            {error, Cause};
        [] ->
            not_found;
        Ret ->
            {ok, Ret}
    end;
find_by_name(_,_,_) ->
    {error, invalid_db}.


%% @doc Insert a record into the table.
%%
-spec(insert(Record) ->
             ok | {error, any} when Record::{atom(),
                                             #member{}}).
insert({Node, Member}) ->
    insert(?MEMBER_TBL_CUR, {Node, Member}).

-spec(insert(Table, Record) ->
             ok | {error, any} when Table::member_table(),
                                    Record::{atom(),
                                             #member{}}).
insert(Table, {Node, Member}) ->
    insert(?table_type(), Table, {Node, Member}).

-spec(insert(DBType, Table, Record) ->
             ok | {error, any} when DBType::?DB_ETS|?DB_MNESIA,
                                    Table::member_table(),
                                    Record::{atom(),
                                             #member{}}).

insert(DBType, Table, {_Node, Member}) ->
    #member{node  = Node,
            state = State,
            clock = Clock} = Member,
    Ret = case lookup(DBType, Table, Node) of
              {ok, #member{state = State,
                           clock = Clock_1}} when Clock > Clock_1 ->
                  ok;
              {ok, #member{state = State,
                           clock = Clock_1}} when Clock =< Clock_1 ->
                  {error, ignore};
              {ok,_} ->
                  ok;
              not_found ->
                  ok;
              {error, Cause} ->
                  {error, Cause}
          end,
    insert(Ret, DBType, Table, {_Node, Member}).

%% @private
insert(ok, ?DB_MNESIA, Table, {_, Member}) ->
    Fun = fun() ->
                  mnesia:write(Table, Member, write)
          end,
    leo_mnesia:write(Fun);
insert(ok, ?DB_ETS, Table, {Node, Member}) ->
    case catch ets:insert(Table, {Node, Member}) of
        true ->
            ok;
        {'EXIT', Cause} ->
            {error, Cause}
    end;
insert({error, ignore},_,_,_) ->
    ok;
insert({error, Cause},_,_,_) ->
    {error, Cause}.


%% @doc Remove a record from the table.
%%
-spec(delete(Node) ->
             ok | {error, any} when Node::atom()).
delete(Node) ->
    delete(?MEMBER_TBL_CUR, Node).

-spec(delete(Table, Node) ->
             ok | {error, any} when Table::member_table(),
                                    Node::atom()).
delete(Table, Node) ->
    delete(?table_type(), Table, Node).

-spec(delete(DBType, Table, Node) ->
             ok | {error, any} when DBType::?DB_ETS|?DB_MNESIA,
                                    Table::member_table(),
                                    Node::atom()).
delete(?DB_MNESIA, Table, Node) ->
    case lookup(?DB_MNESIA, Table, Node) of
        {ok, Member} ->
            Fun = fun() ->
                          mnesia:delete_object(Table, Member, write)
                  end,
            leo_mnesia:delete(Fun);
        Error ->
            Error
    end;
delete(?DB_ETS, Table, Node) ->
    case catch ets:delete(Table, Node) of
        true ->
            ok;
        {'EXIT', Cause} ->
            {error, Cause}
    end;
delete(_,_,_) ->
    {error, invalid_db}.


%% @doc Remove all records
-spec(delete_all(Table) ->
             ok | {error, any()} when Table::member_table()).
delete_all(Table) ->
    delete_all(?table_type(), Table).

-spec(delete_all(DBType, Table) ->
             ok | {error, any()} when DBType::?DB_ETS|?DB_MNESIA,
                                      Table::member_table()).
delete_all(?DB_MNESIA = DBType, Table) ->
    case find_all(DBType, Table) of
        {ok, L} ->
            case mnesia:transaction(
                   fun() ->
                           case delete_all_1(L, Table) of
                               ok -> ok;
                               _  -> mnesia:abort("Not removed")
                           end
                   end) of
                {atomic, ok} ->
                    ok;
                {aborted, Reason} ->
                    {error, Reason}
            end;
        not_found ->
            ok;
        Error ->
            Error
    end;
delete_all(?DB_ETS, Table) ->
    case catch ets:delete_all_objects(Table) of
        {'EXIT', Cause} ->
            {error, Cause};
        true ->
            ok
    end;
delete_all(_,_) ->
    {error, invalid_db}.


%% @private
delete_all_1([],_) ->
    ok;
delete_all_1([#member{node = Node}|Rest], Table) ->
    case mnesia:delete(Table, Node, write) of
        ok ->
            delete_all_1(Rest, Table);
        _ ->
            {error, transaction_abort}
    end.


%% @doc Replace members into the db.
%%
-spec(replace(OldMembers, NewMembers) ->
             ok | {error, any()} when OldMembers::[#member{}],
                                      NewMembers::[#member{}]).
replace(OldMembers, NewMembers) ->
    replace(?MEMBER_TBL_CUR, OldMembers, NewMembers).

-spec(replace(Table, OldMembers, NewMembers) ->
             ok | {error, any()} when Table::member_table(),
                                      OldMembers::[#member{}],
                                      NewMembers::[#member{}]).
replace(Table, OldMembers, NewMembers) ->
    replace(?table_type(), Table, OldMembers, NewMembers).

-spec(replace(DBType, Table, OldMembers, NewMembers) ->
             ok | {error, any()} when DBType::?DB_ETS | ?DB_MNESIA,
                                      Table::member_table(),
                                      OldMembers::[#member{}],
                                      NewMembers::[#member{}]).
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
    lists:foreach(fun(Item) ->
                          delete(DBType, Table, Item#member.node)
                  end, OldMembers),
    lists:foreach(fun(Item) ->
                          insert(DBType, Table, {Item#member.node, Item})
                  end, NewMembers),
    ok.


%% @doc Overwrite current records by source records
%%
-spec(overwrite(SrcTable, DestTable) ->
             ok | {error, any()} when SrcTable::member_table(),
                                      DestTable::member_table()).
overwrite(SrcTable, DestTable) ->
    case find_all(SrcTable) of
        {error, Cause} ->
            {error, Cause};
        not_found ->
            delete_all(DestTable);
        {ok, Members} ->
            overwrite_1(?table_type(), DestTable, Members)
    end.

%% @private
overwrite_1(?DB_MNESIA = DB, Table, Members) ->
    case mnesia:transaction(
           fun() ->
                   overwrite_1_1(DB, Table, Members)
           end) of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
            {error, Reason}
    end;
overwrite_1(?DB_ETS = DB, Table, Members) ->
    overwrite_1_1(DB, Table, Members).

%% @private
overwrite_1_1(_,_,[]) ->
    ok;
overwrite_1_1(?DB_MNESIA = DB, Table, [Member|Rest]) ->
    #member{node = Node} = Member,
    case mnesia:delete(Table, Node, write) of
        ok ->
            case mnesia:write(Table, Member, write) of
                ok ->
                    overwrite_1_1(DB, Table, Rest);
                _ ->
                    mnesia:abort("Not inserted")
            end;
        _ ->
            mnesia:abort("Not removed")
    end;
overwrite_1_1(?DB_ETS = DB, Table, [Member|Rest]) ->
    #member{node = Node} = Member,
    case delete(?DB_ETS, Table, Node) of
        ok ->
            case insert(?DB_ETS, Table, {Node, Member}) of
                ok ->
                    overwrite_1_1(DB, Table, Rest);
                Error ->
                    Error
            end;
        Error ->
            Error
    end.


%% @doc Retrieve total of records.
%%
-spec(table_size() ->
             integer() | {error, any()}).
table_size() ->
    table_size(?MEMBER_TBL_CUR).

-spec(table_size(Table) ->
             integer() | {error, any()} when Table::atom()).
table_size(Table) ->
    table_size(?table_type(), Table).

-spec(table_size(DBType, Table) ->
             integer() | {error, any()} when DBType::?DB_ETS|?DB_MNESIA,
                                             Table::atom()).
table_size(?DB_MNESIA, Table) ->
    case mnesia:ets(fun ets:info/2, [Table, size]) of
        undefined ->
            {error, invalid_db};
        Val ->
            Val
    end;
table_size(?DB_ETS, Table) ->
    case ets:info(Table, size) of
        undefined ->
            {error, invalid_db};
        Val ->
            Val
    end;
table_size(_,_) ->
    {error, invalid_db}.


%% @doc Retrieve list from the table.
%%
-spec(tab2list() ->
             list() | {error, any()}).
tab2list() ->
    tab2list(?MEMBER_TBL_CUR).

-spec(tab2list(Table) ->
             list() | {error, any()} when Table::member_table()).
tab2list(Table) ->
    tab2list(?table_type(), Table).

-spec(tab2list(DBType, Table) ->
             list() | {error, any()} when DBType::?DB_ETS|?DB_MNESIA,
                                          Table::member_table()).
tab2list(?DB_MNESIA, Table) ->
    case mnesia:ets(fun ets:tab2list/1, [Table]) of
        [] ->
            [];
        List when is_list(List) ->
            lists:map(fun(#member{node  = Node,
                                  state = State,
                                  num_of_vnodes = NumOfVNodes}) ->
                              {Node, State, NumOfVNodes}
                      end, List);
        Error ->
            Error
    end;
tab2list(?DB_ETS, Table) ->
    ets:tab2list(Table);
tab2list(_,_) ->
    {error, invalid_db}.


%% Go to first record
-spec(first(Table) ->
             atom() | '$end_of_table' when Table::atom()).
first(Table) ->
    first(?table_type(), Table).

%% @private
-spec(first(DBType, Table) ->
             atom() | '$end_of_table' when DBType::?DB_MNESIA|?DB_ETS,
                                           Table::atom()).
first(?DB_MNESIA, Table) ->
    mnesia:ets(fun ets:first/1, [Table]);
first(?DB_ETS, Table) ->
    ets:first(Table).


%% Go to next record
-spec(next(Table, MemberName) ->
             atom() | '$end_of_table' when Table::atom(),
                                           MemberName::atom()).
next(Table, MemberName) ->
    next(?table_type(), Table, MemberName).

%% @private
-spec(next(DBType, Table, MemberName) ->
             atom() | '$end_of_table' when DBType::?DB_MNESIA|?DB_ETS,
                                           Table::atom(),
                                           MemberName::atom()).
next(?DB_MNESIA, Table, MemberName) ->
    mnesia:ets(fun ets:next/2, [Table, MemberName]);
next(?DB_ETS, Table, MemberName) ->
    ets:next(Table, MemberName).


%% @doc Transform records
%%
-spec(transform() ->
             ok | {error, any()}).
transform() ->
    transform([]).

-spec(transform(MnesiaNodes) ->
             ok | {error, any()} when MnesiaNodes::[atom()]).
transform(MnesiaNodes) ->
    OldTbl  = 'leo_members',
    NewTbls = [?MEMBER_TBL_CUR, ?MEMBER_TBL_PREV],

    Ret = mnesia:table_info(OldTbl, size),
    transform_1(Ret, MnesiaNodes, OldTbl, NewTbls).


%% @private
-spec(transform_1(integer()|_, [atom()], atom(), [atom()]) ->
             ok | {error, any()}).
transform_1(0,_,_,_) ->
    ok;
transform_1(_, MnesiaNodes, OldTbl, NewTbls) ->
    case MnesiaNodes of
        [] -> void;
        _  ->
            leo_cluster_tbl_member:create_table(
              disc_copies, MnesiaNodes, ?MEMBER_TBL_CUR),
            leo_cluster_tbl_member:create_table(
              disc_copies, MnesiaNodes, ?MEMBER_TBL_PREV)
    end,
    case catch mnesia:table_info(?MEMBER_TBL_CUR, all) of
        {'EXIT', _Cause} ->
            ok;
        _ ->
            case catch mnesia:table_info(?MEMBER_TBL_PREV, all) of
                {'EXIT', _Cause} ->
                    ok;
                _ ->
                    transform_2(OldTbl, NewTbls)
            end
    end.

%% @private
-spec(transform_2(atom(), [atom()]) ->
             ok | {error, any()}).
transform_2(OldTbl, NewTbls) ->
    case leo_cluster_tbl_member:table_size(OldTbl) of
        {error, Cause} ->
            {error, Cause};
        Size when Size > 0 ->
            Node = leo_cluster_tbl_member:first(OldTbl),
            case transform_3(OldTbl, NewTbls, Node) of
                ok ->
                    ok;
                Error ->
                    Error
            end;
        _Size ->
            ok
    end.

%% @private
-spec(transform_3(atom(), [atom()], atom()) ->
             ok | {error, any()}).
transform_3(OldTbl, NewTbls, Node) ->
    case leo_cluster_tbl_member:lookup(OldTbl, Node) of
        {ok, Member} ->
            case transform_3_1(NewTbls, Member) of
                ok ->
                    Ret = leo_cluster_tbl_member:next(OldTbl, Node),
                    transform_4(Ret, OldTbl, NewTbls);
                Error ->
                    Error
            end;
        _ ->
            ok
    end.

%% @private
-spec(transform_3_1([atom()], #member{}) ->
             ok | {error, any()}).
transform_3_1([],_) ->
    ok;
transform_3_1([Tbl|Rest], #member{node = Node} = Member) ->
    case leo_cluster_tbl_member:insert(Tbl, {Node, Member}) of
        ok ->
            transform_3_1(Rest, Member);
        Error ->
            Error
    end.

%% @private
-spec(transform_4(atom(), atom(), [atom()]) ->
             ok | {error, any()}).
transform_4('$end_of_table',_OldTbl,_NewTbls) ->
    ok;
transform_4(Node, OldTbl, NewTbls) ->
    transform_3(OldTbl, NewTbls, Node).
