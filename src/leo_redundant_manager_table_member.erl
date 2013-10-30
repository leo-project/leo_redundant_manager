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
%% Leo Redundant Manager - ETS/Mnesia Handler for Member
%% @doc
%% @end
%%======================================================================
-module(leo_redundant_manager_table_member).

-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").
-include_lib("stdlib/include/qlc.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([create_members/1, create_members/3,
         lookup/1, lookup/2, lookup/3,
         find_all/0, find_all/1, find_all/2,
         find_by_status/1, find_by_status/2, find_by_status/3,
         find_by_level1/2, find_by_level1/3, find_by_level1/4,
         find_by_level2/1, find_by_level2/2, find_by_level2/3,
         insert/1, insert/2, insert/3,
         delete/1, delete/2, delete/3,
         replace/2, replace/3, replace/4,
         table_size/0, table_size/1, table_size/2,
         tab2list/0, tab2list/1, tab2list/2]).

%% -define(TABLE, 'leo_members').

-ifdef(TEST).
-define(table_type(), ets).
-else.
-define(table_type(), case leo_misc:get_env(?APP, ?PROP_SERVER_TYPE) of
                          {ok, Value} when Value == ?SERVER_MANAGER -> mnesia;
                          {ok, Value} when Value == ?SERVER_STORAGE -> ets;
                          {ok, Value} when Value == ?SERVER_GATEWAY -> ets;
                          _Error ->
                              undefined
                      end).
-endif.

-type(mnesia_copies() :: disc_copies | ram_copies).

%% @doc create member table.
%%
-spec(create_members(member_table()) -> ok).
create_members(Table) ->
    catch ets:new(Table, [named_table, set, public, {read_concurrency, true}]),
    ok.

%% -spec(create_members(mnesia_copies()) -> ok).
%% create_members(Mode) ->
%%     create_members(Mode, [erlang:node()]).

-spec(create_members(mnesia_copies(), list(), member_table()) -> ok).
create_members(Mode, Nodes, Table) ->
    mnesia:create_table(
      Table,
      [{Mode, Nodes},
       {type, set},
       {record_name, member},
       {attributes, record_info(fields, member)},
       {user_properties,
        [{node,          {varchar,   undefined},  false, primary,   undefined, identity,  atom   },
         {alias,         {varchar,   undefined},  false, primary,   undefined, identity,  atom   },
         {ip,            {varchar,   undefined},  false, primary,   undefined, identity,  varchar},
         {port,          {integer,   undefined},  false, undefined, undefined, undefined, integer},
         {inet,          {varchar,   undefined},  false, undefined, undefined, undefined, atom   },
         {clock,         {integer,   undefined},  false, undefined, undefined, undefined, integer},
         {num_of_vnodes, {integer,   undefined},  false, undefined, undefined, undefined, integer},
         {state,         {varchar,   undefined},  false, undefined, undefined, undefined, atom   },
         {grp_level_1,   {varchar,   undefined},  false, undefined, undefined, undefined, varchar},
         {grp_level_2,   {varchar,   undefined},  false, undefined, undefined, undefined, varchar}
        ]}
      ]),
    ok.


%% @doc Retrieve a record by key from the table.
%%
-spec(lookup(atom()) ->
             {ok, #member{}} | not_found | {error, any()}).
lookup(Node) ->
    lookup(?MEMBER_TBL_CUR, Node).

-spec(lookup(atom(), member_table()) ->
             {ok, #member{}} | not_found | {error, any()}).
lookup(Table, Node) ->
    lookup(?table_type(), Table, Node).

-spec(lookup(ets|mnesia, atom(), member_table()) ->
             {ok, #member{}} | not_found | {error, any()}).
lookup(mnesia, Table, Node) ->
    case catch mnesia:ets(fun ets:lookup/2, [Table, Node]) of
        [H|_T] ->
            {ok, H};
        [] ->
            not_found;
        {'EXIT', Cause} ->
            {error, Cause}
    end;

lookup(ets, Table, Node) ->
    case catch ets:lookup(Table, Node) of
        [{_, H}|_T] ->
            {ok, H};
        [] ->
            not_found;
        {'EXIT', Cause} ->
            {error, Cause}
    end.


%% @doc Retrieve all members from the table.
%%
-spec(find_all() ->
             {ok, list(#member{})} | not_found | {error, any()}).
find_all() ->
    find_all(?MEMBER_TBL_CUR).

-spec(find_all(member_table()) ->
             {ok, list(#member{})} | not_found | {error, any()}).
find_all(Table) ->
    find_all(?table_type(), Table).

-spec(find_all(ets|mnesia, member_table()) ->
             {ok, list(#member{})} | not_found | {error, any()}).
find_all(mnesia, Table) ->
    F = fun() ->
                Q1 = qlc:q([X || X <- mnesia:table(Table)]),
                Q2 = qlc:sort(Q1, [{order, descending}]),
                qlc:e(Q2)
        end,
    leo_mnesia:read(F);

find_all(ets, Table) ->
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
    end.


%% @doc Retrieve members by status
%%
-spec(find_by_status(atom()) ->
             {ok, list(#member{})} | not_found | {error, any()}).
find_by_status(Status) ->
    find_by_status(?MEMBER_TBL_CUR, Status).

-spec(find_by_status(member_table(), atom()) ->
             {ok, list(#member{})} | not_found | {error, any()}).
find_by_status(Table, Status) ->
    find_by_status(?table_type(), Table, Status).

-spec(find_by_status(ets|mnesia, member_table(), atom()) ->
             {ok, list(#member{})} | not_found | {error, any()}).
find_by_status(mnesia, Table, St0) ->
    F = fun() ->
                Q = qlc:q([X || X <- mnesia:table(Table),
                                X#member.state == St0]),
                qlc:e(Q)
        end,
    leo_mnesia:read(F);

find_by_status(ets, Table, St0) ->
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
    end.


%% @doc Retrieve records by L1 and L2
%%
-spec(find_by_level1(atom(), atom()) ->
             {ok, list()} | not_found | {error, any()}).
find_by_level1(L1, L2) ->
    find_by_level1(?MEMBER_TBL_CUR, L1, L2).

-spec(find_by_level1(member_table(), atom(), atom()) ->
             {ok, list()} | not_found | {error, any()}).
find_by_level1(Table, L1, L2) ->
    find_by_level1(?table_type(), Table, L1, L2).

-spec(find_by_level1(ets|mnesia, member_table(), atom(), atom()) ->
             {ok, list()} | not_found | {error, any()}).
find_by_level1(mnesia, Table, L1, L2) ->
    F = fun() ->
                Q = qlc:q([X || X <- mnesia:table(Table),
                                X#member.grp_level_1 == L1,
                                X#member.grp_level_2 == L2]),
                qlc:e(Q)
        end,
    leo_mnesia:read(F);

find_by_level1(ets, Table, L1, L2) ->
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
    end.


%% @doc Retrieve records by L2
%%
-spec(find_by_level2(atom()) ->
             {ok, list()} | not_found | {error, any()}).
find_by_level2(L2) ->
    find_by_level2(?MEMBER_TBL_CUR, L2).

-spec(find_by_level2(member_table(), atom()) ->
             {ok, list()} | not_found | {error, any()}).
find_by_level2(Table, L2) ->
    find_by_level2(?table_type(), Table, L2).

-spec(find_by_level2(ets|mnesia, member_table(), atom()) ->
             {ok, list()} | not_found | {error, any()}).
find_by_level2(mnesia, Table, L2) ->
    F = fun() ->
                Q = qlc:q([X || X <- mnesia:table(Table),
                                X#member.grp_level_2 == L2]),
                qlc:e(Q)
        end,
    leo_mnesia:read(F);

find_by_level2(ets, Table, L2) ->
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
    end.


%% @doc Insert a record into the table.
%%
-spec(insert({atom(), #member{}}) ->
             ok | {error, any}).
insert({Node, Member}) ->
    insert(?MEMBER_TBL_CUR, {Node, Member}).

-spec(insert(member_table(), {atom(), #member{}}) ->
             ok | {error, any}).
insert(Table, {Node, Member}) ->
    insert(?table_type(), Table, {Node, Member}).

-spec(insert(ets|mnesia, member_table(), {atom(), #member{}}) ->
             ok | {error, any}).
insert(mnesia, Table, {_, Member}) ->
    Fun = fun() -> mnesia:write(Table, Member, write) end,
    leo_mnesia:write(Fun);

insert(ets, Table, {Node, Member}) ->
    case catch ets:insert(Table, {Node, Member}) of
        true ->
            ok;
        {'EXIT', Cause} ->
            {error, Cause}
    end.


%% @doc Remove a record from the table.
%%
-spec(delete(atom()) ->
             ok | {error, any}).
delete(Node) ->
    delete(?MEMBER_TBL_CUR, Node).

-spec(delete(member_table(), atom()) ->
             ok | {error, any}).
delete(Table, Node) ->
    delete(?table_type(), Table, Node).

-spec(delete(ets|mnesia, member_table(), atom()) ->
             ok | {error, any}).
delete(mnesia, Table, Node) ->
    case lookup(mnesia, Node) of
        {ok, Member} ->
            Fun = fun() ->
                          mnesia:delete_object(Table, Member, write)
                  end,
            leo_mnesia:delete(Fun);
        Error ->
            Error
    end;

delete(ets, Table, Node) ->
    case catch ets:delete(Table, Node) of
        true ->
            ok;
        {'EXIT', Cause} ->
            {error, Cause}
    end.


%% @doc Replace members into the db.
%%
-spec(replace(list(), list()) ->
             ok).
replace(OldMembers, NewMembers) ->
    replace(?MEMBER_TBL_CUR, OldMembers, NewMembers).

-spec(replace(member_table(), list(), list()) ->
             ok).
replace(Table, OldMembers, NewMembers) ->
    replace(?table_type(), Table, OldMembers, NewMembers).

-spec(replace(ets | mnesia, member_table(), list(), list()) ->
             ok).
replace(DBType, Table, OldMembers, NewMembers) ->
    lists:foreach(fun(Item) ->
                          delete(DBType, Table, Item#member.node)
                  end, OldMembers),
    lists:foreach(
      fun(Item) ->
              insert(DBType, Table, {Item#member.node, Item})
      end, NewMembers),
    ok.


%% @doc Retrieve total of records.
%%
-spec(table_size() ->
             pos_integer()).
table_size() ->
    table_size(?MEMBER_TBL_CUR).

-spec(table_size(member_table()) ->
             pos_integer()).
table_size(Table) ->
    table_size(?table_type(), Table).

-spec(table_size(ets|mnesia, member_table()) ->
             pos_integer()).
table_size(mnesia, Table) ->
    mnesia:ets(fun ets:info/2, [Table, size]);
table_size(ets, Table) ->
    ets:info(Table, size).


%% @doc Retrieve list from the table.
%%
-spec(tab2list() ->
             list() | {error, any()}).
tab2list() ->
    tab2list(?MEMBER_TBL_CUR).

-spec(tab2list(member_table()) ->
             list() | {error, any()}).
tab2list(Table) ->
    tab2list(?table_type(), Table).

-spec(tab2list(ets|mnesia, member_table()) ->
             list() | {error, any()}).
tab2list(mnesia, Table) ->
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
tab2list(ets, Table) ->
    ets:tab2list(Table).

