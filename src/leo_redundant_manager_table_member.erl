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

-export([create_members/0, create_members/1, create_members/2,
         lookup/1, lookup/2, find_all/0, find_all/1,
         insert/1, insert/2, delete/1, delete/2, replace/2, replace/3,
         size/0, size/1, tab2list/0, tab2list/1]).

-define(TABLE, members).
-define(table_type(), case application:get_env(?APP, ?PROP_SERVER_TYPE) of
                          {ok, Value} when Value == ?SERVER_MANAGER -> mnesia;
                          {ok, Value} when Value == ?SERVER_STORAGE -> ets;
                          {ok, Value} when Value == ?SERVER_GATEWAY -> ets;
                          _ ->
                              undefined
                      end).    

-type(mnesia_copies() :: disc_copies | ram_copies).

%% @doc create member table.
%%
-spec(create_members() -> ok).
create_members() ->
    ?TABLE = ets:new(?TABLE, [named_table, ordered_set, public, {read_concurrency, true}]),
    ok.

-spec(create_members(mnesia_copies()) -> ok).
create_members(Mode) ->
    create_members(Mode, [erlang:node()]).

-spec(create_members(mnesia_copies(), list()) -> ok).
create_members(Mode, Nodes) ->
    mnesia:create_table(
      members,
      [{Mode, Nodes},
       {type, set},
       {record_name, member},
       {attributes, record_info(fields, member)},
       {user_properties,
        [{node,          {varchar,   undefined},  false, primary,   undefined, identity,  atom   },
         {clock,         {integer,   undefined},  false, undefined, undefined, undefined, integer},
         {num_of_vnodes, {integer,   undefined},  false, undefined, undefined, undefined, integer},
         {state,         {varchar,   undefined},  false, undefined, undefined, undefined, atom   }
        ]}
      ]).


%% Retrieve a record by key from the table.
%%
lookup(Node) ->
    lookup(?table_type(), Node).

lookup(mnesia, Node) ->
    case catch mnesia:ets(fun ets:lookup/2, [?TABLE, Node]) of
        [H|_T] ->
            {ok, H};
        [] ->
            not_found;
        {'EXIT', Cause} ->
            {error, Cause}
    end;

lookup(ets, Node) ->
    case catch ets:lookup(?TABLE, Node) of
        [{_, H}|_T] ->
            {ok, H};
        [] ->
            not_found;
        {'EXIT', Cause} ->
            {error, Cause}
    end.


%% @doc Retrieve all members from the table.
%%
find_all() ->
    find_all(?table_type()).

find_all(mnesia) ->
    F = fun() ->
                Q1 = qlc:q([X || X <- mnesia:table(members)]),
                Q2 = qlc:sort(Q1, [{order, descending}]),
                qlc:e(Q2)
        end,
    leo_mnesia_utils:read(F);

find_all(ets) ->
    case catch ets:foldl(fun({_, Member}, Acc) ->
                                 ordsets:add_element(Member, Acc)
                         end, [], members) of
        {'EXIT', Cause} ->
            {error, Cause};
        [] ->
            not_found;
        Members ->
            {ok, Members}
    end.


%% @doc Insert a record into the table.
%%
insert({Node, Member}) ->
    insert(?table_type(), {Node, Member}).

insert(mnesia, {_, Member}) ->
    Fun = fun() -> mnesia:write(?TABLE, Member, write) end,
    leo_mnesia_utils:write(Fun);

insert(ets, {Node, Member}) ->
    case catch ets:insert(?TABLE, {Node, Member}) of
        true ->
            ok;
        {'EXIT', Cause} ->
            {error, Cause}
    end.


%% @doc Remove a record from the table.
%%
delete(Node) ->
    delete(?table_type(), Node).

delete(mnesia, Node) ->
    case lookup(mnesia, Node) of
        {ok, Member} ->
            Fun = fun() ->
                          mnesia:delete_object(?TABLE, Member, write)
                  end,
            leo_mnesia_utils:delete(Fun);
        Error ->
            Error
    end;

delete(ets, Node) ->
    case catch ets:delete(?TABLE, Node) of
        true ->
            ok;
        {'EXIT', Cause} ->
            {error, Cause}
    end.


%% @doc Replace members into the db.
%%
replace(OldMembers, NewMembers) ->
    replace(?table_type(), OldMembers, NewMembers).

-spec(replace(ets | mnesia, list(), list()) ->
             ok).
replace(Table, OldMembers, NewMembers) ->
    lists:foreach(fun(Item) ->
                          delete(Table, Item#member.node)
                  end, OldMembers),
    lists:foreach(
      fun(Item) ->
              insert(Table, {Item#member.node, #member{node          = Item#member.node,
                                                       clock         = Item#member.clock,
                                                       num_of_vnodes = Item#member.num_of_vnodes,
                                                       state         = Item#member.state}})
      end, NewMembers),
    ok.


%% @doc Retrieve total of records.
%%
size() ->
    ?MODULE:size(?table_type()).

size(mnesia) ->
    mnesia:ets(fun ets:info/2, [?TABLE, size]);
size(ets) ->
    ets:info(?TABLE, size).


%% @doc Retrieve list from the table.
%%
tab2list() ->
    tab2list(?table_type()).

tab2list(mnesia) ->
    case mnesia:ets(fun ets:tab2list/1, [?TABLE]) of
        [] ->
            [];
        List when is_list(List) ->
            lists:map(fun(#member{node = Node, state = State, num_of_vnodes = NumOfVNodes}) ->
                              {Node, State, NumOfVNodes}
                      end, List);
        Error ->
            Error
    end;
tab2list(ets) ->
    ets:tab2list(?TABLE).

