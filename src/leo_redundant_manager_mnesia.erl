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
%% Leo Redundant Management - Mnesia.
%% @doc
%% @end
%%======================================================================
-module(leo_redundant_manager_mnesia).

-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").
-include_lib("stdlib/include/qlc.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([get_members/0,
         get_members_count/0,
         get_member_by_node/1,
         update_member_by_node/3,
         refresh_members/2,
         insert_member/1,
         delete_member/1
        ]).


%% @doc get member list.
%%
-spec(get_members() ->
             {ok, list()} | {error, any()}).
get_members() ->
    F = fun() ->
                Q1 = qlc:q([X || X <- mnesia:table(members)]),
                Q2 = qlc:sort(Q1, [{order, descending}]),
                qlc:e(Q2)
        end,

    Ret = mnesia:transaction(F),
    case Ret of
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "get_members/0"},
                                    {line, ?LINE}, {body,Cause}]),
            {error, Cause};
        {atomic, []} ->
            not_found;
        {atomic, Members} ->
            {ok, Members}
    end.


%% @doc get the number of records memebers table..
%%
-spec(get_members_count() ->
             integer()).
get_members_count() ->
    case mnesia:table_info(members, size) of
        {aborted, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "get_members_count/0"},
                                    {line, ?LINE}, {body,Cause}]),
            {error, Cause};
        Info ->
            Info
    end.


%% @doc get a member by node-name.
%%
-spec(get_member_by_node(atom()) ->
             {ok, list()} | not_found | {error, any()}).
get_member_by_node(Node) ->
    F = fun() ->
                Q1 = qlc:q([X || X <- mnesia:table(members),
                                 X#member.node =:= Node]),
                Q2 = qlc:sort(Q1, [{order, ascending}]),
                qlc:e(Q2)
        end,
    leo_mnesia_utils:read(F).


%% @doc
%%
-spec(update_member_by_node(atom(), integer(), atom()) ->
             ok | {error, any()}).
update_member_by_node(Node, Clock, State) ->
    case get_member_by_node(Node) of
        {ok, [Member|_]} ->
            F = fun() -> mnesia:write(members, Member#member{clock = Clock,
                                                             state = State} , write) end,
            case leo_mnesia_utils:write(F) of
                ok ->
                    case get_members() of
                        {ok, List} ->
                            application:set_env(?APP, ?PROP_MEMBERS, List, 3000);
                        _ ->
                            void
                    end;
                Error ->
                    Error
            end;
        not_found = Cause ->
            {error, Cause};
        Error ->
            Error
    end.


%% @doc
%%
-spec(refresh_members(list(), list()) ->
             ok).
refresh_members(OldMembers, NewMembers) ->
    lists:foreach(fun(Item) ->
                          leo_redundant_manager_mnesia:delete_member(Item)
                  end, OldMembers),
    lists:foreach(fun(Item) ->
                          leo_redundant_manager_mnesia:insert_member(
                            #member{node          = Item#member.node,
                                    clock         = Item#member.clock,
                                    num_of_vnodes = Item#member.num_of_vnodes,
                                    state         = Item#member.state})
                  end, NewMembers),
    ok.


%% @doc
%%
-spec(insert_member(list()) ->
             ok | {error, any()}).
insert_member(Member) ->
    F = fun()-> mnesia:write(members, Member, write) end,
    leo_mnesia_utils:write(F).

%% @doc
%%
-spec(delete_member(list()) ->
             ok | {error, any()}).
delete_member(Member) ->
    F = fun() ->
                mnesia:delete_object(members, Member, write)
        end,
    leo_mnesia_utils:delete(F).

