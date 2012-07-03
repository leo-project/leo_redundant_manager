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
%% Leo Redundant Manager - ETS/Mnesia Handler
%% @doc
%% @end
%%======================================================================
-module(leo_redundant_manager_table).

-author('Yosuke Hara').
-vsn('0.9.0').

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([lookup/2, insert/2, delete/2, first/1, last/1, prev/2, next/2,
         delete_all_objects/1, size/1, tab2list/1]).


%% Retrieve a record by key from the table.
%%
lookup({mnesia, Table}, VNodeId) ->
    case catch mnesia:ets(fun ets:lookup/2, [Table, VNodeId]) of
        [#ring{node = Node}] ->
            Node;
        [] = Reply ->
            Reply;
        {'EXIT', Cause} ->
            {error, Cause}
    end;
lookup({ets, Table}, VNodeId) ->
    case catch ets:lookup(Table, VNodeId) of
        [{_VNodeId, Node}] ->
            Node;
        [] = Reply ->
            Reply;
        {'EXIT', Cause} ->
            {error, Cause}
    end.

%% @doc Insert a record into the table.
%%
insert({mnesia, Table}, {VNodeId, Node}) ->
    Fun = fun() -> mnesia:write(Table, #ring{vnode_id = VNodeId,
                                             node     = Node}, write) end,
    leo_mnesia_utils:write(Fun),
    true;
insert({ets, Table}, {VNodeId, Node}) ->
    ets:insert(Table, {VNodeId, Node}).

%% @doc Remove a record from the table.
%%
delete({mnesia, Table}, VNodeId) ->
    Node = lookup({mnesia, Table}, VNodeId),
    Fun = fun() ->
                  mnesia:delete_object(Table, #ring{vnode_id = VNodeId,
                                                    node     = Node}, write)
          end,
    leo_mnesia_utils:delete(Fun),
    true;
delete({ets, Table}, VNodeId) ->
    ets:delete(Table, VNodeId).

%% @doc Retrieve a first record from the table.
%%
first({mnesia, Table}) ->
    mnesia:ets(fun ets:first/1, [Table]);
first({ets, Table}) ->
    ets:first(Table).

%% @doc Retrieve a last record from the table.
%%
last({mnesia, Table}) ->
    mnesia:ets(fun ets:first/1, [Table]);
last({ets, Table}) ->
    ets:first(Table).

%% @doc Retrieve a previous record from the table.
%%
prev({mnesia, Table}, VNodeId) ->
    mnesia:ets(fun ets:prev/2, [Table, VNodeId]);
prev({ets, Table}, VNodeId) ->
    ets:prev(Table, VNodeId).

%% @doc Retrieve a next record from the table.
%%
next({mnesia, Table}, VNodeId) ->
    mnesia:ets(fun ets:next/2, [Table, VNodeId]);
next({ets, Table}, VNodeId) ->
    ets:next(Table, VNodeId).


%% @doc Remove all objects from the table.
%%
delete_all_objects({mnesia, Table}) ->
    mnesia:ets(fun ets:delete_all_objects/1, [Table]);
delete_all_objects({ets, Table}) ->
    ets:delete_all_objects(Table).


%% @doc Retrieve a next record from the table.
%%
size({mnesia, Table}) ->
    mnesia:ets(fun ets:info/2, [Table, size]);
size({ets, Table}) ->
    ets:info(Table, size).


%% @doc Retrieve list from the table.
%%
tab2list({mnesia, Table}) ->
    case mnesia:ets(fun ets:tab2list/1, [Table]) of
        [] ->
            [];
        List when is_list(List) ->
            lists:map(fun(#ring{vnode_id = VNodeId, node = Node}) ->
                              {VNodeId, Node}
                      end, List);
        Error ->
            Error
    end;
tab2list({ets, Table}) ->
    ets:tab2list(Table).

