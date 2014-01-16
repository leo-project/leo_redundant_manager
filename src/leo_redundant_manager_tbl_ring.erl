%%======================================================================
%%
%% Leo Redundant Manager
%%
%% Copyright (c) 2012-2014 Rakuten, Inc.
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
-module(leo_redundant_manager_tbl_ring).

-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([create_ring_current/1, create_ring_current/2,
         create_ring_old_for_test/3,
         create_ring_prev/1, create_ring_prev/2,
         lookup/2, insert/2, delete/2, first/1, last/1, prev/2, next/2,
         delete_all/1, size/1, tab2list/1]).

-type(mnesia_copies() :: disc_copies | ram_copies).

%% @doc create ring-current table.
%%
-spec(create_ring_current(mnesia_copies()) -> ok).
create_ring_current(Mode) ->
    create_ring_current(Mode, [erlang:node()]).

create_ring_current(Mode, Nodes) ->
    mnesia:create_table(
      ?RING_TBL_CUR,
      [{Mode, Nodes},
       {type, ordered_set},
       {record_name, ?RING},
       {attributes, record_info(fields, ?RING)},
       {user_properties,
        [{vnode_id, {integer,   undefined},  false, primary,   undefined, identity,  integer},
         {atom,     {varchar,   undefined},  false, undefined, undefined, undefined, atom   },
         {clock,    {integer,   undefined},  false, undefined, undefined, undefined, integer}
        ]}
      ]).

%% @doc create ring-prev table.
%%
-spec(create_ring_prev(mnesia_copies()) -> ok).
create_ring_prev(Mode) ->
    create_ring_prev(Mode, [erlang:node()]).

create_ring_prev(Mode, Nodes) ->
    mnesia:create_table(
      ?RING_TBL_PREV,
      [{Mode, Nodes},
       {type, ordered_set},
       {record_name, ?RING},
       {attributes, record_info(fields, ?RING)},
       {user_properties,
        [{vnode_id, {integer,   undefined},  false, primary,   undefined, identity,  integer},
         {atom,     {varchar,   undefined},  false, undefined, undefined, undefined, atom   },
         {clock,    {integer,   undefined},  false, undefined, undefined, undefined, integer}
        ]}
      ]).


%% @doc create table for the test
%%
create_ring_old_for_test(Mode, Nodes, Table) ->
    mnesia:create_table(
      Table,
      [{Mode, Nodes},
       {type, ordered_set},
       {record_name, ring},
       {attributes, record_info(fields, ring)},
       {user_properties,
        [{vnode_id, {integer,   undefined},  false, primary,   undefined, identity,  integer},
         {atom,     {varchar,   undefined},  false, undefined, undefined, undefined, atom   }
        ]}
      ]).


%% Retrieve a record by key from the table.
%%
lookup({mnesia, Table}, VNodeId) ->
    case catch mnesia:ets(fun ets:lookup/2, [Table, VNodeId]) of
        [Ring|_] ->
            Ring;
        [] = Reply ->
            Reply;
        {'EXIT', Cause} ->
            {error, Cause}
    end;
lookup({ets, Table}, VNodeId) ->
    case catch ets:lookup(Table, VNodeId) of
        [{VNodeId, Node, Clock}|_] ->
            #?RING{vnode_id = VNodeId,
                   node     = Node,
                   clock    = Clock};
        [] = Reply ->
            Reply;
        {'EXIT', Cause} ->
            {error, Cause}
    end.

%% @doc Insert a record into the table.
%%
insert({mnesia, Table}, Ring) when is_record(Ring, ring);
                                   is_record(Ring, ring_0_16_8) ->
    Fun = fun() -> mnesia:write(Table, Ring, write) end,
    leo_mnesia:write(Fun),
    true;
insert({mnesia, Table}, {VNodeId, Node, Clock}) ->
    Fun = fun() -> mnesia:write(Table, #?RING{vnode_id = VNodeId,
                                              node     = Node,
                                              clock    = Clock}, write) end,
    leo_mnesia:write(Fun),
    true;
insert({ets, Table}, {VNodeId, Node, Clock}) ->
    ets:insert(Table, {VNodeId, Node, Clock}).

%% @doc Remove a record from the table.
%%
delete({mnesia, Table}, VNodeId) ->
    Ring = lookup({mnesia, Table}, VNodeId),
    Fun = fun() ->
                  mnesia:delete_object(Table, Ring, write)
          end,
    leo_mnesia:delete(Fun),
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
    mnesia:ets(fun ets:last/1, [Table]);
last({ets, Table}) ->
    ets:last(Table).

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
delete_all({mnesia, Table}) ->
    case mnesia:ets(fun ets:delete_all_objects/1, [Table]) of
        true ->
            ok;
        Error ->
            Error
    end;
delete_all({ets, Table}) ->
    case ets:delete_all_objects(Table) of
        true ->
            ok;
        Error ->
            Error
    end.


%% @doc Retrieve total of records.
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
            lists:map(fun(#?RING{vnode_id = VNodeId,
                                 node = Node,
                                 clock = Clock}) ->
                              {VNodeId, Node, Clock}
                      end, List);
        Error ->
            Error
    end;
tab2list({ets, Table}) ->
    ets:tab2list(Table).

