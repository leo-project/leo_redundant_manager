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
-module(leo_redundant_manager_table_conf).

-author('Yosuke Hara').


-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/qlc.hrl").

%% API
-export([create_system_config/2,
         get_system_config/0,
         update_system_config/1]).

-define(TBL_SYSTEM_CONF, 'leo_system_conf').
-define(ERROR_MNESIA_NOT_START, '').


%% @doc Create a table of system-configutation
%%
create_system_config(Mode, Nodes) ->
    mnesia:create_table(
      ?TBL_SYSTEM_CONF,
      [{Mode, Nodes},
       {type, set},
       {record_name, ?SYSTEM_CONF},
       {attributes, record_info(fields, ?SYSTEM_CONF)},
       {user_properties,
        [{cluster_id,           {string,    undefined},  false, primary,   undefined, identity,  string },
         {dc_id,                {string,    undefined},  false, primary,   undefined, identity,  string },
         {n,                    {integer,   undefined},  false, undefined, undefined, undefined, integer},
         {r,                    {integer,   undefined},  false, undefined, undefined, undefined, integer},
         {w,                    {integer,   undefined},  false, undefined, undefined, undefined, integer},
         {d,                    {integer,   undefined},  false, undefined, undefined, undefined, integer},
         {bit_of_ring,          {integer,   undefined},  false, undefined, undefined, undefined, integer},
         {num_of_dc_replicas,   {integer,   undefined},  false, undefined, undefined, undefined, integer},
         {num_of_rack_replicas, {integer,   undefined},  false, undefined, undefined, undefined, integer}
        ]}
      ]).


%% @doc Retrieve system configuration
%%
-spec(get_system_config() ->
             {ok, #system_conf{}} | not_found | {error, any()}).
get_system_config() ->
    Tbl = ?TBL_SYSTEM_CONF,

    case catch mnesia:table_info(Tbl, all) of
        {'EXIT', _Cause} ->
            {error, ?ERROR_MNESIA_NOT_START};
        _ ->
            F = fun() ->
                        Q1 = qlc:q([X || X <- mnesia:table(Tbl)]),
                        Q2 = qlc:sort(Q1, [{order, descending}]),
                        qlc:e(Q2)
                end,
            get_system_config(leo_mnesia:read(F))
    end.
get_system_config({ok, [H|_]}) ->
    {ok, H};
get_system_config(Other) ->
    Other.


%% @doc Modify system-configuration
%%
-spec(update_system_config(#system_conf{}) ->
             ok | {error, any()}).
update_system_config(SystemConfig) ->
    Tbl = ?TBL_SYSTEM_CONF,

    case catch mnesia:table_info(Tbl, all) of
        {'EXIT', _Cause} ->
            {error, ?ERROR_MNESIA_NOT_START};
        _ ->
            F = fun()-> mnesia:write(Tbl, SystemConfig, write) end,
            leo_mnesia:write(F)
    end.
