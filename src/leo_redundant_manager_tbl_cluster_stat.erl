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
-module(leo_redundant_manager_tbl_cluster_stat).

-author('Yosuke Hara').


-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/qlc.hrl").

%% API
-export([create_table/2,
         all/0, get/1, update/1, delete/1]).

-define(TBL_CLUSTER_STAT, 'leo_cluster_stat').
-define(ERROR_MNESIA_NOT_START, "Mnesia is not available").


%% @doc Create a table of system-configutation
%%
create_table(Mode, Nodes) ->
    mnesia:create_table(
      ?TBL_CLUSTER_STAT,
      [{Mode, Nodes},
       {type, set},
       {record_name, cluster_stat},
       {attributes, record_info(fields, cluster_stat)},
       {user_properties,
        [{cluster_id, string,      primary},
         {status,     atom,        false  },
         {checksum,   pos_integer, false  },
         {updated_at, pos_integer, false  }
        ]}
      ]).


%% @doc Retrieve system configuration by cluster-id
%%
-spec(all() ->
             {ok, [#system_conf{}]} | not_found | {error, any()}).
all() ->
    Tbl = ?TBL_CLUSTER_STAT,

    case catch mnesia:table_info(Tbl, all) of
        {'EXIT', _Cause} ->
            {error, ?ERROR_MNESIA_NOT_START};
        _ ->
            F = fun() ->
                        Q1 = qlc:q([X || X <- mnesia:table(Tbl)]),
                        Q2 = qlc:sort(Q1, [{order, descending}]),
                        qlc:e(Q2)
                end,
            leo_mnesia:read(F)
    end.


%% @doc Retrieve system configuration by cluster-id
%%
-spec(get(string()) ->
             {ok, #system_conf{}} | not_found | {error, any()}).
get(ClusterId) ->
    Tbl = ?TBL_CLUSTER_STAT,

    case catch mnesia:table_info(Tbl, all) of
        {'EXIT', _Cause} ->
            {error, ?ERROR_MNESIA_NOT_START};
        _ ->
            F = fun() ->
                        Q = qlc:q([X || X <- mnesia:table(Tbl),
                                        X#?SYSTEM_CONF.cluster_id == ClusterId]),
                        qlc:e(Q)
                end,
            case leo_mnesia:read(F) of
                {ok, [H|_]} ->
                    {ok, H};
                Other ->
                    Other
            end
    end.


%% @doc Modify system-configuration
%%
-spec(update(#system_conf{}) ->
             ok | {error, any()}).
update(ClusterStat) ->
    Tbl = ?TBL_CLUSTER_STAT,

    case catch mnesia:table_info(Tbl, all) of
        {'EXIT', _Cause} ->
            {error, ?ERROR_MNESIA_NOT_START};
        _ ->
            F = fun()-> mnesia:write(Tbl, ClusterStat, write) end,
            leo_mnesia:write(F)
    end.


%% @doc Remove system-configuration
%%
-spec(delete(string()) ->
             ok | {error, any()}).
delete(ClusterId) ->
    Tbl = ?TBL_CLUSTER_STAT,

    case ?MODULE:get(ClusterId) of
        {ok, ClusterStat} ->
            Fun = fun() ->
                          mnesia:delete_object(Tbl, ClusterStat, write)
                  end,
            leo_mnesia:delete(Fun);
        Error ->
            Error
    end.
