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
%%======================================================================
-module(leo_mdcr_tbl_cluster_info).
-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/qlc.hrl").

%% API
-export([create_table/2,
         all/0, get/1, update/1, delete/1]).


%% @doc Create a table of system-configutation
%%
create_table(Mode, Nodes) ->
    mnesia:create_table(
      ?TBL_CLUSTER_INFO,
      [{Mode, Nodes},
       {type, set},
       {record_name, ?CLUSTER_INFO},
       {attributes, record_info(fields, ?CLUSTER_INFO)},
       {user_properties,
        [{cluster_id,           string,      primary},
         {dc_id,                string,      false  },
         {n,                    pos_integer, false  },
         {r,                    pos_integer, false  },
         {w,                    pos_integer, false  },
         {d,                    pos_integer, false  },
         {bit_of_ring,          pos_integer, false  },
         {num_of_mdcr_targets,  pos_integer, false  },
         {num_of_dc_replicas,   pos_integer, false  },
         {num_of_rack_replicas, pos_integer, false  }
        ]}
      ]).


%% @doc Retrieve system configuration by cluster-id
%%
-spec(all() ->
             {ok, [#?CLUSTER_INFO{}]} | not_found | {error, any()}).
all() ->
    Tbl = ?TBL_CLUSTER_INFO,

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
             {ok, #?CLUSTER_INFO{}} | not_found | {error, any()}).
get(ClusterId) ->
    Tbl = ?TBL_CLUSTER_INFO,

    case catch mnesia:table_info(Tbl, all) of
        {'EXIT', _Cause} ->
            {error, ?ERROR_MNESIA_NOT_START};
        _ ->
            F = fun() ->
                        Q = qlc:q([X || X <- mnesia:table(Tbl),
                                        X#?CLUSTER_INFO.cluster_id == ClusterId]),
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
-spec(update(#?CLUSTER_INFO{}) ->
             ok | {error, any()}).
update(ClusterInfo) ->
    Tbl = ?TBL_CLUSTER_INFO,

    case catch mnesia:table_info(Tbl, all) of
        {'EXIT', _Cause} ->
            {error, ?ERROR_MNESIA_NOT_START};
        _ ->
            F = fun()-> mnesia:write(Tbl, ClusterInfo, write) end,
            leo_mnesia:write(F)
    end.


%% @doc Remove system-configuration
%%
-spec(delete(string()) ->
             ok | {error, any()}).
delete(ClusterId) ->
    Tbl = ?TBL_CLUSTER_INFO,

    case ?MODULE:get(ClusterId) of
        {ok, ClusterInfo} ->
            Fun = fun() ->
                          mnesia:delete_object(Tbl, ClusterInfo, write)
                  end,
            leo_mnesia:delete(Fun);
        Error ->
            Error
    end.
