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
         all/0, get/1, find_by_limit/1,
         update/1, delete/1,
         checksum/0,
         synchronize/1,
         transform/0
        ]).


%% @doc Create a table of configuration of clusters
%%
create_table(Mode, Nodes) ->
    case mnesia:create_table(
           ?TBL_CLUSTER_INFO,
           [{Mode, Nodes},
            {type, set},
            {record_name, ?CLUSTER_INFO},
            {attributes, record_info(fields, ?CLUSTER_INFO)},
            {user_properties,
             [{cluster_id,           atom,        primary},
              {dc_id,                atom,        false  },
              {n,                    pos_integer, false  },
              {r,                    pos_integer, false  },
              {w,                    pos_integer, false  },
              {d,                    pos_integer, false  },
              {bit_of_ring,          pos_integer, false  },
              {num_of_dc_replicas,   pos_integer, false  },
              {num_of_rack_replicas, pos_integer, false  },
              {max_mdc_targets,      pos_integer, false  }
             ]}
           ]) of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
            {error, Reason}
    end.


%% @doc Retrieve all configuration of remote-clusters
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


%% @doc Retrieve configuration of remote-clusters by cluster-id
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


%% @doc Retrieve records by limit
%%
-spec(find_by_limit(pos_integer()) ->
             {ok, #?CLUSTER_INFO{}} | not_found | {error, any()}).
find_by_limit(Rows) ->
    find_by_limit(Rows, []).

%% @private
find_by_limit(Rows, Acc) when Rows == length(Acc) ->
    {ok, Acc};
find_by_limit(Rows, []) ->
    Table =  ?TBL_CLUSTER_INFO,
    case catch mnesia:ets(fun ets:first/1, [Table]) of
        {'EXIT', Cause} ->
            {error, Cause};
        '$end_of_table' ->
            not_found;
        Key ->
            case leo_mdcr_tbl_cluster_info:get(Key) of
                {ok, #?CLUSTER_INFO{} = Value} ->
                    find_by_limit(Rows, Key, [Value]);
                Error ->
                    Error
            end
    end.

%% @private
find_by_limit(Rows,_ClusterId, Acc) when Rows == length(Acc) ->
    {ok, Acc};
find_by_limit(Rows, ClusterId, Acc) ->
    Table =  ?TBL_CLUSTER_INFO,
    case catch mnesia:ets(fun ets:next/2, [Table, ClusterId]) of
        {'EXIT', Cause} ->
            {error, Cause};
        '$end_of_table' ->
            {ok, Acc};
        Key ->
            case leo_mdcr_tbl_cluster_info:get(Key) of
                {ok, #?CLUSTER_INFO{} = Value} ->
                    find_by_limit(Rows, Key, [Value|Acc]);
                Error ->
                    Error
            end
    end.


%% @doc Modify configuration of a cluster
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


%% @doc Remove configuration of a cluster
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


%% @doc Retrieve a checksum
%%
-spec(checksum() ->
             {ok, pos_integer()} | {error, any()}).
checksum() ->
    case all() of
        {ok, Vals} ->
            {ok, erlang:crc32(term_to_binary(Vals))};
        not_found ->
            {ok, -1};
        Error ->
            Error
    end.


%% @doc Synchronize records
%%
-spec(synchronize(list()) ->
             ok | {error, any()}).
synchronize([]) ->
    ok;
synchronize([V|Rest]) ->
    case update(V) of
        ok ->
            synchronize(Rest);
        Error ->
            Error
    end.


%% @doc Transform records
%%
-spec(transform() ->
             ok | {error, any()}).
transform() ->
    {atomic, ok} = mnesia:transform_table(
                     ?TBL_CLUSTER_INFO,  fun transform/1,
                     record_info(fields, ?CLUSTER_INFO),
                     ?CLUSTER_INFO),
    ok.


%% @doc the record is the current verion
%% @private
transform(#?CLUSTER_INFO{} = ClusterInfo) ->
    ClusterInfo;
transform(#cluster_info{cluster_id = ClusterId,
                        dc_id   = DCId,
                        n       = N,
                        r       = R,
                        w       = W,
                        d       = D,
                        bit_of_ring = BitOfRing,
                        num_of_dc_replicas   = Level1,
                        num_of_rack_replicas = Level2}) ->
    ClusterId_1 = case is_atom(ClusterId) of
                      true  -> ClusterId;
                      false -> list_to_atom(ClusterId)
                  end,
    DCId_1 = case is_atom(DCId) of
                 true  -> DCId;
                 false -> list_to_atom(DCId)
             end,
    #?CLUSTER_INFO{cluster_id = ClusterId_1,
                   dc_id = DCId_1,
                   n = N,
                   r = R,
                   w = W,
                   d = D,
                   bit_of_ring = BitOfRing,
                   num_of_dc_replicas = Level1,
                   num_of_rack_replicas = Level2,
                   max_mdc_targets = ?DEF_MAX_MDC_TARGETS
                  }.
