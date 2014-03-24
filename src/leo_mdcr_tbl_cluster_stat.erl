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
%%======================================================================
-module(leo_mdcr_tbl_cluster_stat).
-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/qlc.hrl").

%% API
-export([create_table/2,
         all/0, get/1,
         find_by_state/1, find_by_cluster_id/1,
         update/1, delete/1,
         checksum/0, checksum/1,
         synchronize/1,
         transform/0
        ]).

%% @doc Create a table of system-configutation
%%
create_table(Mode, Nodes) ->
    case mnesia:create_table(
           ?TBL_CLUSTER_STAT,
           [{Mode, Nodes},
            {type, set},
            {record_name, ?CLUSTER_STAT},
            {attributes, record_info(fields, cluster_stat)},
            {user_properties,
             [{cluster_id, atom,        primary},
              {state,      atom,        false  },
              {checksum,   pos_integer, false  },
              {updated_at, pos_integer, false  }
             ]}
           ]) of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
            {error, Reason}
    end.


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
                        Q2 = qlc:sort(Q1, [{order, ascending}]),
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
                                        X#?CLUSTER_STAT.cluster_id == ClusterId]),
                        qlc:e(Q)
                end,
            case leo_mnesia:read(F) of
                {ok, [H|_]} ->
                    {ok, H};
                Other ->
                    Other
            end
    end.


%% @doc Retrieve system configuration by State
%%
-spec(find_by_state(atom()) ->
             {ok, #system_conf{}} | not_found | {error, any()}).
find_by_state(State) ->
    Tbl = ?TBL_CLUSTER_STAT,

    case catch mnesia:table_info(Tbl, all) of
        {'EXIT', _Cause} ->
            {error, ?ERROR_MNESIA_NOT_START};
        _ ->
            F = fun() ->
                        Q1 = qlc:q([X || X <- mnesia:table(Tbl),
                                         X#?CLUSTER_STAT.state == State]),
                        Q2 = qlc:sort(Q1, [{order, ascending}]),
                        qlc:e(Q2)
                end,
            case leo_mnesia:read(F) of
                {ok, Ret} ->
                    {ok, Ret};
                Other ->
                    Other
            end
    end.


%% @doc Retrieve system configuration by cluster-id
%%
-spec(find_by_cluster_id(string()) ->
             {ok, #system_conf{}} | not_found | {error, any()}).
find_by_cluster_id(ClusterId) ->
    Tbl = ?TBL_CLUSTER_STAT,

    case catch mnesia:table_info(Tbl, all) of
        {'EXIT', _Cause} ->
            {error, ?ERROR_MNESIA_NOT_START};
        _ ->
            F = fun() ->
                        Q1 = qlc:q([X || X <- mnesia:table(Tbl),
                                         X#?CLUSTER_STAT.cluster_id == ClusterId]),
                        Q2 = qlc:sort(Q1, [{order, ascending}]),
                        qlc:e(Q2)
                end,
            case leo_mnesia:read(F) of
                {ok, Ret} ->
                    {ok, Ret};
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


%% @doc Retrieve a checksum by cluster-id
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

-spec(checksum(string()) ->
             {ok, pos_integer()} | {error, any()}).
checksum(ClusterId) ->
    case find_by_cluster_id(ClusterId) of
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
                     ?TBL_CLUSTER_STAT,  fun transform/1,
                     record_info(fields, ?CLUSTER_STAT),
                     ?CLUSTER_STAT),
    ok.

%% @doc the record is the current verion
%% @private
transform(#?CLUSTER_STAT{} = ClusterStat) ->
    ClusterStat;
transform(#cluster_stat{cluster_id = ClusterId,
                        status     = State,
                        checksum   = Checksum,
                        updated_at = UpdatedAt}) ->
    ClusterId_1 = case is_atom(ClusterId) of
                      true  -> ClusterId;
                      false -> list_to_atom(ClusterId)
                  end,
    #?CLUSTER_STAT{cluster_id = ClusterId_1,
                   state = State,
                   checksum = Checksum,
                   updated_at = UpdatedAt
                  }.
