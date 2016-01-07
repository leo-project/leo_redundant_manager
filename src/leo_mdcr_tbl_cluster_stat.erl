%%======================================================================
%%
%% Leo Redundant Manager
%%
%% Copyright (c) 2012-2015 Rakuten, Inc.
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
%% @doc The multi-datacenter cluster status talble's operation
%% @reference https://github.com/leo-project/leo_redundant_manager/blob/master/src/leo_mdcr_tbl_cluster_stat.erl
%% @end
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
         checksum/0, checksum/1, size/0,
         synchronize/1,
         transform/0
        ]).

%% @doc Create a table of system-configutation
%%
-spec(create_table(Mode, Nodes) ->
             ok | {error, any()} when Mode::mnesia_copies(),
                                      Nodes::[atom()]).
create_table(Mode, Nodes) ->
    case mnesia:create_table(
           ?TBL_CLUSTER_STAT,
           [{Mode, Nodes},
            {type, set},
            {record_name, ?CLUSTER_STAT},
            {attributes, record_info(fields, ?CLUSTER_STAT)},
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
             {ok, [#?CLUSTER_STAT{}]} |
             not_found |
             {error, any()}).
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
-spec(get(ClusterId) ->
             {ok, #?CLUSTER_STAT{}} |
             not_found |
             {error, any()} when ClusterId::atom()).
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
-spec(find_by_state(State) ->
             {ok, #?CLUSTER_STAT{}} |
             not_found |
             {error, any()} when State::atom()).
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
-spec(find_by_cluster_id(ClusterId) ->
             {ok, #?CLUSTER_STAT{}} |
             not_found |
             {error, any()} when ClusterId::atom()).
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
                {ok, [Ret|_]} ->
                    {ok, Ret};
                Other ->
                    Other
            end
    end.


%% @doc Modify system-configuration
%%
-spec(update(ClusterStat) ->
             ok | {error, any()} when ClusterStat::#?CLUSTER_STAT{}).
update(ClusterStat) ->
    Tbl = ?TBL_CLUSTER_STAT,
    case catch mnesia:table_info(Tbl, all) of
        {'EXIT', _Cause} ->
            {error, ?ERROR_MNESIA_NOT_START};
        _ ->
            F = fun()-> mnesia:write(Tbl, ClusterStat, write) end,
            leo_mnesia:write(F)
    end.


%% @doc Remove a cluster-status
%%
-spec(delete(ClusterId) ->
             ok | {error, any()} when ClusterId::atom()).
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

-spec(checksum(ClusterId) ->
             {ok, pos_integer()} | {error, any()} when ClusterId::atom()).
checksum(ClusterId) ->
    case find_by_cluster_id(ClusterId) of
        {ok, Vals} ->
            {ok, erlang:crc32(term_to_binary(Vals))};
        not_found ->
            {ok, -1};
        Error ->
            Error
    end.


%% @doc Retrieve the records
%%
-spec(size() ->
             pos_integer()).
size() ->
    mnesia:table_info(?TBL_CLUSTER_STAT, size).


%% @doc Synchronize records
%%
-spec(synchronize(list()) ->
             ok | {error, any()}).
synchronize(Vals) ->
    case synchronize_1(Vals) of
        ok ->
            case all() of
                {ok, CurVals} ->
                    ok = synchronize_2(CurVals, Vals);
                _ ->
                    void
            end;
        Error ->
            Error
    end.

%% @private
synchronize_1([]) ->
    ok;
synchronize_1([V|Rest]) ->
    case update(V) of
        ok ->
            synchronize_1(Rest);
        Error ->
            Error
    end.

%% @private
synchronize_2([],_) ->
    ok;
synchronize_2([#?CLUSTER_STAT{cluster_id = ClusterId}|Rest], Vals) ->
    ok = synchronize_2_1(Vals, ClusterId),
    synchronize_2(Rest, Vals).

%% @private
synchronize_2_1([], ClusterId)->
    ok = delete(ClusterId),
    ok;
synchronize_2_1([#?CLUSTER_STAT{cluster_id = ClusterId}|_], ClusterId)->
    ok;
synchronize_2_1([#?CLUSTER_STAT{}|Rest], ClusterId) ->
    synchronize_2_1(Rest, ClusterId).


%% @doc Transform records
%%
-spec(transform() ->
             ok | {error, any()}).
transform() ->
    {atomic, ok} = mnesia:transform_table(
                     ?TBL_CLUSTER_STAT,  fun transform/1,
                     record_info(fields, ?CLUSTER_STAT),
                     ?CLUSTER_STAT),
    case all() of
        {ok, RetL} ->
            ok = transform_1(RetL);
        _ ->
            void
    end,
    ok.

%% @doc the record is the current verion
%% @private
transform(#?CLUSTER_STAT{} = ClusterStat) ->
    ClusterStat;
transform(#cluster_stat{cluster_id = ClusterId,
                        status = State,
                        checksum = Checksum,
                        updated_at = UpdatedAt}) ->
    #?CLUSTER_STAT{cluster_id = ClusterId,
                   state = State,
                   checksum = Checksum,
                   updated_at = UpdatedAt
                  }.

%% @private
transform_1([]) ->
    ok;
transform_1([#?CLUSTER_STAT{cluster_id = ClusterId}|Rest]) when is_atom(ClusterId) ->
    transform_1(Rest);
transform_1([#?CLUSTER_STAT{cluster_id = ClusterId}|Rest]) ->
    case ?MODULE:get(ClusterId) of
        {ok, #?CLUSTER_STAT{} = ClusterStat} when is_list(ClusterId) ->
            _ = ?MODULE:delete(ClusterId),
            ?MODULE:update(ClusterStat#?CLUSTER_STAT{
                                          cluster_id = list_to_atom(ClusterId)});
        _ ->
            void
    end,
    transform_1(Rest).
