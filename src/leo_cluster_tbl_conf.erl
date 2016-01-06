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
%% @doc The cluster conf table's operation
%% @reference https://github.com/leo-project/leo_redundant_manager/blob/master/src/leo_cluster_tbl_conf.erl
%% @end
%%======================================================================
-module(leo_cluster_tbl_conf).
-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/qlc.hrl").

%% API
-export([create_table/2,
         all/0, get/0, find_by_ver/1,
         update/1, delete/1,
         checksum/0, synchronize/1,
         transform/0
        ]).


%% @doc Create the system-configutation table
%%
-spec(create_table(Mode, Nodes) ->
             ok | {error, any()} when Mode::mnesia_copies(),
                                      Nodes::[atom()]).
create_table(Mode, Nodes) ->
    case mnesia:create_table(
           ?TBL_SYSTEM_CONF,
           [{Mode, Nodes},
            {type, set},
            {record_name, ?SYSTEM_CONF},
            {attributes, record_info(fields, ?SYSTEM_CONF)},
            {user_properties,
             [
              {version,              pos_integer, primary},
              {cluster_id,           atom,        false},
              {dc_id,                atom,        false},
              {n,                    pos_integer, false},
              {r,                    pos_integer, false},
              {w,                    pos_integer, false},
              {d,                    pos_integer, false},
              {bit_of_ring,          pos_integer, false},
              {num_of_dc_replicas,   pos_integer, false},
              {num_of_rack_replicas, pos_integer, false},
              {max_mdc_targets,      pos_integer, false}
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
             {ok, [#?SYSTEM_CONF{}]} | not_found | {error, any()}).
all() ->
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
            leo_mnesia:read(F)
    end.


%% @doc Retrieve a system configuration (latest)
%%
-spec(get() ->
             {ok, #?SYSTEM_CONF{}} | not_found | {error, any()}).
get() ->
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
            get_1(leo_mnesia:read(F))
    end.
get_1({ok, [H|_]}) ->
    {ok, H};
get_1(Other) ->
    Other.


%% @doc Retrieve a system configuration
%%
-spec(find_by_ver(Ver) ->
             {ok, #?SYSTEM_CONF{}} |
             not_found |
             {error, any()} when Ver::pos_integer()).
find_by_ver(Ver) ->
    Tbl = ?TBL_SYSTEM_CONF,
    case catch mnesia:table_info(Tbl, all) of
        {'EXIT', _Cause} ->
            {error, ?ERROR_MNESIA_NOT_START};
        _ ->
            F = fun() ->
                        Q = qlc:q([X || X <- mnesia:table(Tbl),
                                        X#?SYSTEM_CONF.version == Ver]),

                        qlc:e(Q)
                end,
            get_1(leo_mnesia:read(F))
    end.



%% @doc Modify a system-configuration
%%
-spec(update(SystemConfig) ->
             ok | {error, any()} when SystemConfig::#?SYSTEM_CONF{}).
update(SystemConfig) ->
    Tbl = ?TBL_SYSTEM_CONF,
    case catch mnesia:table_info(Tbl, all) of
        {'EXIT', _Cause} ->
            {error, ?ERROR_MNESIA_NOT_START};
        _ ->
            F = fun()-> mnesia:write(Tbl, SystemConfig, write) end,
            leo_mnesia:write(F)
    end.


%% @doc Remove a system-configuration
%%
-spec(delete(SystemConfig) ->
             ok | {error, any()} when SystemConfig::#?SYSTEM_CONF{}).
delete(SystemConfig) ->
    Tbl = ?TBL_SYSTEM_CONF,
    case catch mnesia:table_info(Tbl, all) of
        {'EXIT', _Cause} ->
            {error, ?ERROR_MNESIA_NOT_START};
        _ ->
            Fun = fun() ->
                          mnesia:delete_object(Tbl, SystemConfig, write)
                  end,
            leo_mnesia:delete(Fun)
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


%% @doc Synchronize records
%%
-spec(synchronize(ValL) ->
             ok | {error, any()} when ValL::[#?SYSTEM_CONF{}]).
synchronize(ValL) ->
    case synchronize_1(ValL) of
        ok ->
            case all() of
                {ok, CurValL} ->
                    ok = synchronize_2(CurValL, ValL);
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
synchronize_2([#?SYSTEM_CONF{version = Ver}|Rest], Vals) ->
    ok = synchronize_2_1(Vals, Ver),
    synchronize_2(Rest, Vals).

%% @private
synchronize_2_1([], Ver)->
    case find_by_ver(Ver) of
        {ok, #?SYSTEM_CONF{} = SystemConf} ->
            delete(SystemConf);
        _ ->
            void
    end,
    ok;
synchronize_2_1([#?SYSTEM_CONF{version = Ver}|_], Ver)->
    ok;
synchronize_2_1([#?SYSTEM_CONF{}|Rest], Ver) ->
    synchronize_2_1(Rest, Ver).


%% @doc Transform records
%%
-spec(transform() ->
             ok | {error, any()}).
transform() ->
    {atomic, ok} = mnesia:transform_table(
                     ?TBL_SYSTEM_CONF,  fun transform/1,
                     record_info(fields, ?SYSTEM_CONF),
                     ?SYSTEM_CONF),
    ok.


%% @doc the record is the current verion
%% @private
transform(#?SYSTEM_CONF{} = SystemConf) ->
    SystemConf;
transform(#system_conf{version = Vsn,
                       n = N,
                       r = R,
                       w = W,
                       d = D,
                       bit_of_ring = BitOfRing,
                       level_1 = Level1,
                       level_2 = Level2}) ->
    #?SYSTEM_CONF{version = Vsn,
                  cluster_id = ?DEF_CLUSTER_ID,
                  dc_id = ?DEF_DC_ID,
                  n = N,
                  r = R,
                  w = W,
                  d = D,
                  bit_of_ring = BitOfRing,
                  num_of_dc_replicas = Level1,
                  num_of_rack_replicas = Level2,
                  max_mdc_targets = ?DEF_MAX_MDC_TARGETS
                 };
transform(#system_conf_1{version = Vsn,
                         cluster_id = ClusterId,
                         dc_id = DCId,
                         n = N,
                         r = R,
                         w = W,
                         d = D,
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
    #?SYSTEM_CONF{version = Vsn,
                  cluster_id = ClusterId_1,
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
