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
%% @doc The multi-datacenter cluster manager talble's operation
%% @reference https://github.com/leo-project/leo_redundant_manager/blob/master/src/leo_mdcr_tbl_cluster_mgr.erl
%% @end
%%======================================================================
-module(leo_mdcr_tbl_cluster_mgr).
-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/qlc.hrl").

%% API
-export([create_table/2,
         all/0, get/1, update/1,
         delete/1, delete_by_node/1,
         checksum/0, size/0,
         synchronize/1
        ]).


%% @doc Create a table of system-configutation
%%
-spec(create_table(Mode, Node) ->
             ok | {error, any()} when Mode::mnesia_copies(),
                                      Node::[atom()]).
create_table(Mode, Nodes) ->
    case mnesia:create_table(
           ?TBL_CLUSTER_MGR,
           [{Mode, Nodes},
            {type, set},
            {record_name, cluster_manager},
            {attributes, record_info(fields, cluster_manager)},
            {user_properties,
             [{node,       atom, primary},
              {cluster_id, atom, false}
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
             {ok, [#cluster_manager{}]} | not_found | {error, any()}).
all() ->
    Tbl = ?TBL_CLUSTER_MGR,

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
-spec(get(ClusterId) ->
             {ok, [#cluster_manager{}]} |
             not_found |
             {error, any()} when ClusterId::atom()).
get(ClusterId) ->
    Tbl = ?TBL_CLUSTER_MGR,
    case catch mnesia:table_info(Tbl, all) of
        {'EXIT', _Cause} ->
            {error, ?ERROR_MNESIA_NOT_START};
        _ ->
            F = fun() ->
                        Q = qlc:q([X || X <- mnesia:table(Tbl),
                                        X#cluster_manager.cluster_id == ClusterId]),
                        qlc:e(Q)
                end,
            leo_mnesia:read(F)
    end.


%% @doc Modify system-configuration
%%
-spec(update(ClusterMgr) ->
             ok | {error, any()} when ClusterMgr::#cluster_manager{}).
update(ClusterMgr) ->
    Tbl = ?TBL_CLUSTER_MGR,
    case catch mnesia:table_info(Tbl, all) of
        {'EXIT', _Cause} ->
            {error, ?ERROR_MNESIA_NOT_START};
        _ ->
            F = fun()-> mnesia:write(Tbl, ClusterMgr, write) end,
            leo_mnesia:write(F)
    end.


%% @doc Remove system-configuration
%%
-spec(delete(ClusterId) ->
             ok | {error, any()} when ClusterId::atom()).
delete(ClusterId) ->
    Tbl = ?TBL_CLUSTER_MGR,
    case ?MODULE:get(ClusterId) of
        {ok, ClusterMgrs} ->
            delete_1(ClusterMgrs, Tbl);
        Error ->
            Error
    end.

delete_1([],_Tbl) ->
    ok;
delete_1([Value|Rest], Tbl) ->
    Fun = fun() ->
                  mnesia:delete_object(Tbl, Value, write)
          end,
    leo_mnesia:delete(Fun),
    delete_1(Rest, Tbl).


%% @doc Remove a node by a node-name
%%
-spec(delete_by_node(Node) ->
             ok | not_found | {error, any()} when Node::atom()).
delete_by_node(Node) ->
    Tbl = ?TBL_CLUSTER_MGR,
    case catch mnesia:table_info(Tbl, all) of
        {'EXIT', _Cause} ->
            {error, ?ERROR_MNESIA_NOT_START};
        _ ->
            F = fun() ->
                        Q = qlc:q([X || X <- mnesia:table(Tbl),
                                        X#cluster_manager.node == Node]),
                        qlc:e(Q)
                end,
            case leo_mnesia:read(F) of
                {ok, [Value|_]} ->
                    Fun = fun() ->
                                  mnesia:delete_object(Tbl, Value, write)
                          end,
                    leo_mnesia:delete(Fun);
                Error ->
                    Error
            end
    end.


%% @doc Retrieve a checksum
%%
-spec(checksum() ->
             {ok, integer()}).
checksum() ->
    case all() of
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
             integer()).
size() ->
    mnesia:table_info(?TBL_CLUSTER_MGR, size).


%% @doc Synchronize records
%%
-spec(synchronize(ValL) ->
             ok | {error, any()} when ValL::[#cluster_manager{}]).
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
synchronize_2([#cluster_manager{node = Node}|Rest], Vals) ->
    ok = synchronize_2_1(Vals, Node),
    synchronize_2(Rest, Vals).

%% @private
synchronize_2_1([], Node)->
    ok = delete_by_node(Node),
    ok;
synchronize_2_1([#cluster_manager{node = Node}|_], Node)->
    ok;
synchronize_2_1([#cluster_manager{}|Rest], Node) ->
    synchronize_2_1(Rest, Node).
