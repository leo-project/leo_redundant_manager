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
-module(leo_redundant_manager_tbl_cluster_member).

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
      ?TBL_CLUSTER_MEMBER,
      [{Mode, Nodes},
       {type, set},
       {record_name, cluster_member},
       {attributes, record_info(fields, cluster_member)},
       {user_properties,
        [{node,       string, primary},
         {cluster_id, string, false},
         {alias,         varchar, false},
         {ip,            varchar, false},
         {port,          integer, false},
         {inet,          varchar, false},
         {clock,         integer, false},
         {num_of_vnodes, integer, false},
         {state,         varchar, false}
        ]}
      ]).


%% @doc Retrieve system configuration by cluster-id
%%
-spec(all() ->
             {ok, [#cluster_member{}]} | not_found | {error, any()}).
all() ->
    Tbl = ?TBL_CLUSTER_MEMBER,

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
             {ok, #cluster_member{}} | not_found | {error, any()}).
get(ClusterId) ->
    Tbl = ?TBL_CLUSTER_MEMBER,

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
-spec(update(#cluster_member{}) ->
             ok | {error, any()}).
update(Member) ->
    Tbl = ?TBL_CLUSTER_MEMBER,

    case catch mnesia:table_info(Tbl, all) of
        {'EXIT', _Cause} ->
            {error, ?ERROR_MNESIA_NOT_START};
        _ ->
            F = fun()-> mnesia:write(Tbl, Member, write) end,
            leo_mnesia:write(F)
    end.


%% @doc Remove system-configuration
%%
-spec(delete(string()) ->
             ok | {error, any()}).
delete(ClusterId) ->
    Tbl = ?TBL_CLUSTER_MEMBER,

    case ?MODULE:get(ClusterId) of
        {ok, Members} ->
            delete_1(Members, Tbl);
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
