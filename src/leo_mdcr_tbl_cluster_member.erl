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
-module(leo_mdcr_tbl_cluster_member).
-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/qlc.hrl").

%% API
-export([create_table/2,
         all/0, get/1,
         find_by_state/2, find_by_node/1,
         find_by_limit/2, find_by_cluster_id/1,
         update/1, delete/1, delete_by_node/1,
         checksum/0, checksum/1,
         synchronize/1,
         transform/0
        ]).


%% @doc Create a table of system-configutation
%%
create_table(Mode, Nodes) ->
    case mnesia:create_table(
           ?TBL_CLUSTER_MEMBER,
           [{Mode, Nodes},
            {type, set},
            {record_name, ?CLUSTER_MEMBER},
            {attributes, record_info(fields, cluster_member)},
            {user_properties,
             [{node,          string,  primary},
              {cluster_id,    atom,    false},
              {alias,         varchar, false},
              {ip,            varchar, false},
              {port,          integer, false},
              {inet,          varchar, false},
              {clock,         integer, false},
              {num_of_vnodes, integer, false},
              {state,         varchar, false}
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
             {ok, [#?CLUSTER_MEMBER{}]} | not_found | {error, any()}).
all() ->
    Tbl = ?TBL_CLUSTER_MEMBER,
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


%% @doc Retrieve members by cluster-id
%%
-spec(get(string()) ->
             {ok, #?CLUSTER_MEMBER{}} | not_found | {error, any()}).
get(ClusterId) ->
    Tbl = ?TBL_CLUSTER_MEMBER,
    case catch mnesia:table_info(Tbl, all) of
        {'EXIT', _Cause} ->
            {error, ?ERROR_MNESIA_NOT_START};
        _ ->
            F = fun() ->
                        Q1 = qlc:q([X || X <- mnesia:table(Tbl),
                                         X#?CLUSTER_MEMBER.cluster_id == ClusterId]),
                        Q2 = qlc:sort(Q1, [{order, ascending}]),
                        qlc:e(Q2)
                end,
            leo_mnesia:read(F)
    end.


%% @doc Retrieve members by cluseter-id and state
%%
-spec(find_by_state(atom(), node_state()) ->
             {ok, list(#?CLUSTER_MEMBER{})} | not_found | {error, any()}).
find_by_state(ClusterId, State) ->
    Tbl = ?TBL_CLUSTER_MEMBER,
    case catch mnesia:table_info(Tbl, all) of
        {'EXIT', _Cause} ->
            {error, ?ERROR_MNESIA_NOT_START};
        _ ->
            F = fun() ->
                        Q1 = qlc:q([X || X <- mnesia:table(Tbl),
                                         (X#?CLUSTER_MEMBER.cluster_id == ClusterId andalso
                                          X#?CLUSTER_MEMBER.state == State)
                                   ]),
                        Q2 = qlc:sort(Q1, [{order, ascending}]),
                        qlc:e(Q2)
                end,
            leo_mnesia:read(F)
    end.


%% @doc Retrieve a member by a node
%%
-spec(find_by_node(atom())->
             {ok, list(#?CLUSTER_MEMBER{})} | not_found | {error, any()}).
find_by_node(Node) ->
    Tbl = ?TBL_CLUSTER_MEMBER,
    case catch mnesia:table_info(Tbl, all) of
        {'EXIT', _Cause} ->
            {error, ?ERROR_MNESIA_NOT_START};
        _ ->
            F = fun() ->
                        Q = qlc:q([X || X <- mnesia:table(Tbl),
                                        X#?CLUSTER_MEMBER.node == Node
                                  ]),
                        qlc:e(Q)
                end,
            Ret = leo_mnesia:read(F),
            find_by_node_1(Ret)
    end.

%% @private
find_by_node_1({ok, [H|_]}) ->
    {ok, H};
find_by_node_1(Other) ->
    Other.


%% @doc Retrieve members by limit
%%
-spec(find_by_limit(atom(), pos_integer()) ->
             {ok, list(#?CLUSTER_MEMBER{})} | not_found | {error, any()}).
find_by_limit(ClusterId, Rows) ->
    case find_by_state(ClusterId, ?STATE_RUNNING) of
        {ok, List} when Rows >= length(List) ->
            {ok, List};
        {ok, List} ->
            {ok, lists:sublist(List, Rows)};
        Other ->
            Other
    end.


%% @doc Retrieve members by cluster-id
%%
-spec(find_by_cluster_id(string()) ->
             {ok, list(#?CLUSTER_MEMBER{})} | not_found | {error, any()}).
find_by_cluster_id(ClusterId) ->
    Tbl = ?TBL_CLUSTER_MEMBER,
    case catch mnesia:table_info(Tbl, all) of
        {'EXIT', _Cause} ->
            {error, ?ERROR_MNESIA_NOT_START};
        _ ->
            F = fun() ->
                        Q1 = qlc:q([X || X <- mnesia:table(Tbl),
                                         (X#?CLUSTER_MEMBER.cluster_id == ClusterId)
                                   ]),
                        Q2 = qlc:sort(Q1, [{order, ascending}]),
                        qlc:e(Q2)
                end,
            leo_mnesia:read(F)
    end.


%% @doc Modify a member
%%
-spec(update(#?CLUSTER_MEMBER{}) ->
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


%% @doc Remove members by cluster-id
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


%% @doc Remove a member by a node
%%
delete_by_node(Node) ->
    Tbl = ?TBL_CLUSTER_MEMBER,
    case find_by_node(Node) of
        {ok, #?CLUSTER_MEMBER{} = Member} ->
            Fun = fun() ->
                          mnesia:delete_object(Tbl, Member, write)
                  end,
            leo_mnesia:delete(Fun);
        not_found ->
            ok;
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
synchronize_2([#?CLUSTER_MEMBER{node = Node}|Rest], Vals) ->
    ok = synchronize_2_1(Vals, Node),
    synchronize_2(Rest, Vals).

%% @private
synchronize_2_1([], Node)->
    ok = delete(Node),
    ok;
synchronize_2_1([#?CLUSTER_MEMBER{node = Node}|_], Node)->
    ok;
synchronize_2_1([#?CLUSTER_MEMBER{}|Rest], Node) ->
    synchronize_2_1(Rest, Node).


%% @doc Transform records
%%
-spec(transform() ->
             ok | {error, any()}).
transform() ->
    {atomic, ok} = mnesia:transform_table(
                     ?TBL_CLUSTER_MEMBER,  fun transform/1,
                     record_info(fields, ?CLUSTER_MEMBER),
                     ?CLUSTER_MEMBER),
    ok.

%% @doc the record is the current verion
%% @private
transform(#?CLUSTER_MEMBER{} = ClusterInfo) ->
    ClusterInfo;
transform(#cluster_member{node = Node,
                          cluster_id = ClusterId,
                          alias = Alias,
                          ip = IP,
                          port  = Port,
                          inet  = Inet,
                          clock = Clock,
                          num_of_vnodes = NumOfVNodes,
                          status = State
                         }) ->
    ClusterId_1 = case is_atom(ClusterId) of
                      true  -> ClusterId;
                      false -> list_to_atom(ClusterId)
                  end,
    #?CLUSTER_MEMBER{node = Node,
                     cluster_id = ClusterId_1,
                     alias = Alias,
                     ip = IP,
                     port  = Port,
                     inet  = Inet,
                     clock = Clock,
                     num_of_vnodes = NumOfVNodes,
                     state = State
                    }.
