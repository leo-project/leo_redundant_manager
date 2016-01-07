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
%% @doc The multi-datacenter cluster member table's operation
%% @reference https://github.com/leo-project/leo_redundant_manager/blob/master/src/leo_mdcr_tbl_cluster_member.erl
%% @end
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
         find_by_limit/2, find_by_limit_with_rnd/2,
         find_by_cluster_id/1,
         update/1, delete/1, delete_by_node/1,
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
           ?TBL_CLUSTER_MEMBER,
           [{Mode, Nodes},
            {type, set},
            {record_name, ?CLUSTER_MEMBER},
            {attributes, record_info(fields, cluster_member)},
            {user_properties,
             [{node,          atom,    primary},
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
-spec(get(ClusterId) ->
             {ok, [#?CLUSTER_MEMBER{}]} |
             not_found |
             {error, any()} when ClusterId::atom()).
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
-spec(find_by_state(ClusterId, State) ->
             {ok, list(#?CLUSTER_MEMBER{})} |
             not_found |
             {error, any()} when ClusterId::atom(),
                                 State::node_state()).
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
-spec(find_by_node(Node)->
             {ok, #?CLUSTER_MEMBER{}} |
             not_found |
             {error, any()} when Node::atom()).
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
-spec(find_by_limit(ClusterId, Rows) ->
             {ok, list(#?CLUSTER_MEMBER{})} |
             not_found |
             {error, any()} when ClusterId::atom(),
                                 Rows::pos_integer()).
find_by_limit(ClusterId, Rows) ->
    case find_by_state(ClusterId, ?STATE_RUNNING) of
        {ok, List} when Rows >= length(List) ->
            {ok, List};
        {ok, List} ->
            {ok, lists:sublist(List, Rows)};
        Other ->
            Other
    end.


%% @doc Retrieve members by limit
%%
-spec(find_by_limit_with_rnd(ClusterId, Rows) ->
             {ok, list(#?CLUSTER_MEMBER{})} |
             not_found |
             {error, any()} when ClusterId::atom(),
                                 Rows::pos_integer()).
find_by_limit_with_rnd(ClusterId, Rows) ->
    case find_by_state(ClusterId, ?STATE_RUNNING) of
        {ok, List} ->
            Len = length(List),
            Rows_1 = case (Rows >= Len) of
                         true  -> Len;
                         false -> Rows
                     end,
            find_by_limit_with_rnd_1(List, Rows_1, []);
        Other ->
            Other
    end.

%% @private
find_by_limit_with_rnd_1(_, 0, Acc) ->
    {ok, Acc};
find_by_limit_with_rnd_1(List, Rows, Acc) ->
    Index = erlang:phash2(leo_date:clock(), length(List)) + 1,
    Elem  = lists:nth(Index, List),
    case lists:member(Elem, Acc) of
        true ->
            find_by_limit_with_rnd_1(List, Rows, Acc);
        false ->
            find_by_limit_with_rnd_1(List, Rows - 1, [Elem|Acc])
    end.


%% @doc Retrieve members by cluster-id
%%
-spec(find_by_cluster_id(ClusterId) ->
             {ok, list(#?CLUSTER_MEMBER{})} |
             not_found |
             {error, any()} when ClusterId::atom()).
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
-spec(update(Member) ->
             ok | {error, any()} when Member::#?CLUSTER_MEMBER{}).
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
-spec(delete(ClusterId) ->
             ok | {error, any()} when ClusterId::atom()).
delete(ClusterId) ->
    case ?MODULE:get(ClusterId) of
        {ok, Members} ->
            delete_1(Members, ?TBL_CLUSTER_MEMBER);
        Error ->
            Error
    end.

%% @private
-spec(delete_1([#?CLUSTER_MEMBER{}], atom()) ->
             ok).
delete_1([],_Tbl) ->
    ok;
delete_1([Value|Rest], Tbl) ->
    Fun = fun() ->
                  mnesia:delete_object(Tbl, Value, write)
          end,
    _ = leo_mnesia:delete(Fun),
    delete_1(Rest, Tbl).


%% @doc Remove a member by a node
%%
-spec(delete_by_node(Node) ->
             ok | {error, any()} when Node::atom()).
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
    mnesia:table_info(?TBL_CLUSTER_MEMBER, size).


%% @doc Synchronize records
%%
-spec(synchronize(ValL) ->
             ok | {error, any()} when ValL::[#?CLUSTER_MEMBER{}]).
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
-spec(synchronize_1([#?CLUSTER_MEMBER{}]) ->
             ok | {error, any()}).
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
-spec(synchronize_2([#?CLUSTER_MEMBER{}], [#?CLUSTER_MEMBER{}]) ->
             ok).
synchronize_2([],_) ->
    ok;
synchronize_2([#?CLUSTER_MEMBER{node = Node}|Rest], Vals) ->
    ok = synchronize_2_1(Vals, Node),
    synchronize_2(Rest, Vals).

%% @private
-spec(synchronize_2_1([#?CLUSTER_MEMBER{}], atom()) ->
             ok).
synchronize_2_1([], Node)->
    _ = delete(Node),
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
                          port = Port,
                          inet = Inet,
                          clock = Clock,
                          num_of_vnodes = NumOfVNodes,
                          status = State
                         }) ->
    ClusterId_1 = case is_atom(ClusterId) of
                      true ->
                          ClusterId;
                      false ->
                          list_to_atom(ClusterId)
                  end,
    #?CLUSTER_MEMBER{node = Node,
                     cluster_id = ClusterId_1,
                     alias = Alias,
                     ip = IP,
                     port = Port,
                     inet = Inet,
                     clock = Clock,
                     num_of_vnodes = NumOfVNodes,
                     state = State
                    }.
