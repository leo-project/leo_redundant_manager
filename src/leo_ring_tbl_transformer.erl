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
%% @doc The ring table's record transformer
%% @reference https://github.com/leo-project/leo_redundant_manager/blob/master/src/leo_ring_tbl_transformer.erl
%% @end
%%======================================================================
-module(leo_ring_tbl_transformer).

-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([transform/0]).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc The table schema migrate to the new one by using mnesia:transform_table
%%
-spec(transform() -> ok).
transform() ->
    timer:sleep(erlang:phash2(leo_date:clock(), 3000)),
    {atomic, ok} = mnesia:transform_table(
                     ?RING_TBL_CUR,
                     fun transform/1, record_info(fields, ?RING), ?RING),
    {atomic, ok} = mnesia:transform_table(
                     ?RING_TBL_PREV,
                     fun transform/1, record_info(fields, ?RING), ?RING),

    %% execute migration of ring-data
    migrate_ring().

%% @private
transform(#?RING{} = Ring) ->
    Ring;
transform(#ring{vnode_id = VNodeId,
                node = Node}) ->
    #ring_0_16_8{vnode_id = VNodeId,
                 node = Node,
                 clock = 0}.


%% @doc Migrate ring's data
%%
%% @private
migrate_ring() ->
    %% Migrate ring-data both current-ring and previous-ring
    timer:sleep(erlang:phash2(leo_date:clock(), 1000)),
    case catch leo_redundant_manager_api:get_ring(?SYNC_TARGET_RING_CUR) of
        {ok, RingCur} ->
            case migrate_ring(RingCur, ?RING_TBL_CUR) of
                ok ->
                    case leo_redundant_manager_api:get_ring(?SYNC_TARGET_RING_PREV) of
                        {ok, []} ->
                            ok;
                        {ok, RingPrev} ->
                            migrate_ring(RingPrev, ?RING_TBL_PREV)
                    end;
                {error, Cause} ->
                    {error, Cause}
            end;
        {error, Cause} ->
            {error, Cause};
        {'EXIT', Cause} ->
            {error, Cause}
    end.

%% @private
migrate_ring([], _) ->
    ok;
migrate_ring([{VNodeId, Node, _}|Rest], RingTbl) ->
    MemberTbl = case RingTbl of
                    ?RING_TBL_CUR  -> ?MEMBER_TBL_CUR;
                    ?RING_TBL_PREV -> ?MEMBER_TBL_PREV
                end,
    case leo_cluster_tbl_member:find_by_name(MemberTbl, Node) of
        {ok, [#member{clock = Clock}|_]} ->
            ok = leo_cluster_tbl_ring:insert(
                   {mnesia, RingTbl}, {VNodeId, Node, Clock}),
            migrate_ring(Rest, RingTbl);
        not_found ->
            Cause = "Member not found - " ++ atom_to_list(Node),
            error_logger:warning_msg("~p,~p,~p,~p~n",
                                     [{module, ?MODULE_STRING},
                                      {function, "migrate_ring/2"},
                                      {line, ?LINE}, {body, Cause}]),
            {error, Cause};
        {error, Cause} ->
            error_logger:warning_msg("~p,~p,~p,~p~n",
                                     [{module, ?MODULE_STRING},
                                      {function, "migrate_ring/2"},
                                      {line, ?LINE}, {body, Cause}]),
            {error, "Could not get a member"}
    end.

