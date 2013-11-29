%%======================================================================
%%
%% Leo Redundant Manager
%%
%% Copyright (c) 2012-2013 Rakuten, Inc.
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
-module(leo_members_table_transformer).

-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([transform/0, transform/1]).


%% @doc Transform records
%%
-spec(transform() ->
             ok | {error, any()}).
transform() ->
    transform([]).

-spec(transform(list(atom())) ->
             ok | {error, any()}).
transform(MnesiaNodes) ->
    OldTbl  = 'leo_members',
    NewTbls = [?MEMBER_TBL_CUR, ?MEMBER_TBL_PREV],

    Ret = mnesia:table_info(OldTbl, size),
    transform_1(Ret, MnesiaNodes, OldTbl, NewTbls).


%% @private
transform_1(0,_,_,_) ->
    ok;
transform_1(_, MnesiaNodes, OldTbl, NewTbls) ->
    case MnesiaNodes of
        [] -> void;
        _  ->
            leo_redundant_manager_table_member:create_members(
              disc_copies, MnesiaNodes, ?MEMBER_TBL_CUR),
            leo_redundant_manager_table_member:create_members(
              disc_copies, MnesiaNodes, ?MEMBER_TBL_PREV)
    end,
    case catch mnesia:table_info(?MEMBER_TBL_CUR, all) of
        {'EXIT', _Cause} ->
            ok;
        _ ->
            case catch mnesia:table_info(?MEMBER_TBL_PREV, all) of
                {'EXIT', _Cause} ->
                    ok;
                _ ->
                    transform_2(OldTbl, NewTbls)
            end
    end.

%% @private
transform_2(OldTbl, NewTbls) ->
    case (leo_redundant_manager_table_member:table_size(OldTbl) > 0) of
        true ->
            Node = leo_redundant_manager_table_member:first(OldTbl),
            case transform_3(OldTbl, NewTbls, Node) of
                ok ->
                    %% recreate prev-ring and current-ring
                    ok = leo_redundant_manager_api:synchronize(?SYNC_TARGET_RING_CUR,  []),
                    ok = leo_redundant_manager_api:synchronize(?SYNC_TARGET_RING_PREV, []),
                    ok;
                Error ->
                    Error
            end;
        false ->
            ok
    end.

%% @private
transform_3(OldTbl, NewTbls, Node) ->
    case leo_redundant_manager_table_member:lookup(OldTbl, Node) of
        {ok, Member} ->
            case transform_3_1(NewTbls, Member) of
                ok ->
                    Ret = leo_redundant_manager_table_member:next(OldTbl, Node),
                    transform_4(Ret, OldTbl, NewTbls);
                Error ->
                    Error
            end;
        _ ->
            ok
    end.

%% @private
transform_3_1([],_) ->
    ok;
transform_3_1([Tbl|Rest], #member{node = Node} = Member) ->
    case leo_redundant_manager_table_member:insert(Tbl, {Node, Member}) of
        ok ->
            transform_3_1(Rest, Member);
        Error ->
            Error
    end.

%% @private
transform_4('$end_of_table',_OldTblInfo,_NewTblInfo) ->
    ok;
transform_4(Node, OldTbl, NewTbls) ->
    transform_3(OldTbl, NewTbls, Node).
