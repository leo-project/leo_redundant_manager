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
%% ---------------------------------------------------------------------
%% Leo Redundant Manager - ETS/Mnesia Handler for Member
%% @doc
%% @end
%%======================================================================
-module(leo_members_table_transformer).

-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").

-export([transform/2]).


%% @doc Transform records
%%
transform('0.16.0', '0.16.5') ->
    OldTbl  = 'leo_members',
    NewTbls = [?MEMBER_TBL_CUR, ?MEMBER_TBL_PREV],

    case (leo_redundant_manager_table_member:table_size(OldTbl) > 0) of
        true ->
            Node = leo_redundant_manager_table_member:first(OldTbl),
            case transform_1(OldTbl, NewTbls, Node) of
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
    end;
transform(_,_) ->
    {error, not_support}.


%% @private
transform_1(OldTbl, NewTbls, Node) ->
    case leo_redundant_manager_table_member:lookup(OldTbl, Node) of
        {ok, Member} ->
            case transform_1_1(NewTbls, Member) of
                ok ->
                    Ret = leo_redundant_manager_table_member:next(OldTbl, Node),
                    transform_2(Ret, OldTbl, NewTbls);
                Error ->
                    Error
            end;
        _ ->
            ok
    end.

%% @private
transform_1_1([],_) ->
    ok;
transform_1_1([Tbl|Rest], #member{node = Node} = Member) ->
    case leo_redundant_manager_table_member:insert(Tbl, {Node, Member}) of
        ok ->
            transform_1_1(Rest, Member);
        Error ->
            Error
    end.

%% @private
transform_2('$end_of_table',_OldTblInfo,_NewTblInfo) ->
    ok;
transform_2(Node, OldTbl, NewTbls) ->
    transform_1(OldTbl, NewTbls, Node).


