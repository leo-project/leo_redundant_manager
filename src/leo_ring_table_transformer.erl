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
-module(leo_ring_table_transformer).

-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([transform/0, transform/1]).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc The table schema migrate to the new one by using mnesia:transform_table
%%
-spec(transform() -> ok).
transform() ->
        {atomic, ok} = mnesia:transform_table(
                     ?RING_TBL_CUR,
                         fun transform/1, record_info(fields, ?RING), ?RING),
        {atomic, ok} = mnesia:transform_table(
                     ?RING_TBL_PREV,
                         fun transform/1, record_info(fields, ?RING), ?RING),
        ok.

%% @private
transform(#?RING{} = Ring) ->
    Ring;
transform(#ring{vnode_id = VNodeId,
                node     = Node}) ->
    #ring_0_16_8{vnode_id = VNodeId,
                 node     = Node,
                 clock    = 0}.

