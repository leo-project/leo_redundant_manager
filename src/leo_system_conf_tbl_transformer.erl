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
-module(leo_system_conf_tbl_transformer).

-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([transform/0]).


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
                       n       = N,
                       r       = R,
                       w       = W,
                       d       = D,
                       bit_of_ring = BitOfRing,
                       level_1 = Level1,
                       level_2 = Level2}) ->
    #?SYSTEM_CONF{version = Vsn,
                  cluster_id = [],
                  dc_id = [],
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
                         dc_id   = DCId,
                         n       = N,
                         r       = R,
                         w       = W,
                         d       = D,
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
