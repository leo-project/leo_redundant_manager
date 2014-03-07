


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
%%======================================================================
-module(leo_mdcr_manage_sample).
-author('Yosuke Hara').

-behaviour(leo_mdcr_manage_behaviour).

-include_lib("eunit/include/eunit.hrl").

-export([handle_send/1, handle_fail/2]).

handle_send(MDCR_Info) ->
    ?debugVal(MDCR_Info),
    ok.

handle_fail(Metadata, Cause) ->
    ?debugVal({Metadata, Cause}),
    ok.
