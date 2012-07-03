%%======================================================================
%%
%% Leo Redundant Manager
%%
%% Copyright (c) 2012
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
%% Leo Redundant Manager - Basho bench config
%% @doc
%% @end
%%======================================================================
-module(basho_bench_driver_leo_redundant_manager).

-export([new/1,
         run/4]).


%% @doc initialize
%%
-spec(new(any()) ->
             ok).
new(_Id) ->
    [] = os:cmd("epmd -daemon"),
    {ok, Hostname} = inet:gethostname(),
    Test0Node = list_to_atom("test_0@" ++ Hostname),
    net_kernel:start([Test0Node, shortnames]),

    application:start(mnesia),
    leo_redundant_manager_mnesia:create_table(ram_copies),

    leo_redundant_manager_api:start(),
    leo_redundant_manager_api:set_options([{n, 3},
                                           {r, 1},
                                           {w ,2},
                                           {d, 2},
                                           {bit_of_ring, 128}]),
    leo_redundant_manager_api:attach('node_0@127.0.0.1'),
    leo_redundant_manager_api:attach('node_1@127.0.0.1'),
    leo_redundant_manager_api:attach('node_2@127.0.0.1'),
    leo_redundant_manager_api:attach('node_3@127.0.0.1'),
    leo_redundant_manager_api:attach('node_4@127.0.0.1'),
    leo_redundant_manager_api:attach('node_5@127.0.0.1'),
    leo_redundant_manager_api:attach('node_6@127.0.0.1'),
    leo_redundant_manager_api:attach('node_7@127.0.0.1'),
    _Res = leo_redundant_manager_api:create(),

    {ok, null}.


%% @doc run.
%%
-spec(run(get, any(), any(), any()) ->
             {ok, any()} | {error, any(), any()}).
run(get, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case leo_redundant_manager_api:get_redundancies_by_key(integer_to_list(Key)) of
        {ok, _} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

