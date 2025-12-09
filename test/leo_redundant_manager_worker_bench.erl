%%======================================================================
%%
%% Leo Redundant Manager - Benchmark Tests
%%
%% Copyright (c) 2012-2018 Rakuten, Inc.
%% Copyright (c) 2024 LeoProject. All Rights Reserved.
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
%% @doc Benchmark tests for leo_redundant_manager_worker
%%======================================================================
-module(leo_redundant_manager_worker_bench).

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(NUM_OF_LOOKUPS, 10000).
-define(NUM_OF_NODES, 8).

%%--------------------------------------------------------------------
%% Benchmark Test
%%--------------------------------------------------------------------
benchmark_test_() ->
    {timeout, 300,
     {setup,
      fun setup/0,
      fun teardown/1,
      [{"Benchmark: lookup performance",
        fun benchmark_lookup/0}
      ]}}.

setup() ->
    application:start(crypto),

    %% Clean up existing tables
    catch ets:delete_all_objects(?MEMBER_TBL_CUR),
    catch ets:delete_all_objects(?MEMBER_TBL_PREV),
    catch ets:delete_all_objects('leo_ring_cur'),
    catch ets:delete_all_objects('leo_ring_prv'),

    leo_misc:init_env(),
    leo_misc:set_env(?APP, ?PROP_SERVER_TYPE, ?WORKER_NODE),

    {ok, Pid} = leo_redundant_manager_sup:start_link(?WORKER_NODE),

    %% Set options
    leo_redundant_manager_api:set_options(
      [{n, 3}, {r, 1}, {w, 2}, {d, 2}, {bit_of_ring, 128}]),

    %% Add members using attach API
    lists:foreach(
      fun(N) ->
              Node = list_to_atom("node_" ++ integer_to_list(N) ++ "@127.0.0.1"),
              leo_redundant_manager_api:attach(Node)
      end, lists:seq(0, ?NUM_OF_NODES - 1)),

    %% Create ring
    {ok, _, _} = leo_redundant_manager_api:create(),
    timer:sleep(1000),
    Pid.

teardown(Pid) ->
    catch exit(Pid, shutdown),
    catch leo_redundant_manager_sup:stop(),
    application:stop(crypto),
    ok.

%%--------------------------------------------------------------------
%% Benchmark Functions
%%--------------------------------------------------------------------
benchmark_lookup() ->
    io:format(user, "~n", []),
    io:format(user, "=== Benchmark: lookup Performance ===~n", []),
    io:format(user, "Number of lookups: ~p~n", [?NUM_OF_LOOKUPS]),
    io:format(user, "Number of nodes: ~p~n", [?NUM_OF_NODES]),

    %% Get options for generating random addresses
    {ok, Options} = leo_redundant_manager_api:get_options(),
    BitOfRing = proplists:get_value(bit_of_ring, Options),
    MaxAddr = leo_math:power(2, BitOfRing),

    %% Generate random addresses
    Addresses = [rand:uniform(MaxAddr) || _ <- lists:seq(1, ?NUM_OF_LOOKUPS)],

    %% Warmup
    io:format(user, "Warming up...~n", []),
    lists:foreach(
      fun(Addr) ->
              leo_redundant_manager_api:get_redundancies_by_addr_id(Addr)
      end, lists:sublist(Addresses, 100)),

    %% Benchmark
    io:format(user, "Running benchmark...~n", []),
    {Time, Results} = timer:tc(
                        fun() ->
                                lists:map(
                                  fun(Addr) ->
                                          leo_redundant_manager_api:get_redundancies_by_addr_id(Addr)
                                  end, Addresses)
                        end),

    %% Calculate statistics
    TimeMs = Time / 1000,
    AvgUs = Time / ?NUM_OF_LOOKUPS,
    OpsPerSec = ?NUM_OF_LOOKUPS / (Time / 1000000),

    SuccessCount = length([ok || {ok, _} <- Results]),
    ErrorCount = ?NUM_OF_LOOKUPS - SuccessCount,

    io:format(user, "~n=== Results ===~n", []),
    io:format(user, "Total time:       ~.2f ms~n", [TimeMs]),
    io:format(user, "Average per op:   ~.2f us~n", [AvgUs]),
    io:format(user, "Operations/sec:   ~.2f ops/s~n", [OpsPerSec]),
    io:format(user, "Success:          ~p~n", [SuccessCount]),
    io:format(user, "Errors:           ~p~n", [ErrorCount]),
    io:format(user, "================~n~n", []),

    ?assertEqual(?NUM_OF_LOOKUPS, SuccessCount),
    ?assert(AvgUs < 1000), %% Each lookup should be under 1ms
    ok.
