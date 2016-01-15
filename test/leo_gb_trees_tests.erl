%%======================================================================
%%
%% Leo Redundant Manager
%%
%% Copyright (c) 2012-2016 Rakuten, Inc.
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
-module(leo_gb_trees_tests).

-include_lib("eunit/include/eunit.hrl").


%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

suite_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [{with, [T]} || T <- [
                           fun suite_/1
                          ]]}.

setup() ->
    ok.

teardown(_) ->
    ok.

suite_(_) ->
    Tree_1 = leo_gb_trees:empty(),
    NodeL = ['node_0', 'node_1', 'node_2', 'node_3'],
    Tree_2 = lists:foldl(
               fun(N, Acc) ->
                       Node = lists:nth((N rem 4) + 1, NodeL),
                       leo_gb_trees:insert(N * 10, Node, Acc)
               end, Tree_1, lists:seq(1,1024)),
    Tree_3 = leo_gb_trees:balance(Tree_2),

    _ = erlang:statistics(wall_clock),
    [begin
         {Key,_Node} = leo_gb_trees:lookup(N, Tree_3),
         N_1 = leo_math:ceiling(N / 10) * 10,
         ?assertEqual(N_1, Key),
         ok
     end || N <- lists:seq(1, 10240)],
    {_,Time} = erlang:statistics(wall_clock),
    ?debugVal({time, Time}),
    ok.

-endif.
