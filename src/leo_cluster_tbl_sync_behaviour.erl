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
%%======================================================================
-module(leo_cluster_tbl_sync_behaviour).
-author('Yosuke Hara').

-callback(synchronize(HashType::atom(), list()) ->
                 ok | {error, any()}).

-callback(synchronize(BadItems::list(atom()), Node_1::atom(), Node_2::atom()) ->
                 ok | {error, any()}).

-callback(notify(sync, VNodeId::pos_integer(), Node::atom()) ->
                 ok | {error, any()}).
-callback(notify(error, Node_1::atom(), Node_2::atom(), Error::any()) ->
                 ok | {error, any()}).
