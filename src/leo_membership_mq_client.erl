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
%% ---------------------------------------------------------------------
%% Leo Redundant Manager - Membership's MQ Client.
%%
%% @doc The membership operation's messsage-queues client
%% @reference https://github.com/leo-project/leo_redundant_manager/blob/master/src/leo_membership_mq_client.erl
%% @end
%%======================================================================
-module(leo_membership_mq_client).

-author('Yosuke Hara').

-behaviour(leo_mq_behaviour).

-include("leo_redundant_manager.hrl").
-include_lib("leo_mq/include/leo_mq.hrl").
-include_lib("eunit/include/eunit.hrl").


-export([start/2, publish/3]).
-export([init/0, handle_call/1, handle_call/3]).

-define(MQ_MONITOR_NODE, 'mq_monitor_node').
-define(MQ_WORKER_NODE, 'mq_worker_node').
-define(MQ_PERSISTENT_NODE, 'mq_persistent_node').
-define(MQ_DB_PATH, "membership").

-record(message, {node :: atom(),
                  error :: any(),
                  times = 0 :: integer(),
                  published_at = 0 :: integer()}).

-ifdef(TEST).
-define(DEF_RETRY_TIMES, 3).
-define(DEF_TIMEOUT, 1000).
-define(DEF_MAX_INTERVAL, 100).
-define(DEF_MIN_INTERVAL, 50).

-else.
-define(DEF_RETRY_TIMES, 3).
-define(DEF_TIMEOUT, 30000).
-define(DEF_MAX_INTERVAL, 15000).
-define(DEF_MIN_INTERVAL, 7500).
-endif.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc create queues and launch mq-servers.
%%
-spec(start(ServerType, RootPath) ->
             ok | {error, any()} when ServerType::node_type()|atom(),
                                      RootPath::string()).
start(?MONITOR_NODE, RootPath) ->
    start1(?MQ_MONITOR_NODE, RootPath);
start(?PERSISTENT_NODE, RootPath) ->
    start1(?MQ_PERSISTENT_NODE, RootPath);
start(?WORKER_NODE, RootPath) ->
    start1(?MQ_WORKER_NODE, RootPath);
start(_,_) ->
    {error, badarg}.

start1(InstanceId, RootPath0) ->
    RootPath1 = case (string:len(RootPath0) == string:rstr(RootPath0, "/")) of
                    true  -> RootPath0;
                    false -> RootPath0 ++ "/"
                end,
    {ok, RefSup} = application:get_env(leo_redundant_manager, mq_sup_ref),

    leo_mq_api:new(RefSup, InstanceId, [{?MQ_PROP_MOD, ?MODULE},
                                        {?MQ_PROP_DB_PROCS, 1},
                                        {?MQ_PROP_DB_NAME, ?DEF_BACKEND_DB},
                                        {?MQ_PROP_ROOT_PATH, RootPath1 ++ ?MQ_DB_PATH},
                                        {?MQ_PROP_INTERVAL_MAX, ?DEF_CONSUME_MAX_INTERVAL},
                                        {?MQ_PROP_INTERVAL_REG, ?DEF_CONSUME_REG_INTERVAL},
                                        {?MQ_PROP_BATCH_MSGS_MAX, ?DEF_CONSUME_MAX_BATCH_MSGS},
                                        {?MQ_PROP_BATCH_MSGS_REG, ?DEF_CONSUME_REG_BATCH_MSGS}
                                       ]),
    ok.


%% @doc Publish a message into the queue.
%%
-spec(publish(ServerType, Node, Error) ->
             ok | {error, any()} when ServerType::atom(),
                                      Node::atom(),
                                      Error::any()).
publish(ServerType, Node, Error) ->
    publish(ServerType, Node, Error, 1).

%% @doc Publish a message into the queue.
%%
-spec(publish(ServerType, Node, Error, Times) ->
             ok | {error, any()} when ServerType::atom(),
                                      Node::atom(),
                                      Error::any(),
                                      Times::non_neg_integer()).
publish(ServerType, Node, Error, Times) ->
    KeyBin     = term_to_binary(Node),
    MessageBin = term_to_binary(#message{node = Node,
                                         error = Error,
                                         times = Times,
                                         published_at = leo_date:now()}),
    publish(ServerType, {KeyBin, MessageBin}).

%% @doc Publish a message into the queue.
%%
-spec(publish(ServerType, KeyAndMessage) ->
             ok | {error, any()} when ServerType::node_type(),
                                      KeyAndMessage::{binary(),binary()}).
publish(?MONITOR_NODE, {KeyBin, MessageBin}) ->
    leo_mq_api:publish(?MQ_MONITOR_NODE, KeyBin, MessageBin);
publish(?PERSISTENT_NODE, {KeyBin, MessageBin}) ->
    leo_mq_api:publish(?MQ_PERSISTENT_NODE, KeyBin, MessageBin);
publish(?WORKER_NODE, {KeyBin, MessageBin}) ->
    leo_mq_api:publish(?MQ_WORKER_NODE, KeyBin, MessageBin);
publish(InstanceId, {KeyBin, MessageBin}) when InstanceId == ?MQ_MONITOR_NODE;
                                               InstanceId == ?MQ_WORKER_NODE;
                                               InstanceId == ?MQ_PERSISTENT_NODE ->
    leo_mq_api:publish(InstanceId, KeyBin, MessageBin);
publish(_,_) ->
    {error, badarg}.


%%--------------------------------------------------------------------
%% Callbacks
%%--------------------------------------------------------------------
%% @doc Initializer
%%
-spec(init() ->
             ok).
init() ->
    ok.


%% @doc Subscribe callback function
-spec(handle_call({consume, Id, Reply}) ->
             ok when Id::any(),
                     Reply::any()).
handle_call({consume, Id, MessageBin}) ->
    Message = binary_to_term(MessageBin),
    #message{node  = RemoteNode,
             times = Times,
             error = Error} = Message,

    case leo_redundant_manager_api:get_member_by_node(RemoteNode) of
        {ok, #member{state = State}} ->
            case leo_misc:node_existence(RemoteNode, timer:seconds(10)) of
                true when State == ?STATE_STOP ->
                    notify_error_to_manager(Id, RemoteNode, Error);
                true ->
                    ok;
                false ->
                    case State of
                        ?STATE_ATTACHED  ->
                            ok;
                        ?STATE_SUSPEND   ->
                            ok;
                        ?STATE_DETACHED  ->
                            ok;
                        ?STATE_RESTARTED ->
                            ok;
                        _ ->
                            case (Times == ?DEF_RETRY_TIMES) of
                                true ->
                                    notify_error_to_manager(Id, RemoteNode, Error);
                                false ->
                                    NewMessage = Message#message{times = Times + 1},
                                    publish(Id, {term_to_binary(RemoteNode), term_to_binary(NewMessage)})
                            end
                    end
            end;
        _ ->
            ok
    end.

handle_call(_,_,_) ->
    ok.


%%--------------------------------------------------------------------
%% INNTERNAL FUNCTIONS
%%--------------------------------------------------------------------
notify_error_to_manager(Id, RemoteNode, Error) when Id == ?MQ_WORKER_NODE;
                                                    Id == ?MQ_PERSISTENT_NODE ->
    {ok, Monitors} = application:get_env(?APP, ?PROP_MONITORS),
    {ok, [Mod, Method]} = ?env_notify_mod_and_method(),

    lists:foldl(fun(_, true ) ->
                        void;
                   (Dest, false) ->
                        case rpc:call(Dest, Mod, Method,
                                      [error, RemoteNode, erlang:node(), Error], ?DEF_TIMEOUT) of
                            {ok, _}     -> true;
                            {_, _Cause} -> false;
                            timeout     -> false
                        end
                end, false, Monitors),
    ok;
notify_error_to_manager(?MQ_MONITOR_NODE, RemoteNode, Error) ->
    {ok, [Mod, Method]} = ?env_notify_mod_and_method(),
    catch erlang:apply(Mod, Method, [error, RemoteNode, node(), Error]),
    ok;
notify_error_to_manager(_,_,_) ->
    {error, badarg}.
