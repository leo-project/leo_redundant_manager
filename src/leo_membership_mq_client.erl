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

-define(MQ_INSTANCE_ID_MANAGER, 'membership_manager').
-define(MQ_INSTANCE_ID_GATEWAY, 'membership_gateway').
-define(MQ_INSTANCE_ID_STORAGE, 'membership_storage').
-define(MQ_DB_PATH,             "membership").

-type(type_of_server() :: manager | gateway | storage).

-record(message, {node             :: atom(),
                  error            :: any(),
                  times = 0        :: integer(),
                  published_at = 0 :: integer()}).

-ifdef(TEST).
-define(DEF_RETRY_TIMES, 3).
-define(DEF_TIMEOUT,     1000).
-define(DEF_MAX_INTERVAL, 100).
-define(DEF_MIN_INTERVAL,  50).

-else.
-define(DEF_RETRY_TIMES, 3).
-define(DEF_TIMEOUT,     30000).
-define(DEF_MAX_INTERVAL,15000).
-define(DEF_MIN_INTERVAL, 7500).
-endif.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc create queues and launch mq-servers.
%%
-spec(start(ServerType, RootPath) ->
             ok | {error, any()} when ServerType::type_of_server(),
                                      RootPath::string()).
start(?SERVER_MANAGER, RootPath) ->
    start1(?MQ_INSTANCE_ID_MANAGER, RootPath);
start(?SERVER_GATEWAY, RootPath) ->
    start1(?MQ_INSTANCE_ID_GATEWAY, RootPath);
start(?SERVER_STORAGE, RootPath) ->
    start1(?MQ_INSTANCE_ID_STORAGE, RootPath);
start(_, _) ->
    {error, badarg}.

start1(InstanceId, RootPath0) ->
    RootPath1 = case (string:len(RootPath0) == string:rstr(RootPath0, "/")) of
                    true  -> RootPath0;
                    false -> RootPath0 ++ "/"
                end,
    {ok, RefSup} = application:get_env(leo_redundant_manager, mq_sup_ref),

    leo_mq_api:new(RefSup, InstanceId, [{?MQ_PROP_MOD,          ?MODULE},
                                        {?MQ_PROP_DB_PROCS,     1},
                                        {?MQ_PROP_ROOT_PATH,    RootPath1 ++ ?MQ_DB_PATH},
                                        {?MQ_PROP_MAX_INTERVAL, ?DEF_MAX_INTERVAL},
                                        {?MQ_PROP_MIN_INTERVAL, ?DEF_MIN_INTERVAL}
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
             ok | {error, any()} when ServerType::atom(),
                                      KeyAndMessage::{binary(),binary()}).
publish(manager, {KeyBin, MessageBin}) ->
    leo_mq_api:publish(?MQ_INSTANCE_ID_MANAGER, KeyBin, MessageBin);
publish(gateway, {KeyBin, MessageBin}) ->
    leo_mq_api:publish(?MQ_INSTANCE_ID_GATEWAY, KeyBin, MessageBin);
publish(storage, {KeyBin, MessageBin}) ->
    leo_mq_api:publish(?MQ_INSTANCE_ID_STORAGE, KeyBin, MessageBin);
publish(InstanceId, {KeyBin, MessageBin}) when InstanceId == ?MQ_INSTANCE_ID_MANAGER;
                                               InstanceId == ?MQ_INSTANCE_ID_GATEWAY;
                                               InstanceId == ?MQ_INSTANCE_ID_STORAGE ->
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


%% @doc Publish callback function
%%
-spec(handle_call({publish | consume, Id, Reply}) ->
             ok when Id::any(),
                     Reply::any()).
handle_call({publish, _Id, _Reply}) ->
    ok;

%% @doc Subscribe callback function
%%
handle_call({consume, Id, MessageBin}) ->
    Message = binary_to_term(MessageBin),
    #message{node  = RemoteNode,
             times = Times,
             error = Error} = Message,

    case leo_redundant_manager_api:get_member_by_node(RemoteNode) of
        {ok, #member{state = State}} ->
            case leo_misc:node_existence(RemoteNode, (10 * 1000)) of
                true when State == ?STATE_STOP ->
                    notify_error_to_manager(Id, RemoteNode, Error);
                true ->
                    void;
                false ->
                    case State of
                        ?STATE_ATTACHED  -> void;
                        ?STATE_SUSPEND   -> void;
                        ?STATE_DETACHED  -> void;
                        ?STATE_RESTARTED -> void;
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
notify_error_to_manager(Id, RemoteNode, Error) when Id == ?MQ_INSTANCE_ID_GATEWAY;
                                                    Id == ?MQ_INSTANCE_ID_STORAGE ->
    {ok, Managers}   = application:get_env(?APP, ?PROP_MANAGERS),
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
                end, false, Managers),
    ok;
notify_error_to_manager(?MQ_INSTANCE_ID_MANAGER, RemoteNode, Error) ->
    {ok, [Mod, Method]} = ?env_notify_mod_and_method(),
    catch erlang:apply(Mod, Method, [error, RemoteNode, node(), Error]);

notify_error_to_manager(_,_,_) ->
    {error, badarg}.
