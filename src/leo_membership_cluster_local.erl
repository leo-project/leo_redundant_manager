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
%% Leo Redundant Manager - Membership (LOCAL)
%%
%% @doc The membership operation in the local-cluster
%% @reference https://github.com/leo-project/leo_redundant_manager/blob/master/src/leo_membership_cluster_local.erl
%% @end
%%======================================================================
-module(leo_membership_cluster_local).
-author('Yosuke Hara').

-behaviour(gen_server).

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/2, start_link/3,
         stop/0]).
-export([start_heartbeat/0,
         stop_heartbeat/0,
         heartbeat/0,
         update_manager_nodes/1,
         set_proc_auditor/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
	       terminate/2,
         code_change/3]).

-record(state, {type :: atom(),
                interval = 0 :: non_neg_integer(),
                timestamp = 0 :: non_neg_integer(),
                monitors = [] :: [atom()],
                partner_manager :: atom(),
                proc_auditor :: atom(),
                callback :: undefined|function()
               }).

-ifdef(TEST).
-define(CURRENT_TIME, 65432100000).
-define(DEF_MEMBERSHIP_INTERVAL, 1000).
-define(DEF_MIN_INTERVAL, 100).
-define(DEF_MAX_INTERVAL, 100).
-define(DEF_TIMEOUT, 1000).

-else.
-define(CURRENT_TIME, leo_date:now()).
-define(DEF_MEMBERSHIP_INTERVAL, 5000).
-define(DEF_MIN_INTERVAL, 100).
-define(DEF_MAX_INTERVAL, 300).
-define(DEF_TIMEOUT, 30000).
-endif.


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Start the server
-spec(start_link(ServerType, Monitors) ->
             {ok, pid()} | {error, any()} when ServerType::atom(),
                                               Monitors::[atom()]).
start_link(ServerType, Monitors) ->
    Fun =  fun()-> ok end,
    start_link(ServerType, Monitors, Fun).

-spec(start_link(ServerType, Monitors, Callback) ->
             {ok, pid()} | {error, any()} when ServerType::atom(),
                                               Monitors::[atom()],
                                               Callback::function()
                                                         ).
start_link(ServerType, Monitors, Callback) ->
    ok = application:set_env(?APP, ?PROP_MONITORS, Monitors),
    gen_server:start_link({local, ?MODULE}, ?MODULE,
                          [ServerType, Monitors, Callback, ?DEF_MEMBERSHIP_INTERVAL], []).

%% @doc Stop the server
stop() ->
    gen_server:call(?MODULE, stop, 30000).


%% @doc Start the heartbeat operation
-spec(start_heartbeat() -> ok | {error, any()}).
start_heartbeat() ->
    gen_server:cast(?MODULE, {start_heartbeat}).


%% @doc Stop the heartbeat operation
-spec(stop_heartbeat() -> ok | {error, any()}).
stop_heartbeat() ->
    gen_server:cast(?MODULE, {stop_heartbeat}).


%% @doc Start the heartbeat operation
-spec(heartbeat() -> ok | {error, any()}).
heartbeat() ->
    gen_server:cast(?MODULE, {start_heartbeat}).


%% @doc Set the process of an auditor
-spec(set_proc_auditor(ProcAuditor) ->
             ok | {error, any()} when ProcAuditor::atom()).
set_proc_auditor(ProcAuditor) ->
    gen_server:cast(?MODULE, {set_proc_auditor, ProcAuditor}).


%% @doc Update the manager nodes
-spec(update_manager_nodes(Monitors) ->
             ok | {error, any()} when Monitors::[atom()]).
update_manager_nodes(Monitors) ->
    gen_server:cast(?MODULE, {update_manager_nodes, Monitors}).


%%--------------------------------------------------------------------
%% GEN_SERVER CALLBACKS
%%--------------------------------------------------------------------
%% @doc Initiates the server
init([?MONITOR_NODE = ServerType, [Partner|_] = Monitors, Callback, Interval]) ->
    {ok, #state{type = ServerType,
                interval = Interval,
                timestamp = 0,
                partner_manager = Partner,
                monitors = Monitors,
                callback = Callback
               }, Interval};

init([ServerType, Monitors,_Callback, Interval]) ->
    Callback = fun()-> ok end,
    {ok, #state{type = ServerType,
                interval = Interval,
                timestamp = 0,
                monitors = Monitors,
                callback = Callback
               }, Interval}.


%% @doc gen_server callback - Module:handle_call(Request, From, State) -> Result
handle_call(stop,_From,State) ->
    {stop, normal, ok, State}.


%% @doc Handling cast message
%% <p>
%% gen_server callback - Module:handle_cast(Request, State) -> Result.
%% </p>
handle_cast({start_heartbeat}, #state{interval = Interval} = State) ->
    {noreply, State, Interval};

handle_cast({set_proc_auditor, ProcAuditor}, #state{interval = Interval} = State) ->
    {noreply, State#state{proc_auditor = ProcAuditor}, Interval};

handle_cast({update_manager_nodes, Monitors}, #state{interval = Interval} = State) ->
    ok = application:set_env(?APP, ?PROP_MONITORS, Monitors),
    {noreply, State#state{monitors  = Monitors}, Interval};

handle_cast({stop_heartbeat}, State) ->
    {noreply, State}.


%% @doc Handling all non call/cast messages
%% <p>
%% gen_server callback - Module:handle_info(Info, State) -> Result.
%% </p>
handle_info(timeout, #state{interval = Interval} = State) ->
    case catch heartbeat_fun(State) of
        {'EXIT', _Reason} ->
            {noreply, State, Interval};
        NewState ->
            {noreply, NewState, Interval}
    end;
handle_info(_Info, #state{interval = Interval} = State) ->
    {noreply, State, Interval}.

%% @doc This function is called by a gen_server when it is about to
%%      terminate. It should be the opposite of Module:init/1 and do any necessary
%%      cleaning up. When it returns, the gen_server terminates with Reason.
terminate(_Reason, _State) ->
    ok.

%% @doc Convert process state when code is changed
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%--------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Heatbeat
%% @private
-spec(heartbeat_fun(#state{}) ->
             #state{}).
heartbeat_fun(#state{type = ServerType,
                     interval = Interval,
                     timestamp = Timestamp,
                     monitors = Monitors,
                     proc_auditor = ProcAuditor,
                     callback = Callback} = State) ->
    ThisTime = leo_date:now() * 1000,
    case ((ThisTime - Timestamp) < Interval) of
        true ->
            void;
        false ->
            case ServerType of
                ?WORKER_NODE ->
                    catch ProcAuditor:register_in_monitor(again),
                    catch exec(ServerType, Monitors, Callback);
                ?MONITOR_NODE ->
                    catch exec(ServerType, Monitors, Callback);
                ?PERSISTENT_NODE ->
                    case leo_redundant_manager_api:get_member_by_node(erlang:node()) of
                        {ok, #member{state = ?STATE_RUNNING}}  ->
                            catch exec(ServerType, Monitors, Callback);
                        _ ->
                            void
                    end;
                _ ->
                    void
            end
    end,
    State#state{timestamp = leo_date:now() * 1000}.


%% @doc Execute for manager-nodes.
%% @private
-spec(exec(?MONITOR_NODE | ?PERSISTENT_NODE | ?WORKER_NODE, list(), function()) ->
             ok | {error, any()}).
exec(?MONITOR_NODE = ServerType, Monitors, Callback) ->
    ClusterNodes =
        case leo_cluster_tbl_member:find_all() of
            {ok, Members} ->
                [{Node, State} || #member{node  = Node,
                                          state = State} <- Members];
            _ ->
                []
        end,
    exec_1(ServerType, Monitors, ClusterNodes, Callback);

%% @doc Execute for worker-node and persistent-nodes
exec(ServerType, Monitors, Callback) ->
    Redundancies  = ?rnd_nodes_from_ring(),
    NodesAndState = [{Node, State} ||
                        #redundant_node{node = Node,
                                        available = State} <- Redundancies],
    exec_1(ServerType, Monitors, NodesAndState, Callback).


%% @doc Execute for manager-nodes.
%% @private
-spec(exec_1(atom(), [atom()], [{atom(), #state{}}], function()) ->
             ok | {error, any()}).
exec_1(_,_,[],_) ->
    ok;
exec_1(?MONITOR_NODE = ServerType, Monitors, [{Node, State}|T], Callback) ->
    SleepTime = erlang:phash2(leo_date:clock(), ?DEF_MAX_INTERVAL),
    SleepTime_1 = case SleepTime < ?DEF_MIN_INTERVAL of
                      true  -> ?DEF_MIN_INTERVAL;
                      false -> SleepTime
                  end,
    timer:sleep(SleepTime_1),
    case State of
        ?STATE_RUNNING ->
            case is_function(Callback) of
                true ->
                    catch Callback(Node);
                false ->
                    void
            end,
            _ = compare_manager_with_remote_chksum(Node, Monitors);
        _ ->
            void
    end,
    exec_1(ServerType, Monitors, T, Callback);

%% @doc Execute for worker-nodes and persistent-nodes.
%%      they does not use the parameter of "callback"
%% @private
exec_1(ServerType, Monitors, [{Node, State}|T], Callback) ->
    case (erlang:node() == Node) of
        true ->
            void;
        false ->
            Ret = compare_with_remote_chksum(Node),
            _ = inspect_result(Ret, [ServerType, Monitors, Node, State])
    end,
    exec_1(ServerType, Monitors, T, Callback).


%% @doc Inspect result value
%% @private
-spec(inspect_result(ok | {error, any()}, list()) ->
             ok).
inspect_result(ok, [ServerType, _, Node, false]) ->
    leo_membership_mq_client:publish(ServerType, Node, ?ERR_TYPE_NODE_DOWN);
inspect_result(ok, _) ->
    ok;
inspect_result({error, {HashType, ?ERR_TYPE_INCONSISTENT_HASH, NodesWithChksum}}, [_, Monitors, _, _]) ->
    notify_error_to_manager(Monitors, HashType, NodesWithChksum);
inspect_result({error, ?ERR_TYPE_NODE_DOWN}, [ServerType,_,Node,_]) ->
    leo_membership_mq_client:publish(ServerType, Node, ?ERR_TYPE_NODE_DOWN);
inspect_result(Error, _) ->
    error_logger:warning_msg("~p,~p,~p,~p~n",
                             [{module, ?MODULE_STRING},
                              {function, "inspect_result/2"},
                              {line, ?LINE}, {body, Error}]),
    ok.


%% @doc Compare manager-hash with remote-node-hash
%% @private
-spec(compare_manager_with_remote_chksum(atom(), list()) ->
             ok).
compare_manager_with_remote_chksum(Node, Monitors) ->
    compare_manager_with_remote_chksum(
      Node, Monitors, [?CHECKSUM_RING,
                       ?CHECKSUM_MEMBER,
                       ?CHECKSUM_WORKER,
                       ?CHECKSUM_SYS_CONF
                      ]).

compare_manager_with_remote_chksum(_Node,_Monitors, []) ->
    ok;
compare_manager_with_remote_chksum( Node, Monitors, [HashType|T]) ->
    case  leo_redundant_manager_api:checksum(HashType) of
        {ok, LocalChksum} ->
            State = case leo_redundant_manager_api:get_member_by_node(Node) of
                        {ok, #member{state = ?STATE_STOP}} -> false;
                        _ -> true
                    end,

            Ret = compare_with_remote_chksum_1(Node, HashType, LocalChksum),
            ok  = inspect_result(Ret, [?MONITOR_NODE, Monitors, Node, State]),
            compare_manager_with_remote_chksum(Node, Monitors, T);
        Error ->
            Error
    end.


%% @doc Comapare own-hash with remote-node-hash
%% @private
-spec(compare_with_remote_chksum(atom()) ->
             ok | {error, any()}).
compare_with_remote_chksum(Node) ->
    compare_with_remote_chksum(Node, [?CHECKSUM_RING,
                                      ?CHECKSUM_MEMBER,
                                      ?CHECKSUM_WORKER,
                                      ?CHECKSUM_SYS_CONF
                                     ]).

compare_with_remote_chksum(_,[]) ->
    ok;
compare_with_remote_chksum(Node, [HashType|T]) ->
    case leo_redundant_manager_api:checksum(HashType) of
        {ok, LocalChksum} ->
            case compare_with_remote_chksum_1(Node, HashType, LocalChksum) of
                ok ->
                    compare_with_remote_chksum(Node, T);
                Error ->
                    Error
            end;
        _Error ->
            ok
    end.

%% @private
compare_with_remote_chksum_1(Node, HashType, LocalChksum) ->
    case rpc:call(Node, leo_redundant_manager_api, checksum, [HashType], ?DEF_TIMEOUT) of
        {ok, RemoteChksum} when LocalChksum =:= RemoteChksum ->
            ok;
        {ok, RemoteChksum} when LocalChksum =/= RemoteChksum ->
            {error, {HashType, ?ERR_TYPE_INCONSISTENT_HASH, [{node(), LocalChksum},
                                                             {Node,   RemoteChksum}]}};
        not_found = Cause ->
            error_logger:warning_msg("~p,~p,~p,~p~n",
                                     [{module, ?MODULE_STRING},
                                      {function, "compare_with_remote_chksum/3"},
                                      {line, ?LINE}, {body, {Node, Cause}}]),
            {error, {HashType, ?ERR_TYPE_INCONSISTENT_HASH, [{node(), LocalChksum},
                                                             {Node,   -1}]}};
        {_, Cause} ->
            error_logger:warning_msg("~p,~p,~p,~p~n",
                                     [{module, ?MODULE_STRING},
                                      {function, "compare_with_remote_chksum/3"},
                                      {line, ?LINE}, {body, {Node, Cause}}]),
            {error, ?ERR_TYPE_NODE_DOWN}
    end.


%% @doc Notify an incorrect-info to manager-node
%% @private
-spec(notify_error_to_manager([atom()],
                              ?CHECKSUM_RING | ?CHECKSUM_MEMBER,
                              [tuple()]) ->
             ok).
notify_error_to_manager(Monitors, HashType, NodesWithChksum) ->
    lists:foldl(
      fun(Node0, false) ->
              {ok, [Mod, Method]} = application:get_env(?APP, ?PROP_SYNC_MF),
              Node1 = case is_atom(Node0) of
                          true  -> Node0;
                          false -> list_to_atom(Node0)
                      end,
              case rpc:call(Node1, Mod, Method,
                            [HashType, NodesWithChksum], ?DEF_TIMEOUT) of
                  ok ->
                      ok;
                  Error ->
                      error_logger:warning_msg("~p,~p,~p,~p~n",
                                               [{module, ?MODULE_STRING},
                                                {function, "notify_error_to_manager/3"},
                                                {line, ?LINE}, {body, {Node1, Error}}]),
                      Error
              end;
         (_, true) ->
              ok
      end, false, Monitors),
    ok.
