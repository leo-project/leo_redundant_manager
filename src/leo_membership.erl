%%======================================================================
%%
%% Leo Redundant Manager
%%
%% Copyright (c) 2012 Rakuten, Inc.
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
%% Leo Redundant Manager - Membership.
%% @doc
%% @end
%%======================================================================
-module(leo_membership).

-author('Yosuke Hara').

-behaviour(gen_server).

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/2,
         stop/0]).
-export([start_heartbeat/0,
         stop_heartbeat/0,
         heartbeat/0]).


%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
	       terminate/2,
         code_change/3]).

-record(state, {type             :: ?SERVER_GATEWAY | ?SERVER_STORAGE | ?SERVER_MANAGER,
                interval         :: integer(),
                timestamp        :: integer(),
                enable   = false :: boolean(),
                managers = []    :: list(),
                partner_manager  :: list()}).

-ifdef(TEST).
-define(CURRENT_TIME,            65432100000).
-define(DEF_MEMBERSHIP_INTERVAL, 1000).
-define(DEF_TIMEOUT,             1000).
-else.
-define(CURRENT_TIME,            leo_date:now()).
-define(DEF_MEMBERSHIP_INTERVAL, 10000).
-define(DEF_TIMEOUT,             3000).
-endif.


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
start_link(ServerType, Managers) ->
    ok = application:set_env(?APP, ?PROP_MANAGERS, Managers, 3000),
    gen_server:start_link({local, ?MODULE}, ?MODULE,
                          [ServerType, Managers, ?DEF_MEMBERSHIP_INTERVAL], []).

stop() ->
    gen_server:call(?MODULE, stop).


-spec(start_heartbeat() -> ok | {error, any()}).
start_heartbeat() ->
    gen_server:cast(?MODULE, {start_heartbeat}).


-spec(stop_heartbeat() -> ok | {error, any()}).
stop_heartbeat() ->
    gen_server:cast(?MODULE, {stop_heartbeat}).


-spec(heartbeat() -> ok | {error, any()}).
heartbeat() ->
    gen_server:cast(?MODULE, {start_heartbeat}).


%%--------------------------------------------------------------------
%% GEN_SERVER CALLBACKS
%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State}          |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
init([?SERVER_MANAGER = ServerType, [Partner|_], Interval]) ->
    defer_heartbeat(Interval),
    {ok, #state{type      = ServerType,
                interval  = Interval,
                timestamp = 0,
                partner_manager = Partner}};

init([ServerType, Managers, Interval]) ->
    defer_heartbeat(Interval),
    {ok, #state{type      = ServerType,
                interval  = Interval,
                timestamp = 0,
                managers  = Managers}}.


handle_call(stop,_From,State) ->
    {stop, normal, ok, State}.


%% Function: handle_cast(Msg, State) -> {noreply, State}          |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
handle_cast({start_heartbeat}, State) ->
    case catch maybe_heartbeat(State#state{enable=true}) of
        {'EXIT', _Reason} ->
            {noreply, State};
        NewState ->
            {noreply, NewState}
    end;

handle_cast({stop_heartbeat}, State) ->
    State#state{enable=false}.


%% Function: handle_info(Info, State) -> {noreply, State}          |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
handle_info(_Info, State) ->
    {noreply, State}.

%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
terminate(_Reason, _State) ->
    ok.

%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Heatbeat
%% @private
-spec(maybe_heartbeat(#state{}) ->
             #state{}).
maybe_heartbeat(#state{enable = false} = State) ->
    State;
maybe_heartbeat(#state{type            = ServerType,
                       interval        = Interval,
                       timestamp       = Timestamp,
                       enable          = true,
                       managers        = Managers} = State) ->
    ThisTime = leo_date:now() * 1000,
    case ((ThisTime - Timestamp) < Interval) of
        true ->
            State;
        false ->
            case ServerType of
                ?SERVER_GATEWAY ->
                    catch exec(ServerType, Managers);
                ?SERVER_MANAGER ->
                    catch exec(ServerType, Managers);
                ?SERVER_STORAGE ->
                    case leo_redundant_manager_api:get_member_by_node(erlang:node()) of
                        {ok, #member{state = ?STATE_SUSPEND}}   -> void;
                        {ok, #member{state = ?STATE_DETACHED}}  -> void;
                        {ok, #member{state = ?STATE_STOP}}      -> void;
                        {ok, #member{state = ?STATE_RESTARTED}} -> void;
                        _ ->
                            catch exec(ServerType, Managers)
                    end
            end,

            defer_heartbeat(Interval),
            State#state{timestamp = ThisTime}
    end.


%% @doc Heartbeat
%% @private
-spec(defer_heartbeat(integer()) ->
             ok | any()).
defer_heartbeat(Time) ->
    catch timer:apply_after(Time, ?MODULE, start_heartbeat, []).


%% @doc Execute for manager-nodes.
%% @private
-spec(exec(?SERVER_MANAGER | ?SERVER_STORAGE | ?SERVER_GATEWAY, list()) ->
             ok | {error, any()}).
exec(?SERVER_MANAGER = ServerType, Managers) ->
    ClusterNodes =
        case leo_redundant_manager_table_member:find_all() of
            {ok, Members} ->
                lists:map(fun(#member{node = Node, state = State}) ->
                                  {storage, Node ,State}
                          end, Members);
            _Error ->
                []
        end,
    exec1(ServerType, Managers, ClusterNodes);

%% @doc Execute for gateway and storage nodes.
%% @private
exec(ServerType, Managers) ->
    {ok, Options} = leo_redundant_manager_api:get_options(),
    BitOfRing     = proplists:get_value('bit_of_ring', Options),
    AddrId        = random:uniform(leo_math:power(2, BitOfRing)),

    case leo_redundant_manager_api:get_redundancies_by_addr_id(AddrId) of
        {ok, #redundancies{nodes = Redundancies}} ->
            Nodes = lists:map(fun({Node, State}) ->
                                      {storage, Node, State}
                              end, Redundancies),
            exec1(ServerType, Managers, Nodes);
        _ ->
            void
    end.


%% @doc Execute for manager-nodes.
%% @private
-spec(exec1(?SERVER_MANAGER | ?SERVER_STORAGE | ?SERVER_GATEWAY, list(), list()) ->
             ok | {error, any()}).
exec1(_,_,[]) ->
    ok;

exec1(?SERVER_MANAGER = ServerType, Managers, [{_, Node, _NodeState}|T]) ->
    case leo_redundant_manager_api:get_member_by_node(Node) of
        {ok, #member{state = ?STATE_SUSPEND}}   -> void;
        {ok, #member{state = ?STATE_DETACHED}}  -> void;
        {ok, #member{state = ?STATE_STOP}}      -> void;
        {ok, #member{state = ?STATE_RESTARTED}} -> void;
        _ ->
            ok = compare_manager_with_remote_chksum(Node, Managers)
    end,
    exec1(ServerType, Managers, T);

%% @doc Execute for gateway-nodes and storage-nodes.
%% @private
exec1(ServerType, Managers, [{_, Node, true}|T]) ->
    case (erlang:node() == Node) of
        true ->
            void;
        false ->
            Ret = compare_with_remote_chksum(Node, ServerType),
            _ = inspect_result(Ret, [ServerType, Managers, Node])
    end,
    exec1(ServerType, Managers, T);

exec1(ServerType, Managers, [_|T]) ->
    exec1(ServerType, Managers, T).


%% @doc Inspect result value
%% @private
-spec(inspect_result(ok | {error, any()}, list()) ->
             ok).
inspect_result(ok, _) ->
    ok;
inspect_result({error, {IncorrectType, ?ERR_TYPE_INCONSISTENT_HASH, Hashs}}, [_ServerType, Managers,_Node]) ->
    _ = notify_error_to_manager(Managers, IncorrectType, Hashs);
inspect_result({error, ?ERR_TYPE_NODE_DOWN}, [ServerType,_Managers, Node]) ->
    _ = leo_membership_mq_client:publish(ServerType, Node, ?ERR_TYPE_NODE_DOWN);
inspect_result(Error, _) ->
    error_logger:warning_msg("~p,~p,~p,~p~n",
                             [{module, ?MODULE_STRING}, {function, "inspect_result/1"},
                              {line, ?LINE}, {body, Error}]).


%% @doc Compare manager-hash with remote-node-hash
%% @private
-spec(compare_manager_with_remote_chksum(atom(), list()) ->
             ok).
compare_manager_with_remote_chksum(Node, Managers) ->
    compare_manager_with_remote_chksum(
      Node, Managers, [?CHECKSUM_RING, ?CHECKSUM_MEMBER]).

compare_manager_with_remote_chksum(_Node,_Managers, []) ->
    ok;
compare_manager_with_remote_chksum( Node, Managers, [HashType|T]) ->
    case  leo_redundant_manager_api:checksum(HashType) of
        {ok, LocalChksum} ->
            Ret = compare_with_remote_chksum(Node, HashType, ?SERVER_MANAGER, LocalChksum),
            _ = inspect_result(Ret, [?SERVER_MANAGER, Managers, Node]),
            compare_manager_with_remote_chksum(Node, Managers, T);
        Error ->
            Error
    end.


%% @doc Comapare own-hash with remote-node-hash
%% @private
-spec(compare_with_remote_chksum(atom(), ?SERVER_MANAGER | ?SERVER_STORAGE | ?SERVER_GATEWAY) ->
             ok | {error, any()}).
compare_with_remote_chksum(Node, LocalServerType) ->
    compare_with_remote_chksum(Node, [?CHECKSUM_RING, ?CHECKSUM_MEMBER], LocalServerType).

compare_with_remote_chksum(_Node, [], _) ->
    ok;
compare_with_remote_chksum(Node, [HashType|T], LocalServerType) ->
    case leo_redundant_manager_api:checksum(HashType) of
        {ok, LocalChecksum} ->
            case compare_with_remote_chksum(Node, HashType, LocalServerType, LocalChecksum) of
                ok ->
                    compare_with_remote_chksum(Node, T, LocalServerType);
                Error ->
                    Error
            end;
        _Error ->
            ok
    end.

compare_with_remote_chksum(Node, HashType, LocalServerType, LocalChksum) ->
    case rpc:call(Node, leo_redundant_manager_api, checksum, [HashType], ?DEF_TIMEOUT) of
        {ok, RemoteChksum} when LocalChksum =:= RemoteChksum ->
            ok;
        %% Case: "New Attached Node" because only storage-node
        {ok, {RingHash0, RingHash1} = RemoteChksum} when HashType == ?CHECKSUM_RING andalso
                                                         LocalServerType /= ?SERVER_GATEWAY andalso
                                                         RingHash0 == RingHash1 ->
            {LocalChksum0, _} = LocalChksum,
            case (RingHash0 == LocalChksum0) of
                true  -> ok;
                false ->
                    {error, {HashType, ?ERR_TYPE_INCONSISTENT_HASH, [{node(), LocalChksum},
                                                                     {Node,   RemoteChksum}]}}
            end;
        {ok, RemoteChksum} when LocalChksum =/= RemoteChksum ->
            {error, {HashType, ?ERR_TYPE_INCONSISTENT_HASH, [{node(), LocalChksum},
                                                             {Node,   RemoteChksum}]}};
        not_found = Cause ->
            error_logger:warning_msg("~p,~p,~p,~p~n",
                                     [{module, ?MODULE_STRING}, {function, "compare_with_remote_chksum/3"},
                                      {line, ?LINE}, {body, {Node, Cause}}]),
            {error, {HashType, ?ERR_TYPE_INCONSISTENT_HASH, [{node(), LocalChksum},
                                                             {Node,   -1}]}};
        {_, Cause} ->
            error_logger:warning_msg("~p,~p,~p,~p~n",
                                     [{module, ?MODULE_STRING}, {function, "compare_with_remote_chksum/3"},
                                      {line, ?LINE}, {body, {Node, Cause}}]),
            {error, ?ERR_TYPE_NODE_DOWN}
    end.


%% @doc Notify an incorrect-info to manager-node
%% @private
-spec(notify_error_to_manager(list(), ?CHECKSUM_RING | ?CHECKSUM_MEMBER, list()) ->
             ok).
notify_error_to_manager(Managers, HashType, Hashs) ->
    {ok, [Mod, Fun]} = application:get_env(?APP, ?PROP_SYNC_MF),

    lists:foldl(
      fun(Node0, false) ->
              Node1 = case is_atom(Node0) of
                          true  -> Node0;
                          false -> list_to_atom(Node0)
                      end,

              case rpc:call(Node1, Mod, Fun, [HashType, Hashs], ?DEF_TIMEOUT) of
                  ok ->
                      true;
                  Error ->
                      error_logger:warning_msg("~p,~p,~p,~p~n",
                                               [{module, ?MODULE_STRING},
                                                {function, "notify_error_to_manager/3"},
                                                {line, ?LINE}, {body, {Node1, Error}}]),
                      false
              end;
         (_, true) ->
              true
      end, false, Managers).

