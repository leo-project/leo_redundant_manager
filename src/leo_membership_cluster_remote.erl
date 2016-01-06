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
%% Leo Redundant Manager - Membership (REMOTE)
%%
%% @doc The membership operation with remote-cluster(s)
%% @reference https://github.com/leo-project/leo_redundant_manager/blob/master/src/leo_membership_cluster_remote.erl
%% @end
%%======================================================================
-module(leo_membership_cluster_remote).

-author('Yosuke Hara').

-behaviour(gen_server).

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/0,
         stop/0]).
-export([heartbeat/0,
         force_sync/2
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
	       terminate/2,
         code_change/3]).

-record(state, {interval = 30000 :: integer(),
                timestamp = 0 :: integer()
               }).

-ifdef(TEST).
-define(CURRENT_TIME, 65432100000).
-define(DEF_MEMBERSHIP_INTERVAL, 1000).
-define(DEF_TIMEOUT, 1000).
-else.
-define(CURRENT_TIME, leo_date:now()).
-define(DEF_MEMBERSHIP_INTERVAL, 20000).
-define(DEF_TIMEOUT, 30000).
-endif.


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Start the server
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE,
                          [?DEF_MEMBERSHIP_INTERVAL], []).

%% @doc Stop the server
stop() ->
    gen_server:call(?MODULE, stop, 30000).


%% @doc Start the heartbeat operation
-spec(heartbeat() -> ok | {error, any()}).
heartbeat() ->
    gen_server:cast(?MODULE, heartbeat).

%% @doc Force the info to synchronize with the remote-cluster(s)
-spec(force_sync(ClusterId, RemoteMonitors) ->
             ok when ClusterId::atom(),
                     RemoteMonitors::[atom()]).
force_sync(ClusterId, RemoteMonitors) ->
    gen_server:call(?MODULE, {force_sync, ClusterId, RemoteMonitors}).


%%--------------------------------------------------------------------
%% GEN_SERVER CALLBACKS
%%--------------------------------------------------------------------
%% @doc Initiates the server
init([Interval]) ->
    {ok, #state{interval  = Interval,
                timestamp = 0}, Interval}.

%% @doc gen_server callback - Module:handle_call(Request, From, State) -> Result
handle_call(stop,_From,State) ->
    {stop, normal, ok, State};

handle_call({force_sync, ClusterId, RemoteMonitors},_From, #state{interval = Interval} = State) ->
    Mgrs = [#cluster_manager{node = N,
                             cluster_id = ClusterId}
            || N <- RemoteMonitors],
    ok = exec(Mgrs),
    {reply, ok, State, Interval}.


%% @doc Handling cast message
%% <p>
%% gen_server callback - Module:handle_cast(Request, State) -> Result.
%% </p>
handle_cast(heartbeat, #state{interval = Interval} = State) ->
    {noreply, State, Interval}.

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
heartbeat_fun(#state{interval  = Interval,
                     timestamp = Timestamp} = State) ->
    ThisTime = leo_date:now() * 1000,

    case ((ThisTime - Timestamp) < Interval) of
        true ->
            void;
        false ->
            sync()
    end,
    State#state{timestamp = leo_date:now() * 1000}.


%% @doc Synchronize remote-cluster's status/configurations
%% @private
sync() ->
    case leo_mdcr_tbl_cluster_mgr:all() of
        {ok, Monitors} ->
            ok = exec(Monitors);
        not_found ->
            void;
        {error,_Cause} ->
            void
    end,
    ok.


%% @doc Retrieve status and members from a remote-cluster
%%      and then compare 'hash' whether equal or not
%% @private
-spec(exec(list(#cluster_manager{})) ->
             ok | {error, any()}).
exec([]) ->
    ok;
exec([#cluster_manager{node = Node,
                       cluster_id = ClusterId}|Rest]) ->
    %% Retrieve own cluster-id
    case leo_cluster_tbl_conf:get() of
        {ok,  #?SYSTEM_CONF{cluster_id = ClusterId}} ->
            ok;
        {ok,  #?SYSTEM_CONF{}} ->
            %% Retrieve the status of remote-cluster,
            %% then compare its status in the local with the retrieved data
            case catch leo_rpc:call(Node, leo_redundant_manager_api,
                                    get_cluster_status, []) of
                {ok, #?CLUSTER_STAT{state = Status_1,
                                    checksum = Checksum_1} = ClusterStat} ->
                    case leo_mdcr_tbl_cluster_stat:get(ClusterId) of
                        {ok, #?CLUSTER_STAT{state = Status_2,
                                            checksum = Checksum_2}}
                          when Status_1 == Status_2,
                               Checksum_1 == Checksum_2 ->
                            ok;
                        {ok, LocalClusterStat} ->
                            exec_1(Node, ClusterStat, LocalClusterStat);
                        not_found ->
                            exec_1(Node, #?CLUSTER_STAT{cluster_id = ClusterId}, undefined);
                        _ ->
                            ok
                    end;
                _ ->
                    ok
            end,
            exec(Rest);
        Error ->
            Error
    end.


%% @private
-spec(exec_1(atom(), #?CLUSTER_STAT{}, #?CLUSTER_STAT{}|undefined) ->
             ok | {error, any()}).
exec_1(Node,
       #?CLUSTER_STAT{checksum = Checksum_1,
                      cluster_id = ClusterId} = ClusterStat, ClusterStat_2) ->
    Checksum_2 =
        case ClusterStat_2 of
            undefined ->
                -1;
            #?CLUSTER_STAT{checksum = _Checksum} ->
                _Checksum
        end,

    %% Retrieve new members and then store them
    case (Checksum_1 /= Checksum_2) of
        true ->
            %% Retrieve the remote members and then store them
            case exec_2(Node, ClusterId) of
                {ok, {State, Checksum}} ->
                    leo_mdcr_tbl_cluster_stat:update(
                      ClusterStat#?CLUSTER_STAT{state = State,
                                                checksum = Checksum});
                {error, Cause} ->
                    {error, Cause}
            end;
        false ->
            ok
    end.

%% @private
-spec(exec_2(atom(), atom()) ->
             {ok, {node_state() ,integer()}} | {error, any()}).
exec_2(Node, ClusterId) ->
    case catch leo_rpc:call(Node, leo_redundant_manager_api,
                            get_members, []) of
        {ok, Members} ->
            Checksum = erlang:crc32(term_to_binary(lists:sort(Members))),
            case exec_3(Members, ClusterId) of
                ok ->
                    State = case [N ||
                                     #member{node = N,
                                             state = ?STATE_RUNNING} <- Members] of
                                [] ->
                                    ?STATE_STOP;
                                _ ->
                                    ?STATE_RUNNING
                            end,
                    {ok, {State, Checksum}};
                {error, Cause} ->
                    {error, Cause}
            end;
        {_, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "exec_2/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.

%% @private
-spec(exec_3([#member{}], atom()) ->
             ok | {error, any()}).
exec_3([], _) ->
    ok;
exec_3([#member{node = Node,
                alias = Alias,
                ip = IP,
                port = Port,
                inet = Inet,
                clock = Clock,
                state = State,
                num_of_vnodes = NumOfVNodes}|Rest], ClusterId) ->
    case leo_mdcr_tbl_cluster_member:update(
           #?CLUSTER_MEMBER{node = Node,
                            cluster_id = ClusterId,
                            alias = Alias,
                            ip = IP,
                            port = Port,
                            inet = Inet,
                            clock = Clock,
                            num_of_vnodes = NumOfVNodes,
                            state = State}) of
        ok ->
            exec_3(Rest, ClusterId);
        {error, Cause} ->
            {error, Cause}
    end.
