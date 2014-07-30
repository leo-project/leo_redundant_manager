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
%% Leo Redundant Manager - Membership (REMOTE)
%% @doc
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
                timestamp = 0    :: integer()
               }).

-ifdef(TEST).
-define(CURRENT_TIME,            65432100000).
-define(DEF_MEMBERSHIP_INTERVAL, 1000).
-define(DEF_TIMEOUT,             1000).
-else.
-define(CURRENT_TIME,            leo_date:now()).
-define(DEF_MEMBERSHIP_INTERVAL, 20000).
-define(DEF_TIMEOUT,             30000).
-endif.


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE,
                          [?DEF_MEMBERSHIP_INTERVAL], []).

stop() ->
    gen_server:call(?MODULE, stop, 30000).


-spec(heartbeat() -> ok | {error, any()}).
heartbeat() ->
    gen_server:cast(?MODULE, heartbeat).

force_sync(ClusterId, RemoteManagers) ->
    gen_server:call(?MODULE, {force_sync, ClusterId, RemoteManagers}).


%%--------------------------------------------------------------------
%% GEN_SERVER CALLBACKS
%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State}          |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
init([Interval]) ->
    defer_heartbeat(Interval),
    {ok, #state{interval  = Interval,
                timestamp = 0}}.


handle_call(stop,_From,State) ->
    {stop, normal, ok, State};

handle_call({force_sync, ClusterId, RemoteManagers},_From, State) ->
    Mgrs = [#cluster_manager{node = N,
                             cluster_id = ClusterId}
            || N <- RemoteManagers],
    ok = exec(Mgrs),
    {reply, ok, State}.


%% Function: handle_cast(Msg, State) -> {noreply, State}          |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
handle_cast(heartbeat, State) ->
    case catch maybe_heartbeat(State) of
        {'EXIT', _Reason} ->
            {noreply, State};
        NewState ->
            {noreply, NewState}
    end.

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
maybe_heartbeat(#state{interval  = Interval,
                       timestamp = Timestamp} = State) ->
    ThisTime = leo_date:now() * 1000,

    case ((ThisTime - Timestamp) < Interval) of
        true ->
            void;
        false ->
            sync()
    end,

    defer_heartbeat(Interval),
    State#state{timestamp = leo_date:now() * 1000}.


%% @doc Heartbeat
%% @private
-spec(defer_heartbeat(integer()) ->
             ok | any()).
defer_heartbeat(Time) ->
    catch timer:apply_after(Time, ?MODULE, heartbeat, []).


%% @doc Synchronize remote-cluster's status/configurations
%% @private
sync() ->
    case leo_mdcr_tbl_cluster_mgr:all() of
        {ok, Managers} ->
            ok = exec(Managers);
        not_found ->
            void;
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "maybe_heartbeat/1"},
                                    {line, ?LINE}, {body, Cause}])
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
                {ok, #?CLUSTER_STAT{state    = Status_1,
                                    checksum = Checksum_1} = ClusterStat} ->
                    case leo_mdcr_tbl_cluster_stat:get(ClusterId) of
                        {ok, #?CLUSTER_STAT{state    = Status_2,
                                            checksum = Checksum_2}}
                          when Status_1   == Status_2,
                               Checksum_1 == Checksum_2 ->
                            ok;
                        {ok, LocalClusterStat} ->
                            exec_1(Node, ClusterStat, LocalClusterStat);
                        not_found ->
                            exec_1(Node, ClusterStat, #?CLUSTER_STAT{});
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
-spec(exec_1(atom(), #?CLUSTER_STAT{}, #?CLUSTER_STAT{}) ->
             ok | {error, any()}).
exec_1(Node, #?CLUSTER_STAT{checksum   = Checksum_1,
                            cluster_id = ClusterId} = ClusterStat,
       #?CLUSTER_STAT{checksum = Checksum_2}) ->
    %% Retrieve new members and then store them
    case (Checksum_1 /= Checksum_2) of
        true ->
            %% Retrieve the remote members and then store them
            case exec_2(Node, ClusterId) of
                {ok, Checksum} ->
                    %% Update status
                    ok = leo_mdcr_tbl_cluster_stat:update(
                           ClusterStat#?CLUSTER_STAT{checksum = Checksum,
                                                     updated_at = leo_date:now()});
                {error, Cause} ->
                    {error, Cause}
            end;
        false ->
            ok
    end.

%% @private
-spec(exec_2(atom(), atom()) ->
             {ok, integer()} | {error, any()}).
exec_2(Node, ClusterId) ->
    case catch leo_rpc:call(Node, leo_redundant_manager_api,
                            get_members, []) of
        {ok, Members} ->
            Checksum = erlang:crc32(term_to_binary(lists:sort(Members))),
            case exec_3(Members, ClusterId) of
                ok ->
                    {ok, Checksum};
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
                port  = Port,
                inet  = Inet,
                clock = Clock,
                state = State,
                num_of_vnodes = NumOfVNodes}|Rest], ClusterId) ->
    case leo_mdcr_tbl_cluster_member:update(
           #?CLUSTER_MEMBER{node = Node,
                            cluster_id = ClusterId,
                            alias = Alias,
                            ip    = IP,
                            port  = Port,
                            inet  = Inet,
                            clock = Clock,
                            num_of_vnodes = NumOfVNodes,
                            state = State}) of
        ok ->
            exec_3(Rest, ClusterId);
        {error, Cause} ->
            {error, Cause}
    end.
