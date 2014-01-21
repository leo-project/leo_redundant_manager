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
-export([heartbeat/0]).

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
-define(DEF_MEMBERSHIP_INTERVAL, 10000).
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
    {stop, normal, ok, State}.


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
    State_1 = State#state{timestamp = ThisTime},

    case ((ThisTime - Timestamp) < Interval) of
        true ->
            void;
        false ->
            case leo_redundant_manager_tbl_cluster_mgr:all() of
                {ok, Managers} ->
                    ok = exec(Managers, []);
                not_found ->
                    void;
                {error, Cause} ->
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING},
                                            {function, "maybe_heartbeat/1"},
                                            {line, ?LINE}, {body, Cause}])
            end
    end,

    defer_heartbeat(Interval),
    State_1.


%% @doc Heartbeat
%% @private
-spec(defer_heartbeat(integer()) ->
             ok | any()).
defer_heartbeat(Time) ->
    catch timer:apply_after(Time, ?MODULE, heartbeat, []).


%% @doc Retrieve status and members from a remote-cluster
%%      and then compare 'hash' whether equal or not
%% @private
-spec(exec(list(), atom()) ->
             ok | {error, any()}).
exec([], _) ->
    ok;
exec([#cluster_manager{cluster_id = ClusterId}|Rest], ClusterId) ->
    exec(Rest, ClusterId);
exec([#cluster_manager{node = Node,
                       cluster_id = ClusterId}|Rest], PrevClusterId) ->
    %% Retrieve the status of remote-cluster
    case leo_rpc:call(Node, leo_redundant_manager_api, get_cluster_status, []) of
        {ok, #cluster_stat{status  = Status_1,
                           checksum = Checksum_1} = ClusterStat} ->
            %% Compare its status in the local with the retrieved data
            case leo_redundant_manager_tbl_cluster_stat:get(ClusterId) of
                {ok, #cluster_stat{status   = Status_2,
                                   checksum = Checksum_2}}
                  when Status_1   == Status_2,
                       Checksum_1 == Checksum_2 ->
                    void;
                {ok, LocalClusterStat} ->
                    exec_1(ClusterStat, LocalClusterStat);
                not_found ->
                    exec_1(ClusterStat, undefined);
                _ ->
                    void
            end;
        _ ->
            void
    end,
    exec(Rest, PrevClusterId).

%% @private
exec_1(ClusterStat, undefined) ->
    exec_1(ClusterStat, #cluster_stat{});
exec_1(#cluster_stat{checksum = Checksum_1} = ClusterStat,
       #cluster_stat{checksum = Checksum_2}) ->
    %% Update status
    ok = leo_redundant_manager_tbl_cluster_stat:update(
           ClusterStat#cluster_stat{updated_at = leo_date:now()}),

    %% Retrieve new members and then store them
    case (Checksum_1 /= Checksum_2) of
        true ->
            %% @TODO
            ok;
        false ->
            void
    end.
