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
%% Leo Redundant Manager - Membership (REMOTE).
%% @doc
%% @end
%%======================================================================
-module(leo_membership_cluster_remote).

-author('Yosuke Hara').

-behaviour(gen_server).

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/2,
         stop/0]).
-export([start_heartbeat/0,
         stop_heartbeat/0,
         heartbeat/0,
         set_proc_auditor/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
	       terminate/2,
         code_change/3]).

-record(state, {interval         :: integer(),
                timestamp        :: integer(),
                enable   = false :: boolean(),
                proc_auditor     :: atom()
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
start_link(ServerType, Managers) ->
    ok = application:set_env(?APP, ?PROP_MANAGERS, Managers),
    gen_server:start_link({local, ?MODULE}, ?MODULE,
                          [ServerType, Managers, ?DEF_MEMBERSHIP_INTERVAL], []).

stop() ->
    gen_server:call(?MODULE, stop, 30000).


-spec(start_heartbeat() -> ok | {error, any()}).
start_heartbeat() ->
    gen_server:cast(?MODULE, {start_heartbeat}).


-spec(stop_heartbeat() -> ok | {error, any()}).
stop_heartbeat() ->
    gen_server:cast(?MODULE, {stop_heartbeat}).


-spec(heartbeat() -> ok | {error, any()}).
heartbeat() ->
    gen_server:cast(?MODULE, {start_heartbeat}).

-spec(set_proc_auditor(atom()) -> ok | {error, any()}).
set_proc_auditor(ProcAuditor) ->
    gen_server:cast(?MODULE, {set_proc_auditor, ProcAuditor}).


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
handle_cast({start_heartbeat}, State) ->
    case catch maybe_heartbeat(State#state{enable=true}) of
        {'EXIT', _Reason} ->
            {noreply, State};
        NewState ->
            {noreply, NewState}
    end;

handle_cast({set_proc_auditor, ProcAuditor}, State) ->
    {noreply, State#state{proc_auditor = ProcAuditor}};

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
maybe_heartbeat(#state{interval     = Interval,
                       timestamp    = Timestamp,
                       enable       = true} = State) ->
    %% @TODO
    Managers = [],

    ThisTime = leo_date:now() * 1000,
    State_1 = State#state{timestamp = ThisTime},

    case ((ThisTime - Timestamp) < Interval) of
        true ->
            void;
        false ->
            ok = exec(Managers),
            defer_heartbeat(Interval)
    end,
    State_1.


%% @doc Heartbeat
%% @private
-spec(defer_heartbeat(integer()) ->
             ok | any()).
defer_heartbeat(Time) ->
    catch timer:apply_after(Time, ?MODULE, start_heartbeat, []).


%% @doc Execute for manager-nodes.
%% @private
-spec(exec(list()) ->
             ok | {error, any()}).

%% @doc Execute for gateway and storage nodes.
%% @private
exec(_Managers) ->
    %% @TODO
    ok.
