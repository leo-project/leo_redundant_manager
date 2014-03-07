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
%%======================================================================
-module(leo_mdcr_manager).
-author('Yosuke Hara').

-behaviour(gen_server).

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/0,
         stop/0]).
-export([transfer/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
	       terminate/2,
         code_change/3]).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:call(?MODULE, stop, 30000).


%% @doc Manage to transfer an object
%%
-spec(transfer(any(), atom()) -> ok | {error, any()}).
transfer(Metadata, Callback) ->
    gen_server:cast(?MODULE, {transfer, Metadata, Callback}).


%%--------------------------------------------------------------------
%% GEN_SERVER CALLBACKS
%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State}          |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
init([]) ->
    {ok, null}.

handle_call(stop,_From,State) ->
    {stop, normal, ok, State}.


%% Function: handle_cast(Msg, State) -> {noreply, State}          |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
handle_cast({transfer, Metadata, Callback}, State) ->
    case leo_mdcr_tbl_cluster_info:all() of
        {ok, ClusterInfoList} ->
            case leo_cluster_tbl_conf:get() of
                {ok, #?SYSTEM_CONF{num_of_dc_replicas =  NumOfReplicas}} ->
                    case get_cluster_members(ClusterInfoList,
                                             Metadata, NumOfReplicas, []) of
                        {ok, MDCR_Info} ->
                            Callback:handle_send(MDCR_Info);
                        {error, Cause} ->
                            Callback:handle_fail(Metadata, Cause)
                    end;
                {error, Cause} ->
                    Callback:handle_fail(Metadata, Cause)
            end;
        {error, Cause} ->
            Callback:handle_fail(Metadata, Cause)
    end,
    {noreply, State}.


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
%% @doc Retrieve cluster members for MDC-replication
%% @private
%% -spec(get_cluster_members(list(#?CLUSTER_MEMBER{}), list()) ->
%%     {ok, list()} | {error, any()}).
get_cluster_members([],_Metadata,_NumOfReplicas, Acc) ->
    {ok, Acc};
get_cluster_members([#?CLUSTER_INFO{cluster_id = ClusterId}|Rest],
                    Metadata, NumOfReplicas, Acc) ->
    case leo_mdcr_tbl_cluster_member:find_by_limit(
           ClusterId, ?DEF_NUM_OF_REMOTE_MEMBERS) of
        {ok, ClusterMembers} ->
            get_cluster_members(Rest, Metadata, NumOfReplicas,
                                [#mdc_replication_info{cluster_id = ClusterId,
                                                       num_of_replicas = NumOfReplicas,
                                                       cluster_members = ClusterMembers,
                                                       metadata = Metadata}
                                 |Acc]);
        not_found = Cause ->
            {error, Cause};
        {error, Cause} ->
            {error, Cause}
    end.
