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
%% Leo Redundant Manager - Supervisor
%% @doc
%% @end
%%======================================================================
-module(leo_redundant_manager_sup).
-author('Yosuke Hara').

-behaviour(supervisor).

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%% External API
-export([start_link/0, start_link/1,
         start_link/3, start_link/4, start_link/5,
         stop/0]).

%% Callbacks
-export([init/1]).

-ifdef(TEST).
%% -define(MNESIA_TYPE_COPIES, 'ram_copies').
-define(MODULE_SET_ENV_1(), application:set_env(?APP, ?PROP_NOTIFY_MF, [leo_manager_api, notify])).
-define(MODULE_SET_ENV_2(), application:set_env(?APP, ?PROP_SYNC_MF,   [leo_manager_api, synchronize])).
-else.
%% -define(MNESIA_TYPE_COPIES, 'disc_copies').
-define(MODULE_SET_ENV_1(), void).
-define(MODULE_SET_ENV_2(), void).
-endif.

-define(SHUTDOWN_WAITING_TIME, 2000).
-define(MAX_RESTART,              5).
-define(MAX_TIME,                60).


%%-----------------------------------------------------------------------
%% External API
%%-----------------------------------------------------------------------
%% @doc start link.
%% @end
start_link() ->
    Res = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
    after_proc(Res).

start_link(ServerType) ->
    start_link_1(ServerType).

start_link(ServerType, Monitors, MQStoragePath) ->
    start_link(ServerType, Monitors, MQStoragePath, [], undefined).

start_link(ServerType, Monitors, MQStoragePath, Conf) ->
    start_link(ServerType, Monitors, MQStoragePath, Conf, undefined).

start_link(ServerType, Monitors, MQStoragePath, Conf, MembershipCallback) ->
    %% initialize
    case start_link_1(ServerType) of
        {ok, RefSup} ->
            ok = leo_misc:set_env(?APP, ?PROP_SERVER_TYPE, ServerType),
            case (Conf == []) of
                true  ->
                    void;
                false ->
                    ok = leo_redundant_manager_api:set_options(Conf)
            end,

            %% Launch membership for local-cluster,
            %% then lunch mdc-tables sync
            case start_link_3(ServerType, Monitors, MembershipCallback) of
                ok ->
                    ok = leo_membership_mq_client:start(ServerType, MQStoragePath),
                    ok = leo_membership_cluster_local:start_heartbeat(),
                    {ok, RefSup};
                Cause ->
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING},
                                            {function, "start_link/4"},
                                            {line, ?LINE}, {body, Cause}]),
                    case leo_redundant_manager_sup:stop() of
                        ok ->
                            exit(invalid_launch);
                        not_started ->
                            exit(noproc)
                    end
            end;
        Error ->
            Error
    end.

%% @private
start_link_1(ServerType) ->
    %% launch sup
    Ret = case supervisor:start_link({local, ?MODULE}, ?MODULE, [ServerType]) of
              {ok, _RefSup} = Res0 ->
                  Res0;
              {error, {already_started, ResSup}} ->
                  {ok, ResSup};
              Other ->
                  Other
          end,
    start_link_2(Ret, ServerType).

%% @private
start_link_2({ok, _} = Ret, ServerType) ->
    Reply = after_proc(Ret),
    ok = leo_misc:init_env(),
    _ = ?MODULE_SET_ENV_1(),
    _ = ?MODULE_SET_ENV_2(),
    ok = init_tables(ServerType),
    Reply;
start_link_2(Error,_ServerType) ->
    Error.

%% @private
start_link_3(ServerType, Monitors, MembershipCallback) ->
    case supervisor:start_child(leo_redundant_manager_sup,
                                {leo_membership_cluster_local,
                                 {leo_membership_cluster_local,
                                  start_link,
                                  [ServerType, Monitors, MembershipCallback]},
                                 permanent, 2000, worker,
                                 [leo_membership_cluster_local]}) of
        {ok,_} ->
            case supervisor:start_child(leo_redundant_manager_sup,
                                        {leo_mdcr_tbl_sync,
                                         {leo_mdcr_tbl_sync,
                                          start_link,
                                          [ServerType, Monitors]},
                                         permanent, 2000, worker,
                                         [leo_mdcr_tbl_sync]}) of
                {ok,_} ->
                    ok;
                Error ->
                    Error
            end;
        Error ->
            Error
    end.


%% @spec () -> ok |
%%             not_started
%% @doc stop process.
%% @end
stop() ->
    case whereis(leo_mq_sup) of
        Pid when is_pid(Pid) == true ->
            List = supervisor:which_children(Pid),
            ok = close_db(List),
            ok;
        _ ->
            not_started
    end.


%% ---------------------------------------------------------------------
%% Callbacks
%% ---------------------------------------------------------------------
%% @spec (Params) -> ok
%% @doc stop process.
%% @end
%% @private
init([]) ->
    init([undefined]);
init([ServerType]) ->
    %% Define children
    Children = case ServerType of
                   ?MONITOR_NODE ->
                       [
                        {leo_redundant_manager,
                         {leo_redundant_manager, start_link, []},
                         permanent,
                         ?SHUTDOWN_WAITING_TIME,
                         worker,
                         [leo_redundant_manager]},

                        {leo_membership_cluster_remote,
                         {leo_membership_cluster_remote, start_link, []},
                         permanent,
                         ?SHUTDOWN_WAITING_TIME,
                         worker,
                         [leo_membership_cluster_remote]}
                       ];
                   _ ->
                       [
                        {leo_redundant_manager,
                         {leo_redundant_manager, start_link, []},
                         permanent,
                         ?SHUTDOWN_WAITING_TIME,
                         worker,
                         [leo_redundant_manager]}
                       ]
               end,

    WorkerSpec = {leo_redundant_manager_worker,
                  {leo_redundant_manager_worker, start_link, []},
                  permanent,
                  ?SHUTDOWN_WAITING_TIME,
                  worker,
                  [leo_redundant_manager_worker]},
    {ok, {_SupFlags = {one_for_one, 5, 60}, [WorkerSpec|Children]}}.


%% ---------------------------------------------------------------------
%% Inner Function(s)
%% ---------------------------------------------------------------------
%% @doc After processing
%% @private
-spec(after_proc({ok, pid()} | {error, any()}) ->
             {ok, pid()} | {error, any()}).
after_proc({ok, RefSup}) ->
    MQPid = case whereis(leo_mq_sup) of
                undefined ->
                    ChildSpec = {leo_mq_sup,
                                 {leo_mq_sup, start_link, []},
                                 permanent, 2000, supervisor, [leo_mq_sup]},
                    {ok, Pid} = supervisor:start_child(RefSup, ChildSpec),
                    Pid;
                Pid ->
                    Pid
            end,
    ok = application:set_env(leo_redundant_manager, mq_sup_ref, MQPid),
    {ok, RefSup};

after_proc(Error) ->
    Error.


%% @doc Create members table.
%% @private
-ifdef(TEST).
init_tables(_)  ->
    catch leo_cluster_tbl_member:create_table(?MEMBER_TBL_CUR),
    catch leo_cluster_tbl_member:create_table(?MEMBER_TBL_PREV),
    catch ets:new(?RING_TBL_CUR, [named_table, ordered_set, public, {read_concurrency, true}]),
    catch ets:new(?RING_TBL_PREV,[named_table, ordered_set, public, {read_concurrency, true}]),
    ok.
-else.

init_tables(?MONITOR_NODE) -> ok;
init_tables(_Other) ->
    catch leo_cluster_tbl_member:create_table(?MEMBER_TBL_CUR),
    catch leo_cluster_tbl_member:create_table(?MEMBER_TBL_PREV),
    catch ets:new(?RING_TBL_CUR, [named_table, ordered_set, public, {read_concurrency, true}]),
    catch ets:new(?RING_TBL_PREV,[named_table, ordered_set, public, {read_concurrency, true}]),
    ok.
-endif.


%% @doc Stop a mq's db
%% @private
close_db([]) ->
    ok;
close_db([{Id,_Pid, worker, ['leo_mq_server' = Mod|_]}|T]) ->
    case (string:str(atom_to_list(Id),
                     "membership") > 0) of
        true ->
            ok = Mod:close(Id);
        false ->
            void
    end,
    close_db(T);
close_db([_|T]) ->
    close_db(T).
