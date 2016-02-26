%%======================================================================
%%
%% Leo Redundant Manager
%%
%% Copyright (c) 2012-2016 Rakuten, Inc.
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

-behaviour(supervisor).

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%% External API
-export([start_link/0,
         stop/0]).
%% Callbacks
-export([init/1]).
%% Others
-export([start_child/3]).


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
-define(MAX_RESTART, 5).
-define(MAX_TIME, 60).


%%-----------------------------------------------------------------------
%% External API
%%-----------------------------------------------------------------------
%% @doc Launch and link the process
-spec(start_link() ->
             {ok, RefSup} | {error, Cause} when RefSup::pid(),
                                                Cause::any()).
start_link() ->
    case supervisor:start_link({local, ?MODULE}, ?MODULE, []) of
        {ok, RefSup} ->
            %% Launch the mq-sup
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
            ok = application:set_env(
                   leo_redundant_manager, mq_sup_ref, MQPid),

            %% Initialize environment vars
            ok = leo_misc:init_env(),
            ?MODULE_SET_ENV_1(),
            ?MODULE_SET_ENV_2(),
            {ok, RefSup};
        {error, {already_started, RefSup}} ->
            {ok, RefSup};
        Other ->
            Other
    end.


%% @doc Launch the process
-spec(start_child(ClusgerId, NodeType, Option) ->
             {ok, RefSup} | {error, Cause} when ClusgerId::cluster_id(),
                                                NodeType::node_type(),
                                                Option::[{option_item_key(), any()}],
                                                RefSup::pid(),
                                                Cause::any()).
start_child(ClusterId, NodeType, Option) ->
    Monitors = leo_misc:get_value(?ITEM_KEY_MONITORS, Option, []),
    MQDir = leo_misc:get_value(?ITEM_KEY_MQ_DIR, Option),
    SystemConf = leo_misc:get_value(?ITEM_KEY_SYSTEM_CONF, Option),
    MembershipCallback = leo_misc:get_value(?ITEM_KEY_MEMBERSHIP_CALLBACK, Option),

    %% Launch membership for local-cluster,
    %% then lunch mdc-tables sync
    case start_child_1(ClusterId, NodeType,
                       Monitors, MembershipCallback) of
        ok ->
            %% Start membership of the cluster
            ok = leo_membership_mq_client:start(ClusterId, NodeType, MQDir),
            ok = leo_membership_cluster_local:start_heartbeat(
                   ?id_membership_local(ClusterId)),

            %% Set environment vars
            ok = leo_misc:set_env(?APP, ?id_red_type(ClusterId), NodeType),
            case (SystemConf == []) of
                true  ->
                    void;
                false ->
                    ok = leo_redundant_manager_api:set_options(
                           ClusterId, SystemConf)
            end,
            ok;
        Cause ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "start_link/3"},
                                    {line, ?LINE}, {body, Cause}]),
            case leo_redundant_manager_sup:stop() of
                ok ->
                    exit(invalid_launch);
                not_started ->
                    exit(noproc)
            end
    end.

%% @private
start_child_1(ClusterId, NodeType, Monitors, MembershipCallback) ->
    ok = init_tables(NodeType),
    Specs = [
             {?id_red_server(ClusterId),
              {leo_redundant_manager, start_link, [ClusterId]},
              permanent,
              ?SHUTDOWN_WAITING_TIME, worker,
              [leo_redundant_manager]},

             {?id_red_worker(ClusterId),
              {leo_redundant_manager_worker, start_link, [ClusterId]},
              permanent,
              ?SHUTDOWN_WAITING_TIME, worker,
              [leo_redundant_manager_worker]},

             {?id_membership_local(ClusterId),
              {leo_membership_cluster_local,
               start_link,
               [ClusterId, NodeType, Monitors, MembershipCallback]},
              permanent, ?SHUTDOWN_WAITING_TIME, worker,
              [leo_membership_cluster_local]}
             %% @TODO
             %% {leo_mdcr_tbl_sync,
             %%  {leo_mdcr_tbl_sync,
             %%   start_link,
             %%   [NodeType, Monitors]},
             %%  permanent, 2000, worker,
             %%  [leo_mdcr_tbl_sync]}
            ],
    start_child_2(
      begin
          case NodeType of
              ?MONITOR_NODE ->
                  lists:reverse([{leo_membership_cluster_remote,
                                  {leo_membership_cluster_remote, start_link, []},
                                  permanent,
                                  ?SHUTDOWN_WAITING_TIME, worker,
                                  [leo_membership_cluster_remote]}|Specs]);
              _ ->
                  Specs
          end
      end).

%% @private
start_child_2([]) ->
    ok;
start_child_2([Spec|Rest]) ->
    case supervisor:start_child(leo_redundant_manager_sup, Spec) of
        {ok,_} ->
            start_child_2(Rest);
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
    {ok, {_SupFlags = {one_for_one, 5, 60}, []}}.


%% ---------------------------------------------------------------------
%% Inner Function(s)
%% ---------------------------------------------------------------------
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

init_tables(?MONITOR_NODE) ->
    ok;
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
