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
-export([start_link/0, start_link/1, start_link/3, start_link/4, stop/0]).

%% Callbacks
-export([init/1]).

-ifdef(TEST).
%% -define(MNESIA_TYPE_COPIES, 'ram_copies').
-define(MODULE_SET_ENV_1(), application:set_env(?APP, 'notify_mf', [leo_manager_api, notify])).
-define(MODULE_SET_ENV_2(), application:set_env(?APP, 'sync_mf',   [leo_manager_api, synchronize])).
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
%% @spec (Params) -> ok
%% @doc start link.
%% @end
start_link() ->
    Res = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
    after_proc(Res).

start_link(ServerType) ->
    start_link_sub(ServerType).

start_link(ServerType, Managers, MQStoragePath) ->
    start_link(ServerType, Managers, MQStoragePath, []).

start_link(ServerType0, Managers, MQStoragePath, Options) ->
    %% initialize
    Res = start_link_sub(ServerType0),
    ServerType1 = server_type(ServerType0),
    ok = leo_misc:set_env(?APP, ?PROP_SERVER_TYPE, ServerType1),

    case (Options == []) of
        true  -> void;
        false -> ok = leo_redundant_manager_api:set_options(Options)
    end,

    %% launch membership
    Args = [ServerType1, Managers],
    ChildSpec = {leo_membership, {leo_membership, start_link, Args},
                 permanent, 2000, worker, [leo_membership]},

    case supervisor:start_child(leo_redundant_manager_sup, ChildSpec) of
        {ok, _Pid} ->
            ok = leo_membership_mq_client:start(ServerType1, MQStoragePath),
            ok = leo_membership:start_heartbeat(),
            Res;
        Cause ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "start/4"},
                                    {line, ?LINE}, {body, Cause}]),
            case leo_redundant_manager_sup:stop() of
                ok ->
                    exit(invalid_launch);
                not_started ->
                    exit(noproc)
            end
    end.

%% @private
start_link_sub(ServerType) ->
    %% launch sup
    Ret = case supervisor:start_link({local, ?MODULE}, ?MODULE, []) of
              {ok, _RefSup} = Res0 ->
                  Res0;
              {error, {already_started, ResSup}} ->
                  {ok, ResSup};
              Other ->
                  Other
          end,

    case Ret of
        {ok, _} ->
            Res1 = after_proc(Ret),
            ok = leo_misc:init_env(),
            _  = ?MODULE_SET_ENV_1(),
            _  = ?MODULE_SET_ENV_2(),
            ok = init_tables(ServerType),
            Res1;
        Error ->
            Error
    end.


%% @spec () -> ok |
%%             not_started
%% @doc stop process.
%% @end
stop() ->
    case whereis(?MODULE) of
        Pid when is_pid(Pid) == true ->
            exit(Pid, shutdown),
            ok;
        _ -> not_started
    end.

%% ---------------------------------------------------------------------
%% Callbacks
%% ---------------------------------------------------------------------
%% @spec (Params) -> ok
%% @doc stop process.
%% @end
%% @private
init([]) ->
    %% Redundant Manager Server
    Children = [
                {leo_redundant_manager,
                 {leo_redundant_manager, start_link, []},
                 permanent,
                 2000,
                 worker,
                 [leo_redundant_manager]}
               ],

    %% Redundant Manager Worker Pool
    WorkerSpecs =
        lists:map(
          fun(Index) ->
                  Id = list_to_atom(lists:append([?WORKER_POOL_NAME_PREFIX,
                                                  integer_to_list(Index)])),
                  {Id, {leo_redundant_manager_worker, start_link, [Id]},
                   permanent, ?SHUTDOWN_WAITING_TIME, worker, [leo_redundant_manager_worker]}
          end, lists:seq(0, (?RING_WORKER_POOL_SIZE -1))),

    {ok, {_SupFlags = {one_for_one, 5, 60}, Children ++ WorkerSpecs}}.


%% ---------------------------------------------------------------------
%% Inner Function(s)
%% ---------------------------------------------------------------------
%% @doc After processing
%% @private
-spec(after_proc({ok, pid()} | {error, any()}) ->
             {ok, pid()} | {error, any()}).
after_proc({ok, RefSup}) ->
    %% MQ
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


%% @doc Retrieve a server-type.
%% @private
server_type(master) -> ?SERVER_MANAGER;
server_type(slave)  -> ?SERVER_MANAGER;
server_type(Type)   -> Type.


%% @doc Create members table.
%% @private
-ifdef(TEST).
init_tables(_)  ->
    catch leo_redundant_manager_table_member:create_members(),
    catch ets:new(?CUR_RING_TABLE, [named_table, ordered_set, public, {read_concurrency, true}]),
    catch ets:new(?PREV_RING_TABLE,[named_table, ordered_set, public, {read_concurrency, true}]),
    ok.
-else.
init_tables(manager) -> ok;
init_tables(master)  -> ok;
init_tables(slave)   -> ok;
init_tables(_Other)  ->
    catch leo_redundant_manager_table_member:create_members(),
    catch ets:new(?CUR_RING_TABLE, [named_table, ordered_set, public, {read_concurrency, true}]),
    catch ets:new(?PREV_RING_TABLE,[named_table, ordered_set, public, {read_concurrency, true}]),
    ok.
-endif.
