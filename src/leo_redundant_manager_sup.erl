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

%% External API
-export([start_link/0, stop/0]).

%% Callbacks
-export([init/1]).

%%-----------------------------------------------------------------------
%% External API
%%-----------------------------------------------------------------------
%% @spec (Params) -> ok
%% @doc start link.
%% @end
start_link() ->
    Res = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
    after_proc(Res).


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
    Children = [
                {leo_redundant_manager,
                 {leo_redundant_manager, start_link, []},
                 permanent,
                 2000,
                 worker,
                 [leo_redundant_manager]}
               ],
    {ok, {_SupFlags = {one_for_one, 5, 60}, Children}}.


%% ---------------------------------------------------------------------
%% Inner Function(s)
%% ---------------------------------------------------------------------
%% @doc After processing
%% @private
-spec(after_proc({ok, pid()} | {error, any()}) ->
             {ok, pid()} | {error, any()}).
after_proc({ok, RefSup}) ->
    RefMqSup =
        case whereis(leo_mq_sup) of
            undefined ->
                ChildSpec = {leo_mq_sup,
                             {leo_mq_sup, start_link, []},
                             permanent, 2000, supervisor, [leo_mq_sup]},
                {ok, Pid} = supervisor:start_child(RefSup, ChildSpec),
                Pid;
            Pid ->
                Pid
        end,

    ok = application:set_env(leo_redundant_manager, mq_sup_ref, RefMqSup),
    {ok, RefSup};

after_proc(Error) ->
    Error.
