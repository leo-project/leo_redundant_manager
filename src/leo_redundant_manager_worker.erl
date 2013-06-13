%%======================================================================
%%
%% Leo Redundant Manager
%%
%% Copyright (c) 2012-2013 Rakuten, Inc.
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
-module(leo_redundant_manager_worker).

-author('Yosuke Hara').

-behaviour(gen_server).

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/0, start_link/1, stop/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
	       terminate/2,
         code_change/3]).

-record(routing_table, {index = []    :: list(pos_integer()),
                        checksum = -1 :: integer(),
                        table = []    :: list()
                       }).

-record(state, {cur  = #routing_table{} :: #routing_table{},
                prev = #routing_table{} :: #routing_table{},
                interval = 3000  :: pos_integer(),
                enable   = false :: boolean()
               }).

-undef(DEF_TIMEOUT).

-ifdef(TEST).
-define(CURRENT_TIME,      65432100000).
-define(DEF_SYNC_INTERVAL, 1000).
-define(DEF_SYNC_MIN,       100).
-define(DEF_TIMEOUT,       1000).
-else.
-define(CURRENT_TIME,      leo_date:now()).
-define(DEF_SYNC_INTERVAL, 1000).
-define(DEF_SYNC_MIN,       100).
-define(DEF_TIMEOUT,       3000).
-endif.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
start_link() ->
    start_link(?DEF_SYNC_INTERVAL).

start_link([]) ->
    start_link(?DEF_SYNC_INTERVAL);
start_link(Interval) ->
    gen_server:start_link(?MODULE, [Interval], []).

stop() ->
    gen_server:call(?MODULE, stop, ?DEF_TIMEOUT).


%%--------------------------------------------------------------------
%% GEN_SERVER CALLBACKS
%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State}          |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
init([Interval]) ->
    sync(Interval),
    {ok, #state{}}.

handle_call(stop,_From,State) ->
    {stop, normal, ok, State};


handle_call(_Handle, _From, State) ->
    {reply, ok, State}.


%% Function: handle_cast(Msg, State) -> {noreply, State}          |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
handle_cast(sync, State) ->
    case catch maybe_sync(State#state{enable = true}) of
        {'EXIT', _Reason} ->
            {noreply, State};
        NewState ->
            {noreply, NewState}
    end;

handle_cast(_Msg, State) ->
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
%% @doc Synchronize
%% @private
-spec(sync(integer()) ->
             ok | any()).
sync(Time) ->
    Time1 = erlang:phash2(term_to_binary(leo_date:clock()), Time) + ?DEF_SYNC_MIN,
    catch timer:apply_after(Time1, gen_server, cast, [self(), sync]).

%% @doc Heatbeat
%% @private
-spec(maybe_sync(#state{}) ->
             #state{}).
maybe_sync(#state{enable = false} = State) ->
    State;
maybe_sync(#state{cur  = #routing_table{checksum = CurHash},
                  prev = #routing_table{checksum = PrevHash},
                  interval  = Interval,
                  enable    = true} = State) ->

    {ok, {R1, R2}}= leo_redundant_manager_api:checksum(?CHECKSUM_RING),
    ?debugVal({{R1, R2}, {CurHash, PrevHash}}),

    NewState = case (R1 == -1 orelse R2 == -1) of
                   true ->
                       State;
                   false when R1 == CurHash andalso
                              R2 == PrevHash ->
                       State;
                   false ->
                       maybe_sync_1(State, {R1, R2}, {CurHash, PrevHash})
               end,
    sync(Interval),
    NewState#state{enable = false}.



maybe_sync_1(State, {R1, R2}, {CurHash, PrevHash}) ->
    State1 = case (R1 == CurHash) of
                 true  -> State;
                 false ->
                     {ok, CurHash1} = gen_routing_table(?SYNC_MODE_CUR_RING),
                     State#state{cur = #routing_table{checksum = CurHash1}}
             end,
    State2 = case (R2 == PrevHash) of
                 true  -> State1;
                 false ->
                     {ok, PrevHash1} = gen_routing_table(?SYNC_MODE_PREV_RING),
                     State1#state{prev = #routing_table{checksum = PrevHash1}}
             end,
    State2.


%% @doc
%%
gen_routing_table(Version) ->
    %% Retrieve ring from local's master [etc|mnesia]
    {ok, CurRing} = leo_redundant_manager_api:get_ring(Version),
    Checksum = erlang:crc32(term_to_binary(CurRing)),
    ?debugVal(Checksum),
    %%


    {ok, Checksum}.
