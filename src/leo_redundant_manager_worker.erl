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

-undef(DEF_TIMEOUT).

-ifdef(TEST).
-define(CURRENT_TIME,      65432100000).
-define(DEF_SYNC_MIN_INTERVAL,  250).
-define(DEF_SYNC_MAX_INTERVAL, 1500).
-define(DEF_TIMEOUT,       1000).
-else.
-define(CURRENT_TIME,      leo_date:now()).
-define(DEF_SYNC_MIN_INTERVAL,  250).
-define(DEF_SYNC_MAX_INTERVAL, 1500).
-define(DEF_TIMEOUT,       3000).
-endif.

-define(DEF_NUM_OF_DIV, 32).

-record(routing_table, {index = []    :: list(pos_integer()),
                        checksum = -1 :: integer(),
                        table = []    :: list()
                       }).

-record(state, {cur  = #routing_table{} :: #routing_table{},
                prev = #routing_table{} :: #routing_table{},
                min_interval = ?DEF_SYNC_MIN_INTERVAL :: pos_integer(),
                max_interval = ?DEF_SYNC_MAX_INTERVAL :: pos_integer(),
                timestamp = 0 :: pos_integer()
               }).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
start_link() ->
    gen_server:start_link(?MODULE, [], []).
start_link([]) ->
    gen_server:start_link(?MODULE, [], []).

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
init([]) ->
    sync(),
    {ok, #state{timestamp = timestamp()}}.

handle_call(stop,_From,State) ->
    {stop, normal, ok, State};


handle_call(_Handle, _From, State) ->
    {reply, ok, State}.


%% Function: handle_cast(Msg, State) -> {noreply, State}          |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
handle_cast(sync, State) ->
    case catch maybe_sync(State) of
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
%% @private
timestamp() ->
    leo_math:floor(leo_date:clock() / 1000).

%% @doc Synchronize
%% @private
-spec(sync() ->
             ok | any()).
sync() ->
    Time = erlang:phash2(term_to_binary(leo_date:clock()),
                         (?DEF_SYNC_MAX_INTERVAL - ?DEF_SYNC_MIN_INTERVAL)
                        ) + ?DEF_SYNC_MIN_INTERVAL,
    catch timer:apply_after(Time, gen_server, cast, [self(), sync]).

%% @doc Heatbeat
%% @private
-spec(maybe_sync(#state{}) ->
             #state{}).
maybe_sync(#state{cur  = #routing_table{checksum = CurHash},
                  prev = #routing_table{checksum = PrevHash},
                  min_interval = MinInterval,
                  timestamp    = Timestamp} = State) ->

    {ok, {R1, R2}}= leo_redundant_manager_api:checksum(?CHECKSUM_RING),
    ThisTime = timestamp(),

    case ((ThisTime - Timestamp) < MinInterval) of
        true ->
            State;
        false ->
            NewState = case (R1 == -1 orelse R2 == -1) of
                           true ->
                               State;
                           false when R1 == CurHash andalso
                                      R2 == PrevHash ->
                               State;
                           false ->
                               maybe_sync_1(State, {R1, R2}, {CurHash, PrevHash})
                       end,
            sync(),
            NewState#state{timestamp = ThisTime}
    end.

%% @private
maybe_sync_1(State, {R1, R2}, {CurHash, PrevHash}) ->
    State1 = case (R1 == CurHash) of
                 true  -> State;
                 false ->
                     {ok, {CurHash1, CurIdx, CurTbl}} = gen_routing_table(?SYNC_MODE_CUR_RING),
                     State#state{cur = #routing_table{checksum = CurHash1,
                                                      index    = CurIdx,
                                                      table    = CurTbl}}
             end,
    State2 = case (R2 == PrevHash) of
                 true  -> State1;
                 false ->
                     {ok, {PrevHash1, PrevIdx, PrevTbl}} = gen_routing_table(?SYNC_MODE_PREV_RING),
                     State1#state{prev = #routing_table{checksum = PrevHash1,
                                                        index    = PrevIdx,
                                                        table    = PrevTbl}}
             end,
    State2.


%% @doc
%%
gen_routing_table(Version) ->
    %% Retrieve ring from local's master [etc|mnesia]
    {ok, CurRing} = leo_redundant_manager_api:get_ring(Version),
    Checksum  = erlang:crc32(term_to_binary(CurRing)),
    RingSize  = length(CurRing),
    GroupSize = leo_math:ceiling(RingSize / ?DEF_NUM_OF_DIV),

    %% table:{id, vnode-id, node, next-vnode-id}
    {_,_,_,Index,Table} =
        lists:foldl(
          fun({VId, Node}, {Id, GId, NextVId, IdxAcc, TblAcc}) when GId == GroupSize ->
                  {Id - 1, 0, VId, [VId|IdxAcc], [{Id, VId, Node, NextVId}|TblAcc]};
             ({VId, Node}, {Id, GId, NextVId, IdxAcc, TblAcc}) ->
                  {Id - 1, GId + 1, VId, IdxAcc, [{Id, VId, Node, NextVId}|TblAcc]}
          end, {RingSize, 0, '$end_of_table', [], []}, lists:reverse(CurRing)),
    {ok, {Checksum, Index, Table}}.

