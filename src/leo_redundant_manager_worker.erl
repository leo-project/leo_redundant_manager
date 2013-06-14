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
-export([lookup/3, first/2, prev/3, next/3, last/2, force_sync/2]).

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
-define(DEF_SYNC_MIN_INTERVAL,  5).
-define(DEF_SYNC_MAX_INTERVAL, 10).
-define(DEF_TIMEOUT,         1000).
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


lookup(ServerRef, Table, VNodeId) ->
    gen_server:call(ServerRef, {lookup, Table, VNodeId}, ?DEF_TIMEOUT).

first(ServerRef, Table) ->
    gen_server:call(ServerRef, {first, Table}, ?DEF_TIMEOUT).

prev(ServerRef, Table, VNodeId) ->
    gen_server:call(ServerRef, {prev, Table, VNodeId}, ?DEF_TIMEOUT).

next(ServerRef, Table, VNodeId) ->
    gen_server:call(ServerRef, {next, Table, VNodeId}, ?DEF_TIMEOUT).

last(ServerRef, Table) ->
    gen_server:call(ServerRef, {last, Table}, ?DEF_TIMEOUT).

force_sync(ServerRef, Table) ->
    gen_server:call(ServerRef, {force_sync, Table}, ?DEF_TIMEOUT).


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


handle_call({lookup, {_, Tbl}, VNodeId},_From, State) ->
    Reply = case get_ring(Tbl, State) of
                [] ->
                    [];
                Ring ->
                    case get_index(Tbl, State) of
                        [] ->
                            [];
                        Index ->
                            RowId = find_index(VNodeId, Index),
                            get_vnode_id(RowId, VNodeId, Ring)
                    end
            end,
    {reply, Reply, State};


handle_call({first, {_, Tbl}},_From, State) ->
    Reply = case get_ring(Tbl, State) of
                [] ->
                    '$end_of_table';
                [{_,VNodeId,_Node,_}|_]  ->
                    VNodeId
            end,
    {reply, Reply, State};


handle_call({prev, {_, Tbl}, VNodeId},_From, State) ->
    Reply = case get_ring(Tbl, State) of
                [] ->
                    '$end_of_table';
                Ring ->
                    case get_index(Tbl, State) of
                        [] ->
                            '$end_of_table';
                        Index ->
                            RowId = find_index(VNodeId, Index),
                            prev_vnode_id(RowId, VNodeId, Ring)
                    end
            end,
    {reply, Reply, State};


handle_call({next, {_, Tbl}, VNodeId},_From, State) ->
    Reply = case get_ring(Tbl, State) of
                [] ->
                    '$end_of_table';
                Ring ->
                    case get_index(Tbl, State) of
                        [] ->
                            '$end_of_table';
                        Index ->
                            RowId = find_index(VNodeId, Index),
                            next_vnode_id(RowId, VNodeId, Ring)
                    end
            end,
    {reply, Reply, State};


handle_call({last, {_, Tbl}},_From, State) ->
    Reply = case get_ring(Tbl, State) of
                [] ->
                    '$end_of_table';
                Ring ->
                    {_,VNodeId,_Node,_} = lists:last(Ring),
                    VNodeId
            end,
    {reply, Reply, State};


handle_call({force_sync, {_, ?CUR_RING_TABLE}},_From, State) ->
    {ok, {Checksum, Index, Table}} =
        gen_routing_table(?SYNC_MODE_CUR_RING),
    NewState = State#state{cur = #routing_table{checksum = Checksum,
                                                index    = Index,
                                                table    = Table}},
    {reply, ok, NewState};
handle_call({force_sync, {_, ?PREV_RING_TABLE}},_From, State) ->
    {ok, {Checksum, Index, Table}} =
        gen_routing_table(?SYNC_MODE_PREV_RING),
    NewState = State#state{prev = #routing_table{checksum = Checksum,
                                                 index    = Index,
                                                 table    = Table}},    
    {reply, ok, NewState};


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
%% @doc Retrieve current time
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


%% @doc Generate RING for this process
%% @private
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
                  {Id - 1, 0, VId, [{Id, VId}|IdxAcc], [{Id, VId, Node, NextVId}|TblAcc]};
             ({VId, Node}, {Id, GId, NextVId, IdxAcc, TblAcc}) ->
                  {Id - 1, GId + 1, VId, IdxAcc, [{Id, VId, Node, NextVId}|TblAcc]}
          end, {RingSize, 0, '$end_of_table', [], []}, lists:reverse(CurRing)),
    {ok, {Checksum, lists:reverse(Index), Table}}.


%% @doc Retrieve a ring's list
%% @private
get_ring(Tbl, State) ->
    case Tbl of
        ?CUR_RING_TABLE  -> (State#state.cur )#routing_table.table;
        ?PREV_RING_TABLE -> (State#state.prev)#routing_table.table;
        _ ->
            []
    end.


%% @doc Retrieve index's list
%%      - [{rowid, addr-id}|...]
%% @private
get_index(Tbl, State) ->
    case Tbl of
        ?CUR_RING_TABLE  -> (State#state.cur )#routing_table.index;
        ?PREV_RING_TABLE -> (State#state.prev)#routing_table.index;
        _ ->
            []
    end.


%% @doc Retrieve vnode-id from a ring's list
%% @private
get_vnode_id(RowId, VNodeId, Ring) when RowId =< length(Ring) ->
    {_, Rest} = lists:split(RowId, Ring),
    get_vnode_id_1(VNodeId, Rest);
get_vnode_id(_,_,_) ->
    [].

get_vnode_id_1(_VNodeId, []) ->
    [];
get_vnode_id_1(VNodeId, [{_, AddrId,_VNode, _}|_Rest] ) when VNodeId < AddrId ->
    [];
get_vnode_id_1(VNodeId, [{_, AddrId, VNode, _}|_Rest]) when VNodeId == AddrId ->
    VNode;
get_vnode_id_1(VNodeId, [{_, AddrId,_VNode, _}|Rest]) when VNodeId > AddrId ->
    get_vnode_id_1(VNodeId, Rest).


%% @doc Find index by vnode-id
%% @private
find_index(_VNodeId,[]) ->
    0;
find_index(VNodeId, [{RowId, Index}|_]) when VNodeId >= Index ->
    RowId - 1;
find_index(VNodeId, [_|Rest]) ->
    find_index(VNodeId, Rest).


%% @doc Find vnode-id by rowid and addrId
%% @private
next_vnode_id(RowId, VNodeId, Ring) when RowId =< length(Ring) ->
    {_, Rest} = lists:split(RowId, Ring),
    next_vnode_id_1(VNodeId, Rest);
next_vnode_id(_,_,_) ->
    '$end_of_table'.

next_vnode_id_1(_VNodeId, []) ->
    '$end_of_table';
next_vnode_id_1(VNodeId, [{_, AddrId,_Node, _}|_]) when VNodeId < AddrId ->
    AddrId;
next_vnode_id_1(VNodeId, [_|Rest]) ->
    next_vnode_id_1(VNodeId, Rest).


%% @doc Find vnode-id by rowid and addrId
%% @private
prev_vnode_id(RowId, VNodeId, Ring) when RowId =< length(Ring) ->
    {Head, _Rest} = lists:split(RowId + 1, Ring),
    prev_vnode_id_1(VNodeId, lists:reverse(Head));
prev_vnode_id(_,_,_) ->
    '$end_of_table'.

prev_vnode_id_1(_VNodeId, []) ->
    '$end_of_table';
prev_vnode_id_1(VNodeId, [{_, AddrId,_Node, _}|_]) when VNodeId > AddrId ->
    AddrId;
prev_vnode_id_1(VNodeId, [_|Rest]) ->
    prev_vnode_id_1(VNodeId, Rest).

