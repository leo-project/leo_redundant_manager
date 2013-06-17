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
-export([lookup/3, first/2, last/2, force_sync/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
	       terminate/2,
         code_change/3]).

-undef(DEF_TIMEOUT).

-ifdef(TEST).
-define(CURRENT_TIME, 65432100000).
-define(DEF_SYNC_MIN_INTERVAL,  5).
-define(DEF_SYNC_MAX_INTERVAL, 10).
-define(DEF_TIMEOUT,         1000).
-else.
-define(CURRENT_TIME, leo_date:now()).
-define(DEF_SYNC_MIN_INTERVAL,  250).
-define(DEF_SYNC_MAX_INTERVAL, 1500).
-define(DEF_TIMEOUT,           3000).
-endif.

-define(DEF_NUM_OF_DIV, 32).

-record(addrid_nodes, {
          id = 0            :: integer(),
          addr_id_from = 0  :: integer(),
          addr_id_to = 0    :: integer(),
          nodes             :: list(atom())
         }).

-record(ring_group, {
          index_from        :: integer(),
          index_to          :: integer(),
          addrid_nodes_list :: list(#addrid_nodes{})
         }).

-record(ring_info, {
          checksum = -1     :: integer(),
          ring_group_list   :: list(#ring_group{})
         }).

-record(state, {
          cur  = #ring_info{} :: #ring_info{},
          prev = #ring_info{} :: #ring_info{},
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


handle_call({lookup, Tbl,_VNodeId},_From, State) when Tbl /= ?CUR_RING_TABLE,
                                                      Tbl /= ?PREV_RING_TABLE ->
    {reply, {error, invalid_table}, State};

handle_call({lookup, Tbl, VNodeId},_From, #state{cur  = Cur,
                                                 prev = Prev} = State) ->
    Fun = fun (_RingGroupList) ->
                  case _RingGroupList of
                      [] ->
                          not_found;
                      _ ->
                          find_redundancies_by_vnode_id(_RingGroupList, VNodeId)
                  end
          end,

    Reply = case Tbl of
                ?CUR_RING_TABLE ->
                    #ring_info{ring_group_list = RingGroupList} = Cur,
                    Fun(RingGroupList);
                ?PREV_RING_TABLE ->
                    #ring_info{ring_group_list = RingGroupList} = Prev,
                    Fun(RingGroupList)
            end,
    {reply, Reply, State};


handle_call({first, Tbl},_From, State) when Tbl /= ?CUR_RING_TABLE,
                                            Tbl /= ?PREV_RING_TABLE ->
    {reply, {error, invalid_table}, State};
handle_call({first, Tbl},_From, #state{cur  = Cur,
                                       prev = Prev} = State) ->
    Fun = fun (_RingGroupList) ->
                  case _RingGroupList of
                      [] ->
                          not_found;
                      [#ring_group{addrid_nodes_list = AddrId_Nodes}|_] ->
                          case AddrId_Nodes of
                              [] ->
                                  not_found;
                              [#addrid_nodes{nodes = Nodes}|_] ->
                                  {ok, Nodes}
                          end
                  end
          end,

    Reply = case Tbl of
                ?CUR_RING_TABLE ->
                    #ring_info{ring_group_list = RingGroupList} = Cur,
                    Fun(RingGroupList);
                ?PREV_RING_TABLE ->
                    #ring_info{ring_group_list = RingGroupList} = Prev,
                    Fun(RingGroupList)
            end,
    {reply, Reply, State};


handle_call({last, Tbl},_From, State) when Tbl /= ?CUR_RING_TABLE,
                                           Tbl /= ?PREV_RING_TABLE ->
    {reply, {error, invalid_table}, State};
handle_call({last, Tbl},_From, #state{cur  = Cur,
                                      prev = Prev} = State) ->
    Fun = fun (_RingGroupList) ->
                  case _RingGroupList of
                      [] ->
                          not_found;
                      _ ->
                          #ring_group{
                        addrid_nodes_list = AddrId_Nodes} = lists:last(_RingGroupList),
                          case AddrId_Nodes of
                              [] ->
                                  not_found;
                              _ ->
                                  #addrid_nodes{nodes = Nodes} = lists:last(AddrId_Nodes),
                                  {ok, Nodes}
                          end
                  end
          end,

    Reply = case Tbl of
                ?CUR_RING_TABLE ->
                    #ring_info{ring_group_list = RingGroupList} = Cur,
                    Fun(RingGroupList);
                ?PREV_RING_TABLE ->
                    #ring_info{ring_group_list = RingGroupList} = Prev,
                    Fun(RingGroupList)
            end,
    {reply, Reply, State};


handle_call({force_sync, Tbl},_From, State) when Tbl /= ?CUR_RING_TABLE,
                                                 Tbl /= ?PREV_RING_TABLE ->
    {reply, {error, invalid_table}, State};

handle_call({force_sync, Tbl},_From, State) ->
    Version = case Tbl of
                  ?CUR_RING_TABLE ->
                      ?SYNC_MODE_CUR_RING;
                  ?PREV_RING_TABLE ->
                      ?SYNC_MODE_PREV_RING
              end,
    NewState =
        case leo_redundant_manager_table_member:find_all() of
            {ok, Members} ->
                case leo_misc:get_env(?APP, ?PROP_OPTIONS) of
                    {ok, Options} ->
                        N  = leo_misc:get_value(?PROP_N,  Options),
                        L2 = leo_misc:get_value(?PROP_L2, Options, 0),

                        {ok, {Checksum, RingGroupList}} =
                            gen_routing_table(Version, N, L2, Members),
                        State#state{cur = #ring_info{checksum = Checksum,
                                                     ring_group_list = RingGroupList}};
                    _ ->
                        State
                end;
            _ ->
                State
        end,
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
maybe_sync(#state{cur  = #ring_info{checksum = CurHash},
                  prev = #ring_info{checksum = PrevHash},
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
    case leo_redundant_manager_table_member:find_all() of
        {ok, Members} ->
            case leo_misc:get_env(?APP, ?PROP_OPTIONS) of
                {ok, Options} ->
                    N  = leo_misc:get_value(?PROP_N,  Options),
                    L2 = leo_misc:get_value(?PROP_L2, Options, 0),

                    State1 = maybe_sync_1_1(?SYNC_MODE_CUR_RING,  R1, CurHash,  N, L2, Members, State),
                    State2 = maybe_sync_1_1(?SYNC_MODE_PREV_RING, R2, PrevHash, N, L2, Members, State1),
                    State2;
                _ ->
                    State
            end;
        _ ->
            State
    end.

maybe_sync_1_1(_TargetRing, OrgChecksum, CurChecksum,
               _NumOfReplicas,_NumOfAwarenessL2,_Members, State) when OrgChecksum == CurChecksum ->
    State;
maybe_sync_1_1(TargetRing,_OrgChecksum,_CurChecksum,
               NumOfReplicas, NumOfAwarenessL2, Members, State) ->
    {ok, {Checksum, RingGroupList}} =
        gen_routing_table(TargetRing, NumOfReplicas, NumOfAwarenessL2, Members),

    RingInfo = #ring_info{checksum = Checksum,
                          ring_group_list = RingGroupList},
    case TargetRing of
        ?SYNC_MODE_CUR_RING  -> State#state{cur =  RingInfo};
        ?SYNC_MODE_PREV_RING -> State#state{prev = RingInfo}
    end.


%% @doc Generate RING for this process
%% @private
gen_routing_table(Version, NumOfReplicas, NumOfAwarenessL2, Members) ->
    %% Retrieve ring from local's master [etc|mnesia]
    ETS_Tbl = case Version of
                  ?SYNC_MODE_CUR_RING  -> {ets, ?CUR_RING_TABLE};
                  ?SYNC_MODE_PREV_RING -> {ets, ?PREV_RING_TABLE};
                  _ ->
                      {ets, ?PREV_RING_TABLE}
              end,
    {ok, CurRing} = leo_redundant_manager_api:get_ring(Version),
    Checksum  = erlang:crc32(term_to_binary(CurRing)),
    RingSize  = length(CurRing),
    GroupSize = leo_math:ceiling(RingSize / ?DEF_NUM_OF_DIV),

    {_,_,Ring,_,_} =
        lists:foldl(fun({AddrId, _Node},
                        {Id, GId, IdxAcc, TblAcc, StAddrId}) ->
                            Ret = redundancies(ETS_Tbl, AddrId,
                                               NumOfReplicas, NumOfAwarenessL2, Members),
                            gen_routing_table_1(Ret, GroupSize,
                                                Id, AddrId, GId, IdxAcc, TblAcc, StAddrId)
                    end, {0, 0, [], [], 0}, CurRing),

    %% @TODO - debug (unnecessary-codes)
    %% lists:foreach(fun(#ring_group{index_from = From,
    %%                               index_to   = To,
    %%                               addrid_nodes_list = List}) ->
    %%                       ?debugVal({From, To}),
    %%                       lists:foreach(fun(AddrId_Nodes) ->
    %%                                             ?debugVal(AddrId_Nodes)
    %%                                     end, List)
    %%               end, lists:reverse(Ring)),
    {ok, {Checksum, lists:reverse(Ring)}}.


gen_routing_table_1({ok, #redundancies{nodes = Nodes}},
                    GroupSize, Id, AddrId, GId, IdxAcc, TblAcc, StAddrId) ->
    Id1 = Id + 1,
    case (GId == GroupSize) of
        true ->
            RingGroup = [#addrid_nodes{id = Id1,
                                       addr_id_to   = AddrId,
                                       addr_id_from = StAddrId,
                                       nodes = Nodes}|TblAcc],

            #addrid_nodes{id = FirstId,
                          addr_id_to = FirstAddrId} = lists:last(TblAcc),
            FirstAddrId_1 = case FirstId of
                                1 -> 0;
                                _ -> FirstAddrId
                            end,

            {Id1, 0, [#ring_group{index_from = FirstAddrId_1,
                             index_to   = AddrId,
                             addrid_nodes_list = lists:reverse(RingGroup)}|IdxAcc],
             [], AddrId + 1};
        false ->
            {Id1, GId + 1, IdxAcc,
             [#addrid_nodes{id = Id1,
                            addr_id_to   = AddrId,
                            addr_id_from = StAddrId,
                            nodes        = Nodes}|TblAcc], AddrId + 1}
    end;
gen_routing_table_1(_,_GroupSize, AddrId, Id, GId, IdxAcc, TblAcc,_StAddrId) ->
    error_logger:warning_msg("~p,~p,~p,~p~n",
                             [{module, ?MODULE_STRING}, {function, "gen_routing_table_1/8"},
                              {line, ?LINE}, {body, "Could not get redundancies"}]),
    {Id + 1, GId + 1, IdxAcc, [#addrid_nodes{}|TblAcc], AddrId + 1}.


%% @doc get redundancies by key.
%%x
-spec(redundancies(ring_table_info(), any(), pos_integer(), pos_integer(),list()) ->
             {ok, any()} | {error, any()}).
redundancies(_Table,_VNodeId, NumOfReplicas,_L2,_Members) when NumOfReplicas < ?DEF_MIN_REPLICAS;
                                                               NumOfReplicas > ?DEF_MAX_REPLICAS ->
    {error, out_of_renge};
redundancies(_Table,_VNodeId, NumOfReplicas, L2,_Members) when (NumOfReplicas - L2) < 1 ->
    {error, invalid_level2};
redundancies(Table, VNodeId0, NumOfReplicas, L2, Members) ->
    case leo_redundant_manager_table_ring:lookup(Table, VNodeId0) of
        {error, Cause} ->
            {error, Cause};
        [] ->
            case get_node_by_vnodeid(Table, VNodeId0) of
                {ok, VNodeId1} ->
                    redundnacies_1(Table, VNodeId0, VNodeId1,
                                   NumOfReplicas, L2, Members);
                {error, Cause} ->
                    {error, Cause}
            end;
        Value ->
            redundnacies_1(Table, VNodeId0, VNodeId0,
                           NumOfReplicas, L2, Members, Value)
    end.

%% @private
redundnacies_1(Table, VNodeId_Org, VNodeId_Hop, NumOfReplicas, L2, Members) ->
    case leo_redundant_manager_table_ring:lookup(Table, VNodeId_Hop) of
        {error, Cause} ->
            {error, Cause};
        [] ->
            case get_node_by_vnodeid(Table, VNodeId_Hop) of
                {ok, Value} ->
                    redundnacies_1(Table, VNodeId_Org, VNodeId_Hop,
                                   NumOfReplicas, L2, Members, Value);
                {error, Cause} ->
                    {error, Cause}
            end;
        Value ->
            redundnacies_1(Table, VNodeId_Org, VNodeId_Hop,
                           NumOfReplicas, L2, Members, Value)
    end.

redundnacies_1(Table, VNodeId_Org, VNodeId_Hop, NumOfReplicas, L2, Members, Value) ->
    {Node, SetsL2_1} = get_redundancies(Members, Value, []),

    redundancies_2(Table, NumOfReplicas-1, L2, Members, VNodeId_Hop,
                   #redundancies{id           = VNodeId_Org,
                                 vnode_id     = VNodeId_Hop,
                                 temp_nodes   = [Value],
                                 temp_level_2 = SetsL2_1,
                                 nodes        = [Node]}).

%% @private
redundancies_2(_Table,_,_L2,_Members,-1,_R) ->
    {error,  invalid_vnode};
redundancies_2(_Table, 0,_L2,_Members,_VNodeId, #redundancies{nodes = Acc} = R) ->
    {ok, R#redundancies{temp_nodes   = [],
                        temp_level_2 = [],
                        nodes        = lists:reverse(Acc)}};
redundancies_2( Table, NumOfReplicas, L2, Members, VNodeId0, R) ->
    case get_node_by_vnodeid(Table, VNodeId0) of
        {ok, VNodeId1} ->
            case leo_redundant_manager_table_ring:lookup(Table, VNodeId1) of
                {error, Cause} ->
                    {error, Cause};
                [] ->
                    case get_node_by_vnodeid(Table, VNodeId1) of
                        {ok, Node} ->
                            redundancies_3(Table, NumOfReplicas, L2, Members, VNodeId1, Node, R);
                        {error, Cause} ->
                            {error, Cause}
                    end;
                Node ->
                    redundancies_3(Table, NumOfReplicas, L2, Members, VNodeId1, Node, R)
            end;
        _ ->
            {error, out_of_range}
    end.

redundancies_3(Table, NumOfReplicas, L2, Members,
               VNodeId, Node1, #redundancies{temp_nodes   = AccTempNode,
                                             temp_level_2 = AccLevel2,
                                             nodes        = AccNodes} = R) ->
    case lists:member(Node1, AccTempNode) of
        true  ->
            redundancies_2(Table, NumOfReplicas, L2, Members, VNodeId, R);
        false ->
            case get_redundancies(Members, Node1, AccLevel2) of
                not_found ->
                    {error, node_not_found};
                {Node2, AccLevel2_1} ->
                    AccNodesSize  = length(AccNodes),
                    AccLevel2Size = length(AccLevel2_1),

                    case (L2 /= 0 andalso L2 == AccNodesSize) of
                        true when AccLevel2Size < (L2+1) ->
                            redundancies_2(Table, NumOfReplicas, L2, Members, VNodeId, R);
                        _ ->
                            redundancies_2(Table, NumOfReplicas-1, L2, Members, VNodeId,
                                           R#redundancies{temp_nodes   = [Node2|AccTempNode],
                                                          temp_level_2 = AccLevel2_1,
                                                          nodes        = [Node2|AccNodes]})
                    end
            end
    end.

%% @private
get_node_by_vnodeid(Table, VNodeId) ->
    case leo_redundant_manager_table_ring:next(Table, VNodeId) of
        '$end_of_table' ->
            case leo_redundant_manager_table_ring:first(Table) of
                '$end_of_table' ->
                    {error, no_entry};
                Value ->
                    {ok, Value}
            end;
        Value ->
            {ok, Value}
    end.


%% @doc Retrieve a member from an argument.
%% @private
get_redundancies([],_Node1,_) ->
    not_found;
get_redundancies([#member{node        = Node0,
                          grp_level_2 = L2}|_], Node1, SetL2) when Node0 == Node1  ->
    case lists:member(L2, SetL2) of
        false ->
            {Node0, [L2|SetL2]};
        _ ->
            {Node0, SetL2}
    end;
get_redundancies([#member{node = Node0}|T], Node1, SetL2) when Node0 /= Node1 ->
    get_redundancies(T, Node1, SetL2).


%% @doc Find redundanciess by vnodeid
%% @private
find_redundancies_by_vnode_id([],_VNodeId) ->
    not_found;
find_redundancies_by_vnode_id([#ring_group{index_from = From,
                                           index_to   = To,
                                           addrid_nodes_list = List}|_Rest], VNodeId) when From =< VNodeId,
                                                                                           To   >= VNodeId ->
    find_redundancies_by_vnode_id_1(List, VNodeId);
find_redundancies_by_vnode_id([_|Rest], VNodeId) ->
    find_redundancies_by_vnode_id(Rest, VNodeId).


find_redundancies_by_vnode_id_1([],_VNodeId) ->
    not_found;
find_redundancies_by_vnode_id_1([#addrid_nodes{addr_id_from = From,
                                               addr_id_to   = To,
                                               nodes = Nodes}|_Rest], VNodeId) when From =< VNodeId,
                                                                                    To   >=  VNodeId ->
    {ok, Nodes};
find_redundancies_by_vnode_id_1([_|Rest], VNodeId) ->
    find_redundancies_by_vnode_id_1(Rest, VNodeId).

