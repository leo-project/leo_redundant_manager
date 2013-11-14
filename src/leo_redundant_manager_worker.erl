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
-export([start_link/1, stop/1]).
-export([lookup/3, first/2, last/2, force_sync/2, redundancies/4]).

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
-define(DEF_SYNC_MIN_INTERVAL,  10).
-define(DEF_SYNC_MAX_INTERVAL,  50).
-define(DEF_TIMEOUT,          3000).
-else.
-define(CURRENT_TIME, leo_date:now()).
-define(DEF_SYNC_MIN_INTERVAL,  250).
-define(DEF_SYNC_MAX_INTERVAL, 1500).

-define(DEF_TIMEOUT,           3000).
-endif.

-define(DEF_NUM_OF_DIV, 32).

-record(state, {
          id :: atom(),
          cur  = #ring_info{} :: #ring_info{},
          prev = #ring_info{} :: #ring_info{},
          num_of_replicas = 0       :: pos_integer(),
          num_of_rack_awareness = 0 :: pos_integer(),
          min_interval = ?DEF_SYNC_MIN_INTERVAL :: pos_integer(),
          max_interval = ?DEF_SYNC_MAX_INTERVAL :: pos_integer(),
          timestamp = 0 :: pos_integer()
         }).

-record(ring_conf, {
          id = 0 :: pos_integer(),
          ring_size    = 0  :: pos_integer(),
          group_size   = 0  :: pos_integer(),
          group_id     = 0  :: pos_integer(),
          addr_id      = 0  :: pos_integer(),
          from_addr_id = 0  :: pos_integer(),
          index_list   = [] :: list(),
          table_list   = [] :: list(),
          checksum     = -1 :: integer()
         }).


-ifdef(TEST).
-define(output_error_log(Line, Fun, Msg), void).
-else.
-define(output_error_log(Line, Fun, Msg),
        error_logger:warning_msg("~p,~p,~p,~p~n",
                                 [{module, ?MODULE_STRING}, {function, Fun},
                                  {line, Line}, {body, Msg}])).
-endif.

-compile({inline, [lookup_fun/5, find_redundancies_by_addr_id/2,
                   reply_redundancies/3,first_fun/1, last_fun/1,
                   gen_routing_table/2, gen_routing_table_1/4, gen_routing_table_2/2,
                   redundancies/5, redundancies_1/6, redundancies_1_1/7,
                   redundancies_2/6, redundancies_3/7, get_node_by_vnodeid/2,
                   get_redundancies/3, force_sync_fun/2, force_sync_fun_1/3
                  ]}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
start_link(Id) ->
    gen_server:start_link({local, Id}, ?MODULE, [Id], []).

stop(Id) ->
    gen_server:call(Id, stop, ?DEF_TIMEOUT).


-spec(lookup(atom(), atom(), pos_integer()) ->
             {ok, #redundancies{}} | not_found).
lookup(ServerRef, Table, AddrId) ->
    gen_server:call(ServerRef, {lookup, Table, AddrId}, ?DEF_TIMEOUT).

-spec(first(atom(), atom()) ->
             {ok, #redundancies{}} | not_found).
first(ServerRef, Table) ->
    gen_server:call(ServerRef, {first, Table}, ?DEF_TIMEOUT).

-spec(last(atom(), atom()) ->
             {ok, #redundancies{}} | not_found).
last(ServerRef, Table) ->
    gen_server:call(ServerRef, {last, Table}, ?DEF_TIMEOUT).

-spec(force_sync(atom(), atom()) ->
             {ok, #redundancies{}} | not_found).
force_sync(ServerRef, Table) ->
    gen_server:call(ServerRef, {force_sync, Table}, ?DEF_TIMEOUT).

-spec(redundancies(atom(), atom(), number(), list(#member{})) ->
             {ok, #redundancies{}} | not_found).
redundancies(ServerRef, Table, AddrId, Members) ->
    gen_server:call(ServerRef, {redundancies, Table, AddrId, Members}, ?DEF_TIMEOUT).


%%--------------------------------------------------------------------
%% GEN_SERVER CALLBACKS
%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State}          |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
init([Id]) ->
    sync(),
    {ok, #state{id = Id,
                timestamp = timestamp()}}.

handle_call(stop,_From,State) ->
    {stop, normal, ok, State};


handle_call({lookup, Tbl,_AddrId},_From, State) when Tbl /= ?RING_TBL_CUR,
                                                     Tbl /= ?RING_TBL_PREV ->
    {reply, {error, invalid_table}, State};

handle_call({lookup, Tbl, AddrId},_From, State) ->
    #ring_info{ring_group_list = RingGroupList,
               first_vnode_id  = FirstVNodeId,
               last_vnode_id   = LastVNodeId} = ring_info(Tbl, State),
    Reply = lookup_fun(RingGroupList, FirstVNodeId, LastVNodeId, AddrId, State),
    {reply, Reply, State};


handle_call({first, Tbl},_From, State) when Tbl /= ?RING_TBL_CUR,
                                            Tbl /= ?RING_TBL_PREV ->
    {reply, {error, invalid_table}, State};
handle_call({first, Tbl},_From, State) ->
    #ring_info{ring_group_list = RingGroupList} = ring_info(Tbl, State),
    Reply = first_fun(RingGroupList),
    {reply, Reply, State};


handle_call({last, Tbl},_From, State) when Tbl /= ?RING_TBL_CUR,
                                           Tbl /= ?RING_TBL_PREV ->
    {reply, {error, invalid_table}, State};
handle_call({last, Tbl},_From, State) ->
    #ring_info{ring_group_list = RingGroupList} = ring_info(Tbl, State),
    Reply = last_fun(RingGroupList),
    {reply, Reply, State};


handle_call({force_sync, Tbl},_From, State) when Tbl /= ?RING_TBL_CUR,
                                                 Tbl /= ?RING_TBL_PREV ->
    {reply, {error, invalid_table}, State};

handle_call({force_sync, Tbl},_From, State) ->
    TargetRing = case Tbl of
                     ?RING_TBL_CUR  -> ?SYNC_TARGET_RING_CUR;
                     ?RING_TBL_PREV -> ?SYNC_TARGET_RING_PREV
                 end,
    NewState = force_sync_fun(TargetRing, State),
    {reply, ok, NewState};

handle_call({redundancies, Table, AddrId, Members},_From, #state{num_of_replicas = N,
                                                                 num_of_rack_awareness = L2} = State) ->
    Reply = redundancies(Table, AddrId, N, L2, Members),
    {reply, Reply, State};

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
-spec(timestamp() ->
             pos_integer()).
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


%% @doc Heatbeat and synchronize with master's ring-table
%% @private
-spec(maybe_sync(#state{}) ->
             #state{}).
maybe_sync(#state{cur  = #ring_info{checksum = CurHash},
                  prev = #ring_info{checksum = PrevHash},
                  min_interval = MinInterval,
                  timestamp    = Timestamp} = State) ->
    State_1 = case leo_misc:get_env(?APP, ?PROP_OPTIONS) of
                  {ok, Options} ->
                      NumOfReplica = leo_misc:get_value(?PROP_N,  Options),
                      NumOfL2      = leo_misc:get_value(?PROP_L2, Options, 0),
                      State#state{num_of_replicas = NumOfReplica,
                                  num_of_rack_awareness = NumOfL2};
                  _ ->
                      State
              end,
    ThisTime = timestamp(),
    sync(),

    case ((ThisTime - Timestamp) < MinInterval) of
        true ->
            State_1#state{timestamp = ThisTime};
        false ->
            {ok, {R1, R2}} = leo_redundant_manager_api:checksum(?CHECKSUM_RING),
            State_2 = case (R1 == -1 orelse R2 == -1) of
                          true ->
                              State_1;
                          false when R1 == CurHash  andalso
                                     R2 == PrevHash ->
                              State_1;
                          false ->
                              maybe_sync_1(State_1, {R1, R2}, {CurHash, PrevHash})
                      end,
            State_2#state{timestamp = ThisTime}
    end.

%% @doc Fix prev-ring or current-ring inconsistency
%% @private
-spec(maybe_sync_1(#state{}, {pos_integer(), pos_integer()}, {pos_integer(), pos_integer()}) ->
             #state{}).
maybe_sync_1(State, {R1, R2}, {CurHash, PrevHash}) ->
    case leo_redundant_manager_table_member:find_all(?MEMBER_TBL_CUR) of
        {ok, MembersCur} ->
            case leo_redundant_manager_table_member:find_all(?MEMBER_TBL_PREV) of
                {ok, MembersPrev} ->
                    CurSyncInfo  = #sync_info{target = ?SYNC_TARGET_RING_CUR,
                                              org_checksum = R1,
                                              cur_checksum = CurHash},
                    PrevSyncInfo = #sync_info{target = ?SYNC_TARGET_RING_PREV,
                                              org_checksum = R2,
                                              cur_checksum = case (R1 == CurHash) of
                                                                 true  -> PrevHash;
                                                                 false -> -1
                                                             end},
                    #state{cur  = CurRingInfo,
                           prev = PrevRingInfo} = State,
                    State_1 = State#state{cur  = CurRingInfo#ring_info{members = MembersCur},
                                          prev = PrevRingInfo#ring_info{members = MembersPrev}},
                    State_2 = maybe_sync_1_1(CurSyncInfo,  State_1),
                    State_3 = maybe_sync_1_1(PrevSyncInfo, State_2),
                    State_3;
                _ ->
                    State
            end;
        _ ->
            State
    end.

maybe_sync_1_1(#sync_info{org_checksum = OrgChecksum,
                          cur_checksum = CurChecksum}, State) when OrgChecksum == CurChecksum ->
    State;
maybe_sync_1_1(SyncInfo, State) ->
    TargetRing = SyncInfo#sync_info.target,
    case gen_routing_table(SyncInfo, State) of
        {ok, RingInfo} when TargetRing == ?SYNC_TARGET_RING_CUR ->
            State#state{cur  = RingInfo};
        {ok, RingInfo} when TargetRing == ?SYNC_TARGET_RING_PREV ->
            State#state{prev = RingInfo};
        _ ->
            State
    end.


%% @doc Generate RING for this process
%% @private
-spec(gen_routing_table(#sync_info{}, #state{}) ->
             {ok, #ring_info{}} | {error, atom()}).
gen_routing_table(#sync_info{target = TargetRing},_) when TargetRing /= ?SYNC_TARGET_RING_CUR,
                                                          TargetRing /= ?SYNC_TARGET_RING_PREV ->
    {error, invalid_target_ring};
gen_routing_table(SyncInfo, State) ->
    %% Retrieve ring from local's master [etc|mnesia]
    {ok, CurRing} = leo_redundant_manager_api:get_ring(?SYNC_TARGET_RING_CUR),
    Checksum   = erlang:crc32(term_to_binary(CurRing)),
    RingSize   = length(CurRing),
    GroupSize  = leo_math:ceiling(RingSize / ?DEF_NUM_OF_DIV),

    %% Retrieve redundancies by addr-id
    gen_routing_table_1(CurRing, SyncInfo, #ring_conf{id = 0,
                                                      group_id   = 0,
                                                      ring_size  = RingSize,
                                                      group_size = GroupSize,
                                                      index_list = [],
                                                      table_list = [],
                                                      from_addr_id = 0,
                                                      checksum     = Checksum}, State).

%% @private
-spec(gen_routing_table_1(list(), #sync_info{}, #ring_conf{}, #state{}) ->
             {ok, #ring_info{}} | {error, atom()}).
gen_routing_table_1([],_,#ring_conf{index_list = []},_) ->
    {error, ?ERROR_COULD_NOT_GET_REDUNDANCIES};
gen_routing_table_1([], #sync_info{target = TargetRing}, #ring_conf{index_list = IdxAcc,
                                                                    checksum   = Checksum}, State) ->
    IdxAcc_1 = lists:reverse(IdxAcc),
    Members = case TargetRing of
                  ?SYNC_TARGET_RING_CUR  -> (State#state.cur)#ring_info.members;
                  ?SYNC_TARGET_RING_PREV -> (State#state.prev)#ring_info.members
              end,

    {ok, #redundancies{vnode_id_to = FirstAddrId}} = first_fun(IdxAcc_1),
    {ok, #redundancies{vnode_id_to = LastAddrId}}  = last_fun(IdxAcc_1),
    {ok, #ring_info{checksum        = Checksum,
                    ring_group_list = IdxAcc_1,
                    first_vnode_id  = FirstAddrId,
                    last_vnode_id   = LastAddrId,
                    members         = Members}};

gen_routing_table_1([{AddrId,_Node}|Rest], SyncInfo, RingConf, State) ->
    TargetRing       = SyncInfo#sync_info.target,
    MembersCur       = (State#state.cur)#ring_info.members,
    NumOfReplicas    = State#state.num_of_replicas,
    NumOfAwarenessL2 = State#state.num_of_rack_awareness,

    case redundancies(?ring_table(?SYNC_TARGET_RING_CUR),
                      AddrId, NumOfReplicas, NumOfAwarenessL2, MembersCur) of
        {ok, #redundancies{nodes = CurNodes} = Redundancies} ->
            Redundancies_1 =
                case TargetRing of
                    ?SYNC_TARGET_RING_PREV ->
                        MembersPrev = (State#state.prev)#ring_info.members,
                        case redundancies(?ring_table(?SYNC_TARGET_RING_PREV),
                                          AddrId, NumOfReplicas, NumOfAwarenessL2, MembersPrev) of
                            {ok, #redundancies{nodes = PrevNodes}} ->
                                CurNodes_1  = [_N1 || #redundant_node{node = _N1} <- CurNodes ],
                                PrevNodes_1 = [_N2 || #redundant_node{node = _N2} <- PrevNodes],
                                case lists:subtract(CurNodes_1, PrevNodes_1) of
                                    [] ->
                                        Redundancies;
                                    _Difference ->
                                        Redundancies#redundancies{
                                          nodes = gen_routing_table_1_1(PrevNodes_1, CurNodes)}
                                end;
                            _ ->
                                Redundancies
                        end;
                    _ ->
                        Redundancies
                end,
            RingConf_1 = gen_routing_table_2(Redundancies_1,
                                             RingConf#ring_conf{addr_id = AddrId}),
            gen_routing_table_1(Rest, SyncInfo, RingConf_1, State);
        Error ->
            ?output_error_log(?LINE, "gen_routing_table_1/4",
                              ?ERROR_COULD_NOT_GET_REDUNDANCIES),
            Error
    end.

%% @private
gen_routing_table_1_1([], Acc) ->
    Acc;
gen_routing_table_1_1([N|Rest], Acc) ->
    Acc_1 = [_N || #redundant_node{node = _N} <- Acc],
    Acc_2 = case lists:member(N, Acc_1) of
                false -> Acc ++ [#redundant_node{node = N}];
                true  -> Acc
            end,
    gen_routing_table_1_1(Rest, Acc_2).

%% @private
-spec(gen_routing_table_2(#redundancies{}, #ring_conf{}) ->
             #ring_conf{}).
gen_routing_table_2(#redundancies{nodes = Nodes}, #ring_conf{id = Id,
                                                             ring_size    = RingSize,
                                                             group_size   = GroupSize,
                                                             group_id     = GrpId,
                                                             addr_id      = AddrId,
                                                             from_addr_id = FromAddrId,
                                                             index_list   = IdxAcc,
                                                             table_list   = TblAcc} = RingConf) ->
    Id1 = Id + 1,
    VNodeId_Nodes = #vnodeid_nodes{id = Id1,
                                   vnode_id_from = FromAddrId,
                                   vnode_id_to   = AddrId,
                                   nodes = Nodes},

    case (GrpId == GroupSize orelse (RingSize - Id1) < GroupSize) of
        true ->
            FirstAddrId_1 =
                case TblAcc of
                    [] ->
                        FromAddrId;
                    _ ->
                        case lists:last(TblAcc) of
                            #vnodeid_nodes{id = 1} ->
                                0;
                            #vnodeid_nodes{vnode_id_from = FirstAddrId} ->
                                FirstAddrId
                        end
                end,

            RingGroup = lists:reverse([VNodeId_Nodes|TblAcc]),
            RingConf#ring_conf{id = Id1,
                               group_id = 0,
                               from_addr_id = AddrId + 1,
                               index_list = [#ring_group{index_from = FirstAddrId_1,
                                                         index_to   = AddrId,
                                                         vnodeid_nodes_list = RingGroup}|IdxAcc],
                               table_list = []};
        false ->
            RingConf#ring_conf{id = Id1,
                               group_id = GrpId + 1,
                               from_addr_id = AddrId + 1,
                               table_list = [VNodeId_Nodes|TblAcc]}
    end.


%% @doc get redundancies by key.
%% @private
-spec(redundancies(ring_table_info(), any(), pos_integer(), pos_integer(), list()) ->
             {ok, any()} | {error, any()}).
redundancies(_,_,NumOfReplicas,_,_) when NumOfReplicas < ?DEF_MIN_REPLICAS;
                                         NumOfReplicas > ?DEF_MAX_REPLICAS ->
    {error, out_of_renge};
redundancies(_,_,NumOfReplicas, L2,_) when (NumOfReplicas - L2) < 1 ->
    {error, invalid_level2};
redundancies(Table, VNodeId_0, NumOfReplicas, L2, Members) ->
    case leo_redundant_manager_table_ring:lookup(Table, VNodeId_0) of
        {error, Cause} ->
            {error, Cause};
        [] ->
            case get_node_by_vnodeid(Table, VNodeId_0) of
                {ok, VNodeId_1} ->
                    redundancies_1(Table, VNodeId_0, VNodeId_1,
                                   NumOfReplicas, L2, Members);
                {error, Cause} ->
                    {error, Cause}
            end;
        Node ->
            redundancies_1_1(Table, VNodeId_0, VNodeId_0,
                             NumOfReplicas, L2, Members, Node)
    end.

%% @private
redundancies_1(Table, VNodeId_Org, VNodeId_Hop, NumOfReplicas, L2, Members) ->
    case leo_redundant_manager_table_ring:lookup(Table, VNodeId_Hop) of
        {error, Cause} ->
            {error, Cause};
        [] ->
            case get_node_by_vnodeid(Table, VNodeId_Hop) of
                {ok, Node} ->
                    redundancies_1_1(Table, VNodeId_Org, VNodeId_Hop,
                                     NumOfReplicas, L2, Members, Node);
                {error, Cause} ->
                    {error, Cause}
            end;
        Node ->
            redundancies_1_1(Table, VNodeId_Org, VNodeId_Hop,
                             NumOfReplicas, L2, Members, Node)
    end.

%% @private
redundancies_1_1(Table, VNodeId_Org, VNodeId_Hop, NumOfReplicas, L2, Members, Node) ->
    case get_redundancies(Members, Node, []) of
        not_found ->
            {error, ?ERROR_COULD_NOT_GET_REDUNDANCIES};
        {Node, SetsL2} ->
            redundancies_2(Table, NumOfReplicas-1, L2, Members, VNodeId_Hop,
                           #redundancies{id           = VNodeId_Org,
                                         vnode_id_to  = VNodeId_Hop,
                                         temp_nodes   = [Node],
                                         temp_level_2 = SetsL2,
                                         nodes        = [#redundant_node{node = Node}]})
    end.

%% @private
redundancies_2(_Table,_,_L2,_Members,-1,_R) ->
    {error,  invalid_vnode};
redundancies_2(_Table,0,_L2,_Members,_VNodeId, #redundancies{nodes = Acc} = R) ->
    {ok, R#redundancies{temp_nodes   = [],
                        temp_level_2 = [],
                        nodes        = lists:reverse(Acc)}};
redundancies_2(Table, NumOfReplicas, L2, Members, VNodeId_0, R) ->
    case get_node_by_vnodeid(Table, VNodeId_0) of
        {ok, VNodeId_1} ->
            case leo_redundant_manager_table_ring:lookup(Table, VNodeId_1) of
                {error, Cause} ->
                    {error, Cause};
                [] ->
                    case get_node_by_vnodeid(Table, VNodeId_1) of
                        {ok, Node} ->
                            redundancies_3(Table, NumOfReplicas, L2, Members, VNodeId_1, Node, R);
                        {error, Cause} ->
                            {error, Cause}
                    end;
                Node ->
                    redundancies_3(Table, NumOfReplicas, L2, Members, VNodeId_1, Node, R)
            end;
        _ ->
            {error, out_of_range}
    end.

redundancies_3(Table, NumOfReplicas, L2, Members, VNodeId, Node_1, R) ->
    AccTempNode = R#redundancies.temp_nodes,
    AccLevel2   = R#redundancies.temp_level_2,
    AccNodes    = R#redundancies.nodes,

    case lists:member(Node_1, AccTempNode) of
        true  ->
            redundancies_2(Table, NumOfReplicas, L2, Members, VNodeId, R);
        false ->
            case get_redundancies(Members, Node_1, AccLevel2) of
                not_found ->
                    {error, ?ERROR_COULD_NOT_GET_REDUNDANCIES};
                {Node_2, AccLevel2_1} ->
                    case (L2 /= 0 andalso L2 == length(AccNodes)) of
                        true when length(AccLevel2_1) < (L2 + 1) ->
                            redundancies_2(Table, NumOfReplicas, L2, Members, VNodeId, R);
                        _ ->
                            redundancies_2(Table, NumOfReplicas-1, L2, Members, VNodeId,
                                           R#redundancies{temp_nodes   = [Node_2|AccTempNode],
                                                          temp_level_2 = AccLevel2_1,
                                                          nodes        = [#redundant_node{node = Node_2}
                                                                          |AccNodes]})
                    end
            end
    end.


%% @doc Retrieve virtual-node by vnode-id
%% @private
-spec(get_node_by_vnodeid({ets|mnesia, ?RING_TBL_CUR|?RING_TBL_PREV}, pos_integer()) ->
             {ok, pos_integer()} | {error, no_entry}).
get_node_by_vnodeid(Table, VNodeId) ->
    case leo_redundant_manager_table_ring:next(Table, VNodeId) of
        '$end_of_table' ->
            case leo_redundant_manager_table_ring:first(Table) of
                '$end_of_table' ->
                    {error, no_entry};
                Node ->
                    {ok, Node}
            end;
        Node ->
            {ok, Node}
    end.


%% @doc Retrieve a member from an argument.
%% @private
-spec(get_redundancies(list(#member{}), atom(), list(atom())) ->
             {atom, list(atom())}).
get_redundancies([],_,_) ->
    not_found;
get_redundancies([#member{node = Node_0,
                          state = State,
                          grp_level_2 = L2}|_], Node_1, SetL2) when Node_0 == Node_1,
                                                                    State  /= ?STATE_DETACHED ->
    case lists:member(L2, SetL2) of
        false  -> {Node_0, [L2|SetL2]};
        _Other -> {Node_0, SetL2}
    end;
get_redundancies([#member{node = Node_0}|T], Node_1, SetL2) when Node_0 /= Node_1 ->
    get_redundancies(T, Node_1, SetL2).


%% @doc Reply redundancies
%% @private
-spec(reply_redundancies(not_found | {ok, #redundancies{}}, pos_integer(), #state{}) ->
             not_found | {ok, #redundancies{}}).
reply_redundancies(not_found,_,_) ->
    not_found;
reply_redundancies({ok, #redundancies{nodes = Nodes} = Redundancies},
                   AddrId, #state{num_of_replicas = NumOfReplicas}) ->
    reply_redundancies_1(Redundancies, AddrId, Nodes, NumOfReplicas, []).

%% @private
reply_redundancies_1(Redundancies, AddrId, [], _NumOfReplicas, Acc) ->
    {ok, Redundancies#redundancies{id = AddrId,
                                   nodes = lists:reverse(Acc)}};
reply_redundancies_1(Redundancies, AddrId, [#redundant_node{node = Node}|Rest], NumOfReplicas, Acc) ->
    CanReadRepair = (NumOfReplicas >= (length(Acc) + 1)),

    case leo_redundant_manager_table_member:lookup(Node) of
        {ok, #member{state = ?STATE_RUNNING}} ->
            reply_redundancies_1(Redundancies, AddrId, Rest, NumOfReplicas,
                                 [#redundant_node{node = Node,
                                                  available = true,
                                                  can_read_repair = CanReadRepair}|Acc]);
        {ok, #member{state = _State}} ->
            reply_redundancies_1(Redundancies, AddrId, Rest, NumOfReplicas,
                                 [#redundant_node{node = Node,
                                                  available = false,
                                                  can_read_repair = CanReadRepair}|Acc]);
        _ ->
            reply_redundancies_1(Redundancies, AddrId, Rest, NumOfReplicas, Acc)
    end.


%% @doc Retrieve first record
%% @private
-spec(first_fun(list(#ring_group{})) ->
             not_found | {ok, #redundancies{}}).
first_fun([]) ->
    error_logger:warning_msg("~p,~p,~p,~p~n",
                             [{module, ?MODULE_STRING}, {function, "first_fun/1"},
                              {line, ?LINE}, {body, "not_found"}]),
    not_found;
first_fun([#ring_group{vnodeid_nodes_list = AddrId_Nodes_List}|_]) ->
    case AddrId_Nodes_List of
        [] ->
            error_logger:warning_msg("~p,~p,~p,~p~n",
                                     [{module, ?MODULE_STRING}, {function, "first_fun/1"},
                                      {line, ?LINE}, {body, "not_found"}]),
            not_found;
        [#vnodeid_nodes{vnode_id_from = From,
                        vnode_id_to   = To,
                        nodes = Nodes}|_] ->
            {ok, #redundancies{id = From,
                               vnode_id_from = From,
                               vnode_id_to   = To,
                               nodes = Nodes}}
    end;
first_fun(_) ->
    error_logger:warning_msg("~p,~p,~p,~p~n",
                             [{module, ?MODULE_STRING}, {function, "first_fun/1"},
                              {line, ?LINE}, {body, "not_found"}]),
    not_found.


%% @doc Retrieve last record
%% @private
-spec(last_fun(list(#ring_group{})) ->
             not_found | {ok, #redundancies{}}).
last_fun([]) ->
    error_logger:warning_msg("~p,~p,~p,~p~n",
                             [{module, ?MODULE_STRING}, {function, "last_fun/1"},
                              {line, ?LINE}, {body, "not_found"}]),
    not_found;
last_fun(RingGroupList) ->
    #ring_group{vnodeid_nodes_list = AddrId_Nodes_List} = lists:last(RingGroupList),
    case AddrId_Nodes_List of
        [] ->
            error_logger:warning_msg("~p,~p,~p,~p~n",
                                     [{module, ?MODULE_STRING}, {function, "last_fun/1"},
                                      {line, ?LINE}, {body, "not_found"}]),
            not_found;
        _ ->
            #vnodeid_nodes{vnode_id_from = From,
                           vnode_id_to   = To,
                           nodes = Nodes} = lists:last(AddrId_Nodes_List),
            {ok, #redundancies{id = From,
                               vnode_id_from = From,
                               vnode_id_to   = To,
                               nodes = Nodes}}
    end.


%% @doc Retrieve redundancies by vnode-id
%% @private
-spec(lookup_fun(list(#ring_group{}), pos_integer(), pos_integer(), pos_integer(), #state{}) ->
             not_found | {ok, #redundancies{}}).
lookup_fun([],_,_,_,_) ->
    error_logger:warning_msg("~p,~p,~p,~p~n",
                             [{module, ?MODULE_STRING}, {function, "lookup_fun/4"},
                              {line, ?LINE}, {body, "not_found"}]),
    not_found;
lookup_fun(RingGroupList, FirstVNodeId,_LastVNodeId, AddrId, State) when FirstVNodeId >= AddrId ->
    Ret = first_fun(RingGroupList),
    reply_redundancies(Ret, AddrId, State);
lookup_fun(RingGroupList,_FirstVNodeId, LastVNodeId, AddrId, State) when LastVNodeId < AddrId ->
    Ret = first_fun(RingGroupList),
    reply_redundancies(Ret, AddrId, State);
lookup_fun(RingGroupList,_,_, AddrId, State) ->
    Ret = find_redundancies_by_addr_id(RingGroupList, AddrId),
    reply_redundancies(Ret, AddrId, State).


%% @doc Find redundanciess by vnodeid
%% @private
-spec(find_redundancies_by_addr_id(list(#ring_group{}), pos_integer()) ->
             not_found | {ok, #redundancies{}}).
find_redundancies_by_addr_id([],_AddrId) ->
    error_logger:warning_msg("~p,~p,~p,~p~n",
                             [{module, ?MODULE_STRING},
                              {function, "find_redundancies_by_addr_id/2"},
                              {line, ?LINE}, {body, "not_found"}]),
    not_found;
find_redundancies_by_addr_id(
  [#ring_group{index_from = From,
               index_to   = To,
               vnodeid_nodes_list = List}|_Rest], AddrId) when From =< AddrId,
                                                               To   >= AddrId ->
    find_redundancies_by_addr_id_1(List, AddrId);
find_redundancies_by_addr_id([_|Rest], AddrId) ->
    find_redundancies_by_addr_id(Rest, AddrId).


find_redundancies_by_addr_id_1([],_AddrId) ->
    error_logger:warning_msg("~p,~p,~p,~p~n",
                             [{module, ?MODULE_STRING},
                              {function, "find_redundancies_by_addr_id_1/2"},
                              {line, ?LINE}, {body, "not_found"}]),
    not_found;
find_redundancies_by_addr_id_1(
  [#vnodeid_nodes{vnode_id_from = From,
                  vnode_id_to   = To,
                  nodes = Nodes}|_], AddrId) when From =< AddrId,
                                                  To   >= AddrId ->
    {ok, #redundancies{vnode_id_from = From,
                       vnode_id_to   = To,
                       nodes = Nodes}};
find_redundancies_by_addr_id_1([_|Rest], AddrId) ->
    find_redundancies_by_addr_id_1(Rest, AddrId).


%% @doc Force sync
%% @private
-spec(force_sync_fun(?SYNC_TARGET_RING_CUR|?SYNC_TARGET_RING_PREV, #state{}) ->
             #state{}).
force_sync_fun(TargetRing, State) ->
    case leo_redundant_manager_table_member:find_all(?MEMBER_TBL_CUR) of
        {ok, MembersCur} ->
            case leo_redundant_manager_table_member:find_all(?MEMBER_TBL_PREV) of
                {ok, MembersPrev} ->
                    State_1 = State#state{cur  = #ring_info{members = MembersCur},
                                          prev = #ring_info{members = MembersPrev}},
                    Ret = gen_routing_table(#sync_info{target = TargetRing}, State_1),
                    force_sync_fun_1(Ret, TargetRing, State);
                _ ->
                    State
            end;
        _ ->
            State
    end.

%% @private
force_sync_fun_1({ok, RingInfo}, ?SYNC_TARGET_RING_CUR, State) ->
    State#state{cur  = RingInfo};
force_sync_fun_1({ok, RingInfo}, ?SYNC_TARGET_RING_PREV, State) ->
    State#state{prev = RingInfo};
force_sync_fun_1(_,_,State) ->
    State.


%% @doc Retrieve ring-info
%% @private
-spec(ring_info(?RING_TBL_CUR|?RING_TBL_PREV, #state{}) ->
             #ring_info{}).
ring_info(?RING_TBL_CUR, State) ->
    State#state.cur;
ring_info(?RING_TBL_PREV, State) ->
    State#state.prev;
ring_info(_,_) ->
    {error, invalid_table}.
