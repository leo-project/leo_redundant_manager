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
-define(DEF_SYNC_MIN_INTERVAL,    1).
-define(DEF_SYNC_MAX_INTERVAL,   10).
-define(DEF_TIMEOUT,           1000).
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
          min_interval = ?DEF_SYNC_MIN_INTERVAL :: pos_integer(),
          max_interval = ?DEF_SYNC_MAX_INTERVAL :: pos_integer(),
          timestamp = 0 :: pos_integer()
         }).

-record(sync_info, {
          target       :: ?VER_CURRENT | ?VER_PREV,
          members = [] :: list(#member{}),
          org_checksum = 0 :: pos_integer(),
          cur_checksum = 0 :: pos_integer(),
          num_of_replicas = 0       :: pos_integer(),
          num_of_rack_awareness = 0 :: pos_integer()
         }).

-record(ring_conf, {
          id = 0 :: pos_integer(),
          ring_size    = 0  :: pos_integer(),
          group_size   = 0  :: pos_integer(),
          group_id     = 0  :: pos_integer(),
          addr_id      = 0  :: pos_integer(),
          from_addr_id = [] :: string(),
          index_list   = [] :: list(),
          table_list   = [] :: list()
         }).


-ifdef(TEST).
-define(output_error_log(Line, Fun, Msg), void).
-else.
-define(output_error_log(Line, Fun, Msg),
        error_logger:warning_msg("~p,~p,~p,~p~n",
                                 [{module, ?MODULE_STRING}, {function, Fun},
                                  {line, Line}, {body, Msg}])).
-endif.

-compile({inline, [lookup_fun/4, find_redundancies_by_addr_id/2,
                   reply_redundancies/2,first_fun/1, last_fun/1,
                   gen_routing_table/4, gen_routing_table_1/2,
                   redundancies/5, redundnacies_1/6, redundnacies_1/7,
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
    Reply = lookup_fun(RingGroupList, FirstVNodeId, LastVNodeId, AddrId),
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
                     ?RING_TBL_CUR  -> ?SYNC_MODE_CUR_RING;
                     ?RING_TBL_PREV -> ?SYNC_MODE_PREV_RING
                 end,
    NewState = force_sync_fun(TargetRing, State),
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
    {ok, {R1, R2}} = leo_redundant_manager_api:checksum(?CHECKSUM_RING),
    ThisTime = timestamp(),
    sync(),

    case ((ThisTime - Timestamp) < MinInterval) of
        true ->
            State#state{timestamp = ThisTime};
        false ->
            NewState = case (R1 == -1 orelse
                             R2 == -1) of
                           true ->
                               State;
                           false when R1 == CurHash  andalso
                                      R2 == PrevHash ->
                               State;
                           false ->
                               maybe_sync_1(State, {R1, R2}, {CurHash, PrevHash})
                       end,
            NewState#state{timestamp = ThisTime}
    end.

%% @doc Fix prev-ring or current-ring inconsistency
%% @private
-spec(maybe_sync_1(#state{}, {pos_integer(), pos_integer()}, {pos_integer(), pos_integer()}) ->
             #state{}).
maybe_sync_1(State, {R1, R2}, {CurHash, PrevHash}) ->
    case leo_redundant_manager_table_member:find_all() of
        {ok, Members} ->
            case leo_misc:get_env(?APP, ?PROP_OPTIONS) of
                {ok, Options} ->
                    N  = leo_misc:get_value(?PROP_N,  Options),
                    L2 = leo_misc:get_value(?PROP_L2, Options, 0),

                    CurSyncInfo  = #sync_info{target = ?SYNC_MODE_CUR_RING,
                                              org_checksum = R1,
                                              cur_checksum = CurHash,
                                              num_of_replicas = N,
                                              num_of_rack_awareness = L2,
                                              members = Members},
                    PrevSyncInfo = #sync_info{target = ?SYNC_MODE_PREV_RING,
                                              org_checksum = R2,
                                              cur_checksum = PrevHash,
                                              num_of_replicas = N,
                                              num_of_rack_awareness = L2,
                                              members = Members},
                    State1 = maybe_sync_1_1(CurSyncInfo,  State),
                    State2 = maybe_sync_1_1(PrevSyncInfo, State1),
                    State2;
                _ ->
                    State
            end;
        _ ->
            State
    end.

maybe_sync_1_1(#sync_info{org_checksum = OrgChecksum,
                          cur_checksum = CurChecksum}, State) when OrgChecksum == CurChecksum ->
    State;
maybe_sync_1_1(#sync_info{target = TargetRing,
                          num_of_replicas = NumOfReplicas,
                          num_of_rack_awareness = NumOfAwarenessL2,
                          members = Members}, State) ->
    case gen_routing_table(TargetRing, NumOfReplicas, NumOfAwarenessL2, Members) of
        {ok, {Checksum, RingGroupList, FirstAddrId, LastAddrId}} ->
            RingInfo = #ring_info{checksum = Checksum,
                                  ring_group_list = RingGroupList,
                                  first_vnode_id  = FirstAddrId,
                                  last_vnode_id   = LastAddrId
                                 },
            case TargetRing of
                ?SYNC_MODE_CUR_RING  -> State#state{cur  = RingInfo};
                ?SYNC_MODE_PREV_RING -> State#state{prev = RingInfo}
            end;
        _ ->
            State
    end.


%% @doc Generate RING for this process
%% @private
-spec(gen_routing_table(?SYNC_MODE_CUR_RING|?SYNC_MODE_PREV_RING,
                        pos_integer(), pos_integer(), list(#member{})) ->
             {ok, {pos_integer(), list(#ring_group{}),
                   pos_integer(), pos_integer()}} | {error, atom()}).
gen_routing_table(TargetRing,_NumOfReplicas,
                  _NumOfAwarenessL2,_Members) when TargetRing /= ?SYNC_MODE_CUR_RING,
                                                   TargetRing /= ?SYNC_MODE_PREV_RING ->
    {error, invalid_target_ring};
gen_routing_table(_TargetRing,_NumOfReplicas,_NumOfAwarenessL2,[]) ->
    {error, member_empty};
gen_routing_table(TargetRing, NumOfReplicas, NumOfAwarenessL2, Members) ->
    %% Retrieve ring from local's master [etc|mnesia]
    Tbl = case TargetRing of
              ?SYNC_MODE_CUR_RING  -> leo_redundant_manager_api:table_info(?VER_CURRENT);
              ?SYNC_MODE_PREV_RING -> leo_redundant_manager_api:table_info(?VER_PREV)
          end,

    {ok, CurRing} = leo_redundant_manager_api:get_ring(TargetRing),
    Checksum  = erlang:crc32(term_to_binary(CurRing)),
    RingSize  = length(CurRing),
    GroupSize = leo_math:ceiling(RingSize / ?DEF_NUM_OF_DIV),

    %% Retrieve redundancies by addr-id
    {_,_,RingGroup1,_,_} =
        lists:foldl(fun({AddrId, _Node},
                        {Id, GId, IdxAcc, TblAcc, FromAddrId}) ->
                            Ret = redundancies(Tbl, AddrId,
                                               NumOfReplicas, NumOfAwarenessL2, Members),
                            RingConf = #ring_conf{id = Id,
                                                  ring_size    = RingSize,
                                                  group_size   = GroupSize,
                                                  group_id     = GId,
                                                  addr_id      = AddrId,
                                                  from_addr_id = FromAddrId,
                                                  index_list   = IdxAcc,
                                                  table_list   = TblAcc},
                            #ring_conf{id = Id_1,
                                       group_id = GId_1,
                                       from_addr_id = FromAddrId_1,
                                       index_list = IdxAcc_1,
                                       table_list = TblAcc_1
                                      } = gen_routing_table_1(Ret, RingConf),
                            {Id_1, GId_1, IdxAcc_1, TblAcc_1, FromAddrId_1}
                    end, {0, 0, [], [], 0}, CurRing),
    case RingGroup1 of
        [] ->
            {error, empty};
        _ ->
            RingGroup2 = lists:reverse(RingGroup1),
            {ok, #redundancies{vnode_id_to = FirstAddrId}} = first_fun(RingGroup2),
            {ok, #redundancies{vnode_id_to = LastAddrId}}  = last_fun(RingGroup2),
            {ok, {Checksum, RingGroup2, FirstAddrId, LastAddrId}}
    end.

gen_routing_table_1(
  {ok, #redundancies{nodes = Nodes}}, #ring_conf{id = Id,
                                                 ring_size    = RingSize,
                                                 group_size   = GroupSize,
                                                 group_id     = GId,
                                                 addr_id      = AddrId,
                                                 from_addr_id = FromAddrId,
                                                 index_list   = IdxAcc,
                                                 table_list   = TblAcc} = RingConf) ->
    Id1 = Id + 1,
    VNodeId_Nodes = #vnodeid_nodes{id = Id1,
                                   vnode_id_from = FromAddrId,
                                   vnode_id_to   = AddrId,
                                   nodes = Nodes},

    case (GId == GroupSize orelse (RingSize - Id1) < GroupSize) of
        true ->
            FirstAddrId_1 =
                case TblAcc of
                    [] ->
                        FromAddrId;
                    _ ->
                        case lists:last(TblAcc) of
                            #vnodeid_nodes{id = 1} -> 0;
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
                               group_id = GId + 1,
                               from_addr_id = AddrId + 1,
                               table_list = [VNodeId_Nodes|TblAcc]}
    end;

gen_routing_table_1(_, #ring_conf{id = Id,
                                  group_id     = GId,
                                  addr_id      = AddrId,
                                  table_list   = TblAcc} = RingConf) ->
    ?output_error_log(?LINE, "gen_routing_table_1/2", "Could not get redundancies"),
    RingConf#ring_conf{id       = Id + 1,
                       group_id = GId + 1,
                       from_addr_id = AddrId + 1,
                       table_list   = [#vnodeid_nodes{}|TblAcc]}.


%% @doc get redundancies by key.
%% @private
-spec(redundancies(ring_table_info(), any(), pos_integer(), pos_integer(), list()) ->
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
                                 vnode_id_to  = VNodeId_Hop,
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
                Value ->
                    {ok, Value}
            end;
        Value ->
            {ok, Value}
    end.


%% @doc Retrieve a member from an argument.
%% @private
-spec(get_redundancies(list(#member{}), atom(), list(atom())) ->
             {atom, list(atom())}).
get_redundancies([],_Node1,_) ->
    not_found;
get_redundancies([#member{node = Node0,
                          grp_level_2 = L2}|_], Node1, SetL2) when Node0 == Node1  ->
    case lists:member(L2, SetL2) of
        false ->
            {Node0, [L2|SetL2]};
        _ ->
            {Node0, SetL2}
    end;
get_redundancies([#member{node = Node0}|T], Node1, SetL2) when Node0 /= Node1 ->
    get_redundancies(T, Node1, SetL2).


%% @doc Reply redundancies
%% @private
-spec(reply_redundancies(not_found | {ok, #redundancies{}}, pos_integer()) ->
             not_found | {ok, #redundancies{}}).
reply_redundancies(not_found,_) ->
    not_found;
reply_redundancies({ok, #redundancies{nodes = Nodes} = Redundancies}, AddrId) ->
    reply_redundancies_1(Redundancies, AddrId, Nodes, []).

reply_redundancies_1(Redundancies, AddrId, [], Acc) ->
    {ok, Redundancies#redundancies{id = AddrId,
                                   nodes = lists:reverse(Acc)}};

reply_redundancies_1(Redundancies, AddrId, [Node|Rest], Acc) ->
    case leo_redundant_manager_table_member:lookup(Node) of
        {ok, #member{state = ?STATE_RUNNING}} ->
            reply_redundancies_1(Redundancies, AddrId, Rest, [{Node, true}|Acc]);
        {ok, #member{state = _State}} ->
            reply_redundancies_1(Redundancies, AddrId, Rest, [{Node, false}|Acc]);
        _ ->
            reply_redundancies_1(Redundancies, AddrId, Rest, Acc)
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
-spec(lookup_fun(list(#ring_group{}), pos_integer(), pos_integer(), pos_integer()) ->
             not_found | {ok, #redundancies{}}).
lookup_fun([],_FirstVNodeId,_LastVNodeId,_AddrId) ->
    error_logger:warning_msg("~p,~p,~p,~p~n",
                             [{module, ?MODULE_STRING}, {function, "lookup_fun/4"},
                              {line, ?LINE}, {body, "not_found"}]),
    not_found;
lookup_fun(RingGroupList, FirstVNodeId,_LastVNodeId, AddrId) when FirstVNodeId >= AddrId ->
    Ret = first_fun(RingGroupList),
    reply_redundancies(Ret, AddrId);
lookup_fun(RingGroupList,_FirstVNodeId, LastVNodeId, AddrId) when LastVNodeId < AddrId ->
    Ret = first_fun(RingGroupList),
    reply_redundancies(Ret, AddrId);
lookup_fun(RingGroupList,_FirstVNodeId,_LastVNodeId, AddrId) ->
    Ret = find_redundancies_by_addr_id(RingGroupList, AddrId),
    reply_redundancies(Ret, AddrId).


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
-spec(force_sync_fun(?SYNC_MODE_CUR_RING|?SYNC_MODE_PREV_RING, #state{}) ->
             #state{}).
force_sync_fun(TargetRing, State) ->
    case leo_redundant_manager_table_member:find_all() of
        {ok, Members} ->
            case leo_misc:get_env(?APP, ?PROP_OPTIONS) of
                {ok, Options} ->
                    N  = leo_misc:get_value(?PROP_N,  Options),
                    L2 = leo_misc:get_value(?PROP_L2, Options, 0),

                    Ret = gen_routing_table(TargetRing, N, L2, Members),
                    force_sync_fun_1(Ret, TargetRing, State);
                _ ->
                    State
            end;
        _ ->
            State
    end.

force_sync_fun_1({ok, {Checksum, RingGroupList, FirstAddrId, LastAddrId}}, ?SYNC_MODE_CUR_RING, State) ->
    State#state{cur  = #ring_info{checksum = Checksum,
                                  ring_group_list = RingGroupList,
                                  first_vnode_id  = FirstAddrId,
                                  last_vnode_id   = LastAddrId}};
force_sync_fun_1({ok, {Checksum, RingGroupList, FirstAddrId, LastAddrId}}, ?SYNC_MODE_PREV_RING, State) ->
    State#state{prev = #ring_info{checksum = Checksum,
                                  ring_group_list = RingGroupList,
                                  first_vnode_id  = FirstAddrId,
                                  last_vnode_id   = LastAddrId}};
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
