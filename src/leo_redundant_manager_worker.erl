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
%% @doc The redundant manager's worker process
%% @reference https://github.com/leo-project/leo_redundant_manager/blob/master/src/leo_redundant_manager_worker.erl
%% @end
%%======================================================================
-module(leo_redundant_manager_worker).

-author('Yosuke Hara').

-behaviour(gen_server).

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/0, stop/0]).
-export([lookup/2, first/1, last/1, force_sync/1,
         redundancies/3,
         collect/4,
         checksum/0,
         dump/0, subscribe/0]).

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
-define(DEF_SYNC_MIN_INTERVAL, 10).
-define(DEF_SYNC_MAX_INTERVAL, 50).
-define(DEF_TIMEOUT, timer:seconds(3)).
-define(DEF_TIMEOUT_LONG, timer:seconds(3)).
-else.
-define(CURRENT_TIME, leo_date:now()).
-define(DEF_SYNC_MIN_INTERVAL, 250).
-define(DEF_SYNC_MAX_INTERVAL, 1500).
-define(DEF_TIMEOUT, timer:seconds(30)).
-define(DEF_TIMEOUT_LONG, timer:seconds(120)).
-endif.

-define(DEF_NUM_OF_DIV, 32).

-record(state, {
          id :: atom(),
          cur  = #ring_info{} :: #ring_info{},
          prev = #ring_info{} :: #ring_info{},
          num_of_replicas = 0 :: non_neg_integer(),
          num_of_rack_awareness = 0 :: non_neg_integer(),
          min_interval = ?DEF_SYNC_MIN_INTERVAL :: non_neg_integer(),
          max_interval = ?DEF_SYNC_MAX_INTERVAL :: non_neg_integer(),
          timestamp = 0 :: non_neg_integer(),
          checksum = {-1,-1} :: {integer(), integer()}
         }).

-record(ring_conf, {
          id = 0 :: non_neg_integer(),
          ring_size = 0  :: non_neg_integer(),
          group_size = 0  :: non_neg_integer(),
          group_id = 0  :: non_neg_integer(),
          addr_id = 0  :: non_neg_integer(),
          from_addr_id = 0  :: non_neg_integer(),
          index_list = [] :: list(),
          table_list = [] :: list(),
          checksum = -1 :: integer()
         }).

-compile({inline, [lookup_fun/5, find_redundancies_by_addr_id/2,
                   reply_redundancies/3,first_fun/1, last_fun/1,
                   gen_routing_table/2, gen_routing_table_1/4, gen_routing_table_2/2,
                   redundancies/5, redundancies_1/6, redundancies_1_1/7,
                   redundancies_2/6, redundancies_3/7, get_node_by_vnode_id/2,
                   get_redundancies/3, force_sync_fun/2, force_sync_fun_1/3
                  ]}).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Start the server
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Stop the server
stop() ->
    gen_server:call(?MODULE, stop, ?DEF_TIMEOUT).


%% @doc Look up the redundant-nodes by address-id
-spec(lookup(Table, AddrId) ->
             {ok, #redundancies{}} |
             not_found when Table::atom(),
                            AddrId::non_neg_integer()).
lookup(Table, AddrId) ->
    gen_server:call(?MODULE, {lookup, Table, AddrId}, ?DEF_TIMEOUT).


%% @doc Retrieve the first record of the redundant-nodes
-spec(first(Table) ->
             {ok, #redundancies{}} |
             not_found when Table::atom()).
first(Table) ->
    gen_server:call(?MODULE, {first, Table}, ?DEF_TIMEOUT).


%% @doc Retrieve the last record of the redundant-nodes
-spec(last(Table) ->
             {ok, #redundancies{}} |
             not_found when Table::atom()).
last(Table) ->
    gen_server:call(?MODULE, {last, Table}, ?DEF_TIMEOUT).


%% @doc Force RING to synchronize with the manager-node
-spec(force_sync(Table) ->
             ok |
             {error, invalid_table} when Table::atom()).
force_sync(Table) ->
    gen_server:call(?MODULE, {force_sync, Table}, ?DEF_TIMEOUT_LONG).


%% @doc Retrieve redundancies
-spec(redundancies(TableInfo, AddrId, Members) ->
             {ok, #redundancies{}} |
             not_found when TableInfo::ring_table_info(),
                            AddrId::non_neg_integer(),
                            Members::[#member{}]).
redundancies(TableInfo, AddrId, Members) ->
    gen_server:call(?MODULE, {redundancies, TableInfo, AddrId, Members}, ?DEF_TIMEOUT).


%% @doc Collect redundancies
-spec(collect(Table, AddrIdAndKey, NumOfReplicas, MaxNumOfDuplicate) ->
             {ok, RedundanciesL} |
             not_found when Table::atom(),
                            AddrIdAndKey::{pos_integer(), binary()},
                            NumOfReplicas::pos_integer(),
                            MaxNumOfDuplicate::pos_integer(),
                            RedundanciesL::[#redundancies{}]).
collect(Table, AddrIdAndKey, NumOfReplicas, MaxNumOfDuplicate) ->
    gen_server:call(?MODULE, {collect, Table, AddrIdAndKey,
                              NumOfReplicas, MaxNumOfDuplicate}, ?DEF_TIMEOUT).


%% @doc Retrieve the checksums
-spec(checksum() ->
             {ok, {PrevChecksum, CurChecksum}} when PrevChecksum::integer(),
                                                    CurChecksum::integer()).
checksum() ->
    gen_server:call(?MODULE, checksum, ?DEF_TIMEOUT).


%% @doc Dump the current ring-info
-spec(dump() ->
             ok | {error, any()}).
dump() ->
    gen_server:call(?MODULE, dump, ?DEF_TIMEOUT).


%% @doc Sucscrive changing records of the member-tables
-spec(subscribe() ->
             ok | {error, any()}).
subscribe() ->
    gen_server:call(?MODULE, subscribe, ?DEF_TIMEOUT).


%%--------------------------------------------------------------------
%% GEN_SERVER CALLBACKS
%%--------------------------------------------------------------------
%% @doc Initiates the server
init([]) ->
    sync(),
    {ok, #state{id = ?MODULE,
                timestamp = timestamp()}}.

%% @doc gen_server callback - Module:handle_call(Request, From, State) -> Result
handle_call(stop,_From,State) ->
    {stop, normal, ok, State};


handle_call({lookup, Tbl,_AddrId},_From, State) when Tbl /= ?RING_TBL_CUR,
                                                     Tbl /= ?RING_TBL_PREV ->
    {reply, {error, invalid_table}, State};

handle_call({lookup, Tbl, AddrId},_From, State) ->
    #ring_info{ring_group_list = RingGroupList,
               first_vnode_id  = FirstVNodeId,
               last_vnode_id   = LastVNodeId} = ring_info(Tbl, State),
    Reply = lookup_fun(RingGroupList, FirstVNodeId,
                       LastVNodeId, AddrId, State),
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

handle_call({force_sync, Table},_From, State) ->
    TargetRing = ?table_to_sync_target(Table),
    NewState = force_sync_fun(TargetRing, State),
    {reply, ok, NewState};

handle_call({redundancies, TableInfo, AddrId, Members},_From,
            #state{num_of_replicas = N,
                   num_of_rack_awareness = L2} = State) ->
    Reply = redundancies(TableInfo, AddrId, N, L2, Members),
    {reply, Reply, State};

handle_call({collect, Table, AddrIdAndKey, NumOfReplicas, MaxNumOfDuplicate},_From, State) ->
    RingInfo = ring_info(Table, State),
    Reply = case leo_redundant_manager_api:get_members() of
                {ok, Members} ->
                    collect_fun(NumOfReplicas, RingInfo,
                                AddrIdAndKey, erlang:length(Members),
                                MaxNumOfDuplicate, State, []);
                Error ->
                    Error
            end,
    {reply, Reply, State};

handle_call(checksum,_From, #state{checksum = Checksum} = State) ->
    {reply, {ok, Checksum}, State};

handle_call(dump,_From, #state{cur  = #ring_info{ring_group_list = CurRing },
                               prev = #ring_info{ring_group_list = PrevRing}} = State) ->
    try
        LogDir = ?log_dir(),
        _ = filelib:ensure_dir(LogDir),
        leo_file:file_unconsult(LogDir ++ "ring_cur_worker.log."
                                ++ integer_to_list(leo_date:now()), CurRing),
        leo_file:file_unconsult(LogDir ++ "ring_prv_worker.log."
                                ++ integer_to_list(leo_date:now()), PrevRing)
    catch
        _:_ ->
            void
    end,
    {reply, ok, State};

handle_call(subscribe,_From, State) ->
    {reply, ok, State};

handle_call(_Handle, _From, State) ->
    {reply, ok, State}.


%% @doc Handling cast message
%% <p>
%% gen_server callback - Module:handle_cast(Request, State) -> Result.
%% </p>
handle_cast(sync, State) ->
    case catch maybe_sync(State) of
        {'EXIT', _Reason} ->
            {noreply, State};
        NewState ->
            {noreply, NewState}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

%% @doc Handling all non call/cast messages
%% <p>
%% gen_server callback - Module:handle_info(Info, State) -> Result.
%% </p>
handle_info(_Info, State) ->
    {noreply, State}.

%% @doc This function is called by a gen_server when it is about to
%%      terminate. It should be the opposite of Module:init/1 and do any necessary
%%      cleaning up. When it returns, the gen_server terminates with Reason.
terminate(_Reason, _State) ->
    ok.

%% @doc Convert process state when code is changed
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%--------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Retrieve current time
%% @private
-spec(timestamp() ->
             integer()).
timestamp() ->
    leo_math:floor(leo_date:clock() / 1000).


%% @doc Retrieve the number of replicas
%% @private
update_state(State) ->
    {ok, Options} = leo_redundant_manager_api:get_options(),
    NumOfReplica = leo_misc:get_value(?PROP_N, Options, 0),
    case (NumOfReplica > 0) of
        true ->
            NumOfL2 = leo_misc:get_value(?PROP_L2, Options, 0),
            State#state{num_of_replicas = NumOfReplica,
                        num_of_rack_awareness = NumOfL2};
        false ->
            State
    end.


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
maybe_sync(State) ->
    case update_state(State) of
        #state{num_of_replicas = NumOfReplica} = State_1 when NumOfReplica > 0 ->
            maybe_sync_1(State_1);
        _ ->
            State
    end.

%% @private
maybe_sync_1(#state{checksum = {PrevHash, CurHash},
                    cur = #ring_info{ring_group_list = CurRing},
                    prev = #ring_info{ring_group_list = PrevRing},
                    min_interval = MinInterval,
                    timestamp = Timestamp} = State) ->
    ThisTime = timestamp(),
    sync(),

    case ((ThisTime - Timestamp) < MinInterval) of
        true ->
            State#state{timestamp = ThisTime};
        false ->
            CurHash_Now = erlang:crc32(term_to_binary(CurRing)),
            PrevHash_Now = erlang:crc32(term_to_binary(PrevRing)),
            State_3 = case (CurHash  /= CurHash_Now orelse
                            PrevHash /= PrevHash_Now) of
                          true ->
                              State_1 = maybe_sync_2(?RING_TBL_CUR, CurHash_Now,  CurHash, State),
                              State_2 = maybe_sync_2(?RING_TBL_PREV, PrevHash_Now, PrevHash, State_1),
                              State_2;
                          false ->
                              State
                      end,
            State_3#state{timestamp = ThisTime}
    end.

%% @private
maybe_sync_2(_, Hash, Hash, State) ->
    State;
maybe_sync_2(RingVer, Hash_Now, Hash_Old, State) ->
    {Tbl, Target, RingInfo_1} =
        case RingVer of
            ?RING_TBL_CUR ->
                #state{cur = RingInfo} = State,
                {?MEMBER_TBL_CUR, ?SYNC_TARGET_RING_CUR, RingInfo};
            ?RING_TBL_PREV ->
                #state{prev = RingInfo} = State,
                {?MEMBER_TBL_PREV, ?SYNC_TARGET_RING_PREV, RingInfo}
        end,

    case leo_cluster_tbl_member:find_all(Tbl) of
        {ok, Members} ->
            Members_1 = lists:foldl(fun(#member{state = ?STATE_ATTACHED}, Acc) ->
                                            Acc;
                                       (Member, Acc) ->
                                            Acc ++ [Member]
                                    end, [], Members),
            SyncInfo = #sync_info{target = Target,
                                  org_checksum = Hash_Now,
                                  cur_checksum = Hash_Old},
            State_1 = State#state{cur = RingInfo_1#ring_info{members = Members_1}},
            maybe_sync_2_1(SyncInfo, State_1);
        _ ->
            State
    end.

%% @private
-spec(maybe_sync_2_1(#sync_info{}, #state{}) ->
             #state{}).
maybe_sync_2_1(#sync_info{org_checksum = OrgChecksum,
                          cur_checksum = CurChecksum}, State) when OrgChecksum == CurChecksum ->
    State;
maybe_sync_2_1(#sync_info{target = TargetRing} = SyncInfo, #state{checksum = WorkerChecksum} = State) ->
    case gen_routing_table(SyncInfo, State) of
        {ok, #ring_info{ring_group_list = RetL} = RingInfo} ->
            case TargetRing of
                ?SYNC_TARGET_RING_CUR ->
                    {PrevHash,_} = WorkerChecksum,
                    CurHash = erlang:crc32(
                                term_to_binary(RetL)),
                    State#state{cur = RingInfo,
                                checksum = {PrevHash, CurHash}};
                ?SYNC_TARGET_RING_PREV ->
                    {_,CurHash} = WorkerChecksum,
                    PrevHash = erlang:crc32(
                                 term_to_binary(RetL)),
                    State#state{prev = RingInfo,
                                checksum = {PrevHash, CurHash}}
            end;
        {error,_} ->
            State
    end.


%% @doc Generate RING for this process
%% @private
-spec(gen_routing_table(#sync_info{}, #state{}) ->
             {ok, #ring_info{}} | {error, any()}).
gen_routing_table(#sync_info{target = Target},_) when Target /= ?SYNC_TARGET_RING_CUR,
                                                      Target /= ?SYNC_TARGET_RING_PREV ->
    {error, invalid_target_ring};
gen_routing_table(#sync_info{target = Target} = SyncInfo, State) ->
    %% Retrieve ring from local's master [etc|mnesia]
    {ok, Ring_1} = leo_redundant_manager_api:get_ring(?SYNC_TARGET_RING_CUR),
    RingSize  = length(Ring_1),
    GroupSize = leo_math:ceiling(RingSize / ?DEF_NUM_OF_DIV),

    %% Calculate ring's checksum
    Ring_2 = case Target of
                 ?SYNC_TARGET_RING_CUR ->
                     Ring_1;
                 _ ->
                     {ok, RingPrev} = leo_redundant_manager_api:get_ring(Target),
                     RingPrev
             end,
    Checksum = erlang:crc32(term_to_binary(Ring_2)),

    %% Retrieve redundancies by addr-id
    gen_routing_table_1(Ring_1, SyncInfo, #ring_conf{id = 0,
                                                     group_id = 0,
                                                     ring_size = RingSize,
                                                     group_size = GroupSize,
                                                     index_list = [],
                                                     table_list = [],
                                                     from_addr_id = 0,
                                                     checksum = Checksum}, State).

%% @private
-spec(gen_routing_table_1([]|[{integer(),atom(),integer()}],
                          #sync_info{}, #ring_conf{}, #state{}) ->
             {ok, #ring_info{}} | {error, any()}).
gen_routing_table_1([], #sync_info{target = Target},
                    #ring_conf{index_list = IdxAcc,
                               checksum = Checksum},
                    #state{num_of_replicas = NumOfReplicas} = State) ->
    IdxAcc_1 = lists:reverse(IdxAcc),
    Members = case Target of
                  ?SYNC_TARGET_RING_CUR ->
                      (State#state.cur )#ring_info.members;
                  ?SYNC_TARGET_RING_PREV ->
                      (State#state.prev)#ring_info.members
              end,
    FirstAddrId = case first_fun(IdxAcc_1) of
                      not_found ->
                          -1;
                      {ok, #redundancies{vnode_id_to = To_1}} ->
                          To_1
                  end,
    LastAddrId  = case last_fun(IdxAcc_1) of
                      not_found ->
                          -1;
                      {ok, #redundancies{vnode_id_to = To_2}} ->
                          To_2
                  end,
    case check_redandancies(NumOfReplicas, IdxAcc_1) of
        ok ->
            void;
        _ ->
            timer:apply_after(250, ?MODULE,
                              force_sync, [?sync_target_to_table(Target)])
    end,
    {ok, #ring_info{
            checksum = Checksum,
            ring_group_list = IdxAcc_1,
            first_vnode_id = FirstAddrId,
            last_vnode_id = LastAddrId,
            members = Members}};

gen_routing_table_1([{AddrId,_Node,_Clock}|Rest], SyncInfo, RingConf, State) ->
    TargetRing = SyncInfo#sync_info.target,
    MembersCur = (State#state.cur)#ring_info.members,
    NumOfReplicas = State#state.num_of_replicas,
    NumOfAwarenessL2 = State#state.num_of_rack_awareness,
    TblInfo = leo_redundant_manager_api:table_info(?VER_CUR),

    case redundancies(TblInfo, AddrId,
                      NumOfReplicas, NumOfAwarenessL2, MembersCur) of
        {ok, #redundancies{nodes = CurNodes} = Redundancies} ->
            Redundancies_1 =
                case TargetRing of
                    ?SYNC_TARGET_RING_PREV ->
                        MembersPrev = (State#state.prev)#ring_info.members,
                        TblInfoPrev = leo_redundant_manager_api:table_info(?VER_PREV),

                        case redundancies(TblInfoPrev, AddrId,
                                          NumOfReplicas, NumOfAwarenessL2, MembersPrev) of
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
            Error
    end.

%% @private
gen_routing_table_1_1([], Acc) ->
    Acc;
gen_routing_table_1_1([N|Rest], Acc) ->
    Acc_1 = [_N || #redundant_node{node = _N} <- Acc],
    Acc_2 = case lists:member(N, Acc_1) of
                false ->
                    Acc ++ [#redundant_node{node = N}];
                true ->
                    Acc
            end,
    gen_routing_table_1_1(Rest, Acc_2).

%% @private
-spec(gen_routing_table_2(#redundancies{}, #ring_conf{}) ->
             #ring_conf{}).
gen_routing_table_2(#redundancies{nodes = Nodes}, #ring_conf{id = Id,
                                                             ring_size = RingSize,
                                                             group_size = GroupSize,
                                                             group_id = GrpId,
                                                             addr_id = AddrId,
                                                             from_addr_id = FromAddrId,
                                                             index_list = IdxAcc,
                                                             table_list = TblAcc} = RingConf) ->
    Id1 = Id + 1,
    VNodeId_Nodes = #vnodeid_nodes{id = Id1,
                                   vnode_id_from = FromAddrId,
                                   vnode_id_to = AddrId,
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
                                                         index_to = AddrId,
                                                         vnodeid_nodes_list = RingGroup}|IdxAcc],
                               table_list = []};
        false ->
            RingConf#ring_conf{id = Id1,
                               group_id = GrpId + 1,
                               from_addr_id = AddrId + 1,
                               table_list = [VNodeId_Nodes|TblAcc]}
    end.


%% @doc Check to satisfy the number of replicas
%% @private
-spec(check_redandancies(NumOfReplicas, Ring) ->
             ok | {error, any()} when NumOfReplicas::pos_integer(),
                                      Ring::[#ring_group{}]).
check_redandancies(_,[]) ->
    ok;
check_redandancies(NumOfReplicas, [#ring_group{vnodeid_nodes_list = Ring}|Rest]) ->
    case check_redandancies_1(NumOfReplicas, Ring) of
        ok ->
            check_redandancies(NumOfReplicas, Rest);
        {error, Cause} ->
            {error, Cause}
    end.

%% @private
-spec(check_redandancies_1(NumOfReplicas, VNodesL) ->
             ok | {error, invalid_redundancies} when NumOfReplicas::pos_integer(),
                                                     VNodesL::[#vnodeid_nodes{}]).
check_redandancies_1(_,[]) ->
    ok;
check_redandancies_1(NumOfReplicas, [#vnodeid_nodes{nodes = VNodes}|Rest]) ->
    case (NumOfReplicas =< length(VNodes)) of
        true ->
            check_redandancies_1(NumOfReplicas, Rest);
        false ->
            {error, invalid_redundancies}
    end.


%% @doc get redundancies by key.
%% @private
-spec(redundancies(TableInfo, VNodeId, NumOfReplicas, L2, Members) ->
             {ok, any()} |
             {error, any()} when TableInfo::ring_table_info(),
                                 VNodeId::integer(),
                                 NumOfReplicas::integer(),
                                 L2::integer(),
                                 Members::[#member{}]).
redundancies(_,_,NumOfReplicas,_,_) when NumOfReplicas < ?DEF_MIN_REPLICAS;
                                         NumOfReplicas > ?DEF_MAX_REPLICAS ->
    {error, out_of_renge};
redundancies(_,_,NumOfReplicas, L2,_) when (NumOfReplicas - L2) < 1 ->
    {error, invalid_level2};
redundancies(TableInfo, VNodeId_0, NumOfReplicas, L2, Members) ->
    case leo_cluster_tbl_ring:lookup(TableInfo, VNodeId_0) of
        {error, Cause} ->
            {error, Cause};
        not_found ->
            case get_node_by_vnode_id(TableInfo, VNodeId_0) of
                {ok, VNodeId_1} ->
                    redundancies_1(TableInfo, VNodeId_0, VNodeId_1,
                                   NumOfReplicas, L2, Members);
                {error, Cause} ->
                    {error, Cause}
            end;
        #?RING{node = Node} ->
            redundancies_1_1(TableInfo, VNodeId_0, VNodeId_0,
                             NumOfReplicas, L2, Members, Node)
    end.

%% @private
redundancies_1(TableInfo, VNodeId_Org, VNodeId_Hop, NumOfReplicas, L2, Members) ->
    case leo_cluster_tbl_ring:lookup(TableInfo, VNodeId_Hop) of
        {error, Cause} ->
            {error, Cause};
        not_found ->
            {error, ring_not_found};
        #?RING{node = Node} ->
            redundancies_1_1(TableInfo, VNodeId_Org, VNodeId_Hop,
                             NumOfReplicas, L2, Members, Node)
    end.

%% @private
redundancies_1_1(TableInfo, VNodeId_Org, VNodeId_Hop, NumOfReplicas, L2, Members, Node) ->
    case get_redundancies(Members, Node, []) of
        not_found ->
            {error, ?ERROR_COULD_NOT_GET_REDUNDANCIES};
        {Node, SetsL2} ->
            redundancies_2(TableInfo, NumOfReplicas-1, L2, Members, VNodeId_Hop,
                           #redundancies{id = VNodeId_Org,
                                         vnode_id_to = VNodeId_Hop,
                                         temp_nodes = [Node],
                                         temp_level_2 = SetsL2,
                                         nodes = [#redundant_node{node = Node}]})
    end.

%% @private
redundancies_2(_TableInfo,_,_L2,_Members,-1,_R) ->
    {error,  invalid_vnode};
redundancies_2(_TableInfo,0,_L2,_Members,_VNodeId, #redundancies{nodes = Acc} = R) ->
    {ok, R#redundancies{temp_nodes = [],
                        temp_level_2 = [],
                        nodes = lists:reverse(Acc)}};
redundancies_2(TableInfo, NumOfReplicas, L2, Members, VNodeId_0, R) ->
    case get_node_by_vnode_id(TableInfo, VNodeId_0) of
        {ok, VNodeId_1} ->
            case leo_cluster_tbl_ring:lookup(TableInfo, VNodeId_1) of
                {error, Cause} ->
                    {error, Cause};
                not_found ->
                    case get_node_by_vnode_id(TableInfo, VNodeId_1) of
                        {ok, Node} ->
                            redundancies_3(TableInfo, NumOfReplicas, L2, Members, VNodeId_1, Node, R);
                        {error, Cause} ->
                            {error, Cause}
                    end;
                #?RING{node = Node} ->
                    redundancies_3(TableInfo, NumOfReplicas, L2, Members, VNodeId_1, Node, R)
            end;
        _ ->
            {error, out_of_range}
    end.

redundancies_3(TableInfo, NumOfReplicas, L2, Members, VNodeId, Node_1, R) ->
    AccTempNode = R#redundancies.temp_nodes,
    AccLevel2 = R#redundancies.temp_level_2,
    AccNodes = R#redundancies.nodes,

    case lists:member(Node_1, AccTempNode) of
        true  ->
            redundancies_2(TableInfo, NumOfReplicas, L2, Members, VNodeId, R);
        false ->
            case get_redundancies(Members, Node_1, AccLevel2) of
                not_found ->
                    {error, ?ERROR_COULD_NOT_GET_REDUNDANCIES};
                {Node_2, AccLevel2_1} ->
                    case (L2 /= 0 andalso L2 == length(AccNodes)) of
                        true when length(AccLevel2_1) < (L2 + 1) ->
                            redundancies_2(TableInfo, NumOfReplicas, L2, Members, VNodeId, R);
                        _ ->
                            redundancies_2(TableInfo, NumOfReplicas-1, L2, Members, VNodeId,
                                           R#redundancies{temp_nodes = [Node_2|AccTempNode],
                                                          temp_level_2 = AccLevel2_1,
                                                          nodes = [#redundant_node{node = Node_2}
                                                                   |AccNodes]})
                    end
            end
    end.


%% @doc Retrieve virtual-node by vnode-id
%% @private
-spec(get_node_by_vnode_id(TableInfo, VNodeId) ->
             {ok, integer()} |
             {error, any()} when TableInfo::ring_table_info(),
                                 VNodeId::integer()).
get_node_by_vnode_id(TableInfo, VNodeId) ->
    case leo_cluster_tbl_ring:next(TableInfo, VNodeId) of
        '$end_of_table' ->
            case leo_cluster_tbl_ring:first(TableInfo) of
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
-spec(get_redundancies([#member{}], atom(), [string()]) ->
             {atom, [string()]} | not_found).
get_redundancies([],_,_) ->
    not_found;
get_redundancies([#member{node = Node_0,
                          grp_level_2 = L2}|_], Node_1, SetL2) when Node_0 == Node_1 ->
    case lists:member(L2, SetL2) of
        false  ->
            {Node_0, [L2|SetL2]};
        _Other ->
            {Node_0, SetL2}
    end;
get_redundancies([#member{node = Node_0}|T], Node_1, SetL2) when Node_0 /= Node_1 ->
    get_redundancies(T, Node_1, SetL2).


%% @doc Reply redundancies
%% @private
-spec(reply_redundancies(not_found | {ok, #redundancies{}}, integer(), #state{}) ->
             not_found | {ok, #redundancies{}}).
reply_redundancies(not_found,_,_) ->
    not_found;
reply_redundancies({ok, #redundancies{nodes = Nodes} = Redundancies},
                   AddrId, #state{num_of_replicas = NumOfReplicas}) ->
    reply_redundancies_1(Redundancies, AddrId, Nodes, NumOfReplicas, 0, []).

%% @private
reply_redundancies_1(Redundancies, AddrId, [],_NumOfReplicas,_Index,Acc) ->
    {ok, Redundancies#redundancies{id = AddrId,
                                   nodes = lists:reverse(Acc)}};

reply_redundancies_1(Redundancies, AddrId, [#redundant_node{node = Node}|Rest], NumOfReplicas, Index, Acc) ->
    NextIndex = Index + 1,
    case leo_cluster_tbl_member:lookup(Node) of
        {ok, #member{state = State}} ->
            Available = (State == ?STATE_RUNNING),
            CanReadRepair = (NumOfReplicas >= (length(Acc) + 1)),
            ConsensusRole = case Index of
                                0 -> ?CNS_ROLE_LEADER;
                                _ when CanReadRepair == true -> ?CNS_ROLE_FOLLOWER_1;
                                _ when CanReadRepair /= true -> ?CNS_ROLE_OBSERBER
                            end,
            reply_redundancies_1(Redundancies, AddrId, Rest, NumOfReplicas, NextIndex,
                                 [#redundant_node{node = Node,
                                                  available = Available,
                                                  can_read_repair = CanReadRepair,
                                                  role = ConsensusRole}|Acc]);
        _ ->
            reply_redundancies_1(Redundancies, AddrId, Rest, NumOfReplicas, NextIndex, Acc)
    end.


%% @doc Retrieve first record
%% @private
-spec(first_fun(list(#ring_group{})) ->
             not_found | {ok, #redundancies{}}).
first_fun([]) ->
    not_found;
first_fun([#ring_group{vnodeid_nodes_list = AddrId_Nodes_List}|_]) ->
    case AddrId_Nodes_List of
        [] ->
            not_found;
        [#vnodeid_nodes{vnode_id_from = From,
                        vnode_id_to = To,
                        nodes = Nodes}|_] ->
            {ok, #redundancies{id = From,
                               vnode_id_from = From,
                               vnode_id_to = To,
                               nodes = Nodes}}
    end;
first_fun(_) ->
    not_found.


%% @doc Retrieve last record
%% @private
-spec(last_fun(list(#ring_group{})) ->
             not_found | {ok, #redundancies{}}).
last_fun([]) ->
    not_found;
last_fun(RingGroupList) ->
    #ring_group{vnodeid_nodes_list = AddrId_Nodes_List} = lists:last(RingGroupList),
    case AddrId_Nodes_List of
        [] ->
            not_found;
        _ ->
            #vnodeid_nodes{vnode_id_from = From,
                           vnode_id_to = To,
                           nodes = Nodes} = lists:last(AddrId_Nodes_List),
            {ok, #redundancies{id = From,
                               vnode_id_from = From,
                               vnode_id_to = To,
                               nodes = Nodes}}
    end.


%% @doc Retrieve redundancies by vnode-id
%% @private
-spec(lookup_fun(list(#ring_group{}), integer(), integer(), integer(), #state{}) ->
             not_found | {ok, #redundancies{}}).
lookup_fun([],_,_,_AddrId,_) ->
    not_found;
lookup_fun(RingGroupList, FirstVNodeId,
           _LastVNodeId, AddrId, State) when FirstVNodeId >= AddrId ->
    Ret = first_fun(RingGroupList),
    reply_redundancies(Ret, AddrId, State);
lookup_fun(RingGroupList,_FirstVNodeId,
           LastVNodeId, AddrId, State) when LastVNodeId < AddrId ->
    Ret = first_fun(RingGroupList),
    reply_redundancies(Ret, AddrId, State);
lookup_fun(RingGroupList,_,_, AddrId, State) ->
    Ret = find_redundancies_by_addr_id(RingGroupList, AddrId),
    reply_redundancies(Ret, AddrId, State).


%% @doc Find redundanciess by vnodeid
%% @private
-spec(find_redundancies_by_addr_id(list(#ring_group{}), integer()) ->
             not_found | {ok, #redundancies{}}).
find_redundancies_by_addr_id([],_AddrId) ->
    not_found;
find_redundancies_by_addr_id(
  [#ring_group{index_from = From,
               index_to = To,
               vnodeid_nodes_list = List}|_Rest], AddrId) when From =< AddrId,
                                                               To   >= AddrId ->
    find_redundancies_by_addr_id_1(List, AddrId);
find_redundancies_by_addr_id([_|Rest], AddrId) ->
    find_redundancies_by_addr_id(Rest, AddrId).


find_redundancies_by_addr_id_1([],_AddrId) ->
    not_found;
find_redundancies_by_addr_id_1(
  [#vnodeid_nodes{vnode_id_from = From,
                  vnode_id_to = To,
                  nodes = Nodes}|_], AddrId) when From =< AddrId,
                                                  To   >= AddrId ->
    {ok, #redundancies{vnode_id_from = From,
                       vnode_id_to = To,
                       nodes = Nodes}};
find_redundancies_by_addr_id_1([_|Rest], AddrId) ->
    find_redundancies_by_addr_id_1(Rest, AddrId).


%% @doc Force sync
%% @private
-spec(force_sync_fun(?SYNC_TARGET_RING_CUR|?SYNC_TARGET_RING_PREV, #state{}) ->
             #state{}).
force_sync_fun(TargetRing, State) ->
    case update_state(State) of
        #state{num_of_replicas = NumOfReplica} = State_1 when NumOfReplica > 0 ->
            Tbl = case TargetRing of
                      ?SYNC_TARGET_RING_CUR ->
                          ?MEMBER_TBL_CUR;
                      ?SYNC_TARGET_RING_PREV ->
                          ?MEMBER_TBL_PREV
                  end,
            case leo_cluster_tbl_member:find_all(Tbl) of
                {ok, Members} ->
                    State_2 = case TargetRing of
                                  ?SYNC_TARGET_RING_CUR ->
                                      State_1#state{cur = #ring_info{members = Members}};
                                  ?SYNC_TARGET_RING_PREV ->
                                      State_1#state{prev = #ring_info{members = Members}}
                              end,
                    Ret = gen_routing_table(#sync_info{target = TargetRing}, State_2),
                    force_sync_fun_1(Ret, TargetRing, State_2);
                _Other ->
                    State_1
            end;
        _ ->
            State
    end.

%% @private
force_sync_fun_1({ok, #ring_info{ring_group_list = RetL} = RingInfo}, ?SYNC_TARGET_RING_CUR,
                 #state{checksum = Checksum} = State) ->
    {PrevHash,_} = Checksum,
    CurHash = erlang:crc32(term_to_binary(RetL)),
    State#state{cur = RingInfo,
                checksum = {PrevHash, CurHash}};
force_sync_fun_1({ok, #ring_info{ring_group_list = RetL} = RingInfo}, ?SYNC_TARGET_RING_PREV,
                 #state{checksum = Checksum} = State) ->
    {_,CurHash} = Checksum,
    PrevHash = erlang:crc32(term_to_binary(RetL)),
    State#state{prev = RingInfo,
                checksum = {PrevHash, CurHash}};
force_sync_fun_1(_,_,State) ->
    State.


%% @doc Coolect redundancies of the keys
%% @private
-spec(collect_fun(NumOfReplicas, RingInfo, AddrIdAndKey, TotalMembers, MaxNumOfDuplicate, State, Acc) ->
             not_found | {ok, Acc} when NumOfReplicas::pos_integer(),
                                        RingInfo::#ring_info{},
                                        AddrIdAndKey::{non_neg_integer(), binary()},
                                        TotalMembers::pos_integer(),
                                        MaxNumOfDuplicate::pos_integer(),
                                        State::#state{},
                                        Acc::[#redundancies{}]).
collect_fun(NumOfReplicas,_RingInfo, _AddrIdAndKey,_TotalMembers,
            _MaxNumOfDuplicate,_State, Acc) when NumOfReplicas =< erlang:length(Acc)->
    Acc_1 = lists:sublist(Acc, NumOfReplicas),
    {ok, Acc_1};
collect_fun(NumOfReplicas, #ring_info{ring_group_list = RingGroupList,
                                      first_vnode_id = FirstVNodeId,
                                      last_vnode_id = LastVNodeId} = RingInfo,
            {AddrId, Key}, TotalMembers, MaxNumOfDuplicate, State, Acc) ->
    case lookup_fun(RingGroupList, FirstVNodeId,
                    LastVNodeId, AddrId, State) of
        {ok, #redundancies{nodes = RedundantNodeL}} ->
            ChildId = erlang:length(Acc) + 1,
            ChildIdBin = list_to_binary(integer_to_list(ChildId)),
            Key_1 = << Key/binary, "\n", ChildIdBin/binary >>,
            AddrId_1 = leo_redundant_manager_chash:vnode_id(Key_1),
            CanAppend = case Acc of
                            [] ->
                                true;
                            _ ->
                                lists:foldl(
                                  fun(Node_1, true) ->
                                          MaxNumOfDuplicate >= count_node(Acc, Node_1, 0);
                                     (_, false) ->
                                          false
                                  end, true, [Node || #redundant_node{
                                                         node = Node} <- RedundantNodeL])
                        end,
            case (CanAppend == true orelse (CanAppend == false andalso
                                            TotalMembers =< MaxNumOfDuplicate)) of
                true ->
                    NewAcc = lists:append([Acc, RedundantNodeL]),
                    collect_fun(NumOfReplicas, RingInfo, {AddrId_1, Key},
                                TotalMembers, MaxNumOfDuplicate, State, NewAcc);
                false ->
                    collect_fun(NumOfReplicas, RingInfo, {AddrId_1, Key},
                                TotalMembers, MaxNumOfDuplicate, State, Acc)
            end;
        Other ->
            Other
    end.


%% @private
count_node([],_Node, Count) ->
    Count;
count_node([#redundant_node{node = Node}|Rest], Node, Count) ->
    count_node(Rest, Node, Count + 1);
count_node([_|Rest], Node, Count) ->
    count_node(Rest, Node, Count).


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
