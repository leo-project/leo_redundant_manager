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
%% @doc The redundant manager's worker process
%% @reference https://github.com/leo-project/leo_redundant_manager/blob/master/src/leo_redundant_manager_worker.erl
%% @end
%%======================================================================
-module(leo_redundant_manager_worker).

-behaviour(gen_server).

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/1, stop/1]).
-export([add_vnode/3,
         set_vnode_list/3,
         remove_vnode_list/3,
         get_vnode_list/2,
         first_vnode/2,
         last_vnode/2
        ]).
-export([lookup/3, first/2, last/2, force_sync/2,
         redundancies/4,
         collect/5,
         checksum/1,
         dump/1, subscribe/1]).

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
          cluster_id :: cluster_id(),
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
          ring_size = 0 :: non_neg_integer(),
          addr_id = 0 :: non_neg_integer(),
          from_vnode_id = 0 :: non_neg_integer(),
          checksum = -1 :: integer()
         }).

-record(redundancy_info, {
          table_info :: table_info(),
          cluster_id :: cluster_id(),
          num_of_replicas = 0 :: non_neg_integer(),
          rack_awareness_level = 0 :: non_neg_integer(),
          vnode_id = 0 :: non_neg_integer(),
          %% vnode_id_org = 0 :: non_neg_integer(),
          vnode_id_hop = 0 :: non_neg_integer(),
          node :: atom(),
          members = [] :: [#?MEMBER{}],
          result = #redundancies{} :: #redundancies{}
         }).

-compile({inline, [
                   lookup_fun/5,
                   reply_redundancies/3,
                   gen_routing_table/2, gen_routing_table_1/5,
                   redundancies/1, redundancies_1/1, redundancies_2/1,
                   redundancies_3/1, redundancies_4/1, lookup_vnode_id/3,
                   get_redundancies/3, force_sync_fun/2, force_sync_fun_1/3
                  ]}).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Start the server
start_link(ClusterId) ->
    Id = ?id_red_worker(ClusterId),
    gen_server:start_link({local, Id}, ?MODULE, [Id, ClusterId], []).

%% @doc Stop the server
stop(ClusterId) ->
    Id = ?id_red_worker(ClusterId),
    gen_server:call(Id, stop, ?DEF_TIMEOUT).


%% @doc Insert VNode
-spec(add_vnode(ClusterId, Ver, VNode) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Ver::version(),
                                      VNode::{}).
add_vnode(ClusterId, Ver, VNode) ->
    Id = ?id_red_worker(ClusterId),
    gen_server:call(Id, {add_vnode, Ver, VNode}, ?DEF_TIMEOUT).


%% @doc Set a vnode list
-spec(set_vnode_list(ClusterId, Ver, VNodeList) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Ver::version(),
                                      VNodeList::[]).
set_vnode_list(ClusterId, Ver, VNodeList) ->
    Id = ?id_red_worker(ClusterId),
    gen_server:call(Id, {set_vnode_list, Ver, VNodeList}, ?DEF_TIMEOUT).


%% @doc Remove vnodes from a vnode list
-spec(remove_vnode_list(ClusterId, Ver, VNodeList) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Ver::version(),
                                      VNodeList::[]).
remove_vnode_list(ClusterId, Ver, VNodeList) ->
    Id = ?id_red_worker(ClusterId),
    gen_server:call(Id, {remove_vnode_list, Ver, VNodeList}, ?DEF_TIMEOUT).


-spec(get_vnode_list(ClusterId, Ver) ->
             {ok, VNodeList} | {error, any()} when ClusterId::cluster_id(),
                                                   Ver::version(),
                                                   VNodeList::[]).
get_vnode_list(ClusterId, Ver) ->
    Id = ?id_red_worker(ClusterId),
    %% @TODO
    gen_server:call(Id, {get_vnode_list, Ver}, ?DEF_TIMEOUT).


%% @doc Retrieve a first vnode
-spec(first_vnode(ClusterId, Ver) ->
             {ok, []} | {error, any()} when ClusterId::cluster_id(),
                                            Ver::version()).
first_vnode(ClusterId, Ver) ->
    Id = ?id_red_worker(ClusterId),
    %% @TODO
    gen_server:call(Id, {first_vnode, Ver}, ?DEF_TIMEOUT).


%% @doc Retrieve a last vnode
-spec(last_vnode(ClusterId, Ver) ->
             {ok, []} | {error, any()} when ClusterId::cluster_id(),
                                            Ver::version()).
last_vnode(ClusterId, Ver) ->
    Id = ?id_red_worker(ClusterId),
    %% @TODO
    gen_server:call(Id, {last_vnode, Ver}, ?DEF_TIMEOUT).


%% @doc Look up the redundant-nodes by address-id
-spec(lookup(ClusterId, Table, AddrId) ->
             {ok, #redundancies{}} |
             not_found when ClusterId::cluster_id(),
                            Table::atom(),
                            AddrId::non_neg_integer()).
lookup(ClusterId, Table, AddrId) ->
    Id = ?id_red_worker(ClusterId),
    gen_server:call(Id, {lookup, Table, AddrId}, ?DEF_TIMEOUT).


%% @doc Retrieve the first record of the redundant-nodes
-spec(first(ClusterId, Table) ->
             {ok, #redundancies{}} |
             not_found when ClusterId::cluster_id(),
                            Table::atom()).
first(ClusterId, Table) ->
    Id = ?id_red_worker(ClusterId),
    gen_server:call(Id, {first, Table}, ?DEF_TIMEOUT).


%% @doc Retrieve the last record of the redundant-nodes
-spec(last(ClusterId, Table) ->
             {ok, #redundancies{}} |
             not_found when ClusterId::cluster_id(),
                            Table::atom()).
last(ClusterId, Table) ->
    Id = ?id_red_worker(ClusterId),
    gen_server:call(Id, {last, Table}, ?DEF_TIMEOUT).


%% @doc Force RING to synchronize with the manager-node
-spec(force_sync(ClusterId, Table) ->
             ok |
             {error, invalid_table} when ClusterId::cluster_id(),
                                         Table::atom()).
force_sync(ClusterId, Table) ->
    Id = ?id_red_worker(ClusterId),
    gen_server:call(Id, {force_sync, Table}, ?DEF_TIMEOUT_LONG).


%% @doc Retrieve redundancies
-spec(redundancies(ClusterId, TableInfo, AddrId, Members) ->
             {ok, #redundancies{}} |
             not_found when ClusterId::cluster_id(),
                            TableInfo::ring_table_info(),
                            AddrId::non_neg_integer(),
                            Members::[#?MEMBER{}]).
redundancies(ClusterId, TableInfo, AddrId, Members) ->
    Id = ?id_red_worker(ClusterId),
    gen_server:call(Id, {redundancies, TableInfo, AddrId, Members}, ?DEF_TIMEOUT).


%% @doc Collect redundancies
-spec(collect(ClusterId, Table, AddrIdAndKey, NumOfReplicas, MaxNumOfDuplicate) ->
             {ok, RedundanciesL} |
             not_found when ClusterId::cluster_id(),
                            Table::atom(),
                            AddrIdAndKey::{pos_integer(), binary()},
                            NumOfReplicas::pos_integer(),
                            MaxNumOfDuplicate::pos_integer(),
                            RedundanciesL::[#redundancies{}]).
collect(ClusterId, Table, AddrIdAndKey, NumOfReplicas, MaxNumOfDuplicate) ->
    Id = ?id_red_worker(ClusterId),
    gen_server:call(Id, {collect, Table, AddrIdAndKey,
                         NumOfReplicas, MaxNumOfDuplicate}, ?DEF_TIMEOUT).


%% @doc Retrieve the checksums
-spec(checksum(ClusterId) ->
             {ok, {PrevChecksum, CurChecksum}} when ClusterId::cluster_id(),
                                                    PrevChecksum::integer(),
                                                    CurChecksum::integer()).
checksum(ClusterId) ->
    Id = ?id_red_worker(ClusterId),
    gen_server:call(Id, checksum, ?DEF_TIMEOUT).


%% @doc Dump the current ring-info
-spec(dump(ClusterId) ->
             ok | {error, any()} when ClusterId::cluster_id()).
dump(ClusterId) ->
    Id = ?id_red_worker(ClusterId),
    gen_server:call(Id, dump, ?DEF_TIMEOUT).


%% @doc Sucscrive changing records of the member-tables
-spec(subscribe(ClusterId) ->
             ok | {error, any()} when ClusterId::cluster_id()).
subscribe(ClusterId) ->
    Id = ?id_red_worker(ClusterId),
    gen_server:call(Id, subscribe, ?DEF_TIMEOUT).


%%--------------------------------------------------------------------
%% GEN_SERVER CALLBACKS
%%--------------------------------------------------------------------
%% @doc Initiates the server
init([Id, ClusterId]) ->
    sync(),
    {ok, #state{id = Id,
                cluster_id = ClusterId,
                timestamp = timestamp()}}.

%% @doc gen_server callback - Module:handle_call(Request, From, State) -> Result
handle_call(stop,_From,State) ->
    {stop, normal, ok, State};

handle_call({add_vnode, Ver, {VNodeId, Node, Clock}},
            _From, #state{cluster_id = ClusterId} = State) ->
    Reply = leo_cluster_tbl_ring:insert(leo_redundant_manager_api:table_info(Ver),
                                        #?RING{id = {ClusterId, VNodeId},
                                               cluster_id = ClusterId,
                                               vnode_id = VNodeId,
                                               node = Node,
                                               clock = Clock}),
    {reply, Reply, State};

handle_call({set_vnode_list, Ver, VNodeList},
            _From, #state{cluster_id = ClusterId} = State) ->
    TblInfo = leo_redundant_manager_api:table_info(Ver),
    [leo_cluster_tbl_ring:insert(TblInfo,
                                 #?RING{id = {ClusterId, VNodeId},
                                        cluster_id = ClusterId,
                                        vnode_id = VNodeId,
                                        node = Node,
                                        clock = Clock})
     || {VNodeId, Node, Clock} <- VNodeList],
    Reply = ok,
    {reply, Reply, State};

handle_call({remove_vnode_list, Ver, VNodeList},_From,
            #state{cluster_id = ClusterId} = State) ->
    TblInfo = leo_redundant_manager_api:table_info(Ver),
    [leo_cluster_tbl_ring:delete(TblInfo, ClusterId, VNodeId)
     || VNodeId <- VNodeList],
    Reply = ok,
    {reply, Reply, State};

handle_call({get_vnode_list, ?VER_CUR},_From,
            #state{cur = #ring_info{vnode_list = VNodeList}} = State) ->
    {reply, {ok, VNodeList}, State};

handle_call({get_vnode_list, ?VER_PREV},_From,
            #state{prev = #ring_info{vnode_list = VNodeList}} = State) ->
    {reply, {ok, VNodeList}, State};

handle_call({first_vnode,_Ver},_From, State) ->
    Reply = ok,
    {reply, Reply, State};

handle_call({last_vnode,_Ver},_From, State) ->
    Reply = ok,
    {reply, Reply, State};

handle_call({lookup, Tbl,_AddrId},_From, State) when Tbl /= ?RING_TBL_CUR,
                                                     Tbl /= ?RING_TBL_PREV ->
    {reply, {error, invalid_table}, State};

handle_call({lookup, Tbl, AddrId},_From, State) ->
    #ring_info{routing_table = RoutingTbl,
               first_vnode_id = FirstVNodeId,
               last_vnode_id = LastVNodeId} = ring_info(Tbl, State),
    Reply = lookup_fun(RoutingTbl, FirstVNodeId,
                       LastVNodeId, AddrId, State),
    {reply, Reply, State};

handle_call({first, Tbl},_From, State) when Tbl /= ?RING_TBL_CUR,
                                            Tbl /= ?RING_TBL_PREV ->
    {reply, {error, invalid_table}, State};
handle_call({first, Tbl},_From, State) ->
    #ring_info{routing_table = RoutingTbl,
               first_vnode_id = FirstVNodeId,
               last_vnode_id = LastVNodeId} = ring_info(Tbl, State),
    Reply = lookup_fun(RoutingTbl, FirstVNodeId,
                       LastVNodeId, FirstVNodeId, State),
    {reply, Reply, State};


handle_call({last, Tbl},_From, State) when Tbl /= ?RING_TBL_CUR,
                                           Tbl /= ?RING_TBL_PREV ->
    {reply, {error, invalid_table}, State};
handle_call({last, Tbl},_From, State) ->
    #ring_info{routing_table = RoutingTbl,
               first_vnode_id = FirstVNodeId,
               last_vnode_id = LastVNodeId} = ring_info(Tbl, State),
    Reply = lookup_fun(RoutingTbl, FirstVNodeId,
                       LastVNodeId, LastVNodeId, State),
    {reply, Reply, State};


handle_call({force_sync, Tbl},_From, State) when Tbl /= ?RING_TBL_CUR,
                                                 Tbl /= ?RING_TBL_PREV ->
    {reply, {error, invalid_table}, State};

handle_call({force_sync, Table},_From, State) ->
    TargetRing = ?table_to_sync_target(Table),
    NewState = force_sync_fun(TargetRing, State),
    {reply, ok, NewState};

handle_call({redundancies, TableInfo, AddrId, Members},_From,
            #state{cluster_id = ClusterId,
                   num_of_replicas = N,
                   num_of_rack_awareness = L2} = State) ->
    Reply = redundancies(#redundancy_info{table_info = TableInfo,
                                          cluster_id = ClusterId,
                                          vnode_id = AddrId,
                                          num_of_replicas = N,
                                          rack_awareness_level = L2,
                                          members = Members}),
    {reply, Reply, State};

handle_call({collect, Table, AddrIdAndKey, NumOfReplicas, MaxNumOfDuplicate},
            _From, #state{cluster_id = ClusterId} = State) ->
    RingInfo = ring_info(Table, State),
    Reply = case leo_redundant_manager_api:get_members(ClusterId) of
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

handle_call(dump,_From, #state{cur  = #ring_info{routing_table = CurRoutingTbl},
                               prev = #ring_info{routing_table = PrevRoutingTbl}} = State) ->
    try
        CurRoutingTbl_1 = leo_gb_trees:to_list(CurRoutingTbl),
        PrevRoutingTbl_1 = leo_gb_trees:to_list(PrevRoutingTbl),

        LogDir = ?log_dir(),
        _ = filelib:ensure_dir(LogDir),
        leo_file:file_unconsult(LogDir ++ "ring_cur_worker.log."
                                ++ integer_to_list(leo_date:now()), CurRoutingTbl_1),
        leo_file:file_unconsult(LogDir ++ "ring_prv_worker.log."
                                ++ integer_to_list(leo_date:now()), PrevRoutingTbl_1)
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
update_state(#state{cluster_id = ClusterId} = State) ->
    {ok, Options} = leo_redundant_manager_api:get_options(ClusterId),
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
                    cur = #ring_info{routing_table = CurRoutingTbl},
                    prev = #ring_info{routing_table = PrevRoutingTbl},
                    min_interval = MinInterval,
                    timestamp = Timestamp} = State) ->
    ThisTime = timestamp(),
    sync(),

    case ((ThisTime - Timestamp) < MinInterval) of
        true ->
            State#state{timestamp = ThisTime};
        false ->
            CurHash_Now = erlang:crc32(term_to_binary(CurRoutingTbl)),
            PrevHash_Now = erlang:crc32(term_to_binary(PrevRoutingTbl)),
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
maybe_sync_2(RingVer, Hash_Now, Hash_Old, #state{cluster_id = ClusterId} = State) ->
    {Tbl, Target, RingInfo_1} =
        case RingVer of
            ?RING_TBL_CUR ->
                #state{cur = RingInfo} = State,
                {?MEMBER_TBL_CUR, ?SYNC_TARGET_RING_CUR, RingInfo};
            ?RING_TBL_PREV ->
                #state{prev = RingInfo} = State,
                {?MEMBER_TBL_PREV, ?SYNC_TARGET_RING_PREV, RingInfo}
        end,

    case leo_cluster_tbl_member:find_by_cluster_id(
           Tbl, ClusterId) of
        {ok, Members} ->
            Members_1 = lists:foldl(fun(#?MEMBER{state = ?STATE_ATTACHED}, Acc) ->
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
        {ok, #ring_info{routing_table = VNodeIdTree} = RingInfo} ->
            Checksum = erlang:crc32(
                         term_to_binary(VNodeIdTree)),

            case TargetRing of
                ?SYNC_TARGET_RING_CUR ->
                    {PrevHash,_} = WorkerChecksum,
                    State#state{cur = RingInfo,
                                checksum = {PrevHash, Checksum}};
                ?SYNC_TARGET_RING_PREV ->
                    {_,CurHash} = WorkerChecksum,
                    State#state{prev = RingInfo,
                                checksum = {Checksum, CurHash}}
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
gen_routing_table(#sync_info{target = Target} = SyncInfo,
                  #state{cluster_id = ClusterId} = State) ->
    %% Retrieve ring from local's master [etc|mnesia]
    {ok, Ring_1} = leo_redundant_manager_api:get_ring(
                     ClusterId, ?SYNC_TARGET_RING_CUR),
    RingSize = erlang:length(Ring_1),

    %% Calculate ring's checksum
    Ring_2 = case Target of
                 ?SYNC_TARGET_RING_CUR ->
                     Ring_1;
                 _ ->
                     {ok, RingPrev} = leo_redundant_manager_api:get_ring(
                                        ClusterId, Target),
                     RingPrev
             end,
    Checksum = erlang:crc32(term_to_binary(Ring_2)),

    %% Retrieve redundancies by addr-id
    gen_routing_table_1(Ring_1, leo_gb_trees:empty(),
                        SyncInfo, #ring_conf{id = 0,
                                             ring_size = RingSize,
                                             from_vnode_id = 0,
                                             checksum = Checksum}, State).

%% @private
-spec(gen_routing_table_1(Ring, RoutingTbl,
                          #sync_info{}, #ring_conf{}, #state{}) ->
             {ok, #ring_info{}} | {error, any()} when Ring::[#?RING{}],
                                                      RoutingTbl::leo_gb_trees:tree()).
gen_routing_table_1([], RoutingTbl, #sync_info{target = Target},
                    #ring_conf{from_vnode_id = FromVNodeId,
                               checksum = Checksum},
                    #state{num_of_replicas = NumOfReplicas} = State) ->
    Members = case Target of
                  ?SYNC_TARGET_RING_CUR ->
                      (State#state.cur )#ring_info.members;
                  ?SYNC_TARGET_RING_PREV ->
                      (State#state.prev)#ring_info.members
              end,
    RoutingTbl_1 = leo_gb_trees:balance(RoutingTbl),
    VNodeIdL = leo_gb_trees:to_list(RoutingTbl_1),
    {VNodeIdFirst,_} = leo_gb_trees:lookup(0, RoutingTbl_1),
    VNodeIdLast = FromVNodeId - 1,

    case check_redundancies(NumOfReplicas, VNodeIdL) of
        ok ->
            void;
        _ ->
            timer:apply_after(250, ?MODULE,
                              force_sync, [?sync_target_to_table(Target)])
    end,
    {ok, #ring_info{checksum = Checksum,
                    first_vnode_id = VNodeIdFirst,
                    last_vnode_id = VNodeIdLast,
                    members = Members,
                    routing_table = RoutingTbl_1}};

gen_routing_table_1([#?RING{vnode_id = VNodeId}|Rest],
                    RoutingTbl, SyncInfo,
                    #ring_conf{from_vnode_id = FromVNodeId} = RingConf,
                    #state{cluster_id = ClusterId} = State) ->
    TargetRing = SyncInfo#sync_info.target,
    MembersCur = (State#state.cur)#ring_info.members,
    NumOfReplicas = State#state.num_of_replicas,
    NumOfAwarenessL2 = State#state.num_of_rack_awareness,
    TblInfo = leo_redundant_manager_api:table_info(?VER_CUR),

    case redundancies(#redundancy_info{table_info = TblInfo,
                                       cluster_id = ClusterId,
                                       vnode_id = VNodeId,
                                       num_of_replicas = NumOfReplicas,
                                       rack_awareness_level = NumOfAwarenessL2,
                                       members = MembersCur}) of
        {ok, #redundancies{nodes = CurNodes} = Redundancies} ->
            Redundancies_1 =
                case TargetRing of
                    ?SYNC_TARGET_RING_PREV ->
                        TblInfoPrev = leo_redundant_manager_api:table_info(?VER_PREV),
                        MembersPrev = (State#state.prev)#ring_info.members,

                        case redundancies(#redundancy_info{table_info = TblInfoPrev,
                                                           cluster_id = ClusterId,
                                                           vnode_id = VNodeId,
                                                           num_of_replicas = NumOfReplicas,
                                                           rack_awareness_level = NumOfAwarenessL2,
                                                           members = MembersPrev}) of
                            {ok, #redundancies{nodes = PrevNodes}} ->
                                CurNodes_1  = [_N1 || #redundant_node{node = _N1} <- CurNodes ],
                                PrevNodes_1 = [_N2 || #redundant_node{node = _N2} <- PrevNodes],
                                case lists:subtract(CurNodes_1, PrevNodes_1) of
                                    [] ->
                                        Redundancies;
                                    _Difference ->
                                        Redundancies#redundancies{
                                          nodes = gen_routing_table_2(PrevNodes_1, CurNodes)}
                                end;
                            _ ->
                                Redundancies
                        end;
                    _ ->
                        Redundancies
                end,

            #redundancies{nodes = RedundantNodeL} = Redundancies_1,
            RoutingTbl_1 = leo_gb_trees:insert(
                             VNodeId, #redundancies{id = VNodeId,
                                                    vnode_id_from = FromVNodeId,
                                                    vnode_id_to = VNodeId,
                                                    nodes = RedundantNodeL}, RoutingTbl),
            gen_routing_table_1(Rest, RoutingTbl_1, SyncInfo,
                                RingConf#ring_conf{
                                  from_vnode_id = VNodeId + 1}, State);
        Error ->
            Error
    end.

%% @private
gen_routing_table_2([], Acc) ->
    Acc;
gen_routing_table_2([N|Rest], Acc) ->
    Acc_1 = [_N || #redundant_node{node = _N} <- Acc],
    Acc_2 = case lists:member(N, Acc_1) of
                false ->
                    Acc ++ [#redundant_node{node = N}];
                true ->
                    Acc
            end,
    gen_routing_table_2(Rest, Acc_2).


%% @doc Check to satisfy the number of replicas
%% @private
-spec(check_redundancies(NumOfReplicas, Ring) ->
             ok | {error, any()} when NumOfReplicas::pos_integer(),
                                      Ring::[#ring_group{}]).
check_redundancies(_,[]) ->
    ok;
check_redundancies(NumOfReplicas, [{_,#redundancies{nodes = RedundantNodeL}}|Rest]) ->
    case check_redundancies_1(NumOfReplicas, RedundantNodeL) of
        ok ->
            check_redundancies(NumOfReplicas, Rest);
        {error, Cause} ->
            {error, Cause}
    end.

%% @private
-spec(check_redundancies_1(NumOfReplicas, RedundantNodeL) ->
             ok | {error, invalid_redundancies} when NumOfReplicas::pos_integer(),
                                                     RedundantNodeL::[#redundant_node{}]).
check_redundancies_1(NumOfReplicas, RedundantNodeL) ->
    case (NumOfReplicas =< length(RedundantNodeL)) of
        true ->
            ok;
        false ->
            {error, invalid_redundancies}
    end.


%% @doc get redundancies by key.
%% @private
-spec(redundancies(RedundancyInfo) ->
             {ok, any()} |
             {error, any()} when RedundancyInfo::#redundancy_info{}).

redundancies(#redundancy_info{num_of_replicas = NumOfReplicas})
  when NumOfReplicas < ?DEF_MIN_REPLICAS;
       NumOfReplicas > ?DEF_MAX_REPLICAS ->
    {error, out_of_renge};
redundancies(#redundancy_info{num_of_replicas = NumOfReplicas,
                              rack_awareness_level = L2})
  when (NumOfReplicas - L2) < 1 ->
    {error, invalid_level2};
redundancies(#redundancy_info{table_info = TableInfo,
                              cluster_id = ClusterId,
                              vnode_id = VNodeId} = RedundancyInfo) ->
    case leo_cluster_tbl_ring:lookup(
           TableInfo, ClusterId, VNodeId) of
        {error, Cause} ->
            {error, Cause};
        not_found ->
            case lookup_vnode_id(TableInfo, ClusterId, VNodeId) of
                {ok, VNodeId_1} ->
                    redundancies_1(
                      RedundancyInfo#redundancy_info{vnode_id_hop = VNodeId_1});
                {error, Cause} ->
                    {error, Cause}
            end;

        #?RING{node = Node} ->
            redundancies_2(RedundancyInfo#redundancy_info{node = Node})
    end.

%% @private
redundancies_1(#redundancy_info{table_info = TableInfo,
                                cluster_id = ClusterId,
                                vnode_id_hop = VNodeIdHop} = RedundancyInfo) ->
    case leo_cluster_tbl_ring:lookup(
           TableInfo, ClusterId, VNodeIdHop) of
        {error, Cause} ->
            {error, Cause};
        not_found ->
            {error, ring_not_found};
        #?RING{node = Node} ->
            redundancies_2(
              RedundancyInfo#redundancy_info{node = Node})
    end.

%% @private
redundancies_2(#redundancy_info{num_of_replicas = NumOfReplicas,
                                members = Members,
                                node = Node,
                                vnode_id = VNodeId,
                                vnode_id_hop = VNodeIdHop} = RedundancyInfo) ->
    case get_redundancies(Members, Node, []) of
        not_found ->
            {error, ?ERROR_COULD_NOT_GET_REDUNDANCIES};
        {Node, SetsL2} ->
            redundancies_3(RedundancyInfo#redundancy_info{
                             num_of_replicas = (NumOfReplicas - 1),
                             result = #redundancies{
                                         id = VNodeId,
                                         vnode_id_to = VNodeIdHop,
                                         temp_nodes = [Node],
                                         temp_level_2 = SetsL2,
                                         nodes = [#redundant_node{node = Node}]}})
    end.

%% @private
redundancies_3(#redundancy_info{vnode_id = -1}) ->
    {error,  invalid_vnode};
redundancies_3(#redundancy_info{num_of_replicas = 0,
                                result = R}) ->
    #redundancies{nodes = Acc} = R,
    {ok, R#redundancies{temp_nodes = [],
                        temp_level_2 = [],
                        nodes = lists:reverse(Acc)}};

redundancies_3(#redundancy_info{table_info = TableInfo,
                                cluster_id = ClusterId,
                                vnode_id = VNodeId} = RedundancyInfo) ->
    case lookup_vnode_id(TableInfo, ClusterId, VNodeId) of
        {ok, VNodeId_1} ->
            case leo_cluster_tbl_ring:lookup(
                   TableInfo, ClusterId, VNodeId_1) of
                {error, Cause} ->
                    {error, Cause};
                not_found ->
                    case lookup_vnode_id(
                           TableInfo, ClusterId, VNodeId_1) of
                        {ok, Node} ->
                            redundancies_4(
                              RedundancyInfo#redundancy_info{node = Node,
                                                             vnode_id = VNodeId_1});
                        {error, Cause} ->
                            {error, Cause}
                    end;
                #?RING{node = Node} ->
                    redundancies_4(
                      RedundancyInfo#redundancy_info{node = Node,
                                                     vnode_id = VNodeId_1})
            end;
        _ ->
            {error, out_of_range}
    end.

%% @private
redundancies_4(#redundancy_info{node = Node,
                                members = Members,
                                num_of_replicas = NumOfReplicas,
                                rack_awareness_level = L2,
                                result = R} = RedundancyInfo) ->
    #redundancies{temp_nodes = AccTempNode,
                  temp_level_2 = AccLevel2,
                  nodes = AccNodes} = R,

    case lists:member(Node, AccTempNode) of
        true  ->
            redundancies_3(RedundancyInfo);
        false ->
            case get_redundancies(Members, Node, AccLevel2) of
                not_found ->
                    {error, ?ERROR_COULD_NOT_GET_REDUNDANCIES};
                {Node_2, AccLevel2_1} ->
                    case (L2 /= 0 andalso L2 == length(AccNodes)) of
                        true when length(AccLevel2_1) < (L2 + 1) ->
                            redundancies_3(RedundancyInfo);
                        _ ->
                            redundancies_3(RedundancyInfo#redundancy_info{
                                             num_of_replicas = (NumOfReplicas - 1),
                                             result = R#redundancies{
                                                        temp_nodes = [Node_2|AccTempNode],
                                                        temp_level_2 = AccLevel2_1,
                                                        nodes = [#redundant_node{node = Node_2}
                                                                 |AccNodes]}
                                            })
                    end
            end
    end.


%% @doc Retrieve vnode-id
%% @private
-spec(lookup_vnode_id(TableInfo, ClusterId, VNodeId) ->
             {ok, integer()} |
             {error, any()} when TableInfo::ring_table_info(),
                                 ClusterId::cluster_id(),
                                 VNodeId::integer()).
lookup_vnode_id(TableInfo, ClusterId, VNodeId) ->
    %% @TODO
    case leo_cluster_tbl_ring:next(TableInfo, ClusterId, VNodeId) of
        '$end_of_table' ->
            %% @TODO
            case leo_cluster_tbl_ring:first(TableInfo, ClusterId) of
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
-spec(get_redundancies([#?MEMBER{}], atom(), [string()]) ->
             {atom, [string()]} | not_found).
get_redundancies([],_,_) ->
    not_found;
get_redundancies([#?MEMBER{node = Node_0,
                           grp_level_2 = L2}|_], Node_1, SetL2) when Node_0 == Node_1 ->
    case lists:member(L2, SetL2) of
        false  ->
            {Node_0, [L2|SetL2]};
        _Other ->
            {Node_0, SetL2}
    end;
get_redundancies([#?MEMBER{node = Node_0}|T], Node_1, SetL2) when Node_0 /= Node_1 ->
    get_redundancies(T, Node_1, SetL2).


%% @doc Reply redundancies
%% @private
-spec(reply_redundancies(AddrId, Redundancies, State) ->
             not_found | {ok, #redundancies{}} when AddrId::non_neg_integer(),
                                                    Redundancies::#redundancies{},
                                                    State::#state{}).
reply_redundancies(AddrId, #redundancies{nodes = RedundantNodeL} = Redundancies,
                   #state{cluster_id = ClusterId,
                          num_of_replicas = NumOfReplicas}) ->
    reply_redundancies_1(ClusterId, AddrId, Redundancies,
                         RedundantNodeL, NumOfReplicas, 0, []).

%% @private
reply_redundancies_1(_,AddrId, Redundancies,[],_,_,Acc) ->
    {ok, Redundancies#redundancies{id = AddrId,
                                   nodes = lists:reverse(Acc)}};
reply_redundancies_1(ClusterId, AddrId, Redundancies,
                     [#redundant_node{node = Node}|Rest],
                     NumOfReplicas, Index, Acc) ->
    NextIndex = Index + 1,
    case leo_cluster_tbl_member:lookup(ClusterId, Node) of
        {ok, #?MEMBER{state = State}} ->
            Available = (State == ?STATE_RUNNING),
            CanReadRepair = (NumOfReplicas >= (length(Acc) + 1)),
            ConsensusRole = case Index of
                                0 ->
                                    ?CNS_ROLE_LEADER;
                                _ when CanReadRepair == true ->
                                    ?CNS_ROLE_FOLLOWER_1;
                                _ when CanReadRepair /= true ->
                                    ?CNS_ROLE_OBSERBER
                            end,
            reply_redundancies_1(ClusterId, AddrId, Redundancies,
                                 Rest, NumOfReplicas, NextIndex,
                                 [#redundant_node{node = Node,
                                                  available = Available,
                                                  can_read_repair = CanReadRepair,
                                                  role = ConsensusRole}|Acc]);
        _ ->
            reply_redundancies_1(ClusterId, AddrId, Redundancies,
                                 Rest, NumOfReplicas, NextIndex, Acc)
    end.


%% @doc Retrieve redundancies by vnode-id
%% @private
-spec(lookup_fun(RoutingTbl, FirstVNodeId, LastVNodeId, AddrId, State) ->
             not_found | {ok, #redundancies{}} when RoutingTbl::leo_gb_trees:tree(),
                                                    FirstVNodeId::non_neg_integer(),
                                                    LastVNodeId::non_neg_integer(),
                                                    AddrId::non_neg_integer(),
                                                    State::#state{}).
lookup_fun([],_,_,_,_) ->
    not_found;
lookup_fun(VNodeIdTree, FirstVNodeId, LastVNodeId, AddrId, State) ->
    AddrId_1 = case (FirstVNodeId > AddrId) of
                   true ->
                       FirstVNodeId;
                   false when LastVNodeId < AddrId ->
                       FirstVNodeId;
                   false ->
                       AddrId
               end,

    case leo_gb_trees:lookup(AddrId_1, VNodeIdTree) of
        nil ->
            {error, ?ERROR_COULD_NOT_GET_REDUNDANCIES};
        {_VNodeId, Redundancies} ->
            reply_redundancies(AddrId, Redundancies, State)
    end.


%% @doc Force sync
%% @private
-spec(force_sync_fun(?SYNC_TARGET_RING_CUR|
                     ?SYNC_TARGET_RING_PREV, #state{}) ->
             #state{}).
force_sync_fun(TargetRing, #state{cluster_id = ClusterId} = State) ->
    case update_state(State) of
        #state{num_of_replicas = NumOfReplica} = State_1 when NumOfReplica > 0 ->
            %% @TODO > include
            Tbl = case TargetRing of
                      ?SYNC_TARGET_RING_CUR ->
                          ?MEMBER_TBL_CUR;
                      ?SYNC_TARGET_RING_PREV ->
                          ?MEMBER_TBL_PREV
                  end,
            case leo_cluster_tbl_member:find_by_cluster_id(
                   Tbl, ClusterId) of
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
force_sync_fun_1({ok, #ring_info{routing_table = RetL} = RingInfo}, ?SYNC_TARGET_RING_CUR,
                 #state{checksum = Checksum} = State) ->
    {PrevHash,_} = Checksum,
    CurHash = erlang:crc32(term_to_binary(RetL)),
    State#state{cur = RingInfo,
                checksum = {PrevHash, CurHash}};
force_sync_fun_1({ok, #ring_info{routing_table = RetL} = RingInfo}, ?SYNC_TARGET_RING_PREV,
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
collect_fun(NumOfReplicas, #ring_info{routing_table = VNodeIdTree,
                                      first_vnode_id = FirstVNodeId,
                                      last_vnode_id = LastVNodeId} = RingInfo,
            {AddrId, Key}, TotalMembers, MaxNumOfDuplicate, State, Acc) ->
    case lookup_fun(VNodeIdTree, FirstVNodeId,
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
