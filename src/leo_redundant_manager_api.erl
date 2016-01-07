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
%% ---------------------------------------------------------------------
%% Leo Redundant Manager - API
%%
%% @doc leo_redaundant_manager's API
%% @reference https://github.com/leo-project/leo_redundant_manager/blob/master/src/leo_redundant_manager_api.erl
%% @end
%%======================================================================
-module(leo_redundant_manager_api).

-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").
-include_lib("leo_rpc/include/leo_rpc.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Ring-related
-export([create/0, create/1, create/2, create/3,
         set_options/1, get_options/0, get_option/1,
         attach/1, attach/2, attach/3, attach/4, attach/5,
         reserve/3, reserve/5, reserve/6,
         detach/1, detach/2,
         suspend/1, suspend/2,
         checksum/1, synchronize/2, synchronize/3,
         get_ring/0, get_ring/1, dump/1
        ]).
%% Redundancy-related
-export([get_redundancies_by_key/1, get_redundancies_by_key/2,
         get_redundancies_by_addr_id/1, get_redundancies_by_addr_id/2,
         collect_redundancies_by_key/3,
         part_of_collect_redundancies_by_key/4,
         range_of_vnodes/1, rebalance/0,
         get_alias/2, get_alias/3, get_alias/4
        ]).
%% Member-related
-export([has_member/1, has_charge_of_node/2,
         get_members/0, get_members/1, get_all_ver_members/0,
         get_member_by_node/1, get_members_count/0,
         get_members_by_status/1, get_members_by_status/2,
         update_member/1, update_members/1, update_member_by_node/2, update_member_by_node/3,
         delete_member_by_node/1, is_alive/0, table_info/1,
         force_sync_workers/0,
         get_cluster_status/0,
         get_cluster_tbl_checksums/0
        ]).

%% Multi-DC-replciation-related
-export([get_remote_clusters/0, get_remote_clusters/1,
         get_remote_members/1, get_remote_members/2
        ]).

%% Request type
-type(method() :: put | get | delete | head | default).

%%--------------------------------------------------------------------
%% API-1  FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Create the RING
%%
-spec(create() ->
             {ok, Members, HashValues} |
             {error, any()} when Members::[#member{}],
                                 HashValues::[{atom(), integer()}]).
create() ->
    case create(?VER_CUR) of
        {ok, Members, HashValues} ->
            Ret = case leo_cluster_tbl_member:table_size(?MEMBER_TBL_PREV) of
                      0 ->
                          create_1();
                      _ ->
                          case create(?VER_PREV) of
                              {ok,_,_} ->
                                  ok;
                              Error_1 ->
                                  Error_1
                          end
                  end,

            case Ret of
                ok ->
                    {ok, Members, HashValues};
                Error_2 ->
                    Error_2
            end;
        Error ->
            Error
    end.

-spec(create(Ver) ->
             {ok, Members, HashValues} |
             {error, any()} when Ver::?VER_CUR|?VER_PREV,
                                 Members::[#member{}],
                                 HashValues::[{atom(), integer()}]).
create(Ver) when Ver == ?VER_CUR;
                 Ver == ?VER_PREV ->
    case get_option(?PROP_N) of
        0 ->
            {error, ?ERROR_INVALID_CONF};
        _N ->
            case leo_redundant_manager:create(Ver) of
                ok ->
                    case leo_cluster_tbl_member:find_all(?member_table(Ver)) of
                        {ok, Members} ->
                            spawn(
                              fun() ->
                                      ok = leo_redundant_manager_worker:force_sync(?ring_table(Ver))
                              end),
                            {ok, HashRing} = checksum(?CHECKSUM_RING),
                            ok = leo_misc:set_env(?APP, ?PROP_RING_HASH,
                                                  erlang:element(1, HashRing)),
                            {ok, HashMember} = checksum(?CHECKSUM_MEMBER),
                            {ok, Members, [{?CHECKSUM_RING,   HashRing},
                                           {?CHECKSUM_MEMBER, HashMember}]};
                        Error ->
                            Error
                    end;
                Error ->
                    Error
            end
    end;
create(_) ->
    {error, invlid_version}.


-spec(create(Ver, Members) ->
             {ok, Members, HashValues} |
             {error, any()} when Ver::?VER_CUR|?VER_PREV,
                                 Members::[#member{}],
                                 HashValues::[{atom(), integer()}]).
create(Ver, Members) ->
    create(Ver, Members, []).

-spec(create(Ver, Members, Options) ->
             {ok, Members, HashValues} |
             {error, any()} when Ver::?VER_CUR|?VER_PREV,
                                 Members::[#member{}],
                                 Options::[{atom(), any()}],
                                 HashValues::[{atom(), integer()}]).
create(Ver, [], []) ->
    create(Ver);
create(Ver, [], Options) ->
    ok = set_options(Options),
    create(Ver);
create(Ver, [#member{node = Node} = Member|T], Options) when Ver == ?VER_CUR;
                                                             Ver == ?VER_PREV ->
    %% Add a member as "attached node" into member-table
    case leo_cluster_tbl_member:lookup(Node) of
        not_found ->
            Prop = {Node, Member#member{state = ?STATE_ATTACHED}},
            leo_cluster_tbl_member:insert(Prop);
        _ ->
            void
    end,
    create(Ver, T, Options);
create(_,_,_) ->
    {error, invalid_version}.


%% @private
create_1() ->
    case leo_cluster_tbl_member:overwrite(
           ?MEMBER_TBL_CUR, ?MEMBER_TBL_PREV) of
        ok ->
            PrevRingTbl = table_info(?VER_PREV),
            CurRingTbl  = table_info(?VER_CUR),
            case leo_cluster_tbl_ring:overwrite(
                   CurRingTbl, PrevRingTbl) of
                ok ->
                    ok;
                Error ->
                    Error
            end;
        Error ->
            Error
    end.



%% @doc set routing-table's options.
%%
-spec(set_options(Options) ->
             ok when Options::[{atom(), any()}]).
set_options(Options) ->
    ok = leo_misc:set_env(?APP, ?PROP_OPTIONS, Options),
    ok.


%% @doc get routing-table's options.
%%
-spec(get_options() ->
             {ok, Options} when Options::[{atom(), any()}]).
get_options() ->
    case leo_misc:get_env(?APP, ?PROP_OPTIONS) of
        undefined ->
            case catch leo_cluster_tbl_conf:get() of
                {ok, #?SYSTEM_CONF{} = SystemConf} ->
                    Options = record_to_tuplelist(SystemConf),
                    ok = set_options(Options),
                    {ok, Options};
                _ ->
                    Options = record_to_tuplelist(
                                #?SYSTEM_CONF{}),
                    {ok, Options}
            end;
        Ret ->
            Ret
    end.

%% @doc get routing-table's options.
%%
-spec(get_option(Item) ->
             Value when Item::atom(),
                        Value::any()).
get_option(Item) ->
    {ok, Options} = get_options(),
    leo_misc:get_value(Item, Options, 0).


%% @doc record to tuple-list for converting system-conf
%% @private
-spec(record_to_tuplelist(Value) ->
             [{atom(), any()}] when Value::any()).
record_to_tuplelist(Value) ->
    lists:zip(
      record_info(fields, ?SYSTEM_CONF), tl(tuple_to_list(Value))).


%% @doc attach a node.
%%
-spec(attach(Node) ->
             ok | {error, any()} when Node::atom()).
attach(Node) ->
    attach(Node, [], leo_date:clock()).

-spec(attach(Node, AwarenessL2) ->
             ok | {error, any()} when Node::atom(),
                                      AwarenessL2::string()).
attach(Node, AwarenessL2) ->
    attach(Node, AwarenessL2, leo_date:clock()).

-spec(attach(Node, AwarenessL2, Clock) ->
             ok | {error, any()} when Node::atom(),
                                      AwarenessL2::string(),
                                      Clock::integer()).
attach(Node, AwarenessL2, Clock) ->
    attach(Node, AwarenessL2, Clock, ?DEF_NUMBER_OF_VNODES).

-spec(attach(Node, AwarenessL2, Clock, NumOfVNodes) ->
             ok | {error, any()} when Node::atom(),
                                      AwarenessL2::string(),
                                      Clock::integer(),
                                      NumOfVNodes::integer()).
attach(Node, AwarenessL2, Clock, NumOfVNodes) ->
    attach(Node, AwarenessL2, Clock, NumOfVNodes, ?DEF_LISTEN_PORT).

-spec(attach(Node, AwarenessL2, Clock, NumOfVNodes, RPCPort) ->
             ok | {error, any()} when Node::atom(),
                                      AwarenessL2::string(),
                                      Clock::integer(),
                                      NumOfVNodes::integer(),
                                      RPCPort::integer()).
attach(Node, AwarenessL2, Clock, NumOfVNodes, RPCPort) ->
    case leo_redundant_manager:attach(
           Node, AwarenessL2, Clock, NumOfVNodes, RPCPort) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc reserve a node during in operation
%%
-spec(reserve(Node, CurState, Clock) ->
             ok | {error, any()} when Node::atom(),
                                      CurState::atom(),
                                      Clock::integer()).
reserve(Node, CurState, Clock) ->
    reserve(Node, CurState, "", Clock, 0).

-spec(reserve(Node,CurState, AwarenessL2, Clock, NumOfVNodes) ->
             ok | {error, any()} when Node::atom(),
                                      CurState::atom(),
                                      AwarenessL2::string(),
                                      Clock::integer(),
                                      NumOfVNodes::integer()).
reserve(Node, CurState, AwarenessL2, Clock, NumOfVNodes) ->
    reserve(Node, CurState, AwarenessL2, Clock, NumOfVNodes, ?DEF_LISTEN_PORT).

-spec(reserve(Node,CurState, AwarenessL2, Clock, NumOfVNodes, RPCPort) ->
             ok | {error, any()} when Node::atom(),
                                      CurState::atom(),
                                      AwarenessL2::string(),
                                      Clock::integer(),
                                      NumOfVNodes::integer(),
                                      RPCPort::integer()).
reserve(Node, CurState, AwarenessL2, Clock, NumOfVNodes, RPCPort) ->
    case leo_redundant_manager:reserve(
           Node, CurState, AwarenessL2, Clock, NumOfVNodes, RPCPort) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc detach a node.
%%
-spec(detach(Node) ->
             ok | {error, any()} when Node::atom()).
detach(Node) ->
    detach(Node, leo_date:clock()).

-spec(detach(Node, Clock) ->
             ok | {error, any()} when Node::atom(),
                                      Clock::integer()).
detach(Node, Clock) ->
    case leo_redundant_manager:detach(Node, Clock) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc suspend a node. (disable)
%%
-spec(suspend(Node) ->
             ok | {error, any()} when Node::atom()).
suspend(Node) ->
    suspend(Node, leo_date:clock()).

-spec(suspend(Node, Clock) ->
             ok | {error, any()} when Node::atom(),
                                      Clock::integer()).
suspend(Node, Clock) ->
    case leo_redundant_manager:suspend(Node, Clock) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc get routing_table's checksum.
%%
-spec(checksum(Type) ->
             {ok, integer()} |
             {ok, {integer(), integer()}} |
             {error, any()} when Type::checksum_type()).
checksum(?CHECKSUM_MEMBER = Type) ->
    leo_redundant_manager:checksum(Type);
checksum(?CHECKSUM_RING) ->
    TblInfoCur  = table_info(?VER_CUR),
    TblInfoPrev = table_info(?VER_PREV),
    {ok, RingHashCur } = leo_redundant_manager_chash:checksum(TblInfoCur),
    {ok, RingHashPrev} = leo_redundant_manager_chash:checksum(TblInfoPrev),
    {ok, {RingHashCur, RingHashPrev}};
checksum(?CHECKSUM_WORKER) ->
    leo_redundant_manager_worker:checksum();
checksum(?CHECKSUM_SYS_CONF) ->
    {ok, SysConf} = get_options(),
    {ok, erlang:crc32(term_to_binary(SysConf))};
checksum(_) ->
    {error, invalid_type}.


%% @doc synchronize member-list and routing-table.
%%
-spec(synchronize(SyncTarget, SyncData, Options) ->
             {ok, [{atom(), any()}]} |
             {error, any()} when SyncTarget::sync_target(),
                                 SyncData::[{atom(), any()}],
                                 Options::[{atom(), any()}]).
synchronize(?SYNC_TARGET_BOTH, SyncData, Options) ->
    %% set configurations
    case Options of
        [] -> void;
        _ ->
            ok = set_options(Options)
    end,

    %% Synchronize current and previous members
    %%   Then Synchronize ring
    case synchronize(?SYNC_TARGET_MEMBER, SyncData) of
        {ok, ChecksumMembers} ->
            case synchronize_1(?SYNC_TARGET_RING_CUR,  ?VER_CUR) of
                ok ->
                    case synchronize_1(?SYNC_TARGET_RING_PREV, ?VER_PREV) of
                        ok ->
                            {ok, ChecksumRing} = checksum(?CHECKSUM_RING),
                            {ok, [{?CHECKSUM_MEMBER, ChecksumMembers},
                                  {?CHECKSUM_RING,   ChecksumRing}
                                 ]};
                        Error ->
                            Error
                    end;
                Error ->
                    Error
            end;
        Error ->
            Error
    end.


%% @doc synchronize member-list and routing-table.
%%
-spec(synchronize(SyncTarget, SyncData) ->
             {ok, integer()} |
             {ok, [{atom(), any()}]} |
             {error, any()} when SyncTarget::sync_target(),
                                 SyncData::[{atom(), any()}]).
synchronize(?SYNC_TARGET_BOTH, SyncData) ->
    synchronize(?SYNC_TARGET_BOTH, SyncData, []);

synchronize(?SYNC_TARGET_MEMBER = SyncTarget, SyncData) ->
    case synchronize_1(SyncTarget, ?VER_CUR,  SyncData) of
        ok ->
            case synchronize_1(SyncTarget, ?VER_PREV, SyncData) of
                ok ->
                    checksum(?CHECKSUM_MEMBER);
                Error ->
                    Error
            end;
        Error ->
            Error
    end;

synchronize(Target, []) when ?SYNC_TARGET_RING_CUR  == Target;
                             ?SYNC_TARGET_RING_PREV == Target ->
    synchronize_1(Target, ?sync_target_to_ver(Target));

synchronize(Target, SyncData) when ?SYNC_TARGET_RING_CUR  == Target;
                                   ?SYNC_TARGET_RING_PREV == Target ->
    {ok, ChecksumMembers} = synchronize(?SYNC_TARGET_MEMBER, SyncData),
    case synchronize_1(Target, ?sync_target_to_ver(Target)) of
        ok ->
            {ok, ChecksumRing} = checksum(?CHECKSUM_RING),
            {ok, [{?CHECKSUM_MEMBER, ChecksumMembers},
                  {?CHECKSUM_RING,   ChecksumRing}
                 ]};
        Error ->
            Error
    end;
synchronize(_,_) ->
    {error, invalid_target}.


%% @private
synchronize_1(?SYNC_TARGET_MEMBER, Ver, SyncData) ->
    case leo_misc:get_value(Ver, SyncData, []) of
        [] ->
            ok;
        NewMembers ->
            Table = ?member_table(Ver),
            case leo_cluster_tbl_member:find_all(Table) of
                {ok, OldMembers} ->
                    leo_redundant_manager:update_members(Table, OldMembers, NewMembers);
                not_found ->
                    leo_redundant_manager:update_members(Table, [], NewMembers);
                Error ->
                    Error
            end
    end.

%% @private
synchronize_1(Target, Ver) when Target == ?SYNC_TARGET_RING_CUR;
                                Target == ?SYNC_TARGET_RING_PREV ->
    TableInfo = table_info(Ver),
    case leo_cluster_tbl_ring:tab2list(TableInfo) of
        {error, Cause} ->
            {error, Cause};
        CurRing ->
            case CurRing of
                [] ->
                    void;
                _  ->
                    ok = leo_cluster_tbl_ring:delete_all(TableInfo)
            end,

            MemberTbl = case Target of
                            ?SYNC_TARGET_RING_CUR ->
                                ?MEMBER_TBL_CUR;
                            ?SYNC_TARGET_RING_PREV ->
                                ?MEMBER_TBL_PREV
                        end,

            case leo_cluster_tbl_member:find_all(MemberTbl) of
                {ok, Members} when length(Members) > 0 ->
                    case create(Ver) of
                        {ok,_Members,_HashVals} ->
                            ok;
                        Error when CurRing /= [] ->
                            [leo_cluster_tbl_ring:insert(
                               table_info(Ver), R) || R <- CurRing],
                            Error;
                        Error ->
                            Error
                    end;
                _ ->
                    {error, ?ERROR_COULD_NOT_GET_MEMBERS}
            end
    end;
synchronize_1(_,_) ->
    {error, invalid_target}.


%% @doc Retrieve Ring
%%
-spec(get_ring() ->
             {ok, [tuple()]}).
get_ring() ->
    {ok, ets:tab2list(?RING_TBL_CUR)}.

-spec(get_ring(SyncTarget) ->
             {ok, [tuple()]} when SyncTarget::sync_target()).
get_ring(?SYNC_TARGET_RING_CUR) ->
    TblInfo = table_info(?VER_CUR),
    Ring = leo_cluster_tbl_ring:tab2list(TblInfo),
    {ok, Ring};
get_ring(?SYNC_TARGET_RING_PREV) ->
    TblInfo = table_info(?VER_PREV),
    Ring = leo_cluster_tbl_ring:tab2list(TblInfo),
    {ok, Ring}.


%% @doc Dump table-records.
%%
-spec(dump(Type) ->
             ok when Type::member|ring|both|work).
dump(both) ->
    catch dump(member),
    catch dump(ring),
    catch dump(work),
    ok;
dump(work) ->
    dump_1(?RING_WORKER_POOL_SIZE - 1);
dump(Type) ->
    leo_redundant_manager:dump(Type).

%% @private
dump_1(-1) ->
    ok;
dump_1(Index) ->
    ok = leo_redundant_manager_worker:dump(),
    dump_1(Index - 1).


%%--------------------------------------------------------------------
%% API-2  FUNCTIONS (leo_routing_tbl_provide_server)
%%--------------------------------------------------------------------
%% @doc Retrieve redundancies from the ring-table.
%%
-spec(get_redundancies_by_key(Key) ->
             {ok, #redundancies{}} |
             {error, any()} when Key::binary()).
get_redundancies_by_key(Key) ->
    get_redundancies_by_key(default, Key).

-spec(get_redundancies_by_key(Method, Key) ->
             {ok, #redundancies{}} |
             {error, any()} when Method::method(),
                                 Key::binary()).
get_redundancies_by_key(Method, Key) ->
    {ok, Options} = get_options(),
    BitOfRing = leo_misc:get_value(?PROP_RING_BIT, Options),
    AddrId = leo_redundant_manager_chash:vnode_id(BitOfRing, Key),
    get_redundancies_by_addr_id_1(ring_table(Method), AddrId, Options).


%% @doc Retrieve redundancies from the ring-table.
%%
-spec(get_redundancies_by_addr_id(AddrId) ->
             {ok, #redundancies{}} |
             {error, any()} when AddrId::integer()).
get_redundancies_by_addr_id(AddrId) ->
    get_redundancies_by_addr_id(default, AddrId).

-spec(get_redundancies_by_addr_id(Method, AddrId) ->
             {ok, #redundancies{}} |
             {error, any()} when Method::method(), AddrId::integer()).
get_redundancies_by_addr_id(Method, AddrId) ->
    {ok, Options} = get_options(),
    get_redundancies_by_addr_id_1(ring_table(Method), AddrId, Options).

%% @private
-spec(get_redundancies_by_addr_id_1({_,atom()}, integer(), [_]) ->
             {ok, #redundancies{}} | {error, any()}).
get_redundancies_by_addr_id_1({_,Tbl}, AddrId, Options) ->
    N = leo_misc:get_value(?PROP_N, Options),
    R = leo_misc:get_value(?PROP_R, Options),
    W = leo_misc:get_value(?PROP_W, Options),
    D = leo_misc:get_value(?PROP_D, Options),

    case leo_redundant_manager_worker:lookup(Tbl, AddrId) of
        {ok, Redundancies} ->
            CurRingHash =
                case leo_misc:get_env(?APP, ?PROP_RING_HASH) of
                    {ok, RingHash} ->
                        RingHash;
                    undefined ->
                        {ok, {RingHash, _}} = checksum(?CHECKSUM_RING),
                        ok = leo_misc:set_env(?APP, ?PROP_RING_HASH, RingHash),
                        RingHash
                end,
            {ok, Redundancies#redundancies{n = N,
                                           r = R,
                                           w = W,
                                           d = D,
                                           ring_hash = CurRingHash}};
        not_found = Cause ->
            {error, Cause}
    end.


%% @doc Collect redundant nodes for LeoFS' erasure-coding
-spec(collect_redundancies_by_key(Key, NumOfReplicas, MaxNumOfDuplicate) ->
             {ok, {Options, RedundantNodeL}}|{error, any()}
                 when Key::binary(),
                      NumOfReplicas::pos_integer(),
                      MaxNumOfDuplicate::pos_integer(),
                      Options::[{atom(), any()}],
                      RedundantNodeL::[#redundancies{}]).
collect_redundancies_by_key(Key, NumOfReplicas, MaxNumOfDuplicate) ->
    {_, Table} = ring_table(default),
    {ok, Options} = get_options(),
    BitOfRing = leo_misc:get_value(?PROP_RING_BIT, Options),
    AddrId = leo_redundant_manager_chash:vnode_id(BitOfRing, Key),

    case leo_redundant_manager_worker:collect(
           Table, {AddrId, Key}, NumOfReplicas, MaxNumOfDuplicate) of
        {ok, RedundantNodeL} ->
            {ok, {Options, RedundantNodeL}};
        not_found = Cause ->
            {error, Cause};
        Others ->
            Others
    end.


-spec(part_of_collect_redundancies_by_key(Index, ChildKey, NumOfReplicas, MaxNumOfDuplicate) ->
             {ok, RedundantNode}|{error, any()}
                 when Index::pos_integer(),
                      ChildKey::binary(),
                      NumOfReplicas::pos_integer(),
                      MaxNumOfDuplicate::pos_integer(),
                      RedundantNode::#redundant_node{}).
part_of_collect_redundancies_by_key(Index, ChildKey, NumOfReplicas, MaxNumOfDuplicate) ->
    ParentKey = begin
                    case binary:matches(ChildKey, [<<"\n">>], []) of
                        [] ->
                            ChildKey;
                        PosL ->
                            {Pos,_} = lists:last(PosL),
                            binary:part(ChildKey, 0, Pos)
                    end
                end,
    case collect_redundancies_by_key(ParentKey, NumOfReplicas, MaxNumOfDuplicate) of
        {ok, {_Options, RedundantNodeL}}
          when erlang:length(RedundantNodeL) >= NumOfReplicas ->
            {ok, lists:nth(Index, RedundantNodeL)};
        Other ->
            Other
    end.


%% @doc Retrieve range of vnodes.
%%
-spec(range_of_vnodes(ToVNodeId) ->
             {ok, [tuple()]} when ToVNodeId::integer()).
range_of_vnodes(ToVNodeId) ->
    TblInfo = table_info(?VER_CUR),
    leo_redundant_manager_chash:range_of_vnodes(TblInfo, ToVNodeId).


%% @doc Re-balance objects in the cluster.
%%
-spec(rebalance() ->
             {ok, [tuple()]} | {error, any()}).
rebalance() ->
    case leo_cluster_tbl_member:find_all(?MEMBER_TBL_CUR) of
        {ok, MembersCur} ->
            %% Before exec rebalance
            case before_rebalance(MembersCur) of
                {ok, {MembersCur_1, MembersPrev, TakeOverList}} ->
                    %% Exec rebalance
                    {ok, Ret} = leo_redundant_manager_chash:rebalance(
                                  #rebalance{tbl_cur  = table_info(?VER_CUR),
                                             tbl_prev = table_info(?VER_PREV),
                                             members_cur  = MembersCur_1,
                                             members_prev = MembersPrev}),
                    ok = after_rebalance(TakeOverList),
                    {ok, Ret};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.


%% @doc Before execute rebalance:
%%      1. Update current-members when included status of 'attached' and 'detached'
%%      2. Retrieve status of takeover
%%      3. Update previous-members from current-members
%% @private
before_rebalance(MembersCur) ->
    %% If "attach" and "detach" are included in members,
    %% then update current-members
    %% because attach-node need to take over detach-node's data.
    case takeover_status(MembersCur, []) of
        {ok, {MembersCur_1, TakeOverList}} ->
            %% Remove all previous members,
            %% Then insert new members from current members
            case leo_cluster_tbl_member:delete_all(?MEMBER_TBL_PREV) of
                ok ->
                    case before_rebalance_1(MembersCur_1) of
                        {ok, MembersPrev} ->
                            {ok, {MembersCur_1, MembersPrev, TakeOverList}};
                        Error ->
                            Error
                    end;
                Error ->
                    Error
            end;
        Error ->
            Error
    end.


%% @private
takeover_status([], TakeOverList) ->
    case leo_cluster_tbl_member:find_all(?MEMBER_TBL_CUR) of
        {ok, MembersCur} ->
            {ok, {MembersCur, TakeOverList}};
        Error ->
            Error
    end;
takeover_status([#member{state = ?STATE_ATTACHED,
                         node  = Node,
                         alias = Alias,
                         grp_level_2 = GrpL2} = Member|Rest], TakeOverList) ->
    case get_alias(Node, GrpL2) of
        {ok, {SrcMember, Alias_1}} when Alias /= Alias_1 ->
            %% Takeover vnodes:
            %%     Remove vnodes by old-alias,
            %%     then insert vnodes by new-alias
            RingTblCur = table_info(?VER_CUR),
            Member_1 = Member#member{alias = Alias_1},

            ok = leo_redundant_manager_chash:remove(RingTblCur, Member),
            ok = leo_redundant_manager_chash:add(RingTblCur, Member_1),
            ok = leo_cluster_tbl_member:insert(?MEMBER_TBL_CUR, {Node, Member_1}),

            case SrcMember of
                [] -> void;
                #member{node = SrcNode} ->
                    ok = leo_cluster_tbl_member:insert(
                           ?MEMBER_TBL_CUR, {SrcNode, SrcMember#member{alias = []}})
            end,
            takeover_status(Rest, [{Member, Member_1, SrcMember}|TakeOverList]);
        _ ->
            takeover_status(Rest, TakeOverList)
    end;
takeover_status([_|Rest], TakeOverList) ->
    takeover_status(Rest, TakeOverList).


%% @private
before_rebalance_1([]) ->
    %% Synchronize previous-ring
    case synchronize_1(?SYNC_TARGET_RING_PREV, ?VER_PREV) of
        ok -> void;
        {error, Reason} ->
            error_logger:warning_msg("~p,~p,~p,~p~n",
                                     [{module, ?MODULE_STRING},
                                      {function, "after_rebalance_1/0"},
                                      {line, ?LINE},
                                      {body, Reason}])
    end,

    case leo_cluster_tbl_member:find_all(?MEMBER_TBL_PREV) of
        {ok, MembersPrev} ->
            {ok, MembersPrev};
        Error ->
            Error
    end;
before_rebalance_1([#member{state = ?STATE_ATTACHED}|Rest]) ->
    before_rebalance_1(Rest);
before_rebalance_1([#member{state = ?STATE_RESERVED}|Rest]) ->
    before_rebalance_1(Rest);
before_rebalance_1([#member{node = Node} = Member|Rest]) ->
    case leo_cluster_tbl_member:insert(?MEMBER_TBL_PREV,
                                       {Node, Member#member{state = ?STATE_RUNNING}}) of
        ok ->
            before_rebalance_1(Rest);
        Error ->
            Error
    end.


%% @doc After execute rebalance#2:
%%      1. After exec taking over data from detach-node to attach-node
%%      2. Remove detached-nodes fomr ring and members
%%      3. Synchronize previous-ring
%%      4. Export members and ring
%% @private
-spec(after_rebalance(Members) ->
             ok when Members::[{#member{}, #member{}, #member{}}]).
after_rebalance([]) ->
    %% if previous-ring and current-ring has "detached-node(s)",
    %% then remove them, as same as memebers
    case leo_redundant_manager_api:get_members_by_status(
           ?VER_CUR, ?STATE_DETACHED) of
        {ok, DetachedNodes} ->
            TblCur  = leo_redundant_manager_api:table_info(?VER_CUR),
            TblPrev = leo_redundant_manager_api:table_info(?VER_PREV),
            ok = lists:foreach(
                   fun(#member{node  = Node,
                               alias = Alias} = Member) ->
                           %% remove detached node from members
                           leo_cluster_tbl_member:delete(?MEMBER_TBL_CUR,  Node),
                           leo_cluster_tbl_member:delete(?MEMBER_TBL_PREV, Node),
                           %% remove detached node from ring
                           case Alias of
                               [] -> void;
                               _  ->
                                   leo_redundant_manager_chash:remove(TblCur,  Member),
                                   leo_redundant_manager_chash:remove(TblPrev, Member)
                           end
                   end, DetachedNodes);
        {error,_Cause} ->
            ok
    end,

    %% Synchronize previous-ring
    case synchronize_1(?SYNC_TARGET_RING_PREV, ?VER_PREV) of
        ok ->
            void;
        {error, Reason} ->
            error_logger:warning_msg("~p,~p,~p,~p~n",
                                     [{module, ?MODULE_STRING},
                                      {function, "after_rebalance_1/0"},
                                      {line, ?LINE},
                                      {body, Reason}])
    end,
    ok;
after_rebalance([{#member{node = Node} = Member_1, Member_2, SrcMember}|Rest]) ->
    try
        %% After exec taking over data from detach-node to attach-node
        RingTblPrev = table_info(?VER_PREV),
        MembersTblPrev = ?MEMBER_TBL_PREV,

        ok = leo_redundant_manager_chash:remove(RingTblPrev, Member_1),
        ok = leo_redundant_manager_chash:add(RingTblPrev, Member_2),
        ok = leo_cluster_tbl_member:insert(MembersTblPrev, {Node, Member_2}),

        case SrcMember of
            [] -> void;
            #member{node = SrcNode} ->
                ok = leo_cluster_tbl_member:insert(
                       MembersTblPrev,{SrcNode, SrcMember#member{alias = []}})
        end
    catch
        _:Cause ->
            error_logger:warning_msg("~p,~p,~p,~p~n",
                                     [{module, ?MODULE_STRING},
                                      {function, "after_rebalance_1/0"},
                                      {line, ?LINE},
                                      {body, Cause}])
    end,
    after_rebalance(Rest).


%% @doc Generate an alias of the node
%%
-spec(get_alias(Node, GrpL2) ->
             {ok, tuple()} when Node::atom(),
                                GrpL2::string()).
get_alias(Node, GrpL2) ->
    get_alias(?MEMBER_TBL_CUR, Node, GrpL2).

-spec(get_alias(Table, Node, GrpL2) ->
             {ok, tuple()} when Table::member_table(),
                                Node::atom(),
                                GrpL2::string()).
get_alias(Table, Node, GrpL2) ->
    case leo_cluster_tbl_member:find_by_status(
           Table, ?STATE_DETACHED) of
        not_found ->
            get_alias_1([], Table, Node, GrpL2);
        {ok, Members} ->
            get_alias_1(Members, Table, Node, GrpL2);
        {error, Cause} ->
            {error, Cause}
    end.

-spec(get_alias(init, Table, Node, GrpL2) ->
             {ok, tuple()} when Table::member_table(),
                                Node::atom(),
                                GrpL2::string()).
get_alias(init, Table, Node, GrpL2) ->
    get_alias_1([], Table, Node, GrpL2).


%% @private
get_alias_1([],_,Node,_GrpL2) ->
    PartOfAlias = string:substr(
                    leo_hex:binary_to_hex(
                      crypto:hash(md5, lists:append([atom_to_list(Node)]))),1,8),
    {ok, {[], lists:append([?NODE_ALIAS_PREFIX, PartOfAlias])}};

get_alias_1([#member{node  = Node_1}|Rest], Table, Node, GrpL2) when Node == Node_1 ->
    get_alias_1(Rest, Table, Node, GrpL2);

get_alias_1([#member{alias = [],
                     node  = Node_1}|Rest], Table, Node, GrpL2) when Node /= Node_1 ->
    get_alias_1(Rest, Table, Node, GrpL2);

get_alias_1([#member{alias = Alias,
                     node  = Node_1,
                     grp_level_2 = GrpL2_1}|Rest], Table, Node, GrpL2) when Node  /= Node_1 andalso
                                                                            GrpL2 == GrpL2_1 ->
    case leo_cluster_tbl_member:find_by_alias(Alias) of
        {ok, [Member|_]} ->
            {ok, {Member, Member#member.alias}};
        _ ->
            get_alias_1(Rest, Table, Node, GrpL2)
    end;

get_alias_1([_|Rest], Table, Node, GrpL2) ->
    get_alias_1(Rest, Table, Node, GrpL2).



%%--------------------------------------------------------------------
%% API-3  FUNCTIONS (leo_member_management_server)
%%--------------------------------------------------------------------
%% @doc Has a member ?
%%
-spec(has_member(Node) ->
             boolean() when Node::atom()).
has_member(Node) ->
    leo_redundant_manager:has_member(Node).


%% @doc Has charge of node?
%%      'true' is returned even if it detects an error
-spec(has_charge_of_node(Key, NumOfReplicas) ->
             boolean() when Key::binary(),
                            NumOfReplicas::integer()).
has_charge_of_node(Key, 0) ->
    case leo_cluster_tbl_conf:get() of
        {ok, #?SYSTEM_CONF{n = NumOfReplica}} ->
            has_charge_of_node(Key, NumOfReplica);
        _ ->
            true
    end;
has_charge_of_node(Key, NumOfReplica) ->
    case get_redundancies_by_key(put, Key) of
        {ok, #redundancies{nodes = Nodes}} ->
            Nodes_1 = lists:sublist(Nodes, NumOfReplica),
            lists:foldl(
              fun(#redundant_node{node = N,
                                  can_read_repair = CanReadRepair}, false) ->
                      (N == erlang:node() andalso
                       CanReadRepair == true);
                 (_, true ) ->
                      true
              end, false, Nodes_1);
        _ ->
            true
    end.


%% @doc get members.
%%
-spec(get_members() ->
             {ok, [#member{}]} | {error, any()}).
get_members() ->
    get_members(?VER_CUR).

-spec(get_members(Ver) ->
             {ok, Members} | {error, Cause} when Ver::?VER_CUR | ?VER_PREV,
                                                 Members::[#member{}],
                                                 Cause::any()).
get_members(Ver) when Ver == ?VER_CUR;
                      Ver == ?VER_PREV ->
    leo_redundant_manager:get_members(Ver);
get_members(_) ->
    {error, invalid_version}.


%% @doc get all version members
-spec(get_all_ver_members() ->
             {ok, {CurMembers, PrevMembers}} |
             {error, Cause} when CurMembers::[#member{}],
                                 PrevMembers::[#member{}],
                                 Cause::any()).
get_all_ver_members() ->
    case get_members(?VER_CUR) of
        {ok, CurMembers} ->
            case get_members(?VER_PREV) of
                {ok, PrevMembers} ->
                    {ok, {CurMembers, PrevMembers}};
                {error, not_found} ->
                    {ok, {CurMembers, CurMembers}};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.


%% @doc get a member by node-name.
%%
-spec(get_member_by_node(Node) ->
             {ok, Member} |
             {error, any()} when Node::atom(),
                                 Member::#member{}).
get_member_by_node(Node) ->
    leo_redundant_manager:get_member_by_node(Node).


%% @doc get # of members.
%%
-spec(get_members_count() ->
             integer() | {error, any()}).
get_members_count() ->
    leo_cluster_tbl_member:table_size().


%% @doc get members by status
%%
-spec(get_members_by_status(Status) ->
             {ok, Members} |
             {error, any()} when Status::atom(),
                                 Members::[#member{}]
                                          ).
get_members_by_status(Status) ->
    get_members_by_status(?VER_CUR, Status).

-spec(get_members_by_status(Ver, Status) ->
             {ok, Members} |
             {error, any()} when Ver::?VER_CUR | ?VER_PREV,
                                 Status::atom(),
                                 Members::[#member{}]).
get_members_by_status(Ver, Status) ->
    leo_redundant_manager:get_members_by_status(Ver, Status).


%% @doc update members.
%%
-spec(update_member(Member) ->
             ok | {error, any()} when Member::#member{}).
update_member(Member) ->
    case leo_redundant_manager:update_member(Member) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc update members.
%%
-spec(update_members(Members) ->
             ok | {error, any()} when Members::[#member{}]).
update_members(Members) ->
    case leo_redundant_manager:update_members(Members) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc update a member by node-name.
%%
-spec(update_member_by_node(atom(), atom()) ->
             ok | {error, any()}).
update_member_by_node(Node, State) ->
    leo_redundant_manager:update_member_by_node(Node, State).

-spec(update_member_by_node(atom(), integer(), atom()) ->
             ok | {error, any()}).
update_member_by_node(Node, Clock, State) ->
    leo_redundant_manager:update_member_by_node(Node, Clock, State).


%% @doc remove a member by node-name.
%%
-spec(delete_member_by_node(Node) ->
             ok | {error, any()} when Node::atom()).
delete_member_by_node(Node) ->
    leo_redundant_manager:delete_member_by_node(Node).


%% @doc Is alive the process?
%%
-spec(is_alive() ->
             ok).
is_alive() ->
    leo_membership_cluster_local:heartbeat().


%% @doc Retrieve table-info by version.
%%
-spec(table_info(Ver) ->
             ring_table_info() when Ver::?VER_CUR | ?VER_PREV).
-ifdef(TEST).
table_info(?VER_CUR)  -> {ets, ?RING_TBL_CUR };
table_info(?VER_PREV) -> {ets, ?RING_TBL_PREV}.
-else.
table_info(?VER_CUR) ->
    case leo_misc:get_env(?APP, ?PROP_SERVER_TYPE) of
        {ok, ?MONITOR_NODE} ->
            {mnesia, ?RING_TBL_CUR};
        _ ->
            {ets, ?RING_TBL_CUR}
    end;

table_info(?VER_PREV) ->
    case leo_misc:get_env(?APP, ?PROP_SERVER_TYPE) of
        {ok, ?MONITOR_NODE} ->
            {mnesia, ?RING_TBL_PREV};
        _ ->
            {ets, ?RING_TBL_PREV}
    end.
-endif.


%% @doc Force sync ring-workers
%%
-spec(force_sync_workers() ->
             ok).
force_sync_workers() ->
    ok = leo_redundant_manager_worker:force_sync(?RING_TBL_CUR),
    ok = leo_redundant_manager_worker:force_sync(?RING_TBL_PREV),
    ok.


%% @doc Retrieve local cluster's status
-spec(get_cluster_status() ->
             {ok, #?CLUSTER_STAT{}} | not_found).
get_cluster_status() ->
    {ok, #?SYSTEM_CONF{cluster_id = ClusterId}} = leo_cluster_tbl_conf:get(),
    case get_members() of
        {ok, Members} ->
            Status = judge_cluster_status(Members),
            {ok, {Checksum,_}} = checksum(?CHECKSUM_MEMBER),
            {ok, #?CLUSTER_STAT{cluster_id = ClusterId,
                                state      = Status,
                                checksum = Checksum}};
        _ ->
            not_found
    end.


%% @doc Judge status of local cluster
%% @private
-spec(judge_cluster_status(Members) ->
             node_state() when Members::[#member{}]).
judge_cluster_status(Members) ->
    NumOfMembers = length(Members),
    SuspendNode  = length([N || #member{state = ?STATE_SUSPEND,
                                        node  = N} <- Members]),
    RunningNode  = length([N || #member{state = ?STATE_RUNNING,
                                        node  = N} <- Members]),
    case SuspendNode of
        NumOfMembers ->
            ?STATE_SUSPEND;
        _ ->
            case (RunningNode > 0) of
                true  -> ?STATE_RUNNING;
                false -> ?STATE_STOP
            end
    end.


%% @doc Retrieve checksums of cluster-related tables
%%
-spec(get_cluster_tbl_checksums() ->
             {ok, [tuple()]}).
get_cluster_tbl_checksums() ->
    Chksum_1 = leo_cluster_tbl_conf:checksum(),
    Chksum_2 = leo_mdcr_tbl_cluster_info:checksum(),
    Chksum_3 = leo_mdcr_tbl_cluster_mgr:checksum(),
    Chksum_4 = leo_mdcr_tbl_cluster_member:checksum(),
    Chksum_5 = leo_mdcr_tbl_cluster_stat:checksum(),
    {ok, [{?CHKSUM_CLUSTER_CONF, Chksum_1},
          {?CHKSUM_CLUSTER_INFO, Chksum_2},
          {?CHKSUM_CLUSTER_MGR, Chksum_3},
          {?CHKSUM_CLUSTER_MEMBER, Chksum_4},
          {?CHKSUM_CLUSTER_STAT, Chksum_5}
         ]}.


%%--------------------------------------------------------------------
%% API-4  FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Retrieve conf of remote clusters
%%
-spec(get_remote_clusters() ->
             {ok, [#?CLUSTER_INFO{}]} | {error, any()}).
get_remote_clusters() ->
    case leo_cluster_tbl_conf:get() of
        {ok, #?SYSTEM_CONF{max_mdc_targets = MaxTargetClusters}} ->
            get_remote_clusters(MaxTargetClusters);
        _ ->
            not_found
    end.

-spec(get_remote_clusters(NumOfDestClusters) ->
             {ok, [#?CLUSTER_INFO{}]} |
             {error, any()} when NumOfDestClusters::integer()).
get_remote_clusters(NumOfDestClusters) ->
    leo_mdcr_tbl_cluster_info:find_by_limit(NumOfDestClusters).


%% @doc Retrieve remote cluster members
%%
-spec(get_remote_members(ClusterId) ->
             {ok, #?CLUSTER_MEMBER{}} | {error, any()} when ClusterId::atom()).
get_remote_members(ClusterId) ->
    get_remote_members(ClusterId, ?DEF_NUM_OF_REMOTE_MEMBERS).

-spec(get_remote_members(ClusterId, NumOfMembers) ->
             {ok, #?CLUSTER_MEMBER{}} | {error, any()} when ClusterId::atom(),
                                                            NumOfMembers::integer()).
get_remote_members(ClusterId, NumOfMembers) ->
    leo_mdcr_tbl_cluster_member:find_by_limit(ClusterId, NumOfMembers).


%%--------------------------------------------------------------------
%% INNTERNAL FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Specify ETS's table.
%% @private
-spec(ring_table(method()) ->
             ring_table_info()).
ring_table(default) -> table_info(?VER_CUR);
ring_table(put)     -> table_info(?VER_CUR);
ring_table(get)     -> table_info(?VER_PREV);
ring_table(delete)  -> table_info(?VER_CUR);
ring_table(head)    -> table_info(?VER_PREV).
