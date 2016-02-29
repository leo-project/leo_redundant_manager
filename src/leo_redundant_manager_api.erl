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
%% ---------------------------------------------------------------------
%% Leo Redundant Manager - API
%%
%% @doc leo_redaundant_manager's API
%% @reference https://github.com/leo-project/leo_redundant_manager/blob/master/src/leo_redundant_manager_api.erl
%% @end
%%======================================================================
-module(leo_redundant_manager_api).

-include("leo_redundant_manager.hrl").
-include_lib("leo_rpc/include/leo_rpc.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Ring-related
-export([start/3,
         create/1, create/2, create/3, create/4,
         set_options/2, get_options/1, get_option/2,

         attach/2, reserve/2,
         detach/2, detach/3,
         suspend/2, suspend/3,

         checksum/2, synchronize/3, synchronize/4,
         get_ring/1, get_ring/2, dump/2
        ]).
%% Redundancy-related
-export([get_redundancies_by_key/2, get_redundancies_by_key/3,
         get_redundancies_by_addr_id/2, get_redundancies_by_addr_id/3,
         collect_redundancies_by_key/4,
         part_of_collect_redundancies_by_key/5,
         range_of_vnodes/2, rebalance/1,
         get_alias/3, get_alias/4, get_alias/5
        ]).
%% Member-related
-export([has_member/2, has_charge_of_node/3,
         get_members/1, get_members/2, get_all_ver_members/1,
         get_member_by_node/2, get_members_count/1,
         get_members_by_status/2, get_members_by_status/3,
         update_member/2, update_members/2,
         update_member_by_node/3, update_member_by_node/4,

         delete_member_by_node/2, is_alive/1, table_info/1,
         force_sync_workers/1,
         get_cluster_status/1,
         get_cluster_tbl_checksums/1
        ]).

%% Multi-DC-replciation-related
-export([get_remote_clusters/1, get_remote_clusters_by_limit/1,
         get_remote_members/1, get_remote_members/2
        ]).

%% Request type
-type(method() :: put | get | delete | head | default).

%%--------------------------------------------------------------------
%% API-1  FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Launch a node in a cluster
start(ClusterId, NodeType, Options) ->
    leo_redundant_manager_sup:start_child(ClusterId, NodeType, Options).


%% @doc Create the RING
-spec(create(ClusterId) ->
             {ok, Members, HashValues} |
             {error, any()} when ClusterId::cluster_id(),
                                 Members::[#?MEMBER{}],
                                 HashValues::[{atom(), integer()}]).
create(ClusterId) ->
    case create(ClusterId, ?VER_CUR) of
        {ok, Members, HashValues} ->
            Ret = case leo_cluster_tbl_member:table_size(
                         ClusterId, ?MEMBER_TBL_PREV) of
                      0 ->
                          create_1(ClusterId);
                      _ ->
                          case create(ClusterId, ?VER_PREV) of
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

-spec(create(ClusterId, Ver) ->
             {ok, Members, HashValues} |
             {error, any()} when ClusterId::cluster_id(),
                                 Ver::?VER_CUR|?VER_PREV,
                                 Members::[#?MEMBER{}],
                                 HashValues::[{atom(), integer()}]).
create(ClusterId, Ver) when Ver == ?VER_CUR;
                            Ver == ?VER_PREV ->
    case get_option(ClusterId, ?PROP_N) of
        0 ->
            {error, ?ERROR_INVALID_CONF};
        _N ->
            case leo_redundant_manager:create(ClusterId, Ver) of
                ok ->
                    case leo_cluster_tbl_member:find_by_cluster_id(
                           ?member_table(Ver), ClusterId) of
                        {ok, Members} ->
                            spawn(
                              fun() ->
                                      ok = leo_redundant_manager_worker:force_sync(
                                             ClusterId, ?ring_table(Ver))
                              end),
                            {ok, HashRing} = checksum(ClusterId, ?CHECKSUM_RING),
                            ok = leo_misc:set_env(?APP, ?PROP_RING_HASH,
                                                  erlang:element(1, HashRing)),
                            {ok, HashMember} = checksum(ClusterId, ?CHECKSUM_MEMBER),
                            {ok, Members, [{?CHECKSUM_RING,   HashRing},
                                           {?CHECKSUM_MEMBER, HashMember}]};
                        Error ->
                            Error
                    end;
                Error ->
                    Error
            end
    end;
create(_,_) ->
    {error, invlid_version}.


-spec(create(ClusterId, Ver, Members) ->
             {ok, Members, HashValues} |
             {error, any()} when ClusterId::cluster_id(),
                                 Ver::?VER_CUR|?VER_PREV,
                                 Members::[#?MEMBER{}],
                                 HashValues::[{atom(), integer()}]).
create(ClusterId, Ver, Members) ->
    create(ClusterId, Ver, Members, []).

-spec(create(ClusterId, Ver, Members, Options) ->
             {ok, Members, HashValues} |
             {error, any()} when ClusterId::cluster_id(),
                                 Ver::?VER_CUR|?VER_PREV,
                                 Members::[#?MEMBER{}],
                                 Options::[{atom(), any()}],
                                 HashValues::[{atom(), integer()}]).
create(ClusterId, Ver, [], []) ->
    create(ClusterId, Ver);
create(ClusterId, Ver, [], Options) ->
    ok = set_options(ClusterId, Options),
    create(ClusterId, Ver);
create(ClusterId, Ver, [#?MEMBER{node = Node} = Member|T], Options) when Ver == ?VER_CUR;
                                                                         Ver == ?VER_PREV ->
    %% Add a member as "attached node" into member-table
    case leo_cluster_tbl_member:lookup(ClusterId, Node) of
        not_found ->
            %% @TODO
            %% leo_cluster_tbl_member:insert(
            %%   {Node, Member#?MEMBER{cluster_id = ClusterId,
            %%                         state = ?STATE_ATTACHED}});
            leo_cluster_tbl_member:insert(Member#?MEMBER{cluster_id = ClusterId,
                                                         node = Node,
                                                         state = ?STATE_ATTACHED});

        _ ->
            void
    end,
    create(ClusterId, Ver, T, Options);
create(_,_,_,_) ->
    {error, invalid_version}.


%% @private
create_1(ClusterId) ->
    case leo_cluster_tbl_member:overwrite(
           ?MEMBER_TBL_CUR, ?MEMBER_TBL_PREV, ClusterId) of
        ok ->
            %% @TODO
            case leo_cluster_tbl_ring:overwrite(
                   table_info(?VER_CUR),
                   table_info(?VER_PREV), ClusterId) of
                ok ->
                    ok;
                Error ->
                    Error
            end;
        Error ->
            Error
    end.


%% @doc set routing-table's options.
-spec(set_options(ClusterId, Options) ->
             ok when ClusterId::cluster_id(),
                     Options::[{atom(), any()}]).
set_options(ClusterId, Options) ->
    ok = leo_misc:set_env(?APP, ?id_red_options(ClusterId), Options),
    ok.


%% @doc get routing-table's options.
-spec(get_options(ClusterId) ->
             {ok, Options} when ClusterId::cluster_id(),
                                Options::[{atom(), any()}]).
get_options(ClusterId) ->
    case leo_misc:get_env(?APP, ?id_red_options(ClusterId)) of
        undefined ->
            %% @TODO
            case catch leo_cluster_tbl_conf:get(ClusterId) of
                {ok, #?SYSTEM_CONF{} = SystemConf} ->
                    Options = record_to_tuplelist(SystemConf),
                    ok = set_options(ClusterId, Options),
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
-spec(get_option(ClusterId, Item) ->
             Value when ClusterId::cluster_id(),
                        Item::atom(),
                        Value::any()).
get_option(ClusterId, Item) ->
    {ok, Options} = get_options(ClusterId),
    leo_misc:get_value(Item, Options, 0).


%% @doc record to tuple-list for converting system-conf
%% @private
-spec(record_to_tuplelist(Value) ->
             [{atom(), any()}] when Value::any()).
record_to_tuplelist(Value) ->
    lists:zip(
      record_info(fields, ?SYSTEM_CONF), tl(tuple_to_list(Value))).


%% @doc attach a node.
-spec(attach(ClusterId, Member) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Member::#?MEMBER{}).
attach(ClusterId, Member) ->
    case leo_redundant_manager:attach(ClusterId, Member) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc reserve a node during in operation
-spec(reserve(ClusterId, Member) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Member::#?MEMBER{}).
reserve(ClusterId, Member) ->
    case leo_redundant_manager:reserve(ClusterId, Member) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc detach a node.
-spec(detach(ClusterId, Node) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Node::atom()).
detach(ClusterId, Node) ->
    detach(ClusterId, Node, leo_date:clock()).

-spec(detach(ClusterId, Node, Clock) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Node::atom(),
                                      Clock::integer()).
detach(ClusterId, Node, Clock) ->
    case leo_redundant_manager:detach(ClusterId, Node, Clock) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc suspend a node. (disable)
-spec(suspend(ClusterId, Node) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Node::atom()).
suspend(ClusterId, Node) ->
    suspend(ClusterId, Node, leo_date:clock()).

-spec(suspend(ClusterId, Node, Clock) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Node::atom(),
                                      Clock::integer()).
suspend(ClusterId, Node, Clock) ->
    case leo_redundant_manager:suspend(ClusterId, Node, Clock) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc get routing_table's checksum.
-spec(checksum(ClusterId, Type) ->
             {ok, integer()} |
             {ok, {integer(), integer()}} |
             {error, any()} when ClusterId::cluster_id(),
                                 Type::checksum_type()).
checksum(ClusterId, ?CHECKSUM_MEMBER = Type) ->
    leo_redundant_manager:checksum(ClusterId, Type);
checksum(ClusterId, ?CHECKSUM_RING) ->
    TblInfoCur  = table_info(?VER_CUR),
    TblInfoPrev = table_info(?VER_PREV),
    {ok, RingHashCur } = leo_redundant_manager_chash:checksum(ClusterId, TblInfoCur),
    {ok, RingHashPrev} = leo_redundant_manager_chash:checksum(ClusterId, TblInfoPrev),
    {ok, {RingHashCur, RingHashPrev}};
checksum(ClusterId, ?CHECKSUM_WORKER) ->
    leo_redundant_manager_worker:checksum(ClusterId);
checksum(ClusterId, ?CHECKSUM_SYS_CONF) ->
    {ok, SysConf} = get_options(ClusterId),
    {ok, erlang:crc32(term_to_binary(SysConf))};
checksum(_,_) ->
    {error, invalid_type}.


%% @doc synchronize member-list and routing-table.
-spec(synchronize(ClusterId, SyncTarget, SyncData, Options) ->
             {ok, [{atom(), any()}]} |
             {error, any()} when ClusterId::cluster_id(),
                                 SyncTarget::sync_target(),
                                 SyncData::[{atom(), any()}],
                                 Options::[{atom(), any()}]).
synchronize(ClusterId, ?SYNC_TARGET_BOTH, SyncData, Options) ->
    %% set configurations
    case Options of
        [] -> void;
        _ ->
            ok = set_options(ClusterId, Options)
    end,

    %% Synchronize current and previous members
    %%   Then Synchronize ring
    case synchronize(ClusterId, ?SYNC_TARGET_MEMBER, SyncData) of
        {ok, ChecksumMembers} ->
            case synchronize_1(ClusterId, ?SYNC_TARGET_RING_CUR,  ?VER_CUR) of
                ok ->
                    case synchronize_1(ClusterId, ?SYNC_TARGET_RING_PREV, ?VER_PREV) of
                        ok ->
                            {ok, ChecksumRing} = checksum(ClusterId, ?CHECKSUM_RING),
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
-spec(synchronize(ClusterId, SyncTarget, SyncData) ->
             {ok, integer()} |
             {ok, [{atom(), any()}]} |
             {error, any()} when ClusterId::cluster_id(),
                                 SyncTarget::sync_target(),
                                 SyncData::[{atom(), any()}]).
synchronize(ClusterId, ?SYNC_TARGET_BOTH, SyncData) ->
    synchronize(ClusterId, ?SYNC_TARGET_BOTH, SyncData, []);

synchronize(ClusterId, ?SYNC_TARGET_MEMBER = SyncTarget, SyncData) ->
    case synchronize_1(ClusterId, SyncTarget, ?VER_CUR,  SyncData) of
        ok ->
            case synchronize_1(ClusterId, SyncTarget, ?VER_PREV, SyncData) of
                ok ->
                    checksum(ClusterId, ?CHECKSUM_MEMBER);
                Error ->
                    Error
            end;
        Error ->
            Error
    end;

synchronize(ClusterId, Target, []) when ?SYNC_TARGET_RING_CUR  == Target;
                                        ?SYNC_TARGET_RING_PREV == Target ->
    synchronize_1(ClusterId, Target, ?sync_target_to_ver(Target));

synchronize(ClusterId, Target, SyncData) when ?SYNC_TARGET_RING_CUR  == Target;
                                              ?SYNC_TARGET_RING_PREV == Target ->
    {ok, ChecksumMembers} = synchronize(ClusterId, ?SYNC_TARGET_MEMBER, SyncData),
    case synchronize_1(ClusterId, Target, ?sync_target_to_ver(Target)) of
        ok ->
            {ok, ChecksumRing} = checksum(ClusterId, ?CHECKSUM_RING),
            {ok, [{?CHECKSUM_MEMBER, ChecksumMembers},
                  {?CHECKSUM_RING,   ChecksumRing}
                 ]};
        Error ->
            Error
    end;
synchronize(_,_,_) ->
    {error, invalid_target}.

%% @private
synchronize_1(ClusterId, ?SYNC_TARGET_MEMBER, Ver, SyncData) ->
    case leo_misc:get_value(Ver, SyncData, []) of
        [] ->
            ok;
        NewMembers ->
            Table = ?member_table(Ver),
            %% @TODO
            case leo_cluster_tbl_member:find_by_cluster_id(
                   Table, ClusterId) of
                {ok, OldMembers} ->
                    leo_redundant_manager:update_members(
                      ClusterId, Table, OldMembers, NewMembers);
                not_found ->
                    leo_redundant_manager:update_members(
                      ClusterId, Table, [], NewMembers);
                Error ->
                    Error
            end
    end.

%% @private
synchronize_1(ClusterId, Target, Ver) when Target == ?SYNC_TARGET_RING_CUR;
                                           Target == ?SYNC_TARGET_RING_PREV ->
    %% TableInfo = table_info(Ver),
    case leo_redundant_manager_worker:get_vnode_list(ClusterId, Ver) of
        {error, Cause} ->
            {error, Cause};
        CurRing ->
            case CurRing of
                [] ->
                    void;
                _  ->
                    ok = leo_redundant_manager_worker:set_vnode_list(
                           ClusterId, Ver, [])
            end,

            MemberTbl = case Target of
                            ?SYNC_TARGET_RING_CUR ->
                                ?MEMBER_TBL_CUR;
                            ?SYNC_TARGET_RING_PREV ->
                                ?MEMBER_TBL_PREV
                        end,

            case leo_cluster_tbl_member:find_by_cluster_id(
                   MemberTbl, ClusterId) of
                {ok, Members} when length(Members) > 0 ->
                    case create(ClusterId, Ver) of
                        {ok,_Members,_HashVals} ->
                            ok;
                        Error when CurRing /= [] ->
                            [leo_redundant_manager_worker:add_vnode(
                               ClusterId, Ver, R) || R <- CurRing],
                            Error;
                        Error ->
                            Error
                    end;
                _ ->
                    {error, ?ERROR_COULD_NOT_GET_MEMBERS}
            end
    end;
synchronize_1(_,_,_) ->
    {error, invalid_target}.


%% @TODO
%% @doc Retrieve Ring
-spec(get_ring(ClusterId) ->
             {ok, [tuple()]} when ClusterId::cluster_id()).
get_ring(ClusterId) ->
    get_ring(ClusterId, ?SYNC_TARGET_RING_CUR).

-spec(get_ring(ClusterId, SyncTarget) ->
             {ok, [tuple()]} when ClusterId::cluster_id(),
                                  SyncTarget::sync_target()).
get_ring(ClusterId, ?SYNC_TARGET_RING_CUR) ->
    case leo_cluster_tbl_ring:find_by_cluster_id(
           table_info(?VER_CUR), ClusterId) of
        {ok, VNodeList} ->
            {ok, VNodeList};
        Other ->
            Other
    end;
get_ring(ClusterId, ?SYNC_TARGET_RING_PREV) ->
    case leo_cluster_tbl_ring:find_by_cluster_id(
           table_info(?VER_PREV), ClusterId) of
        {ok, VNodeList} ->
            {ok, VNodeList};
        Other ->
            Other
    end.


%% @doc Dump table-records.
-spec(dump(ClusterId, Type) ->
             ok when ClusterId::cluster_id(),
                     Type::member|ring|both|work).
dump(ClusterId, both) ->
    catch dump(ClusterId, member),
    catch dump(ClusterId, ring),
    catch dump(ClusterId, work),
    ok;
dump(ClusterId, work) ->
    dump_1(ClusterId, ?RING_WORKER_POOL_SIZE - 1);
dump(ClusterId, Type) ->
    leo_redundant_manager:dump(ClusterId, Type).

%% @private
dump_1(_,-1) ->
    ok;
dump_1(ClusterId, Index) ->
    ok = leo_redundant_manager_worker:dump(ClusterId),
    dump_1(ClusterId, Index - 1).


%%--------------------------------------------------------------------
%% API-2  FUNCTIONS (leo_routing_tbl_provide_server)
%%--------------------------------------------------------------------
%% @doc Retrieve redundancies from the ring-table.
-spec(get_redundancies_by_key(ClusterId, Key) ->
             {ok, #redundancies{}} |
             {error, any()} when ClusterId::cluster_id(),
                                 Key::binary()).
get_redundancies_by_key(ClusterId, Key) ->
    get_redundancies_by_key(ClusterId, default, Key).

-spec(get_redundancies_by_key(ClusterId, Method, Key) ->
             {ok, #redundancies{}} |
             {error, any()} when ClusterId::cluster_id(),
                                 Method::method(),
                                 Key::binary()).
get_redundancies_by_key(ClusterId, Method, Key) ->
    {ok, Options} = get_options(ClusterId),
    AddrId = leo_redundant_manager_chash:vnode_id(Key),
    get_redundancies_by_addr_id_1(
      ClusterId, ring_table(Method), AddrId, Options).


%% @doc Retrieve redundancies from the ring-table.
-spec(get_redundancies_by_addr_id(ClusterId, AddrId) ->
             {ok, #redundancies{}} |
             {error, any()} when ClusterId::cluster_id(),
                                 AddrId::integer()).
get_redundancies_by_addr_id(ClusterId, AddrId) ->
    get_redundancies_by_addr_id(ClusterId, default, AddrId).

-spec(get_redundancies_by_addr_id(ClusterId, Method, AddrId) ->
             {ok, #redundancies{}} |
             {error, any()} when ClusterId::cluster_id(),
                                 Method::method(),
                                 AddrId::integer()).
get_redundancies_by_addr_id(ClusterId, Method, AddrId) ->
    {ok, Options} = get_options(ClusterId),
    get_redundancies_by_addr_id_1(ClusterId, ring_table(Method), AddrId, Options).

%% @private
-spec(get_redundancies_by_addr_id_1(ClusterId, TableInfo, AddrId, Options) ->
             {ok, #redundancies{}} | {error, any()} when ClusterId::cluster_id(),
                                                         TableInfo::{atom(), atom()},
                                                         AddrId::non_neg_integer(),
                                                         Options::[{atom(), any()}]).
get_redundancies_by_addr_id_1(ClusterId, {_,Tbl}, AddrId, Options) ->
    N = leo_misc:get_value(?PROP_N, Options),
    R = leo_misc:get_value(?PROP_R, Options),
    W = leo_misc:get_value(?PROP_W, Options),
    D = leo_misc:get_value(?PROP_D, Options),

    case leo_redundant_manager_worker:lookup(ClusterId, Tbl, AddrId) of
        {ok, Redundancies} ->
            CurRingHash =
                case leo_misc:get_env(?APP, ?PROP_RING_HASH) of
                    {ok, RingHash} ->
                        RingHash;
                    undefined ->
                        {ok, {RingHash, _}} = checksum(ClusterId, ?CHECKSUM_RING),
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
-spec(collect_redundancies_by_key(ClusterId, Key, NumOfReplicas, MaxNumOfDuplicate) ->
             {ok, {Options, RedundantNodeL}}|{error, any()}
                 when ClusterId::cluster_id(),
                      Key::binary(),
                      NumOfReplicas::pos_integer(),
                      MaxNumOfDuplicate::pos_integer(),
                      Options::[{atom(), any()}],
                      RedundantNodeL::[#redundancies{}]).
collect_redundancies_by_key(ClusterId, Key, NumOfReplicas, MaxNumOfDuplicate) ->
    {_, Table} = ring_table(default),
    {ok, Options} = get_options(ClusterId),
    BitOfRing = leo_misc:get_value(?PROP_RING_BIT, Options),
    AddrId = leo_redundant_manager_chash:vnode_id(BitOfRing, Key),

    case leo_redundant_manager_worker:collect(
           ClusterId, Table, {AddrId, Key},
           NumOfReplicas, MaxNumOfDuplicate) of
        {ok, RedundantNodeL} ->
            {ok, {Options, RedundantNodeL}};
        not_found = Cause ->
            {error, Cause};
        Others ->
            Others
    end.


-spec(part_of_collect_redundancies_by_key(
        ClusterId, Index, ChildKey, NumOfReplicas, MaxNumOfDuplicate) ->
             {ok, RedundantNode}|{error, any()}
                 when ClusterId::cluster_id(),
                      Index::pos_integer(),
                      ChildKey::binary(),
                      NumOfReplicas::pos_integer(),
                      MaxNumOfDuplicate::pos_integer(),
                      RedundantNode::#redundant_node{}).
part_of_collect_redundancies_by_key(ClusterId, Index, ChildKey,
                                    NumOfReplicas, MaxNumOfDuplicate) ->
    ParentKey = begin
                    case binary:matches(ChildKey, [<<"\n">>], []) of
                        [] ->
                            ChildKey;
                        PosL ->
                            {Pos,_} = lists:last(PosL),
                            binary:part(ChildKey, 0, Pos)
                    end
                end,
    case collect_redundancies_by_key(ClusterId, ParentKey,
                                     NumOfReplicas, MaxNumOfDuplicate) of
        {ok, {_Options, RedundantNodeL}}
          when erlang:length(RedundantNodeL) >= NumOfReplicas ->
            {ok, lists:nth(Index, RedundantNodeL)};
        Other ->
            Other
    end.


%% @doc Retrieve range of vnodes.
-spec(range_of_vnodes(ClusterId, ToVNodeId) ->
             {ok, [tuple()]} when ClusterId::cluster_id(),
                                  ToVNodeId::integer()).
range_of_vnodes(ClusterId, ToVNodeId) ->
    TblInfo = table_info(?VER_CUR),
    leo_redundant_manager_chash:range_of_vnodes(
      TblInfo, ClusterId, ToVNodeId).


%% @doc Re-balance objects in the cluster.
-spec(rebalance(ClusterId) ->
             {ok, [tuple()]} | {error, any()} when ClusterId::cluster_id()).
rebalance(ClusterId) ->
    case leo_cluster_tbl_member:find_by_cluster_id(
           ?MEMBER_TBL_CUR, ClusterId) of
        {ok, MembersCur} ->
            %% Before exec rebalance
            case before_rebalance(MembersCur, ClusterId) of
                {ok, {MembersCur_1, MembersPrev, TakeOverList}} ->
                    %% Exec rebalance
                    {ok, Ret} = leo_redundant_manager_chash:rebalance(
                                  ClusterId,
                                  #rebalance{tbl_cur  = table_info(?VER_CUR),
                                             tbl_prev = table_info(?VER_PREV),
                                             members_cur  = MembersCur_1,
                                             members_prev = MembersPrev}),
                    ok = after_rebalance(TakeOverList, ClusterId),
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
before_rebalance(MembersCur, ClusterId) ->
    %% If "attach" and "detach" are included in members,
    %% then update current-members
    %% because attach-node need to take over detach-node's data.
    case takeover_status(MembersCur, ClusterId, []) of
        {ok, {MembersCur_1, TakeOverList}} ->
            %% Remove all previous members,
            %% Then insert new members from current members
            case leo_cluster_tbl_member:delete_all(
                   ?MEMBER_TBL_PREV, ClusterId) of
                ok ->
                    case before_rebalance_1(MembersCur_1, ClusterId) of
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
takeover_status([], ClusterId, TakeOverList) ->
    case leo_cluster_tbl_member:find_by_cluster_id(
           ?MEMBER_TBL_CUR, ClusterId) of
        {ok, MembersCur} ->
            {ok, {MembersCur, TakeOverList}};
        Error ->
            Error
    end;
takeover_status([#?MEMBER{state = ?STATE_ATTACHED,
                          node  = Node,
                          alias = Alias,
                          grp_level_2 = GrpL2} = Member|Rest],
                ClusterId, TakeOverList) ->
    case get_alias(ClusterId, Node, GrpL2) of
        {ok, {SrcMember, Alias_1}} when Alias /= Alias_1 ->
            %% Takeover vnodes:
            %%     Remove vnodes by old-alias,
            %%     then insert vnodes by new-alias
            RingTblCur = table_info(?VER_CUR),
            Member_1 = Member#?MEMBER{alias = Alias_1},

            _ = leo_redundant_manager_chash:remove(RingTblCur, Member),
            _ = leo_redundant_manager_chash:add(RingTblCur, Member_1),

            ok = leo_cluster_tbl_member:insert(
                   ?MEMBER_TBL_CUR, ClusterId, Member_1#?MEMBER{node = Node}),

            case SrcMember of
                [] -> void;
                #?MEMBER{node = SrcNode} ->
                    %% @TODO
                    ok = leo_cluster_tbl_member:insert(
                           ?MEMBER_TBL_CUR, ClusterId,
                           SrcMember#?MEMBER{node = SrcNode,
                                             alias = []})
            end,
            takeover_status(Rest, ClusterId,
                            [{Member, Member_1, SrcMember}|TakeOverList]);
        _ ->
            takeover_status(Rest, ClusterId, TakeOverList)
    end;
takeover_status([_|Rest], ClusterId, TakeOverList) ->
    takeover_status(Rest, ClusterId, TakeOverList).


%% @private
before_rebalance_1([], ClusterId) ->
    %% Synchronize previous-ring
    case synchronize_1(ClusterId, ?SYNC_TARGET_RING_PREV, ?VER_PREV) of
        ok ->
            void;
        {error, Reason} ->
            error_logger:warning_msg("~p,~p,~p,~p~n",
                                     [{module, ?MODULE_STRING},
                                      {function, "after_rebalance_1/0"},
                                      {line, ?LINE},
                                      {body, Reason}])
    end,

    case leo_cluster_tbl_member:find_by_cluster_id(
           ?MEMBER_TBL_PREV, ClusterId) of
        {ok, MembersPrev} ->
            {ok, MembersPrev};
        Error ->
            Error
    end;
before_rebalance_1([#?MEMBER{state = ?STATE_ATTACHED}|Rest], ClusterId) ->
    before_rebalance_1(Rest, ClusterId);
before_rebalance_1([#?MEMBER{state = ?STATE_RESERVED}|Rest], ClusterId) ->
    before_rebalance_1(Rest, ClusterId);
before_rebalance_1([#?MEMBER{} = Member|Rest], ClusterId) ->
    case leo_cluster_tbl_member:insert(?MEMBER_TBL_PREV,
                                       Member#?MEMBER{
                                                 cluster_id = ClusterId,
                                                 state = ?STATE_RUNNING}) of
        ok ->
            before_rebalance_1(Rest, ClusterId);
        Error ->
            Error
    end.


%% @doc After execute rebalance#2:
%%      1. After exec taking over data from detach-node to attach-node
%%      2. Remove detached-nodes fomr ring and members
%%      3. Synchronize previous-ring
%%      4. Export members and ring
%% @private
-spec(after_rebalance(Members, ClusterId) ->
             ok when Members::[{#?MEMBER{},#?MEMBER{},#?MEMBER{}}],
                     ClusterId::cluster_id()).
after_rebalance([], ClusterId) ->
    %% if previous-ring and current-ring has "detached-node(s)",
    %% then remove them, as same as memebers
    case leo_redundant_manager_api:get_members_by_status(
           ClusterId, ?VER_CUR, ?STATE_DETACHED) of
        {ok, DetachedNodes} ->
            TblCur = table_info(?VER_CUR),
            TblPrev = table_info(?VER_PREV),
            ok = lists:foreach(
                   fun(#?MEMBER{node = Node,
                                alias = Alias} = Member) ->
                           %% remove detached node from members
                           leo_cluster_tbl_member:delete(
                             ?MEMBER_TBL_CUR, ClusterId, Node),
                           leo_cluster_tbl_member:delete(
                             ?MEMBER_TBL_PREV, ClusterId, Node),
                           %% remove detached node from ring
                           case Alias of
                               [] ->
                                   void;
                               _  ->
                                   leo_redundant_manager_chash:remove(
                                     TblCur, Member),
                                   leo_redundant_manager_chash:remove(
                                     TblPrev, Member)
                           end
                   end, DetachedNodes);
        {error,_Cause} ->
            ok
    end,

    %% Synchronize previous-ring
    case synchronize_1(ClusterId, ?SYNC_TARGET_RING_PREV, ?VER_PREV) of
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
after_rebalance([{#?MEMBER{node = Node} = Member_1, Member_2, SrcMember}|Rest], ClusterId) ->
    try
        %% After exec taking over data from detach-node to attach-node
        RingTblPrev = table_info(?VER_PREV),
        MembersTblPrev = ?MEMBER_TBL_PREV,

        %% @TODO
        ok = leo_redundant_manager_chash:remove(RingTblPrev, Member_1),
        ok = leo_redundant_manager_chash:add(RingTblPrev, Member_2),
        %% @TODO
        %% ok = leo_cluster_tbl_member:insert(
        %%        MembersTblPrev, {ClusterId, Node, Member_2}),
        ok = leo_cluster_tbl_member:insert(
               MembersTblPrev, Member_2#?MEMBER{cluster_id = ClusterId,
                                                node = Node}),

        case SrcMember of
            [] -> void;
            #?MEMBER{node = SrcNode} ->
                %% @TODO
                ok = leo_cluster_tbl_member:insert(
                       MembersTblPrev, SrcMember#?MEMBER{cluster_id = ClusterId,
                                                         node = SrcNode,
                                                         alias = []})
        end
    catch
        _:Cause ->
            error_logger:warning_msg("~p,~p,~p,~p~n",
                                     [{module, ?MODULE_STRING},
                                      {function, "after_rebalance_1/2"},
                                      {line, ?LINE},
                                      {body, Cause}])
    end,
    after_rebalance(Rest, ClusterId).


%% @doc Generate an alias of the node
-spec(get_alias(ClusterId, Node, GrpL2) ->
             {ok, tuple()} when ClusterId::cluster_id(),
                                Node::atom(),
                                GrpL2::string()).
get_alias(ClusterId, Node, GrpL2) ->
    get_alias(?MEMBER_TBL_CUR, ClusterId, Node, GrpL2).

-spec(get_alias(Table, ClusterId, Node, GrpL2) ->
             {ok, tuple()} when Table::member_table(),
                                ClusterId::cluster_id(),
                                Node::atom(),
                                GrpL2::string()).
get_alias(Table, ClusterId, Node, GrpL2) ->
    case leo_cluster_tbl_member:find_by_status(
           ClusterId, Table, ?STATE_DETACHED) of
        not_found ->
            get_alias_1([], Table, ClusterId, Node, GrpL2);
        {ok, Members} ->
            get_alias_1(Members, Table, ClusterId, Node, GrpL2);
        {error, Cause} ->
            {error, Cause}
    end.

-spec(get_alias(init, Table, ClusterId, Node, GrpL2) ->
             {ok, tuple()} when Table::member_table(),
                                ClusterId::cluster_id(),
                                Node::atom(),
                                GrpL2::string()).
get_alias(init, Table, ClusterId, Node, GrpL2) ->
    get_alias_1([], Table, ClusterId, Node, GrpL2).


%% @private
get_alias_1([],_Table,_ClusterId,Node,_GrpL2) ->
    PartOfAlias = string:substr(
                    leo_hex:binary_to_hex(
                      crypto:hash(md5, lists:append([atom_to_list(Node)]))),1,8),
    {ok, {[], lists:append([?NODE_ALIAS_PREFIX, PartOfAlias])}};

get_alias_1([#?MEMBER{cluster_id = ClusterId_1,
                      node = Node_1}|Rest],
            Table, ClusterId, Node, GrpL2) when ClusterId == ClusterId_1,
                                                Node == Node_1 ->
    get_alias_1(Rest, Table, ClusterId, Node, GrpL2);

get_alias_1([#?MEMBER{cluster_id = ClusterId_1,
                      alias = [],
                      node  = Node_1}|Rest],
            Table, ClusterId, Node, GrpL2) when ClusterId == ClusterId_1,
                                                Node /= Node_1 ->
    get_alias_1(Rest, Table, ClusterId, Node, GrpL2);

get_alias_1([#?MEMBER{cluster_id = ClusterId_1,
                      alias = Alias,
                      node  = Node_1,
                      grp_level_2 = GrpL2_1}|Rest],
            Table, ClusterId, Node, GrpL2) when ClusterId == ClusterId_1,
                                                Node  /= Node_1,
                                                GrpL2 == GrpL2_1 ->
    case leo_cluster_tbl_member:find_by_alias(ClusterId, Alias) of
        {ok, [Member|_]} ->
            {ok, {Member, Member#?MEMBER.alias}};
        _ ->
            get_alias_1(Rest, Table, ClusterId, Node, GrpL2)
    end;

get_alias_1([_|Rest], Table, ClusterId, Node, GrpL2) ->
    get_alias_1(Rest, Table, ClusterId, Node, GrpL2).


%%--------------------------------------------------------------------
%% API-3  FUNCTIONS (leo_member_management_server)
%%--------------------------------------------------------------------
%% @doc Has a member ?
-spec(has_member(ClusterId, Node) ->
             boolean() when ClusterId::cluster_id(),
                            Node::atom()).
has_member(ClusterId, Node) ->
    leo_redundant_manager:has_member(ClusterId, Node).


%% @doc Has charge of node?
%%      'true' is returned even if it detects an error
-spec(has_charge_of_node(ClusterId, Key, NumOfReplicas) ->
             boolean() when ClusterId::cluster_id(),
                            Key::binary(),
                            NumOfReplicas::integer()).
has_charge_of_node(ClusterId, Key, 0) ->
    case leo_cluster_tbl_conf:get(ClusterId) of
        {ok, #?SYSTEM_CONF{n = NumOfReplica}} ->
            has_charge_of_node(ClusterId, Key, NumOfReplica);
        _ ->
            true
    end;
has_charge_of_node(ClusterId, Key, NumOfReplica) ->
    case get_redundancies_by_key(ClusterId, put, Key) of
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
-spec(get_members(ClusterId) ->
             {ok, [#?MEMBER{}]} | {error, any()} when ClusterId::cluster_id()).
get_members(ClusterId) ->
    get_members(ClusterId, ?VER_CUR).

-spec(get_members(ClusterId, Ver) ->
             {ok, Members} | {error, Cause} when ClusterId::cluster_id(),
                                                 Ver::?VER_CUR | ?VER_PREV,
                                                 Members::[#?MEMBER{}],
                                                 Cause::any()).
get_members(ClusterId, Ver) when Ver == ?VER_CUR;
                                 Ver == ?VER_PREV ->
    leo_redundant_manager:get_members(ClusterId, Ver);
get_members(_,_) ->
    {error, invalid_version}.


%% @doc get all version members
-spec(get_all_ver_members(ClusterId) ->
             {ok, {CurMembers, PrevMembers}} |
             {error, Cause} when ClusterId::cluster_id(),
                                 CurMembers::[#?MEMBER{}],
                                 PrevMembers::[#?MEMBER{}],
                                 Cause::any()).
get_all_ver_members(ClusterId) ->
    case get_members(ClusterId, ?VER_CUR) of
        {ok, CurMembers} ->
            case get_members(ClusterId, ?VER_PREV) of
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
-spec(get_member_by_node(ClusterId, Node) ->
             {ok, Member} |
             {error, any()} when ClusterId::cluster_id(),
                                 Node::atom(),
                                 Member::#?MEMBER{}).
get_member_by_node(ClusterId, Node) ->
    leo_redundant_manager:get_member_by_node(
      ClusterId, Node).


%% @doc get # of members.
-spec(get_members_count(ClusterId) ->
             integer() | {error, any()} when ClusterId::cluster_id()).
get_members_count(ClusterId) ->
    leo_cluster_tbl_member:table_size(ClusterId).


%% @doc get members by status
-spec(get_members_by_status(ClusterId, Status) ->
             {ok, Members} |
             {error, any()} when ClusterId::cluster_id(),
                                 Status::atom(),
                                 Members::[#?MEMBER{}]
                                          ).
get_members_by_status(ClusterId, Status) ->
    get_members_by_status(ClusterId, ?VER_CUR, Status).

-spec(get_members_by_status(ClusterId, Ver, Status) ->
             {ok, Members} |
             {error, any()} when ClusterId::cluster_id(),
                                 Ver::?VER_CUR | ?VER_PREV,
                                 Status::atom(),
                                 Members::[#?MEMBER{}]).
get_members_by_status(ClusterId, Ver, Status) ->
    leo_redundant_manager:get_members_by_status(
      ClusterId, Ver, Status).


%% @doc update members.
-spec(update_member(ClusterId, Member) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Member::#?MEMBER{}).
update_member(ClusterId, Member) ->
    case leo_redundant_manager:update_member(
           ClusterId, Member) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc update members.
-spec(update_members(ClusterId, Members) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Members::[#?MEMBER{}]).
update_members(ClusterId, Members) ->
    case leo_redundant_manager:update_members(
           ClusterId, Members) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc update a member by node-name.
-spec(update_member_by_node(ClusterId, Node, State) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Node::atom(),
                                      State::atom()).
update_member_by_node(ClusterId, Node, State) ->
    leo_redundant_manager:update_member_by_node(
      ClusterId, Node, State).

-spec(update_member_by_node(ClusterId, Node, Clock, State) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Node::atom(),
                                      Clock::non_neg_integer(),
                                      State::atom()).
update_member_by_node(ClusterId, Node, Clock, State) ->
    leo_redundant_manager:update_member_by_node(
      ClusterId, Node, Clock, State).


%% @doc remove a member by node-name.
-spec(delete_member_by_node(ClusterId, Node) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Node::atom()).
delete_member_by_node(ClusterId, Node) ->
    leo_redundant_manager:delete_member_by_node(
      ClusterId, Node).


%% @doc Is alive the process?
-spec(is_alive(ClusterId) ->
             ok when ClusterId::cluster_id()).
is_alive(ClusterId) ->
    leo_membership_cluster_local:heartbeat(?id_membership_local(ClusterId)).


%% @doc Retrieve table-info by version.
-spec(table_info(Ver) ->
             ring_table_info() when Ver::?VER_CUR | ?VER_PREV).
-ifdef(TEST).
table_info(?VER_CUR)  ->
    {ets, ?RING_TBL_CUR };
table_info(?VER_PREV) ->
    {ets, ?RING_TBL_PREV}.
-else.
%% @TODO +cluster-id
table_info(?VER_CUR) ->
    %% case leo_misc:get_env(?APP, ?id_red_type(EnvClusterId)) of
    case leo_misc:get_env(?APP, ?PROP_SERVER_TYPE) of
        {ok, ?MONITOR_NODE} ->
            {mnesia, ?RING_TBL_CUR};
        _ ->
            {ets, ?RING_TBL_CUR}
    end;

table_info(?VER_PREV) ->
    %% case leo_misc:get_env(?APP, ?id_red_type(EnvClusterId)) of
    case leo_misc:get_env(?APP, ?PROP_SERVER_TYPE) of
        {ok, ?MONITOR_NODE} ->
            {mnesia, ?RING_TBL_PREV};
        _ ->
            {ets, ?RING_TBL_PREV}
    end.
-endif.


%% @doc Force sync ring-workers
-spec(force_sync_workers(ClusterId) ->
             ok when ClusterId::cluster_id()).
force_sync_workers(ClusterId) ->
    ok = leo_redundant_manager_worker:force_sync(ClusterId, ?RING_TBL_CUR),
    ok = leo_redundant_manager_worker:force_sync(ClusterId, ?RING_TBL_PREV),
    ok.


%% @doc Retrieve local cluster's status
-spec(get_cluster_status(ClusterId) ->
             {ok, #?CLUSTER_STAT{}} | not_found when ClusterId::cluster_id()).
get_cluster_status(ClusterId) ->
    {ok, #?SYSTEM_CONF{cluster_id = ClusterId}} = leo_cluster_tbl_conf:get(ClusterId),
    case get_members(ClusterId) of
        {ok, Members} ->
            Status = judge_cluster_status(Members),
            {ok, {Checksum,_}} = checksum(ClusterId, ?CHECKSUM_MEMBER),
            {ok, #?CLUSTER_STAT{cluster_id = ClusterId,
                                state = Status,
                                checksum = Checksum}};
        _ ->
            not_found
    end.


%% @doc Judge status of local cluster
%% @private
-spec(judge_cluster_status(Members) ->
             node_state() when Members::[#?MEMBER{}]).
judge_cluster_status(Members) ->
    NumOfMembers = length(Members),
    SuspendNode  = length([N || #?MEMBER{state = ?STATE_SUSPEND,
                                         node  = N} <- Members]),
    RunningNode  = length([N || #?MEMBER{state = ?STATE_RUNNING,
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
-spec(get_cluster_tbl_checksums(ClusterId) ->
             {ok, [tuple()]} when ClusterId::cluster_id()).
get_cluster_tbl_checksums(ClusterId) ->
    Chksum_1 = leo_cluster_tbl_conf:checksum(ClusterId),
    Chksum_2 = leo_mdcr_tbl_cluster_info:checksum(),
    Chksum_3 = leo_mdcr_tbl_cluster_mgr:checksum(),
    Chksum_4 = leo_mdcr_tbl_cluster_member:checksum(ClusterId),
    Chksum_5 = leo_mdcr_tbl_cluster_stat:checksum(ClusterId),
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
-spec(get_remote_clusters(ClusterId) ->
             {ok, [#?CLUSTER_INFO{}]} |
             {error, any()} when ClusterId::cluster_id()).
get_remote_clusters(ClusterId) ->
    case leo_cluster_tbl_conf:get(ClusterId) of
        {ok, #?SYSTEM_CONF{max_mdc_targets = MaxTargetClusters}} ->
            get_remote_clusters_by_limit(MaxTargetClusters);
        _ ->
            not_found
    end.

-spec(get_remote_clusters_by_limit(NumOfDestClusters) ->
             {ok, [#?CLUSTER_INFO{}]} |
             {error, any()} when NumOfDestClusters::integer()).
get_remote_clusters_by_limit(NumOfDestClusters) ->
    leo_mdcr_tbl_cluster_info:find_by_limit(NumOfDestClusters).


%% @doc Retrieve remote cluster members
-spec(get_remote_members(ClusterId) ->
             {ok, #?CLUSTER_MEMBER{}} |
             {error, any()} when ClusterId::atom()).
get_remote_members(ClusterId) ->
    get_remote_members(ClusterId, ?DEF_NUM_OF_REMOTE_MEMBERS).

-spec(get_remote_members(ClusterId, NumOfMembers) ->
             {ok, #?CLUSTER_MEMBER{}} |
             {error, any()} when ClusterId::atom(),
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
