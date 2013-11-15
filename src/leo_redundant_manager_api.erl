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
%% ---------------------------------------------------------------------
%% Leo Redundant Manager - API
%% @doc
%% @end
%%======================================================================
-module(leo_redundant_manager_api).

-author('Yosuke Hara').

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([create/0, create/1, create/2, create/3,
         set_options/1, get_options/0,
         attach/1, attach/2, attach/3, attach/4,
         reserve/3, reserve/5, detach/1, detach/2,
         suspend/1, suspend/2, append/3,
         checksum/1, synchronize/2, synchronize/3,
         adjust/1, get_ring/0, get_ring/1, dump/1
        ]).

-export([get_redundancies_by_key/1, get_redundancies_by_key/2,
         get_redundancies_by_addr_id/1, get_redundancies_by_addr_id/2,
         range_of_vnodes/1, rebalance/0
        ]).

-export([has_member/1, has_charge_of_node/1,
         get_members/0, get_members/1, get_member_by_node/1, get_members_count/0,
         get_members_by_status/1, get_members_by_status/2,
         update_member/1, update_members/1, update_member_by_node/3,
         delete_member_by_node/1, is_alive/0, table_info/1
        ]).

-export([get_server_id/0, get_server_id/1]).

-type(method() :: put | get | delete | head).

%%--------------------------------------------------------------------
%% API-1  FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Create the RING
%%
-spec(create() ->
             {ok, list(), list()} | {error, any()}).
create() ->
    case create(?VER_CUR) of
        {ok, Members, HashValues} ->
            case create(?VER_PREV) of
                {ok,_,_} ->
                    {ok, Members, HashValues};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

-spec(create(?VER_CUR|?VER_PREV) ->
             {ok, list(), list()} | {error, any()}).
create(Ver) when Ver == ?VER_CUR;
                 Ver == ?VER_PREV ->
    case leo_redundant_manager:create(Ver) of
        ok ->
            case leo_redundant_manager_table_member:find_all(?member_table(Ver)) of
                {ok, Members} ->
                    {ok, HashRing} = checksum(?CHECKSUM_RING),
                    ok = leo_misc:set_env(?APP, ?PROP_RING_HASH, erlang:element(1, HashRing)),
                    {ok, HashMember} = checksum(?CHECKSUM_MEMBER),
                    {ok, Members, [{?CHECKSUM_RING,   HashRing},
                                   {?CHECKSUM_MEMBER, HashMember}]};
                Error ->
                    Error
            end;
        Error ->
            Error
    end;
create(_) ->
    {error, invlid_version}.


-spec(create(?VER_CUR|?VER_PREV, list()) ->
             {ok, list(), list()} | {error, any()}).
create(Ver, Members) ->
    create(Ver, Members, []).

-spec(create(?VER_CUR|?VER_PREV, list(), list()) ->
             {ok, list(), list()} | {error, any()}).
create(Ver, [], []) ->
    create(Ver);
create(Ver, [], Options) ->
    ok = set_options(Options),
    create(Ver);
create(Ver, [#member{node = Node} = Member|T], Options) when Ver == ?VER_CUR;
                                                             Ver == ?VER_PREV ->
    %% Add a member as "attached node" into member-table
    case leo_redundant_manager_table_member:lookup(Node) of
        not_found ->
            Prop = {Node, Member#member{state = ?STATE_ATTACHED}},
            leo_redundant_manager_table_member:insert(Prop);
        _ ->
            void
    end,
    create(Ver, T, Options);
create(_,_,_) ->
    {error, invalid_version}.



%% @doc set routing-table's options.
%%
-spec(set_options(list()) ->
             ok).
set_options(Options) ->
    ok = leo_misc:set_env(?APP, ?PROP_OPTIONS, Options),
    ok.


%% @doc get routing-table's options.
%%
-spec(get_options() ->
             {ok, list()}).
get_options() ->
    leo_misc:get_env(?APP, ?PROP_OPTIONS).


%% @doc attach a node.
%%
-spec(attach(atom()) ->
             ok | {error, any()}).
attach(Node) ->
    attach(Node, [], leo_date:clock()).
-spec(attach(atom(), string()) ->
             ok | {error, any()}).
attach(Node, NumOfAwarenessL2) ->
    attach(Node, NumOfAwarenessL2, leo_date:clock()).
-spec(attach(atom(), string(), pos_integer()) ->
             ok | {error, any()}).
attach(Node, NumOfAwarenessL2, Clock) ->
    attach(Node, NumOfAwarenessL2, Clock, ?DEF_NUMBER_OF_VNODES).
-spec(attach(atom(), string(), pos_integer(), pos_integer()) ->
             ok | {error, any()}).
attach(Node, NumOfAwarenessL2, Clock, NumOfVNodes) ->
    case leo_redundant_manager:attach(
           Node, NumOfAwarenessL2, Clock, NumOfVNodes) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc reserve a node during in operation
%%
-spec(reserve(atom(), atom(), pos_integer()) ->
             ok | {error, any()}).
reserve(Node, CurState, Clock) ->
    reserve(Node, CurState, [], Clock, 0).

-spec(reserve(atom(), atom(), string(), pos_integer(), pos_integer()) ->
             ok | {error, any()}).
reserve(Node, CurState, NumOfAwarenessL2, Clock, NumOfVNodes) ->
    case leo_redundant_manager:reserve(
           Node, CurState, NumOfAwarenessL2, Clock, NumOfVNodes) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc detach a node.
%%
-spec(detach(atom()) ->
             ok | {error, any()}).
detach(Node) ->
    detach(Node, leo_date:clock()).
detach(Node, Clock) ->
    case leo_redundant_manager:detach(Node, Clock) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc suspend a node. (disable)
%%
-spec(suspend(atom()) ->
             ok | {error, any()}).
suspend(Node) ->
    suspend(Node, leo_date:clock()).
suspend(Node, Clock) ->
    case leo_redundant_manager:suspend(Node, Clock) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc append a node into the ring.
%%
-spec(append(?VER_CUR | ?VER_PREV, integer(), atom()) ->
             ok | {error, invalid_version}).
append(Ver, VNodeId, Node) when Ver == ?VER_CUR;
                                Ver == ?VER_PREV ->
    TblInfo = table_info(Ver),
    ok = leo_redundant_manager_chash:append(TblInfo, VNodeId, Node),
    ok;
append(_,_,_) ->
    {error, invalid_version}.


%% @doc get routing_table's checksum.
%%
-spec(checksum(?CHECKSUM_RING |?CHECKSUM_MEMBER) ->
             {ok, binary()} | {ok, atom()}).
checksum(?CHECKSUM_MEMBER = Type) ->
    leo_redundant_manager:checksum(Type);
checksum(?CHECKSUM_RING) ->
    TblInfo0 = table_info(?VER_CUR),
    TblInfo1 = table_info(?VER_PREV),

    {ok, Chksum0} = leo_redundant_manager_chash:checksum(TblInfo0),
    {ok, Chksum1} = leo_redundant_manager_chash:checksum(TblInfo1),
    {ok, {Chksum0, Chksum1}};
checksum(_) ->
    {error, invalid_type}.


%% @doc synchronize member-list and routing-table.
%%
-spec(synchronize(sync_target(), list(tuple()), list(tuple())) ->
             {ok, list(tuple())} | {error, any()}).
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

-spec(synchronize(sync_target(), list(tuple())) ->
             {ok, list(tuple())} | {error, any()}).
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

synchronize(Target, SyncData) when ?SYNC_TARGET_RING_CUR  == Target;
                                   ?SYNC_TARGET_RING_PREV == Target ->
    Ver = case Target of
              ?SYNC_TARGET_RING_CUR  -> ?VER_CUR;
              ?SYNC_TARGET_RING_PREV -> ?VER_PREV
          end,
    {ok, ChecksumMembers} = synchronize(?SYNC_TARGET_MEMBER, SyncData),

    case synchronize_1(Target, Ver) of
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
            case leo_redundant_manager_table_member:find_all(Table) of
                {ok, OldMembers} ->
                    case leo_redundant_manager_table_member:replace(
                           Table, OldMembers, NewMembers) of
                        ok ->
                            ok;
                        Error ->
                            Error
                    end;
                not_found ->
                    lists:foreach(
                      fun(#member{node = Node} = Member) ->
                              leo_redundant_manager_table_member:insert(Table, {Node, Member})
                      end, NewMembers),
                    ok;
                Error ->
                    Error
            end
    end.

%% @private
synchronize_1(Target, Ver) when Target == ?SYNC_TARGET_RING_CUR;
                                Target == ?SYNC_TARGET_RING_PREV ->
    case leo_redundant_manager_table_ring:delete_all(table_info(Ver)) of
        ok ->
            case create(Ver) of
                {ok,_,_} ->
                    ok;
                Error ->
                    Error
            end;
        Error ->
            Error
    end;
synchronize_1(_,_) ->
    {error, invalid_target}.


%% @doc Adjust current vnode to previous vnode.
%%
-spec(adjust(integer()) ->
             ok | {error, any()}).
adjust(VNodeId) ->
    TblInfo0 = table_info(?VER_CUR),
    TblInfo1 = table_info(?VER_PREV),

    case leo_redundant_manager:adjust(TblInfo0, TblInfo1, VNodeId) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc Retrieve Ring
%%
-spec(get_ring() ->
             {ok, list()} | {error, any()}).
get_ring() ->
    {ok, ets:tab2list(?RING_TBL_CUR)}.

-spec(get_ring(?SYNC_TARGET_RING_CUR | ?SYNC_TARGET_RING_PREV) ->
             {ok, list()}).
get_ring(?SYNC_TARGET_RING_CUR) ->
    TblInfo = table_info(?VER_CUR),
    Ring = leo_redundant_manager_table_ring:tab2list(TblInfo),
    {ok, Ring};
get_ring(?SYNC_TARGET_RING_PREV) ->
    TblInfo = table_info(?VER_PREV),
    Ring = leo_redundant_manager_table_ring:tab2list(TblInfo),
    {ok, Ring}.


%% @doc Dump table-records.
%%
-spec(dump(member | ring) ->
             ok).
dump(Type) ->
    leo_redundant_manager:dump(Type).


%%--------------------------------------------------------------------
%% API-2  FUNCTIONS (leo_routing_table_provide_server)
%%--------------------------------------------------------------------
%% @doc Retrieve redundancies from the ring-table.
%%
-spec(get_redundancies_by_key(string()) ->
             {ok, list(), integer(), integer(), list()} | {error, any()}).
get_redundancies_by_key(Key) ->
    get_redundancies_by_key(default, Key).

-spec(get_redundancies_by_key(method(), string()) ->
             {ok, list(), integer(), integer(), list()} | {error, any()}).
get_redundancies_by_key(Method, Key) ->
    case leo_misc:get_env(?APP, ?PROP_OPTIONS) of
        {ok, Options} ->
            BitOfRing = leo_misc:get_value(?PROP_RING_BIT, Options),
            AddrId = leo_redundant_manager_chash:vnode_id(BitOfRing, Key),

            get_redundancies_by_addr_id_1(ring_table(Method), AddrId, Options);
        _ ->
            {error, not_found}
    end.


%% @doc Retrieve redundancies from the ring-table.
%%
get_redundancies_by_addr_id(AddrId) ->
    get_redundancies_by_addr_id(default, AddrId).

-spec(get_redundancies_by_addr_id(method(), integer()) ->
             {ok, list(), integer(), integer(), list()} | {error, any()}).
get_redundancies_by_addr_id(Method, AddrId) ->
    case leo_misc:get_env(?APP, ?PROP_OPTIONS) of
        {ok, Options} ->
            get_redundancies_by_addr_id_1(ring_table(Method), AddrId, Options);
        _ ->
            {error, not_found}
    end.


%% @private
get_redundancies_by_addr_id_1(TblInfo, AddrId, Options) ->
    ServerRef = get_server_id(AddrId),
    N = leo_misc:get_value(?PROP_N, Options),
    R = leo_misc:get_value(?PROP_R, Options),
    W = leo_misc:get_value(?PROP_W, Options),
    D = leo_misc:get_value(?PROP_D, Options),

    case leo_redundant_manager_chash:redundancies(ServerRef, TblInfo, AddrId) of
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
        Error ->
            Error
    end.


%% @doc Retrieve range of vnodes.
%%
-spec(range_of_vnodes(atom()) ->
             {ok, list()} | {error, any()}).
range_of_vnodes(ToVNodeId) ->
    TblInfo = table_info(?VER_CUR),
    leo_redundant_manager_chash:range_of_vnodes(TblInfo, ToVNodeId).


%% @doc Re-balance objects in the cluster.
%%
-spec(rebalance() ->
             {ok, list()} | {error, any()}).
rebalance() ->
    case leo_redundant_manager_table_member:find_all(?MEMBER_TBL_CUR) of
        {ok, MembersCur} ->
            case leo_redundant_manager_table_member:find_all(?MEMBER_TBL_PREV) of
                {ok, MembersPrev} ->
                    rebalance_1(#rebalance{tbl_cur  = table_info(?VER_CUR),
                                           tbl_prev = table_info(?VER_PREV),
                                           members_cur  = MembersCur,
                                           members_prev = MembersPrev});
                Error ->
                    Error
            end;
        Error ->
            Error
    end.


%% @private
rebalance_1(RebalanceInfo) ->
    %% Remove all previous members,
    %% Then insert new members from current members
    case leo_redundant_manager_table_member:delete_all(?MEMBER_TBL_PREV) of
        ok ->
            case rebalance_1_1(RebalanceInfo#rebalance.members_cur) of
                ok ->
                    leo_redundant_manager_chash:rebalance(RebalanceInfo);
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

%% @private
rebalance_1_1([]) ->
    ok;
rebalance_1_1([#member{state = ?STATE_ATTACHED}|Rest]) ->
    rebalance_1_1(Rest);
rebalance_1_1([#member{state = ?STATE_DETACHED} = _Member|Rest]) ->
    %% ok = leo_redundant_manager_chash:remove(table_info(?VER_PREV), Member),
    rebalance_1_1(Rest);
rebalance_1_1([#member{state = ?STATE_RESERVED}|Rest]) ->
    rebalance_1_1(Rest);
rebalance_1_1([Member|Rest]) ->
    #member{node = Node} = Member,
    case leo_redundant_manager_table_member:insert(?MEMBER_TBL_PREV, {Node, Member}) of
        ok ->
            rebalance_1_1(Rest);
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% API-3  FUNCTIONS (leo_member_management_server)
%%--------------------------------------------------------------------
%% @doc Has a member ?
%%
-spec(has_member(atom()) ->
             boolean()).
has_member(Node) ->
    leo_redundant_manager:has_member(Node).


%% @doc Has charge of node?
%%
-spec(has_charge_of_node(string()) ->
             boolean()).
has_charge_of_node(Key) ->
    case get_redundancies_by_key(put, Key) of
        {ok, #redundancies{nodes = Nodes}} ->
            lists:foldl(fun(#redundant_node{node = N,
                                            can_read_repair = CanReadRepair}, false) ->
                                (N == erlang:node() andalso CanReadRepair == true);
                           (_, true ) ->
                                true
                        end, false, Nodes);
        _ ->
            false
    end.


%% @doc get members.
%%
get_members() ->
    get_members(?VER_CUR).

-spec(get_members(?VER_CUR | ?VER_PREV) ->
             {ok, list()} | {error, any()}).
get_members(Ver) when Ver == ?VER_CUR;
                      Ver == ?VER_PREV ->
    leo_redundant_manager:get_members(Ver);
get_members(_) ->
    {error, invalid_version}.


%% @doc get a member by node-name.
%%
-spec(get_member_by_node(atom()) ->
             {ok, #member{}} | {error, any()}).
get_member_by_node(Node) ->
    leo_redundant_manager:get_member_by_node(Node).


%% @doc get # of members.
%%
-spec(get_members_count() ->
             integer() | {error, any()}).
get_members_count() ->
    leo_redundant_manager_table_member:table_size().


%% @doc get members by status
%%
-spec(get_members_by_status(atom()) ->
             {ok, list(#member{})} | {error, any()}).
get_members_by_status(Status) ->
    get_members_by_status(?VER_CUR, Status).

-spec(get_members_by_status(?VER_CUR | ?VER_PREV, atom()) ->
             {ok, list(#member{})} | {error, any()}).
get_members_by_status(Ver, Status) ->
    leo_redundant_manager:get_members_by_status(Ver, Status).


%% @doc update members.
%%
-spec(update_member(#member{}) ->
             ok | {error, any()}).
update_member(Member) ->
    case leo_redundant_manager:update_member(Member) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc update members.
%%
-spec(update_members(list()) ->
             ok | {error, any()}).
update_members(Members) ->
    case leo_redundant_manager:update_members(Members) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc update a member by node-name.
%%
-spec(update_member_by_node(atom(), integer(), atom()) ->
             ok | {error, any()}).
update_member_by_node(Node, Clock, State) ->
    leo_redundant_manager:update_member_by_node(Node, Clock, State).


%% @doc remove a member by node-name.
%%
-spec(delete_member_by_node(atom()) ->
             ok | {error, any()}).
delete_member_by_node(Node) ->
    leo_redundant_manager:delete_member_by_node(Node).


%% @doc stop membership.
%%
is_alive() ->
    leo_membership:heartbeat().


%% @doc Retrieve table-info by version.
%%
-spec(table_info(?VER_CUR | ?VER_PREV) ->
             ring_table_info()).
-ifdef(TEST).
table_info(?VER_CUR)  -> {ets, ?RING_TBL_CUR };
table_info(?VER_PREV) -> {ets, ?RING_TBL_PREV}.
-else.
table_info(?VER_CUR) ->
    case leo_misc:get_env(?APP, ?PROP_SERVER_TYPE) of
        {ok, ?SERVER_MANAGER} ->
            {mnesia, ?RING_TBL_CUR};
        _ ->
            {ets, ?RING_TBL_CUR}
    end;

table_info(?VER_PREV) ->
    case leo_misc:get_env(?APP, ?PROP_SERVER_TYPE) of
        {ok, ?SERVER_MANAGER} ->
            {mnesia, ?RING_TBL_PREV};
        _ ->
            {ets, ?RING_TBL_PREV}
    end.
-endif.


%% @doc Retrieve a srever id
%% @private
get_server_id() ->
    get_server_id(leo_date:clock()).
get_server_id(AddrId) ->
    Procs = ?RING_WORKER_POOL_SIZE,
    Index = erlang:phash2(AddrId, Procs),
    list_to_atom(lists:append([?WORKER_POOL_NAME_PREFIX,
                               integer_to_list(Index)])).

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

