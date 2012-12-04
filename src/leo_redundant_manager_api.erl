%%======================================================================
%%
%% Leo Redundant Manager
%%
%% Copyright (c) 2012 Rakuten, Inc.
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

-export([start/0, start/1, start/3, start/4, stop/0,
         create/0, create/1, create/2,
         set_options/1, get_options/0,
         attach/1, attach/2, detach/1, detach/2,
         suspend/1, suspend/2, append/3,
         checksum/1, synchronize/2, synchronize/3, adjust/1,
         get_ring/0, dump/1
        ]).

-export([get_redundancies_by_key/1, get_redundancies_by_key/2,
         get_redundancies_by_addr_id/1, get_redundancies_by_addr_id/2,
         range_of_vnodes/1, rebalance/0
        ]).

-export([has_member/1, has_charge_of_node/1,
         get_members/0, get_members/1, get_member_by_node/1, get_members_count/0,
         get_members_by_status/1,
         update_members/1, update_member_by_node/3,
         get_ring/1, is_alive/0, table_info/1
        ]).

-type(method()      :: put | get | delete | head).
-type(server_type() :: master | slave | gateway | storage).

-ifdef(TEST).
-define(MNESIA_TYPE_COPIES, 'ram_copies').
-define(MODULE_SET_ENV_1(), application:set_env(?APP, 'notify_mf', [leo_manager_api, notify])).
-define(MODULE_SET_ENV_2(), application:set_env(?APP, 'sync_mf',   [leo_manager_api, synchronize])).
-else.
-define(MNESIA_TYPE_COPIES, 'disc_copies').
-define(MODULE_SET_ENV_1(), void).
-define(MODULE_SET_ENV_2(), void).
-endif.

%%--------------------------------------------------------------------
%% API-1  FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Start this application.
%%
-spec(start(server_type(), list(), string()) ->
             ok | {error, any()}).
start(ServerType0, Managers, MQStoragePath) ->
    start(ServerType0, Managers, MQStoragePath, []).

start(ServerType0, Managers, MQStoragePath, Options) ->
    case start(ServerType0) of
        ok ->
            ServerType1 = server_type(ServerType0),
            ok = application:set_env(?APP, ?PROP_SERVER_TYPE, ServerType1, 3000),

            case (Options == []) of
                true  -> void;
                false -> ok = set_options(Options)
            end,

            Args = [ServerType1, Managers],
            ChildSpec = {leo_membership, {leo_membership, start_link, Args},
                         permanent, 2000, worker, [leo_membership]},

            case supervisor:start_child(leo_redundant_manager_sup, ChildSpec) of
                {ok, _Pid} ->
                    start_membership(ServerType1, MQStoragePath);
                Cause ->
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING}, {function, "start/4"},
                                            {line, ?LINE}, {body, Cause}]),
                    case leo_redundant_manager_sup:stop() of
                        ok ->
                            exit(invalid_launch);
                        not_started ->
                            exit(noproc)
                    end
            end;
        Error ->
            Error
    end.

-spec(start() ->
             ok | {error, any()}).
start()  ->
    start(master).

-spec(start(server_type()) ->
             ok | {error, any()}).
start(ServerType0)  ->
    start_app(ServerType0).


%% @doc Stop this application.
%%
-spec(stop() ->
             ok).
stop() ->
    application:stop(leo_mq),
    application:stop(leo_backend_db),
    application:stop(leo_redundant_manager),
    halt(),
    ok.


%% @doc Create the RING
%%
-spec(create() ->
             {ok, list(), list()} | {error, any()}).
create() ->
    case leo_redundant_manager:create() of
        {ok, Members} ->
            {ok, Chksums} = checksum(?CHECKSUM_RING),
            {CurRingHash, _PrevRingHash} = Chksums,
            ok = application:set_env(?APP, ?PROP_RING_HASH, CurRingHash, 3000),

            {ok, Chksum0} = checksum(?CHECKSUM_MEMBER),
            {ok, Members, [{?CHECKSUM_RING,   Chksums},
                           {?CHECKSUM_MEMBER, Chksum0}]};
        Error ->
            Error
    end.

-spec(create(list()) ->
             {ok, list(), list()} | {error, any()}).
create([]) ->
    create();
create([#member{node = Node, clock = Clock}|T]) ->
    ok = attach(Node, Clock),
    create(T).

-spec(create(list(), list()) ->
             {ok, list(), list()} | {error, any()}).
create([], Options) ->
    ok = set_options(Options),
    create();
create([#member{node = Node, clock = Clock}|T], Options) ->
    ok = attach(Node, Clock),
    create(T, Options).


%% @doc set routing-table's options.
%%
-spec(set_options(list()) ->
             ok).
set_options(Options) ->
    ok = application:set_env(?APP, ?PROP_OPTIONS, Options, 3000),
    ok.


%% @doc get routing-table's options.
%%
-spec(get_options() ->
             {ok, list()}).
get_options() ->
    application:get_env(?APP, ?PROP_OPTIONS).


%% @doc attach a node.
%%
-spec(attach(atom()) ->
             ok | {error, any()}).
attach(Node) ->
    attach(Node, leo_date:clock()).
attach(Node, Clock) ->
    attach(Node, Clock, ?DEF_NUMBER_OF_VNODES).
attach(Node, Clock, NumOfVNodes) ->
    case leo_redundant_manager:attach(Node, Clock, NumOfVNodes) of
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
-spec(append(?VER_CURRENT | ?VER_PREV, integer(), atom()) ->
             ok).
append(?VER_CURRENT, VNodeId, Node) ->
    TblInfo = table_info(?VER_CURRENT),
    ok = leo_redundant_manager_chash:append(TblInfo, VNodeId, Node),
    ok;
append(?VER_PREV,    VNodeId, Node) ->
    TblInfo = table_info(?VER_PREV),
    ok = leo_redundant_manager_chash:append(TblInfo, VNodeId, Node),
    ok.


%% @doc get routing_table's checksum.
%%
-spec(checksum(?CHECKSUM_RING |?CHECKSUM_MEMBER) ->
             {ok, binary()} | {ok, atom()}).
checksum(?CHECKSUM_MEMBER = Type) ->
    leo_redundant_manager:checksum(Type);
checksum(?CHECKSUM_RING) ->
    TblInfo0 = table_info(?VER_CURRENT),
    TblInfo1 = table_info(?VER_PREV),

    {ok, Chksum0} = leo_redundant_manager_chash:checksum(TblInfo0),
    {ok, Chksum1} = leo_redundant_manager_chash:checksum(TblInfo1),
    {ok, {Chksum0, Chksum1}};
checksum(_) ->
    {error, badarg}.


%% @doc synchronize member-list and routing-table.
%%
-spec(synchronize(sync_mode() | list(), list(), list()) ->
             {ok, list(), list()} | {error, any()}).
synchronize(?SYNC_MODE_BOTH, Members, Options) ->
    case leo_redundant_manager:update_members(Members) of
        ok ->
            create(Members, Options);
        Error ->
            Error
    end;

synchronize([],_Ring0,_Acc) ->
    checksum(?CHECKSUM_RING);

synchronize([RingVer|T], Ring0, Acc) ->
    Ret = synchronize(RingVer, Ring0),
    synchronize(T, Ring0, [Ret|Acc]).


-spec(synchronize(sync_mode(), list()) ->
             {ok, integer()} | {error, any()}).
synchronize(?SYNC_MODE_MEMBERS, Members) ->
    case leo_redundant_manager:update_members(Members) of
        ok ->
            leo_redundant_manager:checksum(?CHECKSUM_MEMBER);
        Error ->
            Error
    end;

synchronize(?SYNC_MODE_CUR_RING = Ver, Ring0) ->
    {ok, Ring1} = get_ring(Ver),
    TblInfo = table_info(?VER_CURRENT),

    case leo_redundant_manager:synchronize(TblInfo, Ring0, Ring1) of
        ok ->
            {ok, {CurRingHash, _PrevRingHash}} = checksum(?CHECKSUM_RING),
            ok = application:set_env(?APP, ?PROP_RING_HASH, CurRingHash, 3000),
            checksum(?CHECKSUM_RING);
        Error ->
            Error
    end;

synchronize(?SYNC_MODE_PREV_RING = Ver, Ring0) ->
    {ok, Ring1} = get_ring(Ver),
    TblInfo = table_info(?VER_PREV),

    case leo_redundant_manager:synchronize(TblInfo, Ring0, Ring1) of
        ok ->
            checksum(?CHECKSUM_RING);
        Error ->
            Error
    end;

synchronize(Ver, Ring0) when is_list(Ver) ->
    synchronize(Ver, Ring0, []);

synchronize(_, _) ->
    {error, badarg}.


%% @doc Adjust current vnode to previous vnode.
%%
-spec(adjust(integer()) ->
             ok | {error, any()}).
adjust(VNodeId) ->
    TblInfo0 = table_info(?VER_CURRENT),
    TblInfo1 = table_info(?VER_PREV),

    case leo_redundant_manager:adjust(TblInfo0, TblInfo1, VNodeId) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc Retrieve Current Ring
%%
-spec(get_ring() ->
             {ok, list()} | {error, any()}).
get_ring() ->
    {ok, ets:tab2list(?CUR_RING_TABLE)}.


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
    case application:get_env(?APP, ?PROP_OPTIONS) of
        {ok, Options} ->
            BitOfRing = leo_misc:get_value('bit_of_ring', Options),
            AddrId = leo_redundant_manager_chash:vnode_id(BitOfRing, Key),

            get_redundancies_by_addr_id(ring_table(Method), AddrId, Options);
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
    case application:get_env(?APP, ?PROP_OPTIONS) of
        {ok, Options} ->
            get_redundancies_by_addr_id(ring_table(Method), AddrId, Options);
        _ ->
            {error, not_found}
    end.

get_redundancies_by_addr_id(TblInfo, AddrId, Options) ->
    {ok, ServerType} = application:get_env(?APP, ?PROP_SERVER_TYPE),
    get_redundancies_by_addr_id(ServerType, TblInfo, AddrId, Options).

get_redundancies_by_addr_id(?SERVER_MANAGER, TblInfo, AddrId, Options) ->
    Ret = leo_redundant_manager_table_member:find_all(),
    get_redundancies_by_addr_id_1(Ret, TblInfo, AddrId, Options);

get_redundancies_by_addr_id(_ServerType, TblInfo, AddrId, Options) ->
    Ret = leo_redundant_manager_table_member:find_all(),
    get_redundancies_by_addr_id_1(Ret, TblInfo, AddrId, Options).

get_redundancies_by_addr_id_1({ok, Members}, TblInfo, AddrId, Options) ->
    N = leo_misc:get_value('n', Options),
    R = leo_misc:get_value('r', Options),
    W = leo_misc:get_value('w', Options),
    D = leo_misc:get_value('d', Options),

    case leo_redundant_manager_chash:redundancies(TblInfo, AddrId, N, Members) of
        {ok, Redundancies} ->
            CurRingHash = case application:get_env(?APP, ?PROP_RING_HASH) of
                              {ok, RingHash} ->
                                  RingHash;
                              undefined ->
                                  {ok, {RingHash, _}} = checksum(?CHECKSUM_RING),
                                  ok = application:set_env(?APP, ?PROP_RING_HASH, RingHash, 3000),
                                  RingHash
                          end,
            {ok, Redundancies#redundancies{n = N,
                                           r = R,
                                           w = W,
                                           d = D,
                                           ring_hash = CurRingHash}};
        Error ->
            Error
    end;
get_redundancies_by_addr_id_1(Error, _TblInfo, _AddrId, _Options) ->
    error_logger:warning_msg("~p,~p,~p,~p~n",
                             [{module, ?MODULE_STRING}, {function, "get_redundancies_by_addr_id_1/4"},
                              {line, ?LINE}, {body, Error}]),
    {error, not_found}.


%% @doc Retrieve range of vnodes.
%%
-spec(range_of_vnodes(atom()) ->
             {ok, list()} | {error, any()}).
range_of_vnodes(ToVNodeId) ->
    TblInfo = table_info(?VER_CURRENT),
    leo_redundant_manager_chash:range_of_vnodes(TblInfo, ToVNodeId).


%% @doc Re-balance objects in the cluster.
%%
-spec(rebalance() ->
             {ok, list()} | {error, any()}).
rebalance() ->
    case application:get_env(?APP, ?PROP_OPTIONS) of
        {ok, Options} ->
            N = leo_misc:get_value('n', Options),

            ServerType = application:get_env(?APP, ?PROP_SERVER_TYPE),
            rebalance(ServerType, N);
        Error ->
            Error
    end.

rebalance(?SERVER_MANAGER, N) ->
    Ret = leo_redundant_manager_table_member:find_all(),
    rebalance_1(Ret, N);
rebalance(_, N) ->
    Ret = leo_redundant_manager_table_member:find_all(),
    rebalance_1(Ret, N).

rebalance_1({ok, Members}, N) ->
    TblInfo0 = table_info(?VER_CURRENT),
    TblInfo1 = table_info(?VER_PREV),

    leo_redundant_manager_chash:rebalance({TblInfo0, TblInfo1}, N, Members);
rebalance_1(Error,_N) ->
    Error.


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
            lists:foldl(fun({N, _}, false) -> N == erlang:node();
                           ({_, _}, true ) -> true
                        end, false, Nodes);
        _ ->
            false
    end.


%% @doc get members.
%%
get_members() ->
    get_members(?VER_CURRENT).

-spec(get_members(?VER_CURRENT | ?VER_PREV) ->
             {ok, list()} | {error, any()}).
get_members(?VER_CURRENT = Ver) ->
    leo_redundant_manager:get_members(Ver);

get_members(?VER_PREV = Ver) ->
    leo_redundant_manager:get_members(Ver).


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
    leo_redundant_manager_table_member:size().


%% @doc get members by status
%%
-spec(get_members_by_status(atom()) ->
             {ok, list(#member{})} | {error, any()}).
get_members_by_status(Status) ->
    leo_redundant_manager:get_members_by_status(Status).


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
    case leo_redundant_manager:update_member_by_node(Node, Clock, State) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc Retrieve ring by version.
%%
-spec(get_ring(?SYNC_MODE_CUR_RING | ?SYNC_MODE_PREV_RING) ->
             {ok, list()}).
get_ring(?SYNC_MODE_CUR_RING) ->
    TblInfo = table_info(?VER_CURRENT),
    Ring = leo_redundant_manager_table_ring:tab2list(TblInfo),
    {ok, Ring};
get_ring(?SYNC_MODE_PREV_RING) ->
    TblInfo = table_info(?VER_PREV),
    Ring = leo_redundant_manager_table_ring:tab2list(TblInfo),
    {ok, Ring}.


%% @doc stop membership.
%%
is_alive() ->
    leo_membership:heartbeat().


%% @doc Retrieve table-info by version.
%%
-spec(table_info(?VER_CURRENT | ?VER_PREV) ->
             ring_table_info()).
table_info(?VER_CURRENT) ->
    {ok, TblInfo} = application:get_env(?APP, ?PROP_CUR_RING_TBL),
    TblInfo;
table_info(?VER_PREV) ->
    {ok, TblInfo} = application:get_env(?APP, ?PROP_PREV_RING_TBL),
    TblInfo.


%%--------------------------------------------------------------------
%% INNTERNAL FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Retrieve a server-type.
%% @private
server_type(master) -> ?SERVER_MANAGER;
server_type(slave)  -> ?SERVER_MANAGER;
server_type(Type)   -> Type.


%% @doc Launch the application.
%% @private
-spec(start_app(server_type()) ->
             ok | {error, any()}).
start_app(ServerType) ->
    Module = leo_redundant_manager,

    case application:start(Module) of
        ok ->
            ?MODULE_SET_ENV_1(),
            ?MODULE_SET_ENV_2(),

            ok = init_members_table(ServerType),
            ok = init_ring_tables(ServerType),
            ok;
        {error, {already_started, Module}} ->
            ok;
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "start_app/1"},
                                    {line, ?LINE}, {body, Cause}]),
            {exit, Cause}
    end.


%% @doc Create members table.
%% @private
-spec(init_members_table(server_type()) ->
             ok).
init_members_table(manager) ->
    ok;
init_members_table(master) ->
    ok;
init_members_table(slave) ->
    ok;
init_members_table(_Other) ->
    ok = leo_redundant_manager_table_member:create_members(),
    ok.


%% @doc Set ring-table related values.
%% @private
-spec(init_ring_tables(server_type()) ->
             ok).
init_ring_tables(master) ->
    ok = application:set_env(?APP, ?PROP_CUR_RING_TBL,  {mnesia, ?CUR_RING_TABLE},  3000),
    ok = application:set_env(?APP, ?PROP_PREV_RING_TBL, {mnesia, ?PREV_RING_TABLE}, 3000),
    ok;
init_ring_tables(slave) ->
    ok = application:set_env(?APP, ?PROP_CUR_RING_TBL,  {mnesia, ?CUR_RING_TABLE},  3000),
    ok = application:set_env(?APP, ?PROP_PREV_RING_TBL, {mnesia, ?PREV_RING_TABLE}, 3000),
    ok;
init_ring_tables(_Other) ->
    ok = application:set_env(?APP, ?PROP_CUR_RING_TBL,  {ets, ?CUR_RING_TABLE},  3000),
    ok = application:set_env(?APP, ?PROP_PREV_RING_TBL, {ets, ?PREV_RING_TABLE}, 3000),
    catch ets:new(?CUR_RING_TABLE, [named_table, ordered_set, public, {read_concurrency, true}]),
    catch ets:new(?PREV_RING_TABLE,[named_table, ordered_set, public, {read_concurrency, true}]),
    ok.


%% @doc Launch the membership.
%% @private
-spec(start_membership(server_type(), string()) ->
             ok).
start_membership(ServerType, Path) ->
    ok = leo_membership_mq_client:start(ServerType, Path),
    ok = leo_membership:start_heartbeat().


%% @doc Specify ETS's table.
%% @private
-spec(ring_table(method()) ->
             ring_table_info()).
ring_table(default) -> table_info(?VER_CURRENT);
ring_table(put)     -> table_info(?VER_CURRENT);
ring_table(get)     -> table_info(?VER_PREV);
ring_table(delete)  -> table_info(?VER_CURRENT);
ring_table(head)    -> table_info(?VER_PREV).

