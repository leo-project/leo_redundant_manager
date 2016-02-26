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
%% Leo Redundant Manageme - Server
%%
%% @doc leo_redaundant_manager's server
%% @reference https://github.com/leo-project/leo_redundant_manager/blob/master/src/leo_redundant_manager_api.erl
%% @end
%%======================================================================
-module(leo_redundant_manager).

-behaviour(gen_server).

-include("leo_redundant_manager.hrl").
-include_lib("leo_rpc/include/leo_rpc.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/1, stop/1]).

-export([create/2, checksum/2, has_member/2, get_members/1, get_members/2,
         get_member_by_node/2, get_members_by_status/3,
         update_member/2,
         update_members/2, update_members/4,
         update_member_by_node/3, update_member_by_node/4,
         delete_member_by_node/2, dump/2]).

-export([attach/2, reserve/2,
         detach/3,  detach/4, suspend/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-undef(DEF_TIMEOUT).
-define(DEF_TIMEOUT, timer:seconds(30)).
-define(DEF_TIMEOUT_LONG, timer:seconds(120)).

-record(state, {id :: atom(),
                cluster_id :: cluster_id()
               }).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Start the process
start_link(ClusterId) ->
    Id = ?id_red_server(ClusterId),
    gen_server:start_link({local, Id}, ?MODULE, [Id, ClusterId], []).

%% @doc Stop the process
stop(ClusterId) ->
    Id = ?id_red_server(ClusterId),
    gen_server:call(Id, stop, ?DEF_TIMEOUT).


%% @doc Create the Rings.
-spec(create(ClusterId, Ver) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Ver::?VER_CUR|?VER_PREV).
create(ClusterId, Ver) ->
    Id = ?id_red_server(ClusterId),
    gen_server:call(Id, {create, Ver}, ?DEF_TIMEOUT_LONG).


%% @doc Retrieve checksum of ring/member
-spec(checksum(ClusterId, Type) ->
             {ok, integer() | tuple()} when ClusterId::cluster_id(),
                                            Type::checksum_type()).
checksum(ClusterId, Type) ->
    Id = ?id_red_server(ClusterId),
    gen_server:call(Id, {checksum, Type}, ?DEF_TIMEOUT).


%% @doc Is exists member?
-spec(has_member(ClusterId, Node) ->
             boolean() when ClusterId::cluster_id(),
                            Node::atom()).
has_member(ClusterId, Node) ->
    Id = ?id_red_server(ClusterId),
    gen_server:call(Id, {has_member, Node}, ?DEF_TIMEOUT).


%% @doc Retrieve all members.
-spec(get_members(ClusterId) ->
             {ok, [#?MEMBER{}]} | {error, any()} when ClusterId::cluster_id()).
get_members(ClusterId) ->
    Id = ?id_red_server(ClusterId),
    gen_server:call(Id, {get_members, ?VER_CUR}, ?DEF_TIMEOUT).

-spec(get_members(ClusterId, Ver) ->
             {ok, [#?MEMBER{}]} | {error, any()} when ClusterId::cluster_id(),
                                                      Ver::?VER_CUR|?VER_PREV).
get_members(ClusterId, Ver) ->
    Id = ?id_red_server(ClusterId),
    gen_server:call(Id, {get_members, Ver}, ?DEF_TIMEOUT).


%% @doc Retrieve a member by node.
-spec(get_member_by_node(ClusterId, Node) ->
             {ok, #?MEMBER{}} | {error, any()} when ClusterId::cluster_id(),
                                                    Node::atom()).
get_member_by_node(ClusterId, Node) ->
    Id = ?id_red_server(ClusterId),
    gen_server:call(Id, {get_member_by_node, Node}, ?DEF_TIMEOUT).


%% @doc Retrieve members by status.
-spec(get_members_by_status(ClusterId, Ver, Status) ->
             {ok, [#?MEMBER{}]} |
             {error, any()} when ClusterId::cluster_id(),
                                 Ver::?VER_CUR|?VER_PREV,
                                 Status::atom()).
get_members_by_status(ClusterId, Ver, Status) ->
    Id = ?id_red_server(ClusterId),
    gen_server:call(Id, {get_members_by_status, Ver, Status}, ?DEF_TIMEOUT).


%% @doc Modify a member.
-spec(update_member(ClusterId, Member) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Member::#?MEMBER{}).
update_member(ClusterId, Member) ->
    Id = ?id_red_server(ClusterId),
    gen_server:call(Id, {update_member, Member}, ?DEF_TIMEOUT).


%% @doc Modify members.
-spec(update_members(ClusterId, Members) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Members::[#?MEMBER{}]).
update_members(ClusterId, Members) ->
    Id = ?id_red_server(ClusterId),
    gen_server:call(Id, {update_members, Members}, ?DEF_TIMEOUT).

-spec(update_members(ClusterId, Table, OldMembers, NewMembers) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Table::atom(),
                                      OldMembers::[#?MEMBER{}],
                                      NewMembers::[#?MEMBER{}]).
update_members(ClusterId, Table, OldMembers, NewMembers) ->
    Id = ?id_red_server(ClusterId),
    gen_server:call(Id, {update_members, Table, OldMembers, NewMembers}, ?DEF_TIMEOUT).


%% @doc Modify a member by node.
-spec(update_member_by_node(ClusterId, Node, NodeState) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Node::atom(),
                                      NodeState::atom()).
update_member_by_node(ClusterId, Node, NodeState) ->
    Id = ?id_red_server(ClusterId),
    gen_server:call(Id, {update_member_by_node, Node, NodeState}, ?DEF_TIMEOUT).

-spec(update_member_by_node(ClusterId, Node, Clock, NodeState) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Node::atom(),
                                      Clock::integer(),
                                      NodeState::atom()).
update_member_by_node(ClusterId, Node, Clock, NodeState) ->
    Id = ?id_red_server(ClusterId),
    gen_server:call(Id, {update_member_by_node, Node, Clock, NodeState}, ?DEF_TIMEOUT).


%% @doc Remove a member by node.
-spec(delete_member_by_node(ClusterId, Node) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Node::atom()).
delete_member_by_node(ClusterId, Node) ->
    Id = ?id_red_server(ClusterId),
    gen_server:call(Id, {delete_member_by_node, Node}, ?DEF_TIMEOUT).


%% @doc Dump files which are member and ring.
-spec(dump(ClusterId, Type) ->
             ok when ClusterId::cluster_id(),
                     Type::atom()).
dump(ClusterId, Type) ->
    Id = ?id_red_server(ClusterId),
    gen_server:call(Id, {dump, Type}, ?DEF_TIMEOUT).


%% @doc Change node status to 'attach'.
-spec(attach(ClusterId, Member) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Member::#?MEMBER{}).
attach(ClusterId, #?MEMBER{} = Member) ->
    Id = ?id_red_server(ClusterId),
    TblInfo = leo_redundant_manager_api:table_info(?VER_CUR),
    gen_server:call(Id, {attach, TblInfo, Member}, ?DEF_TIMEOUT).


%% @doc Change node status to 'reserve'.
-spec(reserve(ClusterId, Member) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Member::#?MEMBER{}).
reserve(ClusterId, Member) ->
    Id = ?id_red_server(ClusterId),
    gen_server:call(Id, {reserve, Member}, ?DEF_TIMEOUT).


%% @doc Change node status to 'detach'.
-spec(detach(ClusterId, Node, Clock) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Node::atom(),
                                      Clock::integer()).
detach(ClusterId, Node, Clock) ->
    TblInfo = leo_redundant_manager_api:table_info(?VER_CUR),
    detach(ClusterId, TblInfo, Node, Clock).

-spec(detach(ClusterId, TableInfo, Node, Clock) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      TableInfo::ring_table_info(),
                                      Node::atom(),
                                      Clock::integer()).
detach(ClusterId, TableInfo, Node, Clock) ->
    Id = ?id_red_server(ClusterId),
    gen_server:call(Id, {detach, TableInfo, Node, Clock}, ?DEF_TIMEOUT).


%% @doc Change node status to 'suspend'.
-spec(suspend(ClusterId, Node, Clock) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Node::atom(),
                                      Clock::integer()).
suspend(ClusterId, Node, Clock) ->
    Id = ?id_red_server(ClusterId),
    gen_server:call(Id, {suspend, Node, Clock}, ?DEF_TIMEOUT).


%%--------------------------------------------------------------------
%% GEN_SERVER CALLBACKS
%%--------------------------------------------------------------------
%% @doc Initiates the server
init([Id, ClusterId]) ->
    {ok, #state{id = Id,
                cluster_id = ClusterId}}.

%% @doc gen_server callback - Module:handle_call(Request, From, State) -> Result
handle_call(stop,_From,State) ->
    {stop, normal, ok, State};


handle_call({create, Ver},_From, #state{cluster_id = ClusterId} = State) when Ver == ?VER_CUR;
                                                                              Ver == ?VER_PREV ->
    Reply = create_1(ClusterId, Ver),
    {reply, Reply, State};

handle_call({create,_Ver}, _From, State) ->
    {reply, {error, invalid_version}, State};

handle_call({checksum, ?CHECKSUM_MEMBER}, _From, #state{cluster_id = ClusterId} = State) ->
    HashCur = case leo_cluster_tbl_member:find_by_cluster_id(
                     ?MEMBER_TBL_CUR, ClusterId) of
                  {ok, MembersCur} ->
                      erlang:crc32(
                        term_to_binary(lists:sort(MembersCur)));
                  _ ->
                      -1
              end,
    HashPrv = case leo_cluster_tbl_member:find_by_cluster_id(
                     ?MEMBER_TBL_PREV, ClusterId) of
                  {ok, MembersPrev} ->
                      erlang:crc32(
                        term_to_binary(lists:sort(MembersPrev)));
                  _ ->
                      -1
              end,
    {reply, {ok, {HashCur, HashPrv}}, State};

handle_call({checksum, _}, _From, State) ->
    {reply, {error, badarg}, State};

handle_call({has_member, Node}, _From, #state{cluster_id = ClusterId} = State) ->
    Reply = case leo_cluster_tbl_member:lookup(ClusterId, Node) of
                {ok, _} ->
                    true;
                _Other ->
                    false
            end,
    {reply, Reply, State};

handle_call({get_members, Ver}, _From, #state{cluster_id = ClusterId} = State) ->
    Reply = get_members_1(Ver, ClusterId),
    {reply, Reply, State};

handle_call({get_member_by_node, Node}, _From, #state{cluster_id = ClusterId} = State) ->
    Reply = case leo_cluster_tbl_member:lookup(ClusterId, Node) of
                {ok, Member} ->
                    {ok, Member};
                not_found = Cause ->
                    {error, Cause};
                Error ->
                    Error
            end,
    {reply, Reply, State};

handle_call({get_members_by_status, Ver, Status}, _From, #state{cluster_id = ClusterId} = State) ->
    Table = ?member_table(Ver),
    %% @TODO
    Reply = case leo_cluster_tbl_member:find_by_status(
                   Table, ClusterId, Status) of
                {ok, Members} ->
                    {ok, Members};
                not_found = Cause ->
                    {error, Cause};
                Error ->
                    Error
            end,
    {reply, Reply, State};

handle_call({update_member, #?MEMBER{state = MemberState} = Member},
            _From, #state{cluster_id = ClusterId} = State) ->
    Reply = case ?is_correct_state(MemberState) of
                true ->
                    leo_cluster_tbl_member:insert(
                      Member#?MEMBER{cluster_id = ClusterId});
                false ->
                    ok
            end,
    {reply, Reply, State};

handle_call({update_members, Members}, _From, #state{cluster_id = ClusterId} = State) ->
    %% @TODO
    Reply = case leo_cluster_tbl_member:find_by_cluster_id(ClusterId) of
                {ok, CurMembers} ->
                    CurMembers_1 = lists:reverse(CurMembers),
                    CurMembersHash = erlang:crc32(term_to_binary(CurMembers_1)),
                    MembersHash = erlang:crc32(term_to_binary(Members)),

                    case (MembersHash =:= CurMembersHash) of
                        true ->
                            ok;
                        false ->
                            %% @TODO
                            leo_cluster_tbl_member:replace(
                              ClusterId, CurMembers_1, Members)
                    end;
                not_found ->
                    %% @TODO
                    leo_cluster_tbl_member:replace(ClusterId, [], Members);
                Error ->
                    Error
            end,
    {reply, Reply, State};

handle_call({update_members, Table, OldMembers, NewMembers},
            _From, #state{cluster_id = ClusterId} = State) ->
    %% @TODO
    Reply = leo_cluster_tbl_member:replace(
              Table, ClusterId, OldMembers, NewMembers),
    {reply, Reply, State};

handle_call({update_member_by_node, Node, NodeState}, _From,
            #state{cluster_id = ClusterId} = State) ->
    Reply = case ?is_correct_state(NodeState) of
                true ->
                    update_member_by_node_1(ClusterId, Node, NodeState);
                false ->
                    {error, incorrect_node_state}
            end,
    {reply, Reply, State};

handle_call({update_member_by_node, Node, Clock, NodeState}, _From,
            #state{cluster_id = ClusterId} = State) ->
    Reply = case ?is_correct_state(NodeState) of
                true ->
                    update_member_by_node_1(ClusterId, Node, Clock, NodeState);
                false ->
                    {error, incorrect_node_state}
            end,
    {reply, Reply, State};

handle_call({delete_member_by_node, Node},
            _From, #state{cluster_id = ClusterId} = State) ->
    %% @TODO
    Reply = leo_cluster_tbl_member:delete(ClusterId, Node),
    {reply, Reply, State};

handle_call({dump, member}, _From, #state{cluster_id = ClusterId} = State) ->
    LogDir = case application:get_env(leo_redundant_manager,
                                      log_dir_member) of
                 undefined ->
                     ?DEF_LOG_DIR_MEMBERS;
                 {ok, Dir} ->
                     case (string:len(Dir) == string:rstr(Dir, "/")) of
                         true  -> Dir;
                         false -> Dir ++ "/"
                     end
             end,
    _ = filelib:ensure_dir(LogDir),

    %% @TODO
    Reply = case leo_cluster_tbl_member:find_by_cluster_id(
                   ?MEMBER_TBL_CUR, ClusterId) of
                {ok, MembersCur} ->
                    Path_1 = lists:append([LogDir,
                                           ?DUMP_FILE_MEMBERS_CUR,
                                           integer_to_list(leo_date:now())]),
                    leo_file:file_unconsult(Path_1, MembersCur),

                    %% @TODO
                    case leo_cluster_tbl_member:find_by_cluster_id(
                           ?MEMBER_TBL_PREV, ClusterId) of
                        {ok, MembersPrev} ->
                            Path_2 = lists:append([LogDir,
                                                   ?DUMP_FILE_MEMBERS_PREV,
                                                   integer_to_list(leo_date:now())]),
                            leo_file:file_unconsult(Path_2, MembersPrev);
                        not_found = Cause ->
                            {error, Cause};
                        Error ->
                            Error
                    end;
                not_found = Cause ->
                    {error, Cause};
                Error ->
                    Error
            end,
    {reply, Reply, State};

handle_call({dump, ring}, _From, State) ->
    {Res0, Res1} = dump_ring_tabs(),
    {reply, [Res0, Res1], State};

handle_call({_, routing_table,_Filename}, _From, State) ->
    {reply, {error, badarg}, State};

handle_call({attach, TblInfo, Member},_From, State) ->
    Reply = attach_1(TblInfo, Member#?MEMBER{state = ?STATE_ATTACHED}),
    {reply, Reply, State};

handle_call({reserve, Node, CurState, AwarenessL2,
             Clock, NumOfVNodes, RPCPort},
            _From, #state{cluster_id = ClusterId} = State) ->
    Reply = case leo_cluster_tbl_member:lookup(ClusterId, Node) of
                {ok, Member} ->
                    leo_cluster_tbl_member:insert(
                      Member#?MEMBER{cluster_id = ClusterId,
                                     state = CurState});
                not_found ->
                    NodeStr = atom_to_list(Node),
                    IP = case (string:chr(NodeStr, $@) > 0) of
                             true ->
                                 lists:nth(2,string:tokens(NodeStr,"@"));
                             false ->
                                 []
                         end,
                    leo_cluster_tbl_member:insert(
                      #?MEMBER{cluster_id = ClusterId,
                               node = Node,
                               ip = IP,
                               clock = Clock,
                               state = CurState,
                               num_of_vnodes = NumOfVNodes,
                               grp_level_2 = AwarenessL2,
                               port = RPCPort});
                {error, Cause} ->
                    {error, Cause}
            end,
    {reply, Reply, State};


handle_call({detach, TblInfo, Node, Clock},
            _From, #state{cluster_id = ClusterId} = State) ->
    Reply = case leo_cluster_tbl_member:lookup(ClusterId, Node) of
                {ok, Member} ->
                    detach_1(TblInfo, Member#?MEMBER{clock = Clock});
                Error ->
                    Error
            end,
    {reply, Reply, State};


handle_call({suspend, Node, _Clock},
            _From, #state{cluster_id = ClusterId} = State) ->
    Reply = case leo_cluster_tbl_member:lookup(ClusterId, Node) of
                {ok, Member} ->
                    case leo_cluster_tbl_member:insert(
                           Member#?MEMBER{cluster_id = ClusterId,
                                          state = ?STATE_SUSPEND}) of
                        ok ->
                            ok;
                        Error ->
                            Error
                    end;
                not_found = Cause ->
                    {error, Cause};
                Error ->
                    Error
            end,
    {reply, Reply, State}.


%% @doc Handling cast message
%% <p>
%% gen_server callback - Module:handle_cast(Request, State) -> Result.
%% </p>
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
%%% INNER FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Create a RING(routing-table)
%% @private
-spec(create_1(ClusterId, Ver) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Ver::?VER_CUR|?VER_PREV).
create_1(ClusterId, Ver) ->
    case leo_cluster_tbl_member:find_by_cluster_id(
           ?member_table(Ver), ClusterId) of
        {ok, Members} ->
            create_2(ClusterId, Ver, Members);
        not_found when Ver == ?VER_PREV ->
            %% overwrite current-ring to prev-ring
            case leo_cluster_tbl_member:overwrite(
                   ?MEMBER_TBL_CUR, ?MEMBER_TBL_PREV, ClusterId) of
                ok ->
                    create_1(ClusterId, Ver);
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

%% @private
-spec(create_2(ClusterId, Ver, Members) ->
             ok | {error, Cause} when ClusterId::cluster_id(),
                                      Ver::?VER_CUR|?VER_PREV,
                                      Members::[#?MEMBER{}],
                                      Cause::any()).
create_2(ClusterId, Ver, Members) ->
    create_2(ClusterId, Ver, Members, []).

-spec(create_2(ClusterId, Ver, Members, Acc) ->
             ok | {ok, Members} when ClusterId::cluster_id(),
                                     Ver::?VER_CUR|?VER_PREV,
                                     Members::[#?MEMBER{}],
                                     Acc::[#?MEMBER{}]).
create_2(ClusterId, Ver,[], Acc) ->
    case create_3(ClusterId, Ver, Acc, []) of
        ok ->
            Tbl = ?member_table(Ver),
            Ret = leo_cluster_tbl_member:bulk_insert(Tbl, Acc),
            ok = leo_redundant_manager_worker:force_sync(
                   ClusterId, ?RING_TBL_CUR),
            ok = leo_redundant_manager_worker:force_sync(
                   ClusterId, ?RING_TBL_PREV),
            Ret;
        Other ->
            Other
    end;
create_2(ClusterId, Ver, [#?MEMBER{state = ?STATE_RESERVED}|Rest], Acc) ->
    create_2(ClusterId, Ver, Rest, Acc);
create_2(ClusterId, Ver, [#?MEMBER{node = Node,
                                   state = State} = Member|Rest], Acc) ->
    %% Modify/Add a member into 'member-table'
    Table = ?member_table(Ver),
    Ret = case leo_cluster_tbl_member:lookup(Table, ClusterId, Node) of
              {ok, Member_1} when State == ?STATE_ATTACHED ->
                  {ok, Member_1#?MEMBER{state = ?STATE_RUNNING}};
              {ok, Member_1} ->
                  {ok, Member_1};
              not_found ->
                  {ok, Member#?MEMBER{state = ?STATE_RUNNING}};
              {error, Cause} ->
                  {error, Cause}
          end,
    case Ret of
        {ok, Member_2} ->
            create_2(ClusterId, Ver, Rest, [Member_2|Acc]);
        Error ->
            Error
    end.

%% @private
-spec(create_3(ClusterId, Ver, Members, Acc) ->
             ok | {ok, Members} when ClusterId::cluster_id(),
                                     Ver::?VER_CUR|?VER_PREV,
                                     Members::[#?MEMBER{}],
                                     Acc::[#?MEMBER{}]).
create_3(_,Ver, [], Acc) ->
    case leo_redundant_manager_chash:add_from_list(
           leo_redundant_manager_api:table_info(Ver),
           Acc) of
        ok ->
            ok;
        Error ->
            Error
    end;
create_3(ClusterId, Ver, [#?MEMBER{alias = []} = Member|Rest], Acc) ->
    TblInfo = leo_redundant_manager_api:table_info(Ver),
    {ok, Member_1} = set_alias(TblInfo, Member),
    case attach_2(TblInfo, Member_1) of
        ok ->
            create_3(ClusterId, Ver, Rest, [Member_1|Acc]);
        Error ->
            Error
    end;
create_3(ClusterId, Ver, [Member|Rest], Acc) ->
    create_3(ClusterId, Ver, Rest, [Member|Acc]).


%% @doc Ser alias of the node
%% @private
set_alias(TblInfo, #?MEMBER{cluster_id = ClusterId,
                            node = Node,
                            alias = [],
                            grp_level_2 = GrpL2} = Member) ->
    NodeStr = atom_to_list(Node),
    IP = case (string:chr(NodeStr, $@) > 0) of
             true ->
                 lists:nth(2,string:tokens(NodeStr,"@"));
             false ->
                 []
         end,

    {ok, {_Member, Alias}} =
        leo_redundant_manager_api:get_alias(
          init, ?ring_table_to_member_table(TblInfo),
          ClusterId, Node, GrpL2),
    {ok, Member#?MEMBER{alias = Alias,
                        ip = IP}};
set_alias(_,Member) ->
    {ok, Member}.


%% @doc Add a node into storage-cluster
%% @private
attach_1(TblInfo, Member) ->
    {ok, Member_1} = set_alias(TblInfo, Member),
    case attach_2(TblInfo, Member_1) of
        ok ->
            case leo_redundant_manager_chash:add(
                   TblInfo, Member_1) of
                ok ->
                    ok;
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

%% @private
attach_2(TblInfo, Member) ->
    case leo_cluster_tbl_member:insert(
           ?ring_table_to_member_table(TblInfo), Member) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc Detach a node from storage-cluster
%% @private
detach_1({_, ?RING_TBL_CUR} = TblInfo, Member) ->
    Node = Member#?MEMBER.node,
    case leo_cluster_tbl_member:insert(
           Member#?MEMBER{node = Node,
                          clock = Member#?MEMBER.clock,
                          state = ?STATE_DETACHED}) of
        ok ->
            detach_2(TblInfo, Member);
        Error ->
            Error
    end;
detach_1({_, ?RING_TBL_PREV} = TblInfo, Member) ->
    detach_2(TblInfo, Member).

%% @private
detach_2(TblInfo, Member) ->
    case leo_redundant_manager_chash:remove(TblInfo, Member) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc Retrieve members
%% @private
get_members_1(Ver, ClusterId) ->
    %% @TODO
    case leo_cluster_tbl_member:find_by_cluster_id(
           ?member_table(Ver), ClusterId) of
        {ok, Members} ->
            {ok, Members};
        not_found = Cause ->
            {error, Cause};
        Error ->
            Error
    end.


%% @doc Update the member by node
%% @private
update_member_by_node_1(ClusterId, Node, NodeState) ->
    update_member_by_node_1(ClusterId, Node, -1, NodeState).

update_member_by_node_1(ClusterId, Node, Clock, NodeState) ->
    case leo_cluster_tbl_member:lookup(ClusterId, Node) of
        {ok, Member} ->
            Member_1 = case Clock of
                           -1 ->
                               Member#?MEMBER{state = NodeState};
                           _  ->
                               Member#?MEMBER{clock = Clock,
                                              state = NodeState}
                       end,
            case leo_cluster_tbl_member:insert(Member_1) of
                ok ->
                    ok;
                Error ->
                    Error
            end;
        not_found = Cause ->
            {error, Cause};
        Error ->
            Error
    end.


%% @doc Export 'Ring' from a table
%% @private
dump_ring_tabs() ->
    LogDir = ?log_dir(),
    _ = filelib:ensure_dir(LogDir),
    File_1 = LogDir ++ ?DUMP_FILE_RING_CUR  ++ integer_to_list(leo_date:now()),
    File_2 = LogDir ++ ?DUMP_FILE_RING_PREV ++ integer_to_list(leo_date:now()),

    Res0 = leo_redundant_manager_chash:export(
             leo_redundant_manager_api:table_info(?VER_CUR), File_1),
    Res1 = leo_redundant_manager_chash:export(
             leo_redundant_manager_api:table_info(?VER_PREV), File_2),
    {Res0, Res1}.
