%%======================================================================
%%
%% Leo Redundant Manager
%%
%% Copyright (c) 2012-2014 Rakuten, Inc.
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
%% @doc
%% @end
%%======================================================================
-module(leo_redundant_manager).

-author('Yosuke Hara').

-behaviour(gen_server).

-include("leo_redundant_manager.hrl").
-include_lib("leo_rpc/include/leo_rpc.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/0, stop/0]).

-export([create/1, checksum/1, has_member/1, get_members/0, get_members/1,
         get_member_by_node/1, get_members_by_status/2,
         update_member/1, update_members/1,
         update_member_by_node/2, update_member_by_node/3,
         delete_member_by_node/1, dump/1]).

-export([attach/4, attach/5, attach/6,
         reserve/5, reserve/6,
         detach/2, detach/3, suspend/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
	       terminate/2,
         code_change/3]).

-undef(DEF_TIMEOUT).
-define(DEF_TIMEOUT,      30000).
-define(DEF_TIMEOUT_LONG, infinity).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:call(?MODULE, stop, ?DEF_TIMEOUT).


%% @doc Create Rings.
%%
-spec(create(?VER_CUR|?VER_PREV) ->
             ok | {error, any()}).
create(Ver) ->
    gen_server:call(?MODULE, {create, Ver}, ?DEF_TIMEOUT_LONG).


%% @doc Retrieve checksum (ring or member).
%%
-spec(checksum(checksum_type()) ->
             {ok, integer() | tuple()}).
checksum(Type) ->
    gen_server:call(?MODULE, {checksum, Type}, ?DEF_TIMEOUT).


%% @doc Is exists member?
%%
-spec(has_member(atom()) ->
             boolean()).
has_member(Node) ->
    gen_server:call(?MODULE, {has_member, Node}, ?DEF_TIMEOUT).


%% @doc Retrieve all members.
%%
-spec(get_members() ->
             {ok, list()}).
get_members() ->
    gen_server:call(?MODULE, {get_members, ?VER_CUR}, ?DEF_TIMEOUT).

get_members(Ver) ->
    gen_server:call(?MODULE, {get_members, Ver}, ?DEF_TIMEOUT).


%% @doc Retrieve a member by node.
%%
-spec(get_member_by_node(atom()) ->
             {ok, #member{}} | {error, any()}).
get_member_by_node(Node) ->
    gen_server:call(?MODULE, {get_member_by_node, Node}, ?DEF_TIMEOUT).


%% @doc Retrieve members by status.
%%
-spec(get_members_by_status(?VER_CUR|?VER_PREV, atom()) ->
             {ok, list(#member{})} | {error, any()}).
get_members_by_status(Ver, Status) ->
    gen_server:call(?MODULE, {get_members_by_status, Ver, Status}, ?DEF_TIMEOUT).


%% @doc Modify a member.
%%
-spec(update_member(#member{}) ->
             ok | {error, any()}).
update_member(Member) ->
    gen_server:call(?MODULE, {update_member, Member}, ?DEF_TIMEOUT).


%% @doc Modify members.
%%
-spec(update_members(list()) ->
             ok | {error, any()}).
update_members(Members) ->
    gen_server:call(?MODULE, {update_members, Members}, ?DEF_TIMEOUT).


%% @doc Modify a member by node.
%%
-spec(update_member_by_node(atom(), atom()) ->
             ok | {error, any()}).
update_member_by_node(Node, NodeState) ->
    gen_server:call(?MODULE, {update_member_by_node, Node, NodeState}, ?DEF_TIMEOUT).

-spec(update_member_by_node(atom(), integer(), atom()) ->
             ok | {error, any()}).
update_member_by_node(Node, Clock, NodeState) ->
    gen_server:call(?MODULE, {update_member_by_node, Node, Clock, NodeState}, ?DEF_TIMEOUT).


%% @doc Remove a member by node.
%%
-spec(delete_member_by_node(atom()) ->
             ok | {error, any()}).
delete_member_by_node(Node) ->
    gen_server:call(?MODULE, {delete_member_by_node, Node}, ?DEF_TIMEOUT).


%% %% @doc Synchronize a ring.
%% %%
%% synchronize(TblInfo, Ring0, Ring1) ->
%%     gen_server:call(?MODULE, {synchronize, TblInfo, Ring0, Ring1}, ?DEF_TIMEOUT_LONG).


%% @doc Dump files which are member and ring.
%%
-spec(dump(atom()) ->
             ok).
dump(Type) ->
    gen_server:call(?MODULE, {dump, Type}, ?DEF_TIMEOUT).


%% @doc Change node status to 'attach'.
%%
-spec(attach(atom(), string(), integer(), integer()) ->
             ok | {error, any()}).
attach(Node, AwarenessL2, Clock, NumOfVNodes) ->
    attach(Node, AwarenessL2, Clock, NumOfVNodes, ?DEF_LISTEN_PORT).

attach(Node, AwarenessL2, Clock, NumOfVNodes, RPCPort) ->
    attach(leo_redundant_manager_api:table_info(?VER_CUR),
           Node, AwarenessL2, Clock, NumOfVNodes, RPCPort).

attach(TableInfo, Node, AwarenessL2, Clock, NumOfVNodes, RPCPort) ->
    gen_server:call(?MODULE, {attach, TableInfo, Node,
                              AwarenessL2, Clock, NumOfVNodes, RPCPort}, ?DEF_TIMEOUT).


%% @doc Change node status to 'reserve'.
%%
-spec(reserve(atom(), atom(), string(), integer(), integer()) ->
             ok | {error, any()}).
reserve(Node, CurState, AwarenessL2, Clock, NumOfVNodes) ->
    reserve(Node, CurState, AwarenessL2, Clock, NumOfVNodes, ?DEF_LISTEN_PORT).

-spec(reserve(atom(), atom(), string(), integer(), integer(), integer()) ->
             ok | {error, any()}).
reserve(Node, CurState, AwarenessL2, Clock, NumOfVNodes, RPCPort) ->
    gen_server:call(?MODULE, {reserve, Node, CurState,
                              AwarenessL2, Clock, NumOfVNodes, RPCPort}, ?DEF_TIMEOUT).

%% @doc Change node status to 'detach'.
%%
-spec(detach(atom(), integer()) ->
             ok | {error, any()}).
detach(Node, Clock) ->
    detach(leo_redundant_manager_api:table_info(?VER_CUR), Node, Clock).

detach(TableInfo, Node, Clock) ->
    gen_server:call(?MODULE, {detach, TableInfo, Node, Clock}, ?DEF_TIMEOUT).


%% @doc Change node status to 'suspend'.
%%
-spec(suspend(atom(), integer()) ->
             ok | {error, any()}).
suspend(Node, Clock) ->
    gen_server:call(?MODULE, {suspend, Node, Clock}, ?DEF_TIMEOUT).


%%--------------------------------------------------------------------
%% GEN_SERVER CALLBACKS
%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State}          |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
init([]) ->
    {ok, null}.

handle_call(stop,_From,State) ->
    {stop, normal, ok, State};


handle_call({create, Ver}, _From, State) when Ver == ?VER_CUR;
                                              Ver == ?VER_PREV ->
    Reply = create_1(Ver),
    {reply, Reply, State};

handle_call({create,_Ver}, _From, State) ->
    {reply, {error, invalid_version}, State};

handle_call({checksum, ?CHECKSUM_MEMBER}, _From, State) ->
    HashCur = case leo_cluster_tbl_member:find_all(?MEMBER_TBL_CUR) of
                  {ok, MembersCur} ->
                      erlang:crc32(term_to_binary(lists:sort(MembersCur)));
                  _ ->
                      -1
              end,
    HashPrv = case leo_cluster_tbl_member:find_all(?MEMBER_TBL_PREV) of
                  {ok, MembersPrev} ->
                      erlang:crc32(term_to_binary(lists:sort(MembersPrev)));
                  _ ->
                      -1
              end,
    {reply, {ok, {HashCur, HashPrv}}, State};

handle_call({checksum, _}, _From, State) ->
    {reply, {error, badarg}, State};

handle_call({has_member, Node}, _From, State) ->
    Reply = case leo_cluster_tbl_member:lookup(Node) of
                {ok, _} ->
                    true;
                _ ->
                    false
            end,
    {reply, Reply, State};

handle_call({get_members, Ver}, _From, State) ->
    Reply = get_members_1(Ver),
    {reply, Reply, State};

handle_call({get_member_by_node, Node}, _From, State) ->
    Reply = case leo_cluster_tbl_member:lookup(Node) of
                {ok, Member} ->
                    {ok, Member};
                not_found = Cause ->
                    {error, Cause};
                Error ->
                    Error
            end,
    {reply, Reply, State};

handle_call({get_members_by_status, Ver, Status}, _From, State) ->
    Table = ?member_table(Ver),
    Reply = case leo_cluster_tbl_member:find_by_status(Table, Status) of
                {ok, Members} ->
                    {ok, Members};
                not_found = Cause ->
                    {error, Cause};
                Error ->
                    Error
            end,
    {reply, Reply, State};

handle_call({update_member, Member}, _From, State) ->
    Reply = leo_cluster_tbl_member:insert({Member#member.node, Member}),
    {reply, Reply, State};

handle_call({update_members, Members}, _From, State) ->
    Reply = case leo_cluster_tbl_member:find_all() of
                {ok, CurMembers} ->
                    CurMembersHash = erlang:crc32(term_to_binary(CurMembers)),
                    MembersHash    = erlang:crc32(term_to_binary(Members)),

                    case (MembersHash =:= CurMembersHash) of
                        true ->
                            ok;
                        false ->
                            leo_cluster_tbl_member:replace(CurMembers, Members)
                    end;
                not_found ->
                    leo_cluster_tbl_member:replace([], Members);
                Error ->
                    Error
            end,
    {reply, Reply, State};

handle_call({update_member_by_node, Node, NodeState}, _From, State) ->
    Reply = update_member_by_node_1(Node, NodeState),
    {reply, Reply, State};

handle_call({update_member_by_node, Node, Clock, NodeState}, _From, State) ->
    Reply = update_member_by_node_1(Node, Clock, NodeState),
    {reply, Reply, State};

handle_call({delete_member_by_node, Node}, _From, State) ->
    Reply = leo_cluster_tbl_member:delete(Node),
    {reply, Reply, State};

handle_call({dump, member}, _From, State) ->
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

    Reply = case leo_cluster_tbl_member:find_all(?MEMBER_TBL_CUR) of
                {ok, MembersCur} ->
                    Path_1 = lists:append([LogDir,
                                           ?DUMP_FILE_MEMBERS_CUR,
                                           integer_to_list(leo_date:now())]),
                    leo_file:file_unconsult(Path_1, MembersCur),

                    case leo_cluster_tbl_member:find_all(?MEMBER_TBL_PREV) of
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


handle_call({attach, TblInfo, Node, GroupL2,
             Clock, NumOfVNodes, RPCPort}, _From, State) ->
    Member = #member{node  = Node,
                     clock = Clock,
                     state = ?STATE_ATTACHED,
                     num_of_vnodes = NumOfVNodes,
                     grp_level_2   = GroupL2,
                     port = RPCPort
                    },
    Reply = attach_1(TblInfo, Member),
    {reply, Reply, State};


handle_call({reserve, Node, CurState, AwarenessL2,
             Clock, NumOfVNodes, RPCPort}, _From, State) ->
    Reply = case leo_cluster_tbl_member:lookup(Node) of
                {ok, Member} ->
                    leo_cluster_tbl_member:insert(
                      {Node, Member#member{state = CurState}});
                not_found ->
                    NodeStr = atom_to_list(Node),
                    IP = case (string:chr(NodeStr, $@) > 0) of
                             true ->
                                 lists:nth(2,string:tokens(NodeStr,"@"));
                             false ->
                                 []
                         end,

                    leo_cluster_tbl_member:insert(
                      {Node, #member{node  = Node,
                                     ip    = IP,
                                     clock = Clock,
                                     state = CurState,
                                     num_of_vnodes = NumOfVNodes,
                                     grp_level_2   = AwarenessL2,
                                     port = RPCPort
                                    }});
                {error, Cause} ->
                    {error, Cause}
            end,
    {reply, Reply, State};


handle_call({detach, TblInfo, Node, Clock}, _From, State) ->
    Reply = case leo_cluster_tbl_member:lookup(Node) of
                {ok, Member} ->
                    detach_1(TblInfo, Member#member{clock = Clock});
                Error ->
                    Error
            end,
    {reply, Reply, State};


handle_call({suspend, Node, _Clock}, _From, State) ->
    Reply = case leo_cluster_tbl_member:lookup(Node) of
                {ok, Member} ->
                    case leo_cluster_tbl_member:insert(
                           {Node, Member#member{state = ?STATE_SUSPEND}}) of
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


%% Function: handle_cast(Msg, State) -> {noreply, State}          |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
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
%%% INNER FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Create a RING(routing-table)
%% @private
-spec(create_1(?VER_CUR|?VER_PREV) ->
             ok | {error, any()}).
create_1(Ver) ->
    case leo_cluster_tbl_member:find_all(?member_table(Ver)) of
        {ok, Members} ->
            create_2(Ver, Members);
        not_found when Ver == ?VER_PREV ->
            %% overwrite current-ring to prev-ring
            case leo_cluster_tbl_member:overwrite(
                   ?MEMBER_TBL_CUR, ?MEMBER_TBL_PREV) of
                ok ->
                    create_1(Ver);
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

%% @private
-spec(create_2(?VER_CUR|?VER_PREV, list()) ->
             ok | {ok, list()}).
create_2(Ver, Members) ->
    create_2(Ver, Members, []).

-spec(create_2(?VER_CUR|?VER_PREV, list(), list()) ->
             ok | {ok, list()}).
create_2(Ver,[], Acc) ->
    create_3(Ver, Acc, []);
    %% create_3(Ver, Acc);
create_2( Ver, [#member{state = ?STATE_DETACHED}|Rest], Acc) ->
    create_2(Ver, Rest, Acc);
create_2( Ver, [#member{state = ?STATE_RESERVED}|Rest], Acc) ->
    create_2(Ver, Rest, Acc);
create_2( Ver, [#member{node = Node} = Member_0|Rest], Acc) ->
    %% Modify/Add a member into 'member-table'
    Table = ?member_table(Ver),
    Ret_2 = case leo_cluster_tbl_member:lookup(Table, Node) of
                {ok, Member_1} ->
                    {ok, Member_1#member{state = ?STATE_RUNNING}};
                not_found ->
                    {ok, Member_0#member{state = ?STATE_RUNNING}};
                {error, Cause} ->
                    {error, Cause}
            end,
    case Ret_2 of
        {ok, Member_2} ->
            create_2(Ver, Rest, [Member_2|Acc]);
        Error ->
            Error
    end.

%% @private
%% -spec(create_3(?VER_CUR|?VER_PREV, list(), list()) ->
%%              ok | {ok, list()}).
create_3(Ver, [], Acc) ->
    TblInfo = leo_redundant_manager_api:table_info(Ver),
    leo_redundant_manager_chash:add_from_list(TblInfo, Acc);
create_3(Ver, [Member|Rest], Acc) ->
    TblInfo = leo_redundant_manager_api:table_info(Ver),

    {ok, Member_1} = set_alias(TblInfo, Member),
    case attach_2(TblInfo, Member_1) of
        ok ->
            create_3(Ver, Rest, [Member_1|Acc]);
        Error ->
            Error
    end.


%% @private
set_alias(TblInfo, #member{node  = Node,
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
          Node, GrpL2),
    {ok, Member#member{alias = Alias,
                       ip    = IP}};
set_alias(_,Member) ->
    {ok, Member}.


%% @doc Add a node into storage-cluster
%% @private
attach_1(TblInfo, Member) ->
    {ok, Member_1} = set_alias(TblInfo, Member),
    case attach_2(TblInfo, Member_1) of
        ok ->
            leo_redundant_manager_chash:add(TblInfo, Member_1);
        Error ->
            Error
    end.

%% @private
attach_2(TblInfo, #member{node  = Node} = Member) ->
    case leo_cluster_tbl_member:insert(
           ?ring_table_to_member_table(TblInfo), {Node, Member}) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc Detach a node from storage-cluster
%% @private
detach_1({_, ?RING_TBL_CUR} = TblInfo, Member) ->
    Node = Member#member.node,
    case leo_cluster_tbl_member:insert(
           {Node, Member#member{node = Node,
                                clock = Member#member.clock,
                                state = ?STATE_DETACHED}}) of
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
get_members_1(Ver) ->
    case leo_cluster_tbl_member:find_all(?member_table(Ver)) of
        {ok, Members} ->
            {ok, Members};
        not_found = Cause ->
            {error, Cause};
        Error ->
            Error
    end.


update_member_by_node_1(Node, NodeState) ->
    update_member_by_node_1(Node, -1, NodeState).

update_member_by_node_1(Node, Clock, NodeState) ->
    case leo_cluster_tbl_member:lookup(Node) of
        {ok, Member} ->
            Member_1 = case Clock of
                           -1 -> Member#member{state = NodeState};
                           _  -> Member#member{clock = Clock,
                                               state = NodeState}
                       end,
            case leo_cluster_tbl_member:insert({Node, Member_1}) of
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
    LogDir = case application:get_env(leo_redundant_manager, log_dir_ring) of
                 undefined -> ?DEF_LOG_DIR_RING;
                 {ok, Dir} ->
                     case (string:len(Dir) == string:rstr(Dir, "/")) of
                         true  -> Dir;
                         false -> Dir ++ "/"
                     end
             end,

    _ = filelib:ensure_dir(LogDir),
    File_1 = LogDir ++ ?DUMP_FILE_RING_CUR  ++ integer_to_list(leo_date:now()),
    File_2 = LogDir ++ ?DUMP_FILE_RING_PREV ++ integer_to_list(leo_date:now()),

    Res0 = leo_redundant_manager_chash:export(
             leo_redundant_manager_api:table_info(?VER_CUR), File_1),
    Res1 = leo_redundant_manager_chash:export(
             leo_redundant_manager_api:table_info(?VER_PREV), File_2),
    {Res0, Res1}.

