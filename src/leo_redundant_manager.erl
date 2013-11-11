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
%% Leo Redundant Manageme - Server
%% @doc
%% @end
%%======================================================================
-module(leo_redundant_manager).

-author('Yosuke Hara').

-behaviour(gen_server).

-include("leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/0, stop/0]).

-export([create/1, checksum/1, has_member/1, get_members/0, get_members/1,
         get_member_by_node/1, get_members_by_status/1,
         update_member/1, update_members/1, update_member_by_node/3,
         delete_member_by_node/1, synchronize/3, adjust/3, dump/1]).

-export([attach/4, attach/5, reserve/5, detach/2, detach/3, suspend/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
	       terminate/2,
         code_change/3]).

-undef(DEF_TIMEOUT).
-define(DEF_TIMEOUT, 30000).

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
-spec(create(?VER_CURRENT|?VER_PREV) ->
             {ok, list()}).
create(Ver) ->
    gen_server:call(?MODULE, {create, Ver}, ?DEF_TIMEOUT).


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
    gen_server:call(?MODULE, {get_members, ?VER_CURRENT}, ?DEF_TIMEOUT).

get_members(Mode) ->
    gen_server:call(?MODULE, {get_members, Mode}, ?DEF_TIMEOUT).


%% @doc Retrieve a member by node.
%%
-spec(get_member_by_node(atom()) ->
             {ok, #member{}}).
get_member_by_node(Node) ->
    gen_server:call(?MODULE, {get_member_by_node, Node}, ?DEF_TIMEOUT).


%% @doc Retrieve members by status.
%%
-spec(get_members_by_status(atom()) ->
             {ok, list(#member{})} | not_found).
get_members_by_status(Status) ->
    gen_server:call(?MODULE, {get_members_by_status, Status}, ?DEF_TIMEOUT).


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


%% @doc Synchronize a ring.
%%
synchronize(TblInfo, Ring0, Ring1) ->
    gen_server:call(?MODULE, {synchronize, TblInfo, Ring0, Ring1}, ?DEF_TIMEOUT).


%% @doc Adjust prev-ring's vnode-id.
%%
adjust(CurRingTable, PrevRingTable, VNodeId) ->
    gen_server:call(?MODULE, {adjust, CurRingTable, PrevRingTable, VNodeId}, ?DEF_TIMEOUT).


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
attach(Node, NumOfAwarenessL2, Clock, NumOfVNodes) ->
    attach(leo_redundant_manager_api:table_info(?VER_CURRENT),
           Node, NumOfAwarenessL2, Clock, NumOfVNodes).

attach(TableInfo, Node, NumOfAwarenessL2, Clock, NumOfVNodes) ->
    gen_server:call(?MODULE, {attach, TableInfo, Node,
                              NumOfAwarenessL2, Clock, NumOfVNodes}, ?DEF_TIMEOUT).


%% @doc Change node status to 'reserve'.
%%
-spec(reserve(atom(), atom(), string(), integer(), integer()) ->
             ok | {error, any()}).
reserve(Node, CurState, NumOfAwarenessL2, Clock, NumOfVNodes) ->
    gen_server:call(?MODULE, {reserve, Node, CurState,
                              NumOfAwarenessL2, Clock, NumOfVNodes}, ?DEF_TIMEOUT).

%% @doc Change node status to 'detach'.
%%
-spec(detach(atom(), integer()) ->
             ok | {error, any()}).
detach(Node, Clock) ->
    detach(leo_redundant_manager_api:table_info(?VER_CURRENT), Node, Clock).

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


handle_call({create, Ver}, _From, State) when Ver == ?VER_CURRENT;
                                              Ver == ?VER_PREV ->
    Reply = create_1(Ver),
    {reply, Reply, State};

handle_call({create,_Ver}, _From, State) ->
    {reply, {error, invalid_version}, State};

handle_call({checksum, ?CHECKSUM_MEMBER}, _From, State) ->
    HashCur = case leo_redundant_manager_table_member:find_all(?MEMBER_TBL_CUR) of
                  {ok, MembersCur} ->
                      erlang:crc32(term_to_binary(MembersCur));
                  _ ->
                      -1
              end,
    HashPrv = case leo_redundant_manager_table_member:find_all(?MEMBER_TBL_PREV) of
                  {ok, MembersPrev} ->
                      erlang:crc32(term_to_binary(MembersPrev));
                  _ ->
                      -1
              end,
    {reply, {ok, {HashCur, HashPrv}}, State};

handle_call({checksum, _}, _From, State) ->
    {reply, {error, badarg}, State};

handle_call({has_member, Node}, _From, State) ->
    Reply = case leo_redundant_manager_table_member:lookup(Node) of
                {ok, _} ->
                    true;
                _ ->
                    false
            end,
    {reply, Reply, State};


handle_call({get_members, ?VER_CURRENT = Mode}, _From, State) ->
    Reply = get_members_1(Mode),
    {reply, Reply, State};

handle_call({get_members, ?VER_PREV    = Mode}, _From, State) ->
    Reply = get_members_1(Mode),
    {reply, Reply, State};

handle_call({get_member_by_node, Node}, _From, State) ->
    Reply = case leo_redundant_manager_table_member:lookup(Node) of
                {ok, Member} ->
                    {ok, Member};
                not_found = Cause ->
                    {error, Cause};
                Error ->
                    Error
            end,
    {reply, Reply, State};

handle_call({get_members_by_status, Status}, _From, State) ->
    Reply = case leo_redundant_manager_table_member:find_by_status(Status) of
                {ok, Member} ->
                    {ok, Member};
                not_found = Cause ->
                    {error, Cause};
                Error ->
                    Error
            end,
    {reply, Reply, State};

handle_call({update_member, Member}, _From, State) ->
    Reply = leo_redundant_manager_table_member:insert({Member#member.node, Member}),
    {reply, Reply, State};

handle_call({update_members, Members}, _From, State) ->
    Reply = case leo_redundant_manager_table_member:find_all() of
                {ok, CurMembers} ->
                    CurMembersHash = erlang:crc32(term_to_binary(CurMembers)),
                    MembersHash    = erlang:crc32(term_to_binary(Members)),

                    case (MembersHash =:= CurMembersHash) of
                        true ->
                            ok;
                        false ->
                            leo_redundant_manager_table_member:replace(CurMembers, Members)
                    end;
                not_found ->
                    leo_redundant_manager_table_member:replace([], Members);
                Error ->
                    Error
            end,
    {reply, Reply, State};

handle_call({update_member_by_node, Node, Clock, NodeState}, _From, State) ->
    Reply = case leo_redundant_manager_table_member:lookup(Node) of
                {ok, Member} ->
                    case leo_redundant_manager_table_member:insert(
                           {Node, Member#member{clock = Clock,
                                                state = NodeState}}) of
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
    {reply, Reply, State};

handle_call({delete_member_by_node, Node}, _From, State) ->
    Reply = leo_redundant_manager_table_member:delete(Node),
    {reply, Reply, State};

handle_call({synchronize, TblInfo, MgrRing, MyRing}, _From, State) ->
    %% 1. MyRing.vnode-id -> MgrRing.vnode-id
    lists:foreach(fun({VNodeId0, Node0}) ->
                          Res = case lists:keyfind(VNodeId0, 1, MgrRing) of
                                    {VNodeId1, Node1} when VNodeId0 == VNodeId1 andalso
                                                           Node0    == Node1 ->
                                        true;
                                    _ ->
                                        false
                                end,

                          case Res of
                              true ->
                                  void;
                              false ->
                                  leo_redundant_manager_table_ring:delete(TblInfo, VNodeId0)
                          end
                  end, MyRing),

    %% 2. MyRing.vnode-id -> MgrRing.vnode-id
    lists:foreach(fun({VNodeId0, Node0}) ->
                          Res = case lists:keyfind(VNodeId0, 1, MyRing) of
                                    {VNodeId1, Node1} when VNodeId0 == VNodeId1 andalso
                                                           Node0    == Node1 ->
                                        true;
                                    _ ->
                                        false
                                end,

                          case Res of
                              true ->
                                  void;
                              false ->
                                  leo_redundant_manager_table_ring:insert(TblInfo, {VNodeId0, Node0})
                          end
                  end, MgrRing),
    dump_ring_tabs(),
    {reply, ok, State};


handle_call({adjust, CurRingTable, PrevRingTable, VNodeId}, _From, State) ->
    Reply = leo_redundant_manager_chash:adjust(CurRingTable, PrevRingTable, VNodeId),
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

    Reply = case leo_redundant_manager_table_member:find_all(?MEMBER_TBL_CUR) of
                {ok, MembersCur} ->
                    Path_1 = lists:append([LogDir,
                                           ?DUMP_FILE_MEMBERS_CUR,
                                           integer_to_list(leo_date:now())]),
                    leo_file:file_unconsult(Path_1, MembersCur),

                    case leo_redundant_manager_table_member:find_all(?MEMBER_TBL_PREV) of
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


handle_call({attach, TblInfo, Node, GroupL2, Clock, NumOfVNodes}, _From, State) ->
    Member = #member{node  = Node,
                     clock = Clock,
                     state = ?STATE_ATTACHED,
                     num_of_vnodes = NumOfVNodes,
                     grp_level_2   = GroupL2},
    Reply = attach_1(TblInfo, Member),
    {reply, Reply, State};


handle_call({reserve, Node, CurState, NumOfAwarenessL2, Clock, NumOfVNodes}, _From, State) ->
    Reply = case leo_redundant_manager_table_member:lookup(Node) of
                {ok, Member} ->
                    leo_redundant_manager_table_member:insert(
                      {Node, Member#member{state = CurState}});
                not_found ->
                    NodeStr = atom_to_list(Node),
                    IP = case (string:chr(NodeStr, $@) > 0) of
                             true ->
                                 lists:nth(2,string:tokens(NodeStr,"@"));
                             false ->
                                 []
                         end,

                    leo_redundant_manager_table_member:insert(
                      {Node, #member{node  = Node,
                                     ip    = IP,
                                     clock = Clock,
                                     state = CurState,
                                     num_of_vnodes = NumOfVNodes,
                                     grp_level_2   = NumOfAwarenessL2}});
                {error, Cause} ->
                    {error, Cause}
            end,
    {reply, Reply, State};


handle_call({detach, TblInfo, Node, Clock}, _From, State) ->
    Reply = case leo_redundant_manager_table_member:lookup(Node) of
                {ok, Member} ->
                    detach_1(TblInfo, Member#member{clock = Clock});
                Error ->
                    Error
            end,
    {reply, Reply, State};


handle_call({suspend, Node, Clock}, _From, State) ->
    Reply = case leo_redundant_manager_table_member:lookup(Node) of
                {ok, Member} ->
                    case leo_redundant_manager_table_member:insert(
                           {Node, Member#member{clock = Clock,
                                                state = ?STATE_SUSPEND}}) of
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
-spec(create_1(?VER_CURRENT|?VER_PREV) ->
             ok | {error, any()}).
create_1(Ver) ->
    case leo_redundant_manager_table_member:find_all(?member_table(Ver)) of
        {ok, Members} ->
            create_2(Ver, Members);
        not_found when Ver == ?VER_PREV ->
            %% overwrite current-ring to prev-ring
            case leo_redundant_manager_table_member:overwrite(
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
-spec(create_2(?VER_CURRENT|?VER_PREV, list()) ->
             ok | {ok, list()}).
create_2(Ver, Members) ->
    create_2(Ver, Members, []).

-spec(create_2(?VER_CURRENT|?VER_PREV, list(), list()) ->
             ok | {ok, list()}).
create_2(Ver,[], Acc) ->
    create_3(Ver, Acc);
create_2( Ver, [#member{node = Node} = Member_0|Rest], Acc) ->
    %% Modify/Add a member into 'member-table'
    Table = ?member_table(Ver),
    Ret_2 = case leo_redundant_manager_table_member:lookup(Node) of
                {error, Cause} ->
                    {error, Cause};
                Ret_1 ->
                    Prop = case Ret_1 of
                               {ok, Member_1} -> {Node, Member_1#member{state = ?STATE_RUNNING}};
                               not_found      -> {Node, Member_0#member{state = ?STATE_RUNNING}}
                           end,
                    case leo_redundant_manager_table_member:insert(Table, Prop) of
                        ok ->
                            {ok, erlang:element(2, Prop)};
                        {error, Cause} ->
                            {error, Cause}
                    end
            end,
    case Ret_2 of
        {ok, Member_2} ->
            create_2(Ver, Rest, [Member_2|Acc]);
        Error ->
            Error
    end.

%% @private
-spec(create_3(?VER_CURRENT|?VER_PREV, list()) ->
             ok | {ok, list()}).
create_3(_, []) ->
    ok;
create_3(Ver, [Member|Rest]) ->
    case attach_1(leo_redundant_manager_api:table_info(Ver), Member) of
        ok ->
            create_3(Ver, Rest);
        Error ->
            Error
    end.


%% @doc Generate an alian from 'node'
%% @private
alias(Node) ->
    case leo_redundant_manager_table_member:find_by_status(?STATE_DETACHED) of
        not_found ->
            PartOfAlias = string:substr(
                            leo_hex:binary_to_hex(
                              crypto:hash(md5, lists:append([atom_to_list(Node)]))),1,8),
            {ok, lists:append([?NODE_ALIAS_PREFIX, PartOfAlias])};
        {ok, [M|_]} ->
            {ok, M#member.alias};
        {error, Cause} ->
            {error, Cause}
    end.


%% @doc Add a node into storage-cluster
%% @private
attach_1(TblInfo, #member{node = Node} = Member) ->
    NodeStr = atom_to_list(Node),
    IP = case (string:chr(NodeStr, $@) > 0) of
             true ->
                 lists:nth(2,string:tokens(NodeStr,"@"));
             false ->
                 []
         end,

    case alias(Node) of
        {ok, Alias} ->
            attach_2(TblInfo, Member#member{alias = Alias,
                                            ip    = IP});
        {error, Cause} ->
            {error, Cause}
    end.

%% @private
attach_2(TblInfo, #member{node = Node} = Member) ->
    case leo_redundant_manager_table_member:insert(
           {Node, Member#member{clock = leo_date:clock()}}) of
        ok ->
            attach_3(TblInfo, Member);
        Error ->
            Error
    end.

%% @private
attach_3(TblInfo, Member) ->
    case leo_redundant_manager_chash:add(TblInfo, Member) of
        ok ->
            dump_ring_tabs(),
            ok;
        Error ->
            Error
    end.


%% @doc Detach a node from storage-cluster
%% @private
detach_1({_, ?RING_TBL_CUR} = TblInfo, Member) ->
    Node = Member#member.node,
    case leo_redundant_manager_table_member:insert(
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
            dump_ring_tabs(),
            ok;
        Error ->
            Error
    end.


%% @doc Retrieve members
%% @private
get_members_1(?VER_CURRENT) ->
    case leo_redundant_manager_table_member:find_all() of
        {ok, Members} ->
            {ok, Members};
        not_found = Cause ->
            {error, Cause};
        Error ->
            Error
    end;
get_members_1(?VER_PREV) ->
    case leo_redundant_manager_table_ring:tab2list(
           leo_redundant_manager_api:table_info(?VER_PREV)) of
        [] ->
            not_found;
        List ->
            Hashtable = leo_hashtable:new(),
            lists:foreach(fun({VNodeId, Node}) ->
                                  leo_hashtable:append(Hashtable, Node, VNodeId)
                          end, List),
            {ok, lists:map(fun({Node, VNodes}) ->
                                   #member{node = Node, num_of_vnodes = length(VNodes)}
                           end, leo_hashtable:all(Hashtable))}
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
             leo_redundant_manager_api:table_info(?VER_CURRENT), File_1),
    Res1 = leo_redundant_manager_chash:export(
             leo_redundant_manager_api:table_info(?VER_PREV), File_2),
    {Res0, Res1}.

