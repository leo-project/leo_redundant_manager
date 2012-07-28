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

-export([create/0, checksum/1, has_member/1, get_members/0, get_members/1, get_member_by_node/1,
         update_members/1, update_member_by_node/3, synchronize/3, adjust/3, dump/1]).

-export([attach/3, detach/2, suspend/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
	       terminate/2,
         code_change/3]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:call(?MODULE, stop).


%% @doc Create Rings.
%%
-spec(create() ->
             {ok, list()}).
create() ->
    gen_server:call(?MODULE, {create}).


%% @doc Retrieve checksum (ring or member).
%%
-spec(checksum(checksum_type()) ->
             {ok, integer() | tuple()}).
checksum(Type) ->
    gen_server:call(?MODULE, {checksum, Type}).


%% @doc Is exists member?
%%
-spec(has_member(atom()) ->
             boolean()).
has_member(Node) ->
    gen_server:call(?MODULE, {has_member, Node}).


%% @doc Retrieve all members.
%%
-spec(get_members() ->
             {ok, list()}).
get_members() ->
    gen_server:call(?MODULE, {get_members, ?VER_CURRENT}).

get_members(Mode) ->
    gen_server:call(?MODULE, {get_members, Mode}).


%% @doc Retrieve a member by node.
%%
-spec(get_member_by_node(atom()) ->
             {ok, #member{}}).
get_member_by_node(Node) ->
    gen_server:call(?MODULE, {get_member_by_node, Node}).


%% @doc Modify members.
%%
-spec(update_members(list()) ->
             ok | {error, any()}).
update_members(Members) ->
    gen_server:call(?MODULE, {update_members, Members}).


%% @doc Modify a member by node.
%%
-spec(update_member_by_node(atom, integer(), atom()) ->
             ok | {error, any()}).
update_member_by_node(Node, Clock, NodeState) ->
    gen_server:call(?MODULE, {update_member_by_node, Node, Clock, NodeState}).


%% @doc Synchronize a ring.
%%
synchronize(TblInfo, Ring0, Ring1) ->
    gen_server:call(?MODULE, {synchronize, TblInfo, Ring0, Ring1}).


%% @doc Adjust prev-ring's vnode-id.
%%
adjust(CurRingTable, PrevRingTable, VNodeId) ->
    gen_server:call(?MODULE, {adjust, CurRingTable, PrevRingTable, VNodeId}).


%% @doc Dump files which are member and ring.
%%
-spec(dump(atom()) ->
             ok).
dump(Type) ->
    gen_server:call(?MODULE, {dump, Type}).


%% @doc Change node status to 'attach'.
%%
-spec(attach(atom(), integer(), integer()) ->
             ok | {error, any()}).
attach(Node, Clock, NumOfVNodes) ->
    gen_server:call(?MODULE, {attach, Node, Clock, NumOfVNodes}).

%% @doc Change node status to 'detach'.
%%
-spec(detach(atom(), integer()) ->
             ok | {error, any()}).
detach(Node, Clock) ->
    gen_server:call(?MODULE, {detach, Node, Clock}).


%% @doc Change node status to 'suspend'.
%%
-spec(suspend(atom(), integer()) ->
             ok | {error, any()}).
suspend(Node, Clock) ->
    gen_server:call(?MODULE, {suspend, Node, Clock}).


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


handle_call({create}, _From, State) ->
    Reply = case leo_redundant_manager_table_member:find_all() of
                {ok, Members} ->
                    add_members(Members);
                Error ->
                    Error
            end,
    {reply, Reply, State};

handle_call({checksum, ?CHECKSUM_MEMBER}, _From, State) ->
    Reply = case leo_redundant_manager_table_member:find_all() of
                {ok, Members} ->
                    {ok, erlang:crc32(term_to_binary(Members))};
                _ ->
                    {ok, -1}
            end,
    {reply, Reply, State};

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
    Reply = get_members_fun(Mode),
    {reply, Reply, State};

handle_call({get_members, ?VER_PREV    = Mode}, _From, State) ->
    Reply = get_members_fun(Mode),
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


handle_call({update_member_by_node, Node, Clock, NodeState}, _From, State) ->
    Reply = case leo_redundant_manager_table_member:insert(
                   {Node, #member{node = Node,
                                  clock = Clock,
                                  state = NodeState}}) of
                ok ->
                    case leo_redundant_manager_table_member:find_all() of
                        {ok, Members} ->
                            {ok, Members};
                        Error ->
                            Error
                    end;
                Error ->
                    Error
            end,
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


handle_call({synchronize, TblInfo, MgrRing, MyRing}, _From, State) ->
    %% 1. MyRing.vnode-id -> MgrRing.vnode-id
    lists:foreach(fun({VNodeId0, Node0}) ->
                          Res = case lists:keyfind(VNodeId0, 1, MgrRing) of
                                    {VNodeId1, Node1} when VNodeId0 == VNodeId1 andalso
                                                           Node0    == Node1 ->
                                        true;
                                    false ->
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
                                    false ->
                                        false
                                end,

                          case Res of
                              true ->
                                  void;
                              false ->
                                  leo_redundant_manager_table_ring:insert(TblInfo, {VNodeId0, Node0})
                          end
                  end, MgrRing),
    {reply, ok, State};


handle_call({adjust, CurRingTable, PrevRingTable, VNodeId}, _From, State) ->
    Reply = leo_redundant_manager_chash:adjust(CurRingTable, PrevRingTable, VNodeId),
    {reply, Reply, State};


handle_call({dump, member}, _From, State) ->
    Reply = case leo_redundant_manager_table_member:find_all() of
                {ok, Members} ->
                    FileName = ?DUMP_FILE_MEMBERS ++ integer_to_list(leo_utils:now()),
                    leo_utils:file_unconsult(FileName, Members);
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


handle_call({attach, Node, Clock, NumOfVNodes}, _From, State) ->
    TblInfo = leo_redundant_manager_api:table_info(?VER_CURRENT),
    Member  = #member{node  = Node,
                      clock = Clock,
                      state = ?STATE_ATTACHED,
                      num_of_vnodes = NumOfVNodes},

    Reply = attach_fun(TblInfo, Member),
    {reply, Reply, State};


handle_call({detach, Node, Clock}, _From, State) ->
    Reply = case leo_redundant_manager_table_member:lookup(Node) of
                {ok, Member} ->
                    TblInfo = leo_redundant_manager_api:table_info(?VER_CURRENT),
                    detach_fun(TblInfo, Member#member{clock = Clock});
                Error ->
                    Error
            end,
    {reply, Reply, State};


handle_call({suspend, Node, Clock}, _From, State) ->
    Reply = case leo_redundant_manager_table_member:insert(
                   {Node, #member{node = Node,
                                  clock = Clock,
                                  state = ?STATE_SUSPEND}}) of
                ok ->
                    case leo_redundant_manager_table_member:find_all() of
                        {ok, Members} ->
                            {ok, Members};
                        Error ->
                            Error
                    end;
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
-spec(add_members(list()) ->
             ok | {ok, list()}).
add_members(Members) ->
    State = ?STATE_RUNNING,
    NewMembers = lists:map(
                   fun(#member{node  = Node,
                               clock = Clock} = Member) ->
                           _ = leo_redundant_manager_table_member:insert(
                                 {Node, #member{node = Node,
                                                clock = Clock,
                                                state = State}}),
                           Member#member{state = State}
                   end, Members),

    TblInfo0 = leo_redundant_manager_api:table_info(?VER_CURRENT),
    TblInfo1 = leo_redundant_manager_api:table_info(?VER_PREV),

    true = leo_redundant_manager_table_ring:delete_all_objects(TblInfo1),
    case leo_redundant_manager_table_ring:tab2list(TblInfo0) of
        [] ->
            void;
        List when is_list(List) ->
            lists:foreach(fun({K,V}) ->
                                  leo_redundant_manager_table_ring:insert(TblInfo1, {K, V})
                          end, List);
        _Error ->
            void
    end,

    dump_ring_tabs(),
    {ok, NewMembers}.


attach_fun({_, ?CUR_RING_TABLE} = TblInfo, #member{node = Node} = Member) ->
    case leo_redundant_manager_table_member:insert({Node, Member}) of
        ok ->
            attach_fun1(TblInfo, Member);
        Error ->
            Error
    end;
attach_fun({_, ?PREV_RING_TABLE} = TblInfo, Member)  ->
    attach_fun1(TblInfo, Member).

attach_fun1(TblInfo, Member) ->
    case leo_redundant_manager_chash:add(TblInfo, Member) of
        ok ->
            dump_ring_tabs(),

            case leo_redundant_manager_table_member:find_all() of
                {ok, Members} ->
                    {ok, Members};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.


detach_fun({_, ?CUR_RING_TABLE} = TblInfo, Member) ->
    Node = Member#member.node,
    case leo_redundant_manager_table_member:insert(
           {Node, #member{node = Node,
                          clock = Member#member.clock,
                          state = ?STATE_DETACHED}}) of
        ok ->
            detach_fun1(TblInfo, Member);
        Error ->
            Error
    end;
detach_fun({_, ?PREV_RING_TABLE} = TblInfo, Member) ->
    detach_fun1(TblInfo, Member).

detach_fun1(TblInfo, Member) ->
    case leo_redundant_manager_chash:remove(TblInfo, Member) of
        ok ->
            dump_ring_tabs(),

            case leo_redundant_manager_table_member:find_all() of
                {ok, Members} ->
                    {ok, Members};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.


get_members_fun(?VER_CURRENT) ->
    case leo_redundant_manager_table_member:find_all() of
        {ok, Members} ->
            {ok, Members};
        not_found = Cause ->
            {error, Cause};
        Error ->
            Error
    end;
get_members_fun(?VER_PREV) ->
    TblInfo = leo_redundant_manager_api:table_info(?VER_PREV),
    Ring    = leo_redundant_manager_table_ring:tab2list(TblInfo),
    case Ring of
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


dump_ring_tabs() ->
    _ = filelib:ensure_dir("./log/ring/"),
    TblInfo0 = leo_redundant_manager_api:table_info(?VER_CURRENT),
    TblInfo1 = leo_redundant_manager_api:table_info(?VER_PREV),

    File0 = ?DUMP_FILE_RING_CUR  ++ integer_to_list(leo_utils:now()),
    File1 = ?DUMP_FILE_RING_PREV ++ integer_to_list(leo_utils:now()),

    Res0 = leo_redundant_manager_chash:export(TblInfo0, File0),
    Res1 = leo_redundant_manager_chash:export(TblInfo1, File1),
    {Res0, Res1}.

