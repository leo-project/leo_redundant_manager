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
%% Leo Redundant Manager
%% @doc
%% @end
%%======================================================================
%% Application Name
-define(APP, 'leo_redundant_manager').


%% Error
-define(ERROR_COULD_NOT_GET_RING,         "Could not get ring").
-define(ERROR_COULD_NOT_GET_CHECKSUM,     "Could not get checksum").
-define(ERROR_COULD_NOT_UPDATE_RING,      "Could not update ring").
-define(ERROR_COULD_NOT_GET_REDUNDANCIES, "Could not get redundancies").

-define(ERR_TYPE_INCONSISTENT_HASH,   inconsistent_hash).
-define(ERR_TYPE_NODE_DOWN,           nodedown).


%% Property name
-define(PROP_N, 'n').
-define(PROP_W, 'w').
-define(PROP_R, 'r').
-define(PROP_D, 'd').
-define(PROP_L1, 'level_1').
-define(PROP_L2, 'level_2').
-define(PROP_RING_BIT, 'bit_of_ring').

-define(DEF_MIN_REPLICAS, 1).
-define(DEF_MAX_REPLICAS, 8).

-define(DB_ETS,    'ets').
-define(DB_MNESIA, 'mnesia').

%% Member tables
-define(MEMBER_TBL_CUR,  'leo_members_cur').
-define(MEMBER_TBL_PREV, 'leo_members_prev').
-type(member_table() :: ?MEMBER_TBL_CUR | ?MEMBER_TBL_PREV).


%% Ring related
-define(TYPE_RING_TABLE_ETS,    'ets').
-define(TYPE_RING_TABLE_MNESIA, 'mnesia').
-define(RING_TBL_CUR,           'leo_ring_cur').
-define(RING_TBL_PREV,          'leo_ring_prv').
-define(NODE_ALIAS_PREFIX,      "node_").

-type(ring_table_type() :: ?TYPE_RING_TABLE_ETS | ?TYPE_RING_TABLE_MNESIA).
-type(ring_table_info() :: {ring_table_type(), ?RING_TBL_CUR} |
                           {ring_table_type(), ?RING_TBL_PREV}).

-define(WORKER_POOL_NAME_PREFIX, "leo_redundant_manager_worker_").

-define(RING_WORKER_POOL_NAME, 'ring_worker_pool').
-ifdef(TEST).
-define(RING_WORKER_POOL_SIZE, 1).
-define(RING_WORKER_POOL_BUF,  0).
-else.
-define(RING_WORKER_POOL_SIZE, 8).
-define(RING_WORKER_POOL_BUF,  0).
-endif.

%% Checksum
-define(CHECKSUM_RING,   'ring').
-define(CHECKSUM_MEMBER, 'member').

-type(checksum_type()   :: ?CHECKSUM_RING  | ?CHECKSUM_MEMBER).


%% Default
-define(MD5, 128).
-define(DEF_OPT_N, 1).
-define(DEF_OPT_R, 1).
-define(DEF_OPT_W, 1).
-define(DEF_OPT_D, 1).
-define(DEF_OPT_BIT_OF_RING, ?MD5).
-ifdef(TEST).
-define(DEF_NUMBER_OF_VNODES, 32).
-else.
-define(DEF_NUMBER_OF_VNODES, 168).
-endif.


%% Node State
%%
-define(STATE_ATTACHED,  'attached').
-define(STATE_DETACHED,  'detached').
-define(STATE_SUSPEND,   'suspend').
-define(STATE_RUNNING,   'running').
-define(STATE_STOP,      'stop').
-define(STATE_RESTARTED, 'restarted').
-define(STATE_RESERVED,  'reserved').

-type(node_state() :: ?STATE_ATTACHED |
                      ?STATE_DETACHED |
                      ?STATE_SUSPEND  |
                      ?STATE_RUNNING  |
                      ?STATE_STOP     |
                      ?STATE_RESTARTED).


%% Property
%%
-define(PROP_SERVER_TYPE,   'server_type').
-define(PROP_MANAGERS,      'managers').
-define(PROP_NOTIFY_MF,     'notify_mf').
-define(PROP_SYNC_MF,       'sync_mf').
-define(PROP_OPTIONS,       'options').
-define(PROP_MEMBERS,       'members').
-define(PROP_RING_HASH,     'ring_hash').
-define(PROP_CUR_RING_TBL,  'cur_ring_table').
-define(PROP_PREV_RING_TBL, 'prev_ring_table').


%% Version
%%
-define(VER_CURRENT, 'cur' ).
-define(VER_PREV,    'prev').
-define(member_table(_VER), case _VER of
                                ?VER_CURRENT -> ?MEMBER_TBL_CUR;
                                ?VER_PREV    -> ?MEMBER_TBL_PREV;
                                _ -> undefind
                            end).
-define(ring_table(_Target),case _Target of
                                ?SYNC_MODE_CUR_RING  ->
                                    leo_redundant_manager_api:table_info(?VER_CURRENT);
                                ?SYNC_MODE_PREV_RING ->
                                    leo_redundant_manager_api:table_info(?VER_PREV);
                                _ ->
                                    undefind
                            end).

%% Synchronization
%%
-define(SYNC_MODE_BOTH,      'both_rings').
-define(SYNC_MODE_MEMBERS,   'members').
-define(SYNC_MODE_CUR_RING,  'ring_cur').
-define(SYNC_MODE_PREV_RING, 'ring_prev').

-type(sync_mode() :: ?SYNC_MODE_BOTH | ?SYNC_MODE_MEMBERS |
                     ?SYNC_MODE_CUR_RING | ?SYNC_MODE_PREV_RING).


%% Server Type
%%
-define(SERVER_MANAGER, 'manager').
-define(SERVER_GATEWAY, 'gateway').
-define(SERVER_STORAGE, 'storage').


%% Dump File
%%
-define(DEF_LOG_DIR_MEMBERS,    "./log/ring/").
-define(DEF_LOG_DIR_RING,       "./log/ring/").
-define(DUMP_FILE_MEMBERS_CUR,  "members_cur.dump.").
-define(DUMP_FILE_MEMBERS_PREV, "members_prv.dump.").
-define(DUMP_FILE_RING_CUR,     "ring_cur.dump.").
-define(DUMP_FILE_RING_PREV,    "ring_prv.dump.").


%% Record
%%
-record(member,
        {node                  :: atom(),        %% actual node-name
         alias = []            :: string(),      %% node-alias
         ip = "0.0.0.0"        :: string(),      %% ip-address
         port  = 13075         :: pos_integer(), %% port-number
         inet  = 'ipv4'        :: 'ipv4'|'ipv6', %% type of ip
         clock = 0             :: pos_integer(), %% joined at
         state = null          :: atom(),        %% current-status
         num_of_vnodes = ?DEF_NUMBER_OF_VNODES :: integer(), %% # of vnodes
         grp_level_1 = []      :: string(),      %% Group of level_1
         grp_level_2 = []      :: string()       %% Group of level_2
        }).

-record(sync_info, {
          target            :: ?VER_CURRENT | ?VER_PREV,
          org_checksum = 0  :: pos_integer(),    %% original checksum
          cur_checksum = 0  :: pos_integer()     %% current chechsum
         }).

-record(vnodeid_nodes, {
          id = 0            :: pos_integer(),    %% id
          vnode_id_from = 0 :: pos_integer(),    %% vnode-id's from
          vnode_id_to = 0   :: pos_integer(),    %% vnode-id's to
          nodes             :: list()            %% list of nodes
         }).

-record(ring_group, {
          index_from = 0     :: pos_integer(),   %% group-index's from
          index_to = 0       :: pos_integer(),   %% group-index's to
          vnodeid_nodes_list :: list(#vnodeid_nodes{}) %% list of vnodeid(s)
         }).

-record(ring_info, {
          checksum = -1      :: integer(),       %% Ring's checksum
          first_vnode_id = 0 :: pos_integer(),   %% start vnode-id
          last_vnode_id = 0  :: pos_integer(),   %% end vnode-id
          ring_group_list    :: list(#ring_group{}), %% list of groups
          members = []       :: list(#member{})  %% cluster-members
         }).

-record(node_state, {
          node                 :: atom(),        %% actual node-name
          state                :: atom(),        %% current-status
          ring_hash_new = "-1" :: string(),      %% current ring-hash
          ring_hash_old = "-1" :: string(),      %% prev ring-hash
          when_is   = 0        :: pos_integer(), %% joined at
          error     = 0        :: pos_integer()  %% # of errors
         }).

-record(redundant_node, {
          node                   :: atom(),      %% node name
          available       = true :: boolean(),   %% alive/dead
          can_read_repair = true :: boolean()    %% able to execute read-repair in case of 'Get Operation'
         }).

-record(redundancies,
        {id = -1               :: pos_integer(), %% ring's address
         vnode_id_from = -1    :: pos_integer(), %% start of vnode_id
         vnode_id_to = -1      :: pos_integer(), %% end   of vnode_id (ex. vnode_id)
         temp_nodes = []       :: list(),        %% tempolary objects of redundant-nodes
         temp_level_2 = []     :: list(),        %% tempolary list of level-2's node
         nodes = []            :: list(#redundant_node{}), %% objects of redundant-nodes
         n = 0                 :: pos_integer(), %% # of replicas
         r = 0                 :: pos_integer(), %% # of successes of READ
         w = 0                 :: pos_integer(), %% # of successes of WRITE
         d = 0                 :: pos_integer(), %% # of successes of DELETE
         level_1 = 0           :: pos_integer(), %% # of dc-awareness's replicas
         level_2 = 0           :: pos_integer(), %% # of rack-awareness's replicas
         ring_hash = -1        :: pos_integer()  %% ring-hash when writing an object
        }).

-record(ring,
        {vnode_id = -1         :: pos_integer(),
         node                  :: atom()
        }).
