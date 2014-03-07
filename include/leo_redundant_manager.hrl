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

-define(ERR_TYPE_INCONSISTENT_HASH, inconsistent_hash).
-define(ERR_TYPE_NODE_DOWN,         nodedown).


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

-type(ring_table() :: ?TYPE_RING_TABLE_ETS | ?TYPE_RING_TABLE_MNESIA).
-type(ring_table_info() :: {ring_table(), ?RING_TBL_CUR} |
                           {ring_table(), ?RING_TBL_PREV}).

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
-define(DEF_NUM_OF_REMOTE_MEMBERS, 3).

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
-define(VER_CUR,  'cur' ).
-define(VER_PREV, 'prev').
-define(member_table(_VER), case _VER of
                                ?VER_CUR  -> ?MEMBER_TBL_CUR;
                                ?VER_PREV -> ?MEMBER_TBL_PREV;
                                _ -> undefind
                            end).
-define(ring_table(_Target),case _Target of
                                ?SYNC_TARGET_RING_CUR  ->
                                    leo_redundant_manager_api:table_info(?VER_CUR);
                                ?SYNC_TARGET_RING_PREV ->
                                    leo_redundant_manager_api:table_info(?VER_PREV);
                                _ ->
                                    undefind
                            end).
-define(ring_table_to_member_table(_Tbl), case _Tbl of
                                              {_, ?RING_TBL_CUR} ->
                                                  ?MEMBER_TBL_CUR;
                                              {_, ?RING_TBL_PREV} ->
                                                  ?MEMBER_TBL_PREV
                                          end).
-define(sync_target_to_ver(_Target), case _Target of
                                         ?SYNC_TARGET_RING_CUR  -> ?VER_CUR;
                                         ?SYNC_TARGET_RING_PREV -> ?VER_PREV
                                     end).

%% Synchronization
%%
-define(SYNC_TARGET_BOTH,      'both').
-define(SYNC_TARGET_RING_CUR,  'ring_cur').
-define(SYNC_TARGET_RING_PREV, 'ring_prev').
-define(SYNC_TARGET_MEMBER,    'member').
-type(sync_target() :: ?SYNC_TARGET_BOTH |
                       ?SYNC_TARGET_RING_CUR  |
                       ?SYNC_TARGET_RING_PREV |
                       ?SYNC_TARGET_MEMBER).

%% Consensus Roles
%%
-define(CNS_ROLE_LEADER,     'L').
-define(CNS_ROLE_FOLLOWER_1, 'FL').
-define(CNS_ROLE_FOLLOWER_2, 'FR').
-define(CNS_ROLE_OBSERBER,   'O').
-type(consensus_role() :: ?CNS_ROLE_LEADER |
                          ?CNS_ROLE_FOLLOWER_1 |
                          ?CNS_ROLE_FOLLOWER_2 |
                          ?CNS_ROLE_OBSERBER).


%% Server Type
%%
-define(SERVER_MANAGER, 'manager').
-define(SERVER_GATEWAY, 'gateway').
-define(SERVER_STORAGE, 'storage').


%% Mnesia Tables
%%
-define(TBL_SYSTEM_CONF,    'leo_system_conf').
-define(TBL_CLUSTER_STAT,   'leo_cluster_stat').
-define(TBL_CLUSTER_INFO,   'leo_cluster_info').
-define(TBL_CLUSTER_MEMBER, 'leo_cluster_member').
-define(TBL_CLUSTER_MGR,    'leo_cluster_manager').
-undef(ERROR_MNESIA_NOT_START).
-define(ERROR_MNESIA_NOT_START, "Mnesia is not available").


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
%% Consistency Level
-record(system_conf, {
          version = 0         :: integer(),
          n       = 1         :: integer(),
          r       = 1         :: integer(),
          w       = 1         :: integer(),
          d       = 1         :: integer(),
          bit_of_ring = 128   :: integer(),
          level_1 = 0         :: integer(),
          level_2 = 0         :: integer()
         }).

-record(system_conf_1, {
          version = 0         :: integer(),
          cluster_id = []     :: string(),
          dc_id      = []     :: string(),
          n       = 1         :: integer(),
          r       = 1         :: integer(),
          w       = 1         :: integer(),
          d       = 1         :: integer(),
          bit_of_ring = 128   :: integer(),
          num_of_dc_replicas   = 0 :: integer(),
          num_of_rack_replicas = 0 :: integer()
         }).

-record(system_conf_2, {
          version = 0         :: integer(),      %% version
          cluster_id = []     :: string(),       %% cluster-id
          dc_id      = []     :: string(),       %% dc-id
          n       = 1         :: integer(),      %% # of replicas
          r       = 1         :: integer(),      %% # of replicas needed for a successful READ operation
          w       = 1         :: integer(),      %% # of replicas needed for a successful WRITE operation
          d       = 1         :: integer(),      %% # of replicas needed for a successful DELETE operation
          bit_of_ring = 128   :: integer(),      %% # of bits for the hash-ring (fixed 128bit)
          num_of_mdcr_targets  = 0 :: integer(), %% # of multi-dc replication targets for MDC-replication
          num_of_dc_replicas   = 0 :: integer(), %% # of destination of nodes a cluster for MDC-replication
          num_of_rack_replicas = 0 :: integer()  %% # of Rack-awareness replicas
         }).
-define(SYSTEM_CONF, 'system_conf_2').


-record(cluster_info, {
          cluster_id = []     :: string(),       %% cluster-id
          dc_id      = []     :: string(),       %% dc-id
          n       = 1         :: integer(),      %% # of replicas
          r       = 1         :: integer(),      %% # of replicas needed for a successful READ operation
          w       = 1         :: integer(),      %% # of replicas needed for a successful WRITE operation
          d       = 1         :: integer(),      %% # of replicas needed for a successful DELETE operation
          bit_of_ring = 128   :: integer(),      %% # of bits for the hash-ring (fixed 128bit)
          num_of_dc_replicas   = 0 :: integer(), %% # of replicas a DC for MDC-replication
          num_of_rack_replicas = 0 :: integer()  %% # of Rack-awareness replicas
         }).

-record(cluster_info_1, {
          cluster_id = []     :: string(),       %% cluster-id
          dc_id      = []     :: string(),       %% dc-id
          n       = 1         :: integer(),      %% # of replicas
          r       = 1         :: integer(),      %% # of replicas needed for a successful READ operation
          w       = 1         :: integer(),      %% # of replicas needed for a successful WRITE operation
          d       = 1         :: integer(),      %% # of replicas needed for a successful DELETE operation
          bit_of_ring = 128   :: integer(),      %% # of bits for the hash-ring (fixed 128bit)
          max_mdc_targets      = 0 :: integer(), %% max multi-dc replication targets for MDC-replication
          num_of_dc_replicas   = 0 :: integer(), %% # of replicas a DC for MDC-replication
          num_of_rack_replicas = 0 :: integer()  %% # of Rack-awareness replicas
         }).
-define(CLUSTER_INFO, 'cluster_info_1').


%% For Multi-DC Replication
-record(cluster_stat, {
          cluster_id = [] :: string(),      %% cluster-id
          status = null   :: node_state(),  %% status:[running | stop]
          checksum = 0    :: pos_integer(), %% checksum of members
          updated_at = 0  :: pos_integer()  %% updated at
         }).

-record(cluster_stat_1, {
          cluster_id = [] :: string(),      %% cluster-id
          state = null    :: node_state(),  %% status:[running | stop]
          checksum = 0    :: pos_integer(), %% checksum of members
          updated_at = 0  :: pos_integer()  %% updated at
         }).
-define(CLUSTER_STAT, 'cluster_stat_1').

%% Cluster Manager
-record(cluster_manager, {
          node                :: atom(),        %% actual node-name
          cluster_id = []     :: string()       %% cluster-id
         }).

%% Cluster Members
-record(cluster_member, {
          node                :: atom(),        %% actual node-name
          cluster_id = []     :: string(),      %% cluster-id
          alias = []          :: string(),      %% node-alias
          ip = "0.0.0.0"      :: string(),      %% ip-address
          port  = 13075       :: pos_integer(), %% port-number
          inet  = 'ipv4'      :: 'ipv4'|'ipv6', %% type of ip
          clock = 0           :: pos_integer(), %% joined at
          num_of_vnodes = ?DEF_NUMBER_OF_VNODES :: integer(), %% # of vnodes
          status = null       :: node_state()
         }).
-record(cluster_member_1, {
          node                :: atom(),        %% actual node-name
          cluster_id = []     :: string(),      %% cluster-id
          alias = []          :: string(),      %% node-alias
          ip = "0.0.0.0"      :: string(),      %% ip-address
          port  = 13075       :: pos_integer(), %% port-number
          inet  = 'ipv4'      :: 'ipv4'|'ipv6', %% type of ip
          clock = 0           :: pos_integer(), %% joined at
          num_of_vnodes = ?DEF_NUMBER_OF_VNODES :: integer(), %% # of vnodes
          state = null        :: node_state()
         }).
-define(CLUSTER_MEMBER, 'cluster_member_1').


-record(member,
        {node                 :: atom(),        %% actual node-name
         alias = []           :: string(),      %% node-alias
         ip = "0.0.0.0"       :: string(),      %% ip-address
         port  = 13075        :: pos_integer(), %% port-number
         inet  = 'ipv4'       :: 'ipv4'|'ipv6', %% type of ip
         clock = 0            :: pos_integer(), %% joined at
         state = null         :: node_state(),  %% current-status
         num_of_vnodes = ?DEF_NUMBER_OF_VNODES :: integer(), %% # of vnodes
         grp_level_1 = []     :: string(),      %% Group of level_1 for multi-dc replication
         grp_level_2 = []     :: string()       %% Group of level_2 for rack-awareness replication
        }).


-record(sync_info, {
          target            :: ?VER_CUR | ?VER_PREV,
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
          can_read_repair = true :: boolean(),   %% able to execute read-repair in case of 'Get Operation'
          role                   :: consensus_role() %% consensus's role
                                    %%   [leader, follower_1. follower_2, observer]
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
        {vnode_id = -1 :: pos_integer(),
         node          :: atom()
        }).
-record(ring_0_16_8,
        {vnode_id = -1 :: pos_integer(),
         node          :: atom(),
         clock = 0     :: pos_integer()
        }).
-define(RING, 'ring_0_16_8').


-record(rebalance, {members_cur  = []  :: list(),
                    members_prev = []  :: list(),
                    tbl_cur            :: atom(),
                    tbl_prev           :: atom()
                   }).

