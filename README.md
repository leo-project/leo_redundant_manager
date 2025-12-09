# leo_redundant_manager

Redundancy and Cluster Management Library for LeoFS

## Overview

**leo_redundant_manager** is an Erlang/OTP application that provides redundancy management and cluster coordination for [LeoFS](https://github.com/leo-project/leofs) distributed storage system. It manages routing tables (RING) based on consistent hashing, monitors cluster nodes, and maintains data consistency across distributed storage and gateway nodes.

### Key Responsibilities

- **Ring Management**: Maintains consistent hash ring for data distribution
- **Cluster Membership**: Tracks node states and manages node lifecycle (join/leave/suspend)
- **Redundancy Queries**: Determines replica locations for read/write operations
- **Multi-DC Replication**: Coordinates replication across multiple data centers

### Usage in LeoFS

This library is used by:
- [leo_storage](https://github.com/leo-project/leo_storage) - Storage node
- [leo_gateway](https://github.com/leo-project/leo_gateway) - Gateway node
- [leo_manager](https://github.com/leo-project/leo_manager) - Cluster manager

## Core Features

### 1. Consistent Hashing with Virtual Nodes

Implements N-way consistent hashing to distribute data evenly across cluster nodes.

- **Hash Algorithm**: MD5 (128-bit address space)
- **Virtual Nodes**: 168 vnodes per physical node (configurable)
- **Rebalancing**: Automatic redistribution when nodes join or leave

```
Key → MD5 Hash → VNode ID → Replica Nodes
```

### 2. Dual Ring Architecture

Maintains two ring versions for zero-downtime operations during cluster changes.

| Ring | Table | Purpose |
|------|-------|---------|
| Current | `leo_ring_cur` | Write operations |
| Previous | `leo_ring_prv` | Read operations during rebalancing |

This architecture enables:
- Seamless ring transitions
- Read repair across ring versions
- Consistent data access during topology changes

### 3. Configurable Consistency Levels

Supports tunable consistency with N/R/W/D parameters:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `n` | Number of replicas | 1 |
| `r` | Read quorum (replicas for successful read) | 1 |
| `w` | Write quorum (replicas for successful write) | 1 |
| `d` | Delete quorum (replicas for successful delete) | 1 |

### 4. Replica Placement Awareness

Supports intelligent replica distribution:

- **DC-Awareness** (`level_1`): Distribute replicas across data centers
- **Rack-Awareness** (`level_2`): Distribute replicas across racks

### 5. Node State Management

Tracks node lifecycle through defined states:

```
attached → running ⇄ suspend → detached
              ↓
           restarted
```

| State | Description |
|-------|-------------|
| `attached` | Node registered but not active |
| `running` | Node active and serving requests |
| `suspend` | Node temporarily paused |
| `restarted` | Node recovered from failure |
| `detached` | Node removed from cluster |

### 6. Multi-DC Replication (MDCR)

Coordinates replication across geographically distributed clusters:

- Remote cluster configuration management
- Cross-datacenter membership tracking
- Asynchronous replication coordination

## Architecture

### Module Structure

```
leo_redundant_manager/
├── leo_redundant_manager.erl          # Main gen_server
├── leo_redundant_manager_api.erl      # Public API
├── leo_redundant_manager_worker.erl   # High-performance ring lookups
├── leo_redundant_manager_chash.erl    # Consistent hashing implementation
├── leo_cluster_tbl_ring.erl           # Ring table operations
├── leo_cluster_tbl_member.erl         # Member table operations
├── leo_cluster_tbl_conf.erl           # System configuration
├── leo_membership_cluster_local.erl   # Local cluster membership
├── leo_membership_cluster_remote.erl  # Remote cluster membership
└── leo_mdcr_tbl_*.erl                 # Multi-DC replication tables
```

### Key Data Structures

**Ring Entry**:

```erlang
{vnode_id, node, clock}
```

**Member**:

```erlang
{node, alias, ip, port, inet, clock, state, num_of_vnodes, grp_level_1, grp_level_2}
```

**Redundancies** (query result):

```erlang
{id, vnode_id_from, vnode_id_to, nodes, n, r, w, d, level_1, level_2, ring_hash}
```

### Storage Backend

| Node Type | Backend | Persistence |
|-----------|---------|-------------|
| Monitor Node | Mnesia (disc_copies) | Persistent |
| Storage/Gateway Node | ETS | In-memory |

## API Reference

### Ring Operations

```erlang
%% Create ring with members and options
leo_redundant_manager_api:create() -> ok | {error, Reason}
leo_redundant_manager_api:create(Members) -> ok | {error, Reason}
leo_redundant_manager_api:create(Members, Options) -> ok | {error, Reason}

%% Retrieve ring
leo_redundant_manager_api:get_ring() -> {ok, Ring} | {error, Reason}

%% Trigger rebalancing
leo_redundant_manager_api:rebalance() -> ok | {error, Reason}
```

### Redundancy Queries

```erlang
%% Get replica nodes for a key
leo_redundant_manager_api:get_redundancies_by_key(Key) ->
    {ok, #redundancies{}} | {error, Reason}

%% Get replica nodes for an address ID
leo_redundant_manager_api:get_redundancies_by_addr_id(AddrId) ->
    {ok, #redundancies{}} | {error, Reason}
```

### Node Management

```erlang
%% Attach node to cluster
leo_redundant_manager_api:attach(Node) -> ok | {error, Reason}
leo_redundant_manager_api:attach(Node, AliasL1, AliasL2, IP, Port) -> ok | {error, Reason}

%% Detach node from cluster
leo_redundant_manager_api:detach(Node) -> ok | {error, Reason}

%% Suspend node
leo_redundant_manager_api:suspend(Node) -> ok | {error, Reason}
```

### Member Queries

```erlang
%% Get all members
leo_redundant_manager_api:get_members() -> {ok, [#member{}]} | {error, Reason}

%% Get member by node
leo_redundant_manager_api:get_member_by_node(Node) -> {ok, #member{}} | {error, Reason}

%% Get members by status
leo_redundant_manager_api:get_members_by_status(Status) -> {ok, [#member{}]} | {error, Reason}
```

### Status & Monitoring

```erlang
%% Get checksums for verification
leo_redundant_manager_api:checksum(Type) -> {ok, Checksum} | {error, Reason}
%% Type: ring | member | worker | system_conf

%% Check system health
leo_redundant_manager_api:is_alive() -> ok | {error, Reason}

%% Force synchronization
leo_redundant_manager_api:force_sync_workers() -> ok
```

## Configuration

### System Configuration

```erlang
%% Example configuration in sys.config
{leo_redundant_manager, [
    {n, 3},                    %% Number of replicas
    {r, 1},                    %% Read quorum
    {w, 2},                    %% Write quorum
    {d, 2},                    %% Delete quorum
    {bit_of_ring, 128},        %% Ring bit width (fixed)
    {num_of_vnodes, 168},      %% Virtual nodes per node
    {level_1, 1},              %% DC-awareness replicas
    {level_2, 0},              %% Rack-awareness replicas
    {log_dir_ring, "./log/ring/"}
]}
```

### Multi-DC Configuration

```erlang
{num_of_dc_replicas, 1},       %% DC replication targets
{max_mdc_targets, 2},          %% Max MDC targets
{mdcr_r, 1},                   %% MDC read quorum
{mdcr_w, 1},                   %% MDC write quorum
{mdcr_d, 1}                    %% MDC delete quorum
```

## Requirements

- **Erlang/OTP**: 19.3 or later (supports up to OTP 28)

### Dependencies

| Library | Version | Purpose |
|---------|---------|---------|
| [leo_mq](https://github.com/leo-project/leo_mq) | 2.1.0 | Message queue for async operations |
| [leo_rpc](https://github.com/leo-project/leo_rpc) | 0.11.0 | RPC for inter-node communication |

## Build

```bash
# Compile
make compile

# Run tests
make eunit

# Type checking
make dialyzer

# Generate documentation
make doc
```

## License

Apache License, Version 2.0

## Sponsors

- [Lions Data, Ltd.](https://lions-data.com/) (since January 2019)
- [Rakuten, Inc.](https://global.rakuten.com/corp/) (2012 - December 2018)
