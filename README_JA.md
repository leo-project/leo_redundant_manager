# leo_redundant_manager

LeoFS向け冗長性・クラスタ管理ライブラリ

## 概要

- **leo_redundant_manager** は、[LeoFS](https://github.com/leo-project/leofs) 分散ストレージシステム向けの冗長性管理とクラスタ協調を提供するErlang/OTPライブラリーです。
- コンシステントハッシュに基づくルーティングテーブル(RING)を管理し、クラスタノードを監視し、分散されたストレージノードとゲートウェイノード間でデータの一貫性を維持します。

### 主な役割

- **Ring管理**: データ分散のためのコンシステントハッシュリングを維持
- **クラスタメンバーシップ**: ノード状態の追跡とライフサイクル管理（参加/離脱/一時停止）
- **冗長性クエリ**: 読み書き操作のレプリカ配置先を決定
- **マルチDCレプリケーション**: 複数データセンター間のレプリケーションを調整

### LeoFSでの利用

本ライブラリは以下のコンポーネントで使用されています:
- [leo_storage](https://github.com/leo-project/leo_storage) - ストレージノード
- [leo_gateway](https://github.com/leo-project/leo_gateway) - ゲートウェイノード
- [leo_manager](https://github.com/leo-project/leo_manager) - クラスタマネージャ

## コア機能

### 1. 仮想ノードを用いたコンシステントハッシュ

N-wayコンシステントハッシュを実装し、クラスタノード間でデータを均等に分散します。

- **ハッシュアルゴリズム**: MD5（128ビットアドレス空間）
- **仮想ノード**: 物理ノードあたり168 vnode（設定可能）
- **リバランス**: ノードの参加・離脱時に自動的に再分散

```
Key → MD5 Hash → VNode ID → レプリカノード群
```

### 2. デュアルリングアーキテクチャ

クラスタ変更時の無停止運用のため、2つのリングバージョンを維持します。

| Ring | テーブル | 用途 |
|------|---------|------|
| Current | `leo_ring_cur` | 書き込み操作 |
| Previous | `leo_ring_prv` | リバランス中の読み取り操作 |

このアーキテクチャにより以下を実現:
- シームレスなリング遷移
- リングバージョン間でのリードリペア
- トポロジ変更中の一貫したデータアクセス

### 3. 設定可能な一貫性レベル

N/R/W/Dパラメータによるチューナブル一貫性をサポート:

| パラメータ | 説明 | デフォルト |
|-----------|------|-----------|
| `n` | レプリカ数 | 1 |
| `r` | 読み取りクォーラム（成功に必要なレプリカ数） | 1 |
| `w` | 書き込みクォーラム（成功に必要なレプリカ数） | 1 |
| `d` | 削除クォーラム（成功に必要なレプリカ数） | 1 |

### 4. レプリカ配置の認識機能

インテリジェントなレプリカ分散をサポート:

- **DC認識** (`level_1`): データセンター間でレプリカを分散
- **ラック認識** (`level_2`): ラック間でレプリカを分散

### 5. ノード状態管理

定義された状態を通じてノードのライフサイクルを追跡:

```
attached → running ⇄ suspend → detached
              ↓
           restarted
```

| 状態 | 説明 |
|------|------|
| `attached` | 登録済みだが非アクティブ |
| `running` | アクティブでリクエスト処理中 |
| `suspend` | 一時的に停止 |
| `restarted` | 障害から復旧 |
| `detached` | クラスタから削除済み |

### 6. マルチDCレプリケーション（MDCR）

地理的に分散したクラスタ間のレプリケーションを調整:

- リモートクラスタ設定管理
- データセンター間メンバーシップ追跡
- 非同期レプリケーション調整

## アーキテクチャ

### モジュール構成

```
leo_redundant_manager/
├── leo_redundant_manager.erl          # メインgen_server
├── leo_redundant_manager_api.erl      # パブリックAPI
├── leo_redundant_manager_worker.erl   # 高性能リング検索
├── leo_redundant_manager_chash.erl    # コンシステントハッシュ実装
├── leo_cluster_tbl_ring.erl           # Ringテーブル操作
├── leo_cluster_tbl_member.erl         # メンバーテーブル操作
├── leo_cluster_tbl_conf.erl           # システム設定
├── leo_membership_cluster_local.erl   # ローカルクラスタメンバーシップ
├── leo_membership_cluster_remote.erl  # リモートクラスタメンバーシップ
└── leo_mdcr_tbl_*.erl                 # マルチDCレプリケーションテーブル群
```

### 主要データ構造

**Ringエントリ**:

```erlang
{vnode_id, node, clock}
```

**メンバー**:

```erlang
{node, alias, ip, port, inet, clock, state, num_of_vnodes, grp_level_1, grp_level_2}
```

**冗長性情報** (クエリ結果):

```erlang
{id, vnode_id_from, vnode_id_to, nodes, n, r, w, d, level_1, level_2, ring_hash}
```

### ストレージバックエンド

| ノードタイプ | バックエンド | 永続性 |
|-------------|-------------|--------|
| Monitorノード | Mnesia (disc_copies) | 永続 |
| Storage/Gatewayノード | ETS | インメモリ |

## APIリファレンス

### Ring操作

```erlang
%% メンバーとオプションを指定してRingを作成
leo_redundant_manager_api:create() -> ok | {error, Reason}
leo_redundant_manager_api:create(Members) -> ok | {error, Reason}
leo_redundant_manager_api:create(Members, Options) -> ok | {error, Reason}

%% Ringを取得
leo_redundant_manager_api:get_ring() -> {ok, Ring} | {error, Reason}

%% リバランスを実行
leo_redundant_manager_api:rebalance() -> ok | {error, Reason}
```

### 冗長性クエリ

```erlang
%% キーに対するレプリカノードを取得
leo_redundant_manager_api:get_redundancies_by_key(Key) ->
    {ok, #redundancies{}} | {error, Reason}

%% アドレスIDに対するレプリカノードを取得
leo_redundant_manager_api:get_redundancies_by_addr_id(AddrId) ->
    {ok, #redundancies{}} | {error, Reason}
```

### ノード管理

```erlang
%% ノードをクラスタに追加
leo_redundant_manager_api:attach(Node) -> ok | {error, Reason}
leo_redundant_manager_api:attach(Node, AliasL1, AliasL2, IP, Port) -> ok | {error, Reason}

%% ノードをクラスタから切り離し
leo_redundant_manager_api:detach(Node) -> ok | {error, Reason}

%% ノードを一時停止
leo_redundant_manager_api:suspend(Node) -> ok | {error, Reason}
```

### メンバークエリ

```erlang
%% 全メンバーを取得
leo_redundant_manager_api:get_members() -> {ok, [#member{}]} | {error, Reason}

%% ノード名でメンバーを取得
leo_redundant_manager_api:get_member_by_node(Node) -> {ok, #member{}} | {error, Reason}

%% ステータスでメンバーを取得
leo_redundant_manager_api:get_members_by_status(Status) -> {ok, [#member{}]} | {error, Reason}
```

### ステータス・監視

```erlang
%% 検証用チェックサムを取得
leo_redundant_manager_api:checksum(Type) -> {ok, Checksum} | {error, Reason}
%% Type: ring | member | worker | system_conf

%% システムヘルスチェック
leo_redundant_manager_api:is_alive() -> ok | {error, Reason}

%% 強制同期
leo_redundant_manager_api:force_sync_workers() -> ok
```

## 設定

### システム設定

```erlang
%% sys.configでの設定例
{leo_redundant_manager, [
    {n, 3},                    %% レプリカ数
    {r, 1},                    %% 読み取りクォーラム
    {w, 2},                    %% 書き込みクォーラム
    {d, 2},                    %% 削除クォーラム
    {bit_of_ring, 128},        %% Ringビット幅（固定）
    {num_of_vnodes, 168},      %% ノードあたりの仮想ノード数
    {level_1, 1},              %% DC認識レプリカ数
    {level_2, 0},              %% ラック認識レプリカ数
    {log_dir_ring, "./log/ring/"}
]}
```

### マルチDC設定

```erlang
{num_of_dc_replicas, 1},       %% DCレプリケーション先数
{max_mdc_targets, 2},          %% 最大MDCターゲット数
{mdcr_r, 1},                   %% MDC読み取りクォーラム
{mdcr_w, 1},                   %% MDC書き込みクォーラム
{mdcr_d, 1}                    %% MDC削除クォーラム
```

## 動作要件

- **Erlang/OTP**: 19.3以降（OTP 28まで対応）

### 依存ライブラリ

| ライブラリ | バージョン | 用途 |
|-----------|-----------|------|
| [leo_mq](https://github.com/leo-project/leo_mq) | 2.1.0 | 非同期操作用メッセージキュー |
| [leo_rpc](https://github.com/leo-project/leo_rpc) | 0.11.0 | ノード間RPC通信 |

## ビルド

```bash
# コンパイル
make compile

# テスト実行
make eunit

# 型チェック
make dialyzer

# ドキュメント生成
make doc
```

## ライセンス

Apache License, Version 2.0

## スポンサー

- [Lions Data, Ltd.](https://lions-data.com/)（2019年1月〜）
- [Rakuten, Inc.](https://global.rakuten.com/corp/)（2012年〜2018年12月）
