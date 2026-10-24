# Directory capacity management

Directory capacity management divides an explicitly configured cluster data
budget between non-overlapping directory subtrees. It is disabled by default.
All masters must use the same configuration; changing it requires a coordinated
restart. A directory's unused share is not lent to another directory.

## Configuration

Add the following tables to the cluster TOML (choose `total` for your deployment;
100 TB below is an example, not an automatic disk-capacity measurement):

```toml
[master.capacity]
total = "100TB"

[[master.capacity.directories]]
path = "/datasets"
percent = 80
overflow = "evict_cache"
low_watermark_percent = 90

[[master.capacity.directories]]
path = "/checkpoints"
percent = 20
overflow = "reject"
```

Create/mount `/datasets` in `cache_mode`, with OSS as its authoritative source.
Use the desired replica count and block size on each mount; the capacity policy
does not set them. `/checkpoints` can be a native Curvine directory or an
`fs_mode` mount. Keep the existing global `enable_quota_eviction` disabled;
configuration validation rejects combining it with directory capacity management.

`total` is a stable budget after allowing for disk protection, metadata and
maintenance headroom. It does not shrink with heartbeat `available` or change
when a worker briefly disconnects. Percentages must be positive integers whose
sum is at most 100; roots must be canonical absolute, non-root, non-overlapping
paths. Data-file creation/allocation outside configured roots is rejected.
Directories themselves can be created to establish the hierarchy.

## Accounting and admission

This is **replica-accounted block-data capacity**, not an exact `statvfs` or `du`
measurement. Committed regular blocks are charged by data length; a fixed-extent
SPDK block retains at least its block allocation budget. Filesystem allocation
overhead, foreign processes and device fragmentation still require physical
headroom and the existing worker-side capacity checks. Quota availability does
not promise that enough suitable workers exist for placement.

Each block has a journaled charge containing its owning directory, inode,
replica target, known workers and lifecycle state. The directory's usage is:

```
committed + reserved + pending_delete
```

The three terms are disjoint. A new block reserves its block-size budget for all
target replicas before the allocation is returned. Concurrent admissions are
serialized by the namespace write lock. Retries update the same block charge.
Final close replaces the reservation with committed data size. Flush keeps the
reservation because the open writer can continue writing existing blocks without
another AddBlock RPC. Reopening an ordinary block reserves room for its committed
copy plus a full staging copy. Consequently, rewriting an existing checkpoint can
fail even when its final length would not grow.

Two replicas consume twice the per-copy data budget. For example, a 20 TB share
can hold at most roughly 10 TB of two-replica checkpoint data, with additional
headroom needed for writes and rewrites. This feature does not change the
underlying write durability or replica-placement guarantees.

Allocation failures before a block is published release their charge when the
metadata confirms that no block was created. Ambiguous operations remain charged.
Deletion remains charged until every recorded worker acknowledges deletion; a
timeout/offline worker is not proof of reclaimed disk space. Pending deletion is
persisted and re-enqueued after recovery. The target replica budget remains
reserved for active blocks; repairs to additional workers are admitted against
the same directory budget and conservatively retain uncertain old copies.

The original worker-wide `scheduled_bytes` and physical allocation checks remain
in force. This feature does not fix their heartbeat-based accounting or provide
per-directory bandwidth/IO isolation. A directory can therefore encounter a
worker allocation failure before reaching its own quota.

## Overflow behavior

`reject` returns the typed `QuotaExceeded` error (`EDQUOT` through FUSE). It never
automatically deletes checkpoints. Reads and operations that release space remain
available. Already admitted writes may finish. An unknown-size checkpoint can
fail on a later block: this is not a whole-checkpoint transaction or preflight.

`evict_cache` performs a bounded, directory-scoped LRU victim selection when a
new reservation would exceed the limit. It targets `low_watermark_percent` of
that directory's budget (90% of the 80% share in the example). Only complete,
UFS-backed, single-link files under a `cache_mode` mount are eligible. Writes in
progress, the requesting file, checkpoint directories and unbacked data are
excluded. TTL expiry is **not** required. Access ordering is based on metadata
location lookups and close; it is not a record of every cached data read, and it
is rebuilt cold after restart.

Eviction uses the internal Curvine deletion path and leaves the UFS source
untouched. Selecting a victim or enqueuing its deletion does not immediately free
the quota. The admission that triggers eviction returns `QuotaExceeded`; retry
the load after deletion acknowledgements. If no safe candidates exist, filling
fails rather than deleting unpersisted data. Existing unified-cache reads can
continue to use their configured UFS fallback. The manager does not synchronously
wait for worker IO while holding the namespace lock.

## Boundaries and recovery

- Same-domain rename keeps ownership. Cross-domain rename/hard-link operations,
  renaming/deleting a quota root or its ancestors, and overlapping quotas are
  rejected. Temporary files must be created within their destination domain.
- Set the replica count before allocating a file. Changing it on an allocated
  capacity-managed file is rejected in this version, including recursive
  attribute updates and attributes attached to close.
- Existing data is backfilled on first leader reconciliation/admission. Changing
  roots so existing charges move domains is not supported. Lowering a limit can
  leave a domain over quota; reductions remain allowed, growth is denied. Cache
  eviction is driven by subsequent admission pressure.
- Journal entries are appended using a new enum variant; RocksDB's `common`
  column family stores the ledger and includes it in snapshots. All masters must
  be upgraded before enabling this feature; old masters cannot replay the new
  journal entry. Downgrading or disabling while charges remain is rejected.
- Partial operations can conservatively retain space. Offline copies and
  uncertain repair targets are not reclaimed merely by timeout. This favors
  accounting safety over availability; inspect these records before operational
  recovery rather than clearing the ledger.
- This is a directory aggregate limit, not a dedicated worker pool or a guarantee
  that a particular fraction remains available on every disk. Leave adequate
  capacity on enough workers for checkpoint replication.

## Observability and tests

`GET /api/capacity` on the Master web port returns each configured directory's
`limit`, `committed`, `reserved`, `pending_delete`, and `available` in bytes.
Use the active master for current information. No live policy mutation endpoint
is provided in this version.

```sh
cargo test -p curvine-master --lib
cargo test -p curvine-config
cargo test -p curvine-error --lib
cargo test -p curvine-fuse quota_exceeded_maps_to_edquot
cargo fmt --all -- --check
cargo clippy -p curvine-master -p curvine-config -p curvine-error -p curvine-fuse \
  --lib --tests -- -D warnings --allow clippy::uninlined-format-args
```

Validated in Linux Docker with Rust 1.98.1: Master 200 passed / 1 existing ignored,
configuration 144 passed, error handling 13 passed, and the FUSE quota mapping
test passed. Formatting and strict Clippy checks passed for the commands above.

Tests cover concurrent admission, two-replica charging, close settlement,
idempotent allocation, unassigned preallocation, staging reservations, deletion
acknowledgements, RocksDB restore, journal serialization/replay, recovered
replicas and SPDK allocation floors, path boundaries, unexpired cache eviction
and typed error propagation. Multi-node crash/recovery and mixed training/checkpoint
load testing are still required before production use.
