use crate::master::{fs::MasterFilesystem, journal::JournalSystem, Master};
use curvine_config::{CapacityConf, CapacityOverflow, ClusterConf, DirectoryCapacityConf};
use curvine_error::FsError;
use curvine_model::*;
use curvine_runtime::common::Utils;

const MIB: i64 = 1024 * 1024;

fn filesystem(name: &str) -> MasterFilesystem {
    filesystem_with_journal(name, false)
}

fn filesystem_with_journal(name: &str, journal: bool) -> MasterFilesystem {
    Master::init_test_metrics();
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = journal;
    conf.master.meta_dir =
        Utils::test_sub_dir(format!("capacity/meta-{name}-{}", Utils::rand_str(6)));
    conf.journal.journal_dir =
        Utils::test_sub_dir(format!("capacity/journal-{name}-{}", Utils::rand_str(6)));
    conf.master.capacity = CapacityConf {
        total: "10MB".into(),
        directories: vec![
            DirectoryCapacityConf {
                path: "/datasets".into(),
                percent: 80,
                overflow: CapacityOverflow::EvictCache,
                low_watermark_percent: 90,
            },
            DirectoryCapacityConf {
                path: "/checkpoints".into(),
                percent: 20,
                overflow: CapacityOverflow::Reject,
                low_watermark_percent: 90,
            },
        ],
    };
    let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
    for id in 1..=2 {
        let mut w = WorkerInfo::new(
            WorkerAddress {
                worker_id: id,
                ..Default::default()
            },
            1,
        );
        w.status = WorkerStatus::Live;
        w.capacity = 1 << 30;
        w.available = 1 << 30;
        fs.add_test_worker(w);
    }
    fs
}

fn create(fs: &MasterFilesystem, path: &str, replicas: i32, block_size: i64) -> FileStatus {
    fs.create_with_opts(
        path,
        CreateFileOptsBuilder::new()
            .create_parent(true)
            .replicas(replicas)
            .block_size(block_size)
            .client_name("capacity-test".into())
            .build(),
        OpenFlags::new_create(),
    )
    .unwrap()
}

fn allocate(fs: &MasterFilesystem, path: &str) -> Result<LocatedBlock, FsError> {
    fs.add_block(
        path,
        None,
        ClientAddress::default(),
        vec![],
        vec![],
        0,
        None,
    )
}

fn finish(fs: &MasterFilesystem, path: &str, block: &LocatedBlock, len: i64) {
    let commit = CommitBlock {
        block_id: block.block.id,
        block_len: len,
        locations: block
            .locs
            .iter()
            .map(|w| BlockLocation::with_id(w.worker_id))
            .collect(),
    };
    fs.complete_file(path, None, len, vec![commit], "capacity-test", false, None)
        .unwrap();
}

fn deleted(fs: &MasterFilesystem, block: &LocatedBlock, worker: u32) {
    fs.block_report(
        BlockReportList {
            cluster_id: String::new(),
            worker_id: worker,
            full_report: false,
            total_len: 1,
            report_id: 0,
            blocks: vec![BlockReportInfo::with_deleted(block.block.id, 0)],
        },
        None,
    )
    .unwrap();
}

#[test]
fn capacity_checkpoint_admission_includes_two_replicas() {
    let fs = filesystem("hard-limit");
    create(&fs, "/checkpoints/a", 2, MIB);
    let a = allocate(&fs, "/checkpoints/a").unwrap();
    create(&fs, "/checkpoints/b", 2, MIB);
    assert!(matches!(
        allocate(&fs, "/checkpoints/b"),
        Err(FsError::QuotaExceeded(_))
    ));
    assert_eq!(
        fs.directory_capacity_usage().unwrap()[1].reserved,
        2 * MIB as u64
    );
    // A rejected checkpoint never steals the read domain's remaining capacity.
    create(&fs, "/datasets/a", 1, MIB);
    allocate(&fs, "/datasets/a").unwrap();
    finish(&fs, "/checkpoints/a", &a, MIB / 4);
    assert_eq!(
        fs.directory_capacity_usage().unwrap()[1].committed,
        MIB as u64 / 2
    );
    // Whole-block reservations still apply; configure a smaller block for small files.
    assert!(matches!(
        allocate(&fs, "/checkpoints/b"),
        Err(FsError::QuotaExceeded(_))
    ));
}

#[test]
fn capacity_delete_waits_for_each_replica_and_survives_restore() {
    let fs = filesystem("delete");
    create(&fs, "/checkpoints/a", 2, MIB);
    let a = allocate(&fs, "/checkpoints/a").unwrap();
    finish(&fs, "/checkpoints/a", &a, MIB);
    fs.delete("/checkpoints/a", false).unwrap();
    fs.fs_dir.write().restore_from_rocksdb().unwrap();
    fs.reconcile_capacity_deletes().unwrap();
    assert_eq!(
        fs.directory_capacity_usage().unwrap()[1].pending_delete,
        2 * MIB as u64
    );
    deleted(&fs, &a, a.locs[0].worker_id);
    deleted(&fs, &a, a.locs[0].worker_id);
    assert_eq!(
        fs.directory_capacity_usage().unwrap()[1].pending_delete,
        MIB as u64
    );
    deleted(&fs, &a, a.locs[1].worker_id);
    assert_eq!(
        fs.directory_capacity_usage().unwrap()[1].available,
        2 * MIB as u64
    );
    create(&fs, "/checkpoints/b", 2, MIB);
    allocate(&fs, "/checkpoints/b").unwrap();
}

#[test]
fn capacity_retries_and_inode_addressing_cannot_bypass_budget() {
    let fs = filesystem("retry");
    let status = create(&fs, "/checkpoints/a", 2, MIB);
    let first = allocate(&fs, "/checkpoints/a").unwrap();
    let retry = fs
        .add_block(
            "/datasets/fake",
            Some(status.id),
            ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        )
        .unwrap();
    assert_eq!(first.block.id, retry.block.id);
    assert_eq!(
        fs.directory_capacity_usage().unwrap()[1].reserved,
        2 * MIB as u64
    );
    assert!(fs
        .rename("/checkpoints/a", "/datasets/a", RenameFlags::empty())
        .is_err());
    assert!(fs.link("/checkpoints/a", "/datasets/a").is_err());
    assert!(fs
        .rename("/checkpoints", "/renamed", RenameFlags::empty())
        .is_err());
    assert!(fs.delete("/checkpoints", true).is_err());
}

#[test]
fn capacity_concurrent_admission_never_exceeds_limit() {
    let fs = filesystem("concurrent");
    let handles: Vec<_> = (0..12)
        .map(|i| {
            let fs = fs.clone();
            std::thread::spawn(move || {
                let path = format!("/checkpoints/{i}");
                create(&fs, &path, 2, MIB);
                allocate(&fs, &path).is_ok()
            })
        })
        .collect();
    let successes = handles
        .into_iter()
        .map(|h| usize::from(h.join().unwrap()))
        .sum::<usize>();
    assert_eq!(successes, 1);
    assert_eq!(
        fs.directory_capacity_usage().unwrap()[1].reserved,
        2 * MIB as u64
    );
}

#[test]
fn capacity_evicts_unexpired_cache_only_within_its_domain() {
    let fs = filesystem("eviction");
    let mount = MountInfo {
        cv_path: "/datasets".into(),
        ufs_path: "s3://bucket/datasets".into(),
        write_type: WriteType::CacheMode,
        ..Default::default()
    };
    fs.fs_dir
        .read()
        .store
        .store
        .add_mountpoint(1, &mount)
        .unwrap();
    create(&fs, "/datasets/old", 1, 8 * MIB);
    let old = allocate(&fs, "/datasets/old").unwrap();
    finish(&fs, "/datasets/old", &old, 8 * MIB);
    // Mark the cache as backed by a valid UFS copy. No TTL expiry is required.
    let guard = fs.fs_dir.write();
    let inp = MasterFilesystem::resolve_file_inode(&guard, "/datasets/old", None).unwrap();
    let mut inode = inp.as_ref().clone();
    if let crate::master::meta::inode::InodeView::File(f) = &mut inode {
        f.storage_policy.save_ufs(123);
    }
    let mut batch = guard.store.store.new_batch();
    batch.write_inode(&inode).unwrap();
    batch.commit().unwrap();
    drop(guard);
    // Restore the tree to ensure both in-memory and RocksDB views are synchronized.
    fs.fs_dir.write().restore_from_rocksdb().unwrap();
    create(&fs, "/checkpoints/keep", 2, MIB);
    let checkpoint = allocate(&fs, "/checkpoints/keep").unwrap();
    finish(&fs, "/checkpoints/keep", &checkpoint, MIB);
    create(&fs, "/datasets/new", 1, MIB);
    assert!(matches!(
        allocate(&fs, "/datasets/new"),
        Err(FsError::QuotaExceeded(_))
    ));
    assert!(fs.get_block_locations("/datasets/old").is_err());
    assert!(fs.get_block_locations("/checkpoints/keep").is_ok());
    assert_eq!(
        fs.directory_capacity_usage().unwrap()[0].pending_delete,
        8 * MIB as u64
    );
    deleted(&fs, &old, old.locs[0].worker_id);
    allocate(&fs, "/datasets/new").unwrap();
}

#[test]
fn capacity_reopen_charges_staging_and_fallocate_cannot_bypass() {
    let fs = filesystem("rewrite");
    create(&fs, "/checkpoints/a", 2, MIB);
    let a = allocate(&fs, "/checkpoints/a").unwrap();
    finish(&fs, "/checkpoints/a", &a, MIB);
    let opts = CreateFileOptsBuilder::new()
        .client_name("capacity-test".into())
        .build();
    assert!(matches!(
        fs.open_file("/checkpoints/a", opts, OpenFlags::new_write_only()),
        Err(FsError::QuotaExceeded(_))
    ));
    create(&fs, "/checkpoints/b", 2, MIB);
    assert!(matches!(
        fs.resize(
            "/checkpoints/b",
            FileAllocOpts::with_alloc(MIB, FileAllocMode::DEFAULT)
        ),
        Err(FsError::QuotaExceeded(_))
    ));
}

#[test]
fn capacity_unassigned_fallocate_is_released_by_truncate_and_delete() {
    let fs = filesystem("unassigned");
    create(&fs, "/checkpoints/a", 2, MIB);
    fs.resize(
        "/checkpoints/a",
        FileAllocOpts::with_alloc(MIB, FileAllocMode::DEFAULT),
    )
    .unwrap();
    assert_eq!(
        fs.directory_capacity_usage().unwrap()[1].reserved,
        2 * MIB as u64
    );
    fs.resize("/checkpoints/a", FileAllocOpts::with_truncate(0))
        .unwrap();
    assert_eq!(
        fs.directory_capacity_usage().unwrap()[1].available,
        2 * MIB as u64
    );
    fs.resize(
        "/checkpoints/a",
        FileAllocOpts::with_alloc(MIB, FileAllocMode::DEFAULT),
    )
    .unwrap();
    fs.delete("/checkpoints/a", false).unwrap();
    assert_eq!(
        fs.directory_capacity_usage().unwrap()[1].available,
        2 * MIB as u64
    );
}

#[test]
fn capacity_assign_worker_retry_does_not_add_new_replica_charges() {
    let fs = filesystem("assign");
    create(&fs, "/checkpoints/a", 2, MIB);
    let blocks = fs
        .resize(
            "/checkpoints/a",
            FileAllocOpts::with_alloc(MIB, FileAllocMode::DEFAULT),
        )
        .unwrap();
    let block = blocks.block_locs[0].block.clone();
    let first = fs
        .assign_worker(
            "/checkpoints/a",
            block.clone(),
            ClientAddress::default(),
            vec![],
        )
        .unwrap();
    let second = fs
        .assign_worker("/checkpoints/a", block, ClientAddress::default(), vec![])
        .unwrap();
    assert_eq!(
        first.locs.iter().map(|w| w.worker_id).collect::<Vec<_>>(),
        second.locs.iter().map(|w| w.worker_id).collect::<Vec<_>>()
    );
    assert_eq!(
        fs.directory_capacity_usage().unwrap()[1].reserved,
        2 * MIB as u64
    );
}

#[test]
fn capacity_failed_allocation_rolls_back_unpublished_charge() {
    let fs = filesystem("rollback");
    create(&fs, "/checkpoints/a", 2, MIB);
    // A commit for a nonexistent block fails after reservation, before publish.
    assert!(fs
        .add_block(
            "/checkpoints/a",
            None,
            ClientAddress::default(),
            vec![CommitBlock {
                block_id: -1,
                block_len: 1,
                locations: vec![]
            }],
            vec![],
            MIB,
            None
        )
        .is_err());
    assert_eq!(
        fs.directory_capacity_usage().unwrap()[1].available,
        2 * MIB as u64
    );
    allocate(&fs, "/checkpoints/a").unwrap();
}

#[test]
fn capacity_unconfigured_prefix_and_noncanonical_paths_are_rejected() {
    let fs = filesystem("paths");
    for p in ["/other/a", "/datasets2/a", "/datasets/../checkpoints/a"] {
        assert!(
            fs.create_with_opts(
                p,
                CreateFileOptsBuilder::new().create_parent(true).build(),
                OpenFlags::new_create()
            )
            .is_err(),
            "{p}"
        );
    }
}

#[test]
fn capacity_reports_account_for_extra_replicas_and_spdk_extents() {
    let fs = filesystem("observed-replicas");
    create(&fs, "/checkpoints/a", 1, MIB);
    let block = allocate(&fs, "/checkpoints/a").unwrap();
    finish(&fs, "/checkpoints/a", &block, MIB / 4);
    assert!(matches!(
        fs.set_attr(
            "/checkpoints/a",
            SetAttrOpts {
                replicas: Some(2),
                ..Default::default()
            }
        ),
        Err(FsError::Unsupported(_))
    ));
    assert_eq!(
        fs.directory_capacity_usage().unwrap()[1].committed,
        MIB as u64 / 4
    );
    // A recovered extra copy is real usage, even if it was not in this leader's allocation.
    let report = vec![(
        true,
        block.block.id,
        BlockLocation::new(3, StorageType::SpdkDisk),
    )];
    fs.fs_dir.write().block_report(report.clone()).unwrap();
    fs.fs_dir.write().block_report(report).unwrap();
    assert_eq!(
        fs.directory_capacity_usage().unwrap()[1].committed,
        2 * MIB as u64
    );
    fs.fs_dir.write().restore_from_rocksdb().unwrap();
    fs.reconcile_capacity_deletes().unwrap();
    assert_eq!(
        fs.directory_capacity_usage().unwrap()[1].committed,
        2 * MIB as u64
    );
    create(&fs, "/checkpoints/b", 1, MIB);
    assert!(matches!(
        allocate(&fs, "/checkpoints/b"),
        Err(FsError::QuotaExceeded(_))
    ));
}

#[test]
fn capacity_journal_updates_round_trip_and_replay_idempotently() {
    use crate::master::journal::{JournalBatch, JournalEntry};
    use crate::master::quota::capacity_manager::CapacityManager;
    use curvine_runtime::common::SerdeUtils;
    let fs = filesystem_with_journal("journal", true);
    create(&fs, "/checkpoints/a", 2, MIB);
    let block = allocate(&fs, "/checkpoints/a").unwrap();
    finish(&fs, "/checkpoints/a", &block, MIB / 4);
    let mut batch = JournalBatch::new(1);
    for entry in fs.fs_dir.read().take_entries() {
        batch.push(entry);
    }
    let batch = JournalBatch::deserialize_compat(&SerdeUtils::serialize(&batch).unwrap()).unwrap();
    let mut replay = CapacityManager::default();
    let mut count = 0;
    for entry in batch.batch {
        if let JournalEntry::Capacity(e) = entry {
            replay.apply(&e.updates);
            replay.apply(&e.updates);
            count += 1;
        }
    }
    assert!(count >= 2);
    assert_eq!(replay.used("/checkpoints"), MIB as u64 / 2);
}
