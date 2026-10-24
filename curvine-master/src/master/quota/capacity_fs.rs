use crate::master::fs::DeleteResult;
use crate::master::meta::inode::{InodeFile, InodeView};
use crate::master::meta::FsDir;
use crate::master::quota::capacity_manager::{CapacityCharge, CapacityManager, CapacityUpdate};
use curvine_config::contains_path;
use curvine_error::{FsError, FsResult};
use curvine_model::{CommitBlock, StorageType, WorkerAddress};
use curvine_runtime::common::SerdeUtils;
use std::collections::BTreeSet;

impl FsDir {
    pub(crate) fn capacity_check_replicas(
        &self,
        blocks: &[i64],
        replicas: Option<i32>,
    ) -> FsResult<()> {
        if let Some(replicas) = replicas {
            if blocks
                .iter()
                .filter_map(|id| self.capacity.charges.get(id))
                .any(|c| !c.deleting && i32::from(c.replicas) != replicas)
            {
                return Err(FsError::unsupported("changing replicas on allocated capacity-managed files is not supported; set replicas before allocation"));
            }
        }
        Ok(())
    }

    /// Reports can reveal an old/extra replica after recovery. Account for real
    /// copies even above the limit; admission, not observation, rejects growth.
    pub(crate) fn capacity_observe(
        &self,
        blocks: &[(bool, i64, curvine_model::BlockLocation)],
    ) -> FsResult<Vec<CapacityUpdate>> {
        let mut changed = std::collections::BTreeMap::new();
        for (add, id, location) in blocks {
            if !add {
                continue;
            }
            let Some(old) = self.capacity.charges.get(id) else {
                continue;
            };
            let next = changed.entry(*id).or_insert_with(|| old.clone());
            next.workers.insert(location.worker_id);
            if location.storage_type == StorageType::SpdkDisk {
                if let Some(InodeView::File(file)) = self.store.get_inode(next.inode_id, None)? {
                    next.allocation_floor = file.block_size as u64;
                    next.bytes = next.bytes.max(next.allocation_floor);
                }
            }
        }
        Ok(changed
            .into_values()
            .filter(|c| self.capacity.charges.get(&c.block_id) != Some(c))
            .map(CapacityUpdate::put)
            .collect())
    }

    pub(crate) fn reload_capacity(&mut self) -> FsResult<()> {
        self.capacity_conf.validate()?;
        self.capacity = CapacityManager::default();
        let updates: Vec<_> = self
            .store
            .store
            .capacity_charges()?
            .into_iter()
            .map(CapacityUpdate::put)
            .collect();
        if !self.capacity_conf.enabled() && !updates.is_empty() {
            return Err(FsError::invalid_argument(
                "cannot disable directory capacity while durable charges remain",
            ));
        }
        self.capacity.apply(&updates);
        self.capacity_scanned = false;
        Ok(())
    }

    pub(crate) fn apply_capacity(&mut self, updates: &[CapacityUpdate]) -> FsResult<()> {
        self.store.store.apply_capacity(updates)?;
        self.capacity.apply(updates);
        Ok(())
    }

    pub(crate) fn record_capacity(&mut self, updates: Vec<CapacityUpdate>) -> FsResult<()> {
        if updates.is_empty() {
            return Ok(());
        }
        self.apply_capacity(&updates)?;
        self.journal_writer.log_capacity(self, updates)
    }

    /// Backfill pre-feature data once after startup/snapshot. Existing data can
    /// exceed a newly configured limit; further growth is then refused.
    pub(crate) fn prepare_capacity(&mut self) -> FsResult<()> {
        if !self.capacity_conf.enabled() || self.capacity_scanned {
            return Ok(());
        }
        let mut updates = Vec::new();
        for item in self.store.store.inodes_iter()? {
            let inode: InodeView = SerdeUtils::deserialize(&item.map_err(FsError::from_error)?.1)?;
            let InodeView::File(file) = inode else {
                continue;
            };
            if file.blocks.is_empty() {
                continue;
            }
            let path = self.get_inode_path(file.id)?;
            let directory = CapacityManager::directory(&self.capacity_conf, &path)?
                .unwrap()
                .to_string();
            let locations = file.get_locs(&self.store)?;
            for b in &file.blocks {
                if let Some(old) = self.capacity.charges.get(&b.id) {
                    if old.directory != directory {
                        return Err(FsError::invalid_argument(
                            "cannot move capacity roots containing existing allocations",
                        ));
                    }
                    continue;
                }
                if file.storage_policy.ufs_only() && !locations.contains_key(&b.id) {
                    continue;
                }
                let reserved = file.is_writing() || b.len == 0;
                let fixed_extent = file.storage_policy.storage_type == StorageType::SpdkDisk
                    || locations.get(&b.id).is_some_and(|locs| {
                        locs.iter().any(|l| l.storage_type == StorageType::SpdkDisk)
                    });
                updates.push(CapacityUpdate::put(CapacityCharge {
                    block_id: b.id,
                    inode_id: file.id,
                    directory: directory.clone(),
                    bytes: if reserved || fixed_extent {
                        file.block_size as u64
                    } else {
                        b.len as u64
                    },
                    allocation_floor: if fixed_extent {
                        file.block_size as u64
                    } else {
                        0
                    },
                    replicas: file.replicas as u16,
                    workers: locations
                        .get(&b.id)
                        .into_iter()
                        .flatten()
                        .map(|l| l.worker_id)
                        .collect(),
                    deleting: false,
                    reserved,
                }));
            }
        }
        // If a crash interrupted metadata deletion before its accounting update,
        // retain the charge and resume deletion; never infer that bytes are free.
        let orphaned: Vec<_> = self
            .capacity
            .charges
            .values()
            .filter_map(|c| {
                if c.deleting {
                    return None;
                }
                match self.store.get_inode(c.inode_id, None) {
                    Ok(Some(InodeView::File(f))) if f.blocks.iter().any(|b| b.id == c.block_id) => {
                        None
                    }
                    Ok(_) => Some(Ok(c.block_id)),
                    Err(e) => Some(Err(e)),
                }
            })
            .collect::<Result<_, _>>()?;
        updates.extend(self.capacity.mark_deleting(orphaned));
        self.record_capacity(updates)?;
        self.capacity_scanned = true;
        Ok(())
    }

    pub(crate) fn capacity_new_block(
        &self,
        file: &InodeFile,
        id: i64,
        workers: &[WorkerAddress],
    ) -> FsResult<Vec<CapacityUpdate>> {
        if !self.capacity_conf.enabled() {
            return Ok(Vec::new());
        }
        let path = self.get_inode_path(file.id)?;
        let directory = CapacityManager::directory(&self.capacity_conf, &path)?
            .unwrap()
            .to_string();
        if let Some(c) = self.capacity.charges.get(&id) {
            if c.deleting {
                return Err(FsError::quota_exceeded(
                    "block is still awaiting deletion acknowledgements",
                ));
            }
            let mut c = c.clone();
            c.workers.extend(workers.iter().map(|w| w.worker_id));
            return Ok(vec![CapacityUpdate::put(c)]);
        }
        Ok(vec![CapacityUpdate::put(CapacityCharge {
            block_id: id,
            inode_id: file.id,
            directory,
            bytes: file.block_size as u64,
            allocation_floor: if file.storage_policy.storage_type == StorageType::SpdkDisk {
                file.block_size as u64
            } else {
                0
            },
            replicas: file.replicas as u16,
            workers: workers.iter().map(|w| w.worker_id).collect(),
            deleting: false,
            reserved: true,
        })])
    }

    pub(crate) fn capacity_reopen(&self, file: &InodeFile) -> Vec<CapacityUpdate> {
        // FileLayout keeps the old committed copy alongside a staging copy.
        file.blocks
            .iter()
            .filter_map(|b| self.capacity.charges.get(&b.id))
            .filter(|c| !c.reserved && !c.deleting)
            .map(|c| {
                let mut next = c.clone();
                next.bytes = if file.storage_policy.storage_type == StorageType::SpdkDisk {
                    next.bytes.max(file.block_size as u64)
                } else {
                    next.bytes.saturating_add(file.block_size as u64)
                };
                next.reserved = true;
                CapacityUpdate::put(next)
            })
            .collect()
    }

    pub(crate) fn capacity_complete(
        &mut self,
        file: &InodeFile,
        commits: &[CommitBlock],
    ) -> FsResult<()> {
        if !self.capacity_conf.enabled() {
            return Ok(());
        }
        let mut updates = Vec::new();
        for b in &file.blocks {
            let Some(old) = self.capacity.charges.get(&b.id) else {
                continue;
            };
            if old.deleting {
                continue;
            }
            let mut next = old.clone();
            if let Some(commit) = commits.iter().find(|c| c.block_id == b.id) {
                next.workers
                    .extend(commit.locations.iter().map(|l| l.worker_id));
            }
            // While a writer is open it can reopen/extend an existing block
            // without AddBlock; keep the reservation until the final close.
            if file.is_complete() {
                next.bytes = if file.storage_policy.storage_type == StorageType::SpdkDisk {
                    file.block_size as u64
                } else {
                    (b.len as u64).max(next.allocation_floor)
                };
                next.reserved = false;
            }
            if next != *old {
                updates.push(CapacityUpdate::put(next));
            }
        }
        self.record_capacity(updates)?;
        self.capacity.touch(file.id);
        Ok(())
    }

    pub(crate) fn capacity_deleting(&mut self, result: &DeleteResult) -> FsResult<()> {
        let mut updates = Vec::new();
        for (id, locations) in &result.blocks {
            let Some(mut c) = self.capacity.charges.get(id).cloned() else {
                continue;
            };
            c.workers.extend(locations.iter().map(|l| l.worker_id));
            c.deleting = true;
            c.reserved = false;
            updates.push(if c.workers.is_empty() {
                CapacityUpdate::remove(*id)
            } else {
                CapacityUpdate::put(c)
            });
        }
        self.record_capacity(updates)
    }

    pub(crate) fn capacity_check_move(&self, src: &str, dst: &str) -> FsResult<()> {
        if !self.capacity_conf.enabled() {
            return Ok(());
        }
        for d in &self.capacity_conf.directories {
            if contains_path(src, &d.path) || contains_path(dst, &d.path) {
                return Err(FsError::invalid_argument(
                    "cannot rename/link a capacity root or its ancestors",
                ));
            }
        }
        if CapacityManager::directory(&self.capacity_conf, src)?
            != CapacityManager::directory(&self.capacity_conf, dst)?
        {
            return Err(FsError::invalid_argument(
                "cross-capacity-directory rename and hard link are not supported",
            ));
        }
        Ok(())
    }

    pub(crate) fn capacity_subtree_blocks(
        &self,
        inp: &crate::master::meta::inode::InodePath,
    ) -> Vec<i64> {
        use crate::master::meta::inode::InodePtr;
        if !self.capacity_conf.enabled() {
            return Vec::new();
        }
        let mut stack: Vec<_> = inp.get_last_inode().into_iter().collect();
        let mut ids = BTreeSet::new();
        while let Some(inode) = stack.pop() {
            match inode.as_ref() {
                InodeView::Dir(d) => stack.extend(d.children_iter().map(InodePtr::from_ref)),
                other => ids.extend(self.capacity.inode_blocks(other.id())),
            }
        }
        ids.into_iter().collect()
    }

    pub(crate) fn capacity_retire_missing(&mut self, ids: &[i64]) -> FsResult<()> {
        let mut missing = Vec::new();
        for id in ids {
            let Some(c) = self.capacity.charges.get(id) else {
                continue;
            };
            if c.deleting {
                continue;
            }
            let exists = matches!(self.store.get_inode(c.inode_id,None)?,Some(InodeView::File(f)) if f.blocks.iter().any(|b|b.id==*id));
            if !exists {
                missing.push(*id);
            }
        }
        self.record_capacity(self.capacity.mark_deleting(missing))
    }

    pub(crate) fn capacity_pending_deletes(&self) -> Vec<(i64, BTreeSet<u32>)> {
        self.capacity
            .charges
            .values()
            .filter(|c| c.deleting)
            .map(|c| (c.block_id, c.workers.clone()))
            .collect()
    }
}
