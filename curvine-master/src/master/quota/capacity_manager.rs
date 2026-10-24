//! Directory admission accounting. All mutations are serialized by FsDir's write
//! lock and journaled before a client is allowed to use newly admitted space.
//! A deletion remains charged until every recorded replica acknowledges removal.
use curvine_config::{contains_path, CapacityConf, CapacityOverflow};
use curvine_error::{FsError, FsResult};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapacityCharge {
    pub block_id: i64,
    pub inode_id: i64,
    pub directory: String,
    /// Per-replica charged bytes: reservation while open; accounted data on close.
    pub bytes: u64,
    /// Fixed-extent backends retain their allocation after a short final block.
    pub allocation_floor: u64,
    pub replicas: u16,
    pub workers: BTreeSet<u32>,
    pub deleting: bool,
    pub reserved: bool,
}

impl CapacityCharge {
    pub fn total(&self) -> u64 {
        let copies = if self.deleting {
            self.workers.len()
        } else {
            self.workers.len().max(self.replicas as usize)
        };
        self.bytes.saturating_mul(copies as u64)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapacityUpdate {
    pub block_id: i64,
    pub charge: Option<CapacityCharge>,
}

impl CapacityUpdate {
    pub fn put(charge: CapacityCharge) -> Self {
        Self {
            block_id: charge.block_id,
            charge: Some(charge),
        }
    }
    pub fn remove(block_id: i64) -> Self {
        Self {
            block_id,
            charge: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DirectoryCapacityUsage {
    pub path: String,
    pub limit: u64,
    pub committed: u64,
    pub reserved: u64,
    pub pending_delete: u64,
    pub available: u64,
}

#[derive(Default)]
pub struct CapacityManager {
    pub(crate) charges: BTreeMap<i64, CapacityCharge>,
    totals: BTreeMap<String, u64>,
    by_inode: BTreeMap<i64, BTreeSet<i64>>,
    accesses: std::sync::Mutex<BTreeMap<i64, u64>>,
    clock: std::sync::atomic::AtomicU64,
}

impl CapacityManager {
    pub fn directory<'a>(conf: &'a CapacityConf, path: &str) -> FsResult<Option<&'a str>> {
        if !conf.enabled() {
            return Ok(None);
        }
        if !path.starts_with('/')
            || path
                .split('/')
                .skip(1)
                .any(|s| s.is_empty() || s == "." || s == "..")
        {
            return Err(FsError::invalid_argument(
                "capacity paths must be canonical absolute paths",
            ));
        }
        conf.directories
            .iter()
            .find(|d| contains_path(&d.path, path))
            .map(|d| Some(d.path.as_str()))
            .ok_or_else(|| {
                FsError::quota_exceeded(format!(
                    "path {path} is outside configured capacity directories"
                ))
            })
    }

    pub fn limit(conf: &CapacityConf, directory: &str) -> FsResult<u64> {
        let d = conf
            .directories
            .iter()
            .find(|d| d.path == directory)
            .ok_or_else(|| {
                FsError::quota_exceeded(format!(
                    "capacity directory {directory} was removed while it still owns data"
                ))
            })?;
        Ok((conf.total_bytes()? as u128 * d.percent as u128 / 100) as u64)
    }

    pub fn used(&self, directory: &str) -> u64 {
        *self.totals.get(directory).unwrap_or(&0)
    }

    /// Check an entire transaction, including reservations for all replicas.
    /// No state is changed by a failed check; callers hold the namespace lock.
    pub fn check(&self, conf: &CapacityConf, updates: &[CapacityUpdate]) -> FsResult<()> {
        let mut totals = self.totals.clone();
        let mut seen = BTreeSet::new();
        for update in updates {
            if !seen.insert(update.block_id) {
                return Err(FsError::invalid_argument("duplicate capacity update"));
            }
            if let Some(old) = self.charges.get(&update.block_id) {
                let total = totals.entry(old.directory.clone()).or_default();
                *total = total.saturating_sub(old.total());
            }
            if let Some(new) = &update.charge {
                let total = totals.entry(new.directory.clone()).or_default();
                *total = total
                    .checked_add(new.total())
                    .ok_or_else(|| FsError::quota_exceeded("capacity accounting overflow"))?;
            }
        }
        for (dir, total) in totals {
            let limit = Self::limit(conf, &dir)?;
            // Reductions and no-op retries must remain possible after lowering a limit.
            if total > limit && total > self.used(&dir) {
                return Err(FsError::quota_exceeded(format!(
                    "directory {dir}: requested usage {total}, limit {limit}, current {}",
                    self.used(&dir)
                )));
            }
        }
        Ok(())
    }

    pub fn apply(&mut self, updates: &[CapacityUpdate]) {
        for update in updates {
            let old_inode = self.charges.get(&update.block_id).map(|c| c.inode_id);
            if let Some(old) = self.charges.remove(&update.block_id) {
                if let Some(ids) = self.by_inode.get_mut(&old.inode_id) {
                    ids.remove(&old.block_id);
                    if ids.is_empty() {
                        self.by_inode.remove(&old.inode_id);
                    }
                }
                let bytes = old.total();
                let total = self.totals.entry(old.directory).or_default();
                *total = total.saturating_sub(bytes);
            }
            if let Some(new) = &update.charge {
                *self.totals.entry(new.directory.clone()).or_default() += new.total();
                self.by_inode
                    .entry(new.inode_id)
                    .or_default()
                    .insert(new.block_id);
                self.charges.insert(update.block_id, new.clone());
            }
            if let Some(id) = old_inode.filter(|id| !self.by_inode.contains_key(id)) {
                if let Ok(mut accesses) = self.accesses.lock() {
                    accesses.remove(&id);
                }
            }
        }
    }

    /// Count already queued deletions toward the target, and account for the
    /// incoming reservation even when current usage is below the low watermark.
    pub fn eviction_bytes(
        &self,
        conf: &CapacityConf,
        directory: &str,
        updates: &[CapacityUpdate],
    ) -> FsResult<u64> {
        let Some(target) = self.eviction_target(conf, directory)? else {
            return Ok(0);
        };
        let mut growth = 0_i128;
        for u in updates {
            if let Some(c) = self
                .charges
                .get(&u.block_id)
                .filter(|c| c.directory == directory)
            {
                growth -= c.total() as i128;
            }
            if let Some(c) = u.charge.as_ref().filter(|c| c.directory == directory) {
                growth += c.total() as i128;
            }
        }
        let limit = Self::limit(conf, directory)?;
        if growth <= 0
            || growth > limit as i128
            || self.used(directory) as i128 + growth <= limit as i128
        {
            return Ok(0);
        }
        let pending: u64 = self
            .charges
            .values()
            .filter(|c| c.directory == directory && c.deleting)
            .map(CapacityCharge::total)
            .sum();
        let target = target.min(limit - growth as u64);
        Ok(self
            .used(directory)
            .saturating_sub(pending)
            .saturating_sub(target))
    }

    pub fn inode_blocks(&self, inode: i64) -> Vec<i64> {
        self.by_inode
            .get(&inode)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn touch(&self, inode: i64) {
        if !self.by_inode.contains_key(&inode) {
            return;
        }
        let tick = self
            .clock
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Ok(mut accesses) = self.accesses.lock() {
            accesses.insert(inode, tick);
        }
    }

    pub fn victims(&self, directory: &str) -> Vec<i64> {
        let inodes: BTreeSet<_> = self
            .charges
            .values()
            .filter(|c| c.directory == directory && !c.deleting && !c.reserved)
            .map(|c| c.inode_id)
            .collect();
        let mut victims: Vec<_> = inodes.into_iter().collect();
        if let Ok(accesses) = self.accesses.lock() {
            victims.sort_by_key(|id| (accesses.get(id).copied().unwrap_or(0), *id));
        }
        victims
    }

    pub fn mark_deleting(&self, ids: impl IntoIterator<Item = i64>) -> Vec<CapacityUpdate> {
        ids.into_iter()
            .filter_map(|id| self.charges.get(&id))
            .map(|c| {
                let mut c = c.clone();
                c.deleting = true;
                c.reserved = false;
                if c.workers.is_empty() {
                    CapacityUpdate::remove(c.block_id)
                } else {
                    CapacityUpdate::put(c)
                }
            })
            .collect()
    }

    pub fn deleted(&self, block: i64, worker: u32) -> Option<CapacityUpdate> {
        let mut c = self.charges.get(&block)?.clone();
        if !c.workers.remove(&worker) {
            return None;
        }
        Some(if c.deleting && c.workers.is_empty() {
            CapacityUpdate::remove(block)
        } else {
            CapacityUpdate::put(c)
        })
    }

    pub fn snapshots(&self, conf: &CapacityConf) -> FsResult<Vec<DirectoryCapacityUsage>> {
        conf.directories
            .iter()
            .map(|d| {
                let mut s = DirectoryCapacityUsage {
                    path: d.path.clone(),
                    limit: Self::limit(conf, &d.path)?,
                    committed: 0,
                    reserved: 0,
                    pending_delete: 0,
                    available: 0,
                };
                for c in self.charges.values().filter(|c| c.directory == d.path) {
                    if c.deleting {
                        s.pending_delete += c.total();
                    } else if c.reserved {
                        s.reserved += c.total();
                    } else {
                        s.committed += c.total();
                    }
                }
                s.available = s.limit.saturating_sub(self.used(&d.path));
                Ok(s)
            })
            .collect()
    }

    pub fn eviction_target(&self, conf: &CapacityConf, directory: &str) -> FsResult<Option<u64>> {
        let Some(d) = conf
            .directories
            .iter()
            .find(|d| d.path == directory && d.overflow == CapacityOverflow::EvictCache)
        else {
            return Ok(None);
        };
        Ok(Some(
            (Self::limit(conf, directory)? as u128 * d.low_watermark_percent as u128 / 100) as u64,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_config::DirectoryCapacityConf;
    fn conf() -> CapacityConf {
        CapacityConf {
            total: "1000B".into(),
            directories: vec![
                DirectoryCapacityConf {
                    path: "/data".into(),
                    percent: 80,
                    overflow: CapacityOverflow::EvictCache,
                    low_watermark_percent: 90,
                },
                DirectoryCapacityConf {
                    path: "/ckpt".into(),
                    percent: 20,
                    overflow: CapacityOverflow::Reject,
                    low_watermark_percent: 90,
                },
            ],
        }
    }
    fn charge(id: i64, dir: &str, bytes: u64, replicas: u16) -> CapacityCharge {
        CapacityCharge {
            block_id: id,
            inode_id: id,
            directory: dir.into(),
            bytes,
            allocation_floor: 0,
            replicas,
            workers: (1..=replicas as u32).collect(),
            deleting: false,
            reserved: true,
        }
    }
    #[test]
    fn eviction_accounts_for_growth_pending_deletes_and_oversized_requests() {
        let mut m = CapacityManager::default();
        let c = conf();
        let mut existing = charge(1, "/data", 600, 1);
        existing.reserved = false;
        m.apply(&[CapacityUpdate::put(existing.clone())]);
        let incoming = CapacityUpdate::put(charge(2, "/data", 300, 1));
        assert_eq!(
            m.eviction_bytes(&c, "/data", std::slice::from_ref(&incoming))
                .unwrap(),
            100
        );
        existing.deleting = true;
        m.apply(&[CapacityUpdate::put(existing)]);
        assert_eq!(m.eviction_bytes(&c, "/data", &[incoming]).unwrap(), 0);
        assert_eq!(
            m.eviction_bytes(
                &c,
                "/data",
                &[CapacityUpdate::put(charge(3, "/data", 801, 1))]
            )
            .unwrap(),
            0
        );
    }

    #[test]
    fn separate_budgets_include_replicas_and_reservations() {
        let mut m = CapacityManager::default();
        let c = conf();
        let a = CapacityUpdate::put(charge(1, "/data", 800, 1));
        m.check(&c, std::slice::from_ref(&a)).unwrap();
        m.apply(&[a]);
        let b = CapacityUpdate::put(charge(2, "/ckpt", 100, 2));
        m.check(&c, std::slice::from_ref(&b)).unwrap();
        m.apply(&[b]);
        assert!(m
            .check(&c, &[CapacityUpdate::put(charge(3, "/ckpt", 1, 2))])
            .is_err());
        assert_eq!(m.used("/ckpt"), 200);
        assert!(m
            .check(&c, &[CapacityUpdate::put(charge(4, "/data", 1, 1))])
            .is_err());
    }
    #[test]
    fn idempotent_settlement_and_delete_ack() {
        let mut m = CapacityManager::default();
        let mut c = charge(1, "/ckpt", 100, 2);
        m.apply(&[CapacityUpdate::put(c.clone())]);
        c.bytes = 30;
        c.reserved = false;
        m.apply(&[CapacityUpdate::put(c.clone()), CapacityUpdate::put(c)]);
        assert_eq!(m.used("/ckpt"), 60);
        m.apply(&m.mark_deleting([1]));
        assert_eq!(m.used("/ckpt"), 60);
        let u = m.deleted(1, 1).unwrap();
        m.apply(&[u]);
        assert_eq!(m.used("/ckpt"), 30);
        assert!(m.deleted(1, 1).is_none());
        let u = m.deleted(1, 2).unwrap();
        m.apply(&[u]);
        assert_eq!(m.used("/ckpt"), 0);
    }
    #[test]
    fn recover_replay_and_lower_limit() {
        let mut m = CapacityManager::default();
        let u = CapacityUpdate::put(charge(1, "/ckpt", 100, 2));
        let bytes = serde_json::to_vec(&vec![u]).unwrap();
        let updates: Vec<CapacityUpdate> = serde_json::from_slice(&bytes).unwrap();
        m.apply(&updates);
        m.apply(&updates);
        assert_eq!(m.used("/ckpt"), 200);
        let mut c = conf();
        c.total = "500B".into();
        m.check(&c, &updates).unwrap();
        assert!(m
            .check(&c, &[CapacityUpdate::put(charge(2, "/ckpt", 1, 1))])
            .is_err());
        m.check(&c, &[CapacityUpdate::remove(1)]).unwrap();
    }
    #[test]
    fn eviction_is_directory_scoped_and_skips_reservations() {
        let mut m = CapacityManager::default();
        for (id, dir) in [(1, "/data"), (2, "/ckpt"), (3, "/data")] {
            let mut c = charge(id, dir, 10, 1);
            c.reserved = id == 3;
            m.apply(&[CapacityUpdate::put(c)]);
        }
        assert_eq!(m.victims("/data"), vec![1]);
        assert!(CapacityManager::directory(&conf(), "/database/f").is_err());
        assert_eq!(
            CapacityManager::directory(&conf(), "/data/f").unwrap(),
            Some("/data")
        );
    }
}
