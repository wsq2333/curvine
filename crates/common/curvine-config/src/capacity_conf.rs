use curvine_core_error::{err_box, CommonResult};
use curvine_runtime::common::ByteUnit;
use serde::{Deserialize, Serialize};

/// An explicit, stable data budget. It does not track heartbeat free space.
/// Include all replicas and leave disk/repair headroom outside this budget.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CapacityConf {
    pub total: String,
    pub directories: Vec<DirectoryCapacityConf>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapacityOverflow {
    #[default]
    Reject,
    EvictCache,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectoryCapacityConf {
    pub path: String,
    pub percent: u8,
    #[serde(default)]
    pub overflow: CapacityOverflow,
    /// Target percentage of this directory's budget after pressure eviction.
    #[serde(default = "default_low_watermark")]
    pub low_watermark_percent: u8,
}

fn default_low_watermark() -> u8 {
    90
}

pub fn contains_path(root: &str, path: &str) -> bool {
    root == "/" || root == path || path.strip_prefix(root).is_some_and(|s| s.starts_with('/'))
}

impl CapacityConf {
    pub fn enabled(&self) -> bool {
        !self.directories.is_empty()
    }

    pub fn total_bytes(&self) -> CommonResult<u64> {
        if !self.enabled() {
            return Ok(0);
        }
        Ok(ByteUnit::from_str(&self.total)?.as_byte())
    }

    pub fn validate(&self) -> CommonResult<()> {
        if !self.enabled() {
            return Ok(());
        }
        let total = self.total_bytes()?;
        if total == 0 || total > i64::MAX as u64 {
            return err_box!("capacity.total must be between 1 and i64::MAX bytes");
        }
        let mut sum = 0_u16;
        for (i, d) in self.directories.iter().enumerate() {
            if !d.path.starts_with('/')
                || d.path == "/"
                || d.path.ends_with('/')
                || d.path
                    .split('/')
                    .skip(1)
                    .any(|s| s.is_empty() || s == "." || s == "..")
            {
                return err_box!(
                    "capacity directory must be a canonical non-root absolute path: {}",
                    d.path
                );
            }
            if d.percent == 0
                || d.percent > 100
                || d.low_watermark_percent == 0
                || d.low_watermark_percent >= 100
            {
                return err_box!("invalid capacity percentage for {}", d.path);
            }
            if (total as u128 * d.percent as u128 / 100) == 0 {
                return err_box!("capacity budget rounds to zero for {}", d.path);
            }
            sum += d.percent as u16;
            for other in &self.directories[..i] {
                if contains_path(&d.path, &other.path) || contains_path(&other.path, &d.path) {
                    return err_box!(
                        "overlapping capacity directories: {} and {}",
                        d.path,
                        other.path
                    );
                }
            }
        }
        if sum > 100 {
            return err_box!("directory capacity percentages exceed 100");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn conf(paths: &[(&str, u8)]) -> CapacityConf {
        CapacityConf {
            total: "100GB".into(),
            directories: paths
                .iter()
                .map(|(p, n)| DirectoryCapacityConf {
                    path: p.to_string(),
                    percent: *n,
                    overflow: CapacityOverflow::Reject,
                    low_watermark_percent: 90,
                })
                .collect(),
        }
    }
    #[test]
    fn validates_disjoint_budgets() {
        assert!(conf(&[("/datasets", 80), ("/checkpoints", 20)])
            .validate()
            .is_ok());
        for paths in [
            vec![("/a", 80), ("/b", 21)],
            vec![("/a", 50), ("/a/b", 50)],
            vec![("/a", 50), ("/a", 50)],
            vec![("/a/../b", 100)],
            vec![("/", 100)],
        ] {
            assert!(conf(&paths).validate().is_err(), "{paths:?}");
        }
        assert!(conf(&[("/a", 50), ("/ab", 50)]).validate().is_ok());
    }
    #[test]
    fn parses_example_and_rejects_unknown_options() {
        let c: CapacityConf = toml::from_str("total = '100TB'\n[[directories]]\npath = '/datasets'\npercent = 80\noverflow = 'evict_cache'\n[[directories]]\npath = '/checkpoints'\npercent = 20").unwrap();
        c.validate().unwrap();
        assert_eq!(c.directories[0].overflow, CapacityOverflow::EvictCache);
        assert!(toml::from_str::<CapacityConf>("totla = '1GB'").is_err());
    }
}
