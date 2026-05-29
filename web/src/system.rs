//! Process and disk metrics collection via `sysinfo`.
//!
//! Kept separate from `/metrics` handler so the JSON shape is easy to evolve
//! and so the sysinfo dependency surface stays in one place.

use std::path::Path;

use log::debug;
use serde_json::{Value, json};
use sysinfo::{Disks, Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::sync::RwLock;
pub(crate) async fn collect_process(system: &RwLock<System>) -> Value {
    let pid = Pid::from_u32(std::process::id());
    let mut sys = system.write().await;
    sys.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        false,
        ProcessRefreshKind::nothing().with_cpu().with_memory(),
    );
    sys.refresh_memory();
    let (cpu, mem_used) = sys
        .process(pid)
        .map_or((0.0_f32, 0_u64), |p| ((p.cpu_usage() * 10.0).round() / 10.0, p.memory()));
    let mem_free = if sys.available_memory() > 0 {
        sys.available_memory()
    } else {
        sys.free_memory()
    };
    json!({
        "cpu_used_percent": cpu,
        "memory_used_bytes": mem_used,
        "memory_used_pretty": bytesize::ByteSize(mem_used).to_string(),
        "memory_free_bytes": mem_free,
        "memory_free_pretty": bytesize::ByteSize(mem_free).to_string(),
    })
}

pub(crate) fn collect_disk(db_path: &Path) -> Value {
    let db_size_bytes = std::fs::metadata(db_path).map_or(0, |m| m.len());
    let disks = Disks::new_with_refreshed_list();
    let canonical = std::fs::canonicalize(db_path).unwrap_or_else(|err| {
        debug!(
            "web: collect_disk: canonicalize({}) failed: {err}; falling back to raw path",
            db_path.display()
        );
        db_path.to_path_buf()
    });
    let best = disks
        .list()
        .iter()
        .filter(|d| canonical.starts_with(d.mount_point()))
        .max_by_key(|d| d.mount_point().as_os_str().len());
    let (free_bytes, total_bytes, mount) = best.map_or((0, 0, String::new()), |d| {
        (d.available_space(), d.total_space(), d.mount_point().display().to_string())
    });
    json!({
        "db_path": db_path.display().to_string(),
        "db_size_bytes": db_size_bytes,
        "db_size_pretty": bytesize::ByteSize(db_size_bytes).to_string(),
        "mount_point": mount,
        "free_bytes": free_bytes,
        "free_pretty": bytesize::ByteSize(free_bytes).to_string(),
        "total_bytes": total_bytes,
        "total_pretty": bytesize::ByteSize(total_bytes).to_string(),
    })
}
