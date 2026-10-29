//! Each device's own sequential write rate, from fio: the ceiling T1 is read against
//!
//! T1 reads the put arm against the rate the pool's device takes whole writes at, over the
//! copies it holds ([X3's record](../../docs/src/object-storage/bytes-through-groups.md)). That
//! rate is fio's, measured at the start of every round on the filesystem each storage root is on,
//! at the leg's four row sizes: sequential writes, direct, through io_uring, eight in flight, as
//! X6 took its ceilings ([X6](../../docs/src/object-storage/device-store-ssd.md)). The file is a
//! sibling of the root, so nothing a node will claim is touched, and it is removed after.

use std::collections::BTreeSet;
use std::path::Path;

use color_eyre::eyre::{eyre, WrapErr};
use shoaladm::bench::devices;
use shoaladm::deploy::inventory::Inventory;
use shoaladm::deploy::remote::{quote, Host};

use crate::measure::SIZES;
use crate::record::{append, Record};

/// The fio run that measures one directory at one size, printing fio's json
///
/// # Arguments
///
/// * `dir` - The directory the file goes in
/// * `size` - The size of each write
/// * `quick` - Whether this is a quick run
#[must_use]
pub fn script(dir: &str, size: usize, quick: bool) -> String {
    let (file, runtime) = if quick { ("1G", 3) } else { ("8G", 30) };
    let dir = quote(dir);
    format!(
        "set -e; sudo -n mkdir -p {dir}; \
         sudo -n fio --name=x3 --filename={dir}/x3-fio --rw=write --bs={size} --iodepth=8 \
         --ioengine=io_uring --direct=1 --size={file} --runtime={runtime} --time_based \
         --output-format=json; sudo -n rm -rf {dir}"
    )
}

/// The bytes a second fio's json says a job wrote
///
/// # Arguments
///
/// * `output` - What fio printed
///
/// # Errors
///
/// When it is not fio's json or holds no job.
pub fn write_rate(output: &str) -> color_eyre::Result<f64> {
    // fio can print a warning line before its json, so the json starts at the first brace
    let json = &output[output.find('{').ok_or_else(|| eyre!("fio printed no json"))?..];
    let value: shoal::serde_json::Value = shoal::serde_json::from_str(json).wrap_err("fio's json")?;
    value["jobs"][0]["write"]["bw_bytes"]
        .as_f64()
        .ok_or_else(|| eyre!("fio's json holds no job's write rate"))
}

/// Measure every distinct filesystem an inventory's roots are on, every host at once
///
/// # Arguments
///
/// * `inventory_path` - The inventory, which no cluster need be deployed from
/// * `round` - The round
/// * `quick` - Whether this is a quick run
/// * `out` - The file records are added to
///
/// # Errors
///
/// When a host cannot be reached or fio fails there.
pub async fn run(inventory_path: &Path, round: u32, quick: bool, out: &Path) -> color_eyre::Result<()> {
    let inventory = Inventory::read(inventory_path)?;
    let hosts = devices::host_roots(&inventory);
    // each host's directories in turn, the hosts at once
    let mut tasks = Vec::new();
    for host in hosts {
        tasks.push(tokio::task::spawn_blocking(move || -> color_eyre::Result<Vec<Record>> {
            let mut records = Vec::new();
            let mut done = BTreeSet::new();
            for (label, root) in &host.roots {
                // a sibling of the root, once a root
                if !done.insert(root.clone()) {
                    continue;
                }
                let role = label.split(' ').nth(1).unwrap_or("latency").to_string();
                let dir = format!("{}-fio", root.trim_end_matches('/'));
                for size in SIZES {
                    let output = Host {
                        target: host.target.clone(),
                    }
                    .run(&script(&dir, size, quick))
                    .wrap_err_with(|| format!("fio on {} in {dir}", host.target))?;
                    let rate = write_rate(&output)?;
                    let mut record = Record::new("fio", &format!("{} {root}", host.target), &format!("size={size}"), round, quick);
                    record
                        .set("size", size as f64)
                        .set("write_mib_s", rate / f64::from(1 << 20))
                        .label("host", host.target.clone())
                        .label("role", role.clone());
                    println!("fio {} {root} {size}: {:.0} MiB/s", host.target, rate / f64::from(1 << 20));
                    records.push(record);
                }
            }
            Ok(records)
        }));
    }
    let mut all = Vec::new();
    for task in tasks {
        all.extend(task.await.map_err(|error| eyre!("a host's fio: {error}"))??);
    }
    append(out, &all)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// fio's write rate is read from its json, past a warning line before it
    #[test]
    fn fios_write_rate_is_read() {
        let output = "fio: some warning\n{\"jobs\": [{\"write\": {\"bw_bytes\": 1048576000}}]}";
        assert_eq!(write_rate(output).expect("a rate"), 1_048_576_000.0);
        assert!(write_rate("nothing").is_err());
    }

    /// The fio run writes a sibling of the root at the size asked, and removes it after
    #[test]
    fn the_script_writes_beside_the_root() {
        let script = script("/xfs/shoal-x3-fio", 1 << 20, false);
        assert!(script.contains("--bs=1048576"));
        assert!(script.contains("--filename=/xfs/shoal-x3-fio/x3-fio"));
        assert!(script.ends_with("sudo -n rm -rf /xfs/shoal-x3-fio"));
    }
}
