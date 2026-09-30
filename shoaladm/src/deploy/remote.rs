//! Running commands on a deployment's hosts over ssh
//!
//! Every command line is built as a `Vec<String>` before it is run, the way shoal-bench's remote
//! launcher builds its own, so what a deployment would run is a value a test can read. ssh is
//! run with `BatchMode`, so a host that would prompt for a password or a host key is refused
//! rather than hanging the deployment.

use color_eyre::eyre::{bail, eyre, WrapErr};
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

/// How long ssh waits for a host to answer before giving up
const CONNECT_TIMEOUT_SECS: u32 = 10;

/// The exit status a shell reports for a process killed by SIGILL
///
/// What a server program built for another CPU dies with on its first instruction the host does
/// not have: the shell's `128 + 4`.
pub const SIGILL_STATUS: i32 = 132;

/// A host the deployment reaches over ssh
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Host {
    /// What ssh and scp are given: `user@host` or `host`
    pub target: String,
}

/// What a command on a host printed and exited with
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Output {
    /// Its exit status, or none if it was killed by a signal locally
    pub status: Option<i32>,
    /// What it printed to stdout
    pub stdout: String,
    /// What it printed to stderr
    pub stderr: String,
}

/// Quote a string for a POSIX shell
///
/// # Arguments
///
/// * `raw` - The string to quote
#[must_use]
pub fn quote(raw: &str) -> String {
    // a string of only safe characters needs nothing
    if !raw.is_empty()
        && raw
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_./:=@+,".contains(c))
    {
        return raw.to_string();
    }
    // otherwise single quotes, with each single quote closed, escaped and reopened
    format!("'{}'", raw.replace('\'', r"'\''"))
}

impl Host {
    /// The ssh options every command is run with
    fn options() -> Vec<String> {
        vec![
            "-o".to_string(),
            "BatchMode=yes".to_string(),
            "-o".to_string(),
            format!("ConnectTimeout={CONNECT_TIMEOUT_SECS}"),
        ]
    }

    /// The ssh that runs a script on this host
    ///
    /// # Arguments
    ///
    /// * `script` - The shell script to run
    #[must_use]
    pub fn ssh_command(&self, script: &str) -> Vec<String> {
        let mut command = vec!["ssh".to_string()];
        command.extend(Self::options());
        command.push("-T".to_string());
        command.push(self.target.clone());
        command.push(script.to_string());
        command
    }

    /// The scp that copies a local file to this host
    ///
    /// # Arguments
    ///
    /// * `local` - The file on this machine
    /// * `remote` - Where it goes on the host
    #[must_use]
    pub fn scp_command(&self, local: &Path, remote: &str) -> Vec<String> {
        let mut command = vec!["scp".to_string(), "-q".to_string()];
        command.extend(Self::options());
        command.push(local.display().to_string());
        command.push(format!("{}:{remote}", self.target));
        command
    }

    /// Run a script on this host and report what it did, whatever its status
    ///
    /// # Arguments
    ///
    /// * `script` - The shell script to run
    /// * `stdin` - What to feed it, if anything
    ///
    /// # Errors
    ///
    /// When ssh itself cannot be started.
    pub fn output(&self, script: &str, stdin: Option<&[u8]>) -> color_eyre::Result<Output> {
        run(&self.ssh_command(script), stdin)
    }

    /// Run a script on this host and return its stdout, refusing a failure
    ///
    /// # Arguments
    ///
    /// * `script` - The shell script to run
    ///
    /// # Errors
    ///
    /// When the script fails, naming the host, the script and what it printed to stderr.
    pub fn run(&self, script: &str) -> color_eyre::Result<String> {
        let output = self.output(script, None)?;
        self.check(script, output)
    }

    /// Refuse a failed command's output, or return its stdout
    ///
    /// # Arguments
    ///
    /// * `script` - The script that ran, for the error
    /// * `output` - What it did
    fn check(&self, script: &str, output: Output) -> color_eyre::Result<String> {
        // a zero status is success
        if output.status == Some(0) {
            return Ok(output.stdout);
        }
        // 255 is ssh's own failure: the host was not reached, not the script
        if output.status == Some(255) {
            bail!(
                "could not reach {} over ssh ({}); keyless ssh with a known host key is required",
                self.target,
                output.stderr.trim()
            );
        }
        Err(eyre!(
            "`{script}` failed on {} with status {:?}: {}",
            self.target,
            output.status,
            output.stderr.trim()
        ))
    }

    /// Write a file on this host through ssh's stdin, atomically, with a mode and an owner
    ///
    /// # Arguments
    ///
    /// * `path` - Where it goes on the host
    /// * `contents` - What it holds
    /// * `mode` - Its octal permissions
    /// * `owner` - Who owns it, written through sudo; or none to write it as the ssh user
    ///
    /// # Errors
    ///
    /// When the write fails.
    pub fn write(
        &self,
        path: &str,
        contents: &[u8],
        mode: u32,
        owner: Option<&str>,
    ) -> color_eyre::Result<()> {
        // written beside the target and renamed, so a reader never sees half of it
        let script = write_script(path, mode, owner);
        let output = self.output(&script, Some(contents))?;
        self.check(&script, output).map(|_| ())
    }

    /// Copy a local file to this host
    ///
    /// # Arguments
    ///
    /// * `local` - The file on this machine
    /// * `remote` - Where it goes on the host
    ///
    /// # Errors
    ///
    /// When scp fails.
    pub fn copy(&self, local: &Path, remote: &str) -> color_eyre::Result<()> {
        let command = self.scp_command(local, remote);
        let output = run(&command, None)?;
        if output.status != Some(0) {
            bail!(
                "copying {} to {}:{remote} failed: {}",
                local.display(),
                self.target,
                output.stderr.trim()
            );
        }
        Ok(())
    }
}

/// The script that writes a file from stdin beside its target and renames it in place
///
/// # Arguments
///
/// * `path` - Where the file goes
/// * `mode` - Its octal permissions
/// * `owner` - Who owns it, written through sudo; or none to write it as the ssh user
#[must_use]
pub fn write_script(path: &str, mode: u32, owner: Option<&str>) -> String {
    let partial = quote(&format!("{path}.partial"));
    let path = quote(path);
    // an owned file is written as root and handed over before it is put in place
    let (sudo, chown) = match owner {
        Some(owner) => (
            "sudo -n ",
            format!("sudo -n chown {}: {partial}; ", quote(owner)),
        ),
        None => ("", String::new()),
    };
    format!(
        "set -e; {sudo}sh -c 'umask 077; cat > {inner}' ; {chown}{sudo}chmod {mode:o} {partial}; {sudo}mv -f {partial} {path}",
        inner = partial.replace('\'', r"'\''"),
    )
}

/// Run a local command, feeding it stdin, and collect what it did
///
/// # Arguments
///
/// * `command` - The program and its arguments
/// * `stdin` - What to feed it, if anything
fn run(command: &[String], stdin: Option<&[u8]>) -> color_eyre::Result<Output> {
    // start it with every stream piped
    let mut child = Command::new(&command[0])
        .args(&command[1..])
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .wrap_err_with(|| format!("failed to start {}", command[0]))?;
    // feed it and close its stdin so it sees the end
    if let Some(bytes) = stdin {
        let mut pipe = child.stdin.take().expect("a piped stdin");
        pipe.write_all(bytes)?;
    }
    // and wait for it
    let output = child.wait_with_output()?;
    Ok(Output {
        status: output.status.code(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every command runs in batch mode against the target it was given
    #[test]
    fn commands_are_batch_mode_and_quoted() {
        // an ssh that cannot prompt
        let host = Host {
            target: "ops@hyperion".to_string(),
        };
        assert_eq!(
            host.ssh_command("uname -r"),
            vec!["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-T", "ops@hyperion", "uname -r"]
        );
        // an scp that cannot either
        assert_eq!(
            host.scp_command(Path::new("/tmp/shoal-node"), "/opt/x/bin/shoal-node.new"),
            vec![
                "scp", "-q", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "/tmp/shoal-node",
                "ops@hyperion:/opt/x/bin/shoal-node.new"
            ]
        );
        // quoting leaves a plain path alone and makes anything else one word
        assert_eq!(quote("/opt/shoal-deploy/lab"), "/opt/shoal-deploy/lab");
        assert_eq!(quote("a b"), "'a b'");
        assert_eq!(quote("it's"), r"'it'\''s'");
        assert_eq!(quote(""), "''");
        // a file is written beside itself and renamed into place with its mode
        assert_eq!(
            write_script("/opt/x/tls/node.key", 0o600, None),
            "set -e; sh -c 'umask 077; cat > /opt/x/tls/node.key.partial' ; chmod 600 /opt/x/tls/node.key.partial; mv -f /opt/x/tls/node.key.partial /opt/x/tls/node.key"
        );
        // an owned file is written as root and handed to its owner before the rename
        assert_eq!(
            write_script("/opt/x/tls/node.key", 0o600, Some("shoal")),
            "set -e; sudo -n sh -c 'umask 077; cat > /opt/x/tls/node.key.partial' ; sudo -n chown shoal: /opt/x/tls/node.key.partial; sudo -n chmod 600 /opt/x/tls/node.key.partial; sudo -n mv -f /opt/x/tls/node.key.partial /opt/x/tls/node.key"
        );
    }

    /// A local command's output and status are reported as they were
    #[test]
    fn a_command_reports_its_output() {
        // a command that succeeds, fed stdin
        let output = run(&["cat".to_string()], Some(b"shoal")).expect("cat ran");
        assert_eq!(output.status, Some(0));
        assert_eq!(output.stdout, "shoal");
        // and one that fails
        let output = run(
            &["sh".to_string(), "-c".to_string(), "echo no >&2; exit 3".to_string()],
            None,
        )
        .expect("sh ran");
        assert_eq!(output.status, Some(3));
        assert_eq!(output.stderr, "no\n");
    }
}
