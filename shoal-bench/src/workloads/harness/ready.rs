//! Waiting for a server to actually be able to answer, rather than for a fixed number of seconds
//!
//! `ShoalPool::start` spawns a thread per shard and returns without joining them. Since F36 the
//! pool hears from every shard and `ShoalPool::ready` waits for all of them, which the harness
//! calls first; the workload this replaced handled the gap by sleeping five seconds, and
//! `shoal/tests/utils.rs` slept two until it too waited on `ready`.
//!
//! A fixed sleep is wrong in both directions. It is too long on the machine that captures a
//! baseline, where a capture pays it once per run of every workload and spends minutes waiting on
//! a server that was ready immediately. It is too short on a slower or busier machine, and there
//! it is not merely slow but wrong: the queries that were sent before the shards were up measure
//! server startup and report it as query latency, with nothing in the artifact saying so.
//!
//! Probing costs a connection and one query, and answers a question `ready` does not: whether
//! the server answers a query the way the workload will send one, TLS handshake included. A bound
//! listener is not that, so the probe stays; what it no longer does is race the bind, which was
//! item 88.

use std::time::{Duration, Instant};

use anyhow::{bail, Result};

/// How long to wait between connection attempts
///
/// Short enough that a server that came up immediately is not held back by the poll interval, and
/// long enough that a server taking a second to bind is not hammered while it does.
const POLL: Duration = Duration::from_millis(25);

/// How long to keep trying before giving up
///
/// A server that has not answered in this long is not slow, it is broken, and a capture should say
/// so rather than hanging until somebody notices.
pub const TIMEOUT: Duration = Duration::from_secs(30);

/// Waits until a server answers a query, or gives up
///
/// Connecting is not enough on its own: the listener accepts before every shard is necessarily
/// able to serve, so this sends a real query and waits for its response. The query asks for a
/// partition key that no workload generates, so it is answered out of an empty table and costs
/// nothing measurable.
///
/// # Arguments
///
/// * `addr` - The address the server should be listening on
/// * `probe` - Sends one query and resolves when its response comes back
pub async fn wait_until_answering<F, Fut>(addr: &str, probe: F) -> Result<()>
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    // the whole point of the module is that the wait is bounded, so the bound is a constant here
    // and a parameter one level down where a test can shorten it
    wait_until_answering_within(addr, probe, TIMEOUT).await
}

/// Waits until a server answers a query, giving up after a given time
///
/// # Arguments
///
/// * `addr` - The address the server should be listening on
/// * `probe` - Sends one query and resolves when its response comes back
/// * `timeout` - How long to keep trying before giving up
pub async fn wait_until_answering_within<F, Fut>(
    addr: &str,
    probe: F,
    timeout: Duration,
) -> Result<()>
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    // remember when we started so the timeout covers the whole wait rather than one attempt
    let started = Instant::now();
    let mut attempts = 0u32;
    loop {
        attempts += 1;
        // try to connect and ask one question
        match probe(addr.to_string()).await {
            // it answered, so every shard that this query routed to is up
            Ok(()) => return Ok(()),
            Err(error) => {
                // give up once we have waited longer than any healthy start takes
                if started.elapsed() > timeout {
                    bail!(
                        "server at {addr} did not answer within {timeout:?} ({attempts} \
                         attempts), last error: {error:?}"
                    );
                }
            }
        }
        // wait a little before asking again
        tokio::time::sleep(POLL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::wait_until_answering;
    use anyhow::{bail, Result};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    /// A server that answers immediately is not waited on
    #[tokio::test]
    async fn an_answering_server_returns_at_once() {
        let started = std::time::Instant::now();
        let result = wait_until_answering("127.0.0.1:1", |_| async { Ok(()) }).await;
        assert!(result.is_ok());
        // no sleep should have been paid, so this is far below one poll interval
        assert!(started.elapsed() < super::POLL, "a ready server was slept on");
    }

    /// A server that comes up late is waited for rather than failed on
    #[tokio::test]
    async fn a_slow_server_is_waited_for() {
        let attempts = Arc::new(AtomicU32::new(0));
        let counter = attempts.clone();
        let result = wait_until_answering("127.0.0.1:1", move |_| {
            let counter = counter.clone();
            async move {
                // refuse the first three attempts, then answer
                if counter.fetch_add(1, Ordering::SeqCst) < 3 {
                    bail!("connection refused");
                }
                Ok::<(), anyhow::Error>(())
            }
        })
        .await;
        assert!(result.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 4);
    }

    /// A server that never answers fails with what it was still failing on
    ///
    /// The last error is carried into the message because "the server did not start" on its own
    /// sends the reader to the wrong place: the useful half is whether it was refusing
    /// connections or answering them with an error.
    ///
    /// Runs against a shortened timeout rather than the real one, so the test costs a few
    /// milliseconds instead of the thirty seconds a real capture is willing to wait.
    #[tokio::test]
    async fn a_dead_server_fails_with_its_last_error() {
        let result: Result<()> = super::wait_until_answering_within(
            "127.0.0.1:1",
            |_| async { bail!("connection refused") },
            std::time::Duration::from_millis(50),
        )
        .await;
        let error = result.expect_err("a server that never answers is an error");
        let message = format!("{error}");
        assert!(message.contains("did not answer"), "{message}");
        assert!(message.contains("connection refused"), "{message}");
    }
}
