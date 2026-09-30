//! `shoaladm stats` as a full screen view: read the figures, chart them, take keys
//!
//! One read is in flight at a time. It is a future the loop keeps across its turns and polls
//! beside the terminal's keys and a redraw tick; a key never drops it, and once it answers the
//! next one is armed to start after the poll interval
//! ([F64](../../../../docs/src/features/stats-tui.md)). Nothing is spawned, so a schema's
//! client needs no bound beyond the ones every admin command already has, and no channel sits
//! between the read and the screen to lose an answer in (#152).

use crossterm::event::{Event, EventStream, KeyEventKind};
use futures::StreamExt;
use rkyv::Archive;
use shoal::Shoal;
use shoal::client::ClientOptions;
use shoal::shared::auth::Credentials;
use shoal::shared::identity::NodeId;
use shoal::shared::traits::QuerySupport;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::screen::{Outcome, Screen};
use super::{StatsModel, leader_stats, view};

/// How often the screen is drawn when nothing else happens, so the chart's edge moves on
const REDRAW_EVERY: Duration = Duration::from_secs(1);

/// What reads the figures: the client the operator reached, the leader's once found, and the
/// names the deployment gave its nodes
pub struct Poller<S: QuerySupport> {
    /// The client the operator reached the cluster through
    shoal: Arc<Shoal<S>>,
    /// The table to narrow the figures to, if one
    table: Option<String>,
    /// The name the deployment gave each node, for a member whose figures carry no hostname
    names: BTreeMap<NodeId, String>,
    /// The leader's client from an earlier read, by its address
    leader: Option<(String, Arc<Shoal<S>>)>,
    /// The principal the leader is dialed as
    admin: String,
    /// Its password
    password: String,
}

impl<S> Poller<S>
where
    S: QuerySupport + Send + Sync + 'static,
    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    /// A poller reading through a client
    ///
    /// # Arguments
    ///
    /// * `shoal` - The client the operator reached the cluster through
    /// * `table` - The table to narrow the figures to, if one
    /// * `names` - The name the deployment gave each node, by its id
    /// * `admin` - The principal the leader is dialed as
    /// * `password` - Its password
    #[must_use]
    pub fn new(
        shoal: Arc<Shoal<S>>,
        table: Option<String>,
        names: BTreeMap<NodeId, String>,
        admin: String,
        password: String,
    ) -> Self {
        Poller {
            shoal,
            table,
            names,
            leader: None,
            admin,
            password,
        }
    }

    /// Read the leader's figures once, named by hostname, deployment name or id
    ///
    /// # Errors
    ///
    /// When the node the client reached cannot be read at all, as the text to show.
    pub async fn poll(&mut self) -> Result<StatsModel, String> {
        // the leader is dialed as the admin, once per read that needs it
        let admin = self.admin.clone();
        let password = self.password.clone();
        let dial = move |addr: String| async move {
            let options = ClientOptions::new().credentials(Credentials::scram(admin, password));
            Shoal::<S>::with_options(addr.as_str(), options)
                .await
                .map(Arc::new)
                .map_err(|error| format!("could not connect to {addr}: {error:?}"))
        };
        // the leader's answer, or the reached node's own with why, named by what is known
        leader_stats(&self.shoal, self.table.as_deref(), &mut self.leader, dial)
            .await
            .map(|model| model.with_names(&self.names))
    }
}

/// Wait, then read the figures once, handing the poller back with the answer
///
/// # Arguments
///
/// * `poller` - What reads the figures
/// * `delay` - How long to wait first
async fn read_after<S>(
    mut poller: Poller<S>,
    delay: Duration,
) -> (Poller<S>, Result<StatsModel, String>)
where
    S: QuerySupport + Send + Sync + 'static,
    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    // the interval is counted from the last answer, so a slow read never stacks up behind it
    tokio::time::sleep(delay).await;
    let answer = poller.poll().await;
    (poller, answer)
}

/// Draw the figures full screen until the operator leaves
///
/// # Arguments
///
/// * `poller` - What reads the figures
/// * `title` - The cluster's name
/// * `every` - How often the figures are read
///
/// # Errors
///
/// When the terminal fails.
pub async fn run<S>(poller: Poller<S>, title: String, every: Duration) -> color_eyre::Result<()>
where
    S: QuerySupport + Send + Sync + 'static,
    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    let mut screen = Screen::new(title, every);
    // the terminal, restored whatever the loop comes to
    let mut terminal = ratatui::init();
    let result = event_loop(&mut terminal, &mut screen, poller).await;
    ratatui::restore();
    result
}

/// Draw, take keys and take answers until the operator leaves
///
/// # Arguments
///
/// * `terminal` - The terminal
/// * `screen` - What is drawn
/// * `poller` - What reads the figures
async fn event_loop<S>(
    terminal: &mut ratatui::DefaultTerminal,
    screen: &mut Screen,
    poller: Poller<S>,
) -> color_eyre::Result<()>
where
    S: QuerySupport + Send + Sync + 'static,
    for<'a> <<S as QuerySupport>::ResponseKinds as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    // keys from the terminal, and a tick so the chart's edge moves while nothing else does
    let mut events = EventStream::new();
    let mut redraw = tokio::time::interval(REDRAW_EVERY);
    // the first read starts at once; it is kept across turns, so a key never drops it
    let mut read = Box::pin(read_after(poller, Duration::ZERO));
    loop {
        terminal.draw(|frame| view::render(frame, screen, Instant::now()))?;
        tokio::select! {
            event = events.next() => {
                // the terminal closing its input is a way out too
                let Some(event) = event else {
                    return Ok(());
                };
                // a resize is drawn on the next turn; only a press is a key
                let Event::Key(key) = event? else {
                    continue;
                };
                if key.kind != KeyEventKind::Press {
                    continue;
                }
                if screen.handle_key(key, Instant::now()) == Outcome::Quit {
                    return Ok(());
                }
            }
            (poller, answer) = &mut read => {
                // sample the answer, and arm the next read an interval after it
                screen.observe(answer, Instant::now());
                read = Box::pin(read_after(poller, screen.every));
            }
            _ = redraw.tick() => (),
        }
    }
}
