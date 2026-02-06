//! shoalctl - A terminal UI for querying Shoal databases
//!
//! This crate provides a generic TUI application for interacting with Shoal databases.
//! Users must compile shoalctl with their specific database types.
//!
//! # Example
//!
//! ```ignore
//! use std::sync::Arc;
//! use shoal_core::client::Shoal;
//!
//! #[tokio::main]
//! async fn main() -> color_eyre::Result<()> {
//!     // Create your Shoal client with your database type
//!     let shoal = Arc::new(Shoal::<MyDbClient>::new("127.0.0.1:12000").await?);
//!     // Run shoalctl
//!     shoalctl::run(shoal).await
//! }
//! ```

pub mod app;
pub mod components;

use std::sync::Arc;

use crossterm::event::{DisableMouseCapture, EnableMouseCapture, Event, EventStream};
use crossterm::execute;
use ratatui::DefaultTerminal;
use rkyv::Archive;
use shoal::client::Shoal;
use shoal::traits::QuerySupport;
use std::io::stdout;
use uuid::Uuid;

use app::{App, QueryRequest, QueryResult};

/// Events that can be received by the main event loop
pub enum AppEvent<S: QuerySupport> {
    /// A terminal event (keyboard, mouse, resize, etc.)
    Terminal(Event),
    /// A query result from the background executor
    QueryResult {
        tab_id: Uuid,
        result: QueryResult<S>,
    },
}

/// Start rendering shoalctl
///
/// This function contains the main event loop that renders frames and
/// processes user input until the application is told to quit.
///
/// # Arguments
///
/// * `terminal` - The terminal backend to render our app on
/// * `shoal` - The Shoal client to use for queries
///
/// # Returns
///
/// * `Ok(())` - The application exited normally
/// * `Err(_)` - An I/O error occurred during rendering or event handling
async fn run_app<S>(terminal: &mut DefaultTerminal, shoal: Arc<Shoal<S>>) -> std::io::Result<()>
where
    S: QuerySupport + Send + Sync + 'static,
    S::QueryKinds: Send,
    S::ResponseKinds: Send,
    <S::ResponseKinds as Archive>::Archived: Send
        + rkyv::Deserialize<
            S::ResponseKinds,
            rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        >,
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
    // Create our app with the request channel
    let mut app = App::new(shoal);
    // start handling events in shoalctl
    app.start(terminal).await?;
    Ok(())
}

/// Run the shoalctl TUI application
///
/// This function initializes the terminal, enables mouse capture, runs the main
/// application loop, and cleans up on exit.
///
/// # Arguments
///
/// * `shoal` - An Arc-wrapped Shoal client for the target database
///
/// # Returns
///
/// * `Ok(())` - The application ran and exited successfully
/// * `Err(_)` - An error occurred during initialization or execution
pub async fn run<S>(shoal: Arc<Shoal<S>>) -> color_eyre::Result<()>
where
    S: QuerySupport + Send + Sync + 'static,
    S::QueryKinds: Send,
    S::ResponseKinds: Send,
    <S::ResponseKinds as Archive>::Archived: Send
        + rkyv::Deserialize<
            S::ResponseKinds,
            rkyv::rancor::Strategy<rkyv::de::Pool, rkyv::rancor::Error>,
        >,
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
    // setup eyre so it can help us have nice errors
    color_eyre::install()?;
    // start capturing and handling mouse clicks
    execute!(stdout(), EnableMouseCapture)?;
    // initialize the terminal
    let mut terminal = ratatui::init();
    // start our app
    let result = run_app(&mut terminal, shoal).await;
    // restore the terminal to its original state
    ratatui::restore();
    // stop capturing and handling mouse clicks
    execute!(stdout(), DisableMouseCapture)?;
    // handle any errors here instead of returning it so it can be converted
    // to a nice eyre error
    result?;
    Ok(())
}
