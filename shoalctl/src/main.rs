//! The main entry point for shoalctl
//!
//! This module initializes the terminal, enables mouse capture, and runs
//! the main application loop using tokio for async event handling.

mod app;
mod components;

use crossterm::event::{DisableMouseCapture, EnableMouseCapture, EventStream};
use crossterm::execute;
use futures::StreamExt;
use ratatui::DefaultTerminal;
use std::io::stdout;

use app::App;

/// Start rendering shoalctl
///
/// This function contains the main event loop that renders frames and
/// processes user input until the application is told to quit.
///
/// # Arguments
///
/// * `terminal` - The terminal backend to render our app on
///
/// # Returns
///
/// * `Ok(())` - The application exited normally
/// * `Err(_)` - An I/O error occurred during rendering or event handling
async fn run_app(terminal: &mut DefaultTerminal) -> std::io::Result<()> {
    // create our app
    let mut app = App::new();
    // create an async event stream for terminal events
    let mut event_stream = EventStream::new();
    // loop forever and handle events until we're told to stop
    while !app.should_quit {
        // draw a terminal frame
        terminal.draw(|frame| app.render(frame))?;
        // get the next event to handle asynchronously
        if let Some(event_result) = event_stream.next().await {
            let event = event_result?;
            // handle this event
            app.handle_event(event);
        }
    }
    Ok(())
}

/// Start the shoalctl TUI
///
/// Initializes error handling, enables mouse capture, runs the application,
/// and cleans up mouse capture on exit.
///
/// # Returns
///
/// * `Ok(())` - The application ran and exited successfully
/// * `Err(_)` - An error occurred during initialization or execution
#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    // setup eyre so it can help us have nice errors
    color_eyre::install()?;
    // start capturing and handling mouse clicks
    execute!(stdout(), EnableMouseCapture)?;
    // initialize the terminal
    let mut terminal = ratatui::init();
    // start our app
    let result = run_app(&mut terminal).await;
    // restore the terminal to its original state
    ratatui::restore();
    // stop capturing and handling mouse clicks
    execute!(stdout(), DisableMouseCapture)?;
    // handle any errors here instead of returning it so it can be converted
    // to a nice eyre error
    result?;
    Ok(())
}
