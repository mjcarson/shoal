//! Turn a password into the stanza a `shoal.yml` carries instead of one
//!
//! ```text
//! cargo run --example scram_credential -- reader
//! ```
//!
//! The password is read from the terminal rather than taken as an argument, so it does not end up
//! in a shell history or in the output of `ps`. What this prints holds no password: it is a salt,
//! an iteration count, and two derived keys, and the password cannot be recovered from them
//! without redoing the work the iteration count names.
//!
//! **What it prints is still a secret.** `stored_key` cannot be turned back into a password and
//! *can* be replayed as a login by anything that reads it, so the file it goes into needs
//! permissions on it. See [`StoredCredential`] and the feature page for authentication.

use std::io::{BufRead, Write};

use shoal::shared::auth::{StoredCredential, DEFAULT_ITERATIONS};

/// Read a password from the terminal and print the config stanza it derives to
fn main() {
    // the name this credential belongs to, which is the key it goes under in the config
    let mut args = std::env::args().skip(1);
    let Some(username) = args.next() else {
        eprintln!("usage: cargo run --example scram_credential -- <username> [iterations]");
        std::process::exit(1);
    };
    // let a deployment that wants a slower derivation say so
    let iterations = match args.next() {
        Some(raw) => raw.parse::<u32>().unwrap_or_else(|_| {
            eprintln!("the iteration count has to be a number");
            std::process::exit(1);
        }),
        None => DEFAULT_ITERATIONS,
    };
    // read the password off stdin rather than off the command line
    eprint!("password for {username}: ");
    std::io::stderr().flush().expect("failed to write a prompt");
    let mut password = String::new();
    std::io::stdin()
        .lock()
        .read_line(&mut password)
        .expect("failed to read a password");
    let password = password.trim_end_matches(['\n', '\r']);
    // a password nobody typed would derive a credential nobody can use
    if password.is_empty() {
        eprintln!("refusing to derive a credential from an empty password");
        std::process::exit(1);
    }
    // derive it, which is the only place this process sees the password
    let credential = StoredCredential::from_password(password, iterations);
    let body = serde_yaml::to_string(&credential).expect("failed to write a credential");
    // print the stanza indented to where it belongs under `auth.users`
    println!("auth:");
    println!("  required: true");
    println!("  users:");
    println!("    {username}:");
    println!("      scram_sha_256:");
    for line in body.lines() {
        println!("        {line}");
    }
}
