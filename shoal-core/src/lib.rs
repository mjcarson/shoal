#![feature(trivial_bounds, integer_sign_cast)]

pub mod client;
pub mod server;
pub mod shared;

pub use client::FromShoal;
pub use rkyv;
pub use server::tables;
pub use server::tables::storage;
pub use server::ShoalPool;
