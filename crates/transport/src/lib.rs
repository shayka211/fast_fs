#![doc = include_str!("../README.md")]

mod error;
mod handler;
mod remote;
mod transport;

pub use error::{Error, Result};
pub use handler::{Handler, Incoming, Reply};
pub use remote::RemoteHandle;
pub use transport::{Transport, TransportBuilder};

#[cfg(test)]
mod tests;
