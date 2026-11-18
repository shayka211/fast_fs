//! Raw, generated FFI bindings to UCX (`ucp` and `ucs`).
//!
//! No safe API lives here — see the `ucx` crate. Note that bindgen cannot bind
//! `static inline` functions or function-like macros (e.g. `ucp_init`,
//! `UCS_PTR_IS_ERR`); the `ucx` crate re-implements the few it needs.

#![allow(
    non_upper_case_globals,
    non_camel_case_types,
    non_snake_case,
    missing_debug_implementations,
    clippy::all,
    clippy::undocumented_unsafe_blocks
)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
