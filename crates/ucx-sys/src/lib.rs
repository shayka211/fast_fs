#![doc = include_str!("../README.md")]
#![allow(
    non_upper_case_globals,
    non_camel_case_types,
    non_snake_case,
    missing_debug_implementations,
    clippy::all,
    clippy::undocumented_unsafe_blocks,
    // UCX's Doxygen comments (`@param [in] x`) aren't valid rustdoc.
    rustdoc::all
)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
