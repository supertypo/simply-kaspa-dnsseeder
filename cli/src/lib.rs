//! Command-line argument definitions for `simply-kaspa-dnsseeder`.
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod cli_args;

#[cfg(test)]
mod cli_args_tests;

pub use cli_args::CliArgs;
