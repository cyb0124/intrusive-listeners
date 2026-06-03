//! An intrusive, type-erased event-listener registry for `no_std` with `alloc`.
//!
//! A [`Registry<T>`](local::Registry) holds a list of listeners for events of
//! type `T`. Each listener is registered through a pinned `&Registry`, where
//! it is moved into a single heap allocation, type-erased to `dyn Listener<T>`,
//! and referred to by a thin, one-word handle.
//!
//! The handle is returned as a [`Guard`](local::Guard) by [`register`](local::Registry::register).
//! It is an RAII scope guard that unregisters its listener when dropped. The
//! cancellation is scan-free O(1) thanks to the intrusiveness. A guard may
//! safely outlive its registry, in which case dropping it does nothing.
//!
//! # Reentrancy
//!
//! This implementation tolerates all kinds of recursive update scenarios.
//! The expected behaviors are listed below.
//!
//! - **Listener cancelling itself in its callback**\
//!   The in-progress call will run to completion and will be destructed afterwards.
//! - **Listener cancelling other listeners**\
//!   Cancelled listeners further down the queue will not receive the in-flight event.
//! - **Listener calling [`broadcast`](local::Registry::broadcast) again in its callback (recursive notification)**\
//!   Callbacks for the new event will immediately run inside the nested `broadcast` call.
//! - **Registering new listeners inside a listener callback**\
//!   The new listeners will not receive the in-flight event.
//!
//! # Caveats
//!
//! - The order in which the listeners run is unspecified.
//! - This crate requires a nightly compiler (needed for accessing vtable pointers).
//! - This crate currently only provides the single-threaded [`local`] flavor.
//!   A thread-safe `shared` flavor may be added in the future.

#![no_std]
#![feature(ptr_metadata)]

extern crate alloc;

pub mod local;

// Run tests using:
// - cargo miri test
// - MIRIFLAGS=-Zmiri-tree-borrows cargo miri test
