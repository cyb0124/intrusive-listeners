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
//! - **Accessing the registry in listener's destructor**\
//!   Listener's destructor may freely register, broadcast, or cancel any listener, including itself.
//!
//! # Caveats
//!
//! - The order in which the listeners run is unspecified.
//! - This crate requires a nightly compiler (needed for accessing vtable pointers).

#![no_std]
#![feature(ptr_metadata)]

extern crate alloc;

pub trait Listener<T> {
    fn accept(&self, event: &T);
}

impl<T, F: Fn(&T)> Listener<T> for F {
    fn accept(&self, event: &T) { self(event) }
}

// Bits stored in `Node::state`.
const ALIVE: usize = 1;
const RECURSIVE_CANCEL: usize = 2;
const RECURSIVE_VISIT: usize = 4;

/// Single-threaded implementation.
pub mod local;

#[cfg(feature = "lock_api")]
/// Thread-safe implementation via a per-registry lock.
pub mod locked;

// Run tests using:
// - MIRIFLAGS=-Zmiri-many-seeds cargo miri test
// - MIRIFLAGS="-Zmiri-tree-borrows -Zmiri-many-seeds" cargo miri test
