//! An intrusive, type-erased event-listener registry for `no_std` with `alloc`.
//!
//! A [`Registry<T>`](local::Registry) holds a list of listeners for events of
//! type `T::Event`. Each listener is registered through a pinned `&Registry`, where
//! it is moved into a single heap allocation, type-erased to `dyn Listener<T>`,
//! and referred to by a thin, one-word handle.
//!
//! The handle is returned as a [`Guard`](local::Guard) by [`register`](local::Registry::register).
//! It is an RAII scope guard that unregisters its listener when dropped. The
//! cancellation is scan-free O(1) thanks to the intrusiveness. A guard may
//! safely outlive its registry, in which case dropping it does nothing.
//!
//! Events can be passed to listeners either by value with [`ByVal<T>`](ByVal) (cloned
//! per listener) or by reference with [`ByRef<T>`](ByRef). You can also implement the
//! [`EventFamily`] yourself for event types that borrow from the sender's stack.
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

use core::marker::PhantomData;

extern crate alloc;

pub trait EventFamily {
    type Event<'a>: Clone
    where Self: 'a;
}

pub struct ByVal<T: Clone>(PhantomData<fn(T) -> T>);

impl<T: Clone> Default for ByVal<T> {
    fn default() -> Self { Self(PhantomData) }
}

impl<T: Clone> EventFamily for ByVal<T> {
    type Event<'a>
        = T
    where T: 'a;
}

pub struct ByRef<T>(PhantomData<fn(T) -> T>);

impl<T> Default for ByRef<T> {
    fn default() -> Self { Self(PhantomData) }
}

impl<T> EventFamily for ByRef<T> {
    type Event<'a>
        = &'a T
    where T: 'a;
}

pub trait Listener<T: EventFamily> {
    fn accept(&self, event: T::Event<'_>);
}

impl<T: EventFamily, F: for<'a> Fn(T::Event<'a>)> Listener<T> for F {
    fn accept(&self, event: T::Event<'_>) { self(event) }
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
