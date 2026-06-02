//! An intrusive, type-erased event-listener registry for `no_std` with `alloc`.
//!
//! A [`Registry<T>`](rc::Registry) holds a list of listeners for events of
//! type `T`. Each listener is registered through a pinned `&Registry`, where
//! it is moved into a single heap allocation, type-erased to `dyn Handler<T>`,
//! and referred to by a thin, one-word handle.
//!
//! The handle is returned as a [`Guard`](rc::Guard) by [`register`](rc::Registry::register).
//! It is an RAII scope guard that unregisters its listener when dropped. The
//! cancellation is scan-free O(1) thanks to the intrusiveness. A guard may
//! safely outlive its registry, in which case dropping it does nothing.

#![no_std]
#![feature(ptr_metadata)]

extern crate alloc;

pub mod rc;
