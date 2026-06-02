use alloc::rc::{Rc, Weak};
use core::cell::Cell;
use core::marker::{PhantomData, PhantomPinned};
use core::pin::Pin;
use core::ptr::{self, DynMetadata};

pub trait Handler<T> {
    fn handle(&self, event: &T);
}

pub struct Registry<T> {
    head: Cell<*const ()>,
    _p: PhantomData<fn(T)>,
    _pin: PhantomPinned,
}

pub struct Guard<T> {
    node: *const (),
    _p: PhantomData<fn(T)>,
}

#[repr(C)]
struct Node<T, H: Handler<T> + ?Sized> {
    meta: DynMetadata<dyn Handler<T>>,
    /// If LSB=1, it points back to the registry.
    prev: Cell<*const ()>,
    next: Cell<*const ()>,
    handler: H,
}

impl<T> Drop for Registry<T> {
    fn drop(&mut self) {
        let mut next = self.head.get();
        while !next.is_null() {
            let meta = unsafe { *next.cast::<DynMetadata<dyn Handler<T>>>() };
            let node = ptr::from_raw_parts::<Node<T, dyn Handler<T>>>(next, meta);
            let node = unsafe { Rc::from_raw(node) };
            next = node.next.get();
        }
    }
}

impl<T> Drop for Guard<T> {
    fn drop(&mut self) {
        // This may read a node already dropped-in-place, but the `meta` field should still be intact.
        let meta = unsafe { *self.node.cast::<DynMetadata<dyn Handler<T>>>() };
        let node = ptr::from_raw_parts::<Node<T, dyn Handler<T>>>(self.node, meta);
        let Some(node) = unsafe { Weak::from_raw(node) }.upgrade() else { return };
        // TODO: remove node from the list.
        todo!()
    }
}

impl<T> Registry<T> {
    pub fn register(self: Pin<&Self>, handler: impl Handler<T> + 'static) -> Guard<T> {
        // TODO: implement.
        todo!()
    }

    pub fn broadcast(&self, event: &T) {
        // TODO: implement and correctly handle reentrancy
        todo!()
    }
}
