use alloc::rc::{Rc, Weak};
use core::cell::Cell;
use core::marker::{PhantomData, PhantomPinned};
use core::mem::forget;
use core::pin::Pin;
use core::ptr::{self, DynMetadata, null};

pub trait Listener<T> {
    fn accept(&self, event: &T);
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
struct Node<T, H: Listener<T> + ?Sized> {
    meta: DynMetadata<dyn Listener<T>>,
    /// If LSB=1, it points back to the registry.
    prev: Cell<*const ()>,
    next: Cell<*const ()>,
    /// If LSB=1, the node is cancelled while still in its `accept` call.
    /// Other bits count the depth of recursive `broadcast` calls.
    recursion: Cell<usize>,
    handler: H,
}

unsafe fn resolve<T>(thin: *const ()) -> *const Node<T, dyn Listener<T>> {
    // In the `Weak` case this may read a node already dropped-in-place, but the `meta` field should still be intact.
    let meta = unsafe { *thin.cast::<DynMetadata<dyn Listener<T>>>() };
    ptr::from_raw_parts::<Node<T, dyn Listener<T>>>(thin, meta)
}

impl<T> Node<T, dyn Listener<T>> {
    unsafe fn unlink(self: Rc<Self>) {
        let (prev, next) = (self.prev.get(), self.next.get());
        if prev.addr() & 1 == 1 {
            let registry = prev.map_addr(|x| x & !1).cast::<Registry<T>>();
            unsafe { (*registry).head.set(next) };
        } else {
            unsafe { (*resolve::<T>(prev)).next.set(next) };
        }
        if !next.is_null() {
            unsafe { (*resolve::<T>(next)).prev.set(prev) };
        }
    }
}

impl<T> Drop for Registry<T> {
    fn drop(&mut self) {
        let mut thin = self.head.get();
        while !thin.is_null() {
            thin = unsafe { Rc::from_raw(resolve::<T>(thin)) }.next.get();
        }
    }
}

impl<T> Drop for Guard<T> {
    fn drop(&mut self) {
        let ptr = unsafe { resolve::<T>(self.node) };
        if unsafe { Weak::from_raw(ptr) }.strong_count() > 0 {
            let node = unsafe { &*ptr };
            let depth = node.recursion.get();
            if depth == 0 {
                unsafe { Rc::from_raw(ptr).unlink() }
            } else {
                node.recursion.set(depth | 1);
            }
        }
    }
}

impl<T> Default for Registry<T> {
    fn default() -> Self { Self::new() }
}

impl<T> Registry<T> {
    pub const fn new() -> Self { Self { head: Cell::new(null()), _p: PhantomData, _pin: PhantomPinned } }

    pub fn register(self: Pin<&Self>, handler: impl Listener<T> + 'static) -> Guard<T> {
        let next = self.head.get();
        let node = Rc::new(Node {
            meta: ptr::metadata(&handler as &dyn Listener<T>),
            prev: (&raw const *self).map_addr(|x| x | 1).cast::<()>().into(),
            next: next.into(),
            recursion: Cell::new(0),
            handler,
        }) as Rc<Node<T, dyn Listener<T>>>;
        forget(Rc::downgrade(&node));
        let thin = Rc::into_raw(node).to_raw_parts().0;
        self.head.set(thin);
        if !next.is_null() {
            unsafe { &*resolve::<T>(next) }.prev.set(thin);
        }
        Guard { node: thin, _p: PhantomData }
    }

    pub fn broadcast(&self, event: &T) {
        let mut thin = self.head.get();
        while !thin.is_null() {
            let ptr = unsafe { resolve::<T>(thin) };
            let node = unsafe { &*ptr };
            let mut depth = node.recursion.get();
            if depth & 1 == 1 {
                // Node already cancelled by an outer `accept` call. Outermost `broadcast` will unlink it.
                thin = node.next.get();
                continue;
            }
            node.recursion.set(depth + 2);
            node.handler.accept(event);
            thin = node.next.get();
            depth = node.recursion.get() - 2;
            if depth == 1 {
                unsafe { Rc::from_raw(ptr).unlink() }
            } else {
                node.recursion.set(depth);
            }
        }
    }
}
    }
}
