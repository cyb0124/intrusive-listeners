use alloc::rc::{Rc, Weak};
use core::cell::Cell;
use core::marker::{PhantomData, PhantomPinned};
use core::mem::forget;
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

unsafe fn resolve<T>(node: *const ()) -> *const Node<T, dyn Handler<T>> {
    // In the `Weak` case this may read a node already dropped-in-place, but the `meta` field should still be intact.
    let meta = unsafe { *node.cast::<DynMetadata<dyn Handler<T>>>() };
    ptr::from_raw_parts::<Node<T, dyn Handler<T>>>(node, meta)
}

impl<T> Drop for Registry<T> {
    fn drop(&mut self) {
        let mut node = self.head.get();
        while !node.is_null() {
            node = unsafe { Rc::from_raw(resolve::<T>(node)) }.next.get();
        }
    }
}

impl<T> Drop for Guard<T> {
    fn drop(&mut self) {
        let ptr = unsafe { resolve::<T>(self.node) };
        let Some(node) = unsafe { Weak::from_raw(ptr) }.upgrade() else { return };
        let (prev, next) = (node.prev.get(), node.next.get());
        // Temporarily leaks a strong count; decremented later.
        if prev.addr() & 1 == 1 {
            let registry = prev.map_addr(|x| x & !1).cast::<Registry<T>>();
            unsafe { &*registry }.head.set(next);
        } else {
            unsafe { &*resolve::<T>(prev) }.next.set(next);
        }
        if !next.is_null() {
            unsafe { &*resolve::<T>(next) }.prev.set(prev);
        }
        unsafe { Rc::decrement_strong_count(ptr) };
    }
}

impl<T> Registry<T> {
    pub fn register(self: Pin<&Self>, handler: impl Handler<T> + 'static) -> Guard<T> {
        let next = self.head.get();
        let node = Rc::new(Node {
            meta: ptr::metadata(&handler as &dyn Handler<T>),
            prev: (&raw const *self).map_addr(|x| x | 1).cast::<()>().into(),
            next: next.into(),
            handler,
        }) as Rc<Node<T, dyn Handler<T>>>;
        forget(Rc::downgrade(&node));
        let ptr = Rc::into_raw(node).to_raw_parts().0;
        self.head.set(ptr);
        if !next.is_null() {
            unsafe { &*resolve::<T>(next) }.prev.set(ptr);
        }
        Guard { node: ptr, _p: PhantomData }
    }

    pub fn broadcast(&self, event: &T) {
        // TODO: implement and correctly handle reentrancy
        todo!()
    }
}
