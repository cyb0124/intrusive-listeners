use crate::{ALIVE, Listener, RECURSIVE_CANCEL, RECURSIVE_VISIT};
use alloc::boxed::Box;
use core::cell::Cell;
use core::marker::PhantomData;
use core::mem::ManuallyDrop;
use core::ptr::{self, DynMetadata, null};
use lock_api::RawMutex;

pub struct Registry<T, R: RawMutex> {
    inner: *const Inner<T, R>,
}

pub struct Guard<T, R: RawMutex> {
    node: *const (),
    _p: PhantomData<Inner<T, R>>,
}

struct Inner<T, R: RawMutex> {
    lock: R,
    ref_count: Cell<usize>,
    head: Cell<*const ()>,
    _p: PhantomData<fn(T)>,
}

unsafe impl<T, R: RawMutex + Send + Sync> Send for Registry<T, R> {}
unsafe impl<T, R: RawMutex + Send + Sync> Sync for Registry<T, R> {}
unsafe impl<T, R: RawMutex + Send + Sync> Send for Guard<T, R> {}
unsafe impl<T, R: RawMutex> Sync for Guard<T, R> {}

#[repr(C)]
struct Node<T, R: RawMutex, L: Listener<T> + ?Sized> {
    meta: DynMetadata<dyn Listener<T>>,
    parent: *const Inner<T, R>,
    prev: Cell<*const ()>,
    next: Cell<*const ()>,
    state: Cell<usize>,
    listener: ManuallyDrop<L>,
}

unsafe fn resolve<T, R: RawMutex>(thin: *const ()) -> *const Node<T, R, dyn Listener<T>> {
    let meta = unsafe { *thin.cast::<DynMetadata<dyn Listener<T>>>() };
    ptr::from_raw_parts::<Node<T, R, dyn Listener<T>>>(thin, meta)
}

impl<T, R: RawMutex> Node<T, R, dyn Listener<T>> {
    unsafe fn unlink(&self, parent: &Inner<T, R>) {
        let (prev, next) = (self.prev.get(), self.next.get());
        if prev.is_null() {
            parent.head.set(next);
        } else {
            unsafe { (*resolve::<T, R>(prev)).next.set(next) };
        }
        if !next.is_null() {
            unsafe { (*resolve::<T, R>(next)).prev.set(prev) };
        }
    }
}

impl<T, R: RawMutex> Drop for Registry<T, R> {
    fn drop(&mut self) {
        let inner = unsafe { &*self.inner };
        inner.lock.lock();
        let mut thin = inner.head.get();
        while !thin.is_null() {
            let node = unsafe { &mut *resolve::<T, R>(thin).cast_mut() };
            *node.state.get_mut() |= RECURSIVE_VISIT;
            thin = *node.next.get_mut();
        }
        thin = inner.head.get();
        while !thin.is_null() {
            let ptr = unsafe { resolve::<T, R>(thin) }.cast_mut();
            thin = unsafe { *(*ptr).next.get_mut() };
            unsafe { inner.lock.unlock() };
            unsafe { ManuallyDrop::drop(&mut (*ptr).listener) };
            inner.lock.lock();
            let state = unsafe { (*ptr).state.get_mut() };
            if *state & RECURSIVE_CANCEL != 0 {
                drop(unsafe { Box::from_raw(ptr) });
            } else {
                *state &= !ALIVE;
            }
        }
        let ref_count = inner.ref_count.get() - 1;
        if ref_count == 0 {
            // Unnecessary to unlock here.
            drop(unsafe { Box::from_raw(self.inner.cast_mut()) });
        } else {
            inner.ref_count.set(ref_count);
            unsafe { inner.lock.unlock() };
        }
    }
}

impl<T, R: RawMutex> Drop for Guard<T, R> {
    /// May overlap listener destructor.
    fn drop(&mut self) {
        let ptr = unsafe { resolve::<T, R>(self.node) };
        let parent_ptr = unsafe { (*ptr).parent };
        let parent = unsafe { &*parent_ptr };
        parent.lock.lock();
        let state = unsafe { (*ptr).state.get() };
        let (destruct, free) = if state & ALIVE == 0 {
            (false, true)
        } else if state & !(RECURSIVE_VISIT - 1) == 0 {
            unsafe { (*ptr).unlink(parent) };
            (true, true)
        } else {
            unsafe { (*ptr).state.set(state | RECURSIVE_CANCEL) };
            (false, false)
        };
        let ref_count = parent.ref_count.get() - 1;
        if ref_count == 0 {
            // Unnecessary to unlock here.
            drop(unsafe { Box::from_raw(parent_ptr.cast_mut()) });
        } else {
            parent.ref_count.set(ref_count);
            unsafe { parent.lock.unlock() };
        }
        if free {
            let ptr = ptr.cast_mut();
            if destruct {
                unsafe { ManuallyDrop::drop(&mut (*ptr).listener) };
            }
            drop(unsafe { Box::from_raw(ptr) });
        }
    }
}

impl<T, R: RawMutex> Default for Registry<T, R> {
    fn default() -> Self { Self::new() }
}

impl<T, R: RawMutex> Registry<T, R> {
    pub fn new() -> Self {
        Self { inner: Box::into_raw(Box::new(Inner { lock: R::INIT, head: Cell::new(null()), ref_count: Cell::new(1), _p: PhantomData })) }
    }

    pub fn register(&self, listener: impl Listener<T> + Send + Sync + 'static) -> Guard<T, R> {
        let mut node = Box::new(Node {
            meta: ptr::metadata(&listener as &dyn Listener<T>),
            parent: self.inner,
            prev: Cell::new(null()),
            next: Cell::new(null()),
            state: Cell::new(ALIVE),
            listener: ManuallyDrop::new(listener),
        }) as Box<Node<T, R, dyn Listener<T>>>;
        let inner = unsafe { &*self.inner };
        inner.lock.lock();
        let next = inner.head.get();
        *node.next.get_mut() = next;
        let thin = Box::into_raw(node).cast_const().to_raw_parts().0;
        inner.head.set(thin);
        if !next.is_null() {
            unsafe { &*resolve::<T, R>(next) }.prev.set(thin);
        }
        inner.ref_count.update(|x| x + 1);
        unsafe { inner.lock.unlock() };
        Guard { node: thin, _p: PhantomData }
    }

    pub fn broadcast(&self, event: &T) {
        let inner = unsafe { &*self.inner };
        inner.lock.lock();
        let mut deferred_cancels = null::<()>();
        let mut thin = inner.head.get();
        while !thin.is_null() {
            let node = unsafe { &*resolve::<T, R>(thin) };
            let mut state = node.state.get();
            if state & RECURSIVE_CANCEL != 0 {
                // Node already cancelled by an outer `accept` call. Outermost `broadcast` will unlink it.
                thin = node.next.get();
                continue;
            }
            node.state.set(state + RECURSIVE_VISIT);
            unsafe { inner.lock.unlock() };
            node.listener.accept(event);
            inner.lock.lock();
            let next = node.next.get();
            state = node.state.get() - RECURSIVE_VISIT;
            if state & !(RECURSIVE_CANCEL - 1) == RECURSIVE_CANCEL {
                unsafe { node.unlink(inner) };
                node.next.set(deferred_cancels);
                deferred_cancels = thin;
            } else {
                node.state.set(state);
            }
            thin = next;
        }
        unsafe { inner.lock.unlock() };
        while !deferred_cancels.is_null() {
            let ptr = unsafe { resolve::<T, R>(deferred_cancels) }.cast_mut();
            let node = unsafe { &mut *ptr };
            deferred_cancels = *node.next.get_mut();
            unsafe { ManuallyDrop::drop(&mut node.listener) };
            drop(unsafe { Box::from_raw(ptr) });
        }
    }
}
