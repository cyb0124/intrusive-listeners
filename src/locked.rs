use crate::{ALIVE, EventFamily, Listener, RECURSIVE_CANCEL, RECURSIVE_VISIT};
use alloc::boxed::Box;
use core::cell::Cell;
use core::marker::PhantomData;
use core::mem::ManuallyDrop;
use core::ptr::{self, DynMetadata, null};
use lock_api::RawMutex;

pub struct Registry<T: EventFamily, R: RawMutex> {
    inner: *const Inner<T, R>,
}

#[must_use]
pub struct Guard<T: EventFamily, R: RawMutex> {
    node: *const (),
    _p: PhantomData<Inner<T, R>>,
}

struct Inner<T: EventFamily, R: RawMutex> {
    lock: R,
    ref_count: Cell<usize>,
    head: Cell<*const ()>,
    _p: PhantomData<for<'a> fn(T::Event<'a>)>,
}

unsafe impl<T: EventFamily, R: RawMutex + Send + Sync> Send for Registry<T, R> {}
unsafe impl<T: EventFamily, R: RawMutex + Send + Sync> Sync for Registry<T, R> {}
unsafe impl<T: EventFamily, R: RawMutex + Send + Sync> Send for Guard<T, R> {}
unsafe impl<T: EventFamily, R: RawMutex> Sync for Guard<T, R> {}

#[repr(C)]
struct Node<T: EventFamily, R: RawMutex, L: Listener<T> + ?Sized> {
    meta: DynMetadata<dyn Listener<T>>,
    parent: *const Inner<T, R>,
    prev: Cell<*const ()>,
    next: Cell<*const ()>,
    state: Cell<usize>,
    listener: ManuallyDrop<L>,
}

unsafe fn resolve<T: EventFamily, R: RawMutex>(thin: *const ()) -> *const Node<T, R, dyn Listener<T>> {
    let meta = unsafe { *thin.cast::<DynMetadata<dyn Listener<T>>>() };
    ptr::from_raw_parts::<Node<T, R, dyn Listener<T>>>(thin, meta)
}

impl<T: EventFamily, R: RawMutex> Node<T, R, dyn Listener<T>> {
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

impl<T: EventFamily, R: RawMutex> Drop for Registry<T, R> {
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

impl<T: EventFamily, R: RawMutex> Drop for Guard<T, R> {
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

impl<T: EventFamily, R: RawMutex> Default for Registry<T, R> {
    fn default() -> Self { Self::new() }
}

impl<T: EventFamily, R: RawMutex> Registry<T, R> {
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

    pub fn broadcast(&self, event: T::Event<'_>) {
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
            node.listener.accept(event.clone());
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

#[cfg(test)]
mod tests {
    extern crate std;

    use super::{Guard, Registry};
    use crate::{ByVal, Listener};
    use alloc::sync::Arc;
    use core::array;
    use core::sync::atomic::Ordering::{Acquire, Relaxed, Release};
    use core::sync::atomic::{AtomicBool, AtomicU32};
    use lock_api::{GuardSend, Mutex, RawMutex};
    use std::sync::OnceLock;
    use std::thread;

    struct TestLock<const SPIN: bool>(AtomicBool);

    unsafe impl<const SPIN: bool> RawMutex for TestLock<SPIN> {
        const INIT: Self = Self(AtomicBool::new(false));
        type GuardMarker = GuardSend;

        fn lock(&self) {
            while !self.try_lock() {
                assert!(SPIN);
                core::hint::spin_loop();
            }
        }

        fn try_lock(&self) -> bool { !self.0.swap(true, Acquire) }
        unsafe fn unlock(&self) { self.0.store(false, Release); }
    }

    type NoSpinReg = Registry<ByVal<u32>, TestLock<false>>;
    type SpinReg = Registry<ByVal<u32>, TestLock<true>>;
    type GuardSlot = Mutex<TestLock<false>, Option<Guard<ByVal<u32>, TestLock<false>>>>;

    #[derive(Default)]
    struct State {
        accept_count: AtomicU32,
        drop_count: AtomicU32,
        sum: AtomicU32,
    }

    struct Capturer(Arc<State>);

    impl Listener<ByVal<u32>> for Capturer {
        fn accept(&self, event: u32) {
            self.0.accept_count.fetch_add(1, Relaxed);
            self.0.sum.fetch_add(event, Relaxed);
        }
    }

    impl Drop for Capturer {
        fn drop(&mut self) { self.0.drop_count.fetch_add(1, Relaxed); }
    }

    #[test]
    fn normal_path() {
        let reg = NoSpinReg::new();
        let states = <[Arc<State>; 3]>::default();
        let _guards = states.each_ref().map(|x| reg.register(Capturer(x.clone())));
        reg.broadcast(3);
        reg.broadcast(4);
        assert_eq!(states.each_ref().map(|x| x.accept_count.load(Relaxed)), [2, 2, 2]);
        assert_eq!(states.each_ref().map(|x| x.sum.load(Relaxed)), [7, 7, 7]);
    }

    #[test]
    fn registry_dropped_early() {
        let state = Arc::<State>::default();
        let _guards: [_; 2];
        {
            let reg = NoSpinReg::new();
            _guards = array::from_fn(|_| reg.register(Capturer(state.clone())));
        }
        assert_eq!(state.drop_count.load(Relaxed), 2);
    }

    #[test]
    fn registry_dropped_late() {
        let state = Arc::<State>::default();
        let reg = NoSpinReg::new();
        let guards: [_; 3] = array::from_fn(|_| reg.register(Capturer(state.clone())));
        drop(guards);
        assert_eq!(state.drop_count.load(Relaxed), 3);
    }

    #[test]
    fn cancel_internal() {
        let state = Arc::<State>::default();
        let reg = NoSpinReg::new();
        let [_a, b, _c] = array::from_fn(|_| reg.register(Capturer(state.clone())));
        drop(b);
        assert_eq!(state.drop_count.load(Relaxed), 1);
        reg.broadcast(5);
        assert_eq!(state.sum.load(Relaxed), 10);
    }

    #[test]
    fn register_inside_callback() {
        let reg = Arc::<NoSpinReg>::default();
        let state = Arc::<State>::default();
        let _g = reg.register({
            let (reg, state, capturer) = (Arc::downgrade(&reg), state.clone(), OnceLock::new());
            move |_: u32| {
                capturer.get_or_init(|| {
                    let reg = reg.upgrade().unwrap();
                    reg.register(Capturer(state.clone()))
                });
            }
        });
        reg.broadcast(0);
        assert_eq!(state.accept_count.load(Relaxed), 0);
        reg.broadcast(0);
        assert_eq!(state.accept_count.load(Relaxed), 1);
    }

    #[test]
    fn self_cancel() {
        let reg = NoSpinReg::new();
        let state = Arc::<State>::default();
        let guard = Arc::<GuardSlot>::default();
        *guard.lock() = Some(reg.register({
            let (state, me) = (state.clone(), guard.clone());
            move |_| {
                *me.lock() = None;
                // Access listener state after self-cancel.
                state.accept_count.fetch_add(1, Relaxed);
            }
        }));
        reg.broadcast(0);
        assert_eq!(state.accept_count.load(Relaxed), 1);
        reg.broadcast(0);
        assert_eq!(state.accept_count.load(Relaxed), 1);
    }

    #[test]
    fn cancel_in_flight() {
        let reg = NoSpinReg::new();
        let state = Arc::<State>::default();
        let victim = GuardSlot::new(Some(reg.register(Capturer(state.clone()))));
        let _g = reg.register(move |_| *victim.lock() = None);
        reg.broadcast(0);
        assert_eq!(state.accept_count.load(Relaxed), 0);
        assert_eq!(state.drop_count.load(Relaxed), 1);
        reg.broadcast(0);
        assert_eq!(state.accept_count.load(Relaxed), 0);
    }

    #[test]
    fn destructor_cancel_in_flight() {
        struct Saboteur {
            me: Arc<GuardSlot>,
            victim: Arc<GuardSlot>,
        }

        impl Listener<ByVal<u32>> for Saboteur {
            fn accept(&self, _: u32) { *self.me.lock() = None; }
        }

        impl Drop for Saboteur {
            fn drop(&mut self) { *self.victim.lock() = None; }
        }

        let reg: NoSpinReg = Registry::new();
        let state = Arc::<State>::default();
        let a = Arc::new(GuardSlot::new(Some(reg.register(Capturer(state.clone())))));
        let b = Arc::<GuardSlot>::default();
        *b.lock() = Some(reg.register(Saboteur { me: b.clone(), victim: a.clone() }));
        reg.broadcast(7);
        assert_eq!(state.accept_count.load(Relaxed), 1);
        assert_eq!(state.sum.load(Relaxed), 7);
        assert_eq!(state.drop_count.load(Relaxed), 1);
    }

    #[test]
    fn cancel_other_in_registry_destructor() {
        struct Saboteur {
            victim: Arc<GuardSlot>,
        }

        impl Listener<ByVal<u32>> for Saboteur {
            fn accept(&self, _: u32) {}
        }

        impl Drop for Saboteur {
            fn drop(&mut self) { *self.victim.lock() = None; }
        }

        let state = Arc::<State>::default();
        let a = Arc::<GuardSlot>::default();
        let b = {
            let reg = NoSpinReg::new();
            *a.lock() = Some(reg.register(Capturer(state.clone())));
            reg.register(Saboteur { victim: a.clone() })
        };
        assert_eq!(state.drop_count.load(Relaxed), 1);
        assert!(a.lock().is_none());
        drop(b);
        assert_eq!(state.drop_count.load(Relaxed), 1);
    }

    #[test]
    fn cancel_self_in_registry_destructor() {
        struct SelfCanceller {
            me: Arc<GuardSlot>,
            state: Arc<State>,
        }

        impl Listener<ByVal<u32>> for SelfCanceller {
            fn accept(&self, _: u32) {}
        }

        impl Drop for SelfCanceller {
            fn drop(&mut self) {
                *self.me.lock() = None;
                // Access listener state after self-cancel.
                self.state.drop_count.fetch_add(1, Relaxed);
            }
        }

        let state = Arc::<State>::default();
        let me = Arc::<GuardSlot>::default();
        {
            let reg = NoSpinReg::new();
            *me.lock() = Some(reg.register(SelfCanceller { me: me.clone(), state: state.clone() }));
        }
        assert_eq!(state.drop_count.load(Relaxed), 1);
        assert!(me.lock().is_none());
    }

    #[test]
    fn nested_broadcast_is_safe() {
        let reg = Arc::<NoSpinReg>::default();
        let state = Arc::<State>::default();
        let _g = reg.register({
            let (reg, state, depth) = (Arc::downgrade(&reg), state.clone(), AtomicU32::new(0));
            move |event: u32| {
                state.accept_count.fetch_add(1, Relaxed);
                if depth.fetch_add(1, Relaxed) < 2 {
                    reg.upgrade().unwrap().broadcast(event);
                }
            }
        });
        reg.broadcast(0);
        assert_eq!(state.accept_count.load(Relaxed), 3);
    }

    #[test]
    fn concurrent_broadcast() {
        let reg = Arc::<SpinReg>::default();
        let state = Arc::<State>::default();
        let _guards: [_; 4] = array::from_fn(|_| reg.register(Capturer(state.clone())));
        thread::scope(|scope| {
            for _ in 0..8 {
                let reg = reg.clone();
                scope.spawn(move || reg.broadcast(1));
            }
        });
        assert_eq!(state.accept_count.load(Relaxed), 32);
        assert_eq!(state.sum.load(Relaxed), 32);
    }

    #[test]
    fn concurrent_register_cancel_broadcast() {
        let reg = Arc::<SpinReg>::default();
        let state = Arc::<State>::default();
        thread::scope(|scope| {
            for _ in 0..5 {
                let reg = reg.clone();
                scope.spawn(move || {
                    for _ in 0..5 {
                        reg.broadcast(1);
                    }
                });
            }
            for _ in 0..5 {
                let (reg, state) = (reg.clone(), state.clone());
                scope.spawn(move || {
                    for _ in 0..5 {
                        let guard = reg.register(Capturer(state.clone()));
                        scope.spawn(move || drop(guard));
                    }
                });
            }
        });
        assert_eq!(state.drop_count.load(Relaxed), 25);
    }

    #[test]
    fn concurrent_registry_guard_drop() {
        let state = Arc::<State>::default();
        let reg = SpinReg::new();
        let guards: [_; 16] = array::from_fn(|_| reg.register(Capturer(state.clone())));
        thread::scope(|s| {
            s.spawn(move || drop(reg));
            s.spawn(move || drop(guards));
        });
        assert_eq!(state.drop_count.load(Relaxed), 16);
    }
}
