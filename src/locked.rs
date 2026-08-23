use crate::{ALIVE, EventFamily, LIFO, Listener, Ordering, RECURSIVE_CANCEL, RECURSIVE_VISIT, private};
use alloc::boxed::Box;
use core::cell::Cell;
use core::marker::PhantomData;
use core::mem::ManuallyDrop;
use core::ptr::{self, DynMetadata, NonNull, null};
use lock_api::RawMutex;

pub trait Policy {
    type Sealable: Sealable;
    type Ordering: Ordering;

    /// Called when `Sealable = Yes` and the last listener is cancelled (i.e. registry just became empty).
    /// Note that other threads may have already repopulated the registry before and during the call.
    /// Use `try_seal` to ensure it is actually empty and stays empty.
    fn last_listener_cancelled(&self);
}

impl Policy for () {
    type Sealable = No;
    type Ordering = LIFO;
    fn last_listener_cancelled(&self) {}
}

pub struct No;
pub struct Yes;

impl private::Private for No {}
impl private::Private for Yes {}

pub trait Sealable: private::Private {
    const VALUE: bool;
    type Flag: Copy + Default;
    type RegisterResult<T>;
    fn gate_register<T>(flag: Self::Flag, f: impl FnOnce() -> T) -> Self::RegisterResult<T>;
}

impl Sealable for No {
    const VALUE: bool = false;
    type Flag = ();
    type RegisterResult<T> = T;
    fn gate_register<T>((): (), f: impl FnOnce() -> T) -> T { f() }
}

impl Sealable for Yes {
    const VALUE: bool = true;
    type Flag = bool;
    type RegisterResult<T> = Option<T>;
    fn gate_register<T>(flag: bool, f: impl FnOnce() -> T) -> Option<T> { (!flag).then(f) }
}

pub struct Registry<T: EventFamily, R: RawMutex, P: Policy = ()> {
    inner: NonNull<Inner<T, R, P>>,
}

#[must_use]
pub struct Guard<T: EventFamily, R: RawMutex, P: Policy = ()> {
    node: NonNull<()>,
    _p: PhantomData<Inner<T, R, P>>,
}

struct Inner<T: EventFamily, R: RawMutex, P: Policy> {
    lock: R,
    sealed: Cell<<P::Sealable as Sealable>::Flag>,
    ref_count: Cell<usize>,
    head: Cell<*const ()>,
    tail: Cell<<P::Ordering as Ordering>::Tail>,
    policy: P,
    _p: PhantomData<for<'a> fn(T::Event<'a>)>,
}

unsafe impl<T: EventFamily, R: RawMutex + Send + Sync, P: Policy + Send + Sync> Send for Registry<T, R, P> {}
unsafe impl<T: EventFamily, R: RawMutex + Send + Sync, P: Policy + Send + Sync> Sync for Registry<T, R, P> {}
unsafe impl<T: EventFamily, R: RawMutex + Send + Sync, P: Policy + Send + Sync> Send for Guard<T, R, P> {}
unsafe impl<T: EventFamily, R: RawMutex, P: Policy> Sync for Guard<T, R, P> {}

#[repr(C)]
struct Node<T: EventFamily, L: Listener<T> + ?Sized> {
    meta: DynMetadata<dyn Listener<T>>,
    parent: NonNull<()>,
    prev: Cell<*const ()>,
    next: Cell<*const ()>,
    state: Cell<usize>,
    listener: ManuallyDrop<L>,
}

unsafe fn resolve<T: EventFamily>(thin: *const ()) -> *const Node<T, dyn Listener<T>> {
    let meta = unsafe { *thin.cast::<DynMetadata<dyn Listener<T>>>() };
    ptr::from_raw_parts::<Node<T, dyn Listener<T>>>(thin, meta)
}

impl<T: EventFamily> Node<T, dyn Listener<T>> {
    unsafe fn unlink<O: Ordering>(&self, head: &Cell<*const ()>, tail: &Cell<O::Tail>) {
        let (prev, next) = (self.prev.get(), self.next.get());
        if prev.is_null() {
            head.set(next);
        } else {
            unsafe { (*resolve::<T>(prev)).next.set(next) };
        }
        if next.is_null() {
            tail.set(O::into_tail(prev));
        } else {
            unsafe { (*resolve::<T>(next)).prev.set(prev) };
        }
    }
}

impl<T: EventFamily, R: RawMutex, P: Policy> Drop for Registry<T, R, P> {
    fn drop(&mut self) {
        let inner = unsafe { self.inner.as_ref() };
        inner.lock.lock();
        let mut thin = inner.head.get();
        while !thin.is_null() {
            let node = unsafe { &mut *resolve::<T>(thin).cast_mut() };
            *node.state.get_mut() = RECURSIVE_VISIT;
            thin = *node.next.get_mut();
        }
        thin = inner.head.get();
        while !thin.is_null() {
            let ptr = unsafe { resolve::<T>(thin) }.cast_mut();
            thin = unsafe { *(*ptr).next.get_mut() };
            unsafe { inner.lock.unlock() };
            unsafe { ManuallyDrop::drop(&mut (*ptr).listener) };
            inner.lock.lock();
            let state = unsafe { (*ptr).state.get_mut() };
            if *state & RECURSIVE_CANCEL != 0 {
                drop(unsafe { Box::from_raw(ptr) });
            } else {
                *state = 0;
            }
        }
        let ref_count = inner.ref_count.get() - 1;
        if ref_count == 0 {
            // Unnecessary to unlock here.
            drop(unsafe { Box::from_raw(self.inner.as_ptr()) });
        } else {
            inner.ref_count.set(ref_count);
            unsafe { inner.lock.unlock() };
        }
    }
}

impl<T: EventFamily, R: RawMutex, P: Policy> Guard<T, R, P> {
    /// Obtain the pointer to the listener. It may be already destructed, but the allocation stays valid.
    pub fn as_ptr(&self) -> NonNull<dyn Listener<T>> {
        let node = unsafe { resolve::<T>(self.node.as_ptr()) };
        unsafe { NonNull::new_unchecked(&raw const (*node).listener as *mut dyn Listener<T>) }
    }

    /// If the listener hasn't been (or about to be) destructed yet, run the closure while keeping
    /// the listener alive by holding the registry lock, so the closure should avoid expensive
    /// operations as well as any recursive registry access (will deadlock).
    pub fn enter<U>(&self, f: impl FnOnce(&dyn Listener<T>) -> U) -> Option<U> {
        let ptr = unsafe { resolve::<T>(self.node.as_ptr()) };
        let parent = unsafe { (*ptr).parent.cast::<Inner<T, R, P>>().as_ref() };
        parent.lock.lock();
        let alive = unsafe { (*ptr).state.get() } & ALIVE != 0;
        let result = alive.then(|| f(unsafe { &*(&raw const (*ptr).listener as *mut dyn Listener<T>).cast_const() }));
        unsafe { parent.lock.unlock() };
        result
    }
}

impl<T: EventFamily, R: RawMutex, P: Policy> Drop for Guard<T, R, P> {
    /// May overlap listener destructor.
    fn drop(&mut self) {
        let ptr = unsafe { resolve::<T>(self.node.as_ptr()) };
        let parent_ptr = unsafe { (*ptr).parent }.cast::<Inner<T, R, P>>();
        let parent = unsafe { parent_ptr.as_ref() };
        parent.lock.lock();
        let state = unsafe { (*ptr).state.get() };
        let (destruct, dealloc) = if state & !(RECURSIVE_VISIT - 1) != 0 {
            unsafe { (*ptr).state.set(state | RECURSIVE_CANCEL) };
            (false, false)
        } else if state & ALIVE != 0 {
            unsafe { (*ptr).unlink::<P::Ordering>(&parent.head, &parent.tail) };
            if <P::Sealable as Sealable>::VALUE && parent.head.get().is_null() {
                unsafe { parent.lock.unlock() };
                parent.policy.last_listener_cancelled();
                parent.lock.lock();
            }
            (true, true)
        } else {
            (false, true)
        };
        let ref_count = parent.ref_count.get() - 1;
        if ref_count == 0 {
            // Unnecessary to unlock here.
            drop(unsafe { Box::from_raw(parent_ptr.as_ptr()) });
        } else {
            parent.ref_count.set(ref_count);
            unsafe { parent.lock.unlock() };
        }
        if dealloc {
            let ptr = ptr.cast_mut();
            if destruct {
                unsafe { ManuallyDrop::drop(&mut (*ptr).listener) };
            }
            drop(unsafe { Box::from_raw(ptr) });
        }
    }
}

impl<T: EventFamily, R: RawMutex, P: Policy + Default> Default for Registry<T, R, P> {
    fn default() -> Self { Self::new(P::default()) }
}

impl<T: EventFamily, R: RawMutex, P: Policy> Registry<T, R, P> {
    pub fn new(policy: P) -> Self {
        let inner = Box::new(Inner {
            lock: R::INIT,
            sealed: <_>::default(),
            ref_count: Cell::new(1),
            head: <_>::default(),
            tail: <_>::default(),
            policy,
            _p: PhantomData,
        });
        Self { inner: unsafe { NonNull::new_unchecked(Box::into_raw(inner)) } }
    }

    pub fn register(&self, listener: impl Listener<T> + Send + Sync + 'static) -> <P::Sealable as Sealable>::RegisterResult<Guard<T, R, P>> {
        let inner = unsafe { self.inner.as_ref() };
        inner.lock.lock();
        let result = <P::Sealable as Sealable>::gate_register(inner.sealed.get(), || {
            let mut node = Box::new(Node {
                meta: ptr::metadata(&listener as &dyn Listener<T>),
                parent: self.inner.cast::<()>(),
                prev: Cell::new(null()),
                next: Cell::new(null()),
                state: Cell::new(ALIVE),
                listener: ManuallyDrop::new(listener),
            }) as Box<Node<T, dyn Listener<T>>>;
            let old = if <P::Ordering as Ordering>::FIFO { <P::Ordering as Ordering>::from_tail(inner.tail.get()) } else { inner.head.get() };
            *if <P::Ordering as Ordering>::FIFO { node.prev.get_mut() } else { node.next.get_mut() } = old;
            let thin = Box::into_raw(node).cast_const().to_raw_parts().0;
            if <P::Ordering as Ordering>::FIFO {
                if old.is_null() { &inner.head } else { &unsafe { &*resolve::<T>(old) }.next }.set(thin);
                inner.tail.set(<P::Ordering as Ordering>::into_tail(thin));
            } else {
                inner.head.set(thin);
                if !old.is_null() {
                    unsafe { &*resolve::<T>(old) }.prev.set(thin);
                }
            }
            inner.ref_count.update(|x| x + 1);
            Guard { node: unsafe { NonNull::new_unchecked(thin.cast_mut()) }, _p: PhantomData }
        });
        unsafe { inner.lock.unlock() };
        result
    }

    pub fn broadcast(&self, event: T::Event<'_>) {
        let inner = unsafe { self.inner.as_ref() };
        inner.lock.lock();
        let mut deferred_cancels = null::<()>();
        let mut thin = inner.head.get();
        while !thin.is_null() {
            let node = unsafe { &*resolve::<T>(thin) };
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
                unsafe { node.unlink::<P::Ordering>(&inner.head, &inner.tail) };
                node.next.set(deferred_cancels);
                deferred_cancels = thin;
            } else {
                node.state.set(state);
            }
            thin = next;
        }
        let notify = <P::Sealable as Sealable>::VALUE && !deferred_cancels.is_null() && inner.head.get().is_null();
        unsafe { inner.lock.unlock() };
        if notify {
            inner.policy.last_listener_cancelled();
        }
        while !deferred_cancels.is_null() {
            let ptr = unsafe { resolve::<T>(deferred_cancels) }.cast_mut();
            let node = unsafe { &mut *ptr };
            deferred_cancels = *node.next.get_mut();
            unsafe { ManuallyDrop::drop(&mut node.listener) };
            drop(unsafe { Box::from_raw(ptr) });
        }
    }
}

impl<T: EventFamily, R: RawMutex, P: Policy<Sealable = Yes>> Registry<T, R, P> {
    /// Atomically check if the registry is empty, and if true, prevent it to ever become non-empty again.
    pub fn try_seal(&self) -> bool {
        let inner = unsafe { self.inner.as_ref() };
        inner.lock.lock();
        let empty = inner.head.get().is_null();
        if empty {
            inner.sealed.set(true);
        }
        unsafe { inner.lock.unlock() };
        empty
    }
}

#[cfg(test)]
mod tests {
    extern crate std;

    use super::{Guard, No, Policy, Registry};
    use crate::{ByVal, FIFO, Listener};
    use alloc::sync::Arc;
    use alloc::vec::Vec;
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

    struct FifoPolicy;

    impl Policy for FifoPolicy {
        type Sealable = No;
        type Ordering = FIFO;
        fn last_listener_cancelled(&self) {}
    }

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
        let reg = NoSpinReg::default();
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
            let reg = NoSpinReg::default();
            _guards = array::from_fn(|_| reg.register(Capturer(state.clone())));
        }
        assert_eq!(state.drop_count.load(Relaxed), 2);
    }

    #[test]
    fn registry_dropped_late() {
        let state = Arc::<State>::default();
        let reg = NoSpinReg::default();
        let guards: [_; 3] = array::from_fn(|_| reg.register(Capturer(state.clone())));
        drop(guards);
        assert_eq!(state.drop_count.load(Relaxed), 3);
    }

    #[test]
    fn cancel_internal() {
        let state = Arc::<State>::default();
        let reg = NoSpinReg::default();
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
        let reg = NoSpinReg::default();
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
        let reg = NoSpinReg::default();
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

        let reg: NoSpinReg = Registry::default();
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
            let reg = NoSpinReg::default();
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
            let reg = NoSpinReg::default();
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
        let reg = SpinReg::default();
        let guards: [_; 16] = array::from_fn(|_| reg.register(Capturer(state.clone())));
        thread::scope(|s| {
            s.spawn(move || drop(reg));
            s.spawn(move || drop(guards));
        });
        assert_eq!(state.drop_count.load(Relaxed), 16);
    }

    type OrderLog = Mutex<TestLock<false>, Vec<usize>>;

    #[test]
    fn lifo_broadcast() {
        let reg = NoSpinReg::default();
        let order = Arc::new(Mutex::<TestLock<false>, _>::new(Vec::new()));
        let _g: [_; 3] = array::from_fn(|i| {
            let order = order.clone();
            reg.register(move |_: u32| order.lock().push(i))
        });
        reg.broadcast(0);
        assert_eq!(*order.lock(), [2, 1, 0]);
    }

    #[test]
    fn fifo_broadcast() {
        let reg = Registry::<ByVal<u32>, TestLock<false>, _>::new(FifoPolicy);
        let order = Arc::new(OrderLog::new(Vec::new()));
        let mut guards: [_; 5] = array::from_fn(|i| {
            let order = order.clone();
            Some(reg.register(move |_: u32| order.lock().push(i)))
        });
        guards[2] = None;
        reg.broadcast(0);
        assert_eq!(*order.lock(), [0, 1, 3, 4]);
        guards[4] = None;
        guards[0] = None;
        let _g = reg.register({
            let order = order.clone();
            move |_: u32| order.lock().push(9)
        });
        order.lock().clear();
        reg.broadcast(0);
        assert_eq!(*order.lock(), [1, 3, 9]);
    }

    #[test]
    fn fifo_concurrent_register_cancel_broadcast() {
        let reg = Arc::new(Registry::<ByVal<u32>, TestLock<true>, _>::new(FifoPolicy));
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
}
