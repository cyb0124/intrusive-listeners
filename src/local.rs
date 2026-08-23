use crate::{ALIVE, EventFamily, LIFO, Listener, Ordering, RECURSIVE_CANCEL, RECURSIVE_VISIT};
use alloc::boxed::Box;
use core::cell::{Cell, UnsafeCell};
use core::future::Future;
use core::marker::{PhantomData, PhantomPinned};
use core::mem::{self, ManuallyDrop};
use core::pin::Pin;
use core::ptr::{self, DynMetadata, NonNull, null};
use core::task::{Context, LocalWaker, Poll};

pub trait Policy {
    type Ordering: Ordering;

    /// Called when the last listener is cancelled (i.e. registry just became empty).
    /// Dropping the registry in this function is allowed, but doing so will invalidate `this`;
    /// hence it is a `NonNull` instead of a `&self`.
    fn last_listener_cancelled(this: NonNull<Self>);
}

impl Policy for () {
    type Ordering = LIFO;
    fn last_listener_cancelled(_: NonNull<Self>) {}
}

#[repr(C, align(2))]
pub struct Registry<T: EventFamily, P: Policy = ()> {
    head: Cell<*const ()>,
    tail: Cell<<P::Ordering as Ordering>::Tail>,
    pub policy: P,
    _p: PhantomData<for<'a> fn(T::Event<'a>)>,
    _pin: PhantomPinned,
}

#[must_use]
pub struct Guard<T: EventFamily, P: Policy = ()> {
    node: NonNull<()>,
    _p: PhantomData<*const Registry<T, P>>,
}

#[repr(C, align(2))]
struct Node<T: EventFamily, L: Listener<T> + ?Sized> {
    meta: DynMetadata<dyn Listener<T>>,
    /// At head, points back to the registry with LSB=1.
    prev: Cell<*const ()>,
    /// At tail, points back to the registry with LSB=1.
    next: Cell<*const ()>,
    state: Cell<usize>,
    listener: ManuallyDrop<L>,
}

unsafe fn resolve<T: EventFamily>(thin: *const ()) -> *const Node<T, dyn Listener<T>> {
    let meta = unsafe { *thin.cast::<DynMetadata<dyn Listener<T>>>() };
    ptr::from_raw_parts::<Node<T, dyn Listener<T>>>(thin, meta)
}

impl<T: EventFamily> Node<T, dyn Listener<T>> {
    /// Return the registry iff it just became empty.
    unsafe fn unlink<O: Ordering>(&self) -> *const () {
        let (prev, next) = (self.prev.get(), self.next.get());
        let is_tail = next.addr() & 1 == 1;
        let mut registry = null();
        if prev.addr() & 1 == 1 {
            registry = prev.map_addr(|x| x & !1);
            unsafe { (*registry.cast::<Cell<*const ()>>()).set(if is_tail { null() } else { next }) };
        } else {
            unsafe { (*resolve::<T>(prev)).next.set(next) };
        }
        if !is_tail {
            unsafe { (*resolve::<T>(next)).prev.set(prev) };
            registry = null();
        } else if O::FIFO {
            let tail = unsafe { &*next.map_addr(|x| x & !1).cast::<Cell<*const ()>>().add(1) };
            tail.set(if registry.is_null() { prev } else { null() });
        }
        registry
    }
}

impl<T: EventFamily, P: Policy> Drop for Registry<T, P> {
    fn drop(&mut self) {
        self.walk(|thin| {
            let node = unsafe { &mut *resolve::<T>(thin).cast_mut() };
            *node.state.get_mut() = RECURSIVE_VISIT;
            *node.next.get_mut()
        });
        self.walk(|thin| {
            let ptr = unsafe { resolve::<T>(thin) }.cast_mut();
            let next = unsafe { *(*ptr).next.get_mut() };
            unsafe { ManuallyDrop::drop(&mut (*ptr).listener) };
            let state = unsafe { (*ptr).state.get_mut() };
            if *state & RECURSIVE_CANCEL != 0 {
                drop(unsafe { Box::from_raw(ptr) });
            } else {
                *state = 0;
            }
            next
        });
    }
}

impl<T: EventFamily, P: Policy> Guard<T, P> {
    /// Obtain the pointer to the listener. It may be already destructed if
    /// [`is_alive`](Self::is_alive) returns false, but the allocation stays valid.
    pub fn as_ptr(&self) -> NonNull<dyn Listener<T>> {
        let node = unsafe { resolve::<T>(self.node.as_ptr()) };
        unsafe { NonNull::new_unchecked(&raw const (*node).listener as *mut dyn Listener<T>) }
    }

    /// Whether the listener hasn't been (or about to be) destructed yet.
    pub fn is_alive(&self) -> bool { unsafe { (*resolve::<T>(self.node.as_ptr())).state.get() & ALIVE != 0 } }
}

impl<T: EventFamily, P: Policy> Drop for Guard<T, P> {
    /// May overlap listener destructor.
    fn drop(&mut self) {
        let ptr = unsafe { resolve::<T>(self.node.as_ptr()) };
        let state = unsafe { (*ptr).state.get() };
        if state & !(RECURSIVE_VISIT - 1) != 0 {
            unsafe { (*ptr).state.set(state | RECURSIVE_CANCEL) };
        } else if state & ALIVE != 0 {
            let node = unsafe { &mut *ptr.cast_mut() };
            let registry = unsafe { node.unlink::<P::Ordering>() };
            if !registry.is_null() {
                P::last_listener_cancelled(NonNull::from_ref(unsafe { &(*registry.cast::<Registry<T, P>>()).policy }));
            }
            unsafe { ManuallyDrop::drop(&mut node.listener) };
            drop(unsafe { Box::from_raw(ptr.cast_mut()) });
        } else {
            drop(unsafe { Box::from_raw(ptr.cast_mut()) });
        }
    }
}

impl<T: EventFamily, P: Policy + Default> Default for Registry<T, P> {
    fn default() -> Self { Self::new(P::default()) }
}

impl<T: EventFamily, P: Policy> Registry<T, P> {
    pub fn new(policy: P) -> Self { Self { head: <_>::default(), tail: <_>::default(), policy, _p: PhantomData, _pin: PhantomPinned } }

    pub fn is_empty(&self) -> bool { self.head.get().is_null() }

    pub fn register(self: Pin<&Self>, listener: impl Listener<T> + 'static) -> Guard<T, P> {
        let old = if <P::Ordering as Ordering>::FIFO { <P::Ordering as Ordering>::from_tail(self.tail.get()) } else { self.head.get() };
        let sentinel = (&raw const *self).map_addr(|x| x | 1).cast::<()>();
        let old_as_link = if old.is_null() { sentinel } else { old };
        let (prev, next) = if <P::Ordering as Ordering>::FIFO { (old_as_link, sentinel) } else { (sentinel, old_as_link) };
        let node = Box::new(Node {
            meta: ptr::metadata(&listener as &dyn Listener<T>),
            prev: prev.into(),
            next: next.into(),
            state: Cell::new(ALIVE),
            listener: ManuallyDrop::new(listener),
        }) as Box<Node<T, dyn Listener<T>>>;
        let thin = Box::into_raw(node).cast_const().to_raw_parts().0;
        if <P::Ordering as Ordering>::FIFO {
            if old.is_null() { &self.head } else { &unsafe { &*resolve::<T>(old) }.next }.set(thin);
            self.tail.set(<P::Ordering as Ordering>::into_tail(thin));
        } else {
            self.head.set(thin);
            if !old.is_null() {
                unsafe { &*resolve::<T>(old) }.prev.set(thin);
            }
        }
        Guard { node: unsafe { NonNull::new_unchecked(thin.cast_mut()) }, _p: PhantomData }
    }

    fn walk(&self, mut f: impl FnMut(*const ()) -> *const ()) {
        let mut thin = self.head.get();
        if !thin.is_null() {
            loop {
                thin = f(thin);
                if thin.addr() & 1 == 1 {
                    break;
                }
            }
        }
    }

    pub fn broadcast(&self, event: T::Event<'_>) {
        let mut deferred_cancels = null::<()>();
        self.walk(|thin| {
            let node = unsafe { &*resolve::<T>(thin) };
            let mut state = node.state.get();
            if state & RECURSIVE_CANCEL != 0 {
                // Node already cancelled by an outer `accept` call. Outermost `broadcast` will unlink it.
                return node.next.get();
            }
            node.state.set(state + RECURSIVE_VISIT);
            node.listener.accept(event.clone());
            let next = node.next.get();
            state = node.state.get() - RECURSIVE_VISIT;
            if state & !(RECURSIVE_CANCEL - 1) == RECURSIVE_CANCEL {
                unsafe { node.unlink::<P::Ordering>() };
                node.next.set(deferred_cancels);
                deferred_cancels = thin;
            } else {
                node.state.set(state);
            }
            next
        });
        if !deferred_cancels.is_null() && self.is_empty() {
            P::last_listener_cancelled(NonNull::from_ref(&self.policy));
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

impl<E: 'static, T: for<'a> EventFamily<Event<'a> = E> + 'static, P: Policy> Registry<T, P> {
    /// Return a future that holds a listener to wait for the immediate next event.
    /// The future will resolve to the event, or `None` if the registry is dropped.
    pub fn next(self: Pin<&Self>) -> Next<E, T, P> {
        Next { guard: self.register(NextListener(ManuallyDrop::new(UnsafeCell::new(NextState::Init)))), _p: PhantomData }
    }
}

/// See [`Registry::next`].
pub struct Next<E, T: EventFamily, P: Policy> {
    guard: Guard<T, P>,
    _p: PhantomData<E>,
}

enum NextState<E> {
    Init,
    Wait(LocalWaker),
    Ready(E),
    Dead,
}

#[repr(transparent)]
struct NextListener<E>(ManuallyDrop<UnsafeCell<NextState<E>>>);

impl<E, T: for<'a> EventFamily<Event<'a> = E> + 'static> Listener<T> for NextListener<E> {
    fn accept(&self, event: E) {
        let state = unsafe { &mut *self.0.get() };
        if matches!(*state, NextState::Init | NextState::Wait(_)) {
            if let NextState::Wait(waker) = mem::replace(state, NextState::Ready(event)) {
                waker.wake();
            }
        }
    }
}

impl<E> Drop for NextListener<E> {
    fn drop(&mut self) {
        let state = self.0.get_mut();
        if matches!(*state, NextState::Init | NextState::Wait(_)) {
            if let NextState::Wait(waker) = mem::replace(state, NextState::Dead) {
                waker.wake();
            }
        }
    }
}

impl<E, T: EventFamily, P: Policy> Drop for Next<E, T, P> {
    fn drop(&mut self) { let _defer = mem::replace(unsafe { self.guard.as_ptr().cast::<NextState<E>>().as_mut() }, NextState::Dead); }
}

impl<E, T: for<'a> EventFamily<Event<'a> = E> + 'static, P: Policy> Future for Next<E, T, P> {
    type Output = Option<E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<E>> {
        let mut state = self.guard.as_ptr().cast::<NextState<E>>();
        let waker = matches!(unsafe { state.as_ref() }, NextState::Init | NextState::Wait(_)).then(|| cx.local_waker().clone());
        // State may have changed if the waker's `clone` impl for whatever reason touches the registry.
        let state = unsafe { state.as_mut() };
        if matches!(*state, NextState::Init | NextState::Wait(_)) {
            let _defer = mem::replace(state, NextState::Wait(unsafe { waker.unwrap_unchecked() }));
            Poll::Pending
        } else {
            Poll::Ready(if let NextState::Ready(event) = mem::replace(state, NextState::Dead) { Some(event) } else { None })
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate std;

    use super::{Guard, Policy, Registry};
    use crate::{ByVal, FIFO, LIFO, Listener};
    use alloc::rc::{Rc, Weak};
    use alloc::vec::Vec;
    use core::array;
    use core::cell::{Cell, OnceCell, RefCell};
    use core::pin::{Pin, pin};
    use core::ptr::NonNull;

    struct CounterPolicy(Rc<Cell<u32>>);

    impl Policy for CounterPolicy {
        type Ordering = LIFO;
        fn last_listener_cancelled(this: NonNull<Self>) { unsafe { this.as_ref() }.0.update(|x| x + 1); }
    }

    struct FifoPolicy;

    impl Policy for FifoPolicy {
        type Ordering = FIFO;
        fn last_listener_cancelled(_: NonNull<Self>) {}
    }

    #[test]
    fn guard_drop_notifies_policy() {
        let ctr = Rc::new(Cell::new(0u32));
        let reg = pin!(Registry::<ByVal<u32>, _>::new(CounterPolicy(ctr.clone())));
        let reg = reg.as_ref();
        let a = reg.register(|_| ());
        let b = reg.register(|_| ());
        drop(a);
        assert_eq!(ctr.get(), 0);
        drop(b);
        assert_eq!(ctr.get(), 1);
        let c = reg.register(|_| ());
        drop(c);
        assert_eq!(ctr.get(), 2);
    }

    #[test]
    fn self_cancel_notifies_policy() {
        let ctr = Rc::new(Cell::new(0u32));
        let reg = pin!(Registry::<ByVal<u32>, _>::new(CounterPolicy(ctr.clone())));
        let reg = reg.as_ref();
        let guard = Rc::new(Cell::new(None));
        guard.set(Some(reg.register({
            let me = guard.clone();
            move |_: u32| me.set(None)
        })));
        reg.broadcast(0);
        assert_eq!(ctr.get(), 1);
    }

    #[test]
    fn policy_observes_empty_before_destructor_reregisters() {
        type Reg = Registry<ByVal<u32>, Observer>;
        struct Observer {
            reg: OnceCell<Weak<Reg>>,
            empty_observed: Rc<Cell<bool>>,
        }
        impl Policy for Observer {
            type Ordering = LIFO;
            fn last_listener_cancelled(this: NonNull<Self>) {
                let this = unsafe { this.as_ref() };
                let reg = this.reg.get().unwrap().upgrade().unwrap();
                this.empty_observed.set(reg.is_empty());
            }
        }
        struct Reregisterer {
            reg: Weak<Reg>,
            child: Rc<OnceCell<Guard<ByVal<u32>, Observer>>>,
        }
        impl Listener<ByVal<u32>> for Reregisterer {
            fn accept(&self, _: u32) {}
        }
        impl Drop for Reregisterer {
            fn drop(&mut self) {
                self.child.get_or_init(|| {
                    let reg = self.reg.upgrade().unwrap();
                    unsafe { Pin::new_unchecked(&*reg) }.register(|_| {})
                });
            }
        }
        let empty_observed = Rc::new(Cell::new(false));
        let reg = Rc::new(Reg::new(Observer { reg: OnceCell::new(), empty_observed: empty_observed.clone() }));
        reg.policy.reg.get_or_init(|| Rc::downgrade(&reg));
        let child = Rc::new(OnceCell::new());
        let guard = unsafe { Pin::new_unchecked(&*reg) }.register(Reregisterer { reg: Rc::downgrade(&reg), child: child.clone() });
        drop(guard);
        assert_eq!(empty_observed.get(), true);
        assert!(!reg.is_empty());
        empty_observed.set(false);
        drop(child);
        assert_eq!(empty_observed.get(), true);
        assert!(reg.is_empty());
    }

    #[derive(Default)]
    struct State {
        accept_count: Cell<u32>,
        drop_count: Cell<u32>,
        sum: Cell<u32>,
    }

    struct Capturer(Rc<State>);

    impl Listener<ByVal<u32>> for Capturer {
        fn accept(&self, event: u32) {
            self.0.accept_count.update(|x| x + 1);
            self.0.sum.update(|x| x + event);
        }
    }

    impl Drop for Capturer {
        fn drop(&mut self) { self.0.drop_count.update(|x| x + 1); }
    }

    #[test]
    fn is_empty() {
        let reg = pin!(Registry::<ByVal<u32>>::default());
        let reg = reg.as_ref();
        assert!(reg.is_empty());
        let a = reg.register(Capturer(Rc::default()));
        assert!(!reg.is_empty());
        let b = reg.register(Capturer(Rc::default()));
        drop(a);
        assert!(!reg.is_empty());
        drop(b);
        assert!(reg.is_empty());
    }

    #[test]
    fn normal_path() {
        let reg = pin!(Registry::<ByVal<u32>>::default());
        let reg = reg.as_ref();
        let states = <[Rc<State>; 3]>::default();
        let _guards = states.each_ref().map(|x| reg.register(Capturer(x.clone())));
        reg.broadcast(3);
        reg.broadcast(4);
        assert_eq!(states.each_ref().map(|x| x.accept_count.get()), [2, 2, 2]);
        assert_eq!(states.each_ref().map(|x| x.sum.get()), [7, 7, 7]);
    }

    #[test]
    fn registry_dropped_early() {
        let state = Rc::<State>::default();
        let _guards: [_; 2];
        {
            let reg = pin!(Registry::<ByVal<u32>>::default());
            let reg = reg.as_ref();
            _guards = array::from_fn(|_| reg.register(Capturer(state.clone())));
        }
        assert_eq!(state.drop_count.get(), 2);
    }

    #[test]
    fn registry_dropped_late() {
        let state = Rc::<State>::default();
        let reg = pin!(Registry::<ByVal<u32>>::default());
        let reg = reg.as_ref();
        let guards: [_; 3] = array::from_fn(|_| reg.register(Capturer(state.clone())));
        drop(guards);
        assert_eq!(state.drop_count.get(), 3);
    }

    #[test]
    fn cancel_internal() {
        let state = Rc::<State>::default();
        let reg = pin!(Registry::<ByVal<u32>>::default());
        let reg = reg.as_ref();
        let [_a, b, _c] = array::from_fn(|_| reg.register(Capturer(state.clone())));
        drop(b);
        assert_eq!(state.drop_count.get(), 1);
        reg.broadcast(5);
        assert_eq!(state.sum.get(), 10);
    }

    #[test]
    fn register_inside_callback() {
        let reg = Rc::<Registry<ByVal<u32>>>::default();
        let state = Rc::<State>::default();
        let _g = unsafe { Pin::new_unchecked(&*reg) }.register({
            let (reg, state, capturer) = (Rc::downgrade(&reg), state.clone(), OnceCell::new());
            move |_| {
                capturer.get_or_init(|| {
                    let reg = reg.upgrade().unwrap();
                    unsafe { Pin::new_unchecked(&*reg) }.register(Capturer(state.clone()))
                });
            }
        });
        reg.broadcast(0);
        assert_eq!(state.accept_count.get(), 0);
        reg.broadcast(0);
        assert_eq!(state.accept_count.get(), 1);
    }

    #[test]
    fn self_cancel() {
        let reg = pin!(Registry::<ByVal<u32>>::default());
        let reg = reg.as_ref();
        let state = Rc::<State>::default();
        let guard = Rc::new(Cell::new(None));
        guard.set(Some(reg.register({
            let (state, me) = (state.clone(), guard.clone());
            move |_| {
                me.set(None);
                // Access listener state after self-cancel.
                state.accept_count.update(|x| x + 1);
            }
        })));
        reg.broadcast(0);
        assert_eq!(state.accept_count.get(), 1);
        reg.broadcast(0);
        assert_eq!(state.accept_count.get(), 1);
    }

    #[test]
    fn cancel_in_flight() {
        let reg = pin!(Registry::<ByVal<u32>>::default());
        let reg = reg.as_ref();
        let state = Rc::<State>::default();
        let victim = Cell::new(Some(reg.register(Capturer(state.clone()))));
        let _g = reg.register(move |_| victim.set(None));
        reg.broadcast(0);
        assert_eq!(state.accept_count.get(), 0);
        assert_eq!(state.drop_count.get(), 1);
        reg.broadcast(0);
        assert_eq!(state.accept_count.get(), 0);
    }

    #[test]
    fn destructor_cancel_in_flight() {
        struct Saboteur {
            me: Rc<Cell<Option<Guard<ByVal<u32>>>>>,
            victim: Rc<Cell<Option<Guard<ByVal<u32>>>>>,
        }

        impl Listener<ByVal<u32>> for Saboteur {
            fn accept(&self, _: u32) { self.me.set(None); }
        }

        impl Drop for Saboteur {
            fn drop(&mut self) { self.victim.set(None); }
        }

        let reg = pin!(Registry::<ByVal<u32>>::default());
        let reg = reg.as_ref();
        let state = Rc::<State>::default();
        let a = Rc::new(Cell::new(Some(reg.register(Capturer(state.clone())))));
        let b = Rc::new(Cell::new(None));
        b.set(Some(reg.register(Saboteur { me: b.clone(), victim: a.clone() })));
        reg.broadcast(7);
        assert_eq!(state.accept_count.get(), 1);
        assert_eq!(state.sum.get(), 7);
        assert_eq!(state.drop_count.get(), 1);
    }

    #[test]
    fn cancel_other_in_registry_destructor() {
        struct Saboteur {
            victim: Rc<Cell<Option<Guard<ByVal<u32>>>>>,
        }

        impl Listener<ByVal<u32>> for Saboteur {
            fn accept(&self, _: u32) {}
        }

        impl Drop for Saboteur {
            fn drop(&mut self) { self.victim.set(None); }
        }

        let state = Rc::<State>::default();
        let a = Rc::new(Cell::new(None));
        let b = {
            let reg = pin!(Registry::<ByVal<u32>>::default());
            let reg = reg.as_ref();
            a.set(Some(reg.register(Capturer(state.clone()))));
            reg.register(Saboteur { victim: a.clone() })
        };
        assert_eq!(state.drop_count.get(), 1);
        assert!(a.take().is_none());
        drop(b);
        assert_eq!(state.drop_count.get(), 1);
    }

    #[test]
    fn cancel_self_in_registry_destructor() {
        struct SelfCanceller {
            me: Rc<Cell<Option<Guard<ByVal<u32>>>>>,
            state: Rc<State>,
        }

        impl Listener<ByVal<u32>> for SelfCanceller {
            fn accept(&self, _: u32) {}
        }

        impl Drop for SelfCanceller {
            fn drop(&mut self) {
                self.me.set(None);
                // Access listener state after self-cancel.
                self.state.drop_count.update(|x| x + 1);
            }
        }

        let state = Rc::<State>::default();
        let me = Rc::new(Cell::new(None));
        {
            let reg = pin!(Registry::<ByVal<u32>>::default());
            let reg = reg.as_ref();
            me.set(Some(reg.register(SelfCanceller { me: me.clone(), state: state.clone() })));
        }
        assert_eq!(state.drop_count.get(), 1);
        assert!(me.take().is_none());
    }

    #[test]
    fn self_cancel_then_nested_broadcast_lifo() { self_cancel_then_nested_broadcast(()); }

    #[test]
    fn self_cancel_then_nested_broadcast_fifo() { self_cancel_then_nested_broadcast(FifoPolicy); }

    fn self_cancel_then_nested_broadcast<P: Policy + 'static>(policy: P) {
        let reg = Rc::new(Registry::<ByVal<u32>, P>::new(policy));
        let state = Rc::<State>::default();
        let guard = Rc::new(Cell::new(None));
        guard.set(Some(unsafe { Pin::new_unchecked(&*reg) }.register({
            let (reg, state, me) = (Rc::downgrade(&reg), state.clone(), guard.clone());
            move |depth| {
                state.accept_count.update(|x| x + 1);
                if depth == 0 {
                    me.set(None);
                    reg.upgrade().unwrap().broadcast(1);
                }
            }
        })));
        reg.broadcast(0);
        assert_eq!(state.accept_count.get(), 1);
        reg.broadcast(0);
        assert_eq!(state.accept_count.get(), 1);
    }

    #[test]
    fn nested_broadcast_is_safe() {
        let reg = Rc::<Registry<ByVal<u32>>>::default();
        let state = Rc::<State>::default();
        let _g = unsafe { Pin::new_unchecked(&*reg) }.register({
            let (reg, state, depth) = (Rc::downgrade(&reg), state.clone(), Cell::new(0u32));
            move |event| {
                state.accept_count.update(|x| x + 1);
                if depth.replace(depth.get() + 1) < 2 {
                    reg.upgrade().unwrap().broadcast(event);
                }
            }
        });
        reg.broadcast(0);
        assert_eq!(state.accept_count.get(), 3);
    }

    #[test]
    fn lifo_broadcast() {
        let reg = pin!(Registry::<ByVal<u32>>::default());
        let reg = reg.as_ref();
        let order = Rc::new(RefCell::new(Vec::new()));
        let _g: [_; 3] = array::from_fn(|i| {
            let order = order.clone();
            reg.register(move |_| order.borrow_mut().push(i))
        });
        reg.broadcast(0);
        assert_eq!(*order.borrow(), [2, 1, 0]);
    }

    #[test]
    fn fifo_broadcast() {
        let reg = pin!(Registry::<ByVal<u32>, _>::new(FifoPolicy));
        let reg = reg.as_ref();
        let order = Rc::new(RefCell::new(Vec::new()));
        let mut guards: [_; 5] = array::from_fn(|i| {
            let order = order.clone();
            Some(reg.register(move |_| order.borrow_mut().push(i)))
        });
        guards[2] = None;
        reg.broadcast(0);
        assert_eq!(*order.borrow(), [0, 1, 3, 4]);
        guards[4] = None;
        guards[0] = None;
        let _g = reg.register({
            let order = order.clone();
            move |_| order.borrow_mut().push(9)
        });
        order.borrow_mut().clear();
        reg.broadcast(0);
        assert_eq!(*order.borrow(), [1, 3, 9]);
    }

    #[test]
    fn fifo_reentrant_register_receives_inflight() {
        let reg = Rc::new(Registry::<ByVal<u32>, FifoPolicy>::new(FifoPolicy));
        let state = Rc::<State>::default();
        let _g = unsafe { Pin::new_unchecked(&*reg) }.register({
            let (reg, state, capturer) = (Rc::downgrade(&reg), state.clone(), OnceCell::new());
            move |_| {
                capturer.get_or_init(|| {
                    let reg = reg.upgrade().unwrap();
                    unsafe { Pin::new_unchecked(&*reg) }.register(Capturer(state.clone()))
                });
            }
        });
        reg.broadcast(0);
        assert_eq!(state.accept_count.get(), 1);
        reg.broadcast(0);
        assert_eq!(state.accept_count.get(), 2);
    }

    #[test]
    fn raw_ptr_access() {
        let state = Rc::<State>::default();
        let reg = pin!(Registry::<ByVal<u32>>::default());
        let reg = reg.as_ref();
        let guard = reg.register(Capturer(state.clone()));
        assert!(guard.is_alive());
        unsafe { guard.as_ptr().as_ref() }.accept(11);
        assert_eq!(state.sum.get(), 11);
        let downcast = unsafe { guard.as_ptr().cast::<Capturer>().as_ref() };
        assert_eq!(downcast.0.accept_count.get(), 1);
        reg.broadcast(22);
        assert_eq!(state.sum.get(), 33);
        let state = Rc::<State>::default();
        let guard = {
            let reg = pin!(Registry::<ByVal<u32>>::default());
            reg.as_ref().register(Capturer(state.clone()))
        };
        assert_eq!(state.drop_count.get(), 1);
        assert!(!guard.is_alive());
    }

    #[test]
    fn is_alive_cleared_eagerly() {
        struct State {
            other: Cell<Option<Guard<ByVal<u32>>>>,
            wrong: Cell<bool>,
        }

        struct Probe(Rc<State>);

        impl Listener<ByVal<u32>> for Probe {
            fn accept(&self, _: u32) {}
        }

        impl Drop for Probe {
            fn drop(&mut self) {
                let guard = self.0.other.take().unwrap();
                self.0.wrong.set(guard.is_alive());
                self.0.other.set(Some(guard));
            }
        }

        let s0 = Rc::new(State { other: Cell::new(None), wrong: Cell::new(true) });
        let s1 = Rc::new(State { other: Cell::new(None), wrong: Cell::new(true) });
        {
            let reg = pin!(Registry::<ByVal<u32>>::default());
            let reg = reg.as_ref();
            s0.other.set(Some(reg.register(Probe(s1.clone()))));
            s1.other.set(Some(reg.register(Probe(s0.clone()))));
        }
        assert!(!s0.wrong.get() && !s1.wrong.get());
    }
}
