use crate::{ALIVE, Listener, RECURSIVE_CANCEL, RECURSIVE_VISIT};
use alloc::boxed::Box;
use core::cell::Cell;
use core::marker::{PhantomData, PhantomPinned};
use core::mem::ManuallyDrop;
use core::pin::Pin;
use core::ptr::{self, DynMetadata, null};

#[repr(align(2))]
pub struct Registry<T> {
    head: Cell<*const ()>,
    _p: PhantomData<fn(T)>,
    _pin: PhantomPinned,
}

pub struct Guard<T> {
    node: *const (),
    _p: PhantomData<fn(T)>,
}

#[repr(C, align(2))]
struct Node<T, L: Listener<T> + ?Sized> {
    meta: DynMetadata<dyn Listener<T>>,
    /// If LSB=1, it points back to the registry.
    prev: Cell<*const ()>,
    next: Cell<*const ()>,
    state: Cell<usize>,
    listener: ManuallyDrop<L>,
}

unsafe fn resolve<T>(thin: *const ()) -> *const Node<T, dyn Listener<T>> {
    let meta = unsafe { *thin.cast::<DynMetadata<dyn Listener<T>>>() };
    ptr::from_raw_parts::<Node<T, dyn Listener<T>>>(thin, meta)
}

impl<T> Node<T, dyn Listener<T>> {
    unsafe fn unlink(&self) {
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
            let node = unsafe { &mut *resolve::<T>(thin).cast_mut() };
            *node.state.get_mut() |= RECURSIVE_VISIT;
            thin = *node.next.get_mut();
        }
        thin = self.head.get();
        while !thin.is_null() {
            let ptr = unsafe { resolve::<T>(thin) }.cast_mut();
            thin = unsafe { *(*ptr).next.get_mut() };
            unsafe { ManuallyDrop::drop(&mut (*ptr).listener) };
            let state = unsafe { (*ptr).state.get_mut() };
            if *state & RECURSIVE_CANCEL != 0 {
                drop(unsafe { Box::from_raw(ptr) });
            } else {
                *state &= !ALIVE;
            }
        }
    }
}

impl<T> Drop for Guard<T> {
    /// May overlap listener destructor.
    fn drop(&mut self) {
        let ptr = unsafe { resolve::<T>(self.node) };
        let state = unsafe { (*ptr).state.get() };
        if state & ALIVE == 0 {
            drop(unsafe { Box::from_raw(ptr.cast_mut()) });
        } else if state & !(RECURSIVE_VISIT - 1) == 0 {
            let node = unsafe { &mut *ptr.cast_mut() };
            unsafe { node.unlink() };
            unsafe { ManuallyDrop::drop(&mut node.listener) };
            drop(unsafe { Box::from_raw(ptr.cast_mut()) });
        } else {
            unsafe { (*ptr).state.set(state | RECURSIVE_CANCEL) };
        }
    }
}

impl<T> Default for Registry<T> {
    fn default() -> Self { Self::new() }
}

impl<T> Registry<T> {
    pub const fn new() -> Self { Self { head: Cell::new(null()), _p: PhantomData, _pin: PhantomPinned } }

    pub fn register(self: Pin<&Self>, listener: impl Listener<T> + 'static) -> Guard<T> {
        let next = self.head.get();
        let node = Box::new(Node {
            meta: ptr::metadata(&listener as &dyn Listener<T>),
            prev: (&raw const *self).map_addr(|x| x | 1).cast::<()>().into(),
            next: next.into(),
            state: Cell::new(ALIVE),
            listener: ManuallyDrop::new(listener),
        }) as Box<Node<T, dyn Listener<T>>>;
        let thin = Box::into_raw(node).cast_const().to_raw_parts().0;
        self.head.set(thin);
        if !next.is_null() {
            unsafe { &*resolve::<T>(next) }.prev.set(thin);
        }
        Guard { node: thin, _p: PhantomData }
    }

    pub fn broadcast(&self, event: &T) {
        let mut deferred_cancels = null::<()>();
        let mut thin = self.head.get();
        while !thin.is_null() {
            let node = unsafe { &*resolve::<T>(thin) };
            let mut state = node.state.get();
            if state & RECURSIVE_CANCEL != 0 {
                // Node already cancelled by an outer `accept` call. Outermost `broadcast` will unlink it.
                thin = node.next.get();
                continue;
            }
            node.state.set(state + RECURSIVE_VISIT);
            node.listener.accept(event);
            let next = node.next.get();
            state = node.state.get() - RECURSIVE_VISIT;
            if state & !(RECURSIVE_CANCEL - 1) == RECURSIVE_CANCEL {
                unsafe { node.unlink() };
                node.next.set(deferred_cancels);
                deferred_cancels = thin;
            } else {
                node.state.set(state);
            }
            thin = next;
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

#[cfg(test)]
mod tests {
    extern crate std;

    use super::{Guard, Registry};
    use crate::Listener;
    use alloc::rc::Rc;
    use core::array;
    use core::cell::{Cell, OnceCell};
    use core::pin::{Pin, pin};

    #[derive(Default)]
    struct State {
        accept_count: Cell<u32>,
        drop_count: Cell<u32>,
        sum: Cell<u32>,
    }

    struct Capturer(Rc<State>);

    impl Listener<u32> for Capturer {
        fn accept(&self, event: &u32) {
            self.0.accept_count.update(|x| x + 1);
            self.0.sum.update(|x| x + event);
        }
    }

    impl Drop for Capturer {
        fn drop(&mut self) { self.0.drop_count.update(|x| x + 1); }
    }

    #[test]
    fn normal_path() {
        let reg = pin!(Registry::new());
        let reg = reg.as_ref();
        let states = <[Rc<State>; 3]>::default();
        let _guards = states.each_ref().map(|x| reg.register(Capturer(x.clone())));
        reg.broadcast(&3);
        reg.broadcast(&4);
        assert_eq!(states.each_ref().map(|x| x.accept_count.get()), [2, 2, 2]);
        assert_eq!(states.each_ref().map(|x| x.sum.get()), [7, 7, 7]);
    }

    #[test]
    fn registry_dropped_early() {
        let state = Rc::<State>::default();
        let _guards: [_; 2];
        {
            let reg = pin!(Registry::new());
            let reg = reg.as_ref();
            _guards = array::from_fn(|_| reg.register(Capturer(state.clone())));
        }
        assert_eq!(state.drop_count.get(), 2);
    }

    #[test]
    fn registry_dropped_late() {
        let state = Rc::<State>::default();
        let reg = pin!(Registry::new());
        let reg = reg.as_ref();
        let guards: [_; 3] = array::from_fn(|_| reg.register(Capturer(state.clone())));
        drop(guards);
        assert_eq!(state.drop_count.get(), 3);
    }

    #[test]
    fn cancel_internal() {
        let state = Rc::<State>::default();
        let reg = pin!(Registry::new());
        let reg = reg.as_ref();
        let [_a, b, _c] = array::from_fn(|_| reg.register(Capturer(state.clone())));
        drop(b);
        assert_eq!(state.drop_count.get(), 1);
        reg.broadcast(&5);
        assert_eq!(state.sum.get(), 10);
    }

    #[test]
    fn register_inside_callback() {
        let reg = Rc::<Registry<u32>>::default();
        let state = Rc::<State>::default();
        let _g = unsafe { Pin::new_unchecked(&*reg) }.register({
            let (reg, state, capturer) = (Rc::downgrade(&reg), state.clone(), OnceCell::new());
            move |_: &u32| {
                capturer.get_or_init(|| {
                    let reg = reg.upgrade().unwrap();
                    unsafe { Pin::new_unchecked(&*reg) }.register(Capturer(state.clone()))
                });
            }
        });
        reg.broadcast(&0);
        assert_eq!(state.accept_count.get(), 0);
        reg.broadcast(&0);
        assert_eq!(state.accept_count.get(), 1);
    }

    #[test]
    fn self_cancel() {
        let reg = pin!(Registry::new());
        let reg = reg.as_ref();
        let state = Rc::<State>::default();
        let guard = Rc::new(Cell::new(None));
        guard.set(Some(reg.register({
            let (state, me) = (state.clone(), guard.clone());
            move |_: &u32| {
                me.set(None);
                // Access listener state after self-cancel.
                state.accept_count.update(|x| x + 1);
            }
        })));
        reg.broadcast(&0);
        assert_eq!(state.accept_count.get(), 1);
        reg.broadcast(&0);
        assert_eq!(state.accept_count.get(), 1);
    }

    #[test]
    fn cancel_in_flight() {
        let reg = pin!(Registry::new());
        let reg = reg.as_ref();
        let state = Rc::<State>::default();
        let victim = Cell::new(Some(reg.register(Capturer(state.clone()))));
        let _g = reg.register(move |_: &u32| victim.set(None));
        reg.broadcast(&0);
        assert_eq!(state.accept_count.get(), 0);
        assert_eq!(state.drop_count.get(), 1);
        reg.broadcast(&0);
        assert_eq!(state.accept_count.get(), 0);
    }

    #[test]
    fn destructor_cancel_in_flight() {
        struct Saboteur {
            me: Rc<Cell<Option<Guard<u32>>>>,
            victim: Rc<Cell<Option<Guard<u32>>>>,
        }

        impl Listener<u32> for Saboteur {
            fn accept(&self, _: &u32) { self.me.set(None); }
        }

        impl Drop for Saboteur {
            fn drop(&mut self) { self.victim.set(None); }
        }

        let reg = pin!(Registry::new());
        let reg = reg.as_ref();
        let state = Rc::<State>::default();
        let a = Rc::new(Cell::new(Some(reg.register(Capturer(state.clone())))));
        let b = Rc::new(Cell::new(None));
        b.set(Some(reg.register(Saboteur { me: b.clone(), victim: a.clone() })));
        reg.broadcast(&7);
        assert_eq!(state.accept_count.get(), 1);
        assert_eq!(state.sum.get(), 7);
        assert_eq!(state.drop_count.get(), 1);
    }

    #[test]
    fn cancel_other_in_registry_destructor() {
        struct Saboteur {
            victim: Rc<Cell<Option<Guard<u32>>>>,
        }

        impl Listener<u32> for Saboteur {
            fn accept(&self, _: &u32) {}
        }

        impl Drop for Saboteur {
            fn drop(&mut self) { self.victim.set(None); }
        }

        let state = Rc::<State>::default();
        let a = Rc::new(Cell::new(None));
        let b = {
            let reg = pin!(Registry::new());
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
            me: Rc<Cell<Option<Guard<u32>>>>,
            state: Rc<State>,
        }

        impl Listener<u32> for SelfCanceller {
            fn accept(&self, _: &u32) {}
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
            let reg = pin!(Registry::new());
            let reg = reg.as_ref();
            me.set(Some(reg.register(SelfCanceller { me: me.clone(), state: state.clone() })));
        }
        assert_eq!(state.drop_count.get(), 1);
        assert!(me.take().is_none());
    }

    #[test]
    fn nested_broadcast_is_safe() {
        let reg = Rc::<Registry<u32>>::default();
        let state = Rc::<State>::default();
        let _g = unsafe { Pin::new_unchecked(&*reg) }.register({
            let (reg, state, depth) = (Rc::downgrade(&reg), state.clone(), Cell::new(0u32));
            move |event: &u32| {
                state.accept_count.update(|x| x + 1);
                if depth.replace(depth.get() + 1) < 2 {
                    reg.upgrade().unwrap().broadcast(event);
                }
            }
        });
        reg.broadcast(&0);
        assert_eq!(state.accept_count.get(), 3);
    }
}
