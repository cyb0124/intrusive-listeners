use alloc::rc::{Rc, Weak};
use core::cell::Cell;
use core::marker::{PhantomData, PhantomPinned};
use core::mem::forget;
use core::pin::Pin;
use core::ptr::{self, DynMetadata, null};

pub trait Listener<T> {
    fn accept(&self, event: &T);
}

impl<T, F: Fn(&T)> Listener<T> for F {
    fn accept(&self, event: &T) { self(event) }
}

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

#[cfg(test)]
mod tests {
    extern crate std;

    use super::{Listener, Registry};
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
                state.accept_count.update(|x| x + 1);
                me.set(None);
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
