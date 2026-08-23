# intrusive-listeners

A compact reentrant type-erased event-listener registry for `no_std` with `alloc`.

A `Registry<T>` holds an intrusive list of variable-sized
listeners for events of type `T::Event`, linked by thin pointers. Each listener
is registered through a pinned `&Registry`, where it is moved into a single heap
allocation, put into the list, and referred to by a single-word handle.

The handle is returned as a `Guard` by `register`.
It is an RAII scope guard that unregisters its listener when dropped. The
cancellation is scan-free O(1) thanks to the intrusiveness. A guard may
safely outlive its registry, in which case dropping it does nothing.

Events can be passed to listeners either by value with `ByVal<T>` (cloned
per listener) or by reference with `ByRef<T>`. You can also implement the
`EventFamily` yourself for event types that borrow from the sender's stack.

Some features of each registry can be selected at compile time via the `Policy` trait.
The library also provides a `Future` adapter that yields the immediate next event
for use by async code, but will not provide a `Stream` adapter since there is no
queue or backpressure.

## Reentrancy

This implementation tolerates all kinds of recursive update scenarios.
The expected behaviors are listed below.

- **Listener cancelling itself in its callback**\
  The in-progress call will run to completion and will be destructed afterwards.
- **Listener cancelling other listeners**\
  Cancelled listeners further down the queue will not receive the in-flight event.
- **Listener calling `broadcast` again in its callback (recursive notification)**\
  Callbacks for the new event will immediately run inside the nested `broadcast` call.
- **Registering new listeners inside a listener callback**\
  With `LIFO` (default) ordering, the new listeners will not receive the in-flight event.
  Otherwise (`FIFO`), they will.
- **Accessing the registry in listener's destructor**\
  Listener's destructor may freely register, broadcast, or cancel any listener, including itself.

## Cleaning up empty registry

A `last_listener_cancelled` callback can be
provided to a registry: it will be called when cancelling the last listener leaves the
registry empty. It is meant as a clean-up hook: when a registry lives in a parent data
structure, the hook lets you remove the now-unused registry from that structure.
For the multithreaded variant, `try_seal` is provided to
atomically confirm it is still empty and disable further registration.

## Caveats

- This crate requires a nightly compiler (needed for accessing vtable pointers).
- Does not support panic-unwind. Although unwinding shouldn't cause UB, it will cause leaks and deadlocks.
