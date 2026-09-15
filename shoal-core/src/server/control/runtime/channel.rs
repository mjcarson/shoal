//! A bounded multi producer, single consumer channel for one thread
//!
//! glommio's own local channel has a sender that cannot be cloned and no weak sender, and
//! openraft needs both: it clones the sender into every replication task and holds a weak one
//! where a strong one would keep the core alive. So this is written here, and it is small,
//! because on one thread a channel is a queue, a capacity, two counts and some wakers.
//!
//! The semantics are tokio's, which are what openraft was written against and what its
//! conformance suite asserts: a send on a full queue waits for a receive; a receive on an empty
//! queue waits for a send and returns `None` once every strong sender is gone; a weak sender
//! keeps nothing alive and upgrades only while a strong one exists.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use openraft_rt::{
    Mpsc, MpscReceiver, MpscSender, MpscWeakSender, OptionalSend, SendError, TryRecvError,
};

/// The channel's state, shared by every handle to it
struct Shared<T> {
    /// The queued values, oldest first
    queue: VecDeque<T>,
    /// How many values may be queued before a send waits
    capacity: usize,
    /// How many strong senders exist; zero closes the channel for the receiver
    senders: usize,
    /// Whether the receiver still exists; without it every send fails
    receiver_alive: bool,
    /// The receiver, if it is waiting for a value
    receiver_waker: Option<Waker>,
    /// Every sender waiting for room
    sender_wakers: Vec<Waker>,
}

impl<T> Shared<T> {
    /// Wake the receiver, if it is waiting
    fn wake_receiver(&mut self) {
        if let Some(waker) = self.receiver_waker.take() {
            waker.wake();
        }
    }

    /// Wake every sender waiting for room
    ///
    /// All of them rather than one, since a woken sender that finds the queue still full simply
    /// registers again, and waking one that had been dropped would strand the rest.
    fn wake_senders(&mut self) {
        for waker in self.sender_wakers.drain(..) {
            waker.wake();
        }
    }
}

/// The channel type, which is only a namespace for the trait
pub struct GlommioMpsc;

impl Mpsc for GlommioMpsc {
    type Sender<T: OptionalSend> = Sender<T>;
    type Receiver<T: OptionalSend> = Receiver<T>;
    type WeakSender<T: OptionalSend> = WeakSender<T>;

    /// Open a channel that queues at most `buffer` values
    ///
    /// # Arguments
    ///
    /// * `buffer` - The capacity
    fn channel<T: OptionalSend>(buffer: usize) -> (Self::Sender<T>, Self::Receiver<T>) {
        let shared = Rc::new(RefCell::new(Shared {
            queue: VecDeque::with_capacity(buffer),
            // a zero capacity channel would never accept a value; tokio panics, this rounds up
            capacity: buffer.max(1),
            senders: 1,
            receiver_alive: true,
            receiver_waker: None,
            sender_wakers: Vec::new(),
        }));
        (
            Sender {
                shared: shared.clone(),
            },
            Receiver { shared },
        )
    }
}

/// A strong sender: while one exists the receiver keeps waiting
pub struct Sender<T> {
    /// The channel
    shared: Rc<RefCell<Shared<T>>>,
}

impl<T> Clone for Sender<T> {
    /// Another strong sender
    fn clone(&self) -> Self {
        self.shared.borrow_mut().senders += 1;
        Sender {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    /// One fewer strong sender; the last one gone closes the channel
    fn drop(&mut self) {
        let mut shared = self.shared.borrow_mut();
        shared.senders -= 1;
        if shared.senders == 0 {
            shared.wake_receiver();
        }
    }
}

impl<T: OptionalSend> MpscSender<GlommioMpsc, T> for Sender<T> {
    /// Queue a value, waiting for room if there is none
    fn send(&self, msg: T) -> impl Future<Output = Result<(), SendError<T>>> + OptionalSend {
        Send {
            shared: self.shared.clone(),
            msg: Some(msg),
        }
    }

    /// A sender that keeps nothing alive
    fn downgrade(&self) -> WeakSender<T> {
        WeakSender {
            shared: self.shared.clone(),
        }
    }
}

/// A send in progress
struct Send<T> {
    /// The channel
    shared: Rc<RefCell<Shared<T>>>,
    /// The value, until it is queued or handed back
    msg: Option<T>,
}

impl<T> Unpin for Send<T> {}

impl<T> Future for Send<T> {
    type Output = Result<(), SendError<T>>;

    /// Queue the value if there is room and a receiver, or wait for room
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // the value comes out first, and goes back in if this poll cannot place it
        let msg = self
            .msg
            .take()
            .expect("a send is polled once it has completed");
        let mut shared = self.shared.borrow_mut();
        // a value nobody will ever receive goes back to the caller
        if !shared.receiver_alive {
            return Poll::Ready(Err(SendError(msg)));
        }
        // room, so queue it and tell the receiver
        if shared.queue.len() < shared.capacity {
            shared.queue.push_back(msg);
            shared.wake_receiver();
            return Poll::Ready(Ok(()));
        }
        // full, so wait for a receive to make room
        shared.sender_wakers.push(cx.waker().clone());
        drop(shared);
        self.msg = Some(msg);
        Poll::Pending
    }
}

/// A sender that does not keep the channel open
pub struct WeakSender<T> {
    /// The channel
    shared: Rc<RefCell<Shared<T>>>,
}

impl<T> Clone for WeakSender<T> {
    /// Another weak sender, which changes nothing about the channel
    fn clone(&self) -> Self {
        WeakSender {
            shared: self.shared.clone(),
        }
    }
}

impl<T: OptionalSend> MpscWeakSender<GlommioMpsc, T> for WeakSender<T> {
    /// A strong sender, if any strong sender still exists
    fn upgrade(&self) -> Option<Sender<T>> {
        let mut shared = self.shared.borrow_mut();
        // no strong sender means the channel is closed, and an upgrade must not reopen it
        if shared.senders == 0 {
            return None;
        }
        shared.senders += 1;
        Some(Sender {
            shared: self.shared.clone(),
        })
    }
}

/// The one receiver
pub struct Receiver<T> {
    /// The channel
    shared: Rc<RefCell<Shared<T>>>,
}

impl<T> Drop for Receiver<T> {
    /// No receiver means every send from now on fails, including the ones waiting
    fn drop(&mut self) {
        let mut shared = self.shared.borrow_mut();
        shared.receiver_alive = false;
        shared.wake_senders();
    }
}

impl<T: OptionalSend> MpscReceiver<T> for Receiver<T> {
    /// The next value, or `None` once the channel is closed and drained
    fn recv(&mut self) -> impl Future<Output = Option<T>> + OptionalSend {
        Recv {
            shared: self.shared.clone(),
        }
    }

    /// The next value now, or why there is none
    fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let mut shared = self.shared.borrow_mut();
        match shared.queue.pop_front() {
            // a value, and now there is room for another
            Some(value) => {
                shared.wake_senders();
                Ok(value)
            }
            // nothing queued: empty while a sender could still send, closed otherwise
            None if shared.senders > 0 => Err(TryRecvError::Empty),
            None => Err(TryRecvError::Disconnected),
        }
    }
}

/// A receive in progress
struct Recv<T> {
    /// The channel
    shared: Rc<RefCell<Shared<T>>>,
}

impl<T> Unpin for Recv<T> {}

impl<T> Future for Recv<T> {
    type Output = Option<T>;

    /// Take a value if there is one, finish if there never will be, or wait
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut shared = self.shared.borrow_mut();
        // a value, and now there is room for another
        if let Some(value) = shared.queue.pop_front() {
            shared.wake_senders();
            return Poll::Ready(Some(value));
        }
        // nothing queued and nothing that could queue one
        if shared.senders == 0 {
            return Poll::Ready(None);
        }
        // wait for a send
        shared.receiver_waker = Some(cx.waker().clone());
        Poll::Pending
    }
}
