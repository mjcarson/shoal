//! The order a client connection's answers are written in, once an answer can be many frames
//! ([F73](../../../../docs/src/features/bodies-across-frames.md))
//!
//! Before F73 a connection's write relay wrote each answer whole, in the order it took them, so a
//! 60 MiB answer held every answer behind it for as long as its bytes took. Now an answer longer
//! than one data frame is a stream: an opener and data frames. This decides which frame goes next.
//!
//! - **Whole frames go first.** Every whole answer queued - a small answer, a topology frame, an
//!   admin answer, a refusal - is written before the next data frame, so a small answer waits
//!   behind at most one data frame of a stream and not the stream.
//! - **But a stream is never starved.** Once whole frames have taken a data frame's worth of
//!   bytes since the last data frame, one data frame goes, so a flood of small answers slows a
//!   stream by half at the most.
//! - **Streams take turns**, a data frame each, at most `interleave` of them at once; the rest
//!   wait in the order they arrived.
//! - **Two streams of one id are never open at once.** A bundle's answers share its id, and the
//!   client knows a stream by its id, so a second answer of one bundle waits for the first to end.
//!
//! Nothing here does I/O. The relay takes what [`Outbox::next`] gives it, writes it, and gives a
//! stream that is not done back with [`Outbox::put_back`].

use std::collections::VecDeque;

use uuid::Uuid;

use crate::shared::protocol::stream::{Piece, Splitter};

/// One answer being written as a stream
#[derive(Debug)]
pub struct OutStream<T> {
    /// The answer
    pub item: T,
    /// The id its frames carry
    pub id: Uuid,
    /// Whether its opener has been written
    pub opened: bool,
    /// What is left of it to write
    pub splitter: Splitter,
}

impl<T> OutStream<T> {
    /// A stream for an answer, its opener not yet written
    ///
    /// # Arguments
    ///
    /// * `item` - The answer
    /// * `id` - The id its frames carry
    /// * `total` - The bytes it carries in data frames
    /// * `frame` - The payload bytes of each data frame
    #[must_use]
    pub fn new(item: T, id: Uuid, total: u64, frame: usize) -> Self {
        OutStream {
            item,
            id,
            opened: false,
            splitter: Splitter::new(total, frame),
        }
    }

    /// The next data frame to write, once the opener is out
    pub fn next_piece(&mut self) -> Option<Piece> {
        self.splitter.next_piece()
    }

    /// Whether every frame has been written
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.opened && self.splitter.is_done()
    }
}

/// What a connection writes next
#[derive(Debug)]
pub enum Next<T> {
    /// A whole frame
    Whole(T),
    /// A stream's next frame: its opener if it has not been written, a data frame otherwise
    Stream(OutStream<T>),
}

/// The frames a connection owes, in the order they are written
#[derive(Debug)]
pub struct Outbox<T> {
    /// Whole frames waiting, each with its size
    whole: VecDeque<(T, usize)>,
    /// Streams being written, taking turns
    open: VecDeque<OutStream<T>>,
    /// Streams waiting for a turn, in the order they arrived
    waiting: VecDeque<OutStream<T>>,
    /// The most streams written at once
    interleave: usize,
    /// The payload bytes of a data frame
    frame: usize,
    /// Bytes of whole frames written since the last data frame
    since_data: usize,
}

impl<T> Outbox<T> {
    /// An empty outbox
    ///
    /// # Arguments
    ///
    /// * `interleave` - The most streams written at once, at least one
    /// * `frame` - The payload bytes of a data frame
    #[must_use]
    pub fn new(interleave: usize, frame: usize) -> Self {
        Outbox {
            whole: VecDeque::new(),
            open: VecDeque::new(),
            waiting: VecDeque::new(),
            interleave: interleave.max(1),
            frame: frame.max(1),
            since_data: 0,
        }
    }

    /// Queue a whole frame
    ///
    /// # Arguments
    ///
    /// * `item` - The frame's answer
    /// * `size` - Its bytes on the wire, roughly
    pub fn push_whole(&mut self, item: T, size: usize) {
        self.whole.push_back((item, size));
    }

    /// Queue a stream
    ///
    /// # Arguments
    ///
    /// * `stream` - The stream
    pub fn push_stream(&mut self, stream: OutStream<T>) {
        self.waiting.push_back(stream);
        self.promote();
    }

    /// Open waiting streams while there is room and their ids are free
    fn promote(&mut self) {
        let mut index = 0;
        while self.open.len() < self.interleave && index < self.waiting.len() {
            // a stream whose id is already open waits for that one to end
            let id = self.waiting[index].id;
            if self.open.iter().any(|open| open.id == id) {
                index += 1;
                continue;
            }
            let stream = self.waiting.remove(index).expect("the index is in range");
            self.open.push_back(stream);
        }
    }

    /// What to write next, if anything is owed
    pub fn next(&mut self) -> Option<Next<T>> {
        // a whole frame first, unless whole frames have had a data frame's worth since the last
        let stream_due = !self.open.is_empty() && self.since_data >= self.frame;
        if !stream_due {
            if let Some((item, size)) = self.whole.pop_front() {
                self.since_data += size;
                return Some(Next::Whole(item));
            }
        }
        // then the stream whose turn it is
        if let Some(stream) = self.open.pop_front() {
            // an opener is small, so only a data frame resets the count
            if stream.opened {
                self.since_data = 0;
            }
            return Some(Next::Stream(stream));
        }
        // nothing is open, so a whole frame is all there could be
        self.whole.pop_front().map(|(item, size)| {
            self.since_data += size;
            Next::Whole(item)
        })
    }

    /// Give back a stream whose frame was written: to the back of the turns if it has more, or
    /// gone, opening the next waiting stream, if it is done
    ///
    /// # Arguments
    ///
    /// * `stream` - The stream
    ///
    /// Returns the stream when it is done, so its answer can be counted as written.
    pub fn put_back(&mut self, stream: OutStream<T>) -> Option<OutStream<T>> {
        if stream.is_done() {
            self.promote();
            return Some(stream);
        }
        self.open.push_back(stream);
        None
    }

    /// How many answers are owed and not yet started: whole frames and waiting streams
    #[must_use]
    pub fn unstarted(&self) -> usize {
        self.whole.len() + self.waiting.len()
    }

    /// Whether nothing is owed
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.whole.is_empty() && self.open.is_empty() && self.waiting.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An id for a stream
    fn id(n: u8) -> Uuid {
        Uuid::from_bytes([n; 16])
    }

    /// Write everything owed, naming each frame: a whole one by its item, a stream's by its item
    /// and what it carried
    fn drain_order(outbox: &mut Outbox<&'static str>) -> Vec<String> {
        let mut order = Vec::new();
        while let Some(next) = outbox.next() {
            match next {
                Next::Whole(item) => order.push(item.to_string()),
                Next::Stream(mut stream) => {
                    if stream.opened {
                        let piece = stream.next_piece().expect("an open stream has a frame");
                        order.push(format!("{}@{}", stream.item, piece.offset));
                    } else {
                        stream.opened = true;
                        order.push(format!("{}:open", stream.item));
                    }
                    outbox.put_back(stream);
                }
            }
        }
        order
    }

    /// A small answer queued during a stream waits behind at most one data frame
    #[test]
    fn a_small_reply_waits_behind_at_most_one_data_frame() {
        let mut outbox = Outbox::new(4, 100);
        outbox.push_stream(OutStream::new("big", id(1), 300, 100));
        // the opener, then the first data frame
        let mut stream = match outbox.next() {
            Some(Next::Stream(stream)) => stream,
            other => panic!("the stream went first, not {other:?}"),
        };
        stream.opened = true;
        outbox.put_back(stream);
        let mut stream = match outbox.next() {
            Some(Next::Stream(stream)) => stream,
            other => panic!("its first data frame went next, not {other:?}"),
        };
        stream.next_piece();
        outbox.put_back(stream);
        // a small answer arrives while the stream has two frames left
        outbox.push_whole("small", 10);
        assert!(matches!(outbox.next(), Some(Next::Whole("small"))));
    }

    /// A flood of small answers slows a stream but never stops it
    #[test]
    fn streams_progress_under_a_flood_of_small_replies() {
        let mut outbox = Outbox::new(4, 100);
        outbox.push_stream(OutStream::new("big", id(1), 200, 100));
        for _ in 0..40 {
            outbox.push_whole("small", 50);
        }
        let order = drain_order(&mut outbox);
        // the stream's last frame goes long before the last small answer
        let last_data = order
            .iter()
            .position(|frame| frame == "big@100")
            .expect("the stream ends");
        assert!(last_data < 10, "the stream waited for the flood: {order:?}");
    }

    /// Two answers of one id are never open at once, and keep their order
    #[test]
    fn one_ids_streams_keep_their_order() {
        let mut outbox = Outbox::new(4, 100);
        outbox.push_stream(OutStream::new("first", id(1), 200, 100));
        outbox.push_stream(OutStream::new("second", id(1), 200, 100));
        let order = drain_order(&mut outbox);
        let first_end = order.iter().position(|frame| frame == "first@100").unwrap();
        let second_open = order
            .iter()
            .position(|frame| frame == "second:open")
            .unwrap();
        assert!(
            first_end < second_open,
            "the second opened before the first ended: {order:?}"
        );
    }

    /// No more than the interleave are written at once, and different ids take turns
    #[test]
    fn no_more_than_the_interleave_are_open() {
        let mut outbox = Outbox::new(2, 100);
        for (n, name) in ["a", "b", "c"].into_iter().enumerate() {
            outbox.push_stream(OutStream::new(name, id(n as u8), 200, 100));
        }
        let order = drain_order(&mut outbox);
        let c_open = order.iter().position(|frame| frame == "c:open").unwrap();
        let a_end = order.iter().position(|frame| frame == "a@100").unwrap();
        let b_end = order.iter().position(|frame| frame == "b@100").unwrap();
        assert!(
            c_open > a_end.min(b_end),
            "a third stream opened beside two: {order:?}"
        );
        // a and b take turns rather than one running to its end first
        let b_first = order.iter().position(|frame| frame == "b@0").unwrap();
        assert!(b_first < a_end, "the streams did not take turns: {order:?}");
    }

    /// What is owed and not started counts whole frames and waiting streams, not open ones
    #[test]
    fn unstarted_counts_what_has_not_begun() {
        let mut outbox = Outbox::new(1, 100);
        outbox.push_stream(OutStream::new("a", id(1), 100, 100));
        outbox.push_stream(OutStream::new("b", id(2), 100, 100));
        outbox.push_whole("small", 10);
        assert_eq!(outbox.unstarted(), 2);
        drain_order(&mut outbox);
        assert!(outbox.is_empty());
        assert_eq!(outbox.unstarted(), 0);
    }
}
