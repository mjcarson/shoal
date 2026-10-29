//! The body of one request frame, read into a buffer that is never written twice
//!
//! A request body used to be a [`bytes::BytesMut::zeroed`] whose every byte the `read_exact` on
//! the next line overwrote, which is a full write of zeroes over a buffer nothing ever reads in
//! that state. Removing it means handing a reader memory that Rust considers uninitialized, and
//! `futures::io::AsyncRead` takes a `&mut [u8]` and reports nothing about how much of it it
//! initialized - so there is no runtime check available to lean on the way the client's tokio
//! reader has one.
//!
//! The guarantee is therefore structural. [`RequestBody`] holds a private buffer and
//! [`RequestBody::read_from`] is its only constructor, so a `RequestBody` that exists is a
//! buffer some read filled to its full length. A read that fails drops the buffer without ever
//! handing it out.
//!
//! There are two ways out of the type and both preserve that: [`Deref`] borrows the bytes, and
//! [`RequestBody::freeze`] consumes the body for a [`Bytes`] several shards can hold at once.
//! Neither can be reached without having built one, so neither can hand out memory a read did
//! not fill.
//!
//! Since [F73](../../../docs/src/features/bodies-across-frames.md) there is one other way in: a
//! bundle longer than a frame arrives in data frames, and is assembled by a [`BodyAssembly`] whose
//! length only grows by a read that filled the bytes it grew by, and which becomes a
//! [`RequestBody`] only once every byte its opener declared has been read. The guarantee is the
//! same - a body that exists is one reads filled to its full length - with more than one read.

use bytes::{Bytes, BytesMut};
use futures::{AsyncRead, AsyncReadExt};
use std::io;
use std::ops::Deref;

/// The least room an arena takes once it has too little for a body: a frame's worth of small bodies
/// one after another, in pages the kernel zeroed once
/// ([F111](../../../docs/src/features/lane-buffers.md))
pub const ARENA_BYTES: usize = 1 << 20;

/// The bytes of one request frame's body
///
/// # Safety
///
/// The `data` field must never become public and this module must never gain a way to build one
/// that does not read every byte of it. [`RequestBody::read_from`] sets the buffer's length over
/// memory the allocator has not written, and the only thing that makes those bytes initialized is
/// the `read_exact` that follows in the same function; [`BodyAssembly::finish`] hands one out only
/// when its reads have filled every byte. A constructor that skipped a read - or a public field
/// that let a caller build one from a shorter buffer - would hand a shard bytes nothing wrote.
pub struct RequestBody {
    /// The body itself, filled by the read that built this
    data: BytesMut,
}

impl RequestBody {
    /// Read one request body of a known length
    ///
    /// The length comes from a frame header that has already been checked against this server's
    /// frame bound, so it is a size this server agreed to accept rather than one a peer named.
    ///
    /// # Arguments
    ///
    /// * `reader` - The connection to read this body from
    /// * `len` - How many bytes this body is, from its already checked frame header
    ///
    /// # Errors
    ///
    /// Returns whatever the read failed with, having dropped the partially filled buffer. A
    /// stream that ends early is an `UnexpectedEof` rather than a short body, because
    /// `read_exact` does not return until it has the bytes it was asked for.
    pub async fn read_from<R: AsyncRead + Unpin>(reader: &mut R, len: usize) -> io::Result<Self> {
        // an empty body has nothing to read and no allocation to make
        if len == 0 {
            return Ok(RequestBody {
                data: BytesMut::new(),
            });
        }
        // take exactly the bytes this body needs, without writing any of them
        let mut data = BytesMut::with_capacity(len);
        // claim the whole allocation as the body
        //
        // SAFETY: `with_capacity` reserved at least `len` bytes, and the only thing that happens
        // to this buffer before it leaves this function is the `read_exact` below, which fills
        // every one of them or returns an error that drops it. Nothing between these two lines
        // reads `data`.
        unsafe { data.set_len(len) };
        // fill every byte of it from the connection
        reader.read_exact(&mut data).await?;
        Ok(RequestBody { data })
    }

    /// Read one request body of a known length into room an arena gives it (F111)
    ///
    /// A lane that reads many bodies takes each from one arena, which gives a body the room its
    /// earlier bodies gave back once every holder of them dropped them, so most bodies land in
    /// pages already mapped rather than in a fresh allocation the kernel zeroes. A body still held
    /// keeps its room, and the arena takes more beside it. The guarantee is
    /// [`RequestBody::read_from`]'s: a body is handed out only once a read filled every byte of it,
    /// and a read that fails leaves the arena holding no bytes.
    ///
    /// # Arguments
    ///
    /// * `reader` - The connection to read this body from
    /// * `len` - How many bytes this body is, from its already checked frame header
    /// * `arena` - The lane's arena, holding no bytes between bodies
    ///
    /// # Errors
    ///
    /// Returns whatever the read failed with, the arena holding no bytes.
    pub async fn read_from_arena<R: AsyncRead + Unpin>(
        reader: &mut R,
        len: usize,
        arena: &mut BytesMut,
    ) -> io::Result<Self> {
        // an empty body has nothing to read and no room to take
        if len == 0 {
            return Ok(RequestBody {
                data: BytesMut::new(),
            });
        }
        // room for the body: what the arena has, or what its bodies gave back, or more
        arena.clear();
        if arena.capacity() < len {
            arena.reserve(len.max(ARENA_BYTES));
        }
        // claim the body's room
        //
        // SAFETY: the arena holds no bytes, since every body is split off it whole and a failed
        // read clears it, and `reserve` left room for at least `len`. The only thing that happens
        // to these bytes before one leaves this function is the `read_exact` below, which fills
        // every one of them or returns an error after which the arena is cleared, so nothing it
        // did not fill is ever handed out.
        unsafe { arena.set_len(len) };
        if let Err(error) = reader.read_exact(&mut arena[..len]).await {
            arena.clear();
            return Err(error);
        }
        Ok(RequestBody {
            data: arena.split_to(len),
        })
    }

    /// Read the rest of a request body whose first bytes were read already, into room an arena
    /// gives it behind them (F111)
    ///
    /// What [`RequestBody::read_from_arena`] does for a body its reader looked at the head of
    /// first, to tell a piece of a whole chunk, whose units go elsewhere, from every other request.
    ///
    /// # Arguments
    ///
    /// * `reader` - The connection to read the rest from
    /// * `head` - The body's first bytes, read already
    /// * `len` - How many bytes the whole body is, from its already checked frame header
    /// * `arena` - The lane's arena, holding no bytes between bodies
    ///
    /// # Errors
    ///
    /// Returns whatever the read failed with, the arena holding no bytes.
    pub async fn read_rest_from_arena<R: AsyncRead + Unpin>(
        reader: &mut R,
        head: &[u8],
        len: usize,
        arena: &mut BytesMut,
    ) -> io::Result<Self> {
        // a head longer than its body is a caller's mistake, never a body
        if head.len() > len {
            let why = "a body's head is longer than the body";
            return Err(io::Error::new(io::ErrorKind::InvalidInput, why));
        }
        // room for the whole body, the head written into it first
        arena.clear();
        if arena.capacity() < len {
            arena.reserve(len.max(ARENA_BYTES));
        }
        arena.extend_from_slice(head);
        // claim the rest of the body's room
        //
        // SAFETY: the arena holds exactly the head, written above, and `reserve` left room for the
        // whole body. The only thing that happens to the bytes past the head before one leaves
        // this function is the `read_exact` below, which fills every one of them or returns an
        // error after which the arena is cleared, so nothing it did not fill is ever handed out.
        unsafe { arena.set_len(len) };
        if let Err(error) = reader.read_exact(&mut arena[head.len()..len]).await {
            arena.clear();
            return Err(error);
        }
        Ok(RequestBody {
            data: arena.split_to(len),
        })
    }

    /// Turn this body into a buffer several shards can hold at once
    ///
    /// A bundle is routed to every shard that owns one of the partitions its queries name, and
    /// each of those shards reads its own query straight out of these bytes rather than being
    /// handed a copy of it ([F26](../../../docs/src/features/archive-routed-requests.md)). So the
    /// body has to outlive the routing loop and be cheap to hand out, which is what [`Bytes`] is
    /// and [`BytesMut`] is not - cloning a `BytesMut` copies it.
    ///
    /// This consumes the body rather than borrowing it, so it is the second and last exit from
    /// the type. It preserves the guarantee the type exists for: the only way to reach a `Bytes`
    /// through here is to have built a `RequestBody` first, and the only way to do that is a read
    /// that filled every byte.
    #[must_use]
    pub fn freeze(self) -> Bytes {
        // the same allocation, handed over as a shared read only buffer
        self.data.freeze()
    }

    /// Get how many bytes this body is
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Get whether this body carries no bytes at all
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

/// A bundle's body being assembled from the data frames of a stream
///
/// # Safety
///
/// `data`'s length is the bytes reads have filled, and only [`BodyAssembly::read_next`] grows it,
/// by exactly the bytes the `read_exact` beside it filled; a read that fails puts the length back
/// before it returns. Its capacity is the declared length, reserved once at the opener, so a
/// stream can never make it grow past what was admitted.
pub struct BodyAssembly {
    /// The bytes read so far, with room for the rest
    data: BytesMut,
    /// The length the opener declared
    declared: usize,
}

impl BodyAssembly {
    /// Start assembling a body of a declared length
    ///
    /// The length has been checked against the bound this server advertised and against the
    /// shard's budget before this is called, so it is an allocation this server agreed to.
    ///
    /// # Arguments
    ///
    /// * `declared` - The bytes the stream's opener said it carries
    #[must_use]
    pub fn new(declared: usize) -> Self {
        BodyAssembly {
            data: BytesMut::with_capacity(declared),
            declared,
        }
    }

    /// Read the next data frame's bytes onto the end of the body
    ///
    /// # Arguments
    ///
    /// * `reader` - The connection, positioned at the frame's payload
    /// * `len` - The payload's length, already judged to fit the declared length
    ///
    /// # Errors
    ///
    /// Returns whatever the read failed with, with the body as it was before the read. A payload
    /// that would pass the declared length is an `InvalidData` error and reads nothing.
    pub async fn read_next<R: AsyncRead + Unpin>(
        &mut self,
        reader: &mut R,
        len: usize,
    ) -> io::Result<()> {
        // a stream judged frame by frame cannot pass its length, but the buffer never trusts that
        let filled = self.data.len();
        if filled + len > self.declared {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "a data frame past its stream's declared length",
            ));
        }
        // claim the next bytes of the reserved capacity as part of the body
        //
        // SAFETY: the capacity is the declared length and `filled + len` is within it, so these
        // bytes are this allocation's. They are filled by the `read_exact` below before anything
        // reads them, and if it fails the length goes back to `filled`, so no byte nothing wrote
        // is ever inside the body's length when the read returns.
        unsafe { self.data.set_len(filled + len) };
        // fill them from the connection
        if let Err(error) = reader.read_exact(&mut self.data[filled..]).await {
            // the bytes past `filled` may not have been written, so they leave the body
            self.data.truncate(filled);
            return Err(error);
        }
        Ok(())
    }

    /// How many bytes have been read so far
    #[must_use]
    pub fn filled(&self) -> usize {
        self.data.len()
    }

    /// The body, once every declared byte has been read, or the assembly as it was
    ///
    /// # Errors
    ///
    /// Gives the assembly back when it is short of its declared length.
    pub fn finish(self) -> Result<RequestBody, BodyAssembly> {
        if self.data.len() == self.declared {
            Ok(RequestBody { data: self.data })
        } else {
            Err(self)
        }
    }
}

impl std::fmt::Debug for BodyAssembly {
    /// Print how much of the body has arrived rather than what is in it
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BodyAssembly")
            .field("filled", &self.data.len())
            .field("declared", &self.declared)
            .finish()
    }
}

impl Clone for RequestBody {
    /// Copy this body
    ///
    /// Sound for the same reason the read is: the buffer being copied is one a read filled, so
    /// there are no uninitialized bytes for the copy to duplicate. A broadcast of a client
    /// message is what asks for this.
    fn clone(&self) -> Self {
        RequestBody {
            data: self.data.clone(),
        }
    }
}

impl Deref for RequestBody {
    type Target = [u8];

    /// Read this body as the bytes it holds
    fn deref(&self) -> &[u8] {
        &self.data
    }
}

impl std::fmt::Debug for RequestBody {
    /// Print how large this body is rather than what is in it
    ///
    /// A bundle is tens of kibibytes of archive and printing it would bury whatever the caller
    /// was actually looking at.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestBody")
            .field("len", &self.data.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::{BodyAssembly, RequestBody};
    use futures::AsyncRead;
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    /// A reader that hands its bytes over a few at a time
    ///
    /// A socket almost never delivers a whole bundle in one `poll_read`, and a fill loop that
    /// assumed it did would leave a tail nobody wrote. This is what makes the tests run the
    /// loop rather than the first read.
    struct Chunked {
        /// The bytes left to hand out
        rest: Vec<u8>,
        /// The most bytes to hand out per read
        chunk: usize,
    }

    impl Chunked {
        /// Build a reader over a body, handing out at most `chunk` bytes at a time
        ///
        /// # Arguments
        ///
        /// * `body` - The bytes this reader delivers
        /// * `chunk` - The most bytes to deliver per read
        fn new(body: Vec<u8>, chunk: usize) -> Self {
            Chunked { rest: body, chunk }
        }
    }

    impl AsyncRead for Chunked {
        /// Hand over the next few bytes, or report the end of the stream
        ///
        /// # Arguments
        ///
        /// * `buf` - The buffer to write those bytes into
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            // hand over the smallest of what is left, what was asked for, and one chunk
            let take = self.rest.len().min(buf.len()).min(self.chunk);
            // an empty stream is the end of the stream
            if take == 0 {
                return Poll::Ready(Ok(0));
            }
            // move those bytes into the caller's buffer
            buf[..take].copy_from_slice(&self.rest[..take]);
            // and drop them from what is left to hand out
            self.rest.drain(..take);
            Poll::Ready(Ok(take))
        }
    }

    /// Build a body of a given length with no zero byte anywhere in it
    ///
    /// A byte the read never wrote reads back as whatever the allocator left there, which is a
    /// zero far more often than not, so a body with no zeroes in it is what makes an unwritten
    /// tail visible rather than plausible.
    ///
    /// # Arguments
    ///
    /// * `len` - How many bytes to build
    fn body(len: usize) -> Vec<u8> {
        (0..len).map(|index| ((index % 255) + 1) as u8).collect()
    }

    #[test]
    fn a_body_read_in_chunks_holds_every_byte_it_was_sent() {
        // walk a set of lengths so that no single one can pass by luck
        for len in [1, 7, 64, 4095, 65536] {
            // hand the body over seven bytes at a time, so the fill loop really loops
            let sent = body(len);
            let mut reader = Chunked::new(sent.clone(), 7);
            // read it the way the relay does
            let read = futures::executor::block_on(RequestBody::read_from(&mut reader, len))
                .expect("failed to read a body");
            // every byte of it is the byte that was sent
            assert_eq!(read.len(), len, "a body of {len} bytes changed length");
            assert_eq!(
                &read[..],
                &sent[..],
                "a body of {len} bytes changed content"
            );
        }
    }

    #[test]
    fn a_body_whose_stream_ends_early_is_an_error_and_not_a_short_read() {
        // a stream that carries half of what its header promised
        let mut reader = Chunked::new(body(512), 7);
        // reading the promised length has to fail rather than hand back what arrived
        let error = futures::executor::block_on(RequestBody::read_from(&mut reader, 1024))
            .expect_err("a truncated body read back as a whole one");
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    }

    /// Bodies read into an arena are every byte their peer sent; once a body that took the arena's
    /// room is dropped the next is given the same room, and one still held keeps its own (F111)
    #[test]
    fn an_arena_gives_a_body_the_room_its_last_one_gave_back() {
        use super::ARENA_BYTES;
        let mut arena = bytes::BytesMut::new();
        let read = |arena: &mut bytes::BytesMut, sent: &[u8]| {
            let mut reader = Chunked::new(sent.to_vec(), 65_521);
            let read = RequestBody::read_from_arena(&mut reader, sent.len(), arena);
            futures::executor::block_on(read)
                .expect("failed to read a body")
                .freeze()
        };
        // a body that takes all of the arena's room, dropped, and the next given that room
        let first = read(&mut arena, &body(ARENA_BYTES));
        assert_eq!(&first[..], &body(ARENA_BYTES)[..]);
        let at = first.as_ptr();
        drop(first);
        let second = read(&mut arena, &body(ARENA_BYTES));
        assert_eq!(second.as_ptr(), at, "a body's room was not given to the next once dropped");
        // a body still held keeps its room, and the next lands elsewhere, both whole
        let third = read(&mut arena, &body(ARENA_BYTES));
        assert_ne!(third.as_ptr(), second.as_ptr());
        assert_eq!(&second[..], &body(ARENA_BYTES)[..]);
        assert_eq!(&third[..], &body(ARENA_BYTES)[..]);
        // small bodies one after another share a room, and one longer than it is read whole
        let (small, next) = (read(&mut arena, &body(1000)), read(&mut arena, &body(3000)));
        assert_eq!((&small[..], &next[..]), (&body(1000)[..], &body(3000)[..]));
        let long = body(3 << 20);
        assert_eq!(&read(&mut arena, &long)[..], &long[..]);
    }

    /// A body whose stream ends early is an error, and leaves its arena holding no bytes (F111)
    #[test]
    fn an_arena_body_cut_short_leaves_the_arena_empty() {
        let mut arena = bytes::BytesMut::new();
        let mut reader = Chunked::new(body(512), 7);
        let read = RequestBody::read_from_arena(&mut reader, 1024, &mut arena);
        let error = futures::executor::block_on(read)
            .expect_err("a truncated body read back as a whole one");
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        assert!(arena.is_empty(), "a failed read left bytes nothing filled in its arena");
    }

    #[test]
    fn an_empty_body_reads_without_touching_the_reader() {
        // a reader that would panic the moment anything asked it for a byte
        let mut reader = Chunked::new(Vec::new(), 0);
        // an empty body is an empty body, and no read happens to produce one
        let read = futures::executor::block_on(RequestBody::read_from(&mut reader, 0))
            .expect("failed to read an empty body");
        assert!(read.is_empty());
        assert_eq!(read.len(), 0);
    }

    /// A body assembled from frames holds every byte of every frame, in order
    #[test]
    fn an_assembled_body_holds_every_byte_of_every_frame() {
        // three frames of a body cut unevenly, each handed over a few bytes at a time
        let sent = body(10_000);
        let mut assembly = BodyAssembly::new(sent.len());
        for (start, end) in [(0, 4096), (4096, 8192), (8192, 10_000)] {
            let mut reader = Chunked::new(sent[start..end].to_vec(), 7);
            futures::executor::block_on(assembly.read_next(&mut reader, end - start))
                .expect("a frame reads");
        }
        let read = assembly.finish().expect("every declared byte arrived");
        assert_eq!(&read[..], &sent[..]);
    }

    /// An assembly short of its declared length does not become a body
    #[test]
    fn an_assembly_short_of_its_length_does_not_finish() {
        let mut assembly = BodyAssembly::new(100);
        let mut reader = Chunked::new(body(60), 7);
        futures::executor::block_on(assembly.read_next(&mut reader, 60)).expect("a frame reads");
        let assembly = assembly.finish().expect_err("a short assembly finished");
        assert_eq!(assembly.filled(), 60);
    }

    /// A frame past the declared length is refused and reads nothing, and a frame cut short
    /// leaves the body as it was
    #[test]
    fn an_assembly_refuses_a_read_past_its_length() {
        let mut assembly = BodyAssembly::new(100);
        let mut reader = Chunked::new(body(200), 7);
        let error = futures::executor::block_on(assembly.read_next(&mut reader, 101))
            .expect_err("a frame past the declared length was read");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(assembly.filled(), 0);
        // a stream that ends inside a frame puts the length back where it was
        let mut short = Chunked::new(body(30), 7);
        futures::executor::block_on(assembly.read_next(&mut short, 50))
            .expect_err("a frame cut short read as whole");
        assert_eq!(assembly.filled(), 0);
    }
}
