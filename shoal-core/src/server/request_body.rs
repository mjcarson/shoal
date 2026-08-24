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

use bytes::BytesMut;
use futures::{AsyncRead, AsyncReadExt};
use std::io;
use std::ops::Deref;

/// The bytes of one request frame's body
///
/// # Safety
///
/// The `data` field must never become public and this module must never gain a second way to
/// build one. [`RequestBody::read_from`] sets the buffer's length over memory the allocator has
/// not written, and the only thing that makes those bytes initialized is the `read_exact` that
/// follows in the same function. A constructor that skipped that read - or a public field that
/// let a caller build one from a shorter buffer - would hand a shard bytes nothing wrote.
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
        f.debug_struct("RequestBody").field("len", &self.data.len()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::RequestBody;
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
            assert_eq!(&read[..], &sent[..], "a body of {len} bytes changed content");
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
}
