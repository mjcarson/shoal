//! The spike's own frames: Shoal's eight byte header, and bodies shaped like S12's object frames
//!
//! Nothing here is the product's wire. The header has Shoal's layout - a version, a kind, sixteen
//! bits of flags and a 32-bit length that counts every byte after it - so that a frame costs what
//! a Shoal frame would, and the kinds are the few X11 needs: a stream opened, its bytes in bounded
//! frames with `LAST` on the final one, a range asked for, and a small request and its answer.
//! Like every spike's code this is thrown away; the product's are written by S1's prerequisite for
//! more than one frame a query, and by M13.

/// The version byte every frame carries, which no Shoal frame does
pub const VERSION: u8 = 0x58;

/// The length of a header
pub const HEADER_LEN: usize = 8;

/// The length of what follows the header of a data frame: sixteen bytes of id and an offset,
/// the head the product's data frames would carry, so a frame here costs what one of those would
pub const DATA_HEAD_LEN: usize = 24;

/// The length of a small request's body
pub const SMALL_LEN: usize = 64;

/// The length of a small request's answer
pub const ANSWER_LEN: usize = 1024;

/// The flag on a stream's final data frame, as Shoal's `Flags::LAST` would be
pub const LAST: u16 = 1 << 2;

/// The largest frame either side accepts
pub const MAX_FRAME: u32 = 16 << 20;

/// What a frame is
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Kind {
    /// The first frame of a connection: what it is for
    Setup = 1,
    /// The server's answer to a setup: which executor serves the connection
    SetupOk = 2,
    /// A write stream opened: data frames for its id follow
    Write = 3,
    /// Bytes of a stream at an offset
    Data = 4,
    /// A range asked for, answered by one data frame
    Read = 5,
    /// A small request
    Small = 6,
    /// A small request's answer
    Answer = 7,
    /// A write stream's end, with what it held
    Done = 8,
    /// An executor's counters asked for
    Stat = 9,
    /// An executor's counters
    Stats = 10,
}

impl Kind {
    /// The kind a byte names
    ///
    /// # Arguments
    ///
    /// * `byte` - The byte
    #[must_use]
    pub fn from_byte(byte: u8) -> Option<Kind> {
        // every kind the spike speaks, and nothing else
        Some(match byte {
            1 => Kind::Setup,
            2 => Kind::SetupOk,
            3 => Kind::Write,
            4 => Kind::Data,
            5 => Kind::Read,
            6 => Kind::Small,
            7 => Kind::Answer,
            8 => Kind::Done,
            9 => Kind::Stat,
            10 => Kind::Stats,
            _ => return None,
        })
    }
}

/// A frame's header
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    /// What the frame is
    pub kind: Kind,
    /// Its flags
    pub flags: u16,
    /// How many bytes follow the header
    pub len: u32,
}

impl Header {
    /// A header
    ///
    /// # Arguments
    ///
    /// * `kind` - What the frame is
    /// * `flags` - Its flags
    /// * `len` - How many bytes follow
    #[must_use]
    pub fn new(kind: Kind, flags: u16, len: usize) -> Self {
        Header {
            kind,
            flags,
            len: u32::try_from(len).expect("a frame fits a u32"),
        }
    }

    /// The header as the eight bytes that go on the wire
    #[must_use]
    pub fn encode(&self) -> [u8; HEADER_LEN] {
        // version, kind, flags and length, little endian as Shoal's are
        let flags = self.flags.to_le_bytes();
        let len = self.len.to_le_bytes();
        [
            VERSION,
            self.kind as u8,
            flags[0],
            flags[1],
            len[0],
            len[1],
            len[2],
            len[3],
        ]
    }

    /// Read a header, refusing a version, a kind or a length the spike does not speak
    ///
    /// # Arguments
    ///
    /// * `bytes` - The eight bytes
    pub fn decode(bytes: &[u8; HEADER_LEN]) -> Result<Header, String> {
        // the version first, since nothing else means anything under another one
        if bytes[0] != VERSION {
            return Err(format!("version {:#x}", bytes[0]));
        }
        let kind = Kind::from_byte(bytes[1]).ok_or_else(|| format!("kind {}", bytes[1]))?;
        let flags = u16::from_le_bytes([bytes[2], bytes[3]]);
        let len = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        // a length is an allocation, so it is bounded before anything uses it
        if len > MAX_FRAME {
            return Err(format!("length {len}"));
        }
        Ok(Header { kind, flags, len })
    }
}

/// What a connection is for, sent as its first frame
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Setup {
    /// Whether a write lands in the executor's file, or a read comes from it
    pub file: bool,
    /// Where the bytes go once read: here, or to the other executor
    pub route: Route,
    /// Whether the receiving socket is told kTLS records carry no padding
    pub nopad: bool,
    /// Whether every payload is checked against the pattern
    pub verify: bool,
    /// Whether the server writes its frames in the order they were queued, rather than every
    /// small frame before the next data frame
    pub fifo: bool,
    /// How many frames of a write the server holds at once
    pub window: u32,
    /// `TCP_NOTSENT_LOWAT` for the server's socket, zero for the kernel's default
    pub lowat: u32,
    /// Which of the server's patterns a read is answered from: zero for the seed's own, which
    /// is all X11 asks for, or one X13 gave the server
    pub pattern: u8,
}

/// Where a write's bytes go once the accepting executor has read them
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum Route {
    /// The accepting executor writes them
    #[default]
    Direct = 0,
    /// It hands each buffer to the other executor, which writes it
    Hop = 1,
    /// It hands a copy of each to the other executor, which copies it into a buffer of its own
    HopCopy = 2,
    /// It hands the connection itself to the other executor
    Handoff = 3,
}

impl Route {
    /// The route a byte names
    ///
    /// # Arguments
    ///
    /// * `byte` - The byte
    #[must_use]
    pub fn from_byte(byte: u8) -> Route {
        // an unknown route is the direct one, which a spike's own peer never sends
        match byte {
            1 => Route::Hop,
            2 => Route::HopCopy,
            3 => Route::Handoff,
            _ => Route::Direct,
        }
    }

    /// The route's name in a table
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Route::Direct => "direct",
            Route::Hop => "hop",
            Route::HopCopy => "hop-copy",
            Route::Handoff => "handoff",
        }
    }
}

/// The length of a setup's body
pub const SETUP_LEN: usize = 16;

impl Setup {
    /// The setup as its body
    #[must_use]
    pub fn encode(&self) -> [u8; SETUP_LEN] {
        // four flag bytes, the window, the low water mark, then the fifth flag in a byte of its
        // own: it once shared the window's first byte, which overwrote it (item 213). The
        // pattern a read is answered from comes last
        let mut body = [0u8; SETUP_LEN];
        body[0] = u8::from(self.file);
        body[1] = self.route as u8;
        body[2] = u8::from(self.nopad);
        body[3] = u8::from(self.verify);
        body[4..8].copy_from_slice(&self.window.to_le_bytes());
        body[8..12].copy_from_slice(&self.lowat.to_le_bytes());
        body[12] = u8::from(self.fifo);
        body[13] = self.pattern;
        body
    }

    /// Read a setup from its body
    ///
    /// # Arguments
    ///
    /// * `body` - The body
    #[must_use]
    pub fn decode(body: &[u8]) -> Setup {
        // a short body is a default setup, which only a broken peer sends
        if body.len() < SETUP_LEN {
            return Setup::default();
        }
        Setup {
            file: body[0] != 0,
            route: Route::from_byte(body[1]),
            nopad: body[2] != 0,
            verify: body[3] != 0,
            fifo: body[12] != 0,
            window: u32::from_le_bytes(body[4..8].try_into().expect("four bytes")),
            lowat: u32::from_le_bytes(body[8..12].try_into().expect("four bytes")),
            pattern: body[13],
        }
    }
}

/// An executor's counters, as a stat answers them
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Stats {
    /// The executor thread's cpu time, in nanoseconds
    pub thread_cpu_ns: u64,
    /// The host's busy time over every cpu, in nanoseconds
    pub host_busy_ns: u64,
    /// Payload bytes the executor took from streams and finished with
    pub bytes_in: u64,
    /// Payload bytes it wrote to sockets
    pub bytes_out: u64,
    /// The most bytes it held in buffers at once since the last reset
    pub peak_held: u64,
    /// The most socket memory one of its connections held at once since the last reset
    pub peak_sock: u64,
    /// Payloads that did not match the pattern
    pub mismatches: u64,
}

/// The length of a stats body
pub const STATS_LEN: usize = 56;

impl Stats {
    /// The counters as their body
    #[must_use]
    pub fn encode(&self) -> [u8; STATS_LEN] {
        // seven little endian words in the order the struct declares them
        let mut body = [0u8; STATS_LEN];
        let words = [
            self.thread_cpu_ns,
            self.host_busy_ns,
            self.bytes_in,
            self.bytes_out,
            self.peak_held,
            self.peak_sock,
            self.mismatches,
        ];
        for (slot, word) in body.chunks_exact_mut(8).zip(words) {
            slot.copy_from_slice(&word.to_le_bytes());
        }
        body
    }

    /// Read counters from their body
    ///
    /// # Arguments
    ///
    /// * `body` - The body
    #[must_use]
    pub fn decode(body: &[u8]) -> Stats {
        // a word at a time, zero past a short body
        let word = |index: usize| {
            body.get(index * 8..index * 8 + 8).map_or(0, |bytes| {
                u64::from_le_bytes(bytes.try_into().expect("eight bytes"))
            })
        };
        Stats {
            thread_cpu_ns: word(0),
            host_busy_ns: word(1),
            bytes_in: word(2),
            bytes_out: word(3),
            peak_held: word(4),
            peak_sock: word(5),
            mismatches: word(6),
        }
    }
}

/// Two little endian words, as a stream's open or its end
///
/// # Arguments
///
/// * `first` - The first word
/// * `second` - The second word
#[must_use]
pub fn two_words(first: u64, second: u64) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&first.to_le_bytes());
    out[8..].copy_from_slice(&second.to_le_bytes());
    out
}

/// A data frame's head: the stream's id in sixteen bytes, as a query id would be, then the offset
///
/// # Arguments
///
/// * `id` - The stream
/// * `offset` - Where its payload starts in the stream
#[must_use]
pub fn data_head(id: u64, offset: u64) -> [u8; DATA_HEAD_LEN] {
    let mut out = [0u8; DATA_HEAD_LEN];
    out[..8].copy_from_slice(&id.to_le_bytes());
    out[16..].copy_from_slice(&offset.to_le_bytes());
    out
}

/// The word at an index of a body, zero past its end
///
/// # Arguments
///
/// * `body` - The body
/// * `index` - Which word
#[must_use]
pub fn word(body: &[u8], index: usize) -> u64 {
    body.get(index * 8..index * 8 + 8).map_or(0, |bytes| {
        u64::from_le_bytes(bytes.try_into().expect("eight bytes"))
    })
}

/// The bytes every stream carries: a seeded pattern both sides can make
///
/// A payload at a stream offset is the pattern at that offset modulo its length, so a peer can
/// check any frame it is sent, and a write's payload is a slice of it rather than bytes made for
/// each frame.
pub const PATTERN_LEN: usize = 64 << 20;

/// Make the pattern
///
/// # Arguments
///
/// * `seed` - The seed both sides share
#[must_use]
pub fn pattern(seed: u64) -> Vec<u8> {
    let mut bytes = vec![0u8; PATTERN_LEN];
    crate::device::stats::Rng::new(seed).fill(&mut bytes);
    bytes
}

/// Where a payload at a stream offset starts in the pattern
///
/// # Arguments
///
/// * `offset` - The stream offset
#[must_use]
pub fn pattern_at(offset: u64) -> usize {
    (offset % PATTERN_LEN as u64) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A header comes back as it went
    #[test]
    fn a_header_round_trips() {
        let header = Header::new(Kind::Data, LAST, 1 << 20);
        assert_eq!(Header::decode(&header.encode()), Ok(header));
    }

    /// A length past the bound is refused before it is used
    #[test]
    fn a_header_past_the_bound_is_refused() {
        let mut bytes = Header::new(Kind::Data, 0, 0).encode();
        bytes[4..8].copy_from_slice(&(MAX_FRAME + 1).to_le_bytes());
        assert!(Header::decode(&bytes).is_err());
    }

    /// A setup and a stats body come back as they went
    #[test]
    fn bodies_round_trip() {
        let setup = Setup {
            file: true,
            route: Route::Handoff,
            nopad: true,
            verify: true,
            fifo: true,
            window: 8,
            lowat: 131_072,
            pattern: 3,
        };
        assert_eq!(Setup::decode(&setup.encode()), setup);
        let stats = Stats {
            thread_cpu_ns: 1,
            host_busy_ns: 2,
            bytes_in: 3,
            bytes_out: 4,
            peak_held: 5,
            peak_sock: 6,
            mismatches: 7,
        };
        assert_eq!(Stats::decode(&stats.encode()), stats);
    }

    /// A setup asking for small frames first comes back asking for them, at every window X11 ran
    ///
    /// Item 213: the flag and the window once shared a byte, so a window whose low byte was not
    /// zero read back as first in first out.
    #[test]
    fn a_small_first_setup_round_trips() {
        // every window section 2 and section 3 ran, with the server writing small frames first
        for window in [1, 2, 4, 8, 16] {
            let setup = Setup {
                fifo: false,
                window,
                ..Setup::default()
            };
            assert_eq!(Setup::decode(&setup.encode()), setup, "window {window}");
        }
    }
}
