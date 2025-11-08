//! The different messages that are used client side

use rkyv::util::AlignedVec;

#[derive(Debug)]
pub enum ClientMsg {
    /// A response from the server
    Response(AlignedVec),
    /// A client side message to mark the end of a stream
    End(usize),
}
