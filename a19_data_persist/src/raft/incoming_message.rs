use a19_concurrent::queue::mpsc_queue::{MpscQueueWrap, MpscQueueReceive};
use a19_concurrent::buffer::ring_buffer::{ManyToOneBufferWriter, ManyToOneBufferReader, create_many_to_one};
use std::sync::Arc;
use std::sync::atomic::{ AtomicU32, AtomicU64 };
use crate::raft::*;
use crate::raft::write_message::*;
use rand::{thread_rng, RngCore};

const SERVER_ID_SHIFT: usize = 54;
const RANDOM_SHIFT: usize = 38;
const RANDOM_SHIFT_MASK: u32 = 0x00_00_FF_FF;
const SERVER_ID_MASK: u64 = 0b1111111111000000000000000000000000000000000000000000000000000000;

#[derive(Debug, Clone)]
pub(crate) enum IncomingEvt {
    ElectedLeader,
    Follower{server: u32},
    NoHandler,
}

#[derive(Debug, Clone)]
enum IncomingState {
    /// This server is the leader and needs to append message directly to the buffer.
    Leader,
    /// Send messages to the current leader.
    Follower{server_id: u32},
    /// Hold onto the messages.
    NoHandler,
}

pub(crate) struct IncomingMessageProcessor {
    /// The id of the current server.
    server_id: u32,
    /// The current collection.
    message_writer_collection: MessageWriteCollection,
    /// Used to communicate with the thread writing incoming messages.
    incoming_queue: ManyToOneBufferReader,
    /// The current state.
    current_state: IncomingState,
}

pub(crate) struct ImcomingMessageClient {
    /// The incoming buffer we right the messages to.
    incoming_writer: ManyToOneBufferWriter,
    internal_message_id: AtomicU64,
}

/// Creates the starting id for the server.  Uses a 16 bit random number to make the likelihood of it repeating on start very low.
fn create_start_id(server_id: u32) -> u64 {
    let server_id = (server_id as u64) << SERVER_ID_SHIFT;
    let mut rng = thread_rng();
    let value = ((rng.next_u32() & RANDOM_SHIFT_MASK) as u64) << RANDOM_SHIFT;
    server_id + value
}

pub fn get_server_id_from_message(server_msg_id: &u64) -> u32 {
    ((server_msg_id & SERVER_ID_MASK) >> SERVER_ID_SHIFT) as u32
}

pub(crate) fn create_incoming_message(
    server_id: &u32) {
}

#[cfg(test)]
mod test {
    use serial_test::serial;
    use crate::raft::incoming_message::*;

    #[test]
    pub fn create_start_id_test() {
        let server_id = 1;
        let start_id = create_start_id(server_id);
        assert_eq!(server_id, get_server_id_from_message(&start_id));
    }
}

