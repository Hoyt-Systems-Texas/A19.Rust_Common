use std::sync::Arc;
use std::sync::atomic::{ AtomicU64, AtomicU32 };
use crate::file::{ MessageFileStoreRead, MessageFileStoreWrite };

/// Writing to the current memory map file messages sent by the client.
/// Readers (StateMachine, Network) Writer (Client)
const JOURNALING_STATE: u32 = 1;
/// Forwarding the messages to the current leader if there is one.
const FORWARDING_STATE: u32 = 2;
/// Copying the journal messages to the in memory ring buffer.
const COPYING_BUFFER: u32 = 3;
/// Changing to forwarding buffer.  Need to wait until all of the writers are out.  Also have to block while we copy the messages to preserve order.
const CHANGING_TO_FORWARDING: u32 = 4;
/// Changing to journaling buffer.  Need to wait until all of the writers are out.  Also have to block while we copy the messages to preserve order.
const CHANGING_TO_JOURNALING: u32 = 5;

struct CommitFile {
    path: String,
    file_number: u32,
    commit_file_size: usize,
    max_message: Arc<AtomicU64>,
    message_file_reader: MessageFileStoreRead,
    message_file_writer: MessageFileStoreWrite,
}

pub struct MessageStream {
    file_storage_directory: String,
    file_prefix: String,
    current_state: AtomicU32,
    current_leader: u32,
}

impl MessageStream {

}
