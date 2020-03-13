use crate::raft::{CommitFile, TermFile};
use a19_concurrent::buffer::mmap_buffer::MemoryMappedInt;
use a19_concurrent::buffer::ring_buffer::{ManyToOneBufferReader, ManyToOneBufferWriter};
use a19_concurrent::queue::mpsc_queue::MpscQueueWrap;
use a19_concurrent::queue::skip_queue::SkipQueueReader;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::thread::{self, JoinHandle};

const ADD_SERVER_MSG_TYPE: i32 = -1;
const REMOVE_SERVER_MSG_TYPE: i32 = -2;

enum RaftMessageResponse {
    Accepted,
}

enum RaftState {
    Candidate,
    Follower {
        leader: u32,
    },
    Leader {
        next_index: HashMap<u32, u64>,
        match_index: HashMap<u32, u64>,
    },
}

enum RaftEvent {
    LeaderTimeout,
    /// A vote to become the leader has been received.
    VoteRecieved {
        server_id: u32,
    },
    /// The server has been elected the leader.
    ElectedLeader {
        server_id: u32,
    },
    /// A new message has been received.
    InternalMessageReceived {
        body: Vec<u8>,
    },
    /// A message from a client has been received.  There needs to be a future attached so we can signal the message has been commited.
    ClientMessageReceived,
    /// The id of the sterm that has been commited.
    Commited {
        term_id: u64,
    },
    /// Stop the state machine from running.
    Stop,
}

/// Represents a connected server in the clusters.  Servers can be added and removed will the cluster is running.
struct ConnectedServer {
    server_id: u32,
}

struct RaftStateMachine {
    current_state: RaftState,
    current_term_id: u64,
    last_appended_term_id: u64,
    voted_for: Option<u32>,
    connected_server: HashMap<u32, ConnectedServer>,
    message_queue: SkipQueueReader<RaftEvent>,
    buffer_reader: ManyToOneBufferReader,
    commit_term_file: TermFile,
    new_term_file: TermFile,
    term_file_size: usize,
}

pub struct RaftStateMachineClient {
    internal_message_id: AtomicU64,
    buffer_reader: ManyToOneBufferWriter,
    state_machine_thread: JoinHandle<u32>,
    read_thread: JoinHandle<u32>,
    term_file: TermFile,
}

fn run_state_machine(state_machine: RaftStateMachine) -> JoinHandle<u32> {
    thread::spawn(move || {
        loop {}
        1
    })
}
