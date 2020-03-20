use crate::raft::{CommitFile, TermFile};
use a19_concurrent::buffer::mmap_buffer::MemoryMappedInt;
use a19_concurrent::queue::mpsc_queue::MpscQueueWrap;
use a19_concurrent::queue::skip_queue::SkipQueueReader;
use std::collections::{ HashMap, HashSet };
use std::sync::atomic::AtomicU64;
use std::thread::{self, JoinHandle};
use serde::{Deserialize, Serialize};
use a19_concurrent::buffer::ring_buffer::{ManyToOneBufferReader, ManyToOneBufferWriter, create_many_to_one};

/// The internal message id.
pub const internal_message_id: i32 = 1000;

/// Internal messages that are committed as terms.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum InternalMessage {
    AddServer,
    RemoveServer,
}

enum RaftMessageResponse {
    Accepted,
}

enum RaftState {
    Candidate{votes: HashSet<u32>},
    Follower {
        leader: u32,
    },
    Leader {
        next_index: HashMap<u32, u64>,
        match_index: HashMap<u32, u64>,
    },
}

#[derive(Debug)]
enum RaftEvent {
    VoteTimeout,
    LeaderTimeout,
    /// A vote to become the leader has been received.
    VoteRecieved {
        server_id: u32,
        /// The last term on the host.
        last_term_id: u64,
    },
    /// The server has been elected the leader.
    ElectedLeader {
        server_id: u32,
    },
    /// A message from a client has been received.  There needs to be a future attached so we can signal the message has been committed.
    ClientMessageReceived,
    /// The id of the term that has been committed.
    Commited {
        term_id: u64,
    },
    /// Stop the state machine from running.
    Stop,
    FollowerIndex{term_id: u64, server_id: u32},
    Pong{server_id: u32, max_term_id: u64},
    Ping{max_commited_term: u64},
    NoMessagesTimeout,
    /// The internal message to process after it has been committed.
    ProcessInternalMessage{msg: InternalMessage},
}

/// Represents a connected server in the clusters.  Servers can be added and removed will the cluster is running.
struct ConnectedServer {
    server_id: u32,
}

struct RaftStateMachine {
    server_id: u32,
    current_state: RaftState,
    current_term_id: u64,
    last_appended_term_id: u64,
    voted_for: Option<u32>,
    connected_server: HashMap<u32, ConnectedServer>,
    leader_votes: HashMap<u32, u32>,
    message_queue: SkipQueueReader<RaftEvent>,
    buffer_reader: ManyToOneBufferReader,
    commit_term_file: TermFile,
    new_term_file: TermFile,
    term_file_size: usize,
    client_message_reader: ManyToOneBufferReader,
    server_count: u32,
    leader: u32,
}

pub struct RaftStateMachineClient {
    internal_message_id: AtomicU64,
    buffer_reader: ManyToOneBufferWriter,
    state_machine_thread: JoinHandle<u32>,
    read_thread: JoinHandle<u32>,
    term_file: TermFile,
    client_message_writer: ManyToOneBufferWriter,
}

fn run_state_machine(state_machine: RaftStateMachine) -> JoinHandle<u32> {
    thread::spawn(move || {
        let mut state_machine = state_machine;
        loop {
            if let Some(msg) = state_machine.message_queue.poll() {
                match &mut state_machine.current_state {
                    RaftState::Candidate{ votes } => {
                        match msg {
                            RaftEvent::VoteRecieved{server_id, last_term_id} => {
                                // Increment 
                            }
                            RaftEvent::ClientMessageReceived => {
                                state_machine.message_queue.skip(msg);
                            }
                            RaftEvent::ElectedLeader{ server_id } => {
                                if server_id == state_machine.server_id {
                                    
                                }
                            }
                            RaftEvent::VoteTimeout => {
                                votes.clear();
                                state_machine.send_leader_request();
                            }
                            RaftEvent::ProcessInternalMessage{msg} => {
                                // This should never happen since we can't commit while a candidate!
                            }
                            _ => {
                                // Ignore the rest of the events.
                            }
                        }
                    }
                    RaftState::Follower{leader} => {
                        match msg {
                            RaftEvent::ClientMessageReceived => {
                                
                            }
                            RaftEvent::FollowerIndex {server_id, term_id} => {
                                // Ignore
                            }
                            RaftEvent::Commited{term_id} => {
                                
                            }
                            RaftEvent::ElectedLeader {server_id} => {
                                state_machine.leader = server_id;
                            }
                            RaftEvent::LeaderTimeout => {
                                // try to become leader.
                                state_machine.current_state = RaftState::Candidate{votes: HashSet::with_capacity(
                                    state_machine.server_count as usize)
                                };
                                state_machine.send_leader_request();
                            }
                            RaftEvent::Stop => {
                                
                            }
                            RaftEvent::VoteRecieved{server_id, last_term_id} => {
                                if state_machine.current_term_id >= last_term_id {
                                    // Vote for this candidate if we haven't already voted.  Need to figure out how to keep track of this.
                                }
                            }
                            RaftEvent::Pong{server_id, max_term_id} => {
                                
                            }
                            RaftEvent::Ping{max_commited_term} => {
                                
                            }
                            RaftEvent::NoMessagesTimeout => {
                                // Do nothing since this should be a leader timeout.
                            }
                            RaftEvent::VoteTimeout => {
                                // Ignore
                            }
                            RaftEvent::ProcessInternalMessage{msg} => {
                                
                            }
                        }
                    }
                    RaftState::Leader{ next_index, match_index } => {
                        match msg {
                            RaftEvent::ClientMessageReceived => {
                                // Go a new possible term.
                            }
                            RaftEvent::FollowerIndex{server_id, term_id} => {
                                next_index.insert(server_id, term_id + 1);
                            }
                            RaftEvent::Commited{term_id} => {
                                // Would be an error since we are sending these :(
                            }
                            RaftEvent::ElectedLeader {server_id} => {
                                if state_machine.server_id != server_id {
                                    state_machine.current_state = RaftState::Follower{leader: server_id};
                                }
                            }
                            RaftEvent::LeaderTimeout => {
                                // We are the leader so this shouldn't ever happen.
                            }
                            RaftEvent::Stop => {
                                break
                            }
                            RaftEvent::VoteRecieved{server_id, last_term_id} => {
                                // Not sure what to do if this happens.
                            }
                            RaftEvent::Pong{server_id, max_term_id} => {
                                match_index.insert(server_id, max_term_id);
                            }
                            RaftEvent::Ping{max_commited_term} => {
                                
                            }
                            RaftEvent::NoMessagesTimeout => {
                                // Send ping to the followers.
                            }
                            RaftEvent::VoteTimeout => {
                                // Ignore
                            }
                            RaftEvent::ProcessInternalMessage{msg} => {
                                // Process the internal message.
                            }
                        }
                    }
                }
            } else {
                // TODO pause for a little bit.
            }
        }
        1
    })
}

impl RaftStateMachine {
    
    fn send_leader_request(&mut self) {
        
    }
}
