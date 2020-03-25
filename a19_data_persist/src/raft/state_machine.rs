use crate::file;
use crate::raft::*;
use crate::raft::{CommitFile, TermFile};
use crate::raft::network::{ NetworkSendType, NetworkSend };
use a19_core::current_time_ms;
use a19_concurrent::buffer::mmap_buffer::MemoryMappedInt;
use a19_concurrent::buffer::ring_buffer::{
    create_many_to_one, ManyToOneBufferReader, ManyToOneBufferWriter,
};
use a19_concurrent::buffer::{align, DirectByteBuffer};
use a19_concurrent::queue::mpsc_queue::MpscQueueWrap;
use a19_concurrent::queue::skip_queue::SkipQueueReader;
use a19_concurrent::queue::spsc_queue::SpscQueueSendWrap;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::*;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

/// The internal message id.
pub const internal_message_id: i32 = 1000;
/// A message to indicate a snapshot should be taken.
pub const create_snapshot: i32 = 1001;

/// Internal messages that are committed as terms.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) enum InternalMessage {
    AddServer { server_id: u32 },
    RemoveServer { server_id: u32 },
}

enum RaftState {
    Candidate {
        votes: HashSet<u32>,
    },
    Follower {
        leader: u32,
    },
    Leader {
        next_index: HashMap<u32, u64>,
        match_index: HashMap<u32, u64>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum RaftEvent {
    VoteTimeout,
    LeaderTimeout,
    /// A response back for voting for me.
    VoteForCandiate {
        server_id: u32,
    },
    /// A request to vote for me.
    VoteForMe {
        server_id: u32,
        max_term_id: u64,
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
    FollowerIndex {
        term_id: u64,
        server_id: u32,
    },
    Pong {
        server_id: u32,
        max_term_id: u64,
    },
    Ping {
        server_id: u32,
        max_commited_term: u64,
    },
    NoMessagesTimeout,
    /// The internal message to process after it has been committed.  This is internal messages received over the queue.
    ProcessInternalMessage {
        msg: InternalMessage,
    },
}

/// Represents a connected server in the clusters.  Servers can be added and removed will the cluster is running.
struct ConnectedServer {
    /// The id of the server that is connected.  The other connection information is handled by the communication library.
    server_id: u32,
}

struct CommitFileInfo {
    file_storage_directory: String,
    file_prefix: String,
    commit_file_size: usize,
    file_collection: Arc<FileCollection>,
    max_message: Arc<AtomicU64>,
}

struct WriteFileInfo {
    file_storage_directory: String,
    file_prefix: String,
    file_id_start: u32,
    event_file_size: usize,
    pending_write_queue: ManyToOneBufferReader,
}

pub(crate) struct RaftStateMachine {
    server_id: u32,
    current_state: RaftState,
    /// The current term we have written
    current_term_id: u64,
    last_appended_term_id: u64,
    voted_for: Option<u32>,
    connected_server: HashMap<u32, ConnectedServer>,
    leader_votes: HashSet<u32>,
    message_queue: SkipQueueReader<RaftEvent>,
    commit_term_file: TermFile,
    new_term_file: TermFile,
    commit_file_size: usize,
    //pending_write_queue: ManyToOneBufferWriter,
    server_count: u32,
    leader: u32,
    state_message_queue_writer: SpscQueueSendWrap<NetworkSendType>,
    max_message_id: Arc<AtomicU64>,
    /// The current max committed term.
    current_commited_term: u64,
    votes_required: u32,
    file_storage_directory: String,
    file_prefix: String,
}

/// The state machine client use to communicate with the raft state machine.
pub struct RaftStateMachineClient {
    server_id: u32,
    internal_message_id: AtomicU64,
    buffer_reader: ManyToOneBufferWriter,
    state_machine_thread: Option<JoinHandle<u32>>,
    client_message_writer: ManyToOneBufferWriter,
    state_message_queue: Arc<MpscQueueWrap<RaftEvent>>,
    max_message_id: Arc<AtomicU64>,
}

impl RaftStateMachineClient {
    /// Gets the id of the server.
    pub fn server_id(&self) -> &u32 {
        &self.server_id
    }

    /// Used to get the max message id.
    pub(crate) fn max_message_id(&self) -> Arc<AtomicU64> {
        self.max_message_id.clone()
    }

    /// Send an event to the state machine.
    /// # Arguments
    /// `event` - The event to be processed by the state machine.
    pub(crate) fn send_event(&self, event: RaftEvent) -> bool {
        self.state_message_queue.offer(event)
    }

    /// Stops the state machine.
    pub(crate) fn stop(&mut self) {
        if let Some(join) = self.state_machine_thread.take() {
            self.state_message_queue.offer(RaftEvent::Stop);
            join.join().unwrap();
        }
    }
}

/// Creates the state machine and returns the client to communicate with it.
/// # Arguments
/// `server_id` - The id of the server.
/// `file_storage_directory` - The location of the files we are storing.
/// `file_prefix` - The prefix for the files we are storing.
/// `commit_file_size` - The file size for the commit file.
/// `start_servers` - The ids of the starting servers.
fn create_state_machine(
    server_id: u32,
    file_storage_directory: String,
    file_prefix: String,
    commit_file_size: usize,
    starting_servers: HashSet<u32>,
    file_collection: Arc<FileCollection>,
) {
    let max_message = AtomicU64::new(0);
    let (commit_term, max_commit_term) = match find_last_commit_pos(&file_collection.commit_files) {
        LastCommitPos::NoCommits => {
            let file_id = 1;
            let file_name = create_commit_name(&file_storage_directory, &file_prefix, &file_id);
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&file_name);
            let map = unsafe {
                MemoryMappedInt::new(
                    &file_name,
                    align(commit_file_size, COMMIT_SIZE as usize),
                )
                .unwrap()
            };
            let term = TermFile::new(map, 1, file_id);
            (term, 0)
        }
        LastCommitPos::LastCommit {
            start_term_id,
            term_id,
            file_id,
            max_message_id,
            path,
        } => {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(false)
                .open(&path)
                .unwrap();
            let map = unsafe { MemoryMappedInt::open(file).unwrap() };
            max_message.store(max_message_id, atomic::Ordering::Relaxed);
            // Get the current term we need to find.
            let term = TermFile::new(map, start_term_id, file_id);
            (term, term_id)
        }
    };
    let (new_term, last_term) = match find_last_term(&file_collection.commit_files) {
        LastTermPos::Pos {
            file_id,
            last_term,
            term_start,
        } => {
            let path = create_commit_name(&file_storage_directory, &file_prefix, &file_id);
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(false)
                .open(&path)
                .unwrap();
            let map = unsafe { MemoryMappedInt::open(file).unwrap() };
            (TermFile::new(map, term_start, file_id), last_term)
        }
        LastTermPos::NoTerms => {
            let path = create_commit_name(&file_storage_directory, &file_prefix, &1);
            let map = unsafe { MemoryMappedInt::new(&path, commit_file_size).unwrap() };
            (TermFile::new(map, 1, 1), 0)
        }
    };
    if last_term != max_commit_term || new_term.file_id != commit_term.file_id {
        panic!("Terms must match.  This node must have run in cluster node.")
    } else {
        let current_term = max_commit_term;
        let (message_file, read_pos, read_file_id) = if current_term == 0 {
            // New file so we don't need to do much.
            let path = create_event_name(&file_storage_directory, &file_prefix, &1);
            (
                unsafe { MessageFileStore::open_readonly(&path).unwrap() },
                0,
                1,
            )
        } else {
            let term_pos = match commit_term.calculate_pos(&max_commit_term) {
                TermPosResult::Pos(pos) => pos,
                _ => {
                    panic!("Bug in finding the position to read in.");
                }
            };
            let msg_file_id = commit_term.buffer.file_id(&term_pos);
            let position = commit_term.buffer.file_position_offset(&term_pos) as usize
                + commit_term.buffer.length_of_commit(&term_pos) as usize;
            let path = create_event_name(&file_storage_directory, &file_prefix, &msg_file_id);
            (
                unsafe { MessageFileStore::open_readonly(&path).unwrap() },
                position,
                msg_file_id,
            )
        };
        // now need to store the information
    }
}

/// Kicks off the thread to process the state machine events.
fn run_state_machine(state_machine: RaftStateMachine) -> JoinHandle<u32> {
    thread::spawn(move || {
        let mut state_machine = state_machine;
        loop {
            if let Some(msg) = state_machine.message_queue.poll() {
                state_machine.process_event(msg);
            } else {
                // TODO pause for a little bit.
            }
        }
        1
    })
}

impl RaftStateMachine {

    fn send_leader_request(&mut self) {
        self.state_message_queue_writer.offer(
            NetworkSendType::Broadcast{
                msg: NetworkSend::RaftEvent(
                    RaftEvent::VoteForMe{
                        server_id: self.server_id,
                        max_term_id: self.current_commited_term
                    }
                )
            }
        );
    }

    fn handle_internal_message(&mut self, msg: InternalMessage) {
        match msg {
            InternalMessage::AddServer { server_id } => {
                let connected_server = ConnectedServer { server_id };
                self.connected_server.insert(server_id, connected_server);
            }
            InternalMessage::RemoveServer { server_id } => {
                self.connected_server.remove(&server_id);
            }
        }
    }

    /// Handles the request for vote for me.
    /// # Arguments
    /// `server_id` - The id of the server to vote for.
    /// `max_term_id` - The max term of the id.
    fn handle_vote_for_me(&mut self, server_id: u32, max_term_id: u64) {
        if self.voted_for.is_none() {
            self.state_message_queue_writer.offer(NetworkSendType::Single{
                msg: NetworkSend::RaftEvent(
                    RaftEvent::VoteForCandiate{server_id: self.server_id}
                ),
                server_id
            });
            self.voted_for = Some(self.server_id);
        }
    }

    /// Sends a ping to other servers.
    fn send_ping(&mut self) {
        self.state_message_queue_writer.offer(NetworkSendType::Broadcast{
            msg: NetworkSend::RaftEvent(
                RaftEvent::Ping {
                    server_id: self.server_id,
                    max_commited_term: self.current_commited_term
                }
            )
        });
    }

    /// Handles a commit from a leader.
    /// # Arguments
    /// `term_id` - The term to commit. 
    fn handle_commit_term(&mut self, term_id: u64) {
        if term_id > self.current_commited_term {
            let mut current_term = self.current_commited_term + 1;
            loop {
                if current_term <= term_id {
                    self.current_commited_term = term_id;
                    if current_term <= self.current_commited_term {
                        loop {
                            // Update to that term as committed :)
                            match self.commit_term_file.calculate_pos(&current_term) {
                                TermPosResult::Pos(pos) => {
                                    self.commit_term_file.buffer.set_committed(&pos);
                                    self.commit_term_file.buffer.set_committed_timestamp(
                                        &pos,
                                        &current_time_ms());
                                    self.current_term_id = current_term;
                                    current_term += 1;
                                    break;
                                }
                                TermPosResult::Overflow => {
                                    // Goto the next file.
                                    let next_file_id = self.commit_term_file.file_id + 1;
                                    let new_start_term = self.commit_term_file.term_end + 1;
                                    self.commit_term_file = create_term_file(
                                        &self.file_storage_directory,
                                        &self.file_prefix,
                                        next_file_id,
                                        new_start_term,
                                        self.commit_file_size);
                                }
                                TermPosResult::Underflow => {
                                    panic!("We should never underflow when updating terms!");
                                }
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
        }
    }

    fn process_event(&mut self, event: RaftEvent) {
        match &mut self.current_state {
            RaftState::Candidate { votes } => {
                match event {
                    RaftEvent::VoteForCandiate { server_id } => {
                        votes.insert(server_id);
                        if votes.len() >= self.votes_required as usize {
                            self.current_state = RaftState::Leader {
                                next_index: HashMap::with_capacity(10),
                                match_index: HashMap::with_capacity(10),
                            };
                            // We won let the others know.
                            self.state_message_queue_writer.offer(
                                NetworkSendType::Broadcast {
                                    msg: NetworkSend::RaftEvent(
                                        RaftEvent::ElectedLeader{
                                        server_id: self.server_id
                                        }
                                    )
                                });
                        }
                        // Check to see if we have enough votes.
                    }
                    RaftEvent::VoteForMe {
                        server_id,
                        max_term_id,
                    } => {
                        if self.current_term_id <= max_term_id {
                            // Vote for Candidate.
                            self.handle_vote_for_me(server_id, max_term_id);
                        }
                    }
                    RaftEvent::ClientMessageReceived => {
                        self.message_queue.skip(event);
                    }
                    RaftEvent::ElectedLeader { server_id } => {
                        self.leader = server_id;
                        self.current_state = RaftState::Follower {
                            leader: self.leader
                        };
                    }
                    RaftEvent::VoteTimeout => {
                        self.voted_for = None;
                        votes.clear();
                        votes.insert(self.server_id);
                        self.send_leader_request();
                    }
                    RaftEvent::ProcessInternalMessage { msg } => {
                        // This should never happen since we can't commit while a candidate!
                    }
                    _ => {
                        // Ignore the rest of the events.
                    }
                }
            }
            RaftState::Follower { leader } => {
                match event {
                    RaftEvent::ClientMessageReceived => {
                        // Forward to the server.
                    }
                    RaftEvent::FollowerIndex { server_id, term_id } => {
                        // Ignore
                    }
                    RaftEvent::Commited { term_id } => {
                        self.handle_commit_term(term_id);
                    }
                    RaftEvent::ElectedLeader { server_id } => {
                        self.voted_for = None;
                        self.leader = server_id;
                    }
                    RaftEvent::LeaderTimeout => {
                        // try to become leader.
                        let mut votes = HashSet::with_capacity(self.server_count as usize);
                        votes.insert(self.server_id);
                        self.current_state = RaftState::Candidate {
                            votes,
                        };
                        self.send_leader_request();
                    }
                    RaftEvent::Stop => {
                    }
                    RaftEvent::VoteForCandiate { server_id } => {
                        // We should get this see we are following someone :(
                    }
                    RaftEvent::VoteForMe {
                        server_id,
                        max_term_id,
                    } => {
                        if self.current_term_id <= max_term_id {
                            // Vote for this candidate if we haven't already voted.  Need to figure out how to keep track of this.
                            self.handle_vote_for_me(server_id, max_term_id);
                        }
                    }
                    RaftEvent::Pong {
                        server_id,
                        max_term_id,
                    } => {
                        // We aren't processing these since we aren't the leader.
                    }
                    RaftEvent::Ping { max_commited_term, server_id } => {
                    }
                    RaftEvent::NoMessagesTimeout => {
                        // Do nothing since this should be a leader timeout.
                    }
                    RaftEvent::VoteTimeout => {
                        // Clear out who we voted for.
                        self.voted_for = None;
                    }
                    RaftEvent::ProcessInternalMessage { msg } => {
                        self.handle_internal_message(msg);
                    }
                }
            }
            RaftState::Leader {
                next_index,
                match_index,
            } => {
                match event {
                    RaftEvent::ClientMessageReceived => {
                        // Go a new possible term.
                    }
                    RaftEvent::FollowerIndex { server_id, term_id } => {
                        next_index.insert(server_id, term_id + 1);
                    }
                    RaftEvent::Commited { term_id } => {
                        // Would be an error since we are sending these :(
                    }
                    RaftEvent::ElectedLeader { server_id } => {
                        if self.server_id != server_id {
                            self.current_state =
                                RaftState::Follower { leader: server_id };
                        }
                    }
                    RaftEvent::LeaderTimeout => {
                        // We are the leader so this shouldn't ever happen.
                    }
                    RaftEvent::Stop => {
                        
                    },
                    RaftEvent::VoteForMe {
                        server_id,
                        max_term_id,
                    } => {
                        self.handle_vote_for_me(server_id, max_term_id);
                    }
                    RaftEvent::VoteForCandiate { server_id } => {
                        // Ignore invalid event.
                    }
                    RaftEvent::Pong {
                        server_id,
                        max_term_id,
                    } => {
                        if let Some(current_max) = match_index.get(&server_id) {
                            if *current_max > max_term_id {
                                match_index.insert(server_id, max_term_id);
                                let mut next_commit_term = self.current_commited_term + 1;
                                loop {
                                    let mut votes = 1; // Need to have us as 1 vote :);
                                    for c in match_index.values() {
                                        if *c >= next_commit_term {
                                            votes += 1;
                                        }
                                    }
                                    if self.votes_required <= votes {
                                        self.current_commited_term = next_commit_term;
                                        // Send a message to the listeners :)
                                        self.state_message_queue_writer.offer(
                                            NetworkSendType::Broadcast {
                                                msg: NetworkSend::RaftEvent(
                                                    RaftEvent::Commited {
                                                        term_id: self.current_commited_term
                                                    })
                                            }
                                        );
                                        next_commit_term += 1;
                                    } else {
                                        // Nothing to commit :)
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    RaftEvent::Ping { max_commited_term, server_id } => {
                        // Ignore we are the leader and shouldn't be getting a pong.
                    }
                    RaftEvent::NoMessagesTimeout => {
                        // Send ping to the followers.
                        self.send_ping();
                    }
                    RaftEvent::VoteTimeout => {
                        // Ignore we are the leader so this shouldn't happen.
                    }
                    RaftEvent::ProcessInternalMessage { msg } => {
                        // Process the internal message.
                        self.handle_internal_message(msg);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {

    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use crate::raft::*;
    use std::collections::{ HashMap, HashSet };
    use a19_concurrent::queue::skip_queue::create_skip_queue;
    use crate::raft::state_machine::*;
    use crate::raft::network::*;
    
    const FILE_STORAGE_DIRECTORY: &str = "/home/mrh0057/Raft_State_Machine_Test";
    const FILE_PREFIX: &str = "state_machine_test";
    const COMMIT_FILE_SIZE: usize = (COMMIT_SIZE * 128) as usize;
    const EVENT_FILE_SIZE: usize = (EVENT_HEADER_SIZE * 1024) as usize;

    /// Used to crate the state machine.
    fn create_state_machine() -> (RaftStateMachine, NetworkInfo) {
        let (event_writer, event_reader) = create_skip_queue(1024);
        let (net_writer, net_reader) = SpscQueueSendWrap::new(1024);
        let mut raft_state_machine = RaftStateMachine {
            server_id: 1,
            current_state: RaftState::Candidate{votes: HashSet::new()},
            current_term_id: 0,
            last_appended_term_id: 0,
            voted_for: None,
            connected_server: HashMap::with_capacity(1),
            leader_votes: HashSet::new(),
            message_queue: event_reader,
            commit_term_file: create_term_file(
                FILE_STORAGE_DIRECTORY,
                FILE_PREFIX,
                1,
                1,
                COMMIT_FILE_SIZE as usize),
            new_term_file: create_term_file(
                FILE_STORAGE_DIRECTORY,
                FILE_PREFIX,
                1,
                1,
                COMMIT_FILE_SIZE as usize),
            commit_file_size: COMMIT_SIZE as usize,
            server_count: 3,
            leader: 0,
            state_message_queue_writer: net_writer,
            max_message_id: Arc::new(AtomicU64::new(0)),
            current_commited_term: 0,
            votes_required: 2,
            file_storage_directory: FILE_STORAGE_DIRECTORY.to_owned(),
            file_prefix: FILE_PREFIX.to_owned(),
        };
        raft_state_machine.connected_server.insert(1, ConnectedServer{
            server_id: 1
        });
        raft_state_machine.connected_server.insert(2, ConnectedServer{
            server_id: 2
        });
        raft_state_machine.connected_server.insert(3, ConnectedServer{
            server_id: 3
        });

        let net = NetworkInfo {
            net_reader
        };
        (raft_state_machine, net)
    }

    #[test]
    fn test_join_with_other_leader() {
        let (mut state_machine, _) = create_state_machine();
        state_machine.process_event(
            RaftEvent::ElectedLeader{server_id: 2}
        );
        match state_machine.current_state {
            RaftState::Follower{leader} => {
                assert_eq!(2, leader);
            },
            _ => {
                assert!(false);
            }
        }
    }

    #[test]
    fn test_leader_timeout() {
        let (mut state_machine, mut net) = create_state_machine();
        state_machine.process_event(
            RaftEvent::ElectedLeader{server_id: 2}
        );
        state_machine.process_event(
            RaftEvent::LeaderTimeout
        );
        if let Some(top) = net.net_reader.poll() {
            match top {
                NetworkSendType::Broadcast{msg} => {
                    match msg {
                        NetworkSend::RaftEvent(evt) => {
                            match evt {
                                RaftEvent::VoteForMe{max_term_id, server_id} => {
                                    assert_eq!(1, server_id);
                                }
                                _ => {
                                    assert!(false);
                                }
                            }
                        }
                        _ => {
                            assert!(false);
                        }
                    }
                },
                _ => {
                    assert!(false);
                }
            }
        } else {
            assert!(false);
        }

        match &state_machine.current_state {
            RaftState::Candidate{votes} => {
                
            },
            _ => {
                assert!(false);
            }
        }

        state_machine.process_event(RaftEvent::VoteForCandiate{server_id: 2});
        if let Some(top) = net.net_reader.poll() {
            match top {
                NetworkSendType::Broadcast{msg} => {
                    match msg {
                        NetworkSend::RaftEvent(evt) => {
                            match evt {
                                RaftEvent::ElectedLeader{server_id} => {
                                    assert_eq!(1, server_id);
                                }
                                _ => {
                                    assert!(false);
                                }
                            }
                        }
                        _ => {
                            assert!(false);
                        }
                    }
                },
                _ => {
                    assert!(false);
                }
            }
        } else {
            assert!(false);
        }

        match &state_machine.current_state {
            RaftState::Leader{match_index, next_index} => {
                
            },
            _ => {
                assert!(false);
            }
        }
    }

    #[test]
    pub fn commit_term_client() {
        let (mut state_machine, mut net) = create_state_machine();
        let term_id = 1;
        let pos = match state_machine.commit_term_file.calculate_pos(&term_id) {
            TermPosResult::Pos(pos) => pos,
            _ => {
                panic!("Should be in range!");
            }
        };

        let zeros = [0; COMMIT_FILE_SIZE];
        state_machine.commit_term_file.buffer.write_bytes(&0, &zeros);
        state_machine.current_state = RaftState::Follower{leader: 2};
        state_machine.process_event(
            RaftEvent::Commited{term_id});

        let commited = state_machine.commit_term_file.buffer.committed(&pos);
        let time_stamp = state_machine.commit_term_file.buffer.committed_timestamp(&pos);
        assert!(commited > 0);
        assert!(time_stamp > 0);
    }

    struct NetworkInfo {
        net_reader: SpscQueueReceiveWrap<NetworkSendType>,
    }
}
