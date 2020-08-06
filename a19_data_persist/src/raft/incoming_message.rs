use std::sync::atomic::{ AtomicU32, Ordering };
use crate::raft::write_message::*;
use a19_concurrent::buffer::ring_buffer::{
    create_many_to_one, ManyToOneBufferReader, ManyToOneBufferWriter,
};
use rand::{thread_rng, RngCore};
use std::sync::atomic::AtomicU64;

const SERVER_ID_SHIFT: usize = 54;
#[allow(dead_code)]
const RANDOM_SHIFT: usize = 38;
#[allow(dead_code)]
const RANDOM_SHIFT_MASK: u32 = 0x00_00_FF_FF;
const SERVER_ID_MASK: u64 = 0b1111111111000000000000000000000000000000000000000000000000000000;
const MSG_ID_MASK: u64 = 0b0000000000000000000000000011111111111111111111111111111111111111;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum IncomingEvt {
    ElectedLeader{ max_commited_id: u64 },
    Follower { server: u32 },
    NoHandler,
}

const LEADER: u32 = 1;
const LEADER_TO_FOLLOWER: u32 = 2;
const FOLLOWER: u32 = 3;
const FOLLOWER_TO_LEADER: u32 = 4;
const NO_HANDER: u32 = 5;

#[derive(Debug)]
enum IncomingState {
    /// This server is the leader and needs to append message directly to the buffer.
    Leader { writer: MessageWriteAppend },
    /// When we are changing over form leader to follower.  During this time we are copying over the data in the message buffer to the outgoing buffer.
    LeaderToFollower { server_id: u32},
    /// Send messages to the current leader.
    Follower { server_id: u32 },
    /// Copy over the outgoing messages to the message buffer.  During this state not accepting new messages until the copying is done.
    FollowerToLeader,
    /// Hold onto the messages.
    NoHandler,
}

impl PartialEq for IncomingState {
    fn eq(&self, other: &IncomingState) -> bool {
        match self {
            IncomingState::Follower { server_id: c_server_id } => {
                if let IncomingState::Follower { server_id } = other {
                    c_server_id == server_id
                } else {
                    false
                }
            }
            IncomingState::NoHandler => {
                if let IncomingState::NoHandler = other {
                    true
                } else {
                    false
                }
            }
            IncomingState::Leader { writer: _ } => {
                if let IncomingState::Leader { writer: _ } = other {
                    true
                } else {
                    false
                }
            }
            IncomingState::LeaderToFollower{ server_id: c_server_id } => {
                if let IncomingState::LeaderToFollower { server_id } = other {
                    c_server_id == server_id
                } else {
                    false
                }
            }
            IncomingState::FollowerToLeader => {
                if let IncomingState::FollowerToLeader = other {
                    true
                } else {
                    false
                }
            }
        }
    }
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

pub struct IncomingMessageClient {
    /// The incoming buffer we right the messages to.
    incoming_writer: ManyToOneBufferWriter,
    internal_message_id: AtomicU64,
}

/// Creates an incoming message processor.
/// # Arguments
/// `file_storage_directory` - The directory where the message files are stored.
/// `file_prefix` - The file prefix to use for the generated files.
/// `file_size` - The size of the message file.
/// `buffer_size` - The size of the buffer.
/// # Returns
/// A tuple containing the incoming message processor and the client to send messages to the cluster.
pub(crate) fn create_incoming_message_processor(
    server_id: u32,
    file_storage_directory: &str,
    file_prefix: &str,
    file_size: usize,
    buffer_size: usize,
) -> (IncomingMessageProcessor, IncomingMessageClient) {
    let (read, write) = create_many_to_one(buffer_size);
    let collection =
        MessageWriteCollection::open_dir(file_storage_directory, file_prefix, file_size).unwrap();
    let start_id = create_start_id(server_id);
    let processor = IncomingMessageProcessor {
        current_state: IncomingState::NoHandler,
        incoming_queue: read,
        message_writer_collection: collection,
        server_id,
    };
    let client = IncomingMessageClient {
        incoming_writer: write,
        internal_message_id: AtomicU64::new(start_id),
    };
    (processor, client)
}

/// Creates the starting id for the server.  Uses a 16 bit random number to make the likelihood of it repeating on start very low.
/// # Arguments
/// `server_id` - The id of the server to generate the id for.
/// # Returns
/// The id of the server.
fn create_start_id(server_id: u32) -> u64 {
    let server_id = (server_id as u64) << SERVER_ID_SHIFT;
    let mut rng = thread_rng();
    let value = ((rng.next_u32() & RANDOM_SHIFT_MASK) as u64) << RANDOM_SHIFT;
    server_id + value
}

/// Used to get the id of the server.
/// # Arguments
/// `server_msg_id` - The if the server message to get the id for.
/// # Returns
/// The server id of the messaged.
pub fn get_server_id_from_message(server_msg_id: &u64) -> u32 {
    ((server_msg_id & SERVER_ID_MASK) >> SERVER_ID_SHIFT) as u32
}

/// Used to get the id for the message.
/// # Arguments
/// `server_msg_id` - The id of the server msg.
/// # Returns
/// The server id to get the message id.
pub fn get_message_id_from_message(server_msg_id: &u64) -> u64 {
    server_msg_id & MSG_ID_MASK
}

impl IncomingMessageProcessor {
    /// Used to process the incoming event.
    /// # Arguments
    /// `event` - The incoming event to handle.
    fn process_event(&mut self, event: IncomingEvt) {
        match &self.current_state {
            IncomingState::Follower { server_id } => match event {
                IncomingEvt::ElectedLeader{max_commited_id} => {
                    self.change_leader(max_commited_id);
                }
                IncomingEvt::Follower { server } => {
                    self.change_follower(server);
                }
                IncomingEvt::NoHandler => {
                    self.change_no_handler();
                }
            },
            IncomingState::NoHandler => match event {
                IncomingEvt::ElectedLeader{max_commited_id} => {
                    self.change_leader(max_commited_id);
                }
                IncomingEvt::Follower { server } => {
                    self.change_follower(server);
                }
                IncomingEvt::NoHandler => {
                    self.change_no_handler();
                }
            },
            IncomingState::Leader{writer: _} => {
                match event {
                    IncomingEvt::ElectedLeader{max_commited_id} => {
                        // Do nothing
                    }
                    IncomingEvt::Follower { server } => {
                        self.change_follower(server);
                    }
                    IncomingEvt::NoHandler => {
                        self.change_no_handler();
                    }
                }
            },
            IncomingState::FollowerToLeader => {
                
            }
            IncomingState::LeaderToFollower{server_id} => {
                
            }
        }
    }

    fn change_follower(&mut self, leader: u32) {
        if self.server_id == leader {
            panic!("We shouldn't get a leader from our one cluster!");
        } else {
            self.current_state = IncomingState::Follower { server_id: leader };
        }
    }

    fn change_leader(&mut self, max_commit_id: u64) {
        self.current_state = IncomingState::Leader{writer: self.message_writer_collection.get_current_append(max_commit_id).unwrap()};
    }

    fn change_no_handler(&mut self) {
        self.current_state = IncomingState::NoHandler;
    }
}

#[cfg(test)]
mod test {
    use crate::raft::incoming_message::*;
    use serial_test::serial;
    use std::fs::*;
    use std::path::*;

    const FILE_STORAGE_DIRECTORY: &str = "../../../cargo/tests/a19_data_persisted_incoming/";
    const FILE_PREFIX: &str = "incoming";

    fn clean() {
        let path = Path::new(FILE_STORAGE_DIRECTORY);
        if path.exists() {
            remove_dir_all(FILE_STORAGE_DIRECTORY).unwrap();
        }
    }

    #[test]
    #[serial]
    pub fn create_start_id_test() {
        clean();
        let server_id = 1;
        let start_id = create_start_id(server_id);
        assert_eq!(server_id, get_server_id_from_message(&start_id));
        assert_eq!(0, get_message_id_from_message(&start_id));
    }

    #[test]
    #[serial]
    pub fn create_incoming_message_processor_test() {
        clean();
        let (_processor, _client) = create_incoming_message_processor(
            1,
            FILE_STORAGE_DIRECTORY,
            FILE_PREFIX,
            32 * 1000,
            32 * 1000 as usize,
        );
    }

    #[test]
    #[serial]
    pub fn process_event_follower_leader_test() {
        clean();
        let (mut processor, _client) = create_incoming_message_processor(
            1,
            FILE_STORAGE_DIRECTORY,
            FILE_PREFIX,
            (32 * 1000) as usize,
            32 * 1000 as usize,
        );

        processor.current_state = IncomingState::Follower { server_id: 2 };
        processor.process_event(IncomingEvt::ElectedLeader{ max_commited_id:1 });
        let writer = processor.message_writer_collection.get_current_append(1).unwrap();
        assert_eq!(IncomingState::Leader{ writer }, processor.current_state);
    }

    #[test]
    #[serial]
    #[should_panic]
    pub fn process_event_follower_leader_follower_test() {
        clean();
        let (mut processor, _client) = create_incoming_message_processor(
            1,
            FILE_STORAGE_DIRECTORY,
            FILE_PREFIX,
            32 * 1000 as usize,
            32 * 1000 as usize,
        );

        processor.current_state = IncomingState::Follower { server_id: 2 };
        processor.process_event(IncomingEvt::Follower { server: 1 });
        let writer = processor.message_writer_collection.get_current_append(1).unwrap();
        assert_eq!(IncomingState::Leader{writer}, processor.current_state);
    }

    #[test]
    #[serial]
    pub fn process_event_follower_leader_none_test() {
        clean();
        let (mut processor, _client) = create_incoming_message_processor(
            1,
            FILE_STORAGE_DIRECTORY,
            FILE_PREFIX,
            32 * 1000 as usize,
            32 * 1000 as usize,
        );

        processor.current_state = IncomingState::NoHandler;
        processor.process_event(IncomingEvt::ElectedLeader{max_commited_id: 1});
        let writer = processor.message_writer_collection.get_current_append(1).unwrap();
        assert_eq!(IncomingState::Leader{writer}, processor.current_state);
    }

    #[test]
    #[serial]
    pub fn process_event_leader_follower_test() {
        clean();
        let (mut processor, client) = create_incoming_message_processor(
            1,
            FILE_STORAGE_DIRECTORY,
            FILE_PREFIX,
            32 * 1000 as usize,
            32 * 1000 as usize,
        );

        let writer = processor.message_writer_collection.get_current_append(1).unwrap();
        processor.current_state = IncomingState::Leader{ writer };
        processor.process_event(IncomingEvt::Follower { server: 2 });
        assert_eq!(
            IncomingState::Follower { server_id: 2 },
            processor.current_state
        );
    }

    #[test]
    #[serial]
    pub fn process_event_leader_follower_none_test() {
        clean();
        let (mut processor, client) = create_incoming_message_processor(
            1,
            FILE_STORAGE_DIRECTORY,
            FILE_PREFIX,
            32 * 1000 as usize,
            32 * 1000 as usize,
        );

        processor.current_state = IncomingState::NoHandler;
        processor.process_event(IncomingEvt::Follower { server: 2 });
        assert_eq!(
            IncomingState::Follower { server_id: 2 },
            processor.current_state
        );
    }

    #[test]
    #[serial]
    pub fn process_event_leader_none_test() {
        clean();
        let (mut processor, client) = create_incoming_message_processor(
            1,
            FILE_STORAGE_DIRECTORY,
            FILE_PREFIX,
            32 * 1000 as usize,
            32 * 1000 as usize,
        );

        processor.current_state = IncomingState::Leader {
            writer: processor.message_writer_collection.get_current_append(1).unwrap(),
        };
        processor.process_event(IncomingEvt::NoHandler);
        assert_eq!(IncomingState::NoHandler, processor.current_state);
    }

    #[test]
    #[serial]
    pub fn process_event_follower_nohandler_test() {
        clean();
        let (mut processor, client) = create_incoming_message_processor(
            1,
            FILE_STORAGE_DIRECTORY,
            FILE_PREFIX,
            32 * 1000 as usize,
            32 * 1000 as usize,
        );

        processor.current_state = IncomingState::Follower { server_id: 2 };
        processor.process_event(IncomingEvt::NoHandler);
        assert_eq!(IncomingState::NoHandler, processor.current_state);
    }
}
