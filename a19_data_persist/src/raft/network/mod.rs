//! A high level networking protocol to use for log replication.  The design goal is to have
//! something distributed and fast.  Since this applicaiton goal is to be distributed accross
//! networks and maybe cloud providers need to have a replicate registration of ipv6 addresss to
//! make whole punching unecessary.
//!
//! The ip address of the server and/or their names of the servers.  Requires a registration
//! service and log replication.  The goal is to only have 2 nodes per a location.  Every message
//! needs the size of the frame.  To indicate a bad frame due to node failure, set the message
//! length in the buffer to u32::MAX.
//!
//! # Basic Message Layout
//!
//! Every message starts with the following header.  Each message is aligned on a 32 byte
//! boundary.
//!
//! ```text
//!  0                   1                   2                   3
//!  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! | Frame size                                                    |
//! +-----------------+---------------------------------------------+
//! | Version       | Flags         | Type                          |
//! +---------------+---------------+-------------------------------+
//! | Server Id                                                     |
//! +---------------------------------------------------------------+
//! ```
//! # Setup Frame
//!
//! A setup frame is used to setup the connection to a node.  Since we are using raft we need to
//! know who the current leader is, if there isn't a current leader need to do a leader election.
//! We need to distribute the server address to all of the servers.  I think the best option is to
//! have a message that registers a new server to the cluster which can call any server and then
//! download the server list.  Each server needs to have the active server list in order for this
//! to work.  I don't think we can use multicast since the ip address will not be on the same
//! domain.
//! ```text
//!  0                   1                   2                   3
//!  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! | Frame size                                                    |
//! +---------------+---------------+-------------------------------+ 32
//! | Version       | Flags         | Type                          |
//! +---------------+---------------+-------------------------------+ 64
//! | Server Id                                                     |
//! +---------------------------------------------------------------+ 96
//! | Initial Term Id                                               |
//! +---------------------------------------------------------------+ 128
//! | Active Term Id                                                |
//! +---------------------------------------------------------------+ 160
//! | Term Length Max                                               |
//! +---------------------------------------------------------------+ 192
//! | Not Used                                                      |
//! |                                                               | 224
//! |                                                               |
//! +---------------------------------------------------------------+ 256
//! ```
//! * Servier Id - The id of the server sending the message.  This must be a unique identifier for
//! the server.
//! * Version - The offset for the term.
//! * Flags - Not used for this message.
//! * Server Id - The internal server id.  This value is unqiue to all of the servers.
//! * Initial Term Id - The initial id of the term.
//! * Active Term Id - The current active term id.
//! * Term Length Max - The maximum term length.  Must be the same for all of the nodes.  Also has
//! to be a power of 2.
//! * MTU - The maximum size of a data gram.
//!
//! # Setup Response
//!
//! ```text
//!  0                   1                   2                   3
//!  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! | Frame size                                                    |
//! +---------------+---------------+-------------------------------+ 32
//! | Version       | Flags         | Type                          |
//! +---------------+---------------+-------------------------------+ 64
//! | Server Id                                                     |
//! +---------------------------------------------------------------+ 96
//! | Initial Term Id                                               |
//! +---------------------------------------------------------------+ 128
//! | Active Term Id                                                |
//! +---------------------------------------------------------------+ 160
//! | Term Length Max                                               |
//! +---------------------------------------------------------------+ 192
//! |                                                               |
//! |                                                               | 224
//! | Not Used                                                      |
//! +---------------------------------------------------------------+ 256
//!
//! ```
//! # Data Frame
//! ```text
//!  0                   1                   2                   3
//!  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! | Frame size                                                    |
//! +---------------+---------+-+-+-+-------------------------------+ 32
//! | Version       | Flags   |E| |S| Type                          |
//! +---------------+---------+-+-+-+-------------------------------+ 64
//! | Server Id                                                     |
//! +---------------------------------------------------------------+ 96
//! | Term id                                                       |
//! |                                                               | 128
//! |                                                               |
//! +---------------------------------------------------------------+ 160
//! | File Id                                                       |
//! +---------------------------------------------------------------+ 192
//! | File offset                                                   |
//! |                                                               | 224
//! |                                                               |
//! +---------------------------------------------------------------+ 256
//! ... Data                                                        |
//! |                                                               ...
//! +---------------------------------------------------------------+
//!
//! ```
//!
//! # Commit Frame
//!
//! ```text
//!  0                   1                   2                   3
//!  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! | Frame size                                                    |
//! +---------------+---------------+-------------------------------+ 32
//! | Version       | Flags         | Type                          |
//! +---------------+---------------+-------------------------------+ 64
//! | Server Id                                                     |
//! +---------------------------------------------------------------+ 96
//! | Term Id                                                       |
//! |                                                               | 128
//! |                                                               |
//! +---------------------------------------------------------------+ 160
//! |                                                               |
//! |                                                               | 192
//! |                                                               |
//! |                                                               | 224
//! | Not Used                                                      |
//! +---------------------------------------------------------------+ 256
//! ```
//!
//! # Vote Frame
//!
//! ```text
//!  0                   1                   2                   3
//!  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! | Frame size                                                    |
//! +---------------+---------------+-------------------------------+ 32
//! | Version       | Flags         | Type                          |
//! +---------------+---------------+-------------------------------+ 64
//! | Server Id                                                     |
//! +---------------------------------------------------------------+ 96
//! | Vote for Server Id                                            |
//! +---------------------------------------------------------------+ 128
//! |                                                               |
//! |                                                               | 160
//! |                                                               |
//! |                                                               | 192
//! |                                                               |
//! |                                                               | 224
//! | Not Used                                                      |
//! +---------------------------------------------------------------+ 256
//! ```
//!
//! # Vote for me
//!
//! ```text
//!  0                   1                   2                   3
//!  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! | Frame size                                                    |
//! +---------------+---------------+-------------------------------+ 32
//! | Version       | Flags         | Type                          |
//! +---------------+---------------+-------------------------------+ 64
//! | Server Id                                                     |
//! +---------------------------------------------------------------+ 96
//! | Max Term Id                                                   |
//! |                                                               | 128
//! |                                                               |
//! +---------------------------------------------------------------+ 160
//! |                                                               |
//! |                                                               | 192
//! |                                                               |
//! |                                                               | 224
//! | Not Used                                                      |
//! +---------------------------------------------------------------+ 256
//! ```
//! # Elected leader
//!
//! ```text
//!  0                   1                   2                   3
//!  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! | Frame size                                                    |
//! +---------------+---------------+-------------------------------+ 32
//! | Version       | Flags         | Type                          |
//! +---------------+---------------+-------------------------------+ 64
//! | Server Id                                                     |
//! +---------------------------------------------------------------+ 96
//! |                                                               |
//! |                                                               | 128
//! |                                                               |
//! |                                                               | 160
//! |                                                               |
//! |                                                               | 192
//! |                                                               |
//! |                                                               | 224
//! | Not Used                                                      |
//! +---------------------------------------------------------------+ 256
//! ```
//! # Follower Index
//!
//! The current follower index information.
//! ```text
//!  0                   1                   2                   3
//!  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! | Frame size                                                    |
//! +---------------+---------------+-------------------------------+ 32
//! | Version       | Flags         | Type                          |
//! +---------------+---------------+-------------------------------+ 64
//! | Server Id                                                     |
//! +---------------------------------------------------------------+ 96
//! | Max Committed Term Id                                         |
//! |                                                               | 128
//! |                                                               |
//! +---------------------------------------------------------------+ 160
//! |                                                               |
//! |                                                               | 192
//! |                                                               |
//! |                                                               | 224
//! | Not Used                                                      |
//! +---------------------------------------------------------------+ 256
//! ```
//! # Pong
//!
//! ```text
//!  0                   1                   2                   3
//!  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! | Frame size                                                    |
//! +---------------+---------------+-------------------------------+ 32
//! | Version       | Flags         | Type                          |
//! +---------------+---------------+-------------------------------+ 64
//! | Server Id                                                     |
//! +---------------------------------------------------------------+ 96
//! | Max Committed Term Id                                         |
//! |                                                               | 128
//! |                                                               |
//! +---------------------------------------------------------------+ 160
//! |                                                               |
//! |                                                               | 192
//! |                                                               |
//! |                                                               | 224
//! | Not Used                                                      |
//! +---------------------------------------------------------------+ 256
//! ```
//! # Ping
//!
//! ```text
//!  0                   1                   2                   3
//!  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! | Frame size                                                    |
//! +---------------+---------------+-------------------------------+ 32
//! | Version       | Flags         | Type                          |
//! +---------------+---------------+-------------------------------+ 64
//! | Server Id                                                     |
//! +---------------------------------------------------------------+ 96
//! | Max Committed Term Id                                         |
//! |                                                               | 128
//! |                                                               |
//! +---------------------------------------------------------------+ 160
//! |                                                               |
//! |                                                               | 192
//! |                                                               |
//! |                                                               | 224
//! | Not Used                                                      |
//! +---------------------------------------------------------------+ 256
//! ```
//! # Request Term
//!
//! ```text
//!  0                   1                   2                   3
//!  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! | Frame size                                                    |
//! +---------------+---------------+-------------------------------+ 32
//! | Version       | Flags         | Type                          |
//! +---------------+---------------+-------------------------------+ 64
//! | Server Id                                                     |
//! +---------------------------------------------------------------+ 96
//! | Term to get start                                             |
//! |                                                               | 128
//! |                                                               |
//! +---------------------------------------------------------------+ 160
//! | Term to get stop                                              |
//! |                                                               | 192
//! |                                                               |
//! +---------------------------------------------------------------+ 224
//! | Not Used                                                      |
//! +---------------------------------------------------------------+ 256
//! ```
//! Request a missing term(s) from the server.
use crate::file::MessageFileStoreRead;
use crate::raft::state_machine::{RaftEvent, RaftStateMachineClient};
use a19_concurrent::queue::spsc_queue::SpscQueueReceiveWrap;
use byteorder::{BigEndian, ByteOrder};
use std::sync::Arc;
use zmq::Socket;

/// The size of the header.
#[allow(dead_code)]
const HEADER_SIZE: usize = 32;
/// 1 MB is the max message size minus the header size.
#[allow(dead_code)]
const MAX_MESSAGE_SIZE: usize = 0x100000 - HEADER_SIZE;
/// The maximum size of a data frame.
#[allow(dead_code)]
const MAX_DATA_FRAME_SIZE: usize = MAX_MESSAGE_SIZE + HEADER_SIZE;

// Types for the messages.
#[allow(dead_code)]
const CURRENT_VERSION: u8 = 1;
#[allow(dead_code)]
const COMMITED: i16 = 1;
#[allow(dead_code)]
const ELECTED_LEADER: i16 = 2;
#[allow(dead_code)]
const FOLLOWER_INDEX: i16 = 3;
#[allow(dead_code)]
const PING: i16 = 4;
#[allow(dead_code)]
const PONG: i16 = 5;
#[allow(dead_code)]
const VOTE_FOR_CANDIATE: i16 = 6;
#[allow(dead_code)]
const VOTE_FOR_ME: i16 = 7;
#[allow(dead_code)]
const SETUP_FRAME_FOR_SERVER: i16 = 8;
#[allow(dead_code)]
const SETUP_FRAME_RESPONSE: i16 = 9;
#[allow(dead_code)]
const DATA_FRAME: i16 = 20;

#[allow(dead_code)]
const POS_FRAME: usize = 0;
#[allow(dead_code)]
const STOP_FRAME: usize = 4;
#[allow(dead_code)]
const POS_VERSION: usize = 5;
#[allow(dead_code)]
const STOP_VERSION: usize = 6;
#[allow(dead_code)]
const POS_FLAGS: usize = 6;
#[allow(dead_code)]
const STOP_FLAGS: usize = 7;
#[allow(dead_code)]
const POS_TYPE: usize = 7;
#[allow(dead_code)]
const STOP_TYPE: usize = 9;
#[allow(dead_code)]
const POS_SERVER: usize = 9;
#[allow(dead_code)]
const STOP_SERVER: usize = 13;

#[derive(Debug)]
pub(crate) struct EventLogMsg {
    server_id: u32,
    term_id: u64,
    file_id: u32,
    file_offset: usize,
    length: usize,
}

/// The connection information for the different sockets.
#[derive(Debug)]
pub struct NetworkConnections {
    push_socket_uri: String,
    pull_socket_uri: String,
    pub_socket_uri: String,
    sub_socket_uri: String,
}

/// An event to send information over the connection.  These are messages sent on pub/sub.
#[derive(Debug)]
pub(crate) enum NetworkSend {
    RaftEvent(RaftEvent),
    EventLog(EventLogMsg),
}

pub(crate) enum NetworkSendType {
    Broadcast { msg: NetworkSend },
    Single { server_id: u32, msg: NetworkSend },
}

/// A mechanism for sending a single message to the server.
#[derive(Debug)]
pub(crate) enum NetworkSingleMessage {}

/// Represents a socket to send data to.
struct SendSocket {
    network_connections: NetworkConnections,
    server_id: u32,
    push_socket: Socket,
    pub_socket: Socket,
    send_message_queue: SpscQueueReceiveWrap<NetworkSendType>,
    state_machine_client: Arc<RaftStateMachineClient>,
    raft_event_encoder: RaftEventEncoder,
    file_store_location: String,
    file_prefix: String,
}

/// Used to manage the send sockets.
struct ReceiveSockets {
    network_connections: NetworkConnections,
    server_id: u32,
    pull_socket: Socket,
    sub_socket: Socket,
    state_machine_client: Arc<RaftStateMachineClient>,
}

/// Used to encode raft events and send them over the network.  Reuses the same piece of memory.
struct RaftEventEncoder {
    /// The buffer we write the messages to so they can be sent.
    msg_buffer: [u8; HEADER_SIZE],
    zero_buffer: [u8; 20],
    server_id: u32,
}

/// The raft message encoder.
impl RaftEventEncoder {
    fn new(server_id: u32) -> Self {
        let mut s = Self {
            msg_buffer: [0; 32],
            zero_buffer: [0; 20],
            server_id,
        };
        // Write the header size at the top since we don't need to keep it.
        BigEndian::write_u32(&mut s.msg_buffer[POS_FRAME..STOP_FRAME], HEADER_SIZE as u32);
        s.msg_buffer[POS_VERSION] = CURRENT_VERSION;
        BigEndian::write_u32(&mut s.msg_buffer[POS_SERVER..STOP_SERVER], s.server_id);
        s
    }

    /// Gets the buffer to write out.
    fn buffer<'a>(&'a self) -> &'a [u8] {
        &self.msg_buffer[..]
    }

    #[inline]
    fn zero_body(&mut self) {
        self.msg_buffer[STOP_SERVER..].clone_from_slice(&self.zero_buffer);
    }

    fn write_type(&mut self, type_id: i16) {
        BigEndian::write_i16(&mut self.msg_buffer[POS_TYPE..STOP_TYPE], type_id);
    }

    /// Writes the raft event to the buffer.  If the event isn't suppose to be sent over the wire false is returned.
    /// # Arguments
    /// `raft_event` - The raft event to serialize.
    /// # returns
    /// true if the raft event should be sent over the wire.
    fn write(&mut self, raft_event: RaftEvent) -> bool {
        match raft_event {
            RaftEvent::ClientMessageReceived => false,
            RaftEvent::Commited { term_id, server_id } => {
                self.zero_body();
                BigEndian::write_u64(
                    &mut self.msg_buffer[STOP_SERVER..(STOP_SERVER + 8)],
                    term_id,
                );
                self.write_type(COMMITED);
                true
            }
            RaftEvent::ElectedLeader { server_id: _ } => {
                self.zero_body();
                self.write_type(ELECTED_LEADER);
                true
            }
            RaftEvent::FollowerIndex {
                server_id: _,
                term_id,
            } => {
                self.zero_body();
                BigEndian::write_u64(
                    &mut self.msg_buffer[STOP_SERVER..(STOP_SERVER + 8)],
                    term_id,
                );
                self.write_type(FOLLOWER_INDEX);
                true
            }
            RaftEvent::LeaderTimeout => false,
            RaftEvent::NoMessagesTimeout => false,
            RaftEvent::Ping {
                max_commited_term,
                server_id: _,
            } => {
                self.zero_body();
                BigEndian::write_u64(
                    &mut self.msg_buffer[STOP_SERVER..(STOP_SERVER + 8)],
                    max_commited_term,
                );
                self.write_type(PING);
                true
            }
            RaftEvent::Pong {
                max_term_id,
                server_id: _,
            } => {
                self.zero_body();
                BigEndian::write_u64(
                    &mut self.msg_buffer[STOP_SERVER..(STOP_SERVER + 8)],
                    max_term_id,
                );
                self.write_type(PONG);
                true
            }
            RaftEvent::ProcessInternalMessage { msg: _ } => false,
            RaftEvent::Stop => false,
            RaftEvent::VoteForCandiate { server_id } => {
                self.zero_body();
                BigEndian::write_u32(
                    &mut self.msg_buffer[STOP_SERVER..(STOP_SERVER + 4)],
                    server_id,
                );
                self.write_type(VOTE_FOR_CANDIATE);
                true
            }
            RaftEvent::VoteForMe {
                max_term_id,
                server_id: _,
            } => {
                self.zero_body();
                BigEndian::write_u64(
                    &mut self.msg_buffer[STOP_SERVER..(STOP_SERVER + 8)],
                    max_term_id,
                );
                self.write_type(VOTE_FOR_ME);
                true
            }
            RaftEvent::VoteTimeout => false,
        }
    }
}

/// Used to encode the raft data frame to send it to the receivers.
struct RaftDataFrame {
    server_id: u32,
    msg_buffer: Vec<u8>,
    max_data_frame_size: usize,
    current_length: usize,
}

impl RaftDataFrame {
    fn new(server_id: u32, max_data_frame_size: usize) -> Self {
        let mut s = Self {
            server_id,
            msg_buffer: Vec::with_capacity(max_data_frame_size + HEADER_SIZE),
            max_data_frame_size,
            current_length: 0,
        };
        BigEndian::write_u32(&mut s.msg_buffer[POS_SERVER..STOP_SERVER], server_id);
        s.msg_buffer[4] = CURRENT_VERSION;
        BigEndian::write_i16(&mut s.msg_buffer[POS_TYPE..STOP_TYPE], DATA_FRAME);
        s
    }

    /// Gets the buffer with the data to write onto the socket.
    fn buffer<'a>(&'a self) -> &'a [u8] {
        // The buffer is already 32 bit aligned so we don't need to do anything here.
        &self.msg_buffer[..self.current_length]
    }

    /// Used to write an event log message to the internal buffer for writing on the wire.
    /// # Arguments
    /// `msg` - The message we are writing out to the buffer.
    /// `message_file_store` - The message file containing the term block.
    /// # returns
    /// true if we should write it out over the wire.
    fn write(&mut self, msg: EventLogMsg, message_file_store: MessageFileStoreRead) -> bool {
        // Assume the value is already aligned.  We are reading in the raw messages.
        let frame_size = (msg.length + HEADER_SIZE) as u32;
        BigEndian::write_u32(&mut self.msg_buffer[0..4], frame_size);
        BigEndian::write_u64(&mut self.msg_buffer[12..20], msg.term_id);
        BigEndian::write_u32(&mut self.msg_buffer[20..24], msg.file_id);
        BigEndian::write_u64(&mut self.msg_buffer[24..32], msg.file_offset as u64);
        let copy_from = message_file_store
            .read_section(&msg.file_offset, &msg.length)
            .unwrap(); // Potential data corruption bug crash!
        let copy_to = &mut self.msg_buffer[32..msg.length];
        copy_to.clone_from_slice(copy_from);
        true
    }
}
