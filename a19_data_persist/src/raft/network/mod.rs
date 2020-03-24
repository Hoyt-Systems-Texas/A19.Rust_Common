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
use crate::raft::state_machine::{RaftEvent, RaftStateMachineClient};
use a19_concurrent::buffer::align;
use a19_concurrent::buffer::ring_buffer::{
    create_many_to_one, ManyToOneBufferReader, ManyToOneBufferWriter,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use zmq::Message;
use byteorder::{BigEndian, ByteOrder};

/// The size of the header.
const HEADER_SIZE: usize = 32;
/// 1 MB is the max message size minus the header size.
const MAX_MESSAGE_SIZE: usize = 0x100000 - HEADER_SIZE;
/// The maximum size of a data frame.
const MAX_DATA_FRAME_SIZE: usize = MAX_MESSAGE_SIZE + HEADER_SIZE;

// Types for the messages.
const CURRENT_VERSION: u8 = 1;
const COMMITED: i16 = 1;
const ELECTED_LEADER: i16 = 2;
const FOLLOWER_INDEX: i16 = 3;
const PING: i16 = 4;
const PONG: i16 = 5;
const VOTE_FOR_CANDIATE: i16 = 6;
const VOTE_FOR_ME: i16 = 7;
const SETUP_FRAME_FOR_SERVER: i16 = 8;
const SETUP_FRAME_RESPONSE: i16 = 9;
const DATA_FRAME: i16 = 20;

const POS_FRAME: usize = 0;
const STOP_FRAME: usize = 4;
const POS_VERSION: usize = 5;
const STOP_VERSION: usize = 6;
const POS_FLAGS: usize = 6;
const STOP_FLAGS: usize = 7;
const POS_TYPE: usize = 7;
const STOP_type: usize = 9;
const POS_SERVER: usize = 9;
const STOP_SERVER: usize = 13;

#[derive(Debug)]
pub(crate) struct EventLogMsg<'a> {
    server_id: u32,
    term_id: u64,
    file_id: u32,
    file_offset: u64,
    msg: &'a [u8],
}

/// An event to send information over the connection.  These are messages sent on pub/sub.
#[derive(Debug)]
pub(crate) enum NetworkSendBroadcast<'a> {
    RaftEvent(RaftEvent),
    EventLog(EventLogMsg<'a>),
}

/// A mechanism for sending a single message to the server.
#[derive(Debug)]
pub(crate) enum NetworkSingleMessage {}

/// Represents a socket to send data to.
struct SendSocket {
    state_machine_client: Arc<RaftStateMachineClient>,
}

struct Connection {
    /// The sockets we have for sending data too.
    send_sockets: Vec<SendSocket>,
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

    /// Writes the raft event to the buffer.  If the event isn't suppose to be sent over the wire false is returned.
    /// # Arguments
    /// `raft_event` - The raft event to serialize.
    /// # returns
    /// true if the raft event should be sent over the wire.
    fn write(&mut self, raft_event: RaftEvent) -> bool {
        match raft_event {
            RaftEvent::ClientMessageReceived => {
                false
            }
            RaftEvent::Commited{term_id} => {
                self.zero_body();
                BigEndian::write_u64(&mut self.msg_buffer[STOP_SERVER..(STOP_SERVER + 8)], term_id);
                true
            }
            RaftEvent::ElectedLeader{server_id} => {
                self.zero_body();
                true
            }
            RaftEvent::FollowerIndex{server_id, term_id} => {
                self.zero_body();
                BigEndian::write_u64(&mut self.msg_buffer[STOP_SERVER..(STOP_SERVER + 8)], term_id);
                true
            }
            RaftEvent::LeaderTimeout => {
                false
            }
            RaftEvent::NoMessagesTimeout => {
                false
            }
            RaftEvent::Ping{max_commited_term, server_id} => {
                self.zero_body();
                BigEndian::write_u64(&mut self.msg_buffer[STOP_SERVER..(STOP_SERVER + 8)], max_commited_term);
                true
            }
            RaftEvent::Pong{max_term_id, server_id} => {
                self.zero_body();
                BigEndian::write_u64(&mut self.msg_buffer[STOP_SERVER..(STOP_SERVER + 8)], max_term_id);
                true
            }
            RaftEvent::ProcessInternalMessage{msg} => {
                false
            }
            RaftEvent::Stop => {
                false
            }
            RaftEvent::VoteForCandiate{server_id} => {
                self.zero_body();
                BigEndian::write_u32(&mut self.msg_buffer[STOP_SERVER..(STOP_SERVER + 4)], server_id);
                true
            }
            RaftEvent::VoteForMe{ max_term_id, server_id} => {
                self.zero_body();
                BigEndian::write_u64(&mut self.msg_buffer[STOP_SERVER..(STOP_SERVER + 8)], max_term_id);
                true
            }
            RaftEvent::VoteTimeout => {
                false
            }
        }
    }
}

/// Used to encode the raft data frame to send it to the receivers.
struct RaftDataFrame {
    msg_buffer: [u8; MAX_DATA_FRAME_SIZE],
}
