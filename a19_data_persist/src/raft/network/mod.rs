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
//! | Not Used                                                      |
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
//! | MTU                                                           |
//! +---------------------------------------------------------------+ 224
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
//! +---------------------------------------------------------------+ 128
//! | Term offset                                                   |
//! +---------------------------------------------------------------+ 160
//! | Term Length Max                                               |
//! +---------------------------------------------------------------+ 192
//! |                                                               |
//! |                                                               | 224
//! | Not Used                                                      |
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
//! +---------------------------------------------------------------+ 128
//! |                                                               |
//! |                                                               | 160
//! |                                                               |
//! |                                                               | 192
//! |                                                               |
//! |                                                               | 224
//! | Not Used                                                      |
//! +---------------------------------------------------------------+ 256
//!
//! ```
use std::time::Duration;
use a19_concurrent::buffer::align;
use a19_concurrent::buffer::ring_buffer::{ManyToOneBufferReader, ManyToOneBufferWriter, create_many_to_one};
use crate::raft::state_machine::{ RaftEvent, RaftStateMachineClient };
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// An event to send information over the connection.  These are messages sent on pub/sub.
#[derive(Debug, Deserialize, Serialize)]
pub enum NetworkSendBroadcast {
    RaftEvent(RaftEvent),
}

#[derive(Debug, Deserialize, Serialize)]
pub enum NetworkRq {
    
}

#[derive(Debug, Deserialize, Serialize)]
pub enum NetworkRs {
    
}

/// Represents a socket to send data to.
struct SendSocket {
    state_machine_client: Arc<RaftStateMachineClient>,
}

struct Connection {
    /// The sockets we have for sending data too.
    send_sockets: Vec<SendSocket>,
}
