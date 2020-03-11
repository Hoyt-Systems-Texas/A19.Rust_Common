use std::collections::HashMap;

const ADD_SERVER_MSG_TYPE: i32 = -1;
const REMOVE_SERVER_MSG_TYPE: i32 = -2;

enum RaftMessage {
    AppendUserEntry,
    AddServer,
    RemoveServer,
}

enum RaftMessageResponse {
    Accepted,
}

enum RaftState {
    Candidate,
    Follower{leader: u32},
    Leader{next_index: HashMap<u32, u64>, match_index: HashMap<u32, u64>},
}

enum RaftEvent<'a> {
    LeaderTimeout,
    /// A vote to become the leader has been received.
    VoteRecieved{ server_id: u32 },
    /// The server has been elected the leader.
    ElectedLeader,
    /// A new message has been received.
    PersistedMessageReceived{body: Vec<u8>},
    /// A message from a client has been received.  There needs to be a future attached so we can signal the message has been commited.
    ClientMessageReceived{body: &'a [u8]},
    /// The id of the sterm that has been commited.
    Commited{term_id: u64},
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
}
