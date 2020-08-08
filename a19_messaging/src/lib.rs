use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum SystemRequest {
    /// A ping request.
    Ping,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum SystemResponse {
    Pong,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum SystemPush {
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MessageEnvelope<USER, BODY> {
    user_id: USER,
    connection_id: u128,
    timestamp: u64,
    body: BODY,
}

/// The message envelope used to send messages to the client.
#[derive(Serialize, Deserialize, Debug)]
pub enum MessagePayload<REQ, REP, PUSH> {
    /// Define system requests to make it type safe.
    SystemRequest(SystemRequest),
    SystemResponse(SystemResponse),
    Request(REQ),
    Reply(REP),
    /// A push event to a client.
    Push(PUSH),
    SystemPush(SystemPush),
}
