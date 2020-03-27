//! File format for the file stored.  Need to have fast lookup for messages contained in the file.
//! I think the best thing to do would be to have a header on the file so there is less of a chance
//! from someone modifying the file accidently.  All you would need to do is then loop through the
//! files.
//!
//! # Events
//!
//! We need to have different files for streaming the data and have the data moved to a more
//! permanent buffer.  The files will just role over and then we offload the messages onto another
//! buffer when we get all of them.  This buffer can the rollover and is the one that we keep.  We
//! don't want to keep theses buffers.
//!
//! file_prefix.events.1
//! file_prefix.commit.1
//! file_prefix.events.2
//! file_prefix.commit.2
//! file_prefix.events.3
//! file_prefix.commit.3
//!
pub mod network;
pub mod state_machine;
pub mod write_message;

pub const EVENT_FILE_POSTFIX: &str = "events";
pub const COMMIT_FILE_POSTIX: &str = "commit";
pub const COMMIT_SIZE: u64 = 128;
pub const COMMIT_SIZE_BITS: usize = 128 * 8;
pub const EVENT_HEADER_SIZE: usize = 32;

use crate::file;
use crate::file::{MessageFileStore, MessageFileStoreRead, MessageFileStoreWrite, MessageRead};
use a19_concurrent::buffer::mmap_buffer::MemoryMappedInt;
use a19_concurrent::buffer::ring_buffer::{
    create_many_to_one, ManyToOneBufferReader, ManyToOneBufferWriter,
};
use a19_concurrent::buffer::{align, DirectByteBuffer};
use a19_concurrent::queue::mpsc_queue::{MpscQueueReceive, MpscQueueWrap};
use a19_concurrent::queue::spsc_queue::{SpscQueueReceiveWrap, SpscQueueSendWrap};
use futures::channel::oneshot;
use std::cell::Cell;
use std::cmp::Ordering;
use std::fs::*;
use std::io;
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf, MAIN_SEPARATOR};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, AtomicU8};
use std::sync::{atomic, Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};
use std::vec::Vec;

#[path = "../target/a19_data_persist/message/persisted_file_generated.rs"]

pub type QueueFuture<TOUT> = oneshot::Receiver<TOUT>;

/// Represents a message that was read in.
pub struct MessageInfo<'a> {
    message_id: u64,
    time_ms: i64,
    message_type: i32,
    message_body: &'a [u8],
}

struct FileWriteInfo {
    file_id: u32,
    position: usize,
    length: u32,
}

/// Represents a term commited.
/// Represents whats committed and we only flush after the raft protocol has been update.
/// ```text
///  0                   1                   2                   3
///  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// | Term Id                                                       |
/// |                                                               | 32 | 4
/// |                                                               |
/// +-------------------------------+-------------------------------+ 64 | 8
/// | Version                       | Type                          |        
/// +-------------------------------+-------------------------------+ 96 | 12
/// | Server Id                                                     |
/// +---------------------------------------------------------------+ 128 | 16
/// | Leader Id                                                     |
/// +-------------------------------+-------------------------------+ 160 | 20
/// | Committed                     | NOT USED FOR NOW              |
/// +-------------------------------+-------------------------------+ 192 | 24
/// |                                                               |
/// |               Start Timestamp                                 | 224 | 28
/// |                                                               |
/// +---------------------------------------------------------------+ 256 | 32
/// |                                                               |
/// |               Commited On Timestamp                           | 288 | 36
/// |                                                               |
/// +---------------------------------------------------------------+ 320 | 40
/// | File Id                                                       |
/// +---------------------------------------------------------------+ 352 | 44
/// | File Position Offset                                          |
/// |                                                               | 384 | 48
/// |                                                               |
/// +---------------------------------------------------------------+ 416 | 52
/// | Max Message Id                                                |
/// |                                                               | 448 | 56
/// |                                                               |
/// +---------------------------------------------------------------+ 480 | 60
/// | Length of Commit                                              |
/// +-------------------------------+-------------------------------+ 512 | 64
/// |                                                               |
/// +-------------------------------+-------------------------------+ 544 | 72
/// ..                                                              |
/// |                                                               ...
/// +---------------------------------------------------------------+ 1024 | 128
/// ```
const TERM_ID_OFFSET: usize = 0;
const VERSION_OFFSET: usize = 8;
const TYPE_OFFSET: usize = 10;
const SERVER_OFFSET: usize = 12;
const LEADER_OFFSET: usize = 16;
const COMMITTED: usize = 20;
const START_TIMESTAMP: usize = 24;
const COMMITTED_TIMESTAMP: usize = 32;
const FILE_ID: usize = 40;
const FILE_POSITION_OFFSET: usize = 44;
const MAX_MESSAGE_ID: usize = 52;
const LENGTH_OF_COMMIT: usize = 60;
const VOTES: usize = 64;

trait CommitFile {
    fn set_term(&mut self, pos: &usize, val: &u64) -> &mut Self;
    fn term(&self, pos: &usize) -> u64;
    fn set_version(&mut self, pos: &usize, val: &u16) -> &mut Self;
    fn version(&self, pos: &usize) -> u16;
    fn set_msg_type(&mut self, pos: &usize, val: &u16) -> &mut Self;
    fn msg_type(&self, pos: &usize) -> u16;
    fn set_server(&mut self, pos: &usize, server_id: &u32) -> &mut Self;
    fn server(&self, pos: &usize) -> u32;
    fn set_leader(&mut self, pos: &usize, val: &u32) -> &mut Self;
    fn leader(&self, pos: &usize) -> u32;
    fn set_committed(&mut self, pos: &usize) -> &mut Self;
    fn committed(&self, pos: &usize) -> u16;
    fn set_start_time(&mut self, pos: &usize, time: &u64) -> &mut Self;
    fn start_time(&self, pos: &usize) -> u64;
    fn set_committed_timestamp(&mut self, pos: &usize, val: &u64) -> &mut Self;
    fn committed_timestamp(&mut self, pos: &usize) -> u64;
    fn set_file_id(&mut self, pos: &usize, val: &u32) -> &mut Self;
    fn file_id(&self, pos: &usize) -> u32;
    fn set_file_position_offset(&mut self, pos: &usize, val: &u64) -> &mut Self;
    fn file_position_offset(&self, pos: &usize) -> u64;
    fn set_max_message_id(&mut self, pos: &usize, val: &u64) -> &mut Self;
    fn max_message_id(&self, pos: &usize) -> u64;
    fn set_length_of_commit(&mut self, pos: &usize, val: &u32) -> &mut Self;
    fn length_of_commit(&self, pos: &usize) -> u32;
    fn save_term(&mut self, pos: &usize, term: &TermCommit) -> &mut Self;
    fn set_votes(&mut self, pos: &usize, votes: u16) -> &mut Self;
    fn inc_votes(&mut self, pos: &usize) -> u16;
    fn get_votes(&mut self, pos: &usize) -> u16;
}

impl CommitFile for MemoryMappedInt {
    #[inline]
    fn set_term(&mut self, pos: &usize, val: &u64) -> &mut Self {
        let pos = TERM_ID_OFFSET + *pos;
        self.put_u64(&pos, *val);
        self
    }

    #[inline]
    fn term(&self, pos: &usize) -> u64 {
        let pos = TERM_ID_OFFSET + *pos;
        self.get_u64(&pos)
    }

    #[inline]
    fn set_version(&mut self, pos: &usize, val: &u16) -> &mut Self {
        let pos = VERSION_OFFSET + *pos;
        self.put_u16(&pos, *val);
        self
    }

    #[inline]
    fn version(&self, pos: &usize) -> u16 {
        let pos = VERSION_OFFSET + *pos;
        self.get_u16(&pos)
    }

    #[inline]
    fn set_msg_type(&mut self, pos: &usize, val: &u16) -> &mut Self {
        let pos = TYPE_OFFSET + *pos;
        self.put_u16(&pos, *val);
        self
    }

    #[inline]
    fn msg_type(&self, pos: &usize) -> u16 {
        let pos = TYPE_OFFSET + *pos;
        self.get_u16(&pos)
    }

    #[inline]
    fn set_server(&mut self, pos: &usize, server_id: &u32) -> &mut Self {
        let pos = SERVER_OFFSET + *pos;
        self.put_u32(&pos, *server_id);
        self
    }

    #[inline]
    fn server(&self, pos: &usize) -> u32 {
        let pos = SERVER_OFFSET + *pos;
        self.get_u32(&pos)
    }

    #[inline]
    fn set_leader(&mut self, pos: &usize, val: &u32) -> &mut Self {
        let pos = LEADER_OFFSET + *pos;
        self.put_u32(&pos, *val);
        self
    }

    #[inline]
    fn leader(&self, pos: &usize) -> u32 {
        let pos = LEADER_OFFSET + *pos;
        self.get_u32(&pos)
    }

    #[inline]
    fn set_committed(&mut self, pos: &usize) -> &mut Self {
        let pos = COMMITTED + *pos;
        self.put_u16(&pos, 1);
        self
    }

    #[inline]
    fn committed(&self, pos: &usize) -> u16 {
        let pos = COMMITTED + *pos;
        self.get_u16(&pos)
    }

    #[inline]
    fn set_start_time(&mut self, pos: &usize, time: &u64) -> &mut Self {
        let pos = START_TIMESTAMP + *pos;
        self.put_u64(&pos, *time);
        self
    }

    #[inline]
    fn start_time(&self, pos: &usize) -> u64 {
        let pos = START_TIMESTAMP + *pos;
        self.get_u64(&pos)
    }

    #[inline]
    fn set_committed_timestamp(&mut self, pos: &usize, val: &u64) -> &mut Self {
        let pos = COMMITTED_TIMESTAMP + *pos;
        self.put_u64(&pos, *val);
        self
    }

    #[inline]
    fn committed_timestamp(&mut self, pos: &usize) -> u64 {
        let pos = COMMITTED_TIMESTAMP + *pos;
        self.get_u64(&pos)
    }

    #[inline]
    fn set_file_id(&mut self, pos: &usize, val: &u32) -> &mut Self {
        let pos = FILE_ID + *pos;
        self.put_u32(&pos, *val);
        self
    }

    #[inline]
    fn file_id(&self, pos: &usize) -> u32 {
        let pos = FILE_ID + *pos;
        self.get_u32(&pos)
    }

    #[inline]
    fn set_file_position_offset(&mut self, pos: &usize, val: &u64) -> &mut Self {
        let pos = FILE_POSITION_OFFSET + *pos;
        self.put_u64(&pos, *val);
        self
    }

    #[inline]
    fn file_position_offset(&self, pos: &usize) -> u64 {
        let pos = FILE_POSITION_OFFSET + *pos;
        self.get_u64(&pos)
    }

    #[inline]
    fn set_max_message_id(&mut self, pos: &usize, val: &u64) -> &mut Self {
        let pos = MAX_MESSAGE_ID + *pos;
        self.put_u64(&pos, *val);
        self
    }

    #[inline]
    fn max_message_id(&self, pos: &usize) -> u64 {
        let pos = MAX_MESSAGE_ID + *pos;
        self.get_u64(&pos)
    }

    #[inline]
    fn set_length_of_commit(&mut self, pos: &usize, val: &u32) -> &mut Self {
        let pos = LENGTH_OF_COMMIT + *pos;
        self.put_u32(&pos, *val);
        self
    }

    #[inline]
    fn length_of_commit(&self, pos: &usize) -> u32 {
        let pos = LENGTH_OF_COMMIT + *pos;
        self.get_u32(&pos)
    }

    #[inline]
    fn set_votes(&mut self, pos: &usize, votes: u16) -> &mut Self {
        let pos = VOTES + *pos;
        self.put_u16(&pos, votes);
        self
    }

    #[inline]
    fn inc_votes(&mut self, pos: &usize) -> u16 {
        let pos = VOTES + *pos;
        let v = self.get_u16(&pos) + 1;
        self.put_u16(&pos, v);
        v
    }

    #[inline]
    fn get_votes(&mut self, pos: &usize) -> u16 {
        let pos = VOTES + *pos;
        self.get_u16(&pos)
    }

    #[inline]
    fn save_term(&mut self, pos: &usize, term: &TermCommit) -> &mut Self {
        self.set_term(pos, &term.term_id)
            .set_version(pos, &term.version)
            .set_msg_type(pos, &term.type_id)
            .set_server(pos, &term.server_id)
            .set_leader(pos, &term.leader_id)
            .set_start_time(pos, &term.timestamp)
            .set_committed_timestamp(pos, &term.committed_timestamp)
            .set_file_id(pos, &term.file_id)
            .set_file_position_offset(pos, &term.file_position_offset)
            .set_max_message_id(pos, &term.file_max_message_id)
            .set_length_of_commit(pos, &term.length);
        if term.committed > 0 {
            self.set_committed(pos);
        }
        self
    }
}

/// A term committed in the raft protocol.
pub struct TermCommit {
    /// The raft term id.
    term_id: u64,
    /// The current version of the message.
    version: u16,
    /// The type id.
    type_id: u16,
    /// The id of the server.
    server_id: u32,
    /// The id of the current leader.
    leader_id: u32,
    /// 1 if the message is currently commited.
    committed: u16,
    /// The time stamp when the message was created.
    timestamp: u64,
    /// The committed timestamp.
    committed_timestamp: u64,
    /// The id of the file we are committing to.
    file_id: u32,
    /// The offset of the possition of the term.
    file_position_offset: u64,
    /// The max messageid.
    file_max_message_id: u64,
    /// The length of the message commit.
    length: u32,
}

/// The state of the raft node.
enum RaftNodeState {
    /// The node is currently in a follower type and is copying messages to the buffer.
    Follower(PersistedCommitStreamFollower),
    /// The node is a candidate.
    Candidate,
    /// The node is the leader and is writing and sending messages.
    Leader(PersistedCommitStreamLeader),
    /// In single node mode.
    SingleNode(PersistedCommitSingleNode),
}

/// The events for the raft protocol.
enum RaftNodeEvent {
    ElectionTimeout = 0,
    NewTerm = 1,
    HeartbeatFailed = 2,
    HeartbeatTimeout = 3,
    HigherTerm = 4,
}

pub trait MessageProcessor: Send {
    /// Handles an incoming message.
    /// `read` - The message that has been read in.
    fn handle<'a>(&mut self, read: &MessageRead<'a>);
}

struct MessageWriter {
    buffer: MessageFileStoreWrite,
    file_id: u32,
}

struct MessageReader {
    buffer: MessageFileStoreRead,
    file_id: u32,
}

/// Represents the write stream.  Would only be used if we are the current leader.
pub struct PersistedMessageWriteStream {
    /// The buffer we are writing to.
    buffer: MessageFileStoreWrite,
    /// The current id of the file.
    file_id: u32,
    /// The current max message id.
    max_message_id: Arc<AtomicU64>,
    /// The storage directory.
    file_storage_directory: String,
    /// The file prefix.
    file_prefix: String,
    /// The size of the file to create.
    file_size: usize,
    /// The current position.
    current_pos: usize,
    /// The last message id in the file.
    loaded_message_id: u64,
}

impl PersistedMessageWriteStream {
    /// Creates a new message write stream.
    /// # Arguments
    /// `start_file_id` - The starting file id.  This is expected to be the last file.
    /// `file_storage_directory` - The file storage directory.
    /// `file_prefix` - The prefix for the file.
    /// `file_size` - The size of the file.
    /// `max_message_id` - The current maximum message id.
    fn new(
        start_file_id: u32,
        file_storage_directory: String,
        file_prefix: String,
        file_size: usize,
        max_message_id: Arc<AtomicU64>,
    ) -> file::Result<Self> {
        let mut event_name =
            create_event_name(&file_storage_directory, &file_prefix, &start_file_id);
        let mut buffer = unsafe { MessageFileStore::open_write(&event_name, file_size)? };
        let mut file_id = start_file_id;
        let reader = unsafe { MessageFileStore::open_readonly(&event_name)? };
        let (pos, last_msg_id) = match find_end_of_buffer(&reader)? {
            FindEmptySlotResult::Pos(p, last_msg_id) => (p, last_msg_id),
            FindEmptySlotResult::Full(last_msg_id) => {
                file_id += 1;
                event_name = create_event_name(&file_storage_directory, &file_prefix, &file_id);
                buffer = unsafe { MessageFileStore::open_write(&event_name, file_size)? };
                (0, last_msg_id)
            }
        };
        Ok(PersistedMessageWriteStream {
            buffer,
            file_id: start_file_id,
            max_message_id,
            file_storage_directory,
            file_prefix,
            file_size,
            current_pos: pos,
            loaded_message_id: last_msg_id,
        })
    }

    /// Writes to the file at a specified position.  Is done when copying the files.
    /// # Arguments
    fn write_pos(
        &mut self,
        file_id: &usize,
        pos: &usize,
        msg_type: &i32,
        msg_id: &u64,
        buffer: &[u8],
    ) -> crate::file::Result<usize> {
        self.buffer.write(pos, msg_type, msg_id, buffer)
    }

    /// Adds the message to the buffer.  Only to be used if this is the leader.
    /// # Arguments
    /// `pos` - The position
    /// `msg_type` - The type of the message.
    /// `msg_id` - The id of the message.
    /// `buffer` - The buffer to write to the message buffer.
    fn add_message(
        &mut self,
        msg_type: &i32,
        msg_id: &u64,
        buffer: &[u8],
    ) -> crate::file::Result<(usize, u32)> {
        match self
            .buffer
            .write(&self.current_pos, msg_type, msg_id, buffer)
        {
            Ok(s) => {
                self.current_pos = s;
                self.max_message_id
                    .store(*msg_id, atomic::Ordering::Release);
                Ok((s, self.file_id))
            }
            Err(e) => match e {
                file::Error::Full => {
                    self.buffer
                        .write(&self.current_pos, &-1, &std::u64::MAX, &[0, 0])?;
                    self.file_id += 1;
                    self.current_pos = 0;
                    let file = create_commit_name(
                        &self.file_storage_directory,
                        &self.file_prefix,
                        &self.file_id,
                    );
                    self.buffer = unsafe { MessageFileStore::open_write(&file, self.file_size)? };
                    match self
                        .buffer
                        .write(&self.current_pos, msg_type, msg_id, buffer)
                    {
                        Ok(s) => {
                            self.current_pos += s;
                            self.max_message_id
                                .store(*msg_id, atomic::Ordering::Release);
                            Ok((s, self.file_id))
                        }
                        Err(e) => Err(e),
                    }
                }
                e => Err(e),
            },
        }
    }

    pub fn flush(&self) -> crate::file::Result<()> {
        self.buffer.flush()
    }
}

/// Represents a stream we are currently reading in a processing messages.  Each message is
/// processed in a single thread.
pub struct PersistedMessageReadStream<FRead>
where
    FRead: MessageProcessor,
{
    /// The buffer we are currently reading.
    buffer: MessageFileStoreRead,
    /// The id of the file.
    file_id: u32,
    /// The maximum message id.
    max_message_id: Arc<AtomicU64>,
    /// What processes the incomming message as they are commited.
    message_processor: FRead,
    /// The storage directory.
    file_storage_directory: String,
    /// The file prefix.
    file_prefix: String,
    /// The current position in the files.
    current_pos: usize,
}

impl<FRead> PersistedMessageReadStream<FRead>
where
    FRead: MessageProcessor,
{
    /// Used to create a new reader.
    /// # Arguments
    /// `starting_file_id` - The file to start the read from.
    /// `from_message_id` - The id of the message to start reading from.
    /// `max_message_id` - The maximum message id that is safe to read.
    /// `message_processor` - What to call to handle processing the messages.
    /// `file_storage_directory` - The directory the files are stored in.
    /// `file_prefix` - The file prefix for the file storage.
    fn new(
        starting_file_id: u32,
        from_message_id: u64,
        max_message_id: Arc<AtomicU64>,
        message_processor: FRead,
        file_storage_directory: String,
        file_prefix: String,
    ) -> file::Result<Self> {
        let mut file_id = starting_file_id;
        let mut starting_pos = 0;
        let buffer = loop {
            let file_name = create_event_name(&file_storage_directory, &file_prefix, &file_id);
            let buffer = unsafe { MessageFileStore::open_readonly(&file_name)? };
            match find_message(&buffer, from_message_id)? {
                FindMessageResult::Found(pos) => {
                    starting_pos = pos;
                    break buffer;
                }
                FindMessageResult::End(pos) => {
                    starting_pos = pos;
                    break buffer;
                }
                FindMessageResult::EndOfFile => {
                    file_id += 1;
                }
            }
        };
        Ok(PersistedMessageReadStream {
            file_prefix,
            buffer,
            file_storage_directory,
            file_id: starting_file_id,
            max_message_id,
            current_pos: starting_pos,
            message_processor,
        })
    }

    /// called to process the next message in the buffer.
    fn process_next(&mut self) -> file::Result<bool> {
        match self.buffer.read_new(&self.current_pos) {
            Ok(msg) => {
                if msg.messageId() <= self.max_message_id.load(atomic::Ordering::Relaxed) {
                    self.current_pos = msg.next_pos();
                    self.message_processor.handle(&msg);
                    Ok(true)
                } else if msg.messageId() == std::u64::MAX {
                    self.switch_to_next_buffer()?;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Err(e) => match e {
                file::Error::NoMessage => Ok(false),
                file::Error::PositionOutOfRange(_) => {
                    self.switch_to_next_buffer()?;
                    Ok(true)
                }
                file::Error::Full => {
                    self.switch_to_next_buffer()?;
                    Ok(true)
                }
                _ => Err(e),
            },
        }
    }

    /// Switches to the next file buffer.
    fn switch_to_next_buffer(&mut self) -> file::Result<()> {
        let new_buffer_name = create_event_name(
            &self.file_storage_directory,
            &self.file_prefix,
            &(self.file_id + 1),
        );
        let new_buffer = unsafe { MessageFileStore::open_readonly(&new_buffer_name) }?;
        self.buffer = new_buffer;
        self.file_id += 1;
        Ok(())
    }
}

enum FindMessageResult {
    /// Found the end of the file.
    EndOfFile,
    /// The message was found and returns the position.
    Found(usize),
    /// The file ended and this is the position of the end.
    End(usize),
}

/// Used to find a message in a file.
/// # Arguments
/// `buffer` - The buffer to read in.
/// `msg_id` - The id of the message we are trying to find.
/// # Returns
/// Returns the position of the message in the file.
fn find_message(
    buffer: &MessageFileStoreRead,
    msg_id: u64,
) -> crate::file::Result<FindMessageResult> {
    let mut pos = 0;
    loop {
        match buffer.read_new(&pos) {
            Ok(msg) => {
                if msg.messageId() == 0 {
                    break Ok(FindMessageResult::End(pos));
                } else if msg.messageId() == std::u64::MAX {
                    break Ok(FindMessageResult::EndOfFile);
                } else if msg.messageId() >= msg_id {
                    break Ok(FindMessageResult::Found(pos));
                } else {
                    pos = msg.next_pos()
                }
            }
            Err(e) => match e {
                crate::file::Error::NoMessage => break Ok(FindMessageResult::End(pos)),
                crate::file::Error::PositionOutOfRange(_) => {
                    break Ok(FindMessageResult::EndOfFile)
                }
                _ => break Err(e),
            },
        }
    }
}

#[derive(Clone, Debug)]
enum FindEmptySlotResult {
    Full(u64),
    Pos(usize, u64),
}

/// Finds the end of the buffer.
/// # Arguments
/// `buffer` - The buffer we are reading in.
/// # Returns
/// The spot of the empty slot.
fn find_end_of_buffer(buffer: &MessageFileStoreRead) -> crate::file::Result<FindEmptySlotResult> {
    let mut pos = 0;
    let mut last_id = 0;
    loop {
        match buffer.read_new(&pos) {
            Ok(msg) => {
                if msg.messageId() == 0 {
                    break Ok(FindEmptySlotResult::Pos(pos, last_id));
                } else if msg.messageId() == std::u64::MAX {
                    break Ok(FindEmptySlotResult::Full(last_id));
                } else {
                    pos = msg.next_pos();
                    last_id = msg.messageId();
                }
            }
            Err(e) => match e {
                crate::file::Error::NoMessage => break Ok(FindEmptySlotResult::Pos(pos, last_id)),
                crate::file::Error::PositionOutOfRange(_) => {
                    break Ok(FindEmptySlotResult::Full(last_id))
                }
                _ => break Err(e),
            },
        }
    }
}

/// Represents a term file.
pub struct TermFile {
    /// The buffer we are writing to.
    pub buffer: MemoryMappedInt,
    /// The term start.
    pub term_start: u64,
    /// The term end.
    pub term_end: u64,
    /// The id of the fiel.
    pub file_id: u32,
}

enum TermPosResult {
    /// The position of the term.
    Pos(usize),
    /// The term is to big.
    Overflow,
    /// The position is in a previous file.
    Underflow,
}

impl TermFile {
    /// A new term file.
    /// `buffer` - The current buffer.
    /// `term_start` - The starting term buffer.
    /// `file_id` - The id of the file.
    fn new(buffer: MemoryMappedInt, term_start: u64, file_id: u32) -> Self {
        let c = buffer.capacity() as u64;
        TermFile {
            buffer,
            term_start,
            term_end: c / COMMIT_SIZE + term_start - 1,
            file_id,
        }
    }

    /// Calculates the position of the term in the file.
    /// # Arguments
    /// `term_id` - The term to calculate the position of.
    /// # Returns
    /// The position of the term so it can be read in.
    fn calculate_pos(&self, term_id: &u64) -> TermPosResult {
        if *term_id < self.term_start {
            TermPosResult::Underflow
        } else if *term_id > self.term_end {
            TermPosResult::Overflow
        } else {
            let offset = *term_id - self.term_start;
            TermPosResult::Pos((offset * COMMIT_SIZE) as usize)
        }
    }
}

pub struct PersistedCommitStreamLeader {
    /// The current commit file.
    commit_file: Rc<Cell<TermFile>>,
    /// The current file we are placing new terms to.
    new_term_file: Rc<Cell<TermFile>>,
    max_message_id: Arc<AtomicU64>,
    file_storage_directory: String,
    file_prefix: String,
    file_size: u64,
    buffer: MessageFileStoreRead,
}

pub struct PersistedCommitSingleNode {
    /// The current commit file.
    commit_file: Rc<TermFile>,
    /// The current file we are placing new terms to.
    new_term_file: Rc<TermFile>,
    /// The maximum message id.
    max_message_id: Arc<AtomicU64>,
    /// The file storage directory.
    file_storage_directory: String,
    /// The file prefix.
    file_prefix: String,
    /// The maximum size of the file.
    file_size: usize,
    /// The id of the current term.
    current_term_id: u64,
}

impl PersistedCommitSingleNode {
    fn new(
        commit_file: Rc<TermFile>,
        new_term_file: Rc<TermFile>,
        max_message_id: Arc<AtomicU64>,
        file_storage_directory: String,
        file_prefix: String,
        file_size: usize,
        current_term_id: u64,
    ) -> Self {
        PersistedCommitSingleNode {
            commit_file,
            new_term_file,
            max_message_id,
            file_storage_directory,
            file_prefix,
            file_size,
            current_term_id,
        }
    }
}

/// Represents the commit stream.
pub struct PersistedCommitStreamFollower {
    /// The file we are currently commit messages to.
    commit_file: Rc<Cell<TermFile>>,
    /// The new term file.
    new_term_file: Rc<Cell<TermFile>>,
    /// The current max committed file id.
    max_message_id: Arc<AtomicU64>,
    /// The storage directory.
    file_storage_directory: String,
    /// The file prefix.
    file_prefix: String,
    /// The size of the file to create.
    file_size: u64,
    /// The buffer we are writing.
    buffer: MessageFileStoreWrite,
}

#[derive(Debug, Clone)]
struct FileStorageInfo {
    file_storage_directory: String,
    file_prefix: String,
    max_file_size: u64,
}

struct AddMessageWriteRs {
    position_start: usize,
    complete: oneshot::Sender<file::Result<()>>,
}

struct AddMessageCommit {
    file_pos: usize,
    file_id: u32,
    complete: oneshot::Sender<file::Result<()>>,
}

impl AddMessageWriteRs {
    fn new(position_start: usize, complete: oneshot::Sender<file::Result<()>>) -> Self {
        AddMessageWriteRs {
            position_start,
            complete,
        }
    }
}

impl AddMessageCommit {
    #[inline]
    fn new(file_pos: usize, file_id: u32, complete: oneshot::Sender<file::Result<()>>) -> Self {
        AddMessageCommit {
            file_pos,
            file_id,
            complete,
        }
    }

    /// Checks to see if a messaged is processed based on the position.
    #[inline]
    fn is_processed(&self, position: &usize, file_id: &u32) -> bool {
        (self.file_id == *file_id && self.file_pos < *position) || self.file_id < *file_id
    }
}

/// The persisted file.
pub struct PersistedMessageFile {
    /// The maximum file size before it roles overs.
    max_file_size: usize,
    /// The thread that processes the commit.
    commit_join: Option<JoinHandle<u32>>,
    /// The writer joiner.
    writer_join: Option<JoinHandle<u32>>,
    /// The reader joiner.
    reader_join: Option<JoinHandle<u32>>,
    /// The atomic u8.
    stop: Arc<AtomicU8>,
    /// The incoming byte buffer to write the messages to.
    incoming_writer: ManyToOneBufferWriter,
    /// The incoming reader queue that contains the messages to complete.
    incoming_queue_writer: MpscQueueWrap<AddMessageWriteRs>,
    /// The current maximum message id that has been processed.
    max_message_id: Arc<AtomicU64>,
    /// The directory to store the files.
    file_storage_directory: String,
    /// The prefix for the fille storage.
    file_prefix: String,
}

///  The file format for the messages.  The goal is to have raft replicate the messages onto the
///  server.  Would require 2 files, one with the messages and one with raft indicating which
///  messages have been submitted.  The messages are aligned at 32 bytes for faster lookup.
///
///  Raw message log.
///  0                   1                   2                   3
///  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// | Message Size                                                  |
/// +-------------------------------+-------------------------------+ 32
/// | Version                       |  Message Type                 ...
/// +-------------------------------+-------------------------------+ 64
/// ... Message Type cont.          | Global Message Id             |
/// +-------------------------------+                               | 96
/// |   Global message Id cont.                                     |
/// |                               +-------------------------------+ 128
/// |                               | Client Message Id             |
/// +-------------------------------+                               | 128
/// |   Client Message Id cont.                                     |
/// |                               +-------------------------------+ 160
/// |                               |  Not used                     |
/// +-------------------------------+                               | 192
/// |                                                               |
/// |                                                               | 224
/// |                                                               |
/// +-------------------------------+-------------------------------+ 256
/// |                     Message Body 32 byte align                ...
/// ...                                                             |
/// +---------------------------------------------------------------+
/// | Message Size u32 MAX Indicates do not process                 |
/// +---------------------------------------------------------------+
pub trait PersistedMessageFileReadOnly {
    /// Reads a message a speific location.
    /// # Arguments
    /// `start` - The starting index to read in.
    fn read<'a>(&'a self, start: usize) -> MessageInfo;

    /// Reads messages until the func returns false.
    /// # Arguments
    /// `start` - The starting point to start reading the messages.
    /// `func` - The function to call when a message is read in.
    fn read_msg_til<'a>(&'a self, start: usize, func: fn(message: MessageInfo) -> bool) -> u32;
}

/// Represents a mutable message file.  Need to be able to write the messages in a way where raft
/// can be used to distribute the file.  The problem is we need to know up to which memory location
/// is valid.
///
/// With raft the leader indicates a change it would like to make to the rest of the cluster.
/// We need to align on the messages but raft doesn't need to know those are the message
/// alignments.
pub trait PersistedMessageFileMut {
    /// Writes a message at a specific location.
    /// # Arguments
    /// `start` - The starting location of the message to write.  Need for the raft protocol.
    /// `term_id` - The id of the message to write.
    /// `time` - The time in unix.
    /// `body` - The body of the file.
    fn write(&mut self, start: &usize, term_id: &u32, time: &u64, body: &[u8]);

    /// Commits a term after the raft sync.  Flushes the data to disks.
    /// `term_id` - The id of the term we are committing.
    fn commit_term(&mut self, term_id: &u32);

    /// Reads a message a speific location.
    /// # Arguments
    /// `start` - The starting index to read in.
    fn read<'a>(&'a mut self, start: &usize) -> MessageInfo;

    /// Flushes the written data to the disk.
    fn flush(&mut self);

    /// The capacity of the file.
    fn capacity(&self) -> usize;
}

#[derive(Debug, Clone, Eq)]
pub(crate) struct MessageFileInfo {
    path: String,
    file_id: u32,
    message_id_start: u64,
}

impl PartialOrd for MessageFileInfo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MessageFileInfo {
    fn cmp(&self, other: &Self) -> Ordering {
        self.file_id.cmp(&other.file_id)
    }
}

impl PartialEq for MessageFileInfo {
    fn eq(&self, other: &Self) -> bool {
        self.file_id == other.file_id
    }
}

#[derive(Eq, Debug, Clone)]
pub(crate) struct CommitFileInfo {
    path: String,
    file_id: u32,
    term_start: u64,
    message_id: u64,
}

impl MessageFileInfo {
    /// Used to create a new file info.
    /// # Arguments
    /// `path` - The path to the file.
    /// `file_id` - The id of the file.
    fn new(path: String, file_id: u32, message_id_start: u64) -> Self {
        MessageFileInfo {
            path,
            file_id,
            message_id_start,
        }
    }
}

impl CommitFileInfo {
    fn new(path: String, file_id: u32, term_start: u64, message_id: u64) -> Self {
        CommitFileInfo {
            path,
            file_id,
            term_start,
            message_id,
        }
    }
}

impl PartialOrd for CommitFileInfo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CommitFileInfo {
    fn cmp(&self, other: &Self) -> Ordering {
        self.file_id.cmp(&other.file_id)
    }
}

impl PartialEq for CommitFileInfo {
    fn eq(&self, other: &Self) -> bool {
        self.file_id == other.file_id
    }
}

/// The commit file collection.
pub struct FileCollection {
    /// The list of files containing the commit information.
    commit_files: Arc<Mutex<Vec<CommitFileInfo>>>,
    /// A map of the message files.
    message_files: Arc<Mutex<Vec<MessageFileInfo>>>,
    /// The storage directory for the files.
    file_storage_directory: String,
    /// The prefix for the files
    file_prefix: String,
}

unsafe impl Sync for FileCollection {}
unsafe impl Send for FileCollection {}

/// The message iterator.
pub struct MessageIterator {
    current_reader: MessageFileStoreRead,
    current_file_id: u32,
    max_commit_id: u64,
    message_files: Arc<Mutex<Vec<MessageFileInfo>>>,
    number: u32,
    start_message_id: u64,
    pos: usize,
}

pub enum NextResult<'a> {
    /// No more data to read in.
    End(u32),
    /// If there is more after processing the specified number.
    More,
    /// The next file id.
    NextFile { file_id: u32, readed: u32 },
    /// The message that was read in.
    Some(MessageRead<'a>),
}

impl MessageIterator {
    /// Creates a new iterator.
    /// # Arguments
    /// `number` - The maximum number of messages to retreive.
    /// `start_message_id` - The starting message id.
    /// `max_commit_id` - The maximum id that has been commited.
    /// `message_files` - The files containing the messages.
    fn new(
        number: u32,
        start_message_id: u64,
        max_commit_id: u64,
        message_files: Arc<Mutex<Vec<MessageFileInfo>>>,
    ) -> crate::file::Result<Self> {
        let file = MessageIterator::find_starting_file(message_files.clone(), start_message_id);
        let reader = unsafe { MessageFileStore::open_readonly(&file.path) }?;
        let mut pos = 0;
        loop {
            let msg = reader.read_new(&pos)?;
            if msg.messageId() >= start_message_id || msg.messageId() == 0 {
                break;
            } else {
                pos = msg.next_pos();
            }
        }
        Ok(MessageIterator {
            current_reader: reader,
            current_file_id: file.file_id,
            message_files,
            start_message_id,
            number,
            max_commit_id,
            pos,
        })
    }

    /// Finds the starting message file.
    /// # Arguments
    /// `message_files` - The message file to search.
    /// `start_message_id` - The starting message id.
    fn find_starting_file(
        message_files: Arc<Mutex<Vec<MessageFileInfo>>>,
        start_message_id: u64,
    ) -> MessageFileInfo {
        let messages = message_files.lock().unwrap();
        let mut last_file = 0;
        for x in 0..messages.len() {
            let file: &MessageFileInfo = messages.get(x).unwrap();
            if file.message_id_start > start_message_id {
                break;
            }
            last_file = x;
        }
        messages.get(last_file).unwrap().clone()
    }

    /// Used to get the next element.  Since this needs a lifetime because we are just exporting
    /// the memory directly.
    pub fn next<'a>(&'a mut self) -> crate::file::Result<NextResult<'a>> {
        if self.number == 0 {
            Ok(NextResult::More)
        } else {
            match self.current_reader.read_new(&self.pos) {
                Ok(reader) => {
                    if reader.messageId() <= self.max_commit_id {
                        self.pos = reader.next_pos();
                        self.number = self.number - 1;
                        Ok(NextResult::Some(reader))
                    } else {
                        Ok(NextResult::End(self.number))
                    }
                }
                Err(e) => {
                    match e {
                        crate::file::Error::NoMessage => {
                            // Find the next file.  Lets just go backwards :)
                            let message_files = self.message_files.lock().unwrap();
                            let next_file_id = self.current_file_id + 1;
                            let length = message_files.len() - 1;
                            let mut found = false;
                            for m in 0..message_files.len() {
                                let i = m - length;
                                let file: &MessageFileInfo = message_files.get(i).unwrap();
                                if file.file_id == next_file_id {
                                    found = true;
                                    break;
                                } else if file.file_id == self.current_file_id {
                                    found = false;
                                    break;
                                }
                            }
                            if found {
                                Ok(NextResult::NextFile {
                                    file_id: next_file_id,
                                    readed: self.number,
                                })
                            } else {
                                Ok(NextResult::End(self.number))
                            }
                        }
                        _ => Err(e),
                    }
                }
            }
        }
    }
}

pub trait MessageStore {
    /// Used to add a message to a buffer.
    /// # Arguments
    /// `msg_type_id` - The type of the message.
    /// `message_id` - The id of the message.
    /// `buffer` - The buffer to add to the messate queue.
    /// # Returns
    /// TODO a future that gets called when it gets committed.
    fn add_message(&self, msg_type_id: &i32, message_id: &u64, buffer: &[u8]);
}

/// Gets the file id from the extension.
/// # Arguments
/// path_str - The path string to parse in.
/// # Returns
/// The id of the file if it can be parsed in.
pub(crate) fn read_file_id(path_str: &str) -> Option<u32> {
    let file_id_opt: Option<&str> = path_str.split(".").last();
    match file_id_opt {
        Some(file_id) => match file_id.parse::<u32>() {
            Ok(id) => Some(id),
            _ => None,
        },
        None => None,
    }
}

impl FileCollection {
    /// Gets a new file collection.
    /// # Arguments
    /// `file_storage_directory` - The file storage directory.
    /// `file_prefix` - The file name prefix.
    fn new(file_storage_directory: String, file_prefix: String) -> Self {
        FileCollection {
            commit_files: Arc::new(Mutex::new(Vec::with_capacity(10))),
            message_files: Arc::new(Mutex::new(Vec::with_capacity(10))),
            file_storage_directory,
            file_prefix,
        }
    }

    /// Adds a message file if it has a message id.
    /// # Arguments
    /// `path` - The path buf to the file.
    /// `path_str` - The path of the file as a string.
    fn add_message_file(&mut self, path: PathBuf, path_str: &str) -> std::io::Result<()> {
        let mut message_files = self.message_files.lock().unwrap();
        match read_file_id(path_str) {
            Some(id) => {
                let (read, _) = unsafe { MessageFileStore::open(&path)? };
                let mut msg_id: u64 = 0;
                {
                    let result = read.read(0, move |_, id, _| {
                        if id > 0 {
                            msg_id = id;
                        }
                    });
                    match result {
                        Ok(_) => {
                            message_files.push(MessageFileInfo {
                                path: path_str.to_owned(),
                                file_id: id,
                                message_id_start: msg_id,
                            });
                            Ok(())
                        }
                        Err(e) => match e {
                            file::Error::FileError(e) => Err(e),
                            _ => Ok(()),
                        },
                    }?
                }
                message_files.sort();
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Used to add a commit file.
    /// # Arguments
    /// `path` - The path buffer for the file.
    /// `path_str` - The path string.
    fn add_commit_file(&mut self, path: PathBuf, path_str: &str) -> std::io::Result<()> {
        match read_file_id(path_str) {
            Some(id) => {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(false)
                    .open(path)?;
                let buffer = unsafe { MemoryMappedInt::open(file) }?;
                let term_id = buffer.term(&0); // Get the starting message.
                let message_id = buffer.max_message_id(&0);
                let time = buffer.start_time(&0);
                if time > 0 {
                    self.commit_files.lock().unwrap().push(CommitFileInfo::new(
                        path_str.to_owned(),
                        id,
                        term_id,
                        message_id,
                    ));
                    Ok(())
                } else {
                    // Not sure what we should do with the file since it's not valid.
                    Ok(())
                }
            }
            _ => Ok(()),
        }
    }
}

pub fn create_event_name(file_storage_directory: &str, file_prefix: &str, file_id: &u32) -> String {
    let path = Path::new(file_storage_directory);
    if !path.exists() {
        create_dir_all(path).unwrap();
    }
    format!(
        "{}{}{}.{}.{}",
        file_storage_directory, MAIN_SEPARATOR, file_prefix, EVENT_FILE_POSTFIX, file_id
    )
}

/// Used to create the commit file name.
pub fn create_commit_name(
    file_storage_directory: &str,
    file_prefix: &str,
    file_id: &u32,
) -> String {
    format!(
        "{}{}{}.{}.{}",
        file_storage_directory, MAIN_SEPARATOR, file_prefix, COMMIT_FILE_POSTIX, file_id
    )
}

/// Used to process the file collection and get the current term file and message file.
/// # Arguments
/// `file_collection` - The current collection of files to process.
/// `file_prefix` - The prefix name for the file.
/// `file_storage_directory` - The location to store the file.
/// `max_file_size` - The maximum file size.  The value is assumed to be already aligned.
/// `commit_file_size` - The commit file size.
fn process_files(
    file_collection: &mut FileCollection,
    file_prefix: &str,
    file_storage_directory: &str,
    max_file_size: &usize,
    commit_file_size: &usize,
) -> std::io::Result<()> {
    let mut message_files = file_collection.message_files.lock().unwrap();
    let mut commit_files = file_collection.commit_files.lock().unwrap();
    let path = Path::new(file_storage_directory);
    if !path.exists() {
        create_dir_all(file_storage_directory)?;
    }
    if commit_files.len() == 0 && message_files.len() == 0 {
        let file_id: u32 = 1;
        let path_commit = create_event_name(file_storage_directory, file_prefix, &file_id);
        let (read, write) = unsafe { MessageFileStore::new(&path_commit, *max_file_size)? };
        let path_event = create_commit_name(file_storage_directory, file_prefix, &file_id);
        let commit_file = unsafe { MemoryMappedInt::new(&path_event, *commit_file_size)? };
        message_files.push(MessageFileInfo::new(path_commit.clone(), 1, 0));
        commit_files.push(CommitFileInfo::new(path_event.clone(), 1, 0, 0));
        Ok(())
    } else {
        Ok(())
    }
}

/// Loads all of the current files.
/// # Arguments
/// `file_prefix` - The file prefix to load.
/// `file_storage_directory` - The file storage directory.
fn load_current_files(
    file_prefix: &str,
    file_storage_directory: &str,
) -> io::Result<FileCollection> {
    let dir_path = Path::new(&file_storage_directory);
    if dir_path.is_dir() || !dir_path.exists() {
        if !dir_path.exists() {
            create_dir_all(dir_path)?;
        }
        let mut file_collection =
            FileCollection::new(file_storage_directory.to_owned(), file_prefix.to_owned());
        let starts_with_events = format!("{}.{}", &file_prefix, &EVENT_FILE_POSTFIX);
        let starts_with_commits = format!("{}.{}", &file_prefix, &COMMIT_FILE_POSTIX);
        for entry in read_dir(file_storage_directory)? {
            let file = entry?;
            let path: PathBuf = file.path();
            if path.is_file() {
                match path.file_name() {
                    Some(p) => {
                        match p.to_str() {
                            Some(p) => {
                                if p.starts_with(&starts_with_commits) {
                                    file_collection.add_message_file(path.clone(), p)?;
                                } else if p.starts_with(&starts_with_events) {
                                    file_collection.add_commit_file(path.clone(), p)?;
                                }
                            }
                            _ => {
                                // Skip for now
                            }
                        }
                    }
                    None => {
                        // Skip for now.
                    }
                }
            }
        }
        Ok(file_collection)
    } else {
        Err(Error::new(
            ErrorKind::InvalidInput,
            "Path isn't a directory!",
        ))
    }
}

pub(crate) enum LastCommitPos {
    NoCommits,
    LastCommit {
        start_term_id: u64,
        term_id: u64,
        file_id: u32,
        max_message_id: u64,
        path: String,
    },
}

/// Used to find the position and the file of the last commit.
/// # Arguments
/// `commit_files` - The commit files.
pub(crate) fn find_last_commit_pos(
    commit_files: &Arc<Mutex<Vec<CommitFileInfo>>>,
) -> LastCommitPos {
    let commit_files = commit_files.lock().unwrap();
    let length = commit_files.len();
    if length == 0 {
        LastCommitPos::NoCommits
    } else {
        let mut file_pos = length - 1;
        loop {
            let file_commit: &CommitFileInfo = commit_files.get(file_pos).unwrap();
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(false)
                .open(&file_commit.path)
                .unwrap();
            let buffer = unsafe { MemoryMappedInt::open(file).unwrap() };
            let mut pos = 0;
            let mut last_term = 0;
            let mut found_commit = false;
            let mut last_max_message = 0;
            let start_term_id = file_commit.term_start;
            loop {
                let term = buffer.term(&pos);
                if term == 0 {
                    break;
                } else {
                    if buffer.committed(&pos) > 0 {
                        last_term = term;
                        found_commit = true;
                        last_max_message = buffer.max_message_id(&pos);
                        pos += COMMIT_SIZE as usize;
                    } else {
                        break;
                    }
                }
            }
            if found_commit {
                break LastCommitPos::LastCommit {
                    start_term_id,
                    term_id: last_term,
                    file_id: file_commit.file_id,
                    max_message_id: last_max_message,
                    path: file_commit.path.clone(),
                };
            } else {
                if file_pos == 0 {
                    break LastCommitPos::NoCommits;
                } else {
                    file_pos -= 1;
                }
            }
        }
    }
}

enum LastTermPos {
    NoTerms,
    Pos {
        term_start: u64,
        last_term: u64,
        file_id: u32,
    },
}

/// Used to find the last term in the file.
/// `commit_files` - The commit files to scan.
fn find_last_term(commit_files: &Arc<Mutex<Vec<CommitFileInfo>>>) -> LastTermPos {
    let commit_files = commit_files.lock().unwrap();
    let length = commit_files.len();
    if length == 0 {
        LastTermPos::NoTerms
    } else {
        let mut file_pos = length - 1;
        loop {
            let file_commit: &CommitFileInfo = commit_files.get(file_pos).unwrap();
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(false)
                .open(&file_commit.path)
                .unwrap();
            let buffer = unsafe { MemoryMappedInt::open(file).unwrap() };
            let mut pos = 0;
            let mut last_term = 0;
            let mut found_commit = false;
            let start_term_id = file_commit.term_start;
            loop {
                let term = buffer.term(&pos);
                if term == 0 {
                    break;
                } else {
                    if buffer.term(&pos) > 0 {
                        last_term = term;
                        found_commit = true;
                        pos += COMMIT_SIZE as usize;
                    } else {
                        break;
                    }
                }
            }
            if found_commit {
                break LastTermPos::Pos {
                    term_start: start_term_id,
                    last_term,
                    file_id: file_commit.file_id,
                };
            } else {
                if file_pos == 0 {
                    break LastTermPos::NoTerms;
                } else {
                    file_pos -= 1;
                }
            }
        }
    }
}

/// Starts the write thread.
/// # Arguments
/// `stop` - We should stop processing.
/// `receiver` - The queue we are receiving from.
/// `peding_queue` - The pending queue.
/// `max_message_id` - The maximum message id.
/// `event_file_size` - The file size for the event.
/// `file_storage_directory` - The storage directory for the files.
/// `file_prefix` - The file prefix.
/// `file_id_start` - The starting file id.
fn write_thread_single(
    stop: Arc<AtomicU8>,
    receiver: MpscQueueReceive<AddMessageWriteRs>,
    pending_write_queue: ManyToOneBufferReader,
    max_message_id: Arc<AtomicU64>,
    event_file_size: usize,
    file_storage_directory: String,
    file_prefix: String,
    file_id_start: u32,
    commit_writer: SpscQueueSendWrap<AddMessageCommit>,
) -> JoinHandle<u32> {
    thread::spawn(move || {
        let mut file_buffer = PersistedMessageWriteStream::new(
            file_id_start,
            file_storage_directory,
            file_prefix,
            event_file_size,
            max_message_id.clone(),
        )
        .unwrap();
        let mut last_msg_id = file_buffer.loaded_message_id;
        loop {
            if stop.load(atomic::Ordering::Relaxed) > 0 {
                break 0;
            } else {
                let mut write_out_pos = 0;
                let mut write_out_file = 0;
                let r = pending_write_queue.read(
                    |msg_type, bytes| {
                        last_msg_id += 1;
                        let (pos, file) = file_buffer
                            .add_message(&msg_type, &last_msg_id, bytes)
                            .unwrap();
                        let write_out_pos = pos;
                        let write_out_file = file;
                    },
                    100,
                );
                pending_write_queue.read_completed(&r);
                if r.messages_read > 0 {
                    loop {
                        match receiver.peek() {
                            Some(value) => {
                                if value.position_start <= r.start {
                                    match receiver.poll() {
                                        Some(value) => {
                                            let message = AddMessageCommit::new(
                                                write_out_pos,
                                                write_out_file,
                                                value.complete,
                                            );
                                            if !commit_writer.offer(message) {
                                                thread::sleep_ms(2);
                                            }
                                        }
                                        None => {}
                                    }
                                }
                            }
                            None => break,
                        }
                    }
                } else {
                    thread::sleep_ms(1);
                }
            }
        }
    })
}

/// Starts the commit thread.
/// # Arguments
/// `stop` - Indicates the stop the thread.
/// `file_storage_directory` - The file storage location.
/// `file_prefix` - The file prefix.
/// `commit_file_size` - The size of the commit file size.
/// `max_message` - The current maximum message that has been committed.
/// `collection` - The collection of the files.
/// `pending_commit_queue` - The current pending commit queue.
/// # Returns
/// The join handler to indicate when the thread has stopped.
fn commit_thread_single(
    stop: Arc<AtomicU8>,
    file_storage_directory: String,
    file_prefix: String,
    commit_file_size: usize,
    max_message: Arc<AtomicU64>,
    collection: Arc<FileCollection>,
) -> JoinHandle<u32> {
    thread::spawn(move || {
        let (commit_term, max_commit_term) = match find_last_commit_pos(&collection.commit_files) {
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
        let (new_term, last_term) = match find_last_term(&collection.commit_files) {
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
            let mut message_file = message_file;
            let mut read_pos = read_pos;
            let mut read_file_id = read_file_id;
            let mut term_file = commit_term;
            let mut current_term = max_commit_term;
            loop {
                if stop.load(atomic::Ordering::Acquire) > 0 {
                    break 0;
                } else {
                    match message_file.read_block(&read_pos, &std::u64::MAX, &0x10000) {
                        Ok(result) => {
                            let new_term = current_term + 1;
                            let current_time = SystemTime::now();
                            let since_epoch =
                                current_time.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
                            let term = TermCommit {
                                file_position_offset: read_pos as u64,
                                file_id: read_file_id,
                                term_id: new_term,
                                length: result.bytes.len() as u32,
                                version: 1,
                                type_id: 1,
                                leader_id: 1,
                                server_id: 1,
                                committed: 1,
                                file_max_message_id: result.message_id_end,
                                timestamp: since_epoch,
                                committed_timestamp: since_epoch,
                            };
                            match term_file.calculate_pos(&new_term) {
                                TermPosResult::Pos(p) => {
                                    term_file.buffer.save_term(&p, &term);
                                    max_message
                                        .store(result.message_id_end, atomic::Ordering::Relaxed);
                                    read_pos = result.next_pos;
                                    current_term = new_term;
                                }
                                TermPosResult::Overflow => {
                                    let next_file_id = term_file.file_id + 1;
                                    let path = create_commit_name(
                                        &file_storage_directory,
                                        &file_prefix,
                                        &next_file_id,
                                    );
                                    let buffer = unsafe {
                                        MemoryMappedInt::new(&path, commit_file_size).unwrap()
                                    };
                                    term_file = TermFile::new(buffer, new_term, next_file_id);
                                    read_file_id = next_file_id;
                                }
                                TermPosResult::Underflow => {
                                    panic!("We should never underflow when writing new terms!");
                                }
                            }
                        }
                        Err(e) => {
                            match e {
                                file::Error::Full => {
                                    // Next file
                                    let next_file = read_file_id + 1;
                                    let file_path = create_event_name(
                                        &file_storage_directory,
                                        &file_prefix,
                                        &next_file,
                                    );
                                    match unsafe { MessageFileStore::open_readonly(&file_path) } {
                                        Ok(buffer) => {
                                            message_file = buffer;
                                        }
                                        Err(e) => {
                                            // Spin
                                            thread::sleep_ms(10);
                                        }
                                    }
                                }
                                file::Error::FileError(e) => {
                                    // Spin
                                    thread::sleep_ms(100);
                                }
                                file::Error::NoMessage => {
                                    thread::sleep_ms(2);
                                }
                                file::Error::PositionOutOfRange(pos) => {
                                    // Next file
                                    let next_file = read_file_id + 1;
                                    let file_path = create_event_name(
                                        &file_storage_directory,
                                        &file_prefix,
                                        &next_file,
                                    );
                                    match unsafe { MessageFileStore::open_readonly(&file_path) } {
                                        Ok(buffer) => {
                                            message_file = buffer;
                                        }
                                        Err(e) => {
                                            // Spin
                                            thread::sleep_ms(10);
                                        }
                                    }
                                }
                                file::Error::NotEnoughSpace {
                                    position,
                                    capacity,
                                    remaining,
                                    message_size,
                                } => {
                                    panic!("We are reading in a message this should never happen!");
                                }
                                file::Error::AlreadyExists | file::Error::InvalidFile => {
                                    panic!("Unable to get the file!");
                                }
                            }
                        }
                    }
                }
            }
        }
    })
}

/// Starts the process thread.  Currently doesn't support a snapshot.
/// # Arguments
/// `file_storage_directory` - The file storage directory.
/// `file_prefix` - The file prefix to store.
/// `file_collection` - The file collection.
/// `message_processor` - The message processor to call.
/// `max_message_id` - The maximum message we should process.
/// # Returns
/// The join handler for when the thread quits.
fn read_thread<FRead>(
    stop: Arc<AtomicU8>,
    file_storage_directory: String,
    file_prefix: String,
    file_collection: Arc<FileCollection>,
    message_processor: FRead,
    max_message_id: Arc<AtomicU64>,
    pending_commit_queue: SpscQueueReceiveWrap<AddMessageCommit>,
) -> JoinHandle<u32>
where
    FRead: MessageProcessor + 'static,
{
    thread::spawn(move || {
        println!("Starting up reading thread!");
        let mut message_processor = message_processor;
        let mut read_file_id = 1;
        let read_file_path =
            create_event_name(&file_storage_directory, &file_prefix, &read_file_id);
        let mut read = unsafe { MessageFileStore::open_readonly(&read_file_path).unwrap() };
        let mut read_pos = 0;
        loop {
            if stop.load(atomic::Ordering::Relaxed) > 0 {
                break 0;
            } else {
                match read.read_new(&read_pos) {
                    Ok(result) => {
                        if result.msgTypeId() > 0 {
                            if result.messageId() <= max_message_id.load(atomic::Ordering::Relaxed)
                            {
                                message_processor.handle(&result);
                                loop {
                                    let top = pending_commit_queue.peek();
                                    match top {
                                        Some(t) => {
                                            if t.is_processed(&read_pos, &read_file_id) {
                                                match pending_commit_queue.poll() {
                                                    Some(f) => {
                                                        f.complete.send(Ok(()));
                                                    }
                                                    None => {
                                                        panic!("Something took the value from the peek.");
                                                    }
                                                }
                                            }
                                        }
                                        None => break,
                                    }
                                }
                                read_pos = result.next_pos();
                            } else {
                                thread::sleep_ms(1);
                            }
                        } else {
                            // TODO handle the cluster message.
                            read_pos = result.next_pos();
                        }
                    }
                    Err(e) => {
                        match e {
                            file::Error::NoMessage => {
                                thread::sleep_ms(1);
                            }
                            file::Error::Full => {
                                // TODO go to next file
                                let next_file_id = read_file_id + 1;
                                let next_path = create_event_name(
                                    &file_storage_directory,
                                    &file_prefix,
                                    &next_file_id,
                                );
                                match unsafe { MessageFileStore::open_readonly(&next_path) } {
                                    Ok(store) => {
                                        read = store;
                                        read_file_id = next_file_id;
                                        read_pos = 0;
                                    }
                                    Err(e) => {
                                        thread::sleep_ms(10);
                                    }
                                }
                            }
                            file::Error::NotEnoughSpace {
                                position,
                                capacity,
                                remaining,
                                message_size,
                            } => {
                                panic!("We are reading in a message this should never happen!");
                            }
                            file::Error::PositionOutOfRange(p) => {
                                // TODO go to next file
                                let next_file_id = read_file_id + 1;
                                let next_path = create_event_name(
                                    &file_storage_directory,
                                    &file_prefix,
                                    &next_file_id,
                                );
                                match unsafe { MessageFileStore::open_readonly(&next_path) } {
                                    Ok(store) => {
                                        read = store;
                                        read_file_id = next_file_id;
                                        read_pos = 0;
                                    }
                                    Err(e) => {
                                        thread::sleep_ms(10);
                                    }
                                }
                            }
                            file::Error::FileError(e) => {
                                thread::sleep_ms(2);
                            }
                            file::Error::InvalidFile | file::Error::AlreadyExists => {
                                // do nothing
                            }
                        }
                    }
                }
            }
        }
    })
}

pub fn startup_single_node<FRead>(
    file_storage_directory: String,
    file_prefix: String,
    max_file_size: usize,
    commit_file_size: usize,
    message_processor: FRead,
    incoming_buffer_size: &usize,
    incoming_queue_size: &usize,
) -> PersistedMessageFile
where
    FRead: MessageProcessor + 'static,
{
    let store_path = Path::new(&file_storage_directory);
    if !store_path.exists() {
        create_dir_all(&file_storage_directory).unwrap();
    }
    let collection = Arc::new(FileCollection::new(
        file_storage_directory.clone(),
        file_prefix.clone(),
    ));
    let max_message = Arc::new(AtomicU64::new(0));
    let message_files = collection.message_files.lock().unwrap();
    let writer = if message_files.len() > 0 {
        let file: &MessageFileInfo = message_files.get(message_files.len() - 1).unwrap();
        file.file_id
    } else {
        PersistedMessageWriteStream::new(
            1,
            file_storage_directory.clone(),
            file_prefix.clone(),
            max_file_size,
            max_message.clone(),
        )
        .unwrap();
        1
    };
    let (incoming_reader, incoming_writer) = create_many_to_one(incoming_buffer_size);
    let (queue_writer, queue_reader) = MpscQueueWrap::new(*incoming_queue_size);
    let (commit_writer, commit_reader) = SpscQueueSendWrap::new(*incoming_queue_size);
    let stop = Arc::new(AtomicU8::new(0));
    let writer_join = Some(write_thread_single(
        stop.clone(),
        queue_reader,
        incoming_reader,
        max_message.clone(),
        max_file_size,
        file_storage_directory.clone(),
        file_prefix.clone(),
        writer,
        commit_writer,
    ));
    let commit_join = Some(commit_thread_single(
        stop.clone(),
        file_storage_directory.clone(),
        file_prefix.clone(),
        commit_file_size,
        max_message.clone(),
        collection.clone(),
    ));
    let reader_join = Some(read_thread(
        stop.clone(),
        file_storage_directory.clone(),
        file_prefix.clone(),
        collection.clone(),
        message_processor,
        max_message.clone(),
        commit_reader,
    ));
    PersistedMessageFile {
        max_file_size,
        commit_join,
        writer_join,
        reader_join,
        stop,
        incoming_writer,
        incoming_queue_writer: queue_writer,
        max_message_id: max_message,
        file_storage_directory: file_storage_directory.clone(),
        file_prefix: file_prefix.clone(),
    }
}

/// Crates a new term file.
pub(crate) fn create_term_file(
    file_storage_directory: &str,
    file_prefix: &str,
    file_id: u32,
    term_start: u64,
    file_size: usize,
) -> TermFile {
    let path = create_commit_name(file_storage_directory, file_prefix, &file_id);
    let buffer = unsafe {MemoryMappedInt::new(&path, file_size)}.unwrap();
    TermFile::new(buffer, term_start, file_id)
}

impl PersistedMessageFile {
    /// Tells the processes to stop.
    pub fn stop(&mut self) {
        self.stop.store(1, atomic::Ordering::Release);
        self.writer_join.take().map(JoinHandle::join);
        self.reader_join.take().map(JoinHandle::join);
        self.commit_join.take().map(JoinHandle::join);
    }

    /// Writers a message to the buffer.
    /// # Arguments
    /// `msg_type_id` - The type of the message we are writing.
    /// `bytes` - The bytes to write the buffer.
    /// # Returns
    /// The future that gets completed.
    pub fn write(&self, msg_type_id: i32, bytes: &[u8]) -> QueueFuture<file::Result<()>> {
        let (sender, receiver) = oneshot::channel();
        match self.incoming_writer.write(msg_type_id, bytes) {
            Some(p) => {
                let add_message = AddMessageWriteRs::new(p, sender);
                if !self.incoming_queue_writer.offer(add_message) {
                    thread::sleep_ms(2);
                }
                receiver
            }
            None => {
                sender.send(Err(file::Error::Full));
                receiver
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::file::{MessageFileStore, MessageRead};
    use crate::raft::{
        create_event_name, find_end_of_buffer, load_current_files, process_files, read_file_id,
        startup_single_node, FileCollection, FindEmptySlotResult, MessageProcessor,
        PersistedMessageFile, PersistedMessageReadStream, PersistedMessageWriteStream,
    };
    use a19_concurrent::buffer::ring_buffer::create_many_to_one;
    use futures::future::Future;
    use std::fs::{create_dir_all, remove_dir_all};
    use std::path::Path;
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;
    use std::thread;

    const TEST_DIR: &str = "/home/mrh0057/cargo/tests/a19_data_persist";
    const TEST_PREFIX: &str = "test_persist";

    /// Used to crate the directory to test with.
    fn create_dir(name: &str) -> String {
        format!("{}_{}", TEST_DIR, name)
    }

    #[test]
    pub fn load_current_file_test() {
        let path = Path::new(TEST_DIR);
        if path.exists() && path.is_dir() {
            remove_dir_all(TEST_DIR).unwrap();
        }
        let mut files = load_current_files(TEST_PREFIX, TEST_DIR).unwrap();
        assert_eq!(files.commit_files.lock().unwrap().len(), 0);
        assert_eq!(files.message_files.lock().unwrap().len(), 0);

        let size: usize = 128 * 40;
        process_files(&mut files, TEST_PREFIX, TEST_DIR, &size, &size).unwrap();
        assert_eq!(files.commit_files.lock().unwrap().len(), 1);
        assert_eq!(files.message_files.lock().unwrap().len(), 1);

        let mut other_files = load_current_files(TEST_PREFIX, TEST_DIR).unwrap();
    }

    fn clean_dir() {
        let path = Path::new(TEST_DIR);
        if path.exists() && path.is_dir() {
            remove_dir_all(TEST_DIR).unwrap();
        }
    }

    #[test]
    pub fn file_id_test() {
        let file_test = "test.event.2";
        assert_eq!(read_file_id(file_test), Some(2));
    }

    #[test]
    pub fn find_end_of_buffer_test() {
        let file_storage_directory = format!("{}_end_of", TEST_DIR);
        let path = Path::new(&file_storage_directory);
        if path.exists() && path.is_dir() {
            remove_dir_all(&file_storage_directory).unwrap();
        }
        let message_id = Arc::new(AtomicU64::new(0));
        let mut writer = PersistedMessageWriteStream::new(
            1,
            file_storage_directory.to_owned(),
            TEST_PREFIX.to_owned(),
            2048,
            message_id,
        )
        .unwrap();
        let bytes: Vec<u8> = vec![10, 11, 12, 13, 14, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32];
        writer.add_message(&1, &1, &bytes[0..8]).unwrap();
        writer.flush().unwrap();
        let reader = unsafe {
            MessageFileStore::open_readonly(&create_event_name(
                &file_storage_directory,
                TEST_PREFIX,
                &1,
            ))
            .unwrap()
        };
        match find_end_of_buffer(&reader).unwrap() {
            FindEmptySlotResult::Pos(x, last_msg_id) => {
                assert_eq!(x, 32);
                assert_eq!(last_msg_id, 1);
            }
            _ => {
                panic!("Did not find the position.");
            }
        }
        // Test to see if we can read the buffer.
        let mut reader = PersistedMessageReadStream::new(
            1,
            0,
            Arc::new(AtomicU64::new(1)),
            MessageProcessorInt::new(),
            file_storage_directory.clone(),
            TEST_PREFIX.to_owned(),
        )
        .unwrap();
        let r = reader.process_next().unwrap();
        assert_eq!(true, r);
        let r = reader.process_next().unwrap();
        assert_eq!(false, r);
    }

    #[tokio::test]
    pub async fn create_single_node_processor() {
        let file_storage_directory = format!("{}_single_node", TEST_DIR);
        let path = Path::new(&file_storage_directory);
        if path.exists() && path.is_dir() {
            remove_dir_all(&file_storage_directory).unwrap();
        }
        let mut processor = MessageProcessorInt::new();
        let mut single_node = startup_single_node(
            file_storage_directory,
            TEST_PREFIX.to_owned(),
            5000,
            5000,
            processor,
            &0x40,
            &0x40,
        );
        let bytes: Vec<u8> = vec![10, 11, 12, 13, 14, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32];
        let r = single_node.write(1, &bytes[0..8]).await;
        r.unwrap();
        single_node.stop();
    }

    struct MessageProcessorInt {
        ran: bool,
        last_message_id: u64,
    }

    impl MessageProcessorInt {
        fn new() -> Self {
            MessageProcessorInt {
                ran: false,
                last_message_id: 0,
            }
        }
    }

    unsafe impl Send for MessageProcessorInt {}

    impl MessageProcessor for MessageProcessorInt {
        fn handle<'a>(&mut self, read: &MessageRead<'a>) {
            self.ran = true;
            self.last_message_id = read.messageId();
        }
    }
}
