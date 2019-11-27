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

const EVENT_FILE_POSTFIX: &str = "events";
const COMMIT_FILE_POSTIX: &str = "commit";
const HEADER_SIZE: u64 = 512;

use memmap::MmapMut;
use a19_concurrent::buffer::DirectByteBuffer;
use a19_concurrent::buffer::mmap_buffer::MemoryMappedInt;
use crate::file::{MessageFileStore, MessageFileStoreWrite, MessageFileStoreRead};
use crate::file;
use a19_concurrent::buffer::atomic_buffer::{AtomicByteBuffer, AtomicByteBufferInt};
use std::sync::Arc;
use std::cmp::Ordering;
use std::fs::*;
use std::io::{Error, ErrorKind};
use std::io;
use std::collections::HashMap;
use std::io::{BufWriter, BufReader};
use std::io::prelude::*;
use std::path::{Path, MAIN_SEPARATOR, PathBuf};
use std::vec::Vec;

#[path="../target/a19_data_persist/message/persisted_file_generated.rs"]

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
    length: u32
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
/// | Commited                      | NOT USED FOR NOW              |
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
/// +---------------------------------------------------------------+ 512 | 64
/// ..                                                              |
/// |                                                               ...
/// +---------------------------------------------------------------+ 1024 | 128
/// ```
const TERM_ID_OFFSET: usize = 0;
const VERSION_OFFSET: usize = 8;
const TYPE_OFFSET: usize = 10;
const SERVER_OFFSET: usize = 12;
const LEADER_OFFSET: usize = 16;
const COMMITED: usize = 20;
const START_TIMESTAMP: usize = 24;
const COMMITED_TIMESTAMP: usize = 32;
const FILE_ID: usize = 40;
const FILE_POSITION_OFFSET: usize = 44;
const MAX_MESSAGE_ID: usize = 52;
const LENGTH_OF_COMMIT: usize = 60;

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
    fn set_commited(&mut self, pos: &usize) -> &mut Self;
    fn commited(&self, pos: &usize) -> u16;
    fn set_start_time(&mut self, pos: &usize, time: &u64) -> &mut Self;
    fn start_time(&self, pos: &usize) -> u64;
    fn set_commited_timestamp(&mut self, pos: &usize, val: &u64) -> &mut Self;
    fn commited_timestamp(&mut self, pos: &usize) -> u64;
    fn set_file_id(&mut self, pos: &usize, val: &u32) -> &mut Self;
    fn file_id(&self, pos: &usize) -> u32;
    fn set_file_position_offset(&mut self, pos: &usize, val: &u64) -> &mut Self;
    fn file_position_offset(&self, pos: &usize) -> u64;
    fn set_max_message_id(&mut self, pos: &usize, val: &u64) -> &mut Self;
    fn max_message_id(&self, pos: &usize) -> u64;
    fn set_length_of_commit(&mut self, pos: &usize, val: &u32) -> &mut Self;
    fn length_of_commit(&self, pos: &usize) -> u32;
    fn save_term(&mut self, pos: &usize, term: TermCommit) -> &mut Self;
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
    fn set_commited(&mut self, pos: &usize) -> &mut Self {
        let pos = COMMITED + *pos;
        self.put_u16(&pos, 1);
        self
    }

    #[inline]
    fn commited(&self, pos: &usize) -> u16 {
        let pos = COMMITED + *pos;
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
    fn set_commited_timestamp(&mut self, pos: &usize, val: &u64) -> &mut Self {
        let pos = COMMITED_TIMESTAMP + *pos;
        self.put_u64(&pos, *val);
        self
    }

    #[inline]
    fn commited_timestamp(&mut self, pos: &usize) -> u64 {
        let pos = COMMITED_TIMESTAMP + *pos;
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
    fn save_term(&mut self, pos: &usize, term: TermCommit) -> &mut Self {
        self.set_term(pos, &term.term_id)
            .set_version(pos, &term.version)
            .set_msg_type(pos, &term.type_id)
            .set_server(pos, &term.server_id)
            .set_leader(pos, &term.leader_id)
            .set_start_time(pos, &term.timestamp)
            .set_commited_timestamp(pos, &term.committed_timestamp)
            .set_file_id(pos, &term.file_id)
            .set_file_position_offset(pos, &term.file_position_offset)
            .set_max_message_id(pos, &term.file_max_message_id)
            .set_length_of_commit(pos, &term.length)
            ;
        self
    }
}

/// A term committed in the raft protocol.
struct TermCommit<'a> {
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
    commited: u16,
    /// The time stamp when the message was created.
    timestamp: u64,
    /// The commited timestamp.
    committed_timestamp: u64,
    /// The id of the file we are committing to.
    file_id: u32,
    /// The offset of the possition of the term.
    file_position_offset: u64,
    /// The max messageid.
    file_max_message_id: u64,
    /// The length of the message commit.
    length: u32,
    /// The buffer associated with this term.
    buffer: &'a [u8]
}

/// The state of the raft node.
enum RaftNodeState {
    Follower = 0,
    Candidate = 1,
    Leader = 2
}

/// The events for the raft protocol.
enum RaftNodeEvent {
    ElectionTimeout = 0,
    NewTerm = 1,
    HeartbeatFailed = 2,
    HeartbeatTimeout = 3,
    HigherTerm = 4,
}

/// The persisted file.
pub struct PersistedMessageFile {
    /// The memory mapped file we are reading.
    buffer_writer: MessageFileStoreWrite,
    /// The buffer to read.
    buffer_reader: MessageFileStoreRead,
    /// The file containig the commit information.
    commit_file: MemoryMappedInt,
    /// The id of the file we are current writting to.  This can be 1, 2 or 3.
    file_id: u32,
    /// The file prefix on where to store the file.
    file_prefix: String,
    /// The file storate directory.
    file_storage_directory: String,
    /// The maximum file size before it roles overs.
    max_file_size: usize,
    /// The current position in the file.
    current_position: usize,
    /// The id of the current term.
    current_term_id: usize,
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
    fn read<'a>(
        &'a self,
        start: usize) -> MessageInfo;

    /// Reads messages until the func returns false.
    /// # Arguments
    /// `start` - The starting point to start reading the messages.
    /// `func` - The function to call when a message is read in.
    fn read_msg_til<'a>(
        &'a self,
        start: usize,
        func: fn(message: MessageInfo) -> bool) -> u32;
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
    fn write(
        &mut self,
        start: &usize,
        term_id: &u32,
        time: &u64,
        body: &[u8]);

    /// Commits a term after the raft sync.  Flushes the data to disks.
    /// `term_id` - The id of the term we are committing.
    fn commit_term(
        &mut self,
        term_id: &u32);

    /// Reads a message a speific location.
    /// # Arguments
    /// `start` - The starting index to read in.
    fn read<'a>(
        &'a mut self,
        start: &usize) -> MessageInfo;

    /// Flushes the written data to the disk.
    fn flush(&mut self);

    /// The capacity of the file.
    fn capacity(&self) -> usize;
}

#[derive(Debug)]
struct MessageFileInfo {
    path: String,
    file_id: u32,
    message_id_start: u64
}

#[derive(Eq, Debug)]
struct CommitFileInfo {
    path: String,
    file_id: u32,
    term_start: u64
}

impl MessageFileInfo {

    /// Used to create a new file info.
    /// # Arguments
    /// `path` - The path to the file.
    /// `file_id` - The id of the file.
    fn new(
        path: String,
        file_id: u32,
        message_id_start: u64) -> Self {
        MessageFileInfo {
            path,
            file_id,
            message_id_start
        }
    }
}

impl CommitFileInfo {

    fn new(
        path: String,
        file_id: u32,
        term_start: u64) -> Self {
        CommitFileInfo {
            path,
            file_id,
            term_start
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
struct FileCollection {
    /// The list of files containing the commit information.
    commit_files: Vec<CommitFileInfo>,
    /// A map of the message files.
    message_files: HashMap<u32, MessageFileInfo>
}

impl FileCollection {

    /// Gets a new file collection.
    fn new() -> Self {
        FileCollection {
            commit_files: Vec::with_capacity(10),
            message_files: HashMap::with_capacity(10)
        }
    }

    /// Gets the file id from the extension.
    /// # Arguments
    /// path_str - The path string to parse in.
    /// # Returns
    /// The id of the file if it can be parsed in.
    fn read_file_id(path_str: &str) -> Option<u32> {
        let file_id_opt: Option<&str> = path_str.split(".").last();
        match file_id_opt {
            Some(file_id) => {
                match file_id.parse::<u32>() {
                    Ok(id) => {
                        Some(id)
                    },
                    _ => {
                        None
                    }
                }
            },
            None => {
                None
            }
        }
    }

    /// Adds a message file if it has a message id.
    /// # Arguments
    /// `path` - The path buf to the file.
    /// `path_str` - The path of the file as a string.
    fn add_message_file(&mut self, path: PathBuf, path_str: &str) -> std::io::Result<()> {
        match FileCollection::read_file_id(path_str) {
            Some(id) => {
                let (read, _) = unsafe {MessageFileStore::open(&path_str)?};
                let mut msg_id: u64 = 0;
                {
                    let result = read.read(0, move |_, id, _|{
                        if id > 0 {
                            msg_id = id;
                        }
                    });
                    match result {
                        Ok(_) => {
                            self.message_files.insert(
                                id,
                                MessageFileInfo {
                                    path: path_str.to_owned(),
                                    file_id: id,
                                    message_id_start: msg_id
                                }
                            );
                            Ok(())
                        },
                        Err(e) => {
                            match e {
                                file::Error::FileError(e) => {
                                    Err(e)
                                }
                                _ => {
                                    Ok(())
                                }
                            }
                        }
                    }?
                }
                Ok(())
            },
            _ => {
                Ok(())
            }
        }
    }

    /// Used to add a commit file.
    /// # Arguments
    /// `path` - The path buffer for the file.
    /// `path_str` - The path string.
    fn add_commit_file(&mut self, path: PathBuf, path_str: &str) -> std::io::Result<()> {
        match FileCollection::read_file_id(path_str) {
            Some(id) => {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(false)
                    .open(path_str)?;
                let buffer = unsafe {MemoryMappedInt::open(file)}?;
                let term_id = buffer.term(&0); // Get the starting message.
                let time = buffer.start_time(&0);
                if time > 0 {
                    self.commit_files.push(CommitFileInfo::new(path_str.to_owned(), id, term_id));
                    Ok(())
                } else {
                    // Not sure what we should do with the file since it's not valid.
                    Ok(())
                }
            },
            _ => {
                Ok(())
            }
        }
    }

}

impl PersistedMessageFile {

    /// Used to create a new persisted file.
    pub fn new(
        file_prefix: String,
        flie_postfix: String,
        file_storage_directory: String,
        max_file_size: u64,
        commit_file_size: u64) {
            
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
        commit_file_size: &usize) -> std::io::Result<()> {
        let path = Path::new(file_storage_directory);
        if !path.exists() {
            create_dir_all(file_storage_directory)?;
        }
        if (file_collection.commit_files.len() == 0
            && file_collection.message_files.len() == 0) {
            let file_id: u32 = 1;
            let path_commit = PersistedMessageFile::create_event_name(
                file_storage_directory,
                file_prefix,
                &file_id);
            let (read, write) = unsafe{MessageFileStore::new(
                &path_commit,
                *max_file_size)?};
            let path_event = PersistedMessageFile::create_commit_name(
                file_storage_directory,
                file_prefix,
                &file_id);
            let commit_file = unsafe{MemoryMappedInt::new(
                &path_event,
                *commit_file_size)?};
            Ok(())
        } else {
            Ok(())
        }
    }

    /// Used to create the commit file name.
    fn create_commit_name(
        file_storage_directory: &str,
        file_prefix: &str, 
        file_id: &u32) -> String {
        format!("{}{}{}.{}.{}", file_storage_directory, MAIN_SEPARATOR, file_prefix, COMMIT_FILE_POSTIX, file_id)
    }

    fn create_event_name(
        file_storage_directory: &str,
        file_prefix: &str,
        file_id: &u32) -> String {
        format!("{}{}{}.{}.{}", file_storage_directory, MAIN_SEPARATOR, file_prefix, EVENT_FILE_POSTFIX, file_id)
    }

    /// Loads all of the current files.
    /// # Arguments
    /// `file_prefix` - The file prefix to load.
    /// `file_storage_directory` - The file storage directory.
    fn load_current_files(
        file_prefix: &str,
        file_storage_directory: &str) -> io::Result<FileCollection> {
        let dir_path = Path::new(&file_storage_directory);
        if dir_path.is_dir()
            || !dir_path.exists() {
            if !dir_path.exists() {
                create_dir_all(dir_path)?;
            }
            let mut file_collection = FileCollection::new();
            let starts_with_events = format!("{}.{}", &file_prefix, &EVENT_FILE_POSTFIX);
            let starts_with_commits = format!("{}.{}", &file_prefix, &COMMIT_FILE_POSTIX);
            for entry in read_dir(file_storage_directory)? {
                let file = entry?;
                let path: PathBuf = file.path();
                if path.is_file() {
                    match path.as_os_str().to_str() {
                        Some(p) => {
                            if p.starts_with(&starts_with_commits) {
                                file_collection.add_message_file(
                                    path.clone(),
                                    p)?;
                            } else if p.starts_with(&starts_with_events) {
                                file_collection.add_commit_file(
                                    path.clone(),
                                    p)?;
                            }
                        },
                        None => {
                            // Skip for now.
                        }
                    }
                }
            }
            Ok(file_collection)

        }  else {
            Err(Error::new(ErrorKind::InvalidInput, "Path isn't a directory!"))
        }
    }

}

#[cfg(test)]
mod tests {

    use std::fs::{remove_dir_all, create_dir_all};
    use std::path::Path;
    use crate::raft::{PersistedMessageFile, FileCollection};

    const TEST_DIR:&str = "/home/mrh0057/cargo/tests/a19_data_persist";
    const TEST_PREFIX: &str = "test_persist";

    #[test]
    pub fn load_current_file_test() {
        let path = Path::new(TEST_DIR);
        if path.exists() && path.is_dir() {
            remove_dir_all(TEST_DIR).unwrap();
        }
        let mut files = PersistedMessageFile::load_current_files(TEST_PREFIX, TEST_DIR).unwrap();
        assert_eq!(files.commit_files.len(), 0);
        assert_eq!(files.message_files.len(), 0);

        let size: usize = 128 * 40;
        PersistedMessageFile::process_files(&mut files, TEST_PREFIX, TEST_DIR, &size, &size).unwrap();
    }

    #[test]
    pub fn file_id_test() {
        let file_test = "test.event.2";
        assert_eq!(FileCollection::read_file_id(file_test), Some(2));
    }
}
