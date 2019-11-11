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
//! ```text
//!  0                   1                   2                   3
//!  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//! | Term Id                                                       |
//! +-------------------------------+-------------------------------+ 32
//! | Version                       |                               |        
//! +-------------------------------+-------------------------------+ 64
//! | Server Id                                                     |
//! +---------------------------------------------------------------+ 96
//! | Created On Timestamp                                          |
//! +---------------------------------------------------------------+ 128
//! +---------------------------------------------------------------+ 160
//! +---------------------------------------------------------------+ 192
//! +---------------------------------------------------------------+ 224
//! +---------------------------------------------------------------+ 256
//! +---------------------------------------------------------------+ 288
//! +---------------------------------------------------------------+ 320
//! +---------------------------------------------------------------+ 352
//! +---------------------------------------------------------------+ 384
//! +---------------------------------------------------------------+ 416
//! +---------------------------------------------------------------+ 448
//! +---------------------------------------------------------------+ 480
//! +---------------------------------------------------------------+ 512
//! ```
pub mod network;

const EVENT_FILE_POSTFIX: &str = "events";
const COMMIT_FILE_POSTIX: &str = "commit";

use memmap::MmapMut;
use std::fs::{File, create_dir_all, remove_file};
use std::fs::read_dir;
use std::io::{Error, ErrorKind};
use std::io;
use std::io::BufWriter;
use std::io::prelude::*;
use std::path::{Path, MAIN_SEPARATOR};
use std::vec::Vec;

#[path="../target/a19_data_persist/message/persisted_file_generated.rs"]

/// Represents a message that was read in.
pub struct MessageInfo<'a> {
    message_id: u64,
    time_ms: i64,
    message_type: i32,
    message_body: &'a [u8],
}


/// Represents a term commited.
/// Represents whats committed and we only flush after the raft protocol has been update.
/// ```text
///  0                   1                   2                   3
///  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// | Term Id                                                       |
/// +-------------------------------+-------------------------------+ 32
/// | Version                       | Type                          |        
/// +-------------------------------+-------------------------------+ 64
/// | Server Id                                                     |
/// +-------------------------------+-------------------------------+ 96
/// | Commited                      | NOT USED FOR NOW              |
/// +-------------------------------+-------------------------------+ 128
/// |                                                               |
/// |               Start Timestamp                                 | 160
/// |                                                               |
/// +---------------------------------------------------------------+ 192
/// |                                                               |
/// |               Commited On Timestamp                           | 224
/// |                                                               |
/// +---------------------------------------------------------------+ 256
/// | Start                                                         |
/// +---------------------------------------------------------------+ 288
/// | End                                                           |
/// +---------------------------------------------------------------+ 320
/// ...   Note Used                                                 |
/// |                                                             ...
/// +---------------------------------------------------------------+ 512
/// ```
struct TermCommit {
    term_id: u32,
    version: u16,
    type_id: u16,
    commited: u16,
    timestamp: u64,
    committed_timestamp: u64,
    start: u32,
    end: u32
}

/// The persisted file.
pub struct PersistedMessageFile {
    /// The memory mapped file we are reading.
    mmap_mut: MmapMut,
    /// The file containig the commit information.
    commit_file: MmapMut,
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

enum FileState {
    /// The current file we are using.
    USING,
    /// Cleaning the file to be reused.
    CLEANING,
    /// The file is ready to be used.
    READY
}

struct FilePair {
    file_state: FileState,
    message_file: String,
    commite_file: String
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

impl PersistedMessageFile {
    pub fn new(
        file_prefix: String,
        flie_postfix: String,
        file_storage_directory: String,
        max_file_size: u64) {
            
    }

    /// Loads all of the current files.
    /// # Arguments
    /// `file_prefix` - The file prefix to load.
    /// `file_storage_directory` - The file storage directory.
    fn load_current_files(
        file_prefix: &str,
        file_storage_directory: &str,
        file_length: &usize) -> io::Result<Vec<FilePair>> {
        let dir_path = Path::new(&file_storage_directory);
        if dir_path.is_dir()
            || !dir_path.exists() {
            if !dir_path.exists() {
                create_dir_all(dir_path)?;
            }
            let mut file_vec = Vec::<FilePair>::with_capacity(20);
            for i in 1..4 {
                let file_events = format!("{}{}{}.{}.{}",
                    &file_storage_directory,
                    MAIN_SEPARATOR,
                    &file_prefix,
                    &EVENT_FILE_POSTFIX,
                    i);
                let file_commit = format!("{}{}{}.{}.{}", 
                    &file_storage_directory,
                    MAIN_SEPARATOR,
                    &file_prefix,
                    &COMMIT_FILE_POSTIX,
                    i);
                let file_events_path = Path::new(&file_events);
                let file_commit_path = Path::new(&file_commit);
                if file_events_path.is_file() 
                    && file_commit_path.is_file() {
                    // Check to see the status of the file by loading it.
                } else {
                    if file_events_path.is_file() {
                        remove_file(&file_events)?;
                    }
                    if file_commit_path.is_file() {
                        remove_file(&file_commit)?;
                    }
                    PersistedMessageFile::zero_new_file(
                        &file_events,
                        &file_length)?;
                    PersistedMessageFile::zero_new_file(
                        &file_commit,
                        &file_length)?;
                    file_vec.push(FilePair {
                        file_state: FileState::READY,
                        message_file: file_events,
                        commite_file: file_commit
                    })
                }
            }
            Ok(file_vec)

        }  else {
            Err(Error::new(ErrorKind::InvalidInput, "Path isn't a directory!"))
        }
    }

    fn zero_new_file(
        file_path: &str,
        file_length: &usize) -> io::Result<()> {
        let mut current_file = File::create(file_path)?;
        let mut buffer = BufWriter::new(&current_file);
        let value: [u8; 1024] = [0; 1024];
        let mut current_count = *file_length;
        loop {
            if current_count < 1024 {
                if current_count == 0 {
                    break Ok(())
                }
                buffer.write(&value[0..current_count])?;
                break Ok(())
            }
            buffer.write(&value)?;
            current_count = current_count - value.len();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::raft::{PersistedMessageFile, FilePair};
    use std::fs::{remove_dir_all};

    const TEST_DIR:&str = "/home/mrh0057/cargo/tests/a19_data_persist";
    const TEST_PREFIX: &str = "test_persist";

    #[test]
    pub fn load_current_file_test() {
        remove_dir_all(TEST_DIR).unwrap();
        let files = PersistedMessageFile::load_current_files(
            TEST_PREFIX,
            TEST_DIR,
            &1_000_000).unwrap();
        assert_eq!(3, files.len());
    }
}
