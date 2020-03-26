use std::collections::HashMap;
use crate::file::*;
use crate::raft::*;

/// Used for when we are the leader and appending messages.
pub(crate) struct MessageWriteFileAppend {
    file_storage_directory: String,
    file_prefix: String,
    path: String,
    file_id: u32,
    file_size: usize,
    start_message_id: u64,
    writer: MessageFileStoreWrite,
}

impl MessageWriteFileAppend {

    /// The file directory to open.
    /// # Arguments
    /// `file_storage_directory` - The file storage directory.
    /// `file_prefix` - The file prefix.
    /// `file_size` - The file size for the messages.
    pub(crate) fn open(
        file_storage_directory: String,
        file_prefix: String,
        file_size: usize,
    ) {
        
    }
}

/// Used for random writes.
pub(crate) struct MessageWriteFileSeek {
    path: String,
    file_id: u32,
    writer: MessageFileStoreWrite,
}

pub(crate) enum MessageWriteFileType {
    Append(MessageWriteFileAppend),
    Seek(MessageWriteFileSeek),
}

pub(crate) struct MessageWriteCollection {
    files: HashMap<u32, MessageFileInfo>,
    file_storage_directory: String,
    file_prefix: String,
}

/// Used to crate a new message file.
/// # Arguments
fn new_message_file(
    file_storage_directory: &str,
    file_prefix: &str,
    file_id: u32,
    message_id: u64,
    file_size: usize,
) -> crate::file::Result<(MessageFileInfo, MessageFileStoreWrite)> {
    let path = create_commit_name(file_storage_directory, file_prefix, &file_id);
    let path_p = Path::new(&path);
    if path_p.exists() {
        let (read, write) = unsafe {MessageFileStore::new(&path, file_size)?};
        match read.read(0, |_, id, _| {
            
        }) {
            Ok(_) => Err(crate::file::Error::AlreadyExists)?,
            _ => {
                remove_file(&path)?;
            }
        }
    }
    let (_, writer) = unsafe {MessageFileStore::new(&path, file_size)?};
    Ok((MessageFileInfo {
        path: path.to_string(),
        file_id,
        message_id_start: message_id
    }, writer))
}

/// Gets the information about the file.
/// # Arguments
/// `path` - The path of the file to get the information from.
fn get_message_file_info(path: &str) -> crate::file::Result<MessageFileInfo> {
    match read_file_id(&path) {
        Some(id) => {
            let (read, _) = unsafe {MessageFileStore::open(&path)?};
            let mut msg_id: u64 = 0;
            {
                let result = read.read(0, move |_, id, _| {
                    if id > 0 {
                        msg_id = id;
                    }
                })?;
                Ok(MessageFileInfo {
                    path: path.to_owned(),
                    file_id: id,
                    message_id_start: msg_id,
                })
            }
        },
        _ => {
            Err(crate::file::Error::InvalidFile)
        }
    }
}

#[cfg(test)]
mod test {
    use std::fs::*;
    use std::path::Path;
    use crate::raft::write::*;
    use serial_test::serial;

    const FILE_STORAGE_DIRECTORY: &str = "/home/mrh0057/cargo/tests/message_write_test";
    const FILE_PREFIX: &str = "write";

    fn cleanup() {
        let path = Path::new(FILE_STORAGE_DIRECTORY);
        if path.exists() {
            remove_dir_all(&path);
        }
        create_dir(&path);
    }
    
    #[test]
    #[serial]
    pub fn new_file_test_none_exists() {
        cleanup();
        let (file_info, r) = new_message_file(FILE_STORAGE_DIRECTORY, FILE_PREFIX, 1, 1, 32 * 1000).unwrap();
    }

    #[test]
    #[serial]
    pub fn new_file_test_exists_empty() {
        cleanup();
        let (file_info, r) = new_message_file(FILE_STORAGE_DIRECTORY, FILE_PREFIX, 1, 1, 32 * 1000).unwrap();
        let (file_info, r) = new_message_file(FILE_STORAGE_DIRECTORY, FILE_PREFIX, 1, 1, 32 * 1000).unwrap();
    }

    #[test]
    #[serial]
    pub fn new_file_test_not_empty() {
        cleanup();
        let (file_info, r) = new_message_file(FILE_STORAGE_DIRECTORY, FILE_PREFIX, 1, 1, 32 * 1000).unwrap();
        r.write(&0, &1, &1, &[2; 50]);
        match new_message_file(FILE_STORAGE_DIRECTORY, FILE_PREFIX, 1, 1, 32 * 1000) {
            Ok(_) => assert!(false),
            Err(e) => match e {
                crate::file::Error::AlreadyExists => {
                    // Success
                },
                _ => {
                    assert!(false);
                }
            }
        }
    }
}
