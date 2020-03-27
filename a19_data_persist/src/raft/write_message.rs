use std::collections::BTreeMap;
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
    /// The collection of files.
    files: BTreeMap<u32, MessageFileInfo>,
    /// The storage directory of the files.
    file_storage_directory: String,
    /// The file prefix.
    file_prefix: String,
}

impl MessageWriteCollection {
    
    /// Opens a directory and reads in the files containing the messages.
    /// # Arguments
    /// `file_storage_directory` - The directory containing the files.
    /// `file_prefix` - The file prefix.
    /// # Returns
    /// The message write collection.
    fn open_dir(file_storage_directory: &str, file_prefix: &str) -> crate::file::Result<Self> {
        let directory_path = Path::new(file_storage_directory);
        if !directory_path.is_dir() || !directory_path.exists() {
            create_dir(directory_path)?;
        }
        let starts_with_commit = format!("{}.{}", file_prefix, COMMIT_FILE_POSTIX);
        let mut collection = BTreeMap::new();
        for entry in read_dir(directory_path)? {
            let file = entry?;
            let path: PathBuf = file.path();
            if path.is_file() {
                match path.file_name() {
                    Some(p) => {
                        match p.to_str() {
                            Some(p) => {
                                if p.starts_with(&starts_with_commit) {
                                    match read_file_id(p) {
                                        Some(id) => {
                                            let message_file =  get_message_file_info(path.to_str().unwrap())?;
                                            collection.insert(id, message_file);
                                        }
                                        _ => {
                                            
                                        }
                                    }
                                }
                            }
                            _ => {

                            }
                        }
                    }
                    _ => {
                        
                    }
                }
            }
        }
        Ok(MessageWriteCollection{
            files: collection,
            file_prefix: file_prefix.to_string(),
            file_storage_directory: file_storage_directory.to_string(),
        })
    }

    /// Used to get a file with the specified id.
    /// # Arguments
    /// `file_id` - The id of the file to get.
    /// # Returns
    /// The file message file info.
    fn get_message_file<'a>(&'a self, file_id: &u32) -> Option<&'a MessageFileInfo> {
        self.files.get(file_id)
    }
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
            let result = read.read(0, |_, id, _| {
                if id > 0 {
                    msg_id = id;
                }
            })?;
            Ok(MessageFileInfo {
                path: path.to_owned(),
                file_id: id,
                message_id_start: msg_id,
            })
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
    use crate::raft::write_message::*;
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

    #[test]
    #[serial]
    pub fn read_existing_file() {
        cleanup();
        {
            let (file_info, r) = new_message_file(FILE_STORAGE_DIRECTORY, FILE_PREFIX, 1, 1, 32 * 1000).unwrap();
            r.write(&0, &1, &1, &[2; 50]);
            r.flush();
        }
        let path = create_commit_name(FILE_STORAGE_DIRECTORY, FILE_PREFIX, &1);
        let file_info = get_message_file_info(&path).unwrap();
        assert_eq!(file_info.file_id, 1);
        assert_eq!(file_info.message_id_start, 1);
        assert_eq!(path, file_info.path);
    }

    #[test]
    #[serial]
    pub fn get_current_file_collection() {
        cleanup();
        {
            let (file_info, r) = new_message_file(FILE_STORAGE_DIRECTORY, FILE_PREFIX, 1, 1, 32 * 1000).unwrap();
            r.write(&0, &1, &1, &[2; 50]);
            r.flush();

            let (file_info, r) = new_message_file(FILE_STORAGE_DIRECTORY, FILE_PREFIX, 2, 1, 32 * 1000).unwrap();
            r.write(&0, &2, &1, &[2; 50]);
            r.flush();
        }
        let message_file = MessageWriteCollection::open_dir(FILE_STORAGE_DIRECTORY, FILE_PREFIX).unwrap();
        assert_eq!(message_file.files.len(), 2);
        let file = message_file.get_message_file(&1).unwrap();
        assert_eq!(file.file_id, 1);
        assert_eq!(file.message_id_start, 1);
        let file = message_file.get_message_file(&2).unwrap();
        assert_eq!(file.file_id, 2);
        assert_eq!(file.message_id_start, 2);
    }
}
