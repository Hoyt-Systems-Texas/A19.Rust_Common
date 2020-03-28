use crate::file::*;
use crate::raft::*;
use std::collections::BTreeMap;
use std::rc::Rc;

/// Used for when we are the leader and appending messages.
#[allow(dead_code)]
pub(crate) struct MessageWriteFileAppend {
    file_storage_directory: String,
    file_prefix: String,
    path: String,
    file_id: u32,
    file_size: usize,
    start_message_id: u64,
    writer: MessageWriteAppend,
}

#[allow(dead_code)]
impl MessageWriteFileAppend {
    /// The file directory to open.
    /// # Arguments
    /// `file_storage_directory` - The file storage directory.
    /// `file_prefix` - The file prefix.
    /// `file_size` - The file size for the messages.
    pub(crate) fn open(file_storage_directory: String, file_prefix: String, file_size: usize) {}
}

/// Used for random writes.
#[allow(dead_code)]
pub(crate) struct MessageWriteFileSeek {
    path: String,
    file_id: u32,
    writer: MessageFileStoreWrite,
}

#[allow(dead_code)]
pub(crate) struct MessageWriteCollection {
    /// The collection of files.
    files: BTreeMap<u32, Rc<MessageFileInfo>>,
    /// The storage directory of the files.
    file_storage_directory: String,
    /// The file prefix.
    file_prefix: String,
    /// The size of the message to create.
    file_size: usize,
}

impl MessageWriteCollection {
    /// Opens a directory and reads in the files containing the messages.
    /// # Arguments
    /// `file_storage_directory` - The directory containing the files.
    /// `file_prefix` - The file prefix.
    /// # Returns
    /// The message write collection.
    pub(crate) fn open_dir(
        file_storage_directory: &str,
        file_prefix: &str,
        file_size: usize,
    ) -> crate::file::Result<Self> {
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
                    Some(p) => match p.to_str() {
                        Some(p) => {
                            if p.starts_with(&starts_with_commit) {
                                match read_file_id(p) {
                                    Some(id) => {
                                        let message_file =
                                            get_message_file_info(path.to_str().unwrap())?;
                                        collection.insert(id, Rc::new(message_file));
                                    }
                                    _ => {}
                                }
                            }
                        }
                        _ => {}
                    },
                    _ => {}
                }
            }
        }
        Ok(MessageWriteCollection {
            files: collection,
            file_prefix: file_prefix.to_string(),
            file_storage_directory: file_storage_directory.to_string(),
            file_size,
        })
    }

    /// Used to get a file with the specified id.
    /// # Arguments
    /// `file_id` - The id of the file to get.
    /// # Returns
    /// The file message file info.
    pub(crate) fn get_message_file<'a>(&'a self, file_id: &u32) -> Option<&'a Rc<MessageFileInfo>> {
        self.files.get(file_id)
    }

    /// Used to get the current append file.
    /// # Arguments
    /// `max_message_id` - The maximum message id to stop at.
    /// # Returns
    /// A tuple containing the file to write to and the message file information.
    pub(crate) fn get_current_append<'a>(
        &'a mut self,
        max_message_id: u64,
    ) -> crate::file::Result<MessageWriteAppend> {
        if self.files.is_empty() {
            let (file_info, writer) = new_message_file(
                &self.file_storage_directory,
                &self.file_prefix,
                1,
                max_message_id,
                self.file_size,
            )?;
            let file_info = Rc::new(file_info);
            self.files.insert(1, file_info.clone());
            let r = MessageWriteAppend::open(file_info, max_message_id, writer)?;
            match r {
                OpenFileResult::Opened(app) => Ok(app),
                _ => panic!("Creating a new file should have room!"),
            }
        } else {
            let mut iter = self.files.iter().rev();
            loop {
                if let Some((_, file)) = iter.next() {
                    let file: &Rc<MessageFileInfo> = file;
                    if file.message_id_start <= max_message_id {
                        let writer = file.open_write(&self.file_size)?;
                        match MessageWriteAppend::open(file.clone(), max_message_id, writer)? {
                            OpenFileResult::Full => {
                                let file_id = file.file_id + 1;
                                if let Some(file) = self.files.get(&file_id) {
                                    let writer = file.open_write(&self.file_size)?;
                                    if let OpenFileResult::Opened(w) = MessageWriteAppend::open(file.clone(), max_message_id, writer)? {
                                        break Ok(w)
                                    } else {
                                        panic!("File is saying full when we should be at the top!");
                                    }
                                } else {
                                    let (file_info, writer) = new_message_file(
                                        &self.file_storage_directory,
                                        &self.file_prefix,
                                        file_id,
                                        max_message_id,
                                        self.file_size,
                                    )?;
                                    let file_info = Rc::new(file_info);
                                    self.files.insert(file_id, file_info.clone());
                                    if let OpenFileResult::Opened(w) = MessageWriteAppend::open(file_info, max_message_id, writer)? {
                                        break Ok(w)
                                    } else {
                                        panic!("Creating a new file should have room!")                  
                                    }
                                }
                            }
                            OpenFileResult::Opened(w) => break Ok(w),
                        }
                    }
                } else {
                    panic!("Can't find a starting point!");
                }
            }
        }
    }
}

/// Represents a message write append.
#[derive(Debug)]
pub(crate) struct MessageWriteAppend {
    last_commit_id: u64,
    writer: MessageFileStoreWrite,
    next_pos: usize,
    message_file_info: Rc<MessageFileInfo>,
}

#[derive(Debug)]
enum OpenFileResult {
    Full,
    Opened(MessageWriteAppend),
}

impl MessageWriteAppend {
    /// Used to open file at the end of the position.
    /// # Arguments
    /// `path` - The path of the file to open.
    /// `last_committed_id` - The last committed id.
    /// `writer` - The writer associated with the file.
    /// # Returns
    /// Either the message append or full.  If its full need to go to the next file.
    fn open(
        msg_file: Rc<MessageFileInfo>,
        last_commited_id: u64,
        writer: MessageFileStoreWrite,
    ) -> crate::file::Result<OpenFileResult> {
        let mut pos = 0;
        let buffer = unsafe { MessageFileStore::open_readonly(&msg_file.path)? };
        loop {
            match buffer.read_new(&pos) {
                Ok(msg) => {
                    if msg.message_id() >= last_commited_id {
                        pos = msg.next_pos();
                        if buffer.is_end(&pos) {
                            break Ok(OpenFileResult::Full);
                        } else {
                            break Ok(OpenFileResult::Opened(Self {
                                writer,
                                last_commit_id: last_commited_id,
                                next_pos: pos,
                                message_file_info: msg_file,
                            }));
                        }
                    } else if msg.message_id() == std::u64::MAX {
                        break Ok(OpenFileResult::Full);
                    } else {
                        pos = msg.next_pos();
                    }
                }
                Err(e) => match e {
                    crate::file::Error::NoMessage => {
                        break Ok(OpenFileResult::Opened(Self {
                            writer,
                            last_commit_id: last_commited_id,
                            next_pos: pos,
                            message_file_info: msg_file,
                        }))
                    }
                    crate::file::Error::PositionOutOfRange(_) => break Ok(OpenFileResult::Full),
                    _ => Err(e)?,
                },
            }
        }
    }

    /// Used to append a message to the current log.
    pub(crate) fn append(&mut self, message_id: &u64, msg_type_id: &u32, bytes: &[u8]) {}
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
        let (read, write) = unsafe { MessageFileStore::new(&path, file_size)? };
        match read.read(0, |_, id, _| {}) {
            Ok(_) => Err(crate::file::Error::AlreadyExists)?,
            _ => {
                remove_file(&path)?;
            }
        }
    }
    let (_, writer) = unsafe { MessageFileStore::new(&path, file_size)? };
    Ok((
        MessageFileInfo {
            path: path.to_string(),
            file_id,
            message_id_start: message_id,
        },
        writer,
    ))
}

/// Gets the information about the file.
/// # Arguments
/// `path` - The path of the file to get the information from.
fn get_message_file_info(path: &str) -> crate::file::Result<MessageFileInfo> {
    match read_file_id(&path) {
        Some(id) => {
            let (read, _) = unsafe { MessageFileStore::open(&path)? };
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
        }
        _ => Err(crate::file::Error::InvalidFile),
    }
}

#[cfg(test)]
mod test {
    use crate::raft::write_message::*;
    use serial_test::serial;
    use std::fs::*;
    use std::path::Path;

    const FILE_STORAGE_DIRECTORY: &str = "../../../cargo/tests/message_write_test";
    const FILE_PREFIX: &str = "write";

    fn cleanup() {
        let path = Path::new(FILE_STORAGE_DIRECTORY);
        if path.exists() {
            remove_dir_all(&path).unwrap();
        }
        create_dir(&path).unwrap();
    }

    #[test]
    #[serial]
    pub fn new_file_test_none_exists() {
        cleanup();
        let (file_info, r) =
            new_message_file(FILE_STORAGE_DIRECTORY, FILE_PREFIX, 1, 1, 32 * 1000).unwrap();
    }

    #[test]
    #[serial]
    pub fn new_file_test_exists_empty() {
        cleanup();
        let (file_info, r) =
            new_message_file(FILE_STORAGE_DIRECTORY, FILE_PREFIX, 1, 1, 32 * 1000).unwrap();
        let (file_info, r) =
            new_message_file(FILE_STORAGE_DIRECTORY, FILE_PREFIX, 1, 1, 32 * 1000).unwrap();
    }

    #[test]
    #[serial]
    pub fn new_file_test_not_empty() {
        cleanup();
        let (file_info, r) =
            new_message_file(FILE_STORAGE_DIRECTORY, FILE_PREFIX, 1, 1, 32 * 1000).unwrap();
        r.write(&0, &1, &1, &[2; 50]).unwrap();
        match new_message_file(FILE_STORAGE_DIRECTORY, FILE_PREFIX, 1, 1, 32 * 1000) {
            Ok(_) => assert!(false),
            Err(e) => match e {
                crate::file::Error::AlreadyExists => {
                    // Success
                }
                _ => {
                    assert!(false);
                }
            },
        }
    }

    #[test]
    #[serial]
    pub fn read_existing_file() {
        cleanup();
        {
            let (file_info, r) =
                new_message_file(FILE_STORAGE_DIRECTORY, FILE_PREFIX, 1, 1, 32 * 1000).unwrap();
            r.write(&0, &1, &1, &[2; 50]).unwrap();
            r.flush().unwrap();
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
            let (file_info, r) =
                new_message_file(FILE_STORAGE_DIRECTORY, FILE_PREFIX, 1, 1, 32 * 1000).unwrap();
            r.write(&0, &1, &1, &[2; 50]).unwrap();
            r.flush().unwrap();

            let (file_info, r) =
                new_message_file(FILE_STORAGE_DIRECTORY, FILE_PREFIX, 2, 1, 32 * 1000).unwrap();
            r.write(&0, &1, &2, &[2; 50]).unwrap();
            r.flush().unwrap();
        }
        let message_file =
            MessageWriteCollection::open_dir(FILE_STORAGE_DIRECTORY, FILE_PREFIX, 32 * 1000)
                .unwrap();
        assert_eq!(message_file.files.len(), 2);
        let file = message_file.get_message_file(&1).unwrap();
        assert_eq!(file.file_id, 1);
        assert_eq!(file.message_id_start, 1);
        let file = message_file.get_message_file(&2).unwrap();
        assert_eq!(file.file_id, 2);
        assert_eq!(file.message_id_start, 2);
    }

    #[test]
    #[serial]
    pub fn get_current_file_empty() {
        remove_dir_all(FILE_STORAGE_DIRECTORY).unwrap();
        let message_file =
            MessageWriteCollection::open_dir(FILE_STORAGE_DIRECTORY, FILE_PREFIX, 32 * 1000)
                .unwrap();
    }

    #[test]
    #[serial]
    pub fn new_file_test() {
        cleanup();
        let mut message_file =
            MessageWriteCollection::open_dir(FILE_STORAGE_DIRECTORY, FILE_PREFIX, 32 * 1000)
                .unwrap();
        let writer = message_file.get_current_append(1).unwrap();
        assert_eq!(1, writer.message_file_info.file_id);
    }

    #[test]
    #[serial]
    pub fn existing_file_test() {
        cleanup();
        {
            let (file_info, r) =
                new_message_file(FILE_STORAGE_DIRECTORY, FILE_PREFIX, 1, 1, 32 * 1000).unwrap();
            r.write(&0, &1, &1, &[2; 50]).unwrap();
            r.flush().unwrap();

            let (file_info, r) =
                new_message_file(FILE_STORAGE_DIRECTORY, FILE_PREFIX, 2, 1, 32 * 1000).unwrap();
            r.write(&0, &1, &2, &[2; 50]).unwrap();
            r.flush().unwrap();
        }
        let mut message_file =
            MessageWriteCollection::open_dir(FILE_STORAGE_DIRECTORY, FILE_PREFIX, 32 * 1000)
                .unwrap();
        let writer = message_file.get_current_append(1).unwrap();
        assert_eq!(1, writer.message_file_info.file_id);
    }

    #[test]
    #[serial]
    pub fn open_file_test() {
        cleanup();
        let (file_info, r) =
            new_message_file(FILE_STORAGE_DIRECTORY, FILE_PREFIX, 1, 1, 32 * 1000).unwrap();
        r.write(&0, &1, &1, &[2; 50]).unwrap();
        r.flush().unwrap();
        let writer = MessageWriteAppend::open(Rc::new(file_info), 1, r).unwrap();
        match writer {
            OpenFileResult::Full => assert!(false),
            OpenFileResult::Opened(append) => assert!(append.next_pos > 0),
        }
    }

    #[test]
    #[serial]
    pub fn open_file_full_test() {
        cleanup();
        let buffer = [2; 31960];
        let (file_info, r) =
            new_message_file(FILE_STORAGE_DIRECTORY, FILE_PREFIX, 1, 1, 32 * 100).unwrap();
        let next_pos = r.write(&0, &1, &1, &[2; 3150]).unwrap();
        r.write(&next_pos, &1, &2, &[2; 30]).unwrap_or_default();
        r.flush().unwrap();
        let writer = MessageWriteAppend::open(Rc::new(file_info), 1, r).unwrap();
        match writer {
            OpenFileResult::Full => {}
            OpenFileResult::Opened(a) => {
                assert_eq!(0, a.next_pos);
            }
        }
    }
}
