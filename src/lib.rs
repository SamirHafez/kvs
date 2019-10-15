//! A crate for managing key-value stores

#![deny(missing_docs)]
#![feature(seek_convenience)]

use failure::Fail;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, Seek, Write};
use std::iter::FromIterator;
use std::path::{Path, PathBuf};

#[allow(clippy::identity_op)]
const LOG_FILE_SIZE: u64 = 1 * 1024 * 1024; // 1MB
const LOG_COMPACTION_COUNT: u32 = 10;

/// kvs Error structure
#[derive(Debug, Fail)]
pub enum KvError {
    /// An error that occurred due to IO issues
    IoError(io::Error),
    /// An error that occurred due to Serde issues
    SerdeError(serde_json::Error),
    /// Operational error
    KvError(String),
}

impl fmt::Display for KvError {
    fn fmt(&self, format: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KvError::IoError(err) => write!(format, "IO error: {}", err),
            KvError::SerdeError(err) => write!(format, "Serde error: {}", err),
            KvError::KvError(msg) => write!(format, "{}", msg),
        }
    }
}

impl From<io::Error> for KvError {
    fn from(error: io::Error) -> Self {
        KvError::IoError(error)
    }
}

impl From<serde_json::Error> for KvError {
    fn from(error: serde_json::Error) -> Self {
        KvError::SerdeError(error)
    }
}

impl From<std::num::ParseIntError> for KvError {
    fn from(_error: std::num::ParseIntError) -> Self {
        KvError::KvError("Error parsing Int".to_owned())
    }
}

/// Aliases standard Result to always have a KvError error component
pub type Result<T> = std::result::Result<T, KvError>;

/// Represents a key-value store
#[derive(Debug)]
pub struct KvStore {
    current_file_id: u32,
    path: PathBuf,
    file_ids: HashSet<u32>,
    cache: HashMap<String, KvLocation>,
}

#[derive(Debug, Serialize, Deserialize)]
enum KvCommand {
    Set(String, String),
    Get(String),
    Rm(String),
}

#[derive(Debug, Clone)]
struct KvLocation {
    file_id: u32,
    offset: u64,
}

impl KvStore {
    /// Opens a file containing a KvStore
    /// ```rust
    /// use kvs::KvStore;
    /// use std::path::Path;
    /// let path = Path::new("");
    /// match KvStore::open(&path) {
    ///   Ok(store) => println!("{:?}", store),
    ///   Err(err) => println!("{:?}", err)
    /// }
    /// ```
    pub fn open<A: AsRef<Path>>(path: A) -> Result<KvStore> {
        let path = path.as_ref().to_path_buf();
        let file_ids = load_file_ids(&path)?;
        let current_file_id = file_ids.last().unwrap_or(&0);

        let cache = file_ids
            .iter()
            .try_fold(HashMap::default(), |cache, log_id| {
                update_cache(cache, *log_id, &path)
            })?;

        Ok(KvStore {
            current_file_id: *current_file_id,
            file_ids: HashSet::from_iter(file_ids),
            path,
            cache,
        })
    }

    /// Sets the `value` for a given `key`.
    /// Will update if `key` already exists.
    /// ```rust
    /// use kvs::KvStore;
    /// use std::path::Path;
    /// let path = Path::new(".");
    /// let mut store = KvStore::open(&path).unwrap();
    /// store.set("Hello".to_owned(), "World".to_owned()).unwrap();
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let log_path = file_path(&self.path, self.current_file_id);
        let mut log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)?;

        let offset = log_file.stream_len()?;

        let command = KvCommand::Set(key.to_owned(), value.to_owned());
        let serialized = serde_json::to_string(&command)?;

        writeln!(log_file, "{}", serialized)?;

        let location = KvLocation {
            file_id: self.current_file_id,
            offset,
        };
        self.cache.insert(key, location);

        if log_path.metadata()?.len() > LOG_FILE_SIZE {
            self.file_ids.insert(self.current_file_id);

            if self.current_file_id % LOG_COMPACTION_COUNT == 0 {
                self.compact()?;
            }

            self.current_file_id += 1;
        }

        Ok(())
    }

    /// Gets the value, given the `key`.
    /// ```rust
    /// use kvs::KvStore;
    /// use std::path::Path;
    /// let path = Path::new(".");
    /// let mut store = KvStore::open(&path).unwrap();
    /// match store.get("Hello".to_owned()) {
    ///   Ok(opt) => println!("{:?}", opt.unwrap_or("nothing found.".to_string())),
    ///   Err(error) => println!("{:?}", error)
    /// }
    /// ```
    pub fn get(&self, key: String) -> Result<Option<String>> {
        match self.cache.get(&key) {
            Some(position) => {
                let log_file = file_path(&self.path, position.file_id);
                let mut buffered_reader = io::BufReader::new(File::open(log_file)?);

                buffered_reader.seek(io::SeekFrom::Start(position.offset))?;

                let mut line = String::default();
                buffered_reader.read_line(&mut line)?;

                let command = serde_json::from_str(&line.trim())?;

                match command {
                    KvCommand::Set(_key, value) => Ok(Some(value)),
                    _ => Err(KvError::KvError("Inconsistent backing storage".to_string())),
                }
            }
            None => Ok(None),
        }
    }

    /// Removes the value, given a `key`.
    /// Idempotent. Will do nothing if `key` is not present.
    /// ```rust
    /// use kvs::KvStore;
    /// use std::path::Path;
    /// let path = Path::new(".");
    /// let mut store = KvStore::open(&path).unwrap();
    /// match store.remove("Hello".to_owned()) {
    ///   Ok(opt) => println!("done."),
    ///   Err(error) => println!("{:?}", error)
    /// }
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.cache.remove(&key) {
            Some(_) => {
                let log_path = file_path(&self.path, self.current_file_id);
                let mut log_file = OpenOptions::new().append(true).open(log_path)?;

                let command = KvCommand::Rm(key.to_owned());
                let serialized = serde_json::to_string(&command)?;

                writeln!(log_file, "{}", serialized)?;

                Ok(())
            }
            None => Err(KvError::KvError("Key not found".to_owned())),
        }
    }

    fn compact(&mut self) -> Result<()> {
        let mut active_file_ids: HashSet<u32> = HashSet::default();

        for location in self.cache.values() {
            active_file_ids.insert(location.file_id);
        }

        for inactive_id in self
            .file_ids
            .difference(&active_file_ids)
            .cloned()
            .collect::<Vec<u32>>()
        {
            std::fs::remove_file(file_path(&self.path, inactive_id))?;
            self.file_ids.remove(&inactive_id);
        }

        Ok(())
    }
}

fn update_cache(
    mut cache: HashMap<String, KvLocation>,
    log_id: u32,
    path: &PathBuf,
) -> Result<HashMap<String, KvLocation>> {
    let log_path = file_path(&path, log_id);
    let log_file = File::open(&log_path)?;
    let mut buffered_reader = io::BufReader::new(log_file);

    let mut line = String::default();
    let mut offset = 0;

    while let Ok(read) = buffered_reader.read_line(&mut line) {
        if read == 0 {
            break;
        }

        let command = serde_json::from_str(&line.trim())?;
        match command {
            KvCommand::Set(key, _value) => {
                let location = KvLocation {
                    file_id: log_id,
                    offset,
                };
                cache.insert(key, location);
            }
            KvCommand::Rm(key) => {
                cache.remove(&key);
            }
            KvCommand::Get(_) => (),
        }
        offset = buffered_reader.stream_position()?;
        line = String::default();
    }

    Ok(cache)
}

fn load_file_ids(path: &PathBuf) -> Result<Vec<u32>> {
    let mut res = vec![];
    let mut log_files = std::fs::read_dir(path)?
        .map(|res| res.map(|entry| entry.path()).map_err(KvError::IoError))
        .collect::<Result<Vec<PathBuf>>>()?;

    log_files.sort();

    for log_path in log_files {
        if let Some(ext) = log_path.extension() {
            if ext == "log" {
                let log_path_id = log_path
                    .file_stem()
                    .ok_or_else(|| KvError::KvError("Failed to parse filename".to_owned()))?
                    .to_str()
                    .ok_or_else(|| KvError::KvError("Failed to parse filename".to_owned()))?
                    .parse()?;
                res.push(log_path_id);
            }
        }
    }

    Ok(res)
}

fn file_path(path: &PathBuf, file_id: u32) -> PathBuf {
    path.join(format!("{:09}.log", file_id))
}
