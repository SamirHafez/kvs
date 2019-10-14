//! A crate for managing key-value stores

#![deny(missing_docs)]
#![feature(seek_convenience)]

use failure::Fail;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, Seek, Write};
use std::path::{Path, PathBuf};

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

/// Aliases standard Result to always have a KvError error component
pub type Result<T> = std::result::Result<T, KvError>;

/// Represents a key-value store
pub struct KvStore {
    path: PathBuf,
    cache: HashMap<String, u64>,
}

#[derive(Debug, Serialize, Deserialize)]
enum KvCommand {
    Set(String, String),
    Get(String),
    Rm(String),
}

impl KvStore {
    /// Opens a file containing a KvStore
    /// ```rust
    /// use kvs::KvStore;
    /// use std::path::Path;
    /// let path = Path::new("/tmp/p.ath");
    /// match KvStore::open(&path) {
    ///   Ok(store) => println!("{:?}", store),
    ///   Err(err) => println!("{:?}", err)
    /// }
    /// ```
    pub fn open<A: AsRef<Path>>(path: A) -> Result<KvStore> {
        let mut store = KvStore {
            path: path.as_ref().to_path_buf(),
            cache: HashMap::default(),
        };

        let log_path = store.path.join("log.file");
        if log_path.exists() {
            let log_file = File::open(log_path)?;
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
                        store.cache.insert(key, offset);
                    }
                    KvCommand::Rm(key) => {
                        store.cache.remove(&key);
                    }
                    KvCommand::Get(_) => (),
                }
                offset = buffered_reader.stream_position()?;
                line = String::default();
            }
        }

        Ok(store)
    }

    /// Sets the `value` for a given `key`.
    /// Will update if `key` already exists.
    /// ```rust
    /// use kvs::KvStore;
    /// use std::path::Path;
    /// let path = Path::new("/tmp/p.ath");
    /// let mut store = KvStore::open(&path).unwrap();
    /// store.set("Hello".to_owned(), "World".to_owned()).unwrap();
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let log_path = self.path.join("log.file");
        let mut log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)?;

        let position = log_file.stream_len()?;

        let command = KvCommand::Set(key.to_owned(), value.to_owned());
        let serialized = serde_json::to_string(&command)?;

        writeln!(log_file, "{}", serialized)?;
        self.cache.insert(key, position);

        Ok(())
    }

    /// Gets the value, given the `key`.
    /// ```rust
    /// use kvs::KvStore;
    /// use std::path::Path;
    /// let path = Path::new("/tmp/p.ath");
    /// let mut store = KvStore::open(&path).unwrap();
    /// match store.get("Hello".to_owned()) {
    ///   Ok(opt) => println!("{:?}", opt.unwrap_or("nothing found.")),
    ///   Err(error) => println!("{:?}", error)
    /// }
    /// ```
    pub fn get(&self, key: String) -> Result<Option<String>> {
        match self.cache.get(&key) {
            Some(position) => self.read_from(*position),
            None => Ok(None),
        }
    }

    /// Removes the value, given a `key`.
    /// Idempotent. Will do nothing if `key` is not present.
    /// ```rust
    /// use kvs::KvStore;
    /// use std::path::Path;
    /// let path = Path::new("/tmp/p.ath");
    /// let mut store = KvStore::open(&path).unwrap();
    /// store.remove("Hello".to_owned()).unwrap();
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.cache.remove(&key) {
            Some(_) => {
                let log_path = self.path.join("log.file");
                let mut log_file = OpenOptions::new().append(true).open(log_path)?;

                let command = KvCommand::Rm(key.to_owned());
                let serialized = serde_json::to_string(&command)?;

                writeln!(log_file, "{}", serialized)?;

                Ok(())
            }
            None => Err(KvError::KvError("Key not found".to_owned())),
        }
    }

    fn read_from(&self, position: u64) -> Result<Option<String>> {
        let log_path = self.path.join("log.file");
        let log_file = File::open(log_path)?;
        let mut buffered_reader = io::BufReader::new(log_file);

        buffered_reader.seek(io::SeekFrom::Start(position))?;

        let mut line = String::default();
        buffered_reader.read_line(&mut line)?;

        let command = serde_json::from_str::<KvCommand>(&line.trim())?;

        match command {
            KvCommand::Set(_key, value) => Ok(Some(value)),
            _ => Ok(None),
        }
    }
}
