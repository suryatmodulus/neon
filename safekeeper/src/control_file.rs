//! Control file serialization, deserialization and persistence.

use anyhow::{bail, ensure, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};

use crate::control_file_upgrade::upgrade_control_file;
use crate::metrics::PERSIST_CONTROL_FILE_SECONDS;
use crate::safekeeper::{SafeKeeperState, SK_FORMAT_VERSION, SK_MAGIC};
use utils::{bin_ser::LeSer, id::TenantTimelineId};

use crate::SafeKeeperConf;

use std::convert::TryInto;

// contains persistent metadata for safekeeper
const CONTROL_FILE_NAME: &str = "safekeeper.control";
// needed to atomically update the state using `rename`
const CONTROL_FILE_NAME_PARTIAL: &str = "safekeeper.control.partial";
pub const CHECKSUM_SIZE: usize = std::mem::size_of::<u32>();

/// Storage should keep actual state inside of it. It should implement Deref
/// trait to access state fields and have persist method for updating that state.
pub trait Storage: Deref<Target = SafeKeeperState> {
    /// Persist safekeeper state on disk and update internal state.
    fn persist(&mut self, s: &SafeKeeperState) -> Result<()>;
}

#[derive(Debug)]
pub struct FileStorage {
    // save timeline dir to avoid reconstructing it every time
    timeline_dir: PathBuf,
    conf: SafeKeeperConf,

    /// Last state persisted to disk.
    state: SafeKeeperState,
}

impl FileStorage {
    /// Initialize storage by loading state from disk.
    pub fn restore_new(ttid: &TenantTimelineId, conf: &SafeKeeperConf) -> Result<FileStorage> {
        let timeline_dir = conf.timeline_dir(ttid);

        let state = Self::load_control_file_conf(conf, ttid)?;

        Ok(FileStorage {
            timeline_dir,
            conf: conf.clone(),
            state,
        })
    }

    /// Create file storage for a new timeline, but don't persist it yet.
    pub fn create_new(
        ttid: &TenantTimelineId,
        conf: &SafeKeeperConf,
        state: SafeKeeperState,
    ) -> Result<FileStorage> {
        let timeline_dir = conf.timeline_dir(ttid);

        let store = FileStorage {
            timeline_dir,
            conf: conf.clone(),
            state,
        };

        Ok(store)
    }

    /// Check the magic/version in the on-disk data and deserialize it, if possible.
    fn deser_sk_state(buf: &mut &[u8]) -> Result<SafeKeeperState> {
        // Read the version independent part
        let magic = buf.read_u32::<LittleEndian>()?;
        if magic != SK_MAGIC {
            bail!(
                "bad control file magic: {:X}, expected {:X}",
                magic,
                SK_MAGIC
            );
        }
        let version = buf.read_u32::<LittleEndian>()?;
        if version == SK_FORMAT_VERSION {
            let res = SafeKeeperState::des(buf)?;
            return Ok(res);
        }
        // try to upgrade
        upgrade_control_file(buf, version)
    }

    /// Load control file for given ttid at path specified by conf.
    pub fn load_control_file_conf(
        conf: &SafeKeeperConf,
        ttid: &TenantTimelineId,
    ) -> Result<SafeKeeperState> {
        let path = conf.timeline_dir(ttid).join(CONTROL_FILE_NAME);
        Self::load_control_file(path)
    }

    /// Read in the control file.
    pub fn load_control_file<P: AsRef<Path>>(control_file_path: P) -> Result<SafeKeeperState> {
        let mut control_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&control_file_path)
            .with_context(|| {
                format!(
                    "failed to open control file at {}",
                    control_file_path.as_ref().display(),
                )
            })?;

        let mut buf = Vec::new();
        control_file
            .read_to_end(&mut buf)
            .context("failed to read control file")?;

        let calculated_checksum = crc32c::crc32c(&buf[..buf.len() - CHECKSUM_SIZE]);

        let expected_checksum_bytes: &[u8; CHECKSUM_SIZE] =
            buf[buf.len() - CHECKSUM_SIZE..].try_into()?;
        let expected_checksum = u32::from_le_bytes(*expected_checksum_bytes);

        ensure!(
            calculated_checksum == expected_checksum,
            format!(
                "safekeeper control file checksum mismatch: expected {} got {}",
                expected_checksum, calculated_checksum
            )
        );

        let state = FileStorage::deser_sk_state(&mut &buf[..buf.len() - CHECKSUM_SIZE])
            .with_context(|| {
                format!(
                    "while reading control file {}",
                    control_file_path.as_ref().display(),
                )
            })?;
        Ok(state)
    }
}

impl Deref for FileStorage {
    type Target = SafeKeeperState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl Storage for FileStorage {
    /// persists state durably to underlying storage
    /// for description see https://lwn.net/Articles/457667/
    fn persist(&mut self, s: &SafeKeeperState) -> Result<()> {
        let _timer = PERSIST_CONTROL_FILE_SECONDS.start_timer();

        // write data to safekeeper.control.partial
        let control_partial_path = self.timeline_dir.join(CONTROL_FILE_NAME_PARTIAL);
        let mut control_partial = File::create(&control_partial_path).with_context(|| {
            format!(
                "failed to create partial control file at: {}",
                &control_partial_path.display()
            )
        })?;
        let mut buf: Vec<u8> = Vec::new();
        buf.write_u32::<LittleEndian>(SK_MAGIC)?;
        buf.write_u32::<LittleEndian>(SK_FORMAT_VERSION)?;
        s.ser_into(&mut buf)?;

        // calculate checksum before resize
        let checksum = crc32c::crc32c(&buf);
        buf.extend_from_slice(&checksum.to_le_bytes());

        control_partial.write_all(&buf).with_context(|| {
            format!(
                "failed to write safekeeper state into control file at: {}",
                control_partial_path.display()
            )
        })?;

        // fsync the file
        if !self.conf.no_sync {
            control_partial.sync_all().with_context(|| {
                format!(
                    "failed to sync partial control file at {}",
                    control_partial_path.display()
                )
            })?;
        }

        let control_path = self.timeline_dir.join(CONTROL_FILE_NAME);

        // rename should be atomic
        fs::rename(&control_partial_path, &control_path)?;
        // this sync is not required by any standard but postgres does this (see durable_rename)
        if !self.conf.no_sync {
            File::open(&control_path)
                .and_then(|f| f.sync_all())
                .with_context(|| {
                    format!(
                        "failed to sync control file at: {}",
                        &control_path.display()
                    )
                })?;

            // fsync the directory (linux specific)
            File::open(&self.timeline_dir)
                .and_then(|f| f.sync_all())
                .context("failed to sync control file directory")?;
        }

        // update internal state
        self.state = s.clone();
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::FileStorage;
    use super::*;
    use crate::{safekeeper::SafeKeeperState, SafeKeeperConf};
    use anyhow::Result;
    use std::fs;
    use utils::{id::TenantTimelineId, lsn::Lsn};

    fn stub_conf() -> SafeKeeperConf {
        let workdir = tempfile::tempdir().unwrap().into_path();
        SafeKeeperConf {
            workdir,
            ..SafeKeeperConf::dummy()
        }
    }

    fn load_from_control_file(
        conf: &SafeKeeperConf,
        ttid: &TenantTimelineId,
    ) -> Result<(FileStorage, SafeKeeperState)> {
        fs::create_dir_all(conf.timeline_dir(ttid)).expect("failed to create timeline dir");
        Ok((
            FileStorage::restore_new(ttid, conf)?,
            FileStorage::load_control_file_conf(conf, ttid)?,
        ))
    }

    fn create(
        conf: &SafeKeeperConf,
        ttid: &TenantTimelineId,
    ) -> Result<(FileStorage, SafeKeeperState)> {
        fs::create_dir_all(conf.timeline_dir(ttid)).expect("failed to create timeline dir");
        let state = SafeKeeperState::empty();
        let storage = FileStorage::create_new(ttid, conf, state.clone())?;
        Ok((storage, state))
    }

    #[test]
    fn test_read_write_safekeeper_state() {
        let conf = stub_conf();
        let ttid = TenantTimelineId::generate();
        {
            let (mut storage, mut state) = create(&conf, &ttid).expect("failed to create state");
            // change something
            state.commit_lsn = Lsn(42);
            storage.persist(&state).expect("failed to persist state");
        }

        let (_, state) = load_from_control_file(&conf, &ttid).expect("failed to read state");
        assert_eq!(state.commit_lsn, Lsn(42));
    }

    #[test]
    fn test_safekeeper_state_checksum_mismatch() {
        let conf = stub_conf();
        let ttid = TenantTimelineId::generate();
        {
            let (mut storage, mut state) = create(&conf, &ttid).expect("failed to read state");

            // change something
            state.commit_lsn = Lsn(42);
            storage.persist(&state).expect("failed to persist state");
        }
        let control_path = conf.timeline_dir(&ttid).join(CONTROL_FILE_NAME);
        let mut data = fs::read(&control_path).unwrap();
        data[0] += 1; // change the first byte of the file to fail checksum validation
        fs::write(&control_path, &data).expect("failed to write control file");

        match load_from_control_file(&conf, &ttid) {
            Err(err) => assert!(err
                .to_string()
                .contains("safekeeper control file checksum mismatch")),
            Ok(_) => panic!("expected error"),
        }
    }
}
