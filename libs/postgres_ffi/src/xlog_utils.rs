//
// This file contains common utilities for dealing with PostgreSQL WAL files and
// LSNs.
//
// Many of these functions have been copied from PostgreSQL, and rewritten in
// Rust. That's why they don't follow the usual Rust naming conventions, they
// have been named the same as the corresponding PostgreSQL functions instead.
//

use crc32c::crc32c_append;

use super::super::waldecoder::WalStreamDecoder;
use super::bindings::{
    CheckPoint, ControlFileData, DBState_DB_SHUTDOWNED, FullTransactionId, TimeLineID, TimestampTz,
    XLogLongPageHeaderData, XLogPageHeaderData, XLogRecPtr, XLogRecord, XLogSegNo, XLOG_PAGE_MAGIC,
};
use super::PG_MAJORVERSION;
use crate::pg_constants;
use crate::PG_TLI;
use crate::{uint32, uint64, Oid};
use crate::{WAL_SEGMENT_SIZE, XLOG_BLCKSZ};

use bytes::BytesMut;
use bytes::{Buf, Bytes};

use log::*;

use serde::Serialize;
use std::fs::File;
use std::io::prelude::*;
use std::io::ErrorKind;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use utils::bin_ser::DeserializeError;
use utils::bin_ser::SerializeError;

use utils::lsn::Lsn;

pub const XLOG_FNAME_LEN: usize = 24;
pub const XLP_FIRST_IS_CONTRECORD: u16 = 0x0001;
pub const XLP_REM_LEN_OFFS: usize = 2 + 2 + 4 + 8;
pub const XLOG_RECORD_CRC_OFFS: usize = 4 + 4 + 8 + 1 + 1 + 2;

pub const XLOG_SIZE_OF_XLOG_SHORT_PHD: usize = std::mem::size_of::<XLogPageHeaderData>();
pub const XLOG_SIZE_OF_XLOG_LONG_PHD: usize = std::mem::size_of::<XLogLongPageHeaderData>();
pub const XLOG_SIZE_OF_XLOG_RECORD: usize = std::mem::size_of::<XLogRecord>();
#[allow(clippy::identity_op)]
pub const SIZE_OF_XLOG_RECORD_DATA_HEADER_SHORT: usize = 1 * 2;

/// Interval of checkpointing metadata file. We should store metadata file to enforce
/// predicate that checkpoint.nextXid is larger than any XID in WAL.
/// But flushing checkpoint file for each transaction seems to be too expensive,
/// so XID_CHECKPOINT_INTERVAL is used to forward align nextXid and so perform
/// metadata checkpoint only once per XID_CHECKPOINT_INTERVAL transactions.
/// XID_CHECKPOINT_INTERVAL should not be larger than BLCKSZ*CLOG_XACTS_PER_BYTE
/// in order to let CLOG_TRUNCATE mechanism correctly extend CLOG.
const XID_CHECKPOINT_INTERVAL: u32 = 1024;

pub fn XLogSegmentsPerXLogId(wal_segsz_bytes: usize) -> XLogSegNo {
    (0x100000000u64 / wal_segsz_bytes as u64) as XLogSegNo
}

pub fn XLogSegNoOffsetToRecPtr(
    segno: XLogSegNo,
    offset: u32,
    wal_segsz_bytes: usize,
) -> XLogRecPtr {
    segno * (wal_segsz_bytes as u64) + (offset as u64)
}

pub fn XLogFileName(tli: TimeLineID, logSegNo: XLogSegNo, wal_segsz_bytes: usize) -> String {
    format!(
        "{:>08X}{:>08X}{:>08X}",
        tli,
        logSegNo / XLogSegmentsPerXLogId(wal_segsz_bytes),
        logSegNo % XLogSegmentsPerXLogId(wal_segsz_bytes)
    )
}

pub fn XLogFromFileName(fname: &str, wal_seg_size: usize) -> (XLogSegNo, TimeLineID) {
    let tli = u32::from_str_radix(&fname[0..8], 16).unwrap();
    let log = u32::from_str_radix(&fname[8..16], 16).unwrap() as XLogSegNo;
    let seg = u32::from_str_radix(&fname[16..24], 16).unwrap() as XLogSegNo;
    (log * XLogSegmentsPerXLogId(wal_seg_size) + seg, tli)
}

pub fn IsXLogFileName(fname: &str) -> bool {
    return fname.len() == XLOG_FNAME_LEN && fname.chars().all(|c| c.is_ascii_hexdigit());
}

pub fn IsPartialXLogFileName(fname: &str) -> bool {
    fname.ends_with(".partial") && IsXLogFileName(&fname[0..fname.len() - 8])
}

/// If LSN points to the beginning of the page, then shift it to first record,
/// otherwise align on 8-bytes boundary (required for WAL records)
pub fn normalize_lsn(lsn: Lsn, seg_sz: usize) -> Lsn {
    if lsn.0 % XLOG_BLCKSZ as u64 == 0 {
        let hdr_size = if lsn.0 % seg_sz as u64 == 0 {
            XLOG_SIZE_OF_XLOG_LONG_PHD
        } else {
            XLOG_SIZE_OF_XLOG_SHORT_PHD
        };
        lsn + hdr_size as u64
    } else {
        lsn.align()
    }
}

pub fn generate_pg_control(
    pg_control_bytes: &[u8],
    checkpoint_bytes: &[u8],
    lsn: Lsn,
) -> anyhow::Result<(Bytes, u64)> {
    let mut pg_control = ControlFileData::decode(pg_control_bytes)?;
    let mut checkpoint = CheckPoint::decode(checkpoint_bytes)?;

    // Generate new pg_control needed for bootstrap
    checkpoint.redo = normalize_lsn(lsn, WAL_SEGMENT_SIZE).0;

    //reset some fields we don't want to preserve
    //TODO Check this.
    //We may need to determine the value from twophase data.
    checkpoint.oldestActiveXid = 0;

    //save new values in pg_control
    pg_control.checkPoint = 0;
    pg_control.checkPointCopy = checkpoint;
    pg_control.state = DBState_DB_SHUTDOWNED;

    Ok((pg_control.encode(), pg_control.system_identifier))
}

pub fn get_current_timestamp() -> TimestampTz {
    to_pg_timestamp(SystemTime::now())
}

pub fn to_pg_timestamp(time: SystemTime) -> TimestampTz {
    const UNIX_EPOCH_JDATE: u64 = 2440588; /* == date2j(1970, 1, 1) */
    const POSTGRES_EPOCH_JDATE: u64 = 2451545; /* == date2j(2000, 1, 1) */
    const SECS_PER_DAY: u64 = 86400;
    const USECS_PER_SEC: u64 = 1000000;
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => {
            ((n.as_secs() - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY))
                * USECS_PER_SEC
                + n.subsec_micros() as u64) as i64
        }
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}

// Returns (aligned) end_lsn of the last record in data_dir with WAL segments.
// start_lsn must point to some previously known record boundary (beginning of
// the next record). If no valid record after is found, start_lsn is returned
// back.
pub fn find_end_of_wal(
    data_dir: &Path,
    wal_seg_size: usize,
    start_lsn: Lsn, // start reading WAL at this point; must point at record start_lsn.
) -> anyhow::Result<Lsn> {
    let mut result = start_lsn;
    let mut curr_lsn = start_lsn;
    let mut buf = [0u8; XLOG_BLCKSZ];
    let pg_version = PG_MAJORVERSION[1..3].parse::<u32>().unwrap();
    debug!("find_end_of_wal PG_VERSION: {}", pg_version);

    let mut decoder = WalStreamDecoder::new(start_lsn, pg_version);

    // loop over segments
    loop {
        let segno = curr_lsn.segment_number(wal_seg_size);
        let seg_file_name = XLogFileName(PG_TLI, segno, wal_seg_size);
        let seg_file_path = data_dir.join(seg_file_name);
        match open_wal_segment(&seg_file_path)? {
            None => {
                // no more segments
                debug!(
                    "find_end_of_wal reached end at {:?}, segment {:?} doesn't exist",
                    result, seg_file_path
                );
                return Ok(result);
            }
            Some(mut segment) => {
                let seg_offs = curr_lsn.segment_offset(wal_seg_size);
                segment.seek(SeekFrom::Start(seg_offs as u64))?;
                // loop inside segment
                loop {
                    let bytes_read = segment.read(&mut buf)?;
                    if bytes_read == 0 {
                        break; // EOF
                    }
                    curr_lsn += bytes_read as u64;
                    decoder.feed_bytes(&buf[0..bytes_read]);

                    // advance result past all completely read records
                    loop {
                        match decoder.poll_decode() {
                            Ok(Some(record)) => result = record.0,
                            Err(e) => {
                                debug!(
                                    "find_end_of_wal reached end at {:?}, decode error: {:?}",
                                    result, e
                                );
                                return Ok(result);
                            }
                            Ok(None) => break, // need more data
                        }
                    }
                }
            }
        }
    }
}

// Open .partial or full WAL segment file, if present.
fn open_wal_segment(seg_file_path: &Path) -> anyhow::Result<Option<File>> {
    let mut partial_path = seg_file_path.to_owned();
    partial_path.set_extension("partial");
    match File::open(partial_path) {
        Ok(file) => Ok(Some(file)),
        Err(e) => match e.kind() {
            ErrorKind::NotFound => {
                // .partial not found, try full
                match File::open(seg_file_path) {
                    Ok(file) => Ok(Some(file)),
                    Err(e) => match e.kind() {
                        ErrorKind::NotFound => Ok(None),
                        _ => Err(e.into()),
                    },
                }
            }
            _ => Err(e.into()),
        },
    }
}

pub fn main() {
    let mut data_dir = PathBuf::new();
    data_dir.push(".");
    let wal_end = find_end_of_wal(&data_dir, WAL_SEGMENT_SIZE, Lsn(0)).unwrap();
    println!("wal_end={:?}", wal_end);
}

impl XLogRecord {
    pub fn from_slice(buf: &[u8]) -> Result<XLogRecord, DeserializeError> {
        use utils::bin_ser::LeSer;
        XLogRecord::des(buf)
    }

    pub fn from_bytes<B: Buf>(buf: &mut B) -> Result<XLogRecord, DeserializeError> {
        use utils::bin_ser::LeSer;
        XLogRecord::des_from(&mut buf.reader())
    }

    pub fn encode(&self) -> Result<Bytes, SerializeError> {
        use utils::bin_ser::LeSer;
        Ok(self.ser()?.into())
    }

    // Is this record an XLOG_SWITCH record? They need some special processing,
    pub fn is_xlog_switch_record(&self) -> bool {
        self.xl_info == pg_constants::XLOG_SWITCH && self.xl_rmid == pg_constants::RM_XLOG_ID
    }
}

impl XLogPageHeaderData {
    pub fn from_bytes<B: Buf>(buf: &mut B) -> Result<XLogPageHeaderData, DeserializeError> {
        use utils::bin_ser::LeSer;
        XLogPageHeaderData::des_from(&mut buf.reader())
    }
}

impl XLogLongPageHeaderData {
    pub fn from_bytes<B: Buf>(buf: &mut B) -> Result<XLogLongPageHeaderData, DeserializeError> {
        use utils::bin_ser::LeSer;
        XLogLongPageHeaderData::des_from(&mut buf.reader())
    }

    pub fn encode(&self) -> Result<Bytes, SerializeError> {
        use utils::bin_ser::LeSer;
        self.ser().map(|b| b.into())
    }
}

pub const SIZEOF_CHECKPOINT: usize = std::mem::size_of::<CheckPoint>();

impl CheckPoint {
    pub fn encode(&self) -> Result<Bytes, SerializeError> {
        use utils::bin_ser::LeSer;
        Ok(self.ser()?.into())
    }

    pub fn decode(buf: &[u8]) -> Result<CheckPoint, DeserializeError> {
        use utils::bin_ser::LeSer;
        CheckPoint::des(buf)
    }

    /// Update next XID based on provided new_xid and stored epoch.
    /// Next XID should be greater than new_xid. This handles 32-bit
    /// XID wraparound correctly.
    ///
    /// Returns 'true' if the XID was updated.
    pub fn update_next_xid(&mut self, xid: u32) -> bool {
        // nextXid should nw greater than any XID in WAL, so increment provided XID and check for wraparround.
        let mut new_xid = std::cmp::max(xid + 1, pg_constants::FIRST_NORMAL_TRANSACTION_ID);
        // To reduce number of metadata checkpoints, we forward align XID on XID_CHECKPOINT_INTERVAL.
        // XID_CHECKPOINT_INTERVAL should not be larger than BLCKSZ*CLOG_XACTS_PER_BYTE
        new_xid =
            new_xid.wrapping_add(XID_CHECKPOINT_INTERVAL - 1) & !(XID_CHECKPOINT_INTERVAL - 1);
        let full_xid = self.nextXid.value;
        let old_xid = full_xid as u32;
        if new_xid.wrapping_sub(old_xid) as i32 > 0 {
            let mut epoch = full_xid >> 32;
            if new_xid < old_xid {
                // wrap-around
                epoch += 1;
            }
            let nextXid = (epoch << 32) | new_xid as u64;

            if nextXid != self.nextXid.value {
                self.nextXid = FullTransactionId { value: nextXid };
                return true;
            }
        }
        false
    }
}

//
// Generate new, empty WAL segment.
// We need this segment to start compute node.
//
pub fn generate_wal_segment(segno: u64, system_id: u64) -> Result<Bytes, SerializeError> {
    let mut seg_buf = BytesMut::with_capacity(WAL_SEGMENT_SIZE);

    let pageaddr = XLogSegNoOffsetToRecPtr(segno, 0, WAL_SEGMENT_SIZE);
    let hdr = XLogLongPageHeaderData {
        std: {
            XLogPageHeaderData {
                xlp_magic: XLOG_PAGE_MAGIC as u16,
                xlp_info: pg_constants::XLP_LONG_HEADER,
                xlp_tli: PG_TLI,
                xlp_pageaddr: pageaddr,
                xlp_rem_len: 0,
                ..Default::default() // Put 0 in padding fields.
            }
        },
        xlp_sysid: system_id,
        xlp_seg_size: WAL_SEGMENT_SIZE as u32,
        xlp_xlog_blcksz: XLOG_BLCKSZ as u32,
    };

    let hdr_bytes = hdr.encode()?;
    seg_buf.extend_from_slice(&hdr_bytes);

    //zero out the rest of the file
    seg_buf.resize(WAL_SEGMENT_SIZE, 0);
    Ok(seg_buf.freeze())
}

#[repr(C)]
#[derive(Serialize)]
struct XlLogicalMessage {
    db_id: Oid,
    transactional: uint32, // bool, takes 4 bytes due to alignment in C structures
    prefix_size: uint64,
    message_size: uint64,
}

impl XlLogicalMessage {
    pub fn encode(&self) -> Bytes {
        use utils::bin_ser::LeSer;
        self.ser().unwrap().into()
    }
}

/// Create new WAL record for non-transactional logical message.
/// Used for creating artificial WAL for tests, as LogicalMessage
/// record is basically no-op.
///
/// NOTE: This leaves the xl_prev field zero. The safekeeper and
/// pageserver tolerate that, but PostgreSQL does not.
pub fn encode_logical_message(prefix: &str, message: &str) -> Vec<u8> {
    let mut prefix_bytes: Vec<u8> = Vec::with_capacity(prefix.len() + 1);
    prefix_bytes.write_all(prefix.as_bytes()).unwrap();
    prefix_bytes.push(0);

    let message_bytes = message.as_bytes();

    let logical_message = XlLogicalMessage {
        db_id: 0,
        transactional: 0,
        prefix_size: prefix_bytes.len() as u64,
        message_size: message_bytes.len() as u64,
    };

    let mainrdata = logical_message.encode();
    let mainrdata_len: usize = mainrdata.len() + prefix_bytes.len() + message_bytes.len();
    // only short mainrdata is supported for now
    assert!(mainrdata_len <= 255);
    let mainrdata_len = mainrdata_len as u8;

    let mut data: Vec<u8> = vec![pg_constants::XLR_BLOCK_ID_DATA_SHORT, mainrdata_len];
    data.extend_from_slice(&mainrdata);
    data.extend_from_slice(&prefix_bytes);
    data.extend_from_slice(message_bytes);

    let total_len = XLOG_SIZE_OF_XLOG_RECORD + data.len();

    let mut header = XLogRecord {
        xl_tot_len: total_len as u32,
        xl_xid: 0,
        xl_prev: 0,
        xl_info: 0,
        xl_rmid: 21,
        __bindgen_padding_0: [0u8; 2usize],
        xl_crc: 0, // crc will be calculated later
    };

    let header_bytes = header.encode().expect("failed to encode header");
    let crc = crc32c_append(0, &data);
    let crc = crc32c_append(crc, &header_bytes[0..XLOG_RECORD_CRC_OFFS]);
    header.xl_crc = crc;

    let mut wal: Vec<u8> = Vec::new();
    wal.extend_from_slice(&header.encode().expect("failed to encode header"));
    wal.extend_from_slice(&data);

    // WAL start position must be aligned at 8 bytes,
    // this will add padding for the next WAL record.
    const PADDING: usize = 8;
    let padding_rem = wal.len() % PADDING;
    if padding_rem != 0 {
        wal.resize(wal.len() + PADDING - padding_rem, 0);
    }

    wal
}

#[cfg(test)]
mod tests {
    use super::super::PG_MAJORVERSION;
    use super::*;
    use regex::Regex;
    use std::cmp::min;
    use std::fs;
    use std::{env, str::FromStr};
    use utils::const_assert;

    fn init_logging() {
        let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(
            format!("wal_craft=info,postgres_ffi::{PG_MAJORVERSION}::xlog_utils=trace"),
        ))
        .is_test(true)
        .try_init();
    }

    fn test_end_of_wal<C: wal_craft::Crafter>(test_name: &str) {
        use wal_craft::*;

        let pg_version = PG_MAJORVERSION[1..3].parse::<u32>().unwrap();

        // Craft some WAL
        let top_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..");
        let cfg = Conf {
            pg_version,
            pg_distrib_dir: top_path.join("pg_install"),
            datadir: top_path.join(format!("test_output/{}-{PG_MAJORVERSION}", test_name)),
        };
        if cfg.datadir.exists() {
            fs::remove_dir_all(&cfg.datadir).unwrap();
        }
        cfg.initdb().unwrap();
        let srv = cfg.start_server().unwrap();
        let (intermediate_lsns, expected_end_of_wal_partial) =
            C::craft(&mut srv.connect_with_timeout().unwrap()).unwrap();
        let intermediate_lsns: Vec<Lsn> = intermediate_lsns
            .iter()
            .map(|&lsn| u64::from(lsn).into())
            .collect();
        let expected_end_of_wal: Lsn = u64::from(expected_end_of_wal_partial).into();
        srv.kill();

        // Check find_end_of_wal on the initial WAL
        let last_segment = cfg
            .wal_dir()
            .read_dir()
            .unwrap()
            .map(|f| f.unwrap().file_name().into_string().unwrap())
            .filter(|fname| IsXLogFileName(fname))
            .max()
            .unwrap();
        check_pg_waldump_end_of_wal(&cfg, &last_segment, expected_end_of_wal);
        for start_lsn in intermediate_lsns
            .iter()
            .chain(std::iter::once(&expected_end_of_wal))
        {
            // Erase all WAL before `start_lsn` to ensure it's not used by `find_end_of_wal`.
            // We assume that `start_lsn` is non-decreasing.
            info!(
                "Checking with start_lsn={}, erasing WAL before it",
                start_lsn
            );
            for file in fs::read_dir(cfg.wal_dir()).unwrap().flatten() {
                let fname = file.file_name().into_string().unwrap();
                if !IsXLogFileName(&fname) {
                    continue;
                }
                let (segno, _) = XLogFromFileName(&fname, WAL_SEGMENT_SIZE);
                let seg_start_lsn = XLogSegNoOffsetToRecPtr(segno, 0, WAL_SEGMENT_SIZE);
                if seg_start_lsn > u64::from(*start_lsn) {
                    continue;
                }
                let mut f = File::options().write(true).open(file.path()).unwrap();
                const ZEROS: [u8; WAL_SEGMENT_SIZE] = [0u8; WAL_SEGMENT_SIZE];
                f.write_all(
                    &ZEROS[0..min(
                        WAL_SEGMENT_SIZE,
                        (u64::from(*start_lsn) - seg_start_lsn) as usize,
                    )],
                )
                .unwrap();
            }
            check_end_of_wal(&cfg, &last_segment, *start_lsn, expected_end_of_wal);
        }
    }

    fn check_pg_waldump_end_of_wal(
        cfg: &wal_craft::Conf,
        last_segment: &str,
        expected_end_of_wal: Lsn,
    ) {
        // Get the actual end of WAL by pg_waldump
        let waldump_output = cfg
            .pg_waldump("000000010000000000000001", last_segment)
            .unwrap()
            .stderr;
        let waldump_output = std::str::from_utf8(&waldump_output).unwrap();
        let caps = match Regex::new(r"invalid record length at (.+):")
            .unwrap()
            .captures(waldump_output)
        {
            Some(caps) => caps,
            None => {
                error!("Unable to parse pg_waldump's stderr:\n{}", waldump_output);
                panic!();
            }
        };
        let waldump_wal_end = Lsn::from_str(caps.get(1).unwrap().as_str()).unwrap();
        info!(
            "waldump erred on {}, expected wal end at {}",
            waldump_wal_end, expected_end_of_wal
        );
        assert_eq!(waldump_wal_end, expected_end_of_wal);
    }

    fn check_end_of_wal(
        cfg: &wal_craft::Conf,
        last_segment: &str,
        start_lsn: Lsn,
        expected_end_of_wal: Lsn,
    ) {
        // Check end_of_wal on non-partial WAL segment (we treat it as fully populated)
        // let wal_end = find_end_of_wal(&cfg.wal_dir(), WAL_SEGMENT_SIZE, start_lsn).unwrap();
        // info!(
        //     "find_end_of_wal returned wal_end={} with non-partial WAL segment",
        //     wal_end
        // );
        // assert_eq!(wal_end, expected_end_of_wal_non_partial);

        // Rename file to partial to actually find last valid lsn, then rename it back.
        fs::rename(
            cfg.wal_dir().join(last_segment),
            cfg.wal_dir().join(format!("{}.partial", last_segment)),
        )
        .unwrap();
        let wal_end = find_end_of_wal(&cfg.wal_dir(), WAL_SEGMENT_SIZE, start_lsn).unwrap();
        info!(
            "find_end_of_wal returned wal_end={} with partial WAL segment",
            wal_end
        );
        assert_eq!(wal_end, expected_end_of_wal);
        fs::rename(
            cfg.wal_dir().join(format!("{}.partial", last_segment)),
            cfg.wal_dir().join(last_segment),
        )
        .unwrap();
    }

    const_assert!(WAL_SEGMENT_SIZE == 16 * 1024 * 1024);

    #[test]
    pub fn test_find_end_of_wal_simple() {
        init_logging();
        test_end_of_wal::<wal_craft::Simple>("test_find_end_of_wal_simple");
    }

    #[test]
    pub fn test_find_end_of_wal_crossing_segment_followed_by_small_one() {
        init_logging();
        test_end_of_wal::<wal_craft::WalRecordCrossingSegmentFollowedBySmallOne>(
            "test_find_end_of_wal_crossing_segment_followed_by_small_one",
        );
    }

    #[test]
    pub fn test_find_end_of_wal_last_crossing_segment() {
        init_logging();
        test_end_of_wal::<wal_craft::LastWalRecordCrossingSegment>(
            "test_find_end_of_wal_last_crossing_segment",
        );
    }

    /// Check the math in update_next_xid
    ///
    /// NOTE: These checks are sensitive to the value of XID_CHECKPOINT_INTERVAL,
    /// currently 1024.
    #[test]
    pub fn test_update_next_xid() {
        let checkpoint_buf = [0u8; std::mem::size_of::<CheckPoint>()];
        let mut checkpoint = CheckPoint::decode(&checkpoint_buf).unwrap();

        checkpoint.nextXid = FullTransactionId { value: 10 };
        assert_eq!(checkpoint.nextXid.value, 10);

        // The input XID gets rounded up to the next XID_CHECKPOINT_INTERVAL
        // boundary
        checkpoint.update_next_xid(100);
        assert_eq!(checkpoint.nextXid.value, 1024);

        // No change
        checkpoint.update_next_xid(500);
        assert_eq!(checkpoint.nextXid.value, 1024);
        checkpoint.update_next_xid(1023);
        assert_eq!(checkpoint.nextXid.value, 1024);

        // The function returns the *next* XID, given the highest XID seen so
        // far. So when we pass 1024, the nextXid gets bumped up to the next
        // XID_CHECKPOINT_INTERVAL boundary.
        checkpoint.update_next_xid(1024);
        assert_eq!(checkpoint.nextXid.value, 2048);
    }

    #[test]
    pub fn test_encode_logical_message() {
        let expected = [
            64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 21, 0, 0, 170, 34, 166, 227, 255,
            38, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 112, 114,
            101, 102, 105, 120, 0, 109, 101, 115, 115, 97, 103, 101,
        ];
        let actual = encode_logical_message("prefix", "message");
        assert_eq!(expected, actual[..]);
    }
}
