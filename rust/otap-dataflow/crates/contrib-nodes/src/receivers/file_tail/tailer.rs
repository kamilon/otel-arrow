// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Per-file tailing logic with growable, per-file buffers.
//!
//! Each [`FileTailer`] owns a `tokio::fs::File`, a read buffer that starts at
//! the configured size and grows (but never shrinks) up to `max_buffer_size`,
//! and a [`DelimiterScanner`] for extracting individual records.

use crate::receivers::file_tail::config::StartPosition;
use crate::receivers::file_tail::delimiter::{DelimiterScanner, ScanResult};
use crate::receivers::file_tail::encoding::Transcoder;
use crate::receivers::file_tail::rotation::{FileIdentity, RotationOutcome, check_rotation};
use std::borrow::Cow;
use std::io;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

/// A handle that tails a single file, maintaining its own read buffer.
pub struct FileTailer {
    /// Canonical path of the file being tailed.
    path: PathBuf,
    /// Open async file handle.
    file: File,
    /// Read buffer — starts at `initial_buf_size`, grows up to `max_buf_size`.
    buf: Vec<u8>,
    /// Number of valid (unprocessed) bytes currently in `buf`.
    buf_len: usize,
    /// Maximum size the buffer is allowed to grow to.
    max_buf_size: usize,
    /// Current file offset (byte position of next read).
    offset: u64,
    /// Identity of the file when it was opened (for rotation detection).
    identity: FileIdentity,
    /// Whether the buffer has ever been grown.
    regrown: bool,
}

impl FileTailer {
    /// Open a file for tailing.
    ///
    /// Seeks to `offset` if restoring from a bookmark, or to the end of the
    /// file if `start_position` is [`StartPosition::End`] and no bookmark is
    /// available (pass `offset = None`).
    pub async fn open(
        path: &Path,
        start_position: &StartPosition,
        bookmark_offset: Option<u64>,
        initial_buf_size: usize,
        max_buf_size: usize,
    ) -> io::Result<Self> {
        let identity = FileIdentity::from_path(path)?;
        let mut file = File::open(path).await?;

        let offset = match bookmark_offset {
            Some(off) => {
                let _ = file.seek(io::SeekFrom::Start(off)).await?;
                off
            }
            None => match start_position {
                StartPosition::Beginning => 0,
                StartPosition::End => file.seek(io::SeekFrom::End(0)).await?,
            },
        };

        Ok(Self {
            path: path.to_path_buf(),
            file,
            buf: vec![0u8; initial_buf_size],
            buf_len: 0,
            max_buf_size,
            offset,
            identity,
            regrown: false,
        })
    }

    /// The canonical path of the tailed file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The current read offset.
    #[must_use]
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// The file identity obtained when the file was opened.
    #[must_use]
    pub fn identity(&self) -> &FileIdentity {
        &self.identity
    }

    /// Read new data from the file and extract all complete records.
    ///
    /// Returns a (possibly empty) list of records as byte slices transcoded
    /// via `transcoder`. Also returns the total number of bytes read from the
    /// file in this call.
    pub async fn read_records(
        &mut self,
        scanner: &mut DelimiterScanner,
        transcoder: &Transcoder,
    ) -> io::Result<(Vec<Vec<u8>>, u64)> {
        // Read new data into the tail of the buffer.
        let n = self.file.read(&mut self.buf[self.buf_len..]).await?;
        if n == 0 {
            return Ok((Vec::new(), 0));
        }
        self.buf_len += n;
        let bytes_read = n as u64;

        let mut records: Vec<Vec<u8>> = Vec::new();
        let mut scan_start = 0;

        loop {
            match scanner.scan(&self.buf[..self.buf_len], scan_start) {
                ScanResult::Complete {
                    end,
                    payload_start,
                    payload_end,
                } => {
                    let raw = &self.buf[payload_start..payload_end];
                    let transcoded: Cow<'_, [u8]> = transcoder.transcode(raw);
                    records.push(transcoded.into_owned());
                    scan_start = end;
                }
                ScanResult::Incomplete => {
                    break;
                }
            }
        }

        // Advance the offset past the consumed bytes.
        let consumed = scan_start;
        self.offset += consumed as u64;

        // Compact the buffer: move unconsumed bytes to the front.
        if consumed > 0 {
            self.buf.copy_within(consumed..self.buf_len, 0);
            self.buf_len -= consumed;
        }

        // Grow the buffer if it is full and a record was not found (the
        // current record is longer than the buffer).
        if self.buf_len == self.buf.len() && records.is_empty() {
            let new_size = (self.buf.len() * 2).min(self.max_buf_size);
            if new_size > self.buf.len() {
                self.buf.resize(new_size, 0);
                self.regrown = true;
            }
            // If the buffer is already at max size and still no record,
            // the next call will try again with the same buffer contents.
        }

        Ok((records, bytes_read))
    }

    /// Check whether the underlying file has been rotated.
    pub fn check_rotation(&self) -> RotationOutcome {
        check_rotation(&self.path, &self.identity)
    }

    /// Drain remaining data from the current file handle to EOF and return
    /// any complete records.
    pub async fn drain(
        &mut self,
        scanner: &mut DelimiterScanner,
        transcoder: &Transcoder,
    ) -> io::Result<Vec<Vec<u8>>> {
        let mut all_records = Vec::new();
        loop {
            let (records, bytes_read) = self.read_records(scanner, transcoder).await?;
            all_records.extend(records);
            if bytes_read == 0 {
                break;
            }
        }
        Ok(all_records)
    }

    /// Reopen the file at the same path from offset 0 after a rotation.
    pub async fn reopen(&mut self) -> io::Result<()> {
        self.file = File::open(&self.path).await?;
        self.identity = FileIdentity::from_path(&self.path)?;
        self.offset = 0;
        self.buf_len = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::receivers::file_tail::config::Delimiter;

    #[tokio::test]
    async fn test_basic_tail() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("test.log");
        std::fs::write(&path, b"line one\nline two\n").expect("write");

        let mut tailer = FileTailer::open(&path, &StartPosition::Beginning, None, 1024, 4096)
            .await
            .expect("open");
        let mut scanner = DelimiterScanner::new(&Delimiter::Newline);
        let transcoder = Transcoder::new(None, None).expect("transcoder");

        let (records, bytes) = tailer
            .read_records(&mut scanner, &transcoder)
            .await
            .expect("read");
        assert!(bytes > 0);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0], b"line one");
        assert_eq!(records[1], b"line two");
    }

    #[tokio::test]
    async fn test_start_from_end() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("test.log");
        std::fs::write(&path, b"old data\n").expect("write");

        let mut tailer = FileTailer::open(&path, &StartPosition::End, None, 1024, 4096)
            .await
            .expect("open");
        let mut scanner = DelimiterScanner::new(&Delimiter::Newline);
        let transcoder = Transcoder::new(None, None).expect("transcoder");

        let (records, _) = tailer
            .read_records(&mut scanner, &transcoder)
            .await
            .expect("read");
        assert!(records.is_empty(), "should not read pre-existing data");
    }

    #[tokio::test]
    async fn test_buffer_growth() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("test.log");

        // Write a line longer than the initial buffer
        let mut long_line = vec![b'A'; 128];
        long_line.push(b'\n');
        std::fs::write(&path, &long_line).expect("write");

        let mut tailer = FileTailer::open(&path, &StartPosition::Beginning, None, 64, 512)
            .await
            .expect("open");
        let mut scanner = DelimiterScanner::new(&Delimiter::Newline);
        let transcoder = Transcoder::new(None, None).expect("transcoder");

        // First read fills the 64-byte buffer without finding a newline,
        // triggering growth.
        let (records, _) = tailer
            .read_records(&mut scanner, &transcoder)
            .await
            .expect("read1");
        assert!(records.is_empty()); // buffer grew but record not yet complete

        // Second read should find the complete record.
        let (records, _) = tailer
            .read_records(&mut scanner, &transcoder)
            .await
            .expect("read2");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].len(), 128);
    }

    #[tokio::test]
    async fn test_bookmark_offset() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("test.log");
        std::fs::write(&path, b"line one\nline two\n").expect("write");

        // Start from after "line one\n" (9 bytes)
        let mut tailer = FileTailer::open(&path, &StartPosition::Beginning, Some(9), 1024, 4096)
            .await
            .expect("open");
        let mut scanner = DelimiterScanner::new(&Delimiter::Newline);
        let transcoder = Transcoder::new(None, None).expect("transcoder");

        let (records, _) = tailer
            .read_records(&mut scanner, &transcoder)
            .await
            .expect("read");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0], b"line two");
    }
}
