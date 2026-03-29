// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Record delimiter strategies for splitting file content into individual
//! records.

use crate::receivers::file_tail::config::Delimiter;

/// Result of scanning a buffer for a complete record.
#[derive(Debug)]
pub enum ScanResult {
    /// A complete record was found ending at the given byte offset
    /// (exclusive, includes the delimiter).
    Complete {
        /// Byte offset past the end of the record (including delimiter).
        end: usize,
        /// Byte range of the record payload (excluding delimiter).
        payload_start: usize,
        /// End of the payload (excluding trailing delimiter bytes).
        payload_end: usize,
    },
    /// No complete record found — caller should read more data.
    Incomplete,
}

/// Stateful scanner that finds record boundaries in a byte buffer.
pub struct DelimiterScanner {
    kind: ScannerKind,
}

enum ScannerKind {
    Newline,
    Csv,
    Tsv,
    Json { depth: usize },
    Custom(Vec<u8>),
}

impl DelimiterScanner {
    /// Create a scanner from the configured delimiter.
    #[must_use]
    pub fn new(delimiter: &Delimiter) -> Self {
        let kind = match delimiter {
            Delimiter::Newline => ScannerKind::Newline,
            Delimiter::Csv => ScannerKind::Csv,
            Delimiter::Tsv => ScannerKind::Tsv,
            Delimiter::Json => ScannerKind::Json { depth: 0 },
            Delimiter::Custom(s) => ScannerKind::Custom(s.as_bytes().to_vec()),
        };
        Self { kind }
    }

    /// Reset any internal state (e.g. JSON nesting depth) between files.
    pub fn reset(&mut self) {
        if let ScannerKind::Json { depth } = &mut self.kind {
            *depth = 0;
        }
    }

    /// Scan `buf[start..]` for the next complete record.
    ///
    /// Returns [`ScanResult::Complete`] with the byte offsets of the record
    /// payload and the total consumed length (including delimiter), or
    /// [`ScanResult::Incomplete`] if more data is needed.
    #[must_use]
    pub fn scan(&mut self, buf: &[u8], start: usize) -> ScanResult {
        match &mut self.kind {
            ScannerKind::Newline => scan_newline(buf, start),
            ScannerKind::Csv => scan_csv_line(buf, start),
            ScannerKind::Tsv => scan_newline(buf, start), // TSV records are newline-delimited
            ScannerKind::Json { depth } => scan_json(buf, start, depth),
            ScannerKind::Custom(sep) => scan_custom(buf, start, sep),
        }
    }
}

/// Scan for `\n` (also handling `\r\n`).
fn scan_newline(buf: &[u8], start: usize) -> ScanResult {
    for i in start..buf.len() {
        if buf[i] == b'\n' {
            let payload_end = if i > start && buf[i - 1] == b'\r' {
                i - 1
            } else {
                i
            };
            return ScanResult::Complete {
                end: i + 1,
                payload_start: start,
                payload_end,
            };
        }
    }
    ScanResult::Incomplete
}

/// Scan for a newline while respecting double-quoted CSV fields.
fn scan_csv_line(buf: &[u8], start: usize) -> ScanResult {
    let mut in_quotes = false;
    let mut i = start;
    while i < buf.len() {
        let b = buf[i];
        if in_quotes {
            if b == b'"' {
                // Check for escaped quote (`""`)
                if i + 1 < buf.len() && buf[i + 1] == b'"' {
                    i += 2;
                    continue;
                }
                in_quotes = false;
            }
        } else if b == b'"' {
            in_quotes = true;
        } else if b == b'\n' {
            let payload_end = if i > start && buf[i - 1] == b'\r' {
                i - 1
            } else {
                i
            };
            return ScanResult::Complete {
                end: i + 1,
                payload_start: start,
                payload_end,
            };
        }
        i += 1;
    }
    ScanResult::Incomplete
}

/// Scan for a balanced `{...}` JSON object.
fn scan_json(buf: &[u8], start: usize, depth: &mut usize) -> ScanResult {
    let mut in_string = false;
    let mut escape_next = false;

    for i in start..buf.len() {
        let b = buf[i];

        if escape_next {
            escape_next = false;
            continue;
        }

        if in_string {
            match b {
                b'\\' => escape_next = true,
                b'"' => in_string = false,
                _ => {}
            }
            continue;
        }

        match b {
            b'"' => in_string = true,
            b'{' => *depth += 1,
            b'}' => {
                if *depth > 0 {
                    *depth -= 1;
                    if *depth == 0 {
                        // Skip optional trailing whitespace/newline after the object
                        let mut end = i + 1;
                        while end < buf.len()
                            && (buf[end] == b'\n' || buf[end] == b'\r' || buf[end] == b' ')
                        {
                            end += 1;
                        }
                        return ScanResult::Complete {
                            end,
                            payload_start: start,
                            payload_end: i + 1,
                        };
                    }
                }
            }
            _ => {}
        }
    }
    ScanResult::Incomplete
}

/// Scan for an arbitrary byte-sequence separator.
fn scan_custom(buf: &[u8], start: usize, sep: &[u8]) -> ScanResult {
    if sep.is_empty() {
        return ScanResult::Incomplete;
    }
    if let Some(pos) = buf[start..].windows(sep.len()).position(|w| w == sep) {
        let abs = start + pos;
        return ScanResult::Complete {
            end: abs + sep.len(),
            payload_start: start,
            payload_end: abs,
        };
    }
    ScanResult::Incomplete
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_newline_scanner() {
        let mut s = DelimiterScanner::new(&Delimiter::Newline);
        let buf = b"hello\nworld\n";
        match s.scan(buf, 0) {
            ScanResult::Complete {
                end,
                payload_start,
                payload_end,
            } => {
                assert_eq!(payload_start, 0);
                assert_eq!(payload_end, 5);
                assert_eq!(end, 6);
                assert_eq!(&buf[payload_start..payload_end], b"hello");
            }
            ScanResult::Incomplete => panic!("expected complete"),
        }
    }

    #[test]
    fn test_crlf() {
        let mut s = DelimiterScanner::new(&Delimiter::Newline);
        let buf = b"line\r\n";
        match s.scan(buf, 0) {
            ScanResult::Complete {
                payload_start,
                payload_end,
                ..
            } => {
                assert_eq!(&buf[payload_start..payload_end], b"line");
            }
            ScanResult::Incomplete => panic!("expected complete"),
        }
    }

    #[test]
    fn test_csv_quoted_newline() {
        let mut s = DelimiterScanner::new(&Delimiter::Csv);
        let buf = b"\"field\nwith\nnewlines\",other\n";
        match s.scan(buf, 0) {
            ScanResult::Complete {
                payload_start,
                payload_end,
                ..
            } => {
                assert_eq!(
                    &buf[payload_start..payload_end],
                    b"\"field\nwith\nnewlines\",other"
                );
            }
            ScanResult::Incomplete => panic!("expected complete"),
        }
    }

    #[test]
    fn test_json_balanced() {
        let mut s = DelimiterScanner::new(&Delimiter::Json);
        let buf = br#"{"key": "val", "nested": {"a": 1}}"#;
        match s.scan(buf, 0) {
            ScanResult::Complete {
                payload_start,
                payload_end,
                ..
            } => {
                assert_eq!(payload_start, 0);
                assert_eq!(payload_end, buf.len());
            }
            ScanResult::Incomplete => panic!("expected complete"),
        }
    }

    #[test]
    fn test_json_with_escaped_braces_in_string() {
        let mut s = DelimiterScanner::new(&Delimiter::Json);
        let buf = br#"{"msg": "contains } and { chars"}"#;
        match s.scan(buf, 0) {
            ScanResult::Complete {
                payload_start,
                payload_end,
                ..
            } => {
                assert_eq!(payload_start, 0);
                assert_eq!(payload_end, buf.len());
            }
            ScanResult::Incomplete => panic!("expected complete"),
        }
    }

    #[test]
    fn test_custom_delimiter() {
        let mut s = DelimiterScanner::new(&Delimiter::Custom("---".to_owned()));
        let buf = b"record one---record two---";
        match s.scan(buf, 0) {
            ScanResult::Complete {
                end,
                payload_start,
                payload_end,
                ..
            } => {
                assert_eq!(&buf[payload_start..payload_end], b"record one");
                assert_eq!(end, 13); // "record one---".len()
            }
            ScanResult::Incomplete => panic!("expected complete"),
        }
    }

    #[test]
    fn test_incomplete() {
        let mut s = DelimiterScanner::new(&Delimiter::Newline);
        let buf = b"no newline here";
        assert!(matches!(s.scan(buf, 0), ScanResult::Incomplete));
    }
}
