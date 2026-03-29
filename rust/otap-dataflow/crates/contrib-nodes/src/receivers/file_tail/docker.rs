// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Docker container log format parsers.
//!
//! Currently only the JSON-file driver format is implemented. Other drivers
//! (journald, fluentd, syslog) are declared in the configuration enum but
//! return an error at config-validation time.

use serde::Deserialize;

/// A single parsed Docker JSON-file log line.
#[derive(Debug)]
pub struct DockerLogEntry<'a> {
    /// The actual log content (without trailing newline).
    pub log: &'a str,
    /// Stream name (`stdout` or `stderr`).
    pub stream: &'a str,
    /// RFC 3339 timestamp from Docker.
    pub time: &'a str,
}

/// Raw JSON structure of a Docker JSON-file driver log line.
#[derive(Deserialize)]
struct JsonFileLine<'a> {
    log: &'a str,
    stream: &'a str,
    time: &'a str,
}

/// Parse a single Docker JSON-file log line.
///
/// The expected format is:
/// ```json
/// {"log":"actual log line\n","stream":"stdout","time":"2024-01-01T00:00:00.000000000Z"}
/// ```
///
/// Returns `None` if the line is not valid JSON-file format.
#[must_use]
pub fn parse_docker_json_line(line: &[u8]) -> Option<DockerLogEntry<'_>> {
    let s = std::str::from_utf8(line).ok()?;
    let parsed: JsonFileLine<'_> = serde_json::from_str(s).ok()?;
    Some(DockerLogEntry {
        log: parsed.log.trim_end_matches('\n'),
        stream: parsed.stream,
        time: parsed.time,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_json_file_line() {
        let line =
            br#"{"log":"hello world\n","stream":"stdout","time":"2024-01-15T10:30:00.123456789Z"}"#;
        let entry = parse_docker_json_line(line).expect("should parse");
        assert_eq!(entry.log, r"hello world\n");
        assert_eq!(entry.stream, "stdout");
        assert_eq!(entry.time, "2024-01-15T10:30:00.123456789Z");
    }

    #[test]
    fn test_parse_json_file_line_stderr() {
        let line = br#"{"log":"error msg\n","stream":"stderr","time":"2024-01-15T10:30:00Z"}"#;
        let entry = parse_docker_json_line(line).expect("should parse");
        assert_eq!(entry.stream, "stderr");
    }

    #[test]
    fn test_parse_invalid_line() {
        let line = b"this is not json";
        assert!(parse_docker_json_line(line).is_none());
    }

    #[test]
    fn test_parse_empty() {
        assert!(parse_docker_json_line(b"").is_none());
    }
}
