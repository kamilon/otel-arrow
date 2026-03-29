// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Encoding detection and transcoding between character sets.
//!
//! Uses `encoding_rs` for the actual conversion work. When both source and
//! target are UTF-8 (the default) the transcoder is a no-op.

use encoding_rs::Encoding;

/// A reusable transcoder that converts bytes from one encoding to another.
pub struct Transcoder {
    source: &'static Encoding,
    target: &'static Encoding,
}

/// Errors that may occur during encoding resolution or transcoding.
#[derive(Debug)]
pub enum EncodingError {
    /// The requested encoding name is not recognised by `encoding_rs`.
    UnknownEncoding(String),
}

impl std::fmt::Display for EncodingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownEncoding(name) => write!(f, "unknown encoding: {name}"),
        }
    }
}

impl std::error::Error for EncodingError {}

impl Transcoder {
    /// Build a transcoder from optional encoding names.
    ///
    /// If both are `None` the transcoder is a UTF-8 ↔ UTF-8 no-op.
    pub fn new(
        source_encoding: Option<&str>,
        target_encoding: Option<&str>,
    ) -> Result<Self, EncodingError> {
        let source = resolve(source_encoding)?;
        let target = resolve(target_encoding)?;
        Ok(Self { source, target })
    }

    /// Returns `true` when transcoding is a no-op (source == target).
    #[must_use]
    pub fn is_noop(&self) -> bool {
        self.source == self.target
    }

    /// Transcode `input` bytes from the source encoding to the target
    /// encoding.
    ///
    /// When the transcoder is a no-op this returns the input unchanged (as a
    /// `Cow::Borrowed`).
    #[must_use]
    pub fn transcode<'a>(&self, input: &'a [u8]) -> std::borrow::Cow<'a, [u8]> {
        if self.is_noop() {
            return std::borrow::Cow::Borrowed(input);
        }

        // Decode source → UTF-8 string
        let (decoded, _, _) = self.source.decode(input);

        if self.target == encoding_rs::UTF_8 {
            // Already in UTF-8 after decoding
            return match decoded {
                std::borrow::Cow::Borrowed(_) => std::borrow::Cow::Borrowed(input),
                std::borrow::Cow::Owned(s) => std::borrow::Cow::Owned(s.into_bytes()),
            };
        }

        // Re-encode UTF-8 → target
        let (encoded, _, _) = self.target.encode(&decoded);
        match encoded {
            std::borrow::Cow::Borrowed(b) => std::borrow::Cow::Owned(b.to_vec()),
            std::borrow::Cow::Owned(v) => std::borrow::Cow::Owned(v),
        }
    }

    /// Strip a byte-order mark (BOM) from the beginning of `data` if present.
    /// Returns the data with BOM removed and whether a BOM was found.
    #[must_use]
    pub fn strip_bom(data: &[u8]) -> (&[u8], bool) {
        // UTF-8 BOM
        if data.starts_with(&[0xEF, 0xBB, 0xBF]) {
            return (&data[3..], true);
        }
        // UTF-16 LE BOM
        if data.starts_with(&[0xFF, 0xFE]) {
            return (&data[2..], true);
        }
        // UTF-16 BE BOM
        if data.starts_with(&[0xFE, 0xFF]) {
            return (&data[2..], true);
        }
        (data, false)
    }
}

/// Resolve an optional encoding name to an `encoding_rs::Encoding`.
/// `None` → UTF-8.
fn resolve(name: Option<&str>) -> Result<&'static Encoding, EncodingError> {
    match name {
        None => Ok(encoding_rs::UTF_8),
        Some(n) => Encoding::for_label(n.as_bytes())
            .ok_or_else(|| EncodingError::UnknownEncoding(n.to_owned())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_noop_transcoder() {
        let t = Transcoder::new(None, None).expect("should create");
        assert!(t.is_noop());
        let input = b"hello world";
        let out = t.transcode(input);
        assert_eq!(&*out, input);
    }

    #[test]
    fn test_latin1_to_utf8() {
        let t = Transcoder::new(Some("latin1"), Some("utf-8")).expect("should create");
        assert!(!t.is_noop());
        // 0xE9 = 'é' in Latin-1
        let input = &[0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0xE9];
        let out = t.transcode(input);
        let s = std::str::from_utf8(&out).expect("should be valid utf8");
        assert_eq!(s, "Hello é");
    }

    #[test]
    fn test_unknown_encoding() {
        let err = Transcoder::new(Some("not-a-real-encoding"), None);
        assert!(err.is_err());
    }

    #[test]
    fn test_bom_stripping() {
        let utf8_bom = &[0xEF, 0xBB, 0xBF, b'h', b'i'];
        let (data, found) = Transcoder::strip_bom(utf8_bom);
        assert!(found);
        assert_eq!(data, b"hi");

        let no_bom = b"hello";
        let (data, found) = Transcoder::strip_bom(no_bom);
        assert!(!found);
        assert_eq!(data, b"hello");
    }
}
