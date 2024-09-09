/*!
 Errors that can happen when parsing `typedstream` data. This module is for the new `typedstream` deserializer.
*/

use protobuf::Error as protoErr;
use std::io::Error as ioErr;
use std::fmt::{Display, Formatter, Result};

/// Errors that can happen when parsing `handwriting` data
#[derive(Debug)]
pub enum HandwritingError {
    ProtobufError(protoErr),
    InvalidFrameSize(usize),
    XZError(ioErr),
    CompressionUnknown,
    InvalidStrokesLength(usize, usize),
    ConversionError,
    DecompressedNotSet,
    InvalidDecompressedLength(usize, usize),
}

impl Display for HandwritingError {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> Result {
        match self {
            HandwritingError::ProtobufError(why) => write!(fmt, "failed to parse handwriting protobuf: {why}"),
            HandwritingError::InvalidFrameSize(size) => write!(fmt, "expected size 8, got {size}"),
            HandwritingError::XZError(why) => write!(fmt, "failed to decompress xz: {why}"),
            HandwritingError::CompressionUnknown => write!(fmt, "compress method unknown"),
            HandwritingError::InvalidStrokesLength(index, length) => write!(fmt, "can't access index {index} on array length {length}"),
            HandwritingError::ConversionError => write!(fmt, "failed to convert num"),
            HandwritingError::DecompressedNotSet => write!(fmt, "decompressed length not set on compressed message"),
            HandwritingError::InvalidDecompressedLength(expected, got) => write!(fmt, "expected decompressed length of {expected}, got {got}"),
        }
    }
}
