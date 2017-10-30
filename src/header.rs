

use bincode;
use std::io;
use std::io::{Read,Write};

#[derive(Serialize, Deserialize, Copy,Clone)]
pub struct Header {
    magic_file_id: u64,
    pub root_bits: u8,
    _reserved1: [u8;7],
    _reserved2: [u64;4]

}

pub const MAGIC_FILE_ID: u64 = 0x485348_53544f5231;


impl Header {

    pub fn new(root_bits: u8) -> Self {
        Header {
            magic_file_id: MAGIC_FILE_ID,
            root_bits: root_bits,
            _reserved1: [0u8;7],
            _reserved2: [0u64;4],
        }
    }

    pub fn is_correct_fileid(&self) -> bool {
        self.magic_file_id == MAGIC_FILE_ID
    }

    pub fn read<R : Read>(rdr: &mut R) -> Result<Header, io::Error> {
        bincode::deserialize_from(rdr, bincode::Infinite)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))
    }

    pub fn write<W : Write>(wrt: &mut W, hdr: &Header) -> Result<(), io::Error> {
        bincode::serialize_into(wrt, hdr, bincode::Infinite)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))
    }
}