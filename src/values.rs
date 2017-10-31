



/// A persistent pointer to a value in the database
/// It contains the bitfields:
///
/// * bit 0-47   file position
/// * bit 48-51  size of object: X such that size is at most 1 << (X+5) bytes
/// * bit 52     bit set if this or any object in further in this object chain is a dependency
///
/// This mod are some helper functions to encode/decode dataptrs

pub type ValuePtr = u64;


// A prefix for every value in the database
#[derive(Default, Serialize, Deserialize)]
pub struct ValuePrefix {
    pub key: [u8; 32],
    pub prev_pos: u64,
    pub size: u32,
    pub time: u32
}



pub fn ptr_new(filepos: u64, sz: usize) -> ValuePtr {

    // compress size: find S such that size is at least 2^(S+6)
    let s = ((sz as u64)
        .next_power_of_two()
        .trailing_zeros() as u64)
        .saturating_sub(6);

    if s >= 16 {
        filepos
    }
    else {
        filepos | (s << 48)
    }
}

// Returns an *estimate* of the size of the object
pub fn ptr_size_est(dataptr: ValuePtr) -> usize {
    (1 << (6 + ((dataptr >> 48) & 0xF))) as usize
}


pub fn ptr_file_pos(dataptr: ValuePtr) -> u64 {
    dataptr & 0xFFFF_FFFF_FFFF
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size() {
        // test size encode/decode
        for n in 0..2000000 {
            let dp = ptr_new(0, n);
            let sz = ptr_size_est(dp);
            assert!(sz >= n, format!("n={}", n));
        }
    }
}
