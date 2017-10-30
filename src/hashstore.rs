extern crate memmap;

use bincode;

use std::sync::atomic;

use std::{io, fs, mem, path};
use header;

use io::*;
use values::*;

/// Any `HashStoreError` returned indicates corruption of the database
/// or a non-recoverable IO problem
#[derive(Debug)]
pub enum HashStoreError {
    IoError(io::Error),
    InvalidMagicFileId,
    InvalidRootBits,
    Other
}

impl From<io::Error> for HashStoreError {
    fn from(err: io::Error) -> HashStoreError {
        HashStoreError::IoError(err)
    }
}

impl From<bincode::Error> for HashStoreError {
    fn from(err: bincode::Error) -> HashStoreError {
        match *err {
            bincode::ErrorKind::Io(e) => HashStoreError::IoError(e),
            _ => HashStoreError::Other
        }
    }
}

pub enum SearchDepth {
    FullSearch,
    SearchAfter(u32)
}

impl SearchDepth {
    fn check(&self, time: u32) -> bool {
        match *self {
            SearchDepth::FullSearch     => true,
            SearchDepth::SearchAfter(x) => time >= x
        }

    }
}

/// Handle to a hashstore database
///
/// This provides get and set operations
///
/// # Example
///
/// let hs = hashstore::HashStore::new("test", 24);
///
pub struct HashStore {
    // 3 handles to the same file
    _mmap_file: fs::File,
    rw_file: fs::File,
    append_file: fs::File,

    // memory map to root table
    _mmap: memmap::Mmap,
    root: &'static [atomic::AtomicU64],

    root_bits: u8,


}



impl HashStore {

    /// Creates or opens a hashstore
    ///
    /// `root_bits` is the number of bits of each key that are used for the root hash table
    ///
    pub fn new<P : AsRef<path::Path>>(filename: P, root_bits: u8) -> Result<HashStore, HashStoreError> {
        let filename = filename.as_ref();
        if !filename.exists() {

            // create new file
            let hdr = header::Header::new(root_bits);
            let mut f = fs::File::create(&filename)?;

            header::Header::write(&mut f, &hdr)?;

            let root_count = 1 << root_bits;
            f.set_len(mem::size_of::<header::Header>() as u64 + root_count * 8)?;
        }

        // open 3 handles
        let mut rw_file = fs::OpenOptions::new().read(true).write(true).open(&filename)?;
        let mmap_file = fs::OpenOptions::new().read(true).write(true).open(&filename)?;
        let append_file = fs::OpenOptions::new().append(true).open(&filename)?;

        // verify header
        let hdr = header::Header::read(&mut rw_file)?;
        let root_count = 1 << hdr.root_bits;

        if !hdr.is_correct_fileid() {
            return Err(HashStoreError::InvalidMagicFileId);
        }
        if hdr.root_bits != root_bits {
            return Err(HashStoreError::InvalidRootBits);
        }

        // setup memmap
        let mut mmap = memmap::Mmap::open_with_offset(
            &mmap_file,
            memmap::Protection::ReadWrite,
            mem::size_of::<header::Header>(),
            8 * root_count
        )?;

        let u64_ptr = mmap.mut_ptr() as *mut atomic::AtomicU64;
        let root_ptr = unsafe { ::std::slice::from_raw_parts(u64_ptr, root_count) };

        Ok(HashStore {
            _mmap: mmap,
            _mmap_file: mmap_file,
            root: root_ptr,
            rw_file: rw_file,
            append_file: append_file,
            root_bits: root_bits,
        })
    }

    /// Checks if `key` exists and returns a persistent pointer if it does
    ///
    /// If `depth` is `SearchDepth::SearchAfter(x)` the search is abandoned after an element with
    /// `time < x` is encountered
    pub fn exists(&mut self, key: &[u8; 32], depth: SearchDepth) -> Result<Option<ValuePtr>, HashStoreError>
    {
        let idx     = get_root_index(self.root_bits, &key);
        let mut ptr = self.root[idx].load(atomic::Ordering::Relaxed);

        // loop over linked list of value-objects at `ptr`
        loop {

            if ptr == 0 {
                return Ok(None);
            }

            let (prefix, _) = read_value_start(&mut self.rw_file, ptr, Some(0))?;

            if prefix.key == *key && !prefix.is_dependency{
                return Ok(Some(ptr));
            }

            if !depth.check(prefix.time) {
                return Ok(None);
            }
            ptr = prefix.prev_pos;
        }
    }

    /// Checks if `key` exists and returns the value if it does
    ///
    /// If `depth` is `SearchDepth::SearchAfter(x)` the search is abandoned after an element with
    /// `time < x` is encountered.
    ///
    /// If `key` is not found, a dependency anchor is inserted at `key` which will prevent subsequent
    /// `set` of `key` to fail if the dependency isn't met.
    pub fn get_dependency(&mut self, key: &[u8; 32], dependent_on: &[u8; 32], time: u32) -> Result<Option<Vec<u8>>, HashStoreError>
    {
        let idx = get_root_index(self.root_bits, &key);

        // Compare-and-swap loop
        loop {
            let first_ptr = self.root[idx].load(atomic::Ordering::Relaxed);
            let mut ptr = first_ptr;

            // loop over linked list of value-objects at dataptr
            loop {
                if ptr == 0 {
                    break;
                }

                let (prefix, mut value) = read_value_start(&mut self.rw_file, ptr, None)?;

                if prefix.key == *key {
                    read_value_finish(&mut self.rw_file, &prefix, &mut value)?;
                    return Ok(Some(value));
                }

                ptr = prefix.prev_pos;
            }

            // not found; try adding the dependency in the same CAS-loop
            let prefix = ValuePrefix {
                key: *key,
                prev_pos: first_ptr,
                time: time,
                size: 32,
                ..Default::default()
            };

            let new_dataptr = write_value(&mut self.append_file, prefix, &dependent_on[..])?;

            let swap_dataptr = self.root[idx].compare_and_swap
                (first_ptr, new_dataptr, atomic::Ordering::Relaxed);

            if swap_dataptr == first_ptr {
                return Ok(None);
            }

        }
    }


    /// Directly reads the value pointed to by `ptr`
    ///
    /// The `size` field of `ptr` does not need to be accurate and is used as estimate.
    /// If it is too small, a second read is performed
    pub fn get_by_ptr(&mut self, ptr: ValuePtr) -> Result<Vec<u8>, HashStoreError>
    {
        let (prefix, mut content) = read_value_start(&mut self.rw_file, ptr, None)?;
        read_value_finish(&mut self.rw_file, &prefix, &mut content)?;
        Ok(content)
    }


    /// Checks if `key` exists and returns the value if it does
    ///
    /// If `depth` is `SearchDepth::SearchAfter(x)` the search is abandoned after an element with
    /// `time < x` is encountered.
    pub fn get(&mut self, key: [u8; 32], depth: SearchDepth) -> Result<Option<Vec<u8>>, HashStoreError>
    {
        let idx = get_root_index(self.root_bits, &key);

        let mut ptr = self.root[idx].load(atomic::Ordering::Relaxed);

        // loop over linked list of value-objects at dataptr
        loop {

            if ptr == 0 {
                return Ok(None);
            }

            let (prefix, mut value) = read_value_start(&mut self.rw_file, ptr, None)?;

            if prefix.key == key  && !prefix.is_dependency {
                read_value_finish(&mut self.rw_file, &prefix, &mut value)?;
                return Ok(Some(value));
            }

            if !depth.check(prefix.time) {
                return Ok(None);
            }
            ptr = prefix.prev_pos;

        }
    }

    /// Stores `value` at `key`
    ///
    /// If there are any keys in the database that have `key` as dependency, the method will fail
    /// if these are not passed in `solved_dependencies`
    ///
    /// `time` can be any integer that roughly increases with time (eg a block height),
    /// and is used to query only recent keys
    pub fn set(&mut self,
               key: &[u8; 32],
               value: &[u8],
               solved_dependencies: Vec<[u8; 32]>,
               depth: SearchDepth,
               time: u32)
        -> Result<Option<ValuePtr>, HashStoreError>
    {
        let idx = get_root_index(self.root_bits, &key);

        // Compare-and-swap loop
        loop {
            let old_dataptr = self.root[idx].load(atomic::Ordering::Relaxed);

            // check dependencies; loop over linked list of value-objects at ptr
            let mut ptr = old_dataptr;
            loop {
                if ptr == 0 { break; }

                let (prefix, content) = read_value_start(&mut self.rw_file, ptr, Some(32))?;
                if &prefix.key[..] == key && prefix.is_dependency {
                    // unmet dependency?
                    if !solved_dependencies.iter().any(|x| x == &content[..]) {
                        return Ok(None);
                    }
                }

                if !depth.check(prefix.time) { break };

                ptr = prefix.prev_pos;
            }


            let prefix = ValuePrefix {
                key: *key,
                prev_pos: old_dataptr,
                time: time,
                size: value.len() as u32,
                ..Default::default()
            };

            let new_dataptr = write_value(&mut self.append_file, prefix, value)?;

            let swap_dataptr = self.root[idx].compare_and_swap
                (old_dataptr, new_dataptr, atomic::Ordering::Relaxed);

            if swap_dataptr == old_dataptr {
                return Ok(Some(new_dataptr));
            }
            panic!("Hmm; not testing concurrency yet");
        }
    }

    /// Stores `value` at `key` without verifying any dependency
    ///
    /// `time` can be any integer that roughly increases with time (eg a block height),
    /// and is used to query only recent keys
    pub fn set_unchecked(&mut self, key: &[u8; 32], value: &[u8], time: u32) -> Result<ValuePtr, HashStoreError>
    {
        let idx = get_root_index(self.root_bits, key);

        // Compare-and-swap loop
        loop {
            let old_ptr = self.root[idx].load(atomic::Ordering::Relaxed);

            let prefix = ValuePrefix {
                key: *key,
                prev_pos: old_ptr,
                time: time,
                size: value.len() as u32,
                ..Default::default()
            };

            let new_ptr = write_value(&mut self.append_file, prefix, value)?;

            let swap_ptr = self.root[idx].compare_and_swap
                (old_ptr, new_ptr, atomic::Ordering::Relaxed);

            if swap_ptr == old_ptr {
                return Ok(new_ptr);
            }
            panic!("Hmm; not testing concurrency yet");
        }
    }


}

// Returns the index into the root hash table for a key
// This uses the first self.root_bits as index
fn get_root_index(root_bits: u8, key: &[u8; 32]) -> usize {
    let bits32 = ((key[0] as usize) << 24) |
        ((key[1] as usize) << 16) |
        ((key[2] as usize) << 8) |
        (key[3] as usize);
    bits32 >> (32 - root_bits)
}


#[cfg(test)]
mod tests {
    extern crate rand;

    use super::*;
    use self::rand::Rng;

    fn random_key<R: Rng>(rng: &mut R) -> [u8; 32] {
        let mut key = [0; 32];
        rng.fill_bytes(&mut key);
        key
    }

    #[test]
    fn test_get_root_index() {
        for _ in 0..100 {
            let x = random_key(&mut rand::thread_rng());

            assert_eq!(get_root_index(2,&x), (x[0] as usize) >> 6 );
            assert_eq!(get_root_index(6,&x), (x[0] as usize) >> 2 );
            assert_eq!(get_root_index(8,&x), x[0] as usize );
            assert_eq!(get_root_index(9,&x), ((x[0] as usize)<<1) | ((x[1]) as usize) >> 7);
        }
    }

    // Pub function tested in /tests
 }

