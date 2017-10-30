
extern crate hashstore;
extern crate rand;


use hashstore::*;
use std::time::{Instant};
use std::collections::HashMap;
use std::{fs,path};

use self::rand::Rng;

fn random_value<R : Rng>(rng: &mut R) -> Vec<u8> {

    let size = if rng.next_u32() & 100 == 1 {
        100 + (rng.next_u32() % 200_000)
    }
        else {
            100 + (rng.next_u32() % 600)
        };
    let mut value = vec![0; size as usize];
    rng.fill_bytes(&mut value);
    value
}

fn random_key<R: Rng>(rng: &mut R) -> [u8; 32] {
    let mut key = [0; 32];
    rng.fill_bytes(&mut key);
    key
}

fn ms(start: Instant) -> u64 {
    let d = Instant::now() - start;
    (d.as_secs() * 1000) as u64 + (d.subsec_nanos() / 1_000_000) as u64
}



#[test]
fn test_dependency() {

    let mut hs = HashStore::new("./tmp-deps", 24).unwrap();

    hs.set(&[1;32], &[2;8], vec![], SearchDepth::FullSearch, 10).unwrap();

    // successful get dependency
    assert!(hs.get_dependency(&[1;32], &[3;32], 10).unwrap().is_some());
    assert!(hs.exists(&[1;32], SearchDepth::FullSearch).unwrap().is_some());
    assert!(hs.exists(&[3;32], SearchDepth::FullSearch).unwrap().is_none());


}


#[test]
fn test_exists() {
    let p = path::Path::new("./tst-exists");
    if p.exists() {
        fs::remove_file(p).unwrap();
    }
    // we use a root hashtable of size one to test search depth
    let mut hs = HashStore::new(p, 0).unwrap();

    hs.set_unchecked(&[1;32], &[2;8], 10).unwrap();
    hs.set_unchecked(&[3;32], &[4;8], 20).unwrap();
    hs.set_unchecked(&[5;32], &[6;8], 30).unwrap();

    assert!(hs.exists(&[1;32], SearchDepth::FullSearch).unwrap().is_some());

    // after(20) still reaches 1, as we only stop *after* and element has t<20
    assert!(hs.exists(&[1;32], SearchDepth::SearchAfter(20)).unwrap().is_some());
    assert!(hs.exists(&[1;32], SearchDepth::SearchAfter(21)).unwrap().is_none());

    assert!(hs.exists(&[2;32], SearchDepth::FullSearch).unwrap().is_none());
    assert!(hs.exists(&[2;32], SearchDepth::SearchAfter(25)).unwrap().is_none());

    assert!(hs.exists(&[5;32], SearchDepth::FullSearch).unwrap().is_some());
    assert!(hs.exists(&[5;32], SearchDepth::SearchAfter(45)).unwrap().is_some());

}

#[test]
#[ignore]
fn test_big() {
    let mut rng = rand::weak_rng();
    let mut hs = HashStore::new("./tmp-big", 26).unwrap();

    let mut block1 = HashMap::new();
    let mut blockend = HashMap::new();

    let block_count = 20000;
    // load block 1
    println!("Block 1");
    for _ in 0..100000 {
        let k1 = random_key(&mut rng);
        let v1 = random_value(&mut rng);
        block1.insert(k1, v1.clone());
        hs.set_unchecked(&k1, &v1, 1).unwrap();
    }
    let b1 = block1.clone();
    let l = block1.len();
    let tm = Instant::now();
    for (k, _) in b1.into_iter() {

        let _ = hs.get(k, SearchDepth::FullSearch).unwrap().unwrap();

    }
    println!("block 1 {} lookups in {}ms", l, ms(tm));

    // load 20_000
    println!("Next {}", block_count);
    let tm = Instant::now();
    for block in 2..(block_count+2) {
        for _ in 0..2000 {
            let k = random_key(&mut rng);
            let v = random_value(&mut rng);
            hs.set_unchecked(&k, &v, block).unwrap();
        }
    }

    println!("{} blocks in {}ms", block_count, ms(tm));
    let b1 = block1.clone();
    let l = block1.len();
    let tm = Instant::now();
    for (k, _) in b1.into_iter() {

        let _ = hs.get(k, SearchDepth::FullSearch).unwrap().unwrap();

    }
    println!("block 1 {} lookups in {}ms", l, ms(tm));

    for _ in 0..100000 {
        let k1 = random_key(&mut rng);
        let v1 = random_value(&mut rng);
        blockend.insert(k1, v1.clone());
        hs.set_unchecked(&k1, &v1, 1).unwrap();
    }
    println!("Block-end loaded");

    let b1 = block1.clone();
    let l = block1.len();
    let tm = Instant::now();
    for (k, _) in b1.into_iter() {

        let _ = hs.get(k, SearchDepth::FullSearch).unwrap().unwrap();

    }
    println!("block 1 {} lookups in {}ms", l, ms(tm));
    let tm = Instant::now();
    let l = blockend.len();
    for (k, _) in blockend.into_iter() {

        let _ = hs.get(k, SearchDepth::FullSearch).unwrap().unwrap();

    }
    println!("block end {} lookups in {}ms", l, ms(tm));
}
