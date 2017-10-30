
extern crate hashstore;
extern crate rand;


use hashstore::*;
use std::fs;
use std::time::{Instant};
use std::collections::HashMap;

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
    let mut rng = rand::thread_rng();
    let mut hs = HashStore::new("./tmp-deps", 24).unwrap();


}


#[test]
#[ignore]
fn test_big() {
    let mut rng = rand::weak_rng();
    let mut hs = HashStore::new("./tmp-big", 12).unwrap();

    let mut block1 = HashMap::new();
    let mut blockend = HashMap::new();

    let block_count = 30000;
    // load block 1
    println!("Block 1");
    for _ in 0..100000 {
        let k1 = random_key(&mut rng);
        let v1 = random_value(&mut rng);
        block1.insert(k1, v1.clone());
        hs.set_unchecked(k1, &v1, 1).unwrap();
    }

    // load 20_000
    println!("Next {}", block_count);
    let tm = Instant::now();
    for block in 2..(block_count+2) {
        for _ in 0..2000 {
            let k = random_key(&mut rng);
            let v = random_value(&mut rng);
            hs.set_unchecked(k, &v, block).unwrap();
        }
    }

    println!("{} blocks in {}ms", block_count, ms(tm));
    let b1 = block1.clone();
    let l = block1.len();
    let tm = Instant::now();
    for (k, v) in b1.into_iter() {

        let mut hsv = hs.get(k, SearchDepth::FullSearch).unwrap().unwrap();

    }
    println!("block 1 {} lookups in {}ms", l, ms(tm));

    for _ in 0..100000 {
        let k1 = random_key(&mut rng);
        let v1 = random_value(&mut rng);
        blockend.insert(k1, v1.clone());
        hs.set_unchecked(k1, &v1, 1).unwrap();
    }
    println!("Block-end loaded");

    let b1 = block1.clone();
    let l = block1.len();
    let tm = Instant::now();
    for (k, v) in b1.into_iter() {

        let mut hsv = hs.get(k, SearchDepth::FullSearch).unwrap().unwrap();

    }
    println!("block 1 {} lookups in {}ms", l, ms(tm));
    let tm = Instant::now();
    let l = blockend.len();
    for (k, v) in blockend.into_iter() {

        let mut hsv = hs.get(k, SearchDepth::FullSearch).unwrap().unwrap();

    }
    println!("block end {} lookups in {}ms", l, ms(tm));
}
