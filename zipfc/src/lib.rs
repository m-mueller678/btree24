#![allow(clippy::missing_safety_doc)]

use rand::distributions::Uniform;
use rand::prelude::*;
use rand::Fill;
use rand_xoshiro::Xoshiro256StarStar;
use rayon::prelude::*;
use rip_shuffle::RipShuffleParallel;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::ffi::{c_char, CStr};
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader};
use std::mem::{size_of, MaybeUninit};
use std::ptr::hash;
use std::slice::from_raw_parts;
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::atomic::Ordering::Relaxed;
use rand_distr::Geometric;
use random_word::Lang;
use zipf::ZipfDistribution;

unsafe impl Send for Key {}

unsafe impl Sync for Key {}

#[repr(C)]
pub struct Key {
    data: *const u8,
    len: u64,
}

fn init_rayon() {
    static INIT: AtomicBool = AtomicBool::new(false);
    if !INIT.swap(true, Relaxed) {
        rayon::ThreadPoolBuilder::new()
            .thread_name(|i| format!("rayon worker {i}"))
            .build_global().unwrap()
    }
}

impl Debug for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut t = f.debug_tuple("Key");
        let bytes = unsafe { from_raw_parts(self.data, self.len as usize) };
        let show_text =bytes.len() > 10 || bytes.iter().all(|x|x.is_ascii_graphic());
        if show_text{
            t.field(&String::from_utf8_lossy(bytes));
        }
        if bytes.len() <=8 || !show_text{
            t.field(&bytes);
        }
        t.finish()
    }
}

impl Key {
    fn from_vec(x: Vec<u8>) -> Self {
        let x = x.leak();
        Key {
            data: x.as_ptr(),
            len: x.len() as u64,
        }
    }
}

pub type MainRng = Xoshiro256StarStar;

#[no_mangle]
pub unsafe extern "C" fn zipfc_load_keys(
    rng: *mut MainRng,
    name: *const c_char,
    count: u32,
    int_density: f64,
    partition_count: u32,
) -> *const Key {
    init_rayon();
    assert!(int_density <= 1.0);
    assert!(int_density >= 0.05);
    let name = CStr::from_ptr(name);
    let rng = &mut *rng;
    let keys = match name.to_str().unwrap() {
        "rng4" => generate_parallel::<u32>(count, rng),
        "rng8" => generate_parallel::<u32>(count, rng),
        "int" => {
            let gen_count = ((count as f64) / int_density).round();
            assert!(gen_count < 1.0e9);
            let mut generated: Vec<u32> = (0..gen_count as u32).into_par_iter().collect();
            generated.par_shuffle(rng);
            generated.truncate(count as usize);
            generated
                .into_par_iter()
                .map(|x| Key::from_vec(x.to_be_bytes().to_vec()))
                .collect()
        }
        "partitioned_id" => {
            let mut next_id = vec![0u32; partition_count as usize];
            let dist = Uniform::new(0, partition_count);
            (0..count)
                .map(|_| {
                    let partition = dist.sample(rng);
                    let id = next_id[partition as usize];
                    next_id[partition as usize] += 1;
                    Key::from_vec(
                        [partition.to_be_bytes(), id.to_be_bytes()]
                            .into_iter()
                            .flatten()
                            .collect(),
                    )
                })
                .collect()
        }
        "test" => {
            generate_test(count, rng)
        }
        x => {
            let path = x.strip_prefix("file:").expect("bad key set name");
            let mut lines: Vec<Key> = BufReader::new(File::open(path).unwrap())
                .lines()
                .map(|l| Key::from_vec(l.unwrap().into_bytes()))
                .collect();
            lines.par_shuffle(rng);
            lines.truncate(count as usize);
            lines
        }
    };
    assert_eq!(keys.len(), count as usize);
    //dbg!(&keys);
    keys.leak().as_ptr()
}

fn rand_hash(x: &impl Hash) -> u64 {
    let mut h = DefaultHasher::new();
    x.hash(&mut h);
    h.finish()
}

fn generate_test(count: u32, rng: &mut MainRng) -> Vec<Key> {
    let path_seed: u64 = rng.gen();
    let key_type_dist = Uniform::new(0u8, 4);
    let rand_key_len = Uniform::new(3usize, 8);
    let bin_key_len = Uniform::new(20, 60);
    let words = random_word::all(Lang::En);
    let segment_count_dist = Uniform::new(8, 12);
    let segment_choice_dist = Geometric::new(0.08).unwrap();
    let sequential_step_dist = Geometric::new(0.7).unwrap();
    let generate_one = |r: &mut MainRng, sequential: &mut u64| {
        match key_type_dist.sample(r) {
            0 => {
                let len = rand_key_len.sample(r);
                let mut k = vec![0u8; len];
                r.fill_bytes(&mut k);
                k
            }
            1 => {
                let len = bin_key_len.sample(r);
                let mut k = vec![0u8; len];
                for x in &mut k {
                    *x = if r.gen() { b'1' } else { b'0' };
                }
                k
            }
            2 => {
                let mut key = Vec::new();
                let mut segment_seed = path_seed;
                let segment_count = segment_count_dist.sample(r);
                for _ in 0..segment_count {
                    if !key.is_empty() {
                        key.push(b'/');
                    }
                    let choices = 2+segment_choice_dist.sample(&mut MainRng::seed_from_u64(segment_seed));
                    let word_id = rand_hash(&(segment_seed, r.gen::<u64>() % choices)) as usize % words.len();
                    segment_seed = rand_hash(&(word_id as u64, segment_seed));
                    key.extend_from_slice(words[word_id].as_bytes())
                }
                key
            }
            3 => {
                *sequential += sequential_step_dist.sample(r) + 1;
                sequential.to_be_bytes().to_vec()
            }
            _ => unreachable!()
        }
    };

    let mut out = vec![Vec::new(); count as usize];
    let chunks: Vec<(&mut [Vec<u8>], MainRng)> = out
        .chunks_mut(count as usize / 32)
        .zip(rng_stream(rng))
        .collect();
    chunks.into_par_iter().for_each(|(chunk, mut rng)| {
        let mut sequential: u64 = rng.gen();
        for k in chunk {
            *k = generate_one(&mut rng, &mut sequential);
        }
    });
    out.par_sort();
    out.dedup();
    {
        println!("generating {} more keys", count as usize - out.len());
        let mut new_candidates = HashSet::new();
        let mut replacement_sequential: u64 = rng.gen();
        while out.len() + new_candidates.len() < count as usize {
            let new_candidate = generate_one(rng, &mut replacement_sequential);
            if out.binary_search(&new_candidate).is_err() {
                new_candidates.insert(new_candidate);
            }
        }
        out.extend(new_candidates);
    }
    out.par_sort();
    let out = out.into_par_iter()
        .map(Key::from_vec)
        .collect();
    out
}

fn generate_parallel<T>(count: u32, rng: &mut MainRng) -> Vec<Key>
    where
        [T]: Fill,
        T: From<u8> + Clone + Send + Sync + Ord + Hash,
        rand::distributions::Standard: rand::distributions::Distribution<T>,
{
    let mut out = vec![T::from(0); count as usize];
    let chunks: Vec<(&mut [T], MainRng)> = out
        .chunks_mut(count as usize / 32)
        .zip(rng_stream(rng))
        .collect();
    chunks.into_par_iter().for_each(|(chunk, mut rng)| {
        rng.fill(chunk);
    });
    out.par_sort();
    out.dedup();
    let mut new_candidates = HashSet::new();
    while out.len() + new_candidates.len() < count as usize {
        let new_candidate = rng.gen();
        if out.binary_search(&new_candidate).is_err() {
            new_candidates.insert(new_candidate);
        }
    }
    out.extend(new_candidates);
    out.par_shuffle(rng);
    println!("generating {} more keys", count as usize - out.len());
    out.into_par_iter()
        .map(|x| Key {
            data: Box::into_raw(Box::<T>::new(x)) as *const u8,
            len: size_of::<T>() as u64,
        })
        .collect()
}

fn rng_stream(rng: &mut MainRng) -> impl Iterator<Item=MainRng> + '_ {
    std::iter::repeat_with(|| {
        let x = rng.clone();
        rng.jump();
        x
    })
}

#[no_mangle]
pub unsafe extern "C" fn create_zipfc_rng(
    seed: u64,
    thread: u64,
    purpose: *const c_char,
) -> *mut MainRng {
    init_rayon();
    let purpose = CStr::from_ptr(purpose);
    let long_seed = (0..4)
        .flat_map(|i| {
            let mut h = DefaultHasher::new();
            i.hash(&mut h);
            seed.hash(&mut h);
            thread.hash(&mut h);
            purpose.hash(&mut h);
            h.finish().to_le_bytes()
        })
        .collect::<Vec<u8>>();
    Box::into_raw(Box::new(MainRng::from_seed(long_seed.try_into().unwrap())))
}

#[no_mangle]
pub unsafe extern "C" fn generate_workload_c(
    rng: *mut MainRng,
    key_count: u32,
    zipf_parameter: f64,
    count: u32,
) -> *const u32 {
    init_rayon();
    let rng = &mut *rng;
    let mut out = Vec::with_capacity(count as usize);
    fill_zipf(
        &mut *rng,
        &mut out.spare_capacity_mut()[..count as usize],
        key_count,
        zipf_parameter,
    );
    out.set_len(count as usize);
    //dbg!(&out);
    out.leak().as_ptr()
}

fn fill_zipf(rng: &mut Xoshiro256StarStar, dst: &mut [MaybeUninit<u32>], key_count: u32, p: f64) {
    if key_count == 0 {
        assert!(dst.is_empty());
        return;
    }
    let generator = if p > 0.0 {
        Ok(ZipfDistribution::new(key_count as usize, p).unwrap())
    } else {
        Err(Uniform::new(0, key_count))
    };
    // the hashing based sampling described in the ycsb paper deviates noticeably from the desired zipf distribution, so we use a random permutation instead
    let mut permutation: Vec<u32> = (0..key_count).into_par_iter().collect();
    permutation.par_shuffle(rng);

    let chunks: Vec<_> = dst.chunks_mut(1 << 16).zip(rng_stream(rng)).collect();
    chunks.into_par_iter().for_each(|(chunk, mut rng)| {
        for d in chunk {
            let x = match &generator {
                Ok(zipf) => permutation[zipf.sample(&mut rng) - 1],
                Err(uniform) => uniform.sample(&mut rng),
            };
            d.write(x);
        }
    });
}
