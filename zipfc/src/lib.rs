use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::slice::from_raw_parts_mut;
use rand::distributions::Uniform;
use rand::prelude::*;
use zipf::ZipfDistribution;
use bloomfilter::Bloom;
use rand_xoshiro::Xoshiro256StarStar;

fn make_rand(seed:u64,purpose:&str)->Xoshiro256StarStar{
    let long_seed = (0..4).flat_map(|i|{
        let mut h=DefaultHasher::new();
        i.hash(&mut h);
        seed.hash(&mut h);
        purpose.hash(&mut h);
        h.finish().to_le_bytes()
    }).collect::<Vec<u8>>();
    Xoshiro256StarStar::from_seed(long_seed.try_into().unwrap())
}

#[no_mangle]
pub unsafe extern "C" fn generate_zipf(seed: u64, key_count: u32, zipf_parameter: f64, data_out: *mut u32, count: u32) {
    let mut rng = make_rand(seed,"zipf");
    const SHUFFLE_SCALE: usize = 1 << 20;
    if key_count == 0 {
        assert_eq!(count, 0);
        return;
    }
    let generator = if zipf_parameter > 0.0 {
        Ok(ZipfDistribution::new(key_count as usize *  SHUFFLE_SCALE , zipf_parameter).unwrap())
    } else {
        Err(Uniform::new(0, key_count))
    };
    for i in 0..count as usize {
        data_out.add(i).write(match generator {
            Ok(zipf) => {
                // this modulo zipf sampling is described in the ycsb paper
                let mut hasher = DefaultHasher::default();
                hasher.write_usize(zipf.sample(&mut rng));
                let x = (hasher.finish() % key_count as u64) as u32;
                x
            }
            Err(uniform) => uniform.sample(&mut rng),
        });
    }
}

#[no_mangle]
pub unsafe extern "C" fn generate_workload_e(seed: u64, zipf_parameter: f64, base_key_count: u32, available_key_count: u32, op_count: u32, ops_out: *mut u32) {
    let insertions = op_count / 20;
    let post_insert_key_count = base_key_count + insertions;
    assert!(post_insert_key_count <= available_key_count);
    generate_zipf(seed, post_insert_key_count,zipf_parameter,ops_out.add(insertions as usize),op_count-insertions);
    for i in 0..insertions {
        ops_out.add(i as usize).write((base_key_count + i) | (1 << 31));
    }
    let out = from_raw_parts_mut(ops_out,op_count as usize);
    out.shuffle(&mut make_rand(seed,"work-e"));
}

#[no_mangle]
pub unsafe extern "C" fn generate_rng4(seed: u64, num_elements: u32, out: *mut u32) {
    let mut rng = make_rand(seed,"rng4");
    let num_elements = num_elements as usize;
    let mut bloom = Bloom::new_for_fp_rate_with_seed(num_elements, 0.01, &rng.gen());
    for i in 0..num_elements {
        let next = loop {
            let candidate: u32 = rng.gen();
            if !bloom.check(&candidate) {
                break candidate;
            }
        };
        bloom.set(&next);
        out.add(i).write(next);
    }
}


#[no_mangle]
pub unsafe extern "C" fn generate_rng8(seed: u64, num_elements: u32, out: *mut u64) {
    let mut rng = make_rand(seed,"rng8");
    let num_elements = num_elements as usize;
    let mut bloom = Bloom::new_for_fp_rate_with_seed(num_elements, 0.01, &rng.gen());
    for i in 0..num_elements {
        let next = loop {
            let candidate: u64 = rng.gen();
            if !bloom.check(&candidate) {
                break candidate;
            }
        };
        bloom.set(&next);
        out.add(i).write(next);
    }
}