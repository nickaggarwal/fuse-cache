//! Deterministic file content shared with `tools/readbench` (Go): the byte at
//! absolute offset `o` is byte `o % 8` (little-endian) of `splitmix64(o / 8)`.
//! Any range read by either implementation can be verified without a
//! reference copy.

#[inline]
pub fn splitmix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

pub fn fill(buf: &mut [u8], off: u64) {
    let mut i = 0usize;
    while i < buf.len() {
        let o = off + i as u64;
        let v = splitmix64(o >> 3).to_le_bytes();
        let mut k = (o & 7) as usize;
        while k < 8 && i < buf.len() {
            buf[i] = v[k];
            i += 1;
            k += 1;
        }
    }
}

/// Returns the first mismatching absolute offset, if any.
pub fn check(buf: &[u8], off: u64) -> Option<u64> {
    let mut i = 0usize;
    while i < buf.len() {
        let o = off + i as u64;
        let v = splitmix64(o >> 3).to_le_bytes();
        let mut k = (o & 7) as usize;
        while k < 8 && i < buf.len() {
            if buf[i] != v[k] {
                return Some(off + i as u64);
            }
            i += 1;
            k += 1;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fill_is_offset_pure() {
        let mut whole = vec![0u8; 100];
        fill(&mut whole, 1000);
        for start in 0..100 {
            let mut part = vec![0u8; 100 - start];
            fill(&mut part, 1000 + start as u64);
            assert_eq!(&part[..], &whole[start..]);
        }
        assert_eq!(check(&whole, 1000), None);
        whole[37] ^= 1;
        assert_eq!(check(&whole, 1000), Some(1037));
    }

    #[test]
    fn known_vector_matches_go() {
        // splitmix64(0) and (1) from the reference implementation; Go's
        // readbench uses the same constants.
        assert_eq!(splitmix64(0), 0xe220_a839_7b1d_cdaf);
        assert_eq!(splitmix64(1), 0x910a_2dec_8902_5cc1);
        let mut b = [0u8; 8];
        fill(&mut b, 0);
        assert_eq!(b, 0xe220_a839_7b1d_cdafu64.to_le_bytes());
    }
}
