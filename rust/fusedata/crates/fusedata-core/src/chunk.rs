//! Chunk addressing, matching `internal/cache/cache.go` exactly.
//!
//! A chunked file of `size` bytes written with stride `S` is stored as
//! `<path>_chunk_0 .. <path>_chunk_{N-1}` where `N = ceil(size/S)`, every chunk
//! but the last is exactly `S` bytes, and chunk `i` holds bytes
//! `[i*S, min((i+1)*S, size))` of the logical file.
//!
//! The functions here are ports, not reinterpretations: the Go reader must
//! agree with this one about what "chunk N" contains, including when metadata
//! carries a stride that does not match the bytes on disk.

/// The suffix separating a parent path from its chunk index.
pub const CHUNK_SEP: &str = "_chunk_";

/// Stride to address `entry_chunk_size` with: the size the file was written
/// with when known (> 0), else the node's configured size. Port of
/// `effectiveChunkSize`.
pub fn effective_chunk_size(entry_chunk_size: i64, configured: i64) -> i64 {
    if entry_chunk_size > 0 {
        entry_chunk_size
    } else {
        configured
    }
}

/// Number of chunks a file of `size` splits into at `chunk_size`. Port of
/// `chunkCountFor`; non-positive inputs yield 0.
pub fn chunk_count_for(size: i64, chunk_size: i64) -> i64 {
    if size <= 0 || chunk_size <= 0 {
        return 0;
    }
    (size + chunk_size - 1) / chunk_size
}

/// Bound `[from, to)` to a valid sub-slice of a chunk of length `n`. Port of
/// `clampChunkSlice`: both ends are clamped, so a wrong stride that pushes
/// `from` past the end yields an empty slice instead of a panic.
pub fn clamp_chunk_slice(mut from: i64, mut to: i64, n: i64) -> (i64, i64) {
    if from < 0 {
        from = 0;
    }
    if from > n {
        from = n;
    }
    if to > n {
        to = n;
    }
    if to < from {
        to = from;
    }
    (from, to)
}

/// `"<parent>_chunk_<idx>"`, the on-disk / object name of one chunk.
pub fn chunk_path(parent: &str, idx: i64) -> String {
    format!("{parent}{CHUNK_SEP}{idx}")
}

/// Split a chunk path into `(parent, index)`. Port of
/// `parentFilePathFromChunkPath` + index parse: the separator must not be at
/// position 0 and the suffix must parse as an i64 (Go's `strconv.ParseInt`
/// accepts a leading sign, so "-1" parses; callers reject negatives).
pub fn parse_chunk_path(path: &str) -> Option<(&str, i64)> {
    let idx = path.rfind(CHUNK_SEP)?;
    if idx == 0 {
        return None;
    }
    let suffix = &path[idx + CHUNK_SEP.len()..];
    let n: i64 = parse_go_int(suffix)?;
    Some((&path[..idx], n))
}

/// Go `strconv.ParseInt(s, 10, 64)`: optional sign, at least one digit, no
/// whitespace, no underscores.
fn parse_go_int(s: &str) -> Option<i64> {
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// One piece of a logical range read: which chunk, and which bytes of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkSpan {
    pub index: i64,
    /// Offset within the chunk where this span starts (unclamped).
    pub from: i64,
    /// Offset within the chunk where this span ends (unclamped).
    pub to: i64,
}

/// Plan a read of `[offset, offset+size)` from a chunked file of `total` bytes
/// at `stride`, the way `ReadRange` does: clip the end at `total`, then walk
/// `offset/stride ..= (end-1)/stride`. Spans are pre-clamp; the caller clamps
/// each against the actual chunk length with [`clamp_chunk_slice`], which is
/// what makes a stride/disk mismatch return short data rather than panic.
pub fn plan_range(total: i64, stride: i64, offset: i64, size: i64) -> Vec<ChunkSpan> {
    let mut out = Vec::new();
    if size <= 0 || stride <= 0 || offset < 0 || offset >= total {
        return out;
    }
    let end = (offset + size).min(total);
    let start_chunk = offset / stride;
    let end_chunk = (end - 1) / stride;
    for index in start_chunk..=end_chunk {
        let chunk_off = index * stride;
        let from = (offset - chunk_off).max(0);
        // Go: `to = len(chunk)`; `if end < chunkEnd { to = end - chunkOffset }`.
        // Expressed without the chunk length so the caller's clamp applies.
        let to = end - chunk_off;
        out.push(ChunkSpan { index, from, to });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effective_chunk_size_matches_go() {
        assert_eq!(effective_chunk_size(0, 8), 8);
        assert_eq!(effective_chunk_size(-3, 8), 8);
        assert_eq!(effective_chunk_size(4, 8), 4);
    }

    #[test]
    fn chunk_count_matches_go_table() {
        // Same table as TestEffectiveChunkSizeAndChunkCount in chunk_stride_test.go.
        for (size, stride, want) in [(0, 4, 0), (1, 4, 1), (4, 4, 1), (5, 4, 2), (24, 4, 6), (24, 0, 0), (-1, 4, 0)] {
            assert_eq!(chunk_count_for(size, stride), want, "chunkCountFor({size},{stride})");
        }
    }

    #[test]
    fn clamp_matches_go_table() {
        for (from, to, n, wf, wt) in [
            (0, 4, 4, 0, 4),
            (5, 2, 4, 4, 4),  // from past end
            (2, 9, 4, 2, 4),  // to past end
            (-1, 3, 4, 0, 3), // negative from
            (3, 1, 4, 3, 3),  // inverted
        ] {
            assert_eq!(clamp_chunk_slice(from, to, n), (wf, wt), "clamp({from},{to},{n})");
        }
    }

    #[test]
    fn clamp_never_produces_invalid_slice() {
        // Exhaustive over a small domain: the result is always a valid
        // sub-slice of a buffer of length n, which is the panic-freedom
        // property the Go comment is about.
        for n in 0..6 {
            for from in -3..10 {
                for to in -3..10 {
                    let (f, t) = clamp_chunk_slice(from, to, n);
                    assert!(0 <= f && f <= t && t <= n, "clamp({from},{to},{n}) = ({f},{t})");
                }
            }
        }
    }

    #[test]
    fn chunk_path_round_trip() {
        assert_eq!(chunk_path("/a/b.bin", 7), "/a/b.bin_chunk_7");
        assert_eq!(parse_chunk_path("/a/b.bin_chunk_7"), Some(("/a/b.bin", 7)));
        // Last separator wins (a file whose name itself contains _chunk_).
        assert_eq!(parse_chunk_path("/x_chunk_1/y_chunk_2"), Some(("/x_chunk_1/y", 2)));
        assert_eq!(parse_chunk_path("/a_chunk_"), None);
        assert_eq!(parse_chunk_path("/a_chunk_x"), None);
        assert_eq!(parse_chunk_path("/a_chunk_1 "), None);
        assert_eq!(parse_chunk_path("_chunk_3"), None); // idx <= 0 in Go
        assert_eq!(parse_chunk_path("/a_chunk_-1"), Some(("/a", -1)));
        assert_eq!(parse_chunk_path("/a"), None);
    }

    #[test]
    fn plan_simple() {
        // 10 bytes, stride 4: chunks [0,4) [4,8) [8,10).
        assert_eq!(plan_range(10, 4, 0, 4), vec![ChunkSpan { index: 0, from: 0, to: 4 }]);
        assert_eq!(
            plan_range(10, 4, 3, 6),
            vec![
                ChunkSpan { index: 0, from: 3, to: 9 },
                ChunkSpan { index: 1, from: 0, to: 5 },
                ChunkSpan { index: 2, from: 0, to: 1 },
            ]
        );
        // offset 3 size 6 -> end 9: chunk0 [3,9)->clamped [3,4), chunk1 [0,5)->[0,4), chunk2 [0,1).
        let p = plan_range(10, 4, 3, 6);
        assert_eq!(p.len(), 3);
        let lens = [4, 4, 2];
        let got: Vec<(i64, i64)> = p.iter().map(|s| clamp_chunk_slice(s.from, s.to, lens[s.index as usize])).collect();
        assert_eq!(got, vec![(3, 4), (0, 4), (0, 1)]);
        // Read past EOF clips; read at EOF is empty.
        assert_eq!(plan_range(10, 4, 8, 100).len(), 1);
        assert!(plan_range(10, 4, 10, 1).is_empty());
        assert!(plan_range(10, 4, 0, 0).is_empty());
        assert!(plan_range(10, 0, 0, 4).is_empty());
    }

    #[test]
    fn plan_with_wrong_stride_clamps_instead_of_panicking() {
        // File written at stride 4 (chunks of len 4,4,2) but metadata says 8.
        // Go computes chunkOffset from the wrong stride; chunk 1 at offset
        // 8..10 maps to from=0..2 within a 4-byte chunk on disk, and a read
        // starting at 13 in a "16 byte" file would land past the end.
        let lens = [4i64, 4, 2];
        for off in 0..16 {
            for size in 1..10 {
                for s in plan_range(16, 8, off, size) {
                    let n = *lens.get(s.index as usize).unwrap_or(&0);
                    let (f, t) = clamp_chunk_slice(s.from, s.to, n);
                    assert!(0 <= f && f <= t && t <= n);
                }
            }
        }
    }
}
