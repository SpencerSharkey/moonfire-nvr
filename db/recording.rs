// This file is part of Moonfire NVR, a security camera network video recorder.
// Copyright (C) 2016 The Moonfire NVR Authors
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// In addition, as a special exception, the copyright holders give
// permission to link the code of portions of this program with the
// OpenSSL library under certain conditions as described in each
// individual source file, and distribute linked combinations including
// the two.
//
// You must obey the GNU General Public License in all respects for all
// of the code used other than OpenSSL. If you modify file(s) with this
// exception, you may extend this exception to your version of the
// file(s), but you are not obligated to do so. If you do not wish to do
// so, delete this exception statement from your version. If you delete
// this exception statement from all source files in the program, then
// also delete it here.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use crate::coding::{append_varint32, decode_varint32, unzigzag32, zigzag32};
use crate::db;
use failure::{bail, Error};
use log::trace;
use std::ops::Range;

pub use base::time::TIME_UNITS_PER_SEC;

pub const DESIRED_RECORDING_DURATION: i64 = 60 * TIME_UNITS_PER_SEC;
pub const MAX_RECORDING_DURATION: i64 = 5 * 60 * TIME_UNITS_PER_SEC;

pub use base::time::Duration;
pub use base::time::Time;

/// An iterator through a sample index.
/// Initially invalid; call `next()` before each read.
#[derive(Clone, Copy, Debug)]
pub struct SampleIndexIterator {
    /// The index byte position of the next sample to read (low 31 bits) and if the current
    /// same is a key frame (high bit).
    i_and_is_key: u32,

    /// The starting data byte position of this sample within the segment.
    pub pos: i32,

    /// The starting time of this sample within the segment (in 90 kHz units).
    pub start_90k: i32,

    /// The duration of this sample (in 90 kHz units).
    pub duration_90k: i32,

    /// The byte length of this frame.
    pub bytes: i32,

    /// The byte length of the last frame of the "other" type: if this one is key, the last
    /// non-key; if this one is non-key, the last key.
    bytes_other: i32,
}

impl SampleIndexIterator {
    pub fn new() -> SampleIndexIterator {
        SampleIndexIterator {
            i_and_is_key: 0,
            pos: 0,
            start_90k: 0,
            duration_90k: 0,
            bytes: 0,
            bytes_other: 0,
        }
    }

    pub fn next(&mut self, data: &[u8]) -> Result<bool, Error> {
        self.pos += self.bytes;
        self.start_90k += self.duration_90k;
        let i = (self.i_and_is_key & 0x7FFF_FFFF) as usize;
        if i == data.len() {
            return Ok(false);
        }
        let (raw1, i1) = match decode_varint32(data, i) {
            Ok(tuple) => tuple,
            Err(()) => bail!("bad varint 1 at offset {}", i),
        };
        let (raw2, i2) = match decode_varint32(data, i1) {
            Ok(tuple) => tuple,
            Err(()) => bail!("bad varint 2 at offset {}", i1),
        };
        let duration_90k_delta = unzigzag32(raw1 >> 1);
        self.duration_90k += duration_90k_delta;
        if self.duration_90k < 0 {
            bail!(
                "negative duration {} after applying delta {}",
                self.duration_90k,
                duration_90k_delta
            );
        }
        if self.duration_90k == 0 && data.len() > i2 {
            bail!(
                "zero duration only allowed at end; have {} bytes left",
                data.len() - i2
            );
        }
        let (prev_bytes_key, prev_bytes_nonkey) = match self.is_key() {
            true => (self.bytes, self.bytes_other),
            false => (self.bytes_other, self.bytes),
        };
        self.i_and_is_key = (i2 as u32) | (((raw1 & 1) as u32) << 31);
        let bytes_delta = unzigzag32(raw2);
        if self.is_key() {
            self.bytes = prev_bytes_key + bytes_delta;
            self.bytes_other = prev_bytes_nonkey;
        } else {
            self.bytes = prev_bytes_nonkey + bytes_delta;
            self.bytes_other = prev_bytes_key;
        }
        if self.bytes <= 0 {
            bail!(
                "non-positive bytes {} after applying delta {} to key={} frame at ts {}",
                self.bytes,
                bytes_delta,
                self.is_key(),
                self.start_90k
            );
        }
        Ok(true)
    }

    pub fn uninitialized(&self) -> bool {
        self.i_and_is_key == 0
    }
    pub fn is_key(&self) -> bool {
        (self.i_and_is_key & 0x8000_0000) != 0
    }
}

#[derive(Debug)]
pub struct SampleIndexEncoder {
    prev_duration_90k: i32,
    prev_bytes_key: i32,
    prev_bytes_nonkey: i32,
}

impl SampleIndexEncoder {
    pub fn new() -> Self {
        SampleIndexEncoder {
            prev_duration_90k: 0,
            prev_bytes_key: 0,
            prev_bytes_nonkey: 0,
        }
    }

    pub fn add_sample(
        &mut self,
        duration_90k: i32,
        bytes: i32,
        is_key: bool,
        r: &mut db::RecordingToInsert,
    ) -> Result<(), Error> {
        let duration_delta = duration_90k - self.prev_duration_90k;
        self.prev_duration_90k = duration_90k;
        let new_duration_90k = r.duration_90k + duration_90k;
        if new_duration_90k as i64 > MAX_RECORDING_DURATION {
            bail!(
                "Duration {} exceeds maximum {}",
                new_duration_90k,
                MAX_RECORDING_DURATION
            );
        }
        r.duration_90k += duration_90k;
        r.sample_file_bytes += bytes;
        r.video_samples += 1;
        let bytes_delta = bytes
            - if is_key {
                let prev = self.prev_bytes_key;
                r.video_sync_samples += 1;
                self.prev_bytes_key = bytes;
                prev
            } else {
                let prev = self.prev_bytes_nonkey;
                self.prev_bytes_nonkey = bytes;
                prev
            };
        append_varint32(
            (zigzag32(duration_delta) << 1) | (is_key as u32),
            &mut r.video_index,
        );
        append_varint32(zigzag32(bytes_delta), &mut r.video_index);
        Ok(())
    }
}

/// A segment represents a view of some or all of a single recording, starting from a key frame.
/// Used by the `Mp4FileBuilder` class to splice together recordings into a single virtual .mp4.
#[derive(Debug)]
pub struct Segment {
    pub id: db::CompositeId,
    pub open_id: u32,
    pub start: Time,

    /// An iterator positioned at the beginning of the segment, or `None`. Most segments are
    /// positioned at the beginning of the recording, so this is an optional box to shrink a long
    /// of segments. `None` is equivalent to `SampleIndexIterator::new()`.
    begin: Option<Box<SampleIndexIterator>>,
    pub file_end: i32,
    pub desired_range_90k: Range<i32>,
    pub frames: u16,
    pub key_frames: u16,
    video_sample_entry_id_and_trailing_zero: i32,
}

impl Segment {
    /// Creates a segment.
    ///
    /// `desired_range_90k` represents the desired range of the segment relative to the start of
    /// the recording. The actual range will start at the first key frame at or before the
    /// desired start time. (The caller is responsible for creating an edit list to skip the
    /// undesired portion.) It will end at the first frame after the desired range (unless the
    /// desired range extends beyond the recording). (Likewise, the caller is responsible for
    /// trimming the final frame's duration if desired.)
    pub fn new(
        db: &db::LockedDatabase,
        recording: &db::ListRecordingsRow,
        desired_range_90k: Range<i32>,
    ) -> Result<Segment, Error> {
        let mut self_ = Segment {
            id: recording.id,
            open_id: recording.open_id,
            start: recording.start,
            begin: None,
            file_end: recording.sample_file_bytes,
            desired_range_90k: desired_range_90k,
            frames: recording.video_samples as u16,
            key_frames: recording.video_sync_samples as u16,
            video_sample_entry_id_and_trailing_zero: recording.video_sample_entry_id
                | ((((recording.flags & db::RecordingFlags::TrailingZero as i32) != 0) as i32)
                    << 31),
        };

        if self_.desired_range_90k.start > self_.desired_range_90k.end
            || self_.desired_range_90k.end > recording.duration_90k
        {
            bail!(
                "desired range [{}, {}) invalid for recording of length {}",
                self_.desired_range_90k.start,
                self_.desired_range_90k.end,
                recording.duration_90k
            );
        }

        if self_.desired_range_90k.start == 0
            && self_.desired_range_90k.end == recording.duration_90k
        {
            // Fast path. Existing entry is fine.
            trace!(
                "recording::Segment::new fast path, recording={:#?}",
                recording
            );
            return Ok(self_);
        }

        // Slow path. Need to iterate through the index.
        trace!(
            "recording::Segment::new slow path, desired_range_90k={:?}, recording={:#?}",
            self_.desired_range_90k,
            recording
        );
        db.with_recording_playback(self_.id, &mut |playback| {
            let mut begin = Box::new(SampleIndexIterator::new());
            let data = &(&playback).video_index;
            let mut it = SampleIndexIterator::new();
            if !it.next(data)? {
                bail!("no index");
            }
            if !it.is_key() {
                bail!("not key frame");
            }

            // Stop when hitting a frame with this start time.
            // Going until the end of the recording is special-cased because there can be a trailing
            // frame of zero duration. It's unclear exactly how this should be handled, but let's
            // include it for consistency with the fast path. It'd be bizarre to have it included or
            // not based on desired_range_90k.start.
            let end_90k = if self_.desired_range_90k.end == recording.duration_90k {
                i32::max_value()
            } else {
                self_.desired_range_90k.end
            };

            loop {
                if it.start_90k <= self_.desired_range_90k.start && it.is_key() {
                    // new start candidate.
                    *begin = it;
                    self_.frames = 0;
                    self_.key_frames = 0;
                }
                if it.start_90k >= end_90k && self_.frames > 0 {
                    break;
                }
                self_.frames += 1;
                self_.key_frames += it.is_key() as u16;
                if !it.next(data)? {
                    break;
                }
            }
            self_.begin = Some(begin);
            self_.file_end = it.pos;
            self_.video_sample_entry_id_and_trailing_zero =
                recording.video_sample_entry_id | (((it.duration_90k == 0) as i32) << 31);
            Ok(())
        })?;
        Ok(self_)
    }

    pub fn video_sample_entry_id(&self) -> i32 {
        self.video_sample_entry_id_and_trailing_zero & 0x7FFFFFFF
    }

    pub fn have_trailing_zero(&self) -> bool {
        self.video_sample_entry_id_and_trailing_zero < 0
    }

    /// Returns the byte range within the sample file of data associated with this segment.
    pub fn sample_file_range(&self) -> Range<u64> {
        self.begin.as_ref().map(|b| b.pos as u64).unwrap_or(0)..self.file_end as u64
    }

    /// Returns the actual start time as described in `new`.
    pub fn actual_start_90k(&self) -> i32 {
        self.begin.as_ref().map(|b| b.start_90k).unwrap_or(0)
    }

    /// Iterates through each frame in the segment.
    /// Must be called without the database lock held; retrieves video index from the cache.
    pub fn foreach<F>(&self, playback: &db::RecordingPlayback, mut f: F) -> Result<(), Error>
    where
        F: FnMut(&SampleIndexIterator) -> Result<(), Error>,
    {
        trace!(
            "foreach on recording {}: {} frames, actual_start_90k: {}",
            self.id,
            self.frames,
            self.actual_start_90k()
        );
        let data = &(&playback).video_index;
        let mut it = match self.begin {
            Some(ref b) => **b,
            None => SampleIndexIterator::new(),
        };
        if it.uninitialized() {
            if !it.next(data)? {
                bail!("recording {}: no frames", self.id);
            }
            if !it.is_key() {
                bail!("recording {}: doesn't start with key frame", self.id);
            }
        }
        let mut have_frame = true;
        let mut key_frame = 0;

        for i in 0..self.frames {
            if !have_frame {
                bail!(
                    "recording {}: expected {} frames, found only {}",
                    self.id,
                    self.frames,
                    i + 1
                );
            }
            if it.is_key() {
                key_frame += 1;
                if key_frame > self.key_frames {
                    bail!(
                        "recording {}: more than expected {} key frames",
                        self.id,
                        self.key_frames
                    );
                }
            }

            // Note: this inner loop avoids ? for performance. Don't change these lines without
            // reading https://github.com/rust-lang/rust/issues/37939 and running
            // mp4::bench::build_index.
            if let Err(e) = f(&it) {
                return Err(e);
            }
            have_frame = match it.next(data) {
                Err(e) => return Err(e),
                Ok(hf) => hf,
            };
        }
        if key_frame < self.key_frames {
            bail!(
                "recording {}: expected {} key frames, found only {}",
                self.id,
                self.key_frames,
                key_frame
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{self, TestDb};
    use base::clock::RealClocks;

    /// Tests encoding the example from design/schema.md.
    #[test]
    fn test_encode_example() {
        testutil::init();
        let mut r = db::RecordingToInsert::default();
        let mut e = SampleIndexEncoder::new();
        e.add_sample(10, 1000, true, &mut r).unwrap();
        e.add_sample(9, 10, false, &mut r).unwrap();
        e.add_sample(11, 15, false, &mut r).unwrap();
        e.add_sample(10, 12, false, &mut r).unwrap();
        e.add_sample(10, 1050, true, &mut r).unwrap();
        assert_eq!(
            r.video_index,
            b"\x29\xd0\x0f\x02\x14\x08\x0a\x02\x05\x01\x64"
        );
        assert_eq!(10 + 9 + 11 + 10 + 10, r.duration_90k);
        assert_eq!(5, r.video_samples);
        assert_eq!(2, r.video_sync_samples);
    }

    /// Tests a round trip from `SampleIndexEncoder` to `SampleIndexIterator`.
    #[test]
    fn test_round_trip() {
        testutil::init();
        #[derive(Debug, PartialEq, Eq)]
        struct Sample {
            duration_90k: i32,
            bytes: i32,
            is_key: bool,
        }
        let samples = [
            Sample {
                duration_90k: 10,
                bytes: 30000,
                is_key: true,
            },
            Sample {
                duration_90k: 9,
                bytes: 1000,
                is_key: false,
            },
            Sample {
                duration_90k: 11,
                bytes: 1100,
                is_key: false,
            },
            Sample {
                duration_90k: 18,
                bytes: 31000,
                is_key: true,
            },
            Sample {
                duration_90k: 0,
                bytes: 1000,
                is_key: false,
            },
        ];
        let mut r = db::RecordingToInsert::default();
        let mut e = SampleIndexEncoder::new();
        for sample in &samples {
            e.add_sample(sample.duration_90k, sample.bytes, sample.is_key, &mut r)
                .unwrap();
        }
        let mut it = SampleIndexIterator::new();
        for sample in &samples {
            assert!(it.next(&r.video_index).unwrap());
            assert_eq!(
                sample,
                &Sample {
                    duration_90k: it.duration_90k,
                    bytes: it.bytes,
                    is_key: it.is_key()
                }
            );
        }
        assert!(!it.next(&r.video_index).unwrap());
    }

    /// Tests that `SampleIndexIterator` spots several classes of errors.
    /// TODO: test and fix overflow cases.
    #[test]
    fn test_iterator_errors() {
        testutil::init();
        struct Test {
            encoded: &'static [u8],
            err: &'static str,
        }
        let tests = [
            Test {
                encoded: b"\x80",
                err: "bad varint 1 at offset 0",
            },
            Test {
                encoded: b"\x00\x80",
                err: "bad varint 2 at offset 1",
            },
            Test {
                encoded: b"\x00\x02\x00\x00",
                err: "zero duration only allowed at end; have 2 bytes left",
            },
            Test {
                encoded: b"\x02\x02",
                err: "negative duration -1 after applying delta -1",
            },
            Test {
                encoded: b"\x04\x00",
                err: "non-positive bytes 0 after applying delta 0 to key=false frame at ts 0",
            },
        ];
        for test in &tests {
            let mut it = SampleIndexIterator::new();
            assert_eq!(it.next(test.encoded).unwrap_err().to_string(), test.err);
        }
    }

    fn get_frames<F, T>(db: &db::Database, segment: &Segment, f: F) -> Vec<T>
    where
        F: Fn(&SampleIndexIterator) -> T,
    {
        let mut v = Vec::new();
        db.lock()
            .with_recording_playback(segment.id, &mut |playback| {
                segment.foreach(playback, |it| {
                    v.push(f(it));
                    Ok(())
                })
            })
            .unwrap();
        v
    }

    /// Tests that a `Segment` correctly can clip at the beginning and end.
    /// This is a simpler case; all sync samples means we can start on any frame.
    #[test]
    fn test_segment_clipping_with_all_sync() {
        testutil::init();
        let mut r = db::RecordingToInsert::default();
        let mut encoder = SampleIndexEncoder::new();
        for i in 1..6 {
            let duration_90k = 2 * i;
            let bytes = 3 * i;
            encoder
                .add_sample(duration_90k, bytes, true, &mut r)
                .unwrap();
        }
        let db = TestDb::new(RealClocks {});
        let row = db.insert_recording_from_encoder(r);
        // Time range [2, 2 + 4 + 6 + 8) means the 2nd, 3rd, 4th samples should be
        // included.
        let segment = Segment::new(&db.db.lock(), &row, 2..2 + 4 + 6 + 8).unwrap();
        assert_eq!(
            &get_frames(&db.db, &segment, |it| it.duration_90k),
            &[4, 6, 8]
        );
    }

    /// Half sync frames means starting from the last sync frame <= desired point.
    #[test]
    fn test_segment_clipping_with_half_sync() {
        testutil::init();
        let mut r = db::RecordingToInsert::default();
        let mut encoder = SampleIndexEncoder::new();
        for i in 1..6 {
            let duration_90k = 2 * i;
            let bytes = 3 * i;
            encoder
                .add_sample(duration_90k, bytes, (i % 2) == 1, &mut r)
                .unwrap();
        }
        let db = TestDb::new(RealClocks {});
        let row = db.insert_recording_from_encoder(r);
        // Time range [2 + 4 + 6, 2 + 4 + 6 + 8) means the 4th sample should be included.
        // The 3rd also gets pulled in because it is a sync frame and the 4th is not.
        let segment = Segment::new(&db.db.lock(), &row, 2 + 4 + 6..2 + 4 + 6 + 8).unwrap();
        assert_eq!(&get_frames(&db.db, &segment, |it| it.duration_90k), &[6, 8]);
    }

    #[test]
    fn test_segment_clipping_with_trailing_zero() {
        testutil::init();
        let mut r = db::RecordingToInsert::default();
        let mut encoder = SampleIndexEncoder::new();
        encoder.add_sample(1, 1, true, &mut r).unwrap();
        encoder.add_sample(1, 2, true, &mut r).unwrap();
        encoder.add_sample(0, 3, true, &mut r).unwrap();
        let db = TestDb::new(RealClocks {});
        let row = db.insert_recording_from_encoder(r);
        let segment = Segment::new(&db.db.lock(), &row, 1..2).unwrap();
        assert_eq!(&get_frames(&db.db, &segment, |it| it.bytes), &[2, 3]);
    }

    /// Even if the desired duration is 0, there should still be a frame.
    #[test]
    fn test_segment_zero_desired_duration() {
        testutil::init();
        let mut r = db::RecordingToInsert::default();
        let mut encoder = SampleIndexEncoder::new();
        encoder.add_sample(1, 1, true, &mut r).unwrap();
        let db = TestDb::new(RealClocks {});
        let row = db.insert_recording_from_encoder(r);
        let segment = Segment::new(&db.db.lock(), &row, 0..0).unwrap();
        assert_eq!(&get_frames(&db.db, &segment, |it| it.bytes), &[1]);
    }

    /// Test a `Segment` which uses the whole recording.
    /// This takes a fast path which skips scanning the index in `new()`.
    #[test]
    fn test_segment_fast_path() {
        testutil::init();
        let mut r = db::RecordingToInsert::default();
        let mut encoder = SampleIndexEncoder::new();
        for i in 1..6 {
            let duration_90k = 2 * i;
            let bytes = 3 * i;
            encoder
                .add_sample(duration_90k, bytes, (i % 2) == 1, &mut r)
                .unwrap();
        }
        let db = TestDb::new(RealClocks {});
        let row = db.insert_recording_from_encoder(r);
        let segment = Segment::new(&db.db.lock(), &row, 0..2 + 4 + 6 + 8 + 10).unwrap();
        assert_eq!(
            &get_frames(&db.db, &segment, |it| it.duration_90k),
            &[2, 4, 6, 8, 10]
        );
    }

    #[test]
    fn test_segment_fast_path_with_trailing_zero() {
        testutil::init();
        let mut r = db::RecordingToInsert::default();
        let mut encoder = SampleIndexEncoder::new();
        encoder.add_sample(1, 1, true, &mut r).unwrap();
        encoder.add_sample(1, 2, true, &mut r).unwrap();
        encoder.add_sample(0, 3, true, &mut r).unwrap();
        let db = TestDb::new(RealClocks {});
        let row = db.insert_recording_from_encoder(r);
        let segment = Segment::new(&db.db.lock(), &row, 0..2).unwrap();
        assert_eq!(&get_frames(&db.db, &segment, |it| it.bytes), &[1, 2, 3]);
    }

    // TODO: test segment error cases involving mismatch between row frames/key_frames and index.
}

#[cfg(all(test, feature = "nightly"))]
mod bench {
    extern crate test;

    use super::*;

    /// Benchmarks the decoder, which is performance-critical for .mp4 serving.
    #[bench]
    fn bench_decoder(b: &mut test::Bencher) {
        let data = include_bytes!("testdata/video_sample_index.bin");
        b.bytes = data.len() as u64;
        b.iter(|| {
            let mut it = SampleIndexIterator::new();
            while it.next(data).unwrap() {}
            assert_eq!(30104460, it.pos);
            assert_eq!(5399985, it.start_90k);
        });
    }
}
