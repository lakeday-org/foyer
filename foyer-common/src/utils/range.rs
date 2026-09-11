// Copyright 2026 foyer Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::{Add, Bound, Range, RangeBounds, Sub};

mod private {

    pub trait ZeroOne {
        fn zero() -> Self;
        fn one() -> Self;
    }

    /// Non-panicking arithmetic helper for the numeric types supported by
    /// `ZeroOne`.
    ///
    /// Integers delegate to the checked arithmetic of the standard library,
    /// returning `None` on overflow/underflow. Floating-point numbers never
    /// overflow or wrap (IEEE-754); `safe_sub` returns `None` when the result
    /// would be negative, mirroring the integer semantics so that inverted
    /// ranges report an unrepresentable size instead of a misleading value.
    pub trait SafeArith: Sized {
        fn safe_add(self, other: Self) -> Option<Self>;
        fn safe_sub(self, other: Self) -> Option<Self>;
    }

    macro_rules! impl_one {
        ($($t:ty),*) => {
            $(
                impl ZeroOne for $t {
                    fn zero() -> Self {
                        0 as $t
                    }

                    fn one() -> Self {
                        1 as $t
                    }
                }
            )*
        };
    }

    macro_rules! impl_safe_arith_int {
        ($($t:ty),*) => {
            $(
                impl SafeArith for $t {
                    fn safe_add(self, other: Self) -> Option<Self> {
                        self.checked_add(other)
                    }

                    fn safe_sub(self, other: Self) -> Option<Self> {
                        self.checked_sub(other)
                    }
                }
            )*
        };
    }

    macro_rules! impl_safe_arith_float {
        ($($t:ty),*) => {
            $(
                impl SafeArith for $t {
                    fn safe_add(self, other: Self) -> Option<Self> {
                        Some(self + other)
                    }

                    fn safe_sub(self, other: Self) -> Option<Self> {
                        if self < other {
                            None
                        } else {
                            Some(self - other)
                        }
                    }
                }
            )*
        };
    }

    macro_rules! for_all_num_type {
        ($macro:ident) => {
            $macro! { u8, u16, u32, u64, usize, i8, i16, i32, i64, isize, f32, f64 }
        };
    }

    macro_rules! for_int_type {
        ($macro:ident) => {
            $macro! { u8, u16, u32, u64, usize, i8, i16, i32, i64, isize }
        };
    }

    macro_rules! for_float_type {
        ($macro:ident) => {
            $macro! { f32, f64 }
        };
    }

    for_all_num_type! { impl_one }
    for_int_type! { impl_safe_arith_int }
    for_float_type! { impl_safe_arith_float }
}

use private::{SafeArith, ZeroOne};

/// The range extensions.
pub trait RangeBoundsExt<
    T: PartialOrd<T> + Add<Output = T> + Sub<Output = T> + Clone + Copy + Send + Sync + 'static + ZeroOne + SafeArith,
>: RangeBounds<T>
{
    /// Get the start bound of the range.
    ///
    /// Returns `None` if the start is unbounded, or if normalizing an excluded
    /// start bound (`start + 1`) would overflow (for integer `T`).
    fn start(&self) -> Option<T> {
        match self.start_bound() {
            Bound::Included(v) => Some(*v),
            Bound::Excluded(v) => v.safe_add(ZeroOne::one()),
            Bound::Unbounded => None,
        }
    }

    /// Get the end bound of the range.
    ///
    /// Returns `None` if the end is unbounded, or if normalizing an inclusive
    /// end bound (`end + 1`) would overflow (for integer `T`).
    fn end(&self) -> Option<T> {
        match self.end_bound() {
            Bound::Included(v) => v.safe_add(ZeroOne::one()),
            Bound::Excluded(v) => Some(*v),
            Bound::Unbounded => None,
        }
    }

    /// Get the start bound with a default value of the range.
    fn start_with_bound(&self, bound: T) -> T {
        self.start().unwrap_or(bound)
    }

    /// Get the end bound with a default value of the range.
    fn end_with_bound(&self, bound: T) -> T {
        self.end().unwrap_or(bound)
    }

    /// Get the new range with the given range bounds.
    fn bounds(&self, range: Range<T>) -> Range<T> {
        let start = self.start_with_bound(range.start);
        let end = self.end_with_bound(range.end);
        start..end
    }

    /// Get the range size.
    ///
    /// Returns `None` if the size cannot be represented, i.e. when the range is
    /// unbounded on either side, inverted (`start > end`), or when an inclusive
    /// end at the maximum value of integer `T` would overflow the half-open
    /// boundary. Unlike plain `Add`/`Sub`, this never panics in debug nor wraps
    /// in release.
    fn size(&self) -> Option<T> {
        let start = self.start()?;
        let end = self.end()?;
        end.safe_sub(start)
    }

    /// Check if the range is empty.
    ///
    /// Inverted ranges (where the start bound is not strictly before the end
    /// bound) are reported as empty by comparing the raw bounds directly, so no
    /// arithmetic is performed and overflow cannot corrupt the result. Ranges
    /// with at least one unbounded side defer to [`size`](Self::size): such a
    /// range is empty only when its size is provably zero, which an unbounded
    /// side can never establish, so unbounded ranges are considered non-empty.
    fn is_empty(&self) -> bool {
        match (self.start_bound(), self.end_bound()) {
            (Bound::Included(s), Bound::Included(e)) => *s > *e,
            (Bound::Included(s), Bound::Excluded(e)) => *s >= *e,
            (Bound::Excluded(s), Bound::Included(e)) => *s >= *e,
            (Bound::Excluded(s), Bound::Excluded(e)) => *s >= *e,
            _ => match self.size() {
                Some(len) => len == ZeroOne::zero(),
                None => false,
            },
        }
    }

    /// Check is the range is a full range.
    fn is_full(&self) -> bool {
        self.start_bound() == Bound::Unbounded && self.end_bound() == Bound::Unbounded
    }

    /// Map the range with the given method.
    fn map<F, R>(&self, f: F) -> (Bound<R>, Bound<R>)
    where
        F: Fn(&T) -> R,
    {
        (self.start_bound().map(&f), self.end_bound().map(&f))
    }
}

impl<
    T: PartialOrd<T> + Add<Output = T> + Sub<Output = T> + Clone + Copy + Send + Sync + 'static + ZeroOne + SafeArith,
    RB: RangeBounds<T>,
> RangeBoundsExt<T> for RB
{
}

#[cfg(test)]
mod tests {
    #![expect(clippy::reversed_empty_ranges)]

    use super::*;

    #[test]
    fn test_start_excluded() {
        let r: (Bound<u32>, Bound<u32>) = (Bound::Excluded(3), Bound::Excluded(10));
        assert_eq!(r.start(), Some(4));
    }

    #[test]
    fn test_start_excluded_at_max_returns_none() {
        let r: (Bound<usize>, Bound<usize>) = (Bound::Excluded(usize::MAX), Bound::Unbounded);
        assert_eq!(r.start(), None);
    }

    #[test]
    fn test_end_inclusive_at_max_returns_none() {
        assert_eq!(RangeBoundsExt::end(&(0usize..=usize::MAX)), None);
        assert_eq!((..=usize::MAX).end(), None);
        let r: (Bound<usize>, Bound<usize>) = (Bound::Included(0), Bound::Included(usize::MAX));
        assert_eq!(r.end(), None);
    }

    #[test]
    fn test_size_normal() {
        assert_eq!((0u32..10).size(), Some(10));
        assert_eq!((0u32..=9).size(), Some(10));
        assert_eq!((3u32..7).size(), Some(4));
        assert_eq!((3u32..=7).size(), Some(5));
    }

    #[test]
    fn test_size_empty() {
        assert_eq!((5u32..5).size(), Some(0));
        assert_eq!((5u32..=5).size(), Some(1));
        assert_eq!((5i32..5).size(), Some(0));
    }

    #[test]
    fn test_size_inverted_returns_none() {
        assert_eq!((10u32..5).size(), None);
        assert_eq!((10u32..=5).size(), None);
        assert_eq!((10usize..5).size(), None);
        let r: (Bound<usize>, Bound<usize>) = (Bound::Included(10), Bound::Excluded(5));
        assert_eq!(r.size(), None);
    }

    #[test]
    fn test_size_whole_domain_returns_none() {
        assert_eq!((0usize..=usize::MAX).size(), None);
        assert_eq!((0u64..=u64::MAX).size(), None);
    }

    #[test]
    fn test_size_unbounded_returns_none() {
        assert_eq!((..10u32).size(), None);
        assert_eq!((5u32..).size(), None);
        let full = ..;
        assert_eq!(RangeBoundsExt::<u32>::size(&full), None);
    }

    #[test]
    fn test_size_signed() {
        assert_eq!((-5i32..5).size(), Some(10));
        assert_eq!((i32::MIN..=i32::MAX).size(), None);
    }

    #[test]
    fn test_size_floats() {
        assert_eq!((0.0f64..10.0).size(), Some(10.0));
        assert_eq!((0.0f32..=9.0).size(), Some(10.0));
        assert_eq!((10.0f64..5.0).size(), None);
        assert_eq!((10.0f64..=5.0).size(), None);
        assert_eq!((0.0f64..=f64::MAX).size(), Some(f64::MAX));
    }

    #[test]
    fn test_size_all_int_types() {
        assert_eq!((0u8..=u8::MAX).size(), None);
        assert_eq!((0u16..=u16::MAX).size(), None);
        assert_eq!((0u32..=u32::MAX).size(), None);
        assert_eq!((0u64..=u64::MAX).size(), None);
        assert_eq!((0usize..=usize::MAX).size(), None);
        assert_eq!((i8::MIN..=i8::MAX).size(), None);
        assert_eq!((i16::MIN..=i16::MAX).size(), None);
        assert_eq!((i32::MIN..=i32::MAX).size(), None);
        assert_eq!((i64::MIN..=i64::MAX).size(), None);
        assert_eq!((isize::MIN..=isize::MAX).size(), None);
        assert_eq!((0u8..10).size(), Some(10));
        assert_eq!((-3i8..3).size(), Some(6));
    }

    #[test]
    fn test_is_empty_basic() {
        assert!(RangeBoundsExt::is_empty(&(5u32..5)));
        assert!(!RangeBoundsExt::is_empty(&(5u32..=5)));
        assert!(RangeBoundsExt::is_empty(&(5u32..0)));
        assert!(!RangeBoundsExt::is_empty(&(0u32..10)));
    }

    #[test]
    fn test_is_empty_inverted_is_true() {
        assert!(RangeBoundsExt::is_empty(&(10u32..5)));
        assert!(RangeBoundsExt::is_empty(&(10u32..=5)));
        assert!(RangeBoundsExt::is_empty(&(10usize..5)));
    }

    #[test]
    fn test_is_empty_whole_domain_is_false() {
        assert!(!RangeBoundsExt::is_empty(&(0usize..=usize::MAX)));
        assert!(!RangeBoundsExt::is_empty(&(0u64..=u64::MAX)));
    }

    #[test]
    fn test_is_empty_matches_std_inherent() {
        for (s, e) in [(0u32, 0), (0, 1), (5, 5), (5, 6), (10, 5), (10, 10)] {
            let r = s..e;
            assert_eq!(RangeBoundsExt::is_empty(&r), r.is_empty(), "Range {s}..{e} mismatch");
        }
        for (s, e) in [(0u32, 0), (5, 5), (5, 6), (10, 5), (10, 10)] {
            let r = s..=e;
            assert_eq!(RangeBoundsExt::is_empty(&r), r.is_empty(), "Range {s}..={e} mismatch");
        }
    }

    #[test]
    fn test_with_bound_overflow_uses_default() {
        assert_eq!((5u32..10).start_with_bound(0), 5);
        assert_eq!((..10u32).start_with_bound(0), 0);
        assert_eq!((5u32..=9).end_with_bound(100), 10);
        assert_eq!((0usize..=usize::MAX).end_with_bound(usize::MAX), usize::MAX);
        let r: (Bound<usize>, Bound<usize>) = (Bound::Excluded(usize::MAX), Bound::Unbounded);
        assert_eq!(r.start_with_bound(7), 7);
    }

    #[test]
    fn test_bounds() {
        assert_eq!((3u32..7).bounds(0..10), 3..7);
        assert_eq!((..7u32).bounds(0..10), 0..7);
        assert_eq!((3u32..).bounds(0..10), 3..10);
        assert_eq!((0usize..=usize::MAX).bounds(0..usize::MAX), 0..usize::MAX);
    }
}
