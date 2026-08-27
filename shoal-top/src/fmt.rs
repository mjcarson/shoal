//! How a number is written on an axis or in a hover label
//!
//! # Why these are copies
//!
//! `shoal_bench::fmt` is the one place a float becomes text in the generated pages, and the
//! determinism those pages are checked for depends on it. The explorer cannot link that crate -
//! `shoal-bench` pulls `walkdir`, which does not build for `wasm32-unknown-unknown` - so the five
//! functions a chart needs are mirrored here.
//!
//! **Two implementations of the same rendering is a fork waiting to happen**, so
//! `shoal-bench/tests/explore_index.rs` asserts the two agree over a table of inputs covering every
//! branch of each. That test is the only thing keeping them honest; if one of these is changed, it
//! is changed in both places or the test fails.

/// Renders a float at a fixed number of decimal places
///
/// # Arguments
///
/// * `value` - The number to render
/// * `places` - How many decimal places to keep
///
/// # Examples
///
/// ```
/// use shoal_top::fmt::fixed;
///
/// assert_eq!(fixed(1.005, 2), "1.00");
/// assert_eq!(fixed(-0.001, 2), "0.00");
/// ```
pub fn fixed(value: f64, places: usize) -> String {
    // round at the requested precision first
    let rendered = format!("{value:.places$}");
    // then strip a sign that survived rounding to zero, which is an artifact of the input's sign
    // bit rather than a fact about the value
    if rendered.starts_with('-') && rendered[1..].chars().all(|ch| ch == '0' || ch == '.') {
        return rendered[1..].to_string();
    }
    rendered
}

/// Renders a duration in nanoseconds, picking the unit from the magnitude
///
/// # Arguments
///
/// * `nanos` - The duration, in nanoseconds
pub fn duration_ns(nanos: f64) -> String {
    // a negative or non finite duration is not a measurement, and printing one as if it scaled
    // would hide that. say so instead.
    if !nanos.is_finite() {
        return "-".to_string();
    }
    // pick the unit from the magnitude, keeping three significant-ish digits in each band
    let magnitude = nanos.abs();
    if magnitude < 1_000.0 {
        format!("{} ns", fixed(nanos, 2))
    } else if magnitude < 1_000_000.0 {
        format!("{} µs", fixed(nanos / 1_000.0, 2))
    } else if magnitude < 1_000_000_000.0 {
        format!("{} ms", fixed(nanos / 1_000_000.0, 2))
    } else {
        format!("{} s", fixed(nanos / 1_000_000_000.0, 3))
    }
}

/// Renders an integer with a separator every three digits
///
/// # Arguments
///
/// * `value` - The number to render
pub fn thousands(value: u128) -> String {
    // build the digits backwards, inserting a separator every three
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, ch) in digits.chars().enumerate() {
        // a separator goes before every digit whose distance from the end is a multiple of three
        if index > 0 && (digits.len() - index) % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

/// Renders a byte count for an axis tick
///
/// # Arguments
///
/// * `value` - The count, in bytes
pub fn bytes_axis(value: f64) -> String {
    /// The units, from smallest to largest
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    // a non finite value is not a width
    if !value.is_finite() {
        return "-".to_string();
    }
    let mut scaled = value;
    let mut unit = 0;
    // step up while there is a larger unit and the value is at least one of it
    while unit + 1 < UNITS.len() && scaled.abs() >= 1024.0 {
        scaled /= 1024.0;
        unit += 1;
    }
    // whole bytes are always whole, and a scaled value keeps one decimal only while it is small
    // enough for that decimal to mean anything
    let places = if unit == 0 || scaled.abs() >= 100.0 { 0 } else { 1 };
    let rendered = fixed(scaled, places);
    // a scaled value that landed on a whole number is written as one, so an axis of powers of two
    // reads `4 KiB` rather than `4.0 KiB`
    let rendered = match rendered.strip_suffix(".0") {
        Some(whole) => whole.to_string(),
        None => rendered,
    };
    format!("{rendered} {}", UNITS[unit])
}

/// Renders a byte rate
///
/// # Arguments
///
/// * `per_sec` - The rate, in bytes per second
pub fn byte_rate(per_sec: f64) -> String {
    /// The units, from smallest to largest
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    let mut value = per_sec;
    let mut unit = 0;
    // step up while there is a larger unit and the value is at least one of it
    while unit + 1 < UNITS.len() && value >= 1024.0 {
        value /= 1024.0;
        unit += 1;
    }
    // three significant figures, which is more than the measurement carries and less than a raw
    // float would print
    let places = if unit == 0 || value >= 100.0 { 0 } else { 1 };
    format!("{} {}/s", fixed(value, places), UNITS[unit])
}

/// Renders one value in the unit its metric is measured in
///
/// # Arguments
///
/// * `unit` - What the value is in
/// * `value` - The value itself
pub fn value(unit: crate::index::Unit, value: f64) -> String {
    use crate::index::Unit;

    // dispatch on the recorded unit rather than guessing from the magnitude, which is what stops a
    // sub microsecond rate from being rendered as a duration
    match unit {
        // the two rates are counted the same way and differ only in what is being counted, which
        // the axis label carries rather than every tick on it
        Unit::QueryRate | Unit::RowRate => {
            format!("{}/s", thousands(value.max(0.0).round() as u128))
        }
        Unit::ByteRate => byte_rate(value),
        Unit::Duration => duration_ns(value),
        // a spread is recorded already scaled to a percentage, so it is not multiplied again
        Unit::Percent => format!("{}%", fixed(value, 1)),
    }
}
