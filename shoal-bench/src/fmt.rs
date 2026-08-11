//! Turning numbers into the strings that go in a table or a chart label
//!
//! Every float that reaches a page goes through this module. That is not a style preference: the
//! generated page is committed and checked with `shoal-bench render --check`, so a number
//! formatted with `{:?}` - which prints as many digits as it takes to round trip, and can differ
//! between two values that are equal for every purpose the page has - would make the check fail on
//! a tree nobody touched.

/// Formats a duration in nanoseconds, scaled to a unit that reads
///
/// The unit is chosen from the magnitude and the precision is fixed within each unit, so two
/// numbers in the same column line up and the same input always produces the same output.
///
/// # Arguments
///
/// * `nanos` - The duration to format
///
/// # Examples
///
/// ```
/// use shoal_bench::fmt::duration_ns;
///
/// assert_eq!(duration_ns(148.05), "148.05 ns");
/// assert_eq!(duration_ns(3_030.0), "3.03 µs");
/// assert_eq!(duration_ns(1_801_525_357.0), "1.802 s");
/// ```
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

/// Formats two durations in the same unit, chosen from the first of them
///
/// A row of a comparison holds a baseline and a result, and the two are read against each other.
/// Scaling them independently would print 999 ns beside 1.00 µs and make a rounding error look
/// like a change of unit, so the unit is chosen once, from the baseline - the same value the
/// noise tier is chosen from, and for the same reason.
///
/// # Arguments
///
/// * `baseline_ns` - The baseline duration, which picks the unit
/// * `run_ns` - The measured duration, formatted in that same unit
pub fn duration_pair(baseline_ns: f64, run_ns: f64) -> (String, String) {
    // pick the unit and its precision from the baseline alone
    let magnitude = baseline_ns.abs();
    let (divisor, unit, places) = if magnitude < 1_000.0 {
        (1.0, "ns", 2)
    } else if magnitude < 1_000_000.0 {
        (1_000.0, "µs", 2)
    } else if magnitude < 1_000_000_000.0 {
        (1_000_000.0, "ms", 2)
    } else {
        (1_000_000_000.0, "s", 3)
    };
    // then render both through it, so the two cells are directly comparable
    let render = |value: f64| {
        if value.is_finite() {
            format!("{} {unit}", fixed(value / divisor, places))
        } else {
            "-".to_string()
        }
    };
    (render(baseline_ns), render(run_ns))
}

/// Formats a measurement and the interval around it, in one shared unit
///
/// The unit is chosen from the measurement and applied to all three numbers, and it is written
/// once rather than three times. An interval whose endpoints carried their own units would invite
/// reading `1.802 s [1700 ms - 1.878 s]` as three different kinds of quantity.
///
/// # Arguments
///
/// * `value` - The measurement, which picks the unit
/// * `interval` - The observed interval around it, if there is one
/// * `rate` - Whether these are rates rather than durations
pub fn scaled_interval(value: f64, interval: Option<(f64, f64)>, rate: bool) -> String {
    // pick one unit from the central value and use it for everything in the cell
    let (divisor, unit, places) = if rate {
        rate_unit(value)
    } else {
        duration_unit(value)
    };
    let scale = |v: f64| fixed(v / divisor, places);
    // without an interval the cell is just the measurement
    match interval {
        Some((low, high)) => format!(
            "{} [{} - {}] {unit}",
            scale(value),
            scale(low),
            scale(high)
        ),
        None => format!("{} {unit}", scale(value)),
    }
}

/// The divisor, unit and precision a duration of this magnitude should be printed with
///
/// # Arguments
///
/// * `nanos` - The duration deciding the unit
fn duration_unit(nanos: f64) -> (f64, &'static str, usize) {
    // the same bands [`duration_ns`] uses, factored out so a cell and a column agree
    let magnitude = nanos.abs();
    if magnitude < 1_000.0 {
        (1.0, "ns", 2)
    } else if magnitude < 1_000_000.0 {
        (1_000.0, "µs", 2)
    } else if magnitude < 1_000_000_000.0 {
        (1_000_000.0, "ms", 2)
    } else {
        (1_000_000_000.0, "s", 3)
    }
}

/// The divisor, unit and precision a rate of this magnitude should be printed with
///
/// # Arguments
///
/// * `value` - The rate deciding the unit
fn rate_unit(value: f64) -> (f64, &'static str, usize) {
    // rates here are rows per second, which run to the hundreds of thousands
    let magnitude = value.abs();
    if magnitude < 1_000.0 {
        (1.0, "rows/s", 0)
    } else if magnitude < 1_000_000.0 {
        (1_000.0, "k rows/s", 1)
    } else {
        (1_000_000.0, "M rows/s", 2)
    }
}

/// Formats a duration in nanoseconds in milliseconds, whatever its magnitude
///
/// Used where a column has to hold one unit so that its numbers can be compared by eye, which
/// [`duration_ns`] deliberately does not guarantee.
///
/// # Arguments
///
/// * `nanos` - The duration to format
pub fn millis(nanos: f64) -> String {
    // a non finite value is not a measurement
    if !nanos.is_finite() {
        return "-".to_string();
    }
    // one fixed unit, one fixed precision
    format!("{} ms", fixed(nanos / 1_000_000.0, 1))
}

/// Formats a percentage change with an explicit sign
///
/// The sign is always written, including for a change that rounds to zero, because the column it
/// lands in is read as a direction first and a magnitude second.
///
/// # Arguments
///
/// * `pct` - The change, already in percent
///
/// # Examples
///
/// ```
/// use shoal_bench::fmt::signed_pct;
///
/// assert_eq!(signed_pct(3.2149), "+3.21%");
/// assert_eq!(signed_pct(-0.001), "-0.00%");
/// ```
pub fn signed_pct(pct: f64) -> String {
    // a non finite change usually means a zero baseline, which is not a change anyone can act on
    if !pct.is_finite() {
        return "-".to_string();
    }
    // format the magnitude, then attach the sign of the input rather than of the rounded value,
    // so a tiny regression does not print as an improvement
    let body = fixed(pct.abs(), 2);
    if pct.is_sign_negative() {
        format!("-{body}%")
    } else {
        format!("+{body}%")
    }
}

/// Formats a share of a whole as a percentage
///
/// # Arguments
///
/// * `share` - The share, as a fraction between zero and one
pub fn share_pct(share: f64) -> String {
    // a non finite share cannot be drawn or tabulated
    if !share.is_finite() {
        return "-".to_string();
    }
    // shares are read against each other, so one decimal is enough and two is noise
    format!("{}%", fixed(share * 100.0, 1))
}

/// Formats an integer with thousands separators
///
/// # Arguments
///
/// * `value` - The number to format
///
/// # Examples
///
/// ```
/// use shoal_bench::fmt::thousands;
///
/// assert_eq!(thousands(447_251), "447,251");
/// assert_eq!(thousands(0), "0");
/// ```
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

/// Formats a float at a fixed number of decimal places, without a signed zero
///
/// `format!("{:.2}", -0.0001)` produces `-0.00`, which reads as a negative number that is not
/// negative. Every formatter above routes through here so that only one of them has to know.
///
/// # Arguments
///
/// * `value` - The number to format
/// * `places` - How many decimal places to keep
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A duration is scaled into the unit its magnitude calls for
    #[test]
    fn duration_picks_a_unit_from_the_magnitude() {
        assert_eq!(duration_ns(0.0), "0.00 ns");
        assert_eq!(duration_ns(999.994), "999.99 ns");
        assert_eq!(duration_ns(1_000.0), "1.00 µs");
        assert_eq!(duration_ns(999_999.0), "1000.00 µs");
        assert_eq!(duration_ns(1_000_000.0), "1.00 ms");
        assert_eq!(duration_ns(1_801_525_357.0), "1.802 s");
    }

    /// A duration that is not a measurement is not printed as if it were one
    #[test]
    fn duration_refuses_non_finite() {
        assert_eq!(duration_ns(f64::NAN), "-");
        assert_eq!(duration_ns(f64::INFINITY), "-");
    }

    /// The sign of a change always comes from the input, never from the rounded value
    #[test]
    fn signed_pct_keeps_the_direction_of_a_tiny_change() {
        assert_eq!(signed_pct(0.0), "+0.00%");
        assert_eq!(signed_pct(-0.001), "-0.00%");
        assert_eq!(signed_pct(0.001), "+0.00%");
        assert_eq!(signed_pct(-22.4), "-22.40%");
    }

    /// A value that rounds to zero never keeps a minus sign it only had in its sign bit
    #[test]
    fn fixed_strips_a_signed_zero() {
        assert_eq!(fixed(-0.0, 2), "0.00");
        assert_eq!(fixed(-0.0001, 2), "0.00");
        assert_eq!(fixed(-0.006, 2), "-0.01");
    }

    /// Separators land every three digits from the right, and never lead
    #[test]
    fn thousands_separates_every_three_digits() {
        assert_eq!(thousands(0), "0");
        assert_eq!(thousands(999), "999");
        assert_eq!(thousands(1_000), "1,000");
        assert_eq!(thousands(447_251), "447,251");
        assert_eq!(thousands(1_234_567_890), "1,234,567,890");
    }
}
