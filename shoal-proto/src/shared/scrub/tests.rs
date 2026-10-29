//! A scrub's spans, windows, schedule and figures

use super::{
    due, stagger_ms, Due, ScrubCursor, ScrubFigures, ScrubKind, ScrubPolicy, ScrubRequests,
    ScrubState, ScrubWindow, Span, DEFAULT_DEEP_INTERVAL, DEFAULT_LIGHT_INTERVAL,
};

/// An hour in milliseconds
const HOUR: u64 = 3_600_000;

/// A span is written with its unit and read back the same, and a bare number is refused
#[test]
fn a_span_reads_as_it_is_written() {
    assert_eq!(Span::parse("90s"), Ok(Span(90)));
    assert_eq!(Span::parse("30m"), Ok(Span(1800)));
    assert_eq!(Span::parse("12h"), Ok(Span(43_200)));
    assert_eq!(Span::parse("7d"), Ok(DEFAULT_DEEP_INTERVAL));
    assert_eq!(DEFAULT_LIGHT_INTERVAL.to_string(), "1d");
    assert_eq!(Span(90).to_string(), "90s");
    assert_eq!(Span(0).to_string(), "0s");
    // a number with no unit could be any unit, so it is refused
    assert!(Span::parse("600").is_err());
    assert!(Span::parse("5w").is_err());
    assert!(Span::parse("s").is_err());
    // and it round trips through a file
    let written = serde_json::to_string(&Span(600)).unwrap();
    assert_eq!(written, "\"10m\"");
    assert_eq!(serde_json::from_str::<Span>(&written).unwrap(), Span(600));
}

/// A window holds the hours from its start to its end, over midnight when it ends before it
/// starts, and the whole day when it ends where it starts
#[test]
fn a_window_holds_its_hours() {
    let night = ScrubWindow::parse("01:00-06:00").unwrap();
    assert_eq!(night, ScrubWindow { start: 1, end: 6 });
    assert_eq!(ScrubWindow::parse("01-06").unwrap(), night);
    assert!(!night.contains(0));
    assert!(night.contains(HOUR));
    assert!(night.contains(5 * HOUR + HOUR / 2));
    assert!(!night.contains(6 * HOUR));
    // a later day's hours are the same hours
    assert!(night.contains(24 * HOUR + 2 * HOUR));
    // over midnight
    let late = ScrubWindow::parse("22-02").unwrap();
    assert!(late.contains(23 * HOUR));
    assert!(late.contains(HOUR));
    assert!(!late.contains(12 * HOUR));
    // the whole day
    assert!(ScrubWindow::parse("05-05").unwrap().contains(17 * HOUR));
    // what is not a window
    assert!(ScrubWindow::parse("01:30-06:00").is_err());
    assert!(ScrubWindow::parse("01-24").is_err());
    assert!(ScrubWindow::parse("0106").is_err());
    assert_eq!(night.to_string(), "01:00-06:00");
}

/// A pool's policy is read from a configuration, every field optional, and refuses a field it
/// does not know
#[test]
fn a_policy_reads_from_a_pool() {
    let policy: ScrubPolicy = serde_json::from_str(r#"{"light": "12h", "window": "01:00-06:00"}"#).unwrap();
    assert_eq!(policy.interval_ms(ScrubKind::Light), 12 * HOUR);
    assert_eq!(policy.interval_ms(ScrubKind::Deep), DEFAULT_DEEP_INTERVAL.millis());
    assert!(policy.in_window(2 * HOUR));
    assert!(!policy.in_window(12 * HOUR));
    assert!(serde_json::from_str::<ScrubPolicy>(r#"{"often": "1d"}"#).is_err());
    // an empty policy is every default, and any hour
    let empty = ScrubPolicy::default();
    assert_eq!(empty.interval_ms(ScrubKind::Light), DEFAULT_LIGHT_INTERVAL.millis());
    assert!(empty.in_window(13 * HOUR));
    assert_eq!(serde_json::to_string(&empty).unwrap(), "{}");
}

/// A group never scrubbed begins after its stagger, a later pass an interval after the last began
/// or as it finished when it was slower, and a pass under way goes on inside its window
#[test]
fn a_pass_is_due_by_its_schedule() {
    let policy = ScrubPolicy {
        deep: Some(Span(10 * 3600)),
        ..ScrubPolicy::default()
    };
    let never = ScrubCursor::default();
    // never scrubbed: after the stagger since its leader first saw it
    assert_eq!(due(&policy, ScrubKind::Deep, &never, 0, 1000, 1000, HOUR), Due::Wait);
    assert_eq!(
        due(&policy, ScrubKind::Deep, &never, 0, 1000 + HOUR, 1000, HOUR),
        Due::Begin { request: 0, asked: false }
    );
    // under way: goes on
    let running = ScrubCursor {
        pass: 1,
        began_ms: HOUR,
        ..ScrubCursor::default()
    };
    assert_eq!(due(&policy, ScrubKind::Deep, &running, 0, 2 * HOUR, 0, 0), Due::Continue);
    // finished quickly: the next begins an interval after this one began
    let quick = ScrubCursor {
        pass: 1,
        finished: true,
        began_ms: HOUR,
        served: Some(0),
        last_ms: 2 * HOUR,
        ..ScrubCursor::default()
    };
    assert_eq!(due(&policy, ScrubKind::Deep, &quick, 0, 10 * HOUR, 0, 0), Due::Wait);
    assert_eq!(
        due(&policy, ScrubKind::Deep, &quick, 0, 11 * HOUR, 0, 0),
        Due::Begin { request: 0, asked: false }
    );
    // finished slowly: the next begins as it finished, which was past the interval
    let slow = ScrubCursor {
        last_ms: 30 * HOUR,
        ..quick
    };
    assert_eq!(due(&policy, ScrubKind::Deep, &slow, 0, 20 * HOUR, 0, 0), Due::Wait);
    assert_eq!(
        due(&policy, ScrubKind::Deep, &slow, 0, 30 * HOUR, 0, 0),
        Due::Begin { request: 0, asked: false }
    );
    // an interval of zero schedules nothing
    let off = ScrubPolicy {
        light: Some(Span(0)),
        ..ScrubPolicy::default()
    };
    assert_eq!(due(&off, ScrubKind::Light, &never, 0, u64::MAX / 2, 0, 0), Due::Wait);
}

/// An operator's request begins a pass at once, whatever the window or a pass under way, and a
/// pass it began goes on outside the window; a scheduled one waits for it
#[test]
fn an_operators_request_runs_at_once() {
    let policy = ScrubPolicy {
        light: Some(Span(0)),
        window: Some(ScrubWindow { start: 1, end: 2 }),
        ..ScrubPolicy::default()
    };
    let outside = 12 * HOUR;
    let never = ScrubCursor::default();
    assert_eq!(
        due(&policy, ScrubKind::Light, &never, 1, outside, 0, 0),
        Due::Begin { request: 1, asked: true }
    );
    // a pass under way for an older request is begun again for the newer
    let running = ScrubCursor {
        pass: 3,
        request: 1,
        asked: true,
        ..ScrubCursor::default()
    };
    assert_eq!(
        due(&policy, ScrubKind::Light, &running, 2, outside, 0, 0),
        Due::Begin { request: 2, asked: true }
    );
    // an operator's pass goes on outside the window, a scheduled one does not
    assert_eq!(due(&policy, ScrubKind::Light, &running, 1, outside, 0, 0), Due::Continue);
    let scheduled = ScrubCursor { asked: false, ..running };
    assert_eq!(due(&policy, ScrubKind::Light, &scheduled, 1, outside, 0, 0), Due::Wait);
    assert_eq!(due(&policy, ScrubKind::Light, &scheduled, 1, HOUR, 0, 0), Due::Continue);
}

/// A stagger is a share of the interval fixed by the group, never none and never past it
#[test]
fn a_stagger_spreads_groups_over_the_interval() {
    let interval = DEFAULT_LIGHT_INTERVAL.millis();
    // never none, so nothing is scrubbed the moment a leader sees it
    assert_eq!(stagger_ms(0, interval), interval / 997);
    assert!(stagger_ms(995, interval) < interval);
    assert_eq!(stagger_ms(996, interval), stagger_ms(0, interval));
    assert_ne!(stagger_ms(1, interval), stagger_ms(2, interval));
}

/// A group is pending until a finished pass serves the pool's latest request, and figures sum
/// their counters and keep the oldest finish
#[test]
fn figures_count_pending_groups_and_the_oldest_finish() {
    let requests = ScrubRequests {
        light: 0,
        deep: 2,
        ..ScrubRequests::default()
    };
    let mut state = ScrubState::default();
    state.light = ScrubCursor {
        pass: 1,
        finished: true,
        served: Some(0),
        last_ms: 500,
        ..ScrubCursor::default()
    };
    state.deep = ScrubCursor {
        pass: 2,
        request: 2,
        served: Some(1),
        last_ms: 300,
        ..ScrubCursor::default()
    };
    let mut one = ScrubFigures {
        corrupt: 2,
        ..ScrubFigures::default()
    };
    one.note_group(&state, &requests);
    assert_eq!((one.light_pending, one.deep_pending), (0, 1));
    assert_eq!((one.light_last_ms, one.deep_last_ms), (Some(500), Some(300)));
    // a group never scrubbed is pending and finished at zero
    let mut two = ScrubFigures::default();
    two.note_group(&ScrubState::default(), &ScrubRequests::default());
    assert_eq!((two.light_pending, two.deep_pending), (1, 1));
    one.absorb(&two);
    assert_eq!(one.corrupt, 2);
    assert_eq!((one.light_pending, one.deep_pending), (1, 2));
    assert_eq!((one.light_last_ms, one.deep_last_ms), (Some(0), Some(0)));
    // a node leading no group adds nothing to a time
    let mut three = ScrubFigures::default();
    three.absorb(&ScrubFigures { light_last_ms: Some(9), ..ScrubFigures::default() });
    assert_eq!(three.light_last_ms, Some(9));
    assert_eq!(three.deep_last_ms, None);
}

/// A kind reads as an operator writes it and crosses as its byte
#[test]
fn a_kind_reads_and_crosses() {
    assert_eq!("deep".parse::<ScrubKind>(), Ok(ScrubKind::Deep));
    assert_eq!("Light".parse::<ScrubKind>(), Ok(ScrubKind::Light));
    assert!("weekly".parse::<ScrubKind>().is_err());
    for kind in ScrubKind::ALL {
        assert_eq!(ScrubKind::from_byte(kind.byte()), Some(kind));
    }
    assert_eq!(ScrubKind::from_byte(0), None);
    let mut requests = ScrubRequests::default();
    assert_eq!(requests.ask(ScrubKind::Deep), 1);
    assert_eq!(requests.ask(ScrubKind::Deep), 2);
    assert_eq!(requests.of(ScrubKind::Light), 0);
}

/// A deep pass checks a quarter of a pool's placement groups' parity, rotating, so every group's
/// is checked once every four passes; a policy of one checks every group every pass (F90)
#[test]
fn the_parity_sample_rotates() {
    let policy = ScrubPolicy::default();
    for tablet in [0u16, 7, 0x7ab] {
        for sub in 0..16u16 {
            // once in any four passes in a row
            let checked: Vec<u64> = (1..=4u64).filter(|pass| policy.checks_parity(*pass, tablet, sub)).collect();
            assert_eq!(checked.len(), 1, "tablet {tablet} group {sub}: {checked:?}");
        }
        // a quarter of a tablet's sixteen groups each pass
        let each = (0..16u16).filter(|sub| policy.checks_parity(3, tablet, *sub)).count();
        assert_eq!(each, 4);
    }
    let every = ScrubPolicy {
        parity_every: Some(1),
        ..ScrubPolicy::default()
    };
    assert!((0..16u16).all(|sub| every.checks_parity(5, 1, sub)));
}

/// A request narrowed to a target narrows only a group that has served every request before its
/// run, and one whose earlier request named another target scrubs the whole pool (item 266)
#[test]
fn a_narrowed_request_widens_over_an_unserved_one() {
    let device = super::ScrubTarget::Device(crate::shared::devices::DeviceId(uuid::Uuid::from_u128(7)));
    let other = super::ScrubTarget::Device(crate::shared::devices::DeviceId(uuid::Uuid::from_u128(8)));
    let mut requests = ScrubRequests::default();
    // one narrowed request: every group narrows to it
    assert_eq!(requests.ask_for(ScrubKind::Deep, Some(device)), 1);
    assert_eq!(requests.target_for(ScrubKind::Deep, None), Some(device));
    // a whole one, then a narrowed one before it is served: a group that served neither scrubs
    // the whole pool, and one that served the whole one narrows
    requests.ask_for(ScrubKind::Deep, None);
    assert_eq!(requests.ask_for(ScrubKind::Deep, Some(device)), 3);
    assert_eq!(requests.target_for(ScrubKind::Deep, Some(1)), None);
    assert_eq!(requests.target_for(ScrubKind::Deep, Some(2)), Some(device));
    // the same target again stays in its run; another begins one of its own
    requests.ask_for(ScrubKind::Deep, Some(device));
    assert_eq!(requests.target_for(ScrubKind::Deep, Some(2)), Some(device));
    requests.ask_for(ScrubKind::Deep, Some(other));
    assert_eq!(requests.target_for(ScrubKind::Deep, Some(2)), None);
    assert_eq!(requests.target_for(ScrubKind::Deep, Some(4)), Some(other));
    // the other kind is its own
    assert_eq!(requests.target_for(ScrubKind::Light, None), None);
}
