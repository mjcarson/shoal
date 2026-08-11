//! The little pills that say whether a number can still be believed
//!
//! Rendered as raw HTML inline in the markdown, which mdbook passes through untouched, and styled
//! by `docs/theme/charts.css`.
//!
//! Two rules they follow. The word always carries the meaning - no badge is distinguished by its
//! colour alone, so a reader who cannot tell the borders apart still reads "stale · 7 commits".
//! And they appear both in the freshness table and again under every chart drawn from the capture,
//! because a reader who scrolls straight to one chart must not have to scroll back to find out
//! what is wrong with it.

use crate::stale::{CaptureStatus, CodeVerdict, EnvVerdict};

/// Renders one badge
///
/// # Arguments
///
/// * `class` - The css class that styles it
/// * `text` - What it says
pub fn badge(class: &str, text: &str) -> String {
    // escaped, since a verdict can name a governor or a field read out of a capture
    format!(
        r#"<span class="sc-badge {class}">{}</span>"#,
        super::chart::escape_attribute(text)
    )
}

/// Renders the badge for one layer's code verdict
///
/// # Arguments
///
/// * `verdict` - What was concluded about the layer
pub fn code(verdict: &CodeVerdict) -> String {
    // the class only styles it; the label is what says what it means
    badge(verdict.css_class(), &verdict.label())
}

/// Renders the badge for a capture's environment, if it is worth saying anything about
///
/// A capture taken here on this machine needs no badge - saying "comparable" beside every row
/// would train a reader to skip the column that matters.
///
/// # Arguments
///
/// * `verdict` - What was concluded about the environment
pub fn env(verdict: &EnvVerdict) -> Option<String> {
    // only the two states worth interrupting a reader for
    match verdict {
        EnvVerdict::Comparable => None,
        EnvVerdict::Unknown => Some(badge("sc-env", "environment unrecorded")),
        EnvVerdict::Incomparable { .. } => Some(badge("sc-env", &verdict.label())),
    }
}

/// Renders every badge that applies to a capture as a whole
///
/// # Arguments
///
/// * `status` - What was concluded about the capture
pub fn for_capture(status: &CaptureStatus) -> String {
    let mut badges = Vec::new();
    // a partial capture measured a subset nobody looking at the chart chose
    if status.partial {
        badges.push(badge("sc-partial", "partial capture"));
    }
    if let Some(env) = env(&status.env) {
        badges.push(env);
    }
    badges.join(" ")
}

/// Renders the badges that apply to one layer of one capture
///
/// # Arguments
///
/// * `status` - What was concluded about the capture
/// * `layer` - Which layer is being described
pub fn for_layer(status: &CaptureStatus, layer: crate::registry::Layer) -> String {
    let mut badges = Vec::new();
    // the layer's own verdict first, which is the thing being asked about
    let verdict = status.verdict(layer);
    if let Some(verdict) = verdict {
        badges.push(code(verdict));
    }
    // a capture with no provenance at all has no recorded environment either, and saying so twice
    // on the same row teaches a reader to skip the column
    let unrecorded = matches!(verdict, Some(CodeVerdict::NoProvenance));
    if status.partial {
        badges.push(badge("sc-partial", "partial capture"));
    }
    if !unrecorded && let Some(env) = env(&status.env) {
        badges.push(env);
    }
    // and a reminder for the layers whose numbers are not latencies
    if layer.is_instrumented() {
        badges.push(badge("sc-env", "instrumented build, attribution only"));
    }
    badges.join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::Layer;
    use crate::stale::{CaptureStatus, CodeVerdict, EnvVerdict};

    /// Builds a status
    ///
    /// # Arguments
    ///
    /// * `verdict` - The micro layer's verdict
    /// * `partial` - Whether the capture was partial
    /// * `env` - What was concluded about the environment
    fn status(verdict: CodeVerdict, partial: bool, env: EnvVerdict) -> CaptureStatus {
        CaptureStatus {
            label: "L".to_string(),
            captured: Some("2026-08-09T00:00:00Z".to_string()),
            layers: vec![Layer::Micro],
            code: vec![(Layer::Micro, verdict)],
            env,
            partial,
        }
    }

    /// A badge says its verdict in words, not only in colour
    #[test]
    fn a_badge_spells_out_its_verdict() {
        let rendered = code(&CodeVerdict::Stale { behind: 7 });
        assert!(rendered.contains("stale · 7 commits"), "{rendered}");
        assert!(rendered.contains("sc-stale"), "{rendered}");
    }

    /// A capture taken right here gets no environment badge
    #[test]
    fn a_comparable_environment_is_not_badged() {
        assert!(env(&EnvVerdict::Comparable).is_none());
    }

    /// A capture taken somewhere else names what differs
    #[test]
    fn an_incomparable_environment_names_the_difference() {
        let rendered = env(&EnvVerdict::Incomparable {
            fields: vec!["governor performance vs powersave".to_string()],
        })
        .expect("an incomparable environment is badged");
        assert!(rendered.contains("governor performance vs powersave"), "{rendered}");
    }

    /// An instrumented layer says so wherever it is drawn
    #[test]
    fn an_instrumented_layer_is_always_labelled() {
        let status = CaptureStatus {
            label: "L".to_string(),
            captured: None,
            layers: vec![Layer::Hotpath],
            code: vec![(Layer::Hotpath, CodeVerdict::Fresh)],
            env: EnvVerdict::Comparable,
            partial: false,
        };
        let rendered = for_layer(&status, Layer::Hotpath);
        assert!(rendered.contains("attribution only"), "{rendered}");
    }

    /// A partial capture is badged wherever it is drawn
    #[test]
    fn a_partial_capture_is_badged() {
        let status = status(CodeVerdict::Fresh, true, EnvVerdict::Comparable);
        assert!(for_layer(&status, Layer::Micro).contains("partial capture"));
    }

    /// Text that could break the markup is escaped
    #[test]
    fn badge_text_is_escaped() {
        assert!(badge("sc-none", "a <b> & c").contains("a &lt;b&gt; &amp; c"));
    }
}
