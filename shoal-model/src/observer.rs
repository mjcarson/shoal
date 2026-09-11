//! The control-plane observer: reports, an up list, and - under one unsafe knob - promotion
//!
//! The contract (P5) says a control majority cannot activate a data minority and that reports are
//! observability hints, never an authoritative history. So under the safe policy this observer
//! collects reports and tells nodes who it thinks is up, and nothing it says changes a quorum or a
//! leader. Under `Election::HeartbeatMaxReport` it does what the first draft proposed: on marking
//! a leader down it promotes whoever last reported the highest index. The
//! B=100/C=101/A+B=102 schedule is what that loses.

use std::collections::{BTreeMap, BTreeSet};

use crate::event::{Actor, Body, Message, Output};
use crate::ids::{LogIndex, NodeId, TabletId, Term};
use crate::policy::{Election, Policy};

/// What the observer has been told
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Observer {
    /// The last report from each node about each tablet: last index, its term, and whether the
    /// node claimed to lead
    pub reports: BTreeMap<(TabletId, NodeId), (LogIndex, Term, bool)>,
    /// The nodes currently considered up
    pub up: BTreeSet<NodeId>,
}

impl Observer {
    /// A node reported its progress
    ///
    /// # Arguments
    ///
    /// * `msg` - The report
    /// * `all` - Every node, to tell about a changed up list
    pub fn on_report(&mut self, msg: &Message, all: &[NodeId]) -> Vec<Output> {
        let (Actor::Node(from), Body::ProgressReport { last_index, last_term, leader }) =
            (msg.from, &msg.body)
        else {
            return Vec::new();
        };
        // cache the report, and note the reporter is up
        self.reports
            .insert((msg.tablet, from), (*last_index, *last_term, *leader));
        if self.up.insert(from) {
            return self.up_list(msg.tablet, all);
        }
        Vec::new()
    }

    /// A node's grace period expired
    ///
    /// # Arguments
    ///
    /// * `node` - The node marked down
    /// * `policy` - The policy, which decides whether a promotion follows
    /// * `all` - Every node
    /// * `tablets` - Every tablet, to find the ones the node led
    pub fn on_mark_down(
        &mut self,
        node: NodeId,
        policy: &Policy,
        all: &[NodeId],
        tablets: &[TabletId],
    ) -> Vec<Output> {
        let mut out = Vec::new();
        if self.up.remove(&node) {
            for tablet in tablets {
                out.extend(self.up_list(*tablet, all));
            }
        }
        // POLICY P5: the contract never appoints a leader from reports; the unsafe setting does
        if policy.election != Election::HeartbeatMaxReport {
            return out;
        }
        for tablet in tablets {
            // only a tablet the downed node last claimed to lead needs a replacement
            let led = self
                .reports
                .get(&(*tablet, node))
                .is_some_and(|(_, _, leader)| *leader);
            if !led {
                continue;
            }
            // the up node with the highest reported index, lowest id on a tie
            let best = self
                .up
                .iter()
                .filter_map(|candidate| {
                    self.reports
                        .get(&(*tablet, *candidate))
                        .map(|(index, _, _)| (*index, std::cmp::Reverse(*candidate)))
                })
                .max()
                .map(|(_, std::cmp::Reverse(candidate))| candidate);
            let Some(candidate) = best else {
                continue;
            };
            // one past the highest term anyone reported
            let term = self
                .reports
                .iter()
                .filter(|((t, _), _)| t == tablet)
                .map(|(_, (_, term, _))| *term)
                .max()
                .unwrap_or_default();
            out.push(Output::Send(Message {
                from: Actor::Observer,
                to: Actor::Node(candidate),
                tablet: *tablet,
                term: Term(term.0 + 1),
                body: Body::Promote {
                    term: Term(term.0 + 1),
                },
            }));
        }
        out
    }

    /// Tell every node who is up
    fn up_list(&self, tablet: TabletId, all: &[NodeId]) -> Vec<Output> {
        let up: Vec<NodeId> = self.up.iter().copied().collect();
        all.iter()
            .map(|node| {
                Output::Send(Message {
                    from: Actor::Observer,
                    to: Actor::Node(*node),
                    tablet,
                    term: Term(0),
                    body: Body::UpList { up: up.clone() },
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Output;

    const T: TabletId = TabletId::new(1, 0);

    fn report(from: u8, index: u64, leader: bool) -> Message {
        Message {
            from: Actor::Node(NodeId(from)),
            to: Actor::Observer,
            tablet: T,
            term: Term(1),
            body: Body::ProgressReport {
                last_index: LogIndex(index),
                last_term: Term(1),
                leader,
            },
        }
    }

    /// Under the contract a mark-down changes the up list and appoints nobody
    #[test]
    fn the_safe_observer_never_promotes() {
        let all = [NodeId(1), NodeId(2), NodeId(3)];
        let mut observer = Observer::default();
        for (node, index, leader) in [(1, 5, true), (2, 4, false), (3, 5, false)] {
            observer.on_report(&report(node, index, leader), &all);
        }
        let out = observer.on_mark_down(NodeId(1), &Policy::safe(), &all, &[T]);
        assert!(out.iter().all(|o| matches!(o, Output::Send(Message { body: Body::UpList { .. }, .. }))));
        assert_eq!(observer.up.len(), 2);
    }

    /// Under the unsafe election the highest reported index is appointed, lowest id on a tie
    #[test]
    fn the_unsafe_observer_promotes_the_highest_report() {
        let all = [NodeId(1), NodeId(2), NodeId(3)];
        let policy = Policy::unsafe_knobs()[0].1;
        let mut observer = Observer::default();
        for (node, index, leader) in [(1, 5, true), (2, 4, false), (3, 4, false)] {
            observer.on_report(&report(node, index, leader), &all);
        }
        let out = observer.on_mark_down(NodeId(1), &policy, &all, &[T]);
        let promoted: Vec<_> = out
            .iter()
            .filter_map(|o| match o {
                Output::Send(Message { to: Actor::Node(node), body: Body::Promote { term }, .. }) => Some((*node, *term)),
                _ => None,
            })
            .collect();
        assert_eq!(promoted, vec![(NodeId(2), Term(2))]);
    }
}
