//! Markdown tables, each under the label of where it was measured
//!
//! Copied from X6's harness (`shoal-spike/src/device/table.rs`), which has no library target.

/// A table being built a row at a time
#[derive(Debug, Clone, Default)]
pub struct Table {
    /// The column headings
    head: Vec<String>,
    /// The rows, each as wide as the head
    rows: Vec<Vec<String>>,
}

impl Table {
    /// A table with these columns
    ///
    /// # Arguments
    ///
    /// * `head` - The column headings
    #[must_use]
    pub fn new(head: &[&str]) -> Self {
        Table {
            head: head.iter().map(|&heading| heading.to_string()).collect(),
            rows: Vec::new(),
        }
    }

    /// Add a row
    ///
    /// # Arguments
    ///
    /// * `cells` - One cell a column
    pub fn row(&mut self, cells: Vec<String>) {
        debug_assert_eq!(cells.len(), self.head.len(), "a row is as wide as its head");
        self.rows.push(cells);
    }

    /// Whether no row was added
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// The table as markdown, under a title and the label
    ///
    /// # Arguments
    ///
    /// * `title` - The heading above it
    /// * `label` - The line naming where it was measured
    #[must_use]
    pub fn render(&self, title: &str, label: &str) -> String {
        // the heading, the label, then the table itself
        let mut out = format!("### {title}\n\n{label}\n\n");
        out.push_str(&format!("| {} |\n", self.head.join(" | ")));
        out.push_str(&format!(
            "|{}\n",
            self.head.iter().map(|_| " --- |").collect::<String>()
        ));
        for row in &self.rows {
            out.push_str(&format!("| {} |\n", row.join(" | ")));
        }
        out.push('\n');
        out
    }
}
