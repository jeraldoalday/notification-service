use serde::Serialize;

use crate::error::CliResult;

/// How command results are printed.
///
/// Every command renders to one of these two rather than printing as it
/// goes, so the same code path serves an operator reading a terminal and a
/// script piping into `jq`. Commands return the rendered `String` to
/// `main`, which keeps them free of I/O and directly testable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
#[value(rename_all = "lowercase")]
pub enum OutputFormat {
    Text,
    Json,
}

impl OutputFormat {
    /// Renders `value` as JSON, or falls back to `text` for humans.
    /// Taking the text rendering as a closure means the (often more
    /// expensive) human formatting is skipped entirely in JSON mode.
    pub fn render<T, F>(self, value: &T, text: F) -> CliResult<String>
    where
        T: Serialize,
        F: FnOnce() -> String,
    {
        match self {
            OutputFormat::Json => Ok(serde_json::to_string_pretty(value)?),
            OutputFormat::Text => Ok(text()),
        }
    }
}

/// Renders `rows` as a left-aligned, column-padded table with a header.
/// Deliberately hand-rolled: a dependency-free two-loop implementation is
/// less code than wiring up a table crate, and the CLI only ever needs
/// this one style.
pub fn table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in rows {
        for (index, cell) in row.iter().enumerate() {
            if index < widths.len() {
                widths[index] = widths[index].max(cell.chars().count());
            }
        }
    }

    let mut out = String::new();
    push_row(&mut out, &headers.iter().map(|h| h.to_string()).collect::<Vec<_>>(), &widths);
    for row in rows {
        push_row(&mut out, row, &widths);
    }
    out
}

fn push_row(out: &mut String, cells: &[String], widths: &[usize]) {
    let last = cells.len().saturating_sub(1);
    for (index, cell) in cells.iter().enumerate() {
        if index == last {
            // No trailing padding on the final column, so the output has
            // no invisible whitespace at end of line.
            out.push_str(cell);
        } else {
            let pad = widths[index].saturating_sub(cell.chars().count());
            out.push_str(cell);
            out.push_str(&" ".repeat(pad + 2));
        }
    }
    out.push('\n');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_pads_columns_to_the_widest_cell() {
        let rendered = table(
            &["id", "status"],
            &[vec!["a".into(), "queued".into()], vec!["longer".into(), "sent".into()]],
        );
        assert_eq!(rendered, "id      status\na       queued\nlonger  sent\n");
    }

    #[test]
    fn table_with_no_rows_is_just_the_header() {
        assert_eq!(table(&["id"], &[]), "id\n");
    }

    #[test]
    fn table_counts_characters_not_bytes() {
        let rendered = table(&["name", "x"], &[vec!["é".into(), "1".into()]]);
        assert_eq!(rendered, "name  x\n\u{e9}     1\n");
    }

    #[test]
    fn json_output_ignores_the_text_renderer() {
        let rendered =
            OutputFormat::Json.render(&serde_json::json!({"a": 1}), || unreachable!("not rendered")).unwrap();
        assert_eq!(rendered, "{\n  \"a\": 1\n}");
    }

    #[test]
    fn text_output_uses_the_text_renderer() {
        let rendered = OutputFormat::Text.render(&serde_json::json!({"a": 1}), || "a=1".to_string()).unwrap();
        assert_eq!(rendered, "a=1");
    }
}
