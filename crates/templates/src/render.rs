use std::collections::{BTreeSet, HashMap};

use crate::error::{TemplateError, TemplateResult};

/// One piece of a parsed template: either text to emit verbatim, or a
/// placeholder to substitute a variable into.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Segment {
    Literal(String),
    Placeholder(String),
}

/// Deliberately *not* a general-purpose template language. Notification
/// bodies are short messages authored by tenants, and every construct a
/// bigger engine adds (conditionals, loops, partials, arbitrary
/// expressions) is another way for a tenant-supplied string to do
/// something surprising at delivery time. The whole grammar is:
///
/// - `{{ name }}` interpolates the variable `name`; surrounding whitespace
///   inside the braces is ignored, so `{{name}}` and `{{ name }}` are the
///   same placeholder.
/// - `{{{{` emits a literal `{{`, mirroring how Rust's own format strings
///   escape a brace by doubling it. A lone `{` is always literal.
/// - Anything else is literal text.
///
/// Parsing is done once when a template is created (so authoring mistakes
/// are reported at that point, not on the delivery path) and again at
/// render time, which is cheap for strings of this size.
pub fn parse(input: &str) -> TemplateResult<Vec<Segment>> {
    let bytes = input.as_bytes();
    let mut segments = Vec::new();
    let mut literal = String::new();
    let mut index = 0;

    while index < bytes.len() {
        if !bytes[index..].starts_with(b"{{") {
            // Step by whole characters so multi-byte text survives intact.
            let ch = input[index..].chars().next().expect("index is on a char boundary");
            literal.push(ch);
            index += ch.len_utf8();
            continue;
        }

        if bytes[index + 2..].starts_with(b"{{") {
            literal.push_str("{{");
            index += 4;
            continue;
        }

        let rest = &input[index + 2..];
        let Some(end) = rest.find("}}") else {
            return Err(TemplateError::UnclosedPlaceholder { offset: index });
        };

        let name = rest[..end].trim();
        validate_placeholder_name(name)?;

        if !literal.is_empty() {
            segments.push(Segment::Literal(std::mem::take(&mut literal)));
        }
        segments.push(Segment::Placeholder(name.to_string()));
        index += 2 + end + 2;
    }

    if !literal.is_empty() {
        segments.push(Segment::Literal(literal));
    }
    Ok(segments)
}

fn validate_placeholder_name(name: &str) -> TemplateResult<()> {
    let valid = !name.is_empty()
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.');
    if valid {
        Ok(())
    } else {
        Err(TemplateError::InvalidPlaceholderName(name.to_string()))
    }
}

/// Every distinct variable `input` interpolates, in sorted order --
/// a `BTreeSet` rather than a `HashSet` so callers (the API's template
/// responses, the CLI's `template show`) get a stable listing without
/// having to sort it themselves.
pub fn placeholders(input: &str) -> TemplateResult<BTreeSet<String>> {
    Ok(parse(input)?
        .into_iter()
        .filter_map(|segment| match segment {
            Segment::Placeholder(name) => Some(name),
            Segment::Literal(_) => None,
        })
        .collect())
}

/// Substitutes `vars` into `input`. Missing variables are an error rather
/// than an empty string: a notification that goes out reading "Hi , your
/// order  shipped" is worse than one that fails loudly and gets retried
/// after the caller fixes the payload. `template_id` is carried only so
/// the error names the offending template.
pub fn render(template_id: &str, input: &str, vars: &HashMap<String, String>) -> TemplateResult<String> {
    let segments = parse(input)?;
    let mut out = String::with_capacity(input.len());

    for segment in segments {
        match segment {
            Segment::Literal(text) => out.push_str(&text),
            Segment::Placeholder(name) => {
                let value = vars.get(&name).ok_or_else(|| TemplateError::MissingVariable {
                    template_id: template_id.to_string(),
                    name: name.clone(),
                })?;
                out.push_str(value);
            }
        }
    }

    Ok(out)
}

/// The variables `input` needs that `vars` doesn't supply. Lets callers
/// report *all* the gaps at once (the API's preview endpoint does this)
/// instead of surfacing them one failed render at a time.
pub fn missing_variables(input: &str, vars: &HashMap<String, String>) -> TemplateResult<BTreeSet<String>> {
    Ok(placeholders(input)?.into_iter().filter(|name| !vars.contains_key(name)).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn renders_text_without_placeholders_unchanged() {
        let rendered = render("t", "Your order shipped.", &HashMap::new()).unwrap();
        assert_eq!(rendered, "Your order shipped.");
    }

    #[test]
    fn substitutes_a_placeholder() {
        let rendered = render("t", "Hi {{name}}!", &vars(&[("name", "Ada")])).unwrap();
        assert_eq!(rendered, "Hi Ada!");
    }

    #[test]
    fn ignores_whitespace_inside_braces() {
        let rendered = render("t", "Hi {{  name  }}!", &vars(&[("name", "Ada")])).unwrap();
        assert_eq!(rendered, "Hi Ada!");
    }

    #[test]
    fn substitutes_the_same_variable_more_than_once() {
        let rendered = render("t", "{{a}}-{{a}}-{{a}}", &vars(&[("a", "x")])).unwrap();
        assert_eq!(rendered, "x-x-x");
    }

    #[test]
    fn handles_adjacent_placeholders() {
        let rendered = render("t", "{{a}}{{b}}", &vars(&[("a", "1"), ("b", "2")])).unwrap();
        assert_eq!(rendered, "12");
    }

    #[test]
    fn doubled_braces_escape_to_a_literal() {
        let rendered = render("t", "{{{{name}} stays literal", &HashMap::new()).unwrap();
        assert_eq!(rendered, "{{name}} stays literal");
    }

    #[test]
    fn a_lone_brace_is_literal() {
        let rendered = render("t", "{not a placeholder}", &HashMap::new()).unwrap();
        assert_eq!(rendered, "{not a placeholder}");
    }

    #[test]
    fn preserves_multibyte_text_around_placeholders() {
        let rendered = render("t", "こんにちは、{{name}}さん 👋", &vars(&[("name", "アダ")])).unwrap();
        assert_eq!(rendered, "こんにちは、アダさん 👋");
    }

    #[test]
    fn missing_variable_is_an_error_not_an_empty_string() {
        let err = render("welcome", "Hi {{name}}", &HashMap::new()).unwrap_err();
        assert_eq!(
            err,
            TemplateError::MissingVariable { template_id: "welcome".into(), name: "name".into() }
        );
    }

    #[test]
    fn unclosed_placeholder_reports_its_offset() {
        let err = render("t", "Hi {{name", &HashMap::new()).unwrap_err();
        assert_eq!(err, TemplateError::UnclosedPlaceholder { offset: 3 });
    }

    #[test]
    fn rejects_an_empty_placeholder_name() {
        let err = parse("Hi {{}}").unwrap_err();
        assert_eq!(err, TemplateError::InvalidPlaceholderName(String::new()));
    }

    #[test]
    fn rejects_a_placeholder_name_with_punctuation() {
        let err = parse("Hi {{first name!}}").unwrap_err();
        assert_eq!(err, TemplateError::InvalidPlaceholderName("first name!".into()));
    }

    #[test]
    fn accepts_dotted_and_dashed_names() {
        let rendered = render(
            "t",
            "{{order.id}}/{{tracking-code}}",
            &vars(&[("order.id", "7"), ("tracking-code", "Z")]),
        )
        .unwrap();
        assert_eq!(rendered, "7/Z");
    }

    #[test]
    fn placeholders_are_deduplicated_and_sorted() {
        let found = placeholders("{{b}} {{a}} {{b}}").unwrap();
        assert_eq!(found.into_iter().collect::<Vec<_>>(), vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn missing_variables_reports_every_gap_at_once() {
        let missing = missing_variables("{{a}} {{b}} {{c}}", &vars(&[("b", "set")])).unwrap();
        assert_eq!(missing.into_iter().collect::<Vec<_>>(), vec!["a".to_string(), "c".to_string()]);
    }

    #[test]
    fn parse_splits_literals_and_placeholders() {
        let segments = parse("Hi {{name}}!").unwrap();
        assert_eq!(
            segments,
            vec![
                Segment::Literal("Hi ".into()),
                Segment::Placeholder("name".into()),
                Segment::Literal("!".into()),
            ]
        );
    }

    #[test]
    fn parse_of_empty_input_is_empty() {
        assert!(parse("").unwrap().is_empty());
    }
}
