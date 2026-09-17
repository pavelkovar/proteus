//! Glob/regex matching against a string, used wherever config accepts a
//! match rule (route `uri`/`method`/`host`, rate-limit `user_agent`).

use serde::Deserialize;

/// Matches if no non-negated pattern exists or one hits, and no negated
/// pattern hits.
pub(crate) fn matches_any(patterns: &[MatchPattern], value: &str) -> bool {
    let mut has_positive = false;
    let mut positive_matched = false;
    for pattern in patterns {
        if let MatchPattern::Not(inner) = pattern {
            if inner.matches(value) {
                return false;
            }
        } else {
            has_positive = true;
            positive_matched = positive_matched || pattern.matches(value);
        }
    }
    !has_positive || positive_matched
}

/// `~pattern` is a regex, which is linear-time and so safe on hostile input;
/// anything else is a glob. A leading `!` negates. Compiled once at load.
#[derive(Debug)]
pub enum MatchPattern {
    /// No `*`.
    Exact(String),
    Any,
    /// `min_length` lets a short value be rejected in O(1).
    Glob {
        leading: bool,
        trailing: bool,
        parts: Vec<String>,
        min_length: usize,
    },
    Regex(regex::Regex),
    Not(Box<MatchPattern>),
}

impl MatchPattern {
    pub(crate) fn matches(&self, value: &str) -> bool {
        match self {
            MatchPattern::Exact(s) => value == s,
            MatchPattern::Any => true,
            MatchPattern::Regex(re) => re.is_match(value),
            MatchPattern::Not(inner) => !inner.matches(value),
            MatchPattern::Glob {
                leading,
                trailing,
                parts,
                min_length,
            } => {
                if value.len() < *min_length {
                    return false;
                }
                let last = parts.len() - 1;
                let mut rest = value;
                for (i, part) in parts.iter().enumerate() {
                    if i == 0 && !leading {
                        match rest.strip_prefix(part.as_str()) {
                            Some(after) => rest = after,
                            None => return false,
                        }
                    } else if i == last && !trailing {
                        return rest.ends_with(part.as_str());
                    } else {
                        match rest.find(part.as_str()) {
                            Some(offset) => rest = &rest[offset + part.len()..],
                            None => return false,
                        }
                    }
                }
                true
            }
        }
    }
}

impl TryFrom<String> for MatchPattern {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        // Never matches anything real: a typo, not an intent.
        if s.is_empty() {
            return Err("empty match pattern (use \"*\" to match everything)".to_string());
        }
        if let Some(rest) = s.strip_prefix('!') {
            return MatchPattern::try_from(rest.to_string())
                .map(|p| MatchPattern::Not(Box::new(p)));
        }
        if let Some(pattern) = s.strip_prefix('~') {
            // ASCII-only, which drops the sizeable `unicode-*` features from
            // the release binary; \d, \w and \s still work.
            return regex::RegexBuilder::new(pattern)
                .unicode(false)
                .build()
                .map(MatchPattern::Regex)
                .map_err(|e| format!("invalid match regex {pattern:?}: {e}"));
        }
        if !s.contains('*') {
            return Ok(MatchPattern::Exact(s));
        }
        if s.chars().all(|c| c == '*') {
            return Ok(MatchPattern::Any);
        }
        let leading = s.starts_with('*');
        let trailing = s.ends_with('*');
        let parts: Vec<String> = s
            .split('*')
            .filter(|p| !p.is_empty())
            .map(String::from)
            .collect();
        let min_length = parts.iter().map(String::len).sum();
        Ok(MatchPattern::Glob {
            leading,
            trailing,
            parts,
            min_length,
        })
    }
}

impl<'de> Deserialize<'de> for MatchPattern {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .try_into()
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
#[path = "match_pattern_tests.rs"]
mod tests;
