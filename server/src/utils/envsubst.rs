//! Bash-style `$NAME`/`${NAME}` expansion, run once before the surrounding
//! format is parsed.

/// `$$` escapes to a literal `$` (`$$uri`/`$${uri}` survive as `$uri`/
/// `${uri}`). A bare `$NAME` expands like `${NAME}` only when immediately
/// followed by an identifier char, so a regex like `~\.php$` stays literal.
pub(crate) fn substitute(text: &str) -> Result<String, String> {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find('$') {
        out.push_str(&rest[..start]);
        let after = &rest[start + 1..];
        match after.as_bytes().first() {
            Some(b'$') => {
                out.push('$');
                rest = &after[1..];
            }
            Some(b'{') => {
                let brace_body = &after[1..];
                let end = brace_body.find('}').ok_or_else(|| {
                    "unterminated \"${\" in config (missing closing brace)".to_string()
                })?;
                out.push_str(&expand(&brace_body[..end], true)?);
                rest = &brace_body[end + 1..];
            }
            Some(_) if starts_with_ident(after) => {
                let name_len = after
                    .find(|c: char| !is_ident_continue(c))
                    .unwrap_or(after.len());
                let (name, remainder) = after.split_at(name_len);
                out.push_str(&expand(name, false)?);
                rest = remainder;
            }
            _ => {
                // A lone `$` not starting `$$`, `${`, or `$NAME` - e.g. a
                // regex end-of-string anchor - is left exactly as written.
                out.push('$');
                rest = after;
            }
        }
    }
    out.push_str(rest);
    Ok(out)
}

fn starts_with_ident(s: &str) -> bool {
    s.chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
}

fn is_ident_continue(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Bash parameter expansion: `${NAME}`, `${NAME-D}`/`${NAME:-D}`,
/// `${NAME=D}`/`${NAME:=D}`, `${NAME+O}`/`${NAME:+O}`. `:`-forms treat a
/// set-but-empty variable as unset; `=`/`:=` never write the variable back.
///
/// `braced` only affects error wording - a bare `$NAME` never carries an
/// operator, since `name` was already stopped at the first non-identifier char.
fn expand(body: &str, braced: bool) -> Result<String, String> {
    let spelled = |body: &str| {
        if braced {
            format!("${{{body}}}")
        } else {
            format!("${body}")
        }
    };

    let (name, op) = match body.find([':', '-', '=', '+']) {
        Some(i) => (&body[..i], Some(&body[i..])),
        None => (body, None),
    };
    let value = std::env::var(name);
    let Some(op) = op else {
        return value.map_err(|_| {
            format!(
                "environment variable {name:?} is not set (referenced as \"{}\" in config)",
                spelled(body)
            )
        });
    };

    let (empty_counts_as_unset, kind_and_arg) = match op.strip_prefix(':') {
        Some(rest) => (true, rest),
        None => (false, op),
    };
    let kind = kind_and_arg
        .chars()
        .next()
        .filter(|c| "-=+".contains(*c))
        .ok_or_else(|| format!("invalid substitution \"{}\" in config", spelled(body)))?;
    let arg = &kind_and_arg[1..];

    let is_unset = value.is_err() || (empty_counts_as_unset && value.as_deref() == Ok(""));
    match kind {
        '-' | '=' => Ok(if is_unset {
            arg.to_string()
        } else {
            value.unwrap()
        }),
        '+' => Ok(if is_unset {
            String::new()
        } else {
            arg.to_string()
        }),
        _ => unreachable!("filtered to -=+ above"),
    }
}

#[cfg(test)]
#[path = "envsubst_tests.rs"]
mod tests;
