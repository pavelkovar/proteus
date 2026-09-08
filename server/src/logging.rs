//! Flat JSON log lines, every field top-level, matching the C php-mod's own
//! shape. `tracing_subscriber`'s built-in JSON nests most fields instead.

use std::fmt;
use std::fmt::Write as _;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::format::{FormatEvent, FormatFields, Writer};
use tracing_subscriber::fmt::time::{FormatTime, SystemTime};
use tracing_subscriber::fmt::FmtContext;
use tracing_subscriber::registry::LookupSpan;

/// Every line carries a `type` field, so one mixed stream stays filterable.
///
/// `background_writer` is only safe in a process that never forks without an
/// immediate exec: `fork()` continues just the calling thread, so the writer
/// thread would not exist in the child and its logging would silently break.
pub(crate) fn init(background_writer: bool) {
    if !background_writer {
        tracing_subscriber::fmt().event_format(FlatJson).init();
        return;
    }

    let (writer, guard) = tracing_appender::non_blocking(std::io::stdout());
    // Nothing narrower than the process outlives logging.
    Box::leak(Box::new(guard));
    tracing_subscriber::fmt().with_writer(writer).event_format(FlatJson).init();
}

struct FlatJson;

impl<S, N> FormatEvent<S, N> for FlatJson
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        write!(writer, "{{\"timestamp\":\"")?;
        SystemTime.format_time(&mut writer)?;
        write!(writer, "\",\"level\":\"{}\"", event.metadata().level())?;

        let mut visitor = JsonFieldVisitor { writer: &mut writer, result: Ok(()) };
        event.record(&mut visitor);
        visitor.result?;

        writeln!(writer, "}}")
    }
}

/// Writes each field straight into the line, so a flat object falls out with
/// no intermediate map. The macro's format string arrives as a field named
/// `message`.
struct JsonFieldVisitor<'a, 'w> {
    writer: &'a mut Writer<'w>,
    result: fmt::Result,
}

impl JsonFieldVisitor<'_, '_> {
    fn write_raw(&mut self, field: &Field, value: impl fmt::Display) {
        if self.result.is_err() {
            return;
        }
        self.result = write!(self.writer, ",\"{}\":{value}", field.name());
    }

    /// Straight into the writer, with no intermediate `String`.
    fn write_escaped(&mut self, field: &Field, value: impl fmt::Display) {
        if self.result.is_err() {
            return;
        }
        self.result = (|| {
            write!(self.writer, ",\"{}\":\"", field.name())?;
            write!(JsonEscape(self.writer), "{value}")?;
            write!(self.writer, "\"")
        })();
    }
}

/// Escapes on the way through, letting a `Display` or `Debug` impl format
/// directly into JSON with no intermediate `String`.
struct JsonEscape<'a, 'b>(&'a mut Writer<'b>);

impl fmt::Write for JsonEscape<'_, '_> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        for c in s.chars() {
            match c {
                '"' => self.0.write_str("\\\"")?,
                '\\' => self.0.write_str("\\\\")?,
                '\u{8}' => self.0.write_str("\\b")?,
                '\t' => self.0.write_str("\\t")?,
                '\n' => self.0.write_str("\\n")?,
                '\u{c}' => self.0.write_str("\\f")?,
                '\r' => self.0.write_str("\\r")?,
                c if (c as u32) < 0x20 => write!(self.0, "\\u{:04x}", c as u32)?,
                c => self.0.write_char(c)?,
            }
        }
        Ok(())
    }
}

impl Visit for JsonFieldVisitor<'_, '_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.write_escaped(field, value);
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.write_raw(field, value);
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.write_raw(field, value);
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.write_raw(field, value);
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.write_raw(field, value);
    }

    /// Formatted with `{:?}` and escaped, as `tracing_subscriber`'s own JSON
    /// visitor does.
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        struct DebugDisplay<'a>(&'a dyn fmt::Debug);
        impl fmt::Display for DebugDisplay<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Debug::fmt(self.0, f)
            }
        }
        self.write_escaped(field, DebugDisplay(value));
    }
}
