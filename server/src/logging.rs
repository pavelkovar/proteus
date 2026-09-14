use serde::{Serialize, Serializer as _};
use std::cell::RefCell;
use std::fmt;
use std::io::Write as _;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError};
use std::sync::OnceLock;
use std::time::Duration;

const QUEUE_CAPACITY: usize = 8192;
const LINE_CAPACITY: usize = 512;

#[doc(hidden)]
pub(crate) mod level {
    pub(crate) const DEBUG: u8 = 10;
    pub(crate) const INFO: u8 = 20;
    pub(crate) const WARN: u8 = 30;
    pub(crate) const ERROR: u8 = 40;
}

#[doc(hidden)]
pub(crate) const MIN_LEVEL: u8 = level::INFO;

const MAX_BATCH: usize = 4096;
const FLUSH_TIMEOUT: Duration = Duration::from_secs(1);

static SENDER: OnceLock<SyncSender<Vec<u8>>> = OnceLock::new();

static QUEUED: AtomicU64 = AtomicU64::new(0);
static WRITTEN: AtomicU64 = AtomicU64::new(0);
static SINK_FAILED: AtomicBool = AtomicBool::new(false);

pub(crate) fn init(background_writer: bool) {
    install_panic_hook();
    if !background_writer {
        return;
    }
    let (sender, receiver) = std::sync::mpsc::sync_channel(QUEUE_CAPACITY);
    let _ = SENDER.set(sender);
    std::thread::Builder::new()
        .name("logger".into())
        .spawn(move || drain_loop(receiver))
        .expect("failed to spawn logger thread");
}

fn install_panic_hook() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let payload = info.payload();
        let message = payload
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("panicked");
        let location = info.location().map_or_else(String::new, ToString::to_string);
        crate::logging::error!(r#type = "panic", pid = std::process::id(), location = %location, "{message}");
        flush();
        previous(info);
    }));
}

fn drain_loop(receiver: Receiver<Vec<u8>>) {
    let mut out = std::io::stdout().lock();
    let mut batch = Batch { bytes: Vec::with_capacity(MAX_BATCH), lines: 0 };
    while let Ok(line) = receiver.recv() {
        batch.push(&line, &mut out);
        while let Ok(line) = receiver.try_recv() {
            batch.push(&line, &mut out);
        }
        batch.write(&mut out);
    }
}

struct Batch {
    bytes: Vec<u8>,
    lines: u64,
}

impl Batch {
    fn push<W: std::io::Write>(&mut self, line: &[u8], out: &mut W) {
        if !self.bytes.is_empty() && self.bytes.len() + line.len() > MAX_BATCH {
            self.write(out);
        }
        self.bytes.extend_from_slice(line);
        self.lines += 1;
    }

    fn write<W: std::io::Write>(&mut self, out: &mut W) {
        if self.bytes.is_empty() {
            return;
        }
        match out.write_all(&self.bytes).and_then(|()| out.flush()) {
            Ok(()) => {
                WRITTEN.fetch_add(self.lines, Ordering::Release);
            }
            Err(error) => report_sink_failure(&error),
        }
        self.bytes.clear();
        self.lines = 0;
    }
}

fn report_sink_failure(error: &std::io::Error) {
    if !SINK_FAILED.swap(true, Ordering::Relaxed) {
        let report = format!("[logger] writing to stdout failed, lines are being dropped: {error}\n");
        let _ = std::io::stderr().write_all(report.as_bytes());
    }
}

pub(crate) fn flush() {
    if SENDER.get().is_none() {
        return;
    }
    let target = QUEUED.load(Ordering::Acquire);
    let deadline = std::time::Instant::now() + FLUSH_TIMEOUT;
    while WRITTEN.load(Ordering::Acquire) < target {
        if std::time::Instant::now() >= deadline {
            return;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

thread_local! {
    static SPARE: RefCell<Option<Vec<u8>>> = const { RefCell::new(None) };
}

fn take_spare() -> Vec<u8> {
    SPARE
        .try_with(|spare| spare.borrow_mut().take())
        .ok()
        .flatten()
        .unwrap_or_else(|| Vec::with_capacity(LINE_CAPACITY))
}

fn return_spare(mut out: Vec<u8>) {
    out.clear();
    let _ = SPARE.try_with(move |spare| *spare.borrow_mut() = Some(out));
}

#[doc(hidden)]
pub(crate) fn begin(level: &str) -> Vec<u8> {
    let mut out = take_spare();
    let _ = write!(
        out,
        "{{\"timestamp\":\"{}\",\"level\":\"{level}\"",
        humantime::format_rfc3339_micros(std::time::SystemTime::now()),
    );
    out
}
#[doc(hidden)]
pub(crate) fn emit(line: &[u8]) {
    let Some(sender) = SENDER.get() else {
        return write_direct(line);
    };
    match sender.try_send(line.to_vec()) {
        Ok(()) => {
            QUEUED.fetch_add(1, Ordering::Relaxed);
        }
        Err(TrySendError::Full(_)) => {}
        Err(TrySendError::Disconnected(_)) => write_direct(line),
    }
}

fn write_direct(line: &[u8]) {
    let mut out = std::io::stdout().lock();
    if let Err(error) = out.write_all(line).and_then(|()| out.flush()) {
        report_sink_failure(&error);
    }
}

fn push_key(out: &mut Vec<u8>, name: &str) {
    out.extend_from_slice(b",\"");
    out.extend_from_slice(name.strip_prefix("r#").unwrap_or(name).as_bytes());
    out.extend_from_slice(b"\":");
}

#[doc(hidden)]
pub(crate) fn field_raw(out: &mut Vec<u8>, name: &str, value: impl Serialize) {
    push_key(out, name);
    let _ = serde_json::to_writer(out, &value);
}

#[doc(hidden)]
pub(crate) fn field_display(out: &mut Vec<u8>, name: &str, value: &dyn fmt::Display) {
    push_key(out, name);
    push_escaped(out, value);
}

#[doc(hidden)]
pub(crate) fn field_debug(out: &mut Vec<u8>, name: &str, value: &dyn fmt::Debug) {
    push_key(out, name);
    push_escaped(out, &Displayed(value));
}

fn push_escaped(out: &mut Vec<u8>, value: &dyn fmt::Display) {
    let _ = serde_json::Serializer::new(out).collect_str(value);
}

struct Displayed<'a>(&'a dyn fmt::Debug);

impl fmt::Display for Displayed<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

#[doc(hidden)]
pub(crate) fn finish(mut out: Vec<u8>, message: fmt::Arguments<'_>) {
    push_key(&mut out, "message");
    match message.as_str() {
        Some(literal) => {
            let _ = serde_json::to_writer(&mut out, literal);
        }
        None => push_escaped(&mut out, &message),
    }
    finish_bare(out);
}

#[doc(hidden)]
pub(crate) fn finish_bare(mut out: Vec<u8>) {
    out.extend_from_slice(b"}\n");
    emit(&out);
    return_spare(out);
}

macro_rules! log_line {
    ($ordinal:expr, $level:literal; $($rest:tt)*) => {{
        if $ordinal >= $crate::logging::MIN_LEVEL {
            let mut line = $crate::logging::begin($level);
            $crate::logging::log_line!(@field line; $($rest)*)
        }
    }};

    (@field $line:ident; % $key:ident, $($rest:tt)*) => {{
        $crate::logging::field_display(&mut $line, stringify!($key), &$key);
        $crate::logging::log_line!(@field $line; $($rest)*)
    }};
    (@field $line:ident; ? $key:ident, $($rest:tt)*) => {{
        $crate::logging::field_debug(&mut $line, stringify!($key), &$key);
        $crate::logging::log_line!(@field $line; $($rest)*)
    }};
    (@field $line:ident; $key:ident = % $val:expr, $($rest:tt)*) => {{
        $crate::logging::field_display(&mut $line, stringify!($key), &$val);
        $crate::logging::log_line!(@field $line; $($rest)*)
    }};
    (@field $line:ident; $key:ident = ? $val:expr, $($rest:tt)*) => {{
        $crate::logging::field_debug(&mut $line, stringify!($key), &$val);
        $crate::logging::log_line!(@field $line; $($rest)*)
    }};
    (@field $line:ident; $key:ident = $val:expr, $($rest:tt)*) => {{
        $crate::logging::field_raw(&mut $line, stringify!($key), &$val);
        $crate::logging::log_line!(@field $line; $($rest)*)
    }};
    (@field $line:ident; $key:ident, $($rest:tt)*) => {{
        $crate::logging::field_raw(&mut $line, stringify!($key), &$key);
        $crate::logging::log_line!(@field $line; $($rest)*)
    }};
    (@field $line:ident; $fmt:literal $(, $arg:expr)* $(,)?) => {{
        $crate::logging::finish($line, format_args!($fmt $(, $arg)*))
    }};
    (@field $line:ident;) => {{
        $crate::logging::finish_bare($line)
    }};
}
pub(crate) use log_line;

macro_rules! info {
    ($($t:tt)*) => { $crate::logging::log_line!($crate::logging::level::INFO, "INFO"; $($t)*) };
}
macro_rules! debug {
    ($($t:tt)*) => { $crate::logging::log_line!($crate::logging::level::DEBUG, "DEBUG"; $($t)*) };
}
macro_rules! error {
    ($($t:tt)*) => { $crate::logging::log_line!($crate::logging::level::ERROR, "ERROR"; $($t)*) };
}
macro_rules! warn_ {
    ($($t:tt)*) => { $crate::logging::log_line!($crate::logging::level::WARN, "WARN"; $($t)*) };
}
pub(crate) use debug;
pub(crate) use error;
pub(crate) use info;
pub(crate) use warn_ as warn;

#[cfg(test)]
#[path = "logging_tests.rs"]
mod tests;
