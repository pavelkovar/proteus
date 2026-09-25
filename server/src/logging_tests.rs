use super::*;

/// Tests that write batches move the global `WRITTEN` counter.
static WRITTEN_COUNTER: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn a_batch_writes_whole_lines_and_counts_only_what_the_sink_took() {
    let _counter = WRITTEN_COUNTER.lock().unwrap_or_else(|e| e.into_inner());
    let line = vec![b'x'; 1000];
    let mut batch = Batch {
        bytes: Vec::new(),
        lines: 0,
    };

    let mut sink: Vec<Vec<u8>> = Vec::new();
    let before = WRITTEN.load(Ordering::Acquire);
    for _ in 0..9 {
        batch.push(&line, &mut CapturingWriter(&mut sink));
    }
    batch.write(&mut CapturingWriter(&mut sink));

    assert_eq!(WRITTEN.load(Ordering::Acquire) - before, 9);
    assert_eq!(sink.iter().map(Vec::len).sum::<usize>(), 9000);
    for write in &sink {
        assert!(write.len() <= MAX_BATCH, "a write may not exceed PIPE_BUF");
        assert_eq!(
            write.len() % line.len(),
            0,
            "a line may not be split across writes"
        );
    }

    let before = WRITTEN.load(Ordering::Acquire);
    batch.push(&line, &mut FailingWriter);
    batch.write(&mut FailingWriter);
    assert_eq!(
        WRITTEN.load(Ordering::Acquire),
        before,
        "a refused write is not a write"
    );
}

#[test]
fn a_drain_writes_everything_queued_and_reports_when_senders_are_gone() {
    let _counter = WRITTEN_COUNTER.lock().unwrap_or_else(|e| e.into_inner());
    let (sender, receiver) = std::sync::mpsc::sync_channel(16);
    let mut batch = Batch {
        bytes: Vec::new(),
        lines: 0,
    };
    let mut sink: Vec<Vec<u8>> = Vec::new();

    assert_eq!(
        drain_queued(&receiver, &mut batch, &mut CapturingWriter(&mut sink)),
        Drained::Nothing
    );
    assert!(sink.is_empty());

    sender.try_send(b"a\n".to_vec()).unwrap();
    sender.try_send(b"b\n".to_vec()).unwrap();
    assert_eq!(
        drain_queued(&receiver, &mut batch, &mut CapturingWriter(&mut sink)),
        Drained::Lines
    );
    assert_eq!(sink.concat(), b"a\nb\n");

    sender.try_send(b"c\n".to_vec()).unwrap();
    drop(sender);
    assert_eq!(
        drain_queued(&receiver, &mut batch, &mut CapturingWriter(&mut sink)),
        Drained::Disconnected
    );
    assert_eq!(
        sink.concat(),
        b"a\nb\nc\n",
        "lines queued before the last sender left are still written"
    );
}

struct CapturingWriter<'a>(&'a mut Vec<Vec<u8>>);

impl std::io::Write for CapturingWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.push(buf.to_vec());
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct FailingWriter;

impl std::io::Write for FailingWriter {
    fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
        Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn set_min_level_changes_what_min_level_reports() {
    let original = min_level();
    set_min_level(level::ERROR);
    assert_eq!(min_level(), level::ERROR);

    set_min_level(level::DEBUG);
    assert_eq!(min_level(), level::DEBUG);

    set_min_level(original);
}

#[test]
fn record_is_flat_json_with_the_fields_in_call_order() {
    let mut line = begin("INFO");
    field_raw(&mut line, "r#type", "controller");
    field_raw(&mut line, "pid", 42u32);
    field_raw(&mut line, "dropped", true);
    field_display(&mut line, "error", &"quote\" and \\ and \n");
    field_debug(&mut line, "reason", &Some(7u8));
    line.extend_from_slice(b"}\n");

    let text = String::from_utf8(line).expect("a log line is always UTF-8");
    let (timestamp, rest) = text
        .split_once("\",\"level\"")
        .expect("timestamp comes first");
    assert_eq!(
        timestamp.len(),
        "{\"timestamp\":\"1970-01-01T00:00:00.000000Z".len()
    );
    assert_eq!(
        rest,
        ":\"INFO\",\"type\":\"controller\",\"pid\":42,\"dropped\":true,\
         \"error\":\"quote\\\" and \\\\ and \\n\",\"reason\":\"Some(7)\"}\n"
    );

    let parsed: serde_json::Value = serde_json::from_str(&text).expect("a record parses back");
    assert!(
        parsed.get("message").is_none(),
        "a fields-only record carries no message"
    );
}
