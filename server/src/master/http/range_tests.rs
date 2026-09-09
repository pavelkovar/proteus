use super::*;
use tokio_stream::StreamExt;

fn write_temp_file(name: &str, content: &[u8]) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("range-test-{name}-{}", std::process::id()));
    std::fs::write(&path, content).unwrap();
    path
}

#[test]
fn parse_range_first_last() {
    assert_eq!(parse_range("bytes=0-99", 1000), Some(Ok((0, 99))));
    assert_eq!(parse_range("bytes=100-199", 1000), Some(Ok((100, 199))));
}

#[test]
fn parse_range_open_ended_reads_to_the_end() {
    assert_eq!(parse_range("bytes=900-", 1000), Some(Ok((900, 999))));
}

#[test]
fn parse_range_suffix_reads_the_last_n_bytes() {
    assert_eq!(parse_range("bytes=-100", 1000), Some(Ok((900, 999))));
}

#[test]
fn parse_range_suffix_larger_than_file_clamps_to_the_whole_file() {
    assert_eq!(parse_range("bytes=-5000", 1000), Some(Ok((0, 999))));
}

#[test]
fn parse_range_end_clamps_to_the_last_byte() {
    assert_eq!(parse_range("bytes=500-99999", 1000), Some(Ok((500, 999))));
}

#[test]
fn parse_range_start_past_eof_is_unsatisfiable() {
    assert_eq!(parse_range("bytes=1000-1005", 1000), Some(Err(())));
    assert_eq!(parse_range("bytes=0-99", 0), Some(Err(())), "nothing to range over in an empty file");
}

#[test]
fn parse_range_end_before_start_is_unsatisfiable() {
    assert_eq!(parse_range("bytes=500-100", 1000), Some(Err(())));
}

#[test]
fn parse_range_zero_length_suffix_is_unsatisfiable() {
    assert_eq!(parse_range("bytes=-0", 1000), Some(Err(())));
}

#[test]
fn parse_range_multi_range_is_ignored_not_rejected() {
    // Multi-range is ignored like an absent header, not a 416.
    assert_eq!(parse_range("bytes=0-10,20-30", 1000), None);
}

#[test]
fn parse_range_malformed_is_ignored() {
    assert_eq!(parse_range("not-a-range", 1000), None);
    assert_eq!(parse_range("bytes=abc-def", 1000), None);
    assert_eq!(parse_range("bytes=", 1000), None);
}

#[test]
fn parse_range_malformed_syntax_is_ignored_the_same_way_regardless_of_file_length() {
    // The empty-file check must not fire before the syntax is confirmed, or
    // identical garbage gets a different outcome purely from the file's
    // length.
    for garbage in ["bytes=-", "bytes=abc-def", "bytes="] {
        assert_eq!(parse_range(garbage, 0), None, "garbage {garbage:?} against an empty file");
        assert_eq!(parse_range(garbage, 1000), None, "garbage {garbage:?} against a non-empty file");
    }
}

#[test]
fn parse_range_well_formed_range_against_an_empty_file_is_still_unsatisfiable() {
    // A well-formed range genuinely cannot be satisfied here.
    assert_eq!(parse_range("bytes=0-99", 0), Some(Err(())));
    assert_eq!(parse_range("bytes=-100", 0), Some(Err(())));
    assert_eq!(parse_range("bytes=0-", 0), Some(Err(())));
}

#[tokio::test]
async fn range_body_streams_exactly_the_requested_bytes() {
    let content: Vec<u8> = (0..10_000u32).map(|i| (i % 256) as u8).collect();
    let path = write_temp_file("range-body", &content);
    let file = std::fs::File::open(&path).unwrap();

    let mut stream = FileBody::new(file, 100, 500);
    let mut collected = Vec::new();
    while let Some(chunk) = stream.next().await {
        collected.extend_from_slice(&chunk.unwrap());
    }

    assert_eq!(collected, content[100..600]);
    std::fs::remove_file(&path).ok();
}

/// The length reaches the client as `Content-Length` before the body is read,
/// so coming up short has to fail the stream rather than end it early and
/// leave the connection desynced.
#[tokio::test]
async fn file_body_fails_rather_than_truncating_a_file_shorter_than_promised() {
    let content: Vec<u8> = (0..5000u32).map(|i| (i % 256) as u8).collect();
    let path = write_temp_file("file-body-short", &content);
    let file = std::fs::File::open(&path).unwrap();

    let mut stream = FileBody::new(file, 0, content.len() as u64 + 4096);
    let mut collected = Vec::new();
    let mut failure = None;
    while let Some(item) = stream.next().await {
        match item {
            Ok(bytes) => collected.extend_from_slice(&bytes),
            Err(e) => {
                failure = Some(e);
                break;
            }
        }
    }

    assert_eq!(collected, content, "everything the file did hold must still arrive");
    assert_eq!(
        failure.expect("a file shorter than promised must fail the stream").kind(),
        std::io::ErrorKind::UnexpectedEof
    );
    assert!(stream.next().await.is_none(), "a failed stream must stay ended");
    std::fs::remove_file(&path).ok();
}

/// The other direction: a file appended to after the `stat` must not overrun
/// the `Content-Length` already sent, which would desync the connection just
/// as badly as coming up short.
#[tokio::test]
async fn file_body_stops_at_the_promised_length_when_the_file_grew() {
    let content: Vec<u8> = (0..200_000u32).map(|i| (i % 256) as u8).collect();
    let path = write_temp_file("file-body-grown", &content);
    let file = std::fs::File::open(&path).unwrap();

    // Not a chunk multiple, so the last read has to be clamped to the promise
    // rather than filled.
    let promised = 130_000u64;
    let mut stream = FileBody::new(file, 0, promised);
    let mut collected = Vec::new();
    while let Some(item) = stream.next().await {
        collected.extend_from_slice(&item.expect("a longer file must not fail the stream"));
    }

    assert_eq!(collected.len() as u64, promised, "must send exactly what the header promised");
    assert_eq!(collected, content[..promised as usize]);
    std::fs::remove_file(&path).ok();
}

#[tokio::test]
async fn range_body_reads_across_multiple_chunks() {
    // More than one chunk, so this is not just a single read.
    let len = COALESCE_FLUSH_THRESHOLD * 2 + 1234;
    let content: Vec<u8> = (0..len).map(|i| (i % 256) as u8).collect();
    let path = write_temp_file("range-body-multi", &content);
    let file = std::fs::File::open(&path).unwrap();

    let mut stream = FileBody::new(file, 0, len as u64);
    let mut collected = Vec::new();
    while let Some(chunk) = stream.next().await {
        collected.extend_from_slice(&chunk.unwrap());
    }

    assert_eq!(collected, content);
    std::fs::remove_file(&path).ok();
}
