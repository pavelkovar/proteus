use super::*;
use std::os::fd::AsRawFd;
use std::time::Duration;
use tokio::io::unix::AsyncFd;

fn small_headers(n: usize) -> HeaderBlob<'static> {
    let mut blob = HeaderBlob::default();
    for i in 0..n {
        blob.push(&format!("X-Test-{i}"), &format!("value-{i}"));
    }
    blob
}

/// Rebuilds an owned blob from a borrowed piece, so a split can be
/// reassembled and compared against the original.
fn blob_from_ref(r: &HeaderBlobRef<'_>) -> HeaderBlob<'static> {
    let wire = postcard::to_allocvec(r).unwrap();
    let decoded: HeaderBlob<'_> = postcard::from_bytes(&wire).unwrap();

    decoded.into_owned()
}

fn cookie_blob(n: usize, vsize: usize) -> HeaderBlob<'static> {
    let mut blob = HeaderBlob::default();
    for i in 0..n {
        blob.push("Set-Cookie", &format!("cookie_{i}={}", "v".repeat(vsize)));
    }
    blob
}

#[test]
fn split_headers_into_frames_is_one_chunk_for_ordinary_header_sets() {
    let headers = small_headers(10);
    let frames = split_headers_into_frames(&headers);
    assert_eq!(frames.len(), 1);
    assert_eq!(blob_from_ref(&frames[0]), headers);
}

#[test]
fn split_headers_into_frames_splits_large_sets_and_preserves_order_and_content() {
    // Comfortably past the response ring's capacity.
    let headers = cookie_blob(3000, 80);

    let frames = split_headers_into_frames(&headers);
    assert!(
        frames.len() > 1,
        "expected more than one frame, got {}",
        frames.len()
    );

    let mut reassembled = HeaderBlob::default();
    let last = frames.len() - 1;
    for (i, chunk) in frames.iter().enumerate() {
        // Each chunk must itself fit one frame.
        let frame = ResponseFrameRef::Headers {
            status: 200,
            headers: HeaderBlobRef(chunk.0),
            more: i != last,
        };
        let bytes = postcard::to_allocvec(&frame).unwrap();
        assert!(
            bytes.len() <= shm::RESPONSE_RING_CAPACITY - 4,
            "a single chunk must fit in one ring frame"
        );
        reassembled.append(&blob_from_ref(chunk));
    }
    assert_eq!(
        reassembled, headers,
        "order and content must survive the split"
    );
}

#[test]
fn split_headers_into_frames_keeps_one_oversized_entry_whole() {
    // There is no way to split one pair across frames.
    let big_value = "v".repeat(100_000);
    let mut headers = HeaderBlob::default();
    headers.push("Content-Security-Policy", &big_value);

    let frames = split_headers_into_frames(&headers);
    assert_eq!(frames.len(), 1);
    let got = blob_from_ref(&frames[0]);
    assert_eq!(got.iter().count(), 1);
    assert_eq!(
        got.iter().next(),
        Some(("Content-Security-Policy", big_value.as_str()))
    );
}

#[test]
fn write_headers_to_ring_fails_cleanly_on_one_header_value_too_big_for_any_frame() {
    // A single pair too big for the ring has no smaller grouping to fall
    // back to, and must fail plainly rather than panic or hang.
    let (fd, prototype_side) = shm::create_channel().unwrap();
    let efd = shm::create_notify_eventfd().unwrap();
    let efd_raw = efd.as_raw_fd();
    let peer = &prototype_side.channel().peer_death;

    let one_giant_value = "v".repeat(shm::RESPONSE_RING_CAPACITY + 1);
    let mut headers = HeaderBlob::default();
    headers.push("X-Too-Big", &one_giant_value);

    let mut scratch = Vec::new();
    let err = write_headers_to_ring(
        &prototype_side.channel().response,
        peer,
        200,
        &headers,
        &mut scratch,
        efd_raw,
    )
    .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

    drop(fd);
}

#[tokio::test]
async fn write_headers_to_ring_handles_a_ten_megabyte_header_set_via_many_small_entries() {
    // A set of many reasonable entries has no ceiling, only however many
    // round-trips it takes.
    let (fd, prototype_side) = shm::create_channel().unwrap();
    let master_side = shm::map_existing_channel(fd).unwrap();
    let data_efd_owned = shm::create_notify_eventfd().unwrap();
    let data_efd_raw = data_efd_owned.as_raw_fd();
    let data_efd = AsyncFd::new(data_efd_owned).unwrap();

    // Large enough to force several round-trips, not just one.
    let headers = cookie_blob(2000, 5300);
    let total_bytes = headers.byte_len();
    assert!(
        total_bytes > 10 * 1024 * 1024,
        "test setup should exceed 10MB, got {total_bytes}"
    );
    let expected_frame_count = split_headers_into_frames(&headers).len();

    let headers_for_writer = headers.clone();
    let writer = std::thread::spawn(move || {
        let peer = &prototype_side.channel().peer_death;
        let mut scratch = Vec::new();
        write_headers_to_ring(
            &prototype_side.channel().response,
            peer,
            200,
            &headers_for_writer,
            &mut scratch,
            data_efd_raw,
        )
        .unwrap();
    });

    let master_peer = &master_side.channel().peer_death;
    let mut reassembled_headers = HeaderBlob::default();
    let mut scratch = Vec::new();
    let mut frame_count = 0;
    while frame_count < expected_frame_count {
        tokio::time::timeout(
            Duration::from_secs(20),
            master_side
                .channel()
                .response
                .read_frame_async(&mut scratch, master_peer, &data_efd),
        )
        .await
        .expect("should not hang")
        .unwrap();
        let frame: ResponseFrame = postcard::from_bytes(&scratch).unwrap();
        let ResponseFrame::Headers {
            headers: chunk,
            more,
            ..
        } = frame
        else {
            panic!("expected Headers")
        };
        frame_count += 1;
        assert_eq!(
            more,
            frame_count < expected_frame_count,
            "more should be false only on the final frame"
        );
        reassembled_headers.append(&chunk);
    }
    tokio::task::spawn_blocking(move || writer.join().unwrap())
        .await
        .unwrap();

    assert_eq!(reassembled_headers, headers);
}

#[tokio::test]
async fn write_headers_to_ring_then_read_back_reassembles_correctly() {
    // The real pairing only. A blocking write notifies the eventfd and never
    // a futex, so pairing it with a blocking reader loses the wakeup outright
    // the moment that reader parks - which this header set is large enough to
    // force. The timeout turns a regression into a failure, not a hang.
    let (fd, prototype_side) = shm::create_channel().unwrap();
    let master_side = shm::map_existing_channel(fd).unwrap();
    let data_efd_owned = shm::create_notify_eventfd().unwrap();
    let data_efd_raw = data_efd_owned.as_raw_fd();
    let data_efd = AsyncFd::new(data_efd_owned).unwrap();

    let headers = cookie_blob(3000, 80);
    let expected_frame_count = split_headers_into_frames(&headers).len();

    // On its own thread, because this header set outgrows the ring and the
    // write blocks once it fills, exactly as a real worker's does.
    let headers_for_writer = headers.clone();
    let writer = std::thread::spawn(move || {
        let peer = &prototype_side.channel().peer_death;
        let mut scratch = Vec::new();
        write_headers_to_ring(
            &prototype_side.channel().response,
            peer,
            200,
            &headers_for_writer,
            &mut scratch,
            data_efd_raw,
        )
        .unwrap();
    });

    let master_peer = &master_side.channel().peer_death;
    let mut reassembled_status = None;
    let mut reassembled_headers = HeaderBlob::default();
    let mut scratch = Vec::new();
    let mut frame_count = 0;
    // Nothing but headers was written, so read exactly the promised count.
    while frame_count < expected_frame_count {
        tokio::time::timeout(
            Duration::from_secs(10),
            master_side
                .channel()
                .response
                .read_frame_async(&mut scratch, master_peer, &data_efd),
        )
        .await
        .expect("should not hang - see this test's own doc comment")
        .unwrap();
        let frame: ResponseFrame = postcard::from_bytes(&scratch).unwrap();
        let ResponseFrame::Headers {
            status,
            headers: chunk,
            more,
        } = frame
        else {
            panic!("expected Headers")
        };
        frame_count += 1;
        assert_eq!(
            more,
            frame_count < expected_frame_count,
            "more should be false only on the final frame"
        );
        reassembled_status.get_or_insert(status);
        reassembled_headers.append(&chunk);
    }
    tokio::task::spawn_blocking(move || writer.join().unwrap())
        .await
        .unwrap();

    assert!(
        frame_count > 1,
        "this header set should have needed more than one frame"
    );
    assert_eq!(reassembled_status, Some(200));
    assert_eq!(reassembled_headers, headers);
}

// --- borrowed write path (`ResponseFrameRef`, `HeaderBlob`) ---

/// `ResponseFrameRef` must serialize to exactly what the owned form would,
/// since that is what the far side decodes into. Nothing in the type system
/// enforces it, so every variant is pinned here.
#[test]
fn response_frame_ref_matches_owned_encoding() {
    let mut headers = HeaderBlob::default();
    headers.push("X-A", "1");
    headers.push("Set-Cookie", "b=2");
    // Straddling postcard's varint boundaries, where a divergence would hide.
    let bodies: Vec<Vec<u8>> = [0usize, 1, 127, 128, 16_383, 16_384, 70_000]
        .iter()
        .map(|&n| (0..n).map(|i| (i % 251) as u8).collect())
        .collect();

    for more in [false, true] {
        let owned = ResponseFrame::Headers {
            status: 207,
            headers: headers.clone(),
            more,
        };
        let borrowed = ResponseFrameRef::Headers {
            status: 207,
            headers: (&headers).into(),
            more,
        };
        assert_eq!(
            postcard::to_allocvec(&owned).unwrap(),
            postcard::to_allocvec(&borrowed).unwrap(),
            "Headers encoding diverged (more={more})"
        );
    }
    for body in &bodies {
        let owned = ResponseFrame::Body(Cow::Owned(body.clone()));
        let borrowed = ResponseFrameRef::Body(body);
        assert_eq!(
            postcard::to_allocvec(&owned).unwrap(),
            postcard::to_allocvec(&borrowed).unwrap(),
            "Body encoding diverged at len={}",
            body.len()
        );
    }
    for retiring in [false, true] {
        assert_eq!(
            postcard::to_allocvec(&ResponseFrame::End { retiring }).unwrap(),
            postcard::to_allocvec(&ResponseFrameRef::End { retiring }).unwrap(),
        );
    }
}

/// Data is already present, so the blocking read never parks.
fn drain_frames(channel: &shm::Channel, efd: RawFd, count: usize) -> Vec<ResponseFrame<'static>> {
    let mut scratch = Vec::new();
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        channel
            .response
            .read_frame(&mut scratch, &channel.peer_death, efd)
            .unwrap();
        let frame: ResponseFrame<'_> = postcard::from_bytes(&scratch).unwrap();

        out.push(frame.into_owned());
    }
    out
}

/// However large a single `ub_write` was, every frame reaching the wire
/// stays within `chunk_size` and the bytes come back identical.
#[test]
fn write_body_to_ring_bounds_every_frame_and_preserves_bytes() {
    let (fd, prototype_side) = shm::create_channel().unwrap();
    let master_side = shm::map_existing_channel(fd).unwrap();
    let efd = shm::create_notify_eventfd().unwrap();
    let efd_raw = efd.as_raw_fd();
    let channel = prototype_side.channel();

    // Several frames that all fit at once, so nothing parks and this can
    // stay single-threaded.
    const CHUNK: usize = 1024;
    let bytes: Vec<u8> = (0..CHUNK * 4 + 17).map(|i| (i % 251) as u8).collect();
    let expected_frames = bytes.len().div_ceil(CHUNK);

    let mut scratch = Vec::new();
    write_body_to_ring(
        &channel.response,
        &channel.peer_death,
        &bytes,
        CHUNK,
        &mut scratch,
        efd_raw,
    )
    .unwrap();

    let frames = drain_frames(master_side.channel(), efd_raw, expected_frames);
    assert!(
        frames.len() > 1,
        "the oversized input should actually have split"
    );

    let mut reassembled = Vec::new();
    for frame in frames {
        let ResponseFrame::Body(chunk) = frame else {
            panic!("write_body_to_ring must only produce Body frames")
        };
        assert!(
            chunk.len() <= CHUNK,
            "frame of {} bytes exceeds chunk_size {CHUNK}",
            chunk.len()
        );
        reassembled.extend_from_slice(&chunk);
    }
    assert_eq!(reassembled, bytes);
}

#[test]
fn write_body_to_ring_handles_empty_and_boundary_sized_input() {
    let (fd, prototype_side) = shm::create_channel().unwrap();
    let master_side = shm::map_existing_channel(fd).unwrap();
    let efd = shm::create_notify_eventfd().unwrap();
    let efd_raw = efd.as_raw_fd();
    let channel = prototype_side.channel();
    const CHUNK: usize = 1024;
    let mut scratch = Vec::new();

    // Empty input must write nothing at all, not a zero-length Body frame.
    write_body_to_ring(
        &channel.response,
        &channel.peer_death,
        &[],
        CHUNK,
        &mut scratch,
        efd_raw,
    )
    .unwrap();
    // A zero-length frame is indistinguishable from the worker-done marker,
    // so this checks by writing a sentinel next and getting it back first.
    write_body_to_ring(
        &channel.response,
        &channel.peer_death,
        b"sentinel",
        CHUNK,
        &mut scratch,
        efd_raw,
    )
    .unwrap();
    let frames = drain_frames(master_side.channel(), efd_raw, 1);
    assert!(
        matches!(&frames[0], ResponseFrame::Body(b) if b.as_ref() == b"sentinel"),
        "empty input must write no frames"
    );

    let exact = vec![7u8; CHUNK];
    write_body_to_ring(
        &channel.response,
        &channel.peer_death,
        &exact,
        CHUNK,
        &mut scratch,
        efd_raw,
    )
    .unwrap();
    let frames = drain_frames(master_side.channel(), efd_raw, 1);
    assert!(matches!(&frames[0], ResponseFrame::Body(b) if b.len() == CHUNK));

    let one_over = vec![7u8; CHUNK + 1];
    write_body_to_ring(
        &channel.response,
        &channel.peer_death,
        &one_over,
        CHUNK,
        &mut scratch,
        efd_raw,
    )
    .unwrap();
    let frames = drain_frames(master_side.channel(), efd_raw, 2);
    assert!(matches!(&frames[0], ResponseFrame::Body(b) if b.len() == CHUNK));
    assert!(matches!(&frames[1], ResponseFrame::Body(b) if b.len() == 1));
}

/// The scratch buffer is reused for a worker's whole life, so a shorter
/// frame must not leave the previous one's tail behind.
#[test]
fn a_reused_scratch_buffer_does_not_leak_the_previous_frame() {
    let (fd, prototype_side) = shm::create_channel().unwrap();
    let master_side = shm::map_existing_channel(fd).unwrap();
    let efd = shm::create_notify_eventfd().unwrap();
    let efd_raw = efd.as_raw_fd();
    let channel = prototype_side.channel();

    let mut scratch = Vec::new();
    let long = vec![9u8; 4096];
    write_response_frame_to_ring(
        &channel.response,
        &channel.peer_death,
        &ResponseFrameRef::Body(&long),
        &mut scratch,
        efd_raw,
    )
    .unwrap();
    write_response_frame_to_ring(
        &channel.response,
        &channel.peer_death,
        &ResponseFrameRef::Body(b"x"),
        &mut scratch,
        efd_raw,
    )
    .unwrap();
    write_response_frame_to_ring(
        &channel.response,
        &channel.peer_death,
        &ResponseFrameRef::End { retiring: false },
        &mut scratch,
        efd_raw,
    )
    .unwrap();

    let frames = drain_frames(master_side.channel(), efd_raw, 3);
    assert!(matches!(&frames[0], ResponseFrame::Body(b) if b.len() == 4096));
    assert!(
        matches!(&frames[1], ResponseFrame::Body(b) if b.as_ref() == b"x"),
        "short frame picked up stale bytes: {:?}",
        frames[1]
    );
    assert!(matches!(frames[2], ResponseFrame::End { retiring: false }));
}

// --- HeaderBlob ---

#[test]
fn header_blob_round_trips_names_and_values_in_order() {
    let mut blob = HeaderBlob::with_capacity(64);
    blob.push("Host", "example.com");
    blob.push("X-Empty", "");
    blob.push("Set-Cookie", "a=1");
    blob.push("Set-Cookie", "b=2"); // repeats are legal and must both survive

    let got: Vec<(&str, &str)> = blob.iter().collect();
    assert_eq!(
        got,
        vec![
            ("Host", "example.com"),
            ("X-Empty", ""),
            ("Set-Cookie", "a=1"),
            ("Set-Cookie", "b=2")
        ]
    );

    let encoded = postcard::to_allocvec(&blob).unwrap();
    let decoded: HeaderBlob = postcard::from_bytes(&encoded).unwrap();
    assert_eq!(decoded, blob);
    assert_eq!(decoded.iter().collect::<Vec<_>>(), got);
}

#[test]
fn header_blob_is_empty_when_nothing_was_pushed() {
    let blob = HeaderBlob::default();
    assert_eq!(blob.iter().count(), 0);
    assert_eq!(blob.byte_len(), 0);
}

/// A NUL would terminate an entry early and silently re-pair every following
/// name and value, which is worse than dropping the one bad header.
#[test]
fn header_blob_drops_an_entry_containing_a_nul_without_disturbing_its_neighbours() {
    let mut blob = HeaderBlob::with_capacity(64);
    blob.push("Before", "ok");
    blob.push("Bad", "has\0nul");
    blob.push("Also-Bad\0", "value");
    blob.push("After", "also-ok");

    assert_eq!(
        blob.iter().collect::<Vec<_>>(),
        vec![("Before", "ok"), ("After", "also-ok")]
    );
}

/// A corrupt blob arrives across an IPC boundary, so iteration must stop
/// cleanly rather than panic or mis-pair an entry.
#[test]
fn header_blob_iteration_stops_cleanly_on_a_malformed_blob() {
    // Hand-built, since `push` cannot produce a malformed blob.
    let from_raw = |raw: &[u8]| -> HeaderBlob<'static> {
        let wire = postcard::to_allocvec(&serde_bytes::Bytes::new(raw)).unwrap();
        let decoded: HeaderBlob<'_> = postcard::from_bytes(&wire).unwrap();
        decoded.into_owned()
    };

    // The same bytes, well-formed, do yield both pairs.
    let well_formed = from_raw(b"Host\0example.com\0X-Ok\0v\0");
    assert_eq!(well_formed.iter().count(), 2);

    let missing_terminator = from_raw(b"Host\0example.com\0X-Trunc\0no-terminator");
    assert_eq!(
        missing_terminator.iter().collect::<Vec<_>>(),
        vec![("Host", "example.com")],
        "an unterminated trailing value must be dropped, not guessed at"
    );

    let invalid_utf8 = from_raw(b"Host\0\xff\xfe\0X-After\0v\0");
    assert_eq!(
        invalid_utf8.iter().count(),
        0,
        "iteration must stop at the first non-UTF-8 pair"
    );
}

/// The same invariant one level down: a borrowed piece must serialize as the
/// owned form would, or a split header set decodes to garbage.
#[test]
fn header_blob_ref_matches_owned_encoding() {
    for n in [0usize, 1, 8, 400] {
        let blob = small_headers(n);
        let borrowed: HeaderBlobRef<'_> = (&blob).into();
        assert_eq!(
            postcard::to_allocvec(&blob).unwrap(),
            postcard::to_allocvec(&borrowed).unwrap(),
            "encoding diverged at {n} headers"
        );
    }
}

/// Cuts may only land between entries: one inside would hand the far side a
/// name with no value, or re-pair every following header.
#[test]
fn split_at_budget_never_tears_an_entry_and_reassembles_exactly() {
    // Uneven, so cuts do not all land on a convenient multiple.
    let mut blob = HeaderBlob::default();
    for i in 0..200 {
        blob.push(&format!("X-H-{i}"), &"v".repeat(1 + (i * 7) % 50));
    }

    for budget in [1usize, 16, 64, 257, 1024, 100_000] {
        let pieces = blob.split_at_budget(budget);
        let mut reassembled = HeaderBlob::default();
        for piece in &pieces {
            let owned = blob_from_ref(piece);
            // `iter` stops at a torn pair, so parsing cleanly is the proof.
            let parsed: Vec<_> = owned.iter().collect();
            assert_eq!(
                parsed
                    .iter()
                    .map(|(n, v)| n.len() + v.len() + 2)
                    .sum::<usize>(),
                owned.byte_len(),
                "budget {budget}: a piece did not parse back to its own full byte length - an entry was torn"
            );
            reassembled.append(&owned);
        }
        assert_eq!(
            reassembled, blob,
            "budget {budget}: reassembly must be byte-exact"
        );
        assert_eq!(
            reassembled.iter().count(),
            200,
            "budget {budget}: every header must survive"
        );
    }
}

/// A budget smaller than one entry cannot be honoured, so that entry gets
/// its own oversized piece rather than being split or dropped.
#[test]
fn split_at_budget_keeps_an_over_budget_entry_whole_rather_than_dropping_it() {
    let mut blob = HeaderBlob::default();
    blob.push("Small", "x");
    blob.push("Huge", &"v".repeat(1000));
    blob.push("Also-Small", "y");

    let pieces = blob.split_at_budget(64);
    let mut reassembled = HeaderBlob::default();
    for piece in &pieces {
        reassembled.append(&blob_from_ref(piece));
    }
    assert_eq!(reassembled, blob);
    assert!(
        pieces.iter().any(|p| p.0.len() > 64),
        "the over-budget entry must survive in a piece of its own"
    );
}

/// Empty input must still yield one piece: the caller indexes the last
/// element to set `more`, and would panic on an empty result.
#[test]
fn split_at_budget_always_yields_at_least_one_piece() {
    assert_eq!(HeaderBlob::default().split_at_budget(64).len(), 1);
    assert_eq!(small_headers(1).split_at_budget(1).len(), 1);
}

/// A decoding worker must borrow out of the ring scratch rather than rebuild
/// it. Asserted structurally rather than by counting allocations, so it holds
/// regardless of allocator behaviour; a stray owned field or a lost
/// `#[serde(borrow)]` reverts it silently.
#[test]
fn a_decoded_request_borrows_every_field_from_the_scratch_buffer() {
    use std::borrow::Cow;

    let mut headers = HeaderBlob::with_capacity(64);
    headers.push("host", "example.com");
    headers.push("user-agent", "curl/8.0");
    let owned = PhpRequest {
        script_path: Cow::Owned("/var/www/app/index.php".into()),
        document_root: Cow::Owned("/var/www/app".into()),
        script_name: Cow::Owned("/index.php".into()),
        path_info: Cow::Owned(String::new()),
        method: Cow::Borrowed("GET"),
        uri: Cow::Owned("/items?page=2".into()),
        query_string: Cow::Owned("page=2".into()),
        content_type: Cow::Owned("application/json".into()),
        headers,
        client_ip: std::net::IpAddr::V4(std::net::Ipv4Addr::new(203, 0, 113, 7)),
        body: RequestBody::Inline(Cow::Owned(vec![7u8; 256])),
        server_name: Cow::Owned("example.com".into()),
        server_port: 443,
        server_protocol: Cow::Borrowed("HTTP/1.1"),
        https: true,
    };

    let wire = postcard::to_allocvec(&owned).unwrap();
    let decoded: PhpRequest<'_> = postcard::from_bytes(&wire).unwrap();

    for (name, field) in [
        ("script_path", &decoded.script_path),
        ("document_root", &decoded.document_root),
        ("script_name", &decoded.script_name),
        ("path_info", &decoded.path_info),
        ("method", &decoded.method),
        ("uri", &decoded.uri),
        ("query_string", &decoded.query_string),
        ("server_name", &decoded.server_name),
        ("server_protocol", &decoded.server_protocol),
        ("content_type", &decoded.content_type),
    ] {
        assert!(
            matches!(field, Cow::Borrowed(_)),
            "{name} was rebuilt instead of borrowed"
        );
    }
    let RequestBody::Inline(body) = &decoded.body else {
        panic!("expected an inline body")
    };
    assert!(
        matches!(body, Cow::Borrowed(_)),
        "the request body was copied instead of borrowed"
    );

    // The content has to survive too, not just the borrowing.
    assert_eq!(decoded.script_path, owned.script_path);
    assert_eq!(
        decoded.headers.iter().collect::<Vec<_>>(),
        vec![("host", "example.com"), ("user-agent", "curl/8.0")]
    );
    assert_eq!(body.len(), 256);
}

/// Master and worker are the same binary, so an encoding change stays
/// invisible until a mixed-version deploy or a stale worker meets a new
/// master.
#[test]
fn the_cow_encoding_is_byte_identical_to_the_owned_one() {
    #[derive(serde::Serialize)]
    struct OwnedShape {
        script_path: String,
        #[serde(with = "serde_bytes")]
        headers: Vec<u8>,
        #[serde(with = "serde_bytes")]
        body: Vec<u8>,
    }
    #[derive(serde::Serialize)]
    struct CowShape<'a> {
        script_path: Cow<'a, str>,
        #[serde(with = "serde_bytes")]
        headers: Cow<'a, [u8]>,
        #[serde(with = "serde_bytes")]
        body: Cow<'a, [u8]>,
    }

    let (path, headers, body) = ("/var/www/index.php", b"host\0x\0".to_vec(), vec![9u8; 300]);
    let owned = OwnedShape {
        script_path: path.into(),
        headers: headers.clone(),
        body: body.clone(),
    };
    let borrowed = CowShape {
        script_path: Cow::Borrowed(path),
        headers: Cow::Borrowed(&headers),
        body: Cow::Borrowed(&body),
    };
    assert_eq!(
        postcard::to_allocvec(&owned).unwrap(),
        postcard::to_allocvec(&borrowed).unwrap()
    );
}

/// Nothing in the real system ever writes an empty frame to the request
/// ring - `write_request_to_ring` is the ring's only writer, and it always
/// postcard-encodes a real `PhpRequest` - but the ring API itself doesn't
/// forbid one. Proves the worker-side reader degrades to a clean decode
/// error rather than a panic, or the silently-wrong `Retire` it used to be.
#[test]
fn read_command_from_ring_treats_an_empty_frame_as_a_decode_error_not_a_retire() {
    use std::alloc::{Layout, alloc};
    let ring: &'static shm::RequestRing = unsafe {
        let ptr = alloc(Layout::new::<shm::RequestRing>()) as *mut shm::RequestRing;
        assert!(!ptr.is_null());
        shm::RequestRing::init_in_place(ptr);
        &*ptr
    };
    let peer: &'static shm::PeerDeath = unsafe {
        let ptr = alloc(Layout::new::<shm::PeerDeath>()) as *mut shm::PeerDeath;
        assert!(!ptr.is_null());
        shm::PeerDeath::init_in_place(ptr);
        &*ptr
    };
    let efd = shm::create_notify_eventfd().unwrap();

    ring.write_frame(&[], peer, efd.as_raw_fd()).unwrap();

    let mut scratch = Vec::new();
    let outcome = match read_command_from_ring(ring, peer, &mut scratch, efd.as_raw_fd(), None) {
        Err(_) => "Err",
        Ok(Some(WorkerCommand::Retire)) => "Ok(Retire)",
        Ok(Some(WorkerCommand::Request(_))) => "Ok(Request)",
        Ok(None) => "Ok(None)",
    };
    assert_eq!(
        outcome, "Err",
        "an empty request-ring frame must be a decode error, not silently Retire"
    );
}
