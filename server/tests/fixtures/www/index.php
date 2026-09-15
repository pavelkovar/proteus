<?php
// Autoload, not require: the crash this reproduces depends on the class
// being compiled through spl_perform_autoload. Must stay ahead of the plain
// /fastcgi-finish branch, whose path is a substring of this one.
if (strpos($_SERVER['REQUEST_URI'], '/fastcgi-finish-autoload') !== false) {
    spl_autoload_register(function ($class) {
        $file = __DIR__ . '/' . $class . '.php';
        if (is_file($file)) {
            require $file;
        }
    });
    $has = FastcgiFinishAutoloadCheck::hasFastcgiFinishRequest();
    echo "has_fastcgi_finish_request=" . var_export($has, true) . "\n";
    exit;
}

// Exits early, so the body and timing stay clean to assert on.
if (strpos($_SERVER['REQUEST_URI'], '/fastcgi-finish') !== false) {
    ignore_user_abort(true);
    // Big enough to be worth compressing, so the early-response path can be
    // shown to take the same compression route as any other.
    if (($_GET['big'] ?? '') === '1') {
        echo str_repeat('x', 2048) . "\n";
    } else {
        echo "quick-response pid=" . getmypid() . "\n";
    }
    $ok = fastcgi_finish_request();
    // Must never reach the client.
    usleep(300000);
    $marker = '/tmp/fastcgi_finish_marker_' . ($_GET['marker'] ?? 'none') . '.txt';
    // Via rename, or a reader polling for the file can catch it between
    // O_TRUNC and the write and read a half-written value.
    file_put_contents($marker . '.tmp', 'background-work-done:' . var_export($ok, true));
    rename($marker . '.tmp', $marker);
    exit;
}

// Emits its first chunk immediately and keeps streaming, opening a window
// where the handler has returned but the body has not finished. Must stay
// ahead of /slow, whose path is a substring of this one.
// `flush()` alone leaves small writes in PHP's own output layer, so a test
// that wants real streaming has to push that too.
if (strpos($_SERVER['REQUEST_URI'], '/flush-stream') !== false) {
    header('Content-Type: text/plain');
    for ($i = 0; $i < 5; $i++) {
        echo "chunk-$i\n";
        @ob_flush();
        @flush();
        usleep(200000);
    }
    exit;
}

if (strpos($_SERVER['REQUEST_URI'], '/slow-stream') !== false) {
    header('Content-Type: text/plain');
    for ($i = 0; $i < 5; $i++) {
        echo "chunk-$i\n";
        @flush();
        usleep(200000);
    }
    exit;
}

// One real chunk, then a sleep long enough to be killed mid-stream.
// Requires output_buffering=0 in the caller's config: this environment's
// default would hold the first chunk until the script ends rather than
// delivering it on flush().
if (strpos($_SERVER['REQUEST_URI'], '/stream-then-die') !== false) {
    header('Content-Type: text/plain');
    echo "first-chunk\n";
    @flush();
    sleep(30);
    echo "should never be reached\n";
    exit;
}

// An ordinary bounded delay, so a test has something in flight.
if (strpos($_SERVER['REQUEST_URI'], '/slow') !== false) {
    usleep(500000);
    echo "slow done\n";
    exit;
}

// A bare Location header with no explicit code picks 303 on
// POST/PUT/DELETE and 302 on GET/HEAD, but only once the SAPI declares
// HTTP/1.1.
if (strpos($_SERVER['REQUEST_URI'], '/redirect') !== false) {
    header('Location: /done');
    exit;
}

// Many separate echo() calls, each reaching the SAPI immediately, so this is
// genuinely multi-chunk rather than one large write.
if (strpos($_SERVER['REQUEST_URI'], '/stream-big') !== false) {
    header('Content-Type: text/plain');
    $chunk = str_repeat('0123456789abcdef', 256); // 4096 bytes
    for ($i = 0; $i < 1000; $i++) { // ~4MB total
        echo $chunk;
    }
    exit;
}

// The opposite of /stream-big: one echo() of a multi-MB string, which must
// still reach the wire as bounded frames.
if (strpos($_SERVER['REQUEST_URI'], '/stream-one-echo') !== false) {
    header('Content-Type: text/plain');
    echo str_repeat('0123456789abcdef', 256 * 1000); // one ~4MB echo() call
    exit;
}

// A header set too big for one ring frame. `n` cookies of `vsize` bytes plus
// one giant CSP value cover both shapes the splitting has to handle.
//
// Callers wanting a large total should raise `vsize` rather than `n`: a real
// HTTP client caps how many distinct response headers it will parse at all,
// whatever our own ring-level limit is.
if (strpos($_SERVER['REQUEST_URI'], '/many-headers') !== false) {
    $n = (int)($_GET['n'] ?? 50);
    $vsize = (int)($_GET['vsize'] ?? 80);
    for ($i = 0; $i < $n; $i++) {
        header('Set-Cookie: cookie_' . $i . '=' . str_repeat('v', $vsize) . '; Path=/', false);
    }
    if (($_GET['csp'] ?? '') === '1') {
        // No trailing separator: header values have their trailing
        // whitespace trimmed as ordinary OWS handling, which would make this
        // an off-by-one trap for whatever compares against it.
        header('Content-Security-Policy: ' . implode('; ', array_fill(0, 5000, "default-src 'self'")));
    }
    echo "headers-ok n=$n\n";
    exit;
}

// header() only rejects CR/LF, not other control bytes, so an app that
// reflects untrusted input into a header can hand master a value that is
// not valid HTTP grammar - must not take the connection down with it.
if (strpos($_SERVER['REQUEST_URI'], '/reflect-header') !== false) {
    header('X-Before: still-here');
    header('X-Reflected: ' . ($_GET['v'] ?? ''));
    header('X-After: also-here');
    echo "reflected\n";
    exit;
}

// Echoes the CGI variables derived from the request target, so a test can
// prove QUERY_STRING still matches REQUEST_URI after the wire stopped
// carrying it separately.
if (strpos($_SERVER['REQUEST_URI'], '/cgi-vars') !== false) {
    header('Content-Type: text/plain');
    echo 'uri=', $_SERVER['REQUEST_URI'], "\n";
    echo 'query=', $_SERVER['QUERY_STRING'] ?? '', "\n";
    echo 'get_a=', $_GET['a'] ?? 'MISSING', "\n";
    exit;
}

// Checks the X-Probe-N headers against the index each one carries, so a lost,
// duplicated or reordered piece of a fragmented header run is caught here
// rather than by a test that only looks at one header.
if (strpos($_SERVER['REQUEST_URI'], '/probe-headers') !== false) {
    header('Content-Type: text/plain');
    $n = 0;
    while (isset($_SERVER['HTTP_X_PROBE_' . $n])) {
        $want = $n . '-';
        if (substr($_SERVER['HTTP_X_PROBE_' . $n], 0, strlen($want)) !== $want) {
            echo 'mismatch@', $n;
            exit;
        }
        $n++;
    }
    echo 'ok:', $n;
    exit;
}

// Echoes the body back verbatim, so a large one can be checked byte-exact
// across the disk-spillover path rather than only by length.
if (strpos($_SERVER['REQUEST_URI'], '/echo-body') !== false) {
    header('Content-Type: application/octet-stream');
    echo file_get_contents('php://input');
    exit;
}

// The script compresses the body itself, exactly as ob_gzhandler would, and
// says so. Master must leave both alone rather than encode them again.
if (strpos($_SERVER['REQUEST_URI'], '/pre-encoded') !== false) {
    $payload = str_repeat('compress me please ', 400);
    header('Content-Type: text/plain');
    header('Content-Encoding: gzip');
    echo gzencode($payload);
    exit;
}

// Declares an accurate Content-Length. Master must never forward the header
// itself, framing always being its own, but may use it to decide whether the
// response is worth compressing.
if (strpos($_SERVER['REQUEST_URI'], '/declared-length') !== false) {
    header('Content-Type: text/html');
    $size = (int)($_GET['size'] ?? 100);
    $body = str_repeat('a', $size);
    header('Content-Length: ' . strlen($body));
    echo $body;
    exit;
}

// A script-set Content-Length that doesn't match what it actually
// echoes - proves master doesn't just trust it blindly onto the wire.
if (strpos($_SERVER['REQUEST_URI'], '/bad-content-length-over') !== false) {
    header('Content-Length: 999999');
    echo "hi";
    exit;
}
if (strpos($_SERVER['REQUEST_URI'], '/bad-content-length-under') !== false) {
    header('Content-Length: 2');
    echo "much more than two bytes of actual body\n";
    exit;
}

// Checks more than $_FILES being populated: is_uploaded_file() and
// move_uploaded_file() work only if PHP's own rfc1867 handler ran, so a bug
// that filled $_FILES by some other path still shows up here.
if (strpos($_SERVER['REQUEST_URI'], '/upload') !== false) {
    header('Content-Type: text/plain');
    if (!isset($_FILES['upload'])) {
        echo "NO_FILE\n";
        exit;
    }
    $tmp = $_FILES['upload']['tmp_name'];
    echo "NAME=" . $_FILES['upload']['name'] . "\n";
    echo "SIZE=" . $_FILES['upload']['size'] . "\n";
    echo "ERROR=" . $_FILES['upload']['error'] . "\n";
    echo "IS_UPLOADED_FILE=" . var_export(is_uploaded_file($tmp), true) . "\n";
    // Not just getmypid(): workers recycle at limits.requests, so across a
    // parallel test run the OS hands the same pid out again and two requests
    // race over one destination - one unlinks it under the other's read.
    $dest = tempnam(sys_get_temp_dir(), 'proteus_upload_test_');
    $moved = move_uploaded_file($tmp, $dest);
    echo "MOVE_UPLOADED_FILE=" . var_export($moved, true) . "\n";
    if ($moved) {
        $content = file_get_contents($dest);
        echo "MOVED_LEN=" . strlen($content) . "\n";
        if (strlen($content) <= 1024) {
            echo "MOVED_CONTENT=" . $content . "\n";
        } else {
            // Checked here rather than echoed back: an upload big enough to
            // spill is by definition too big to return in the response.
            $ok = true;
            for ($i = 0, $n = strlen($content); $i < $n; $i++) {
                if (ord($content[$i]) !== $i % 251) {
                    $ok = false;
                    break;
                }
            }
            echo "MOVED_PATTERN_OK=" . var_export($ok, true) . "\n";
        }
        unlink($dest);
    }
    // A regular (non-file) field must survive alongside the file part -
    // proves the whole multipart body was parsed, not just the file.
    echo "FIELD=" . ($_POST['note'] ?? 'MISSING') . "\n";
    exit;
}

// Busy rather than sleeping: on Unix the execution timer counts CPU time, and
// a sleeping script would sit here until the master watchdog noticed instead.
// Nothing is echoed first, so PHP can still put 500 on the wire.
if (strpos($_SERVER['REQUEST_URI'], '/time-limit') !== false) {
    if (($_GET['set'] ?? '') === '1') {
        set_time_limit(1);
    }
    $x = 0;
    while (true) {
        $x++;
    }
}

// Core buffers what it pulls out of read_post, so a second reader must get
// the same bytes even though the SAPI can only be drained once.
if (strpos($_SERVER['REQUEST_URI'], '/input-twice') !== false) {
    $first = file_get_contents('php://input');
    $second = file_get_contents('php://input');
    echo "FIRST_LEN=" . strlen($first) . "\n";
    echo "SECOND_LEN=" . strlen($second) . "\n";
    echo "IDENTICAL=" . var_export($first === $second, true) . "\n";
    exit;
}

// These come from a raw header handed over on the side, not the
// header-to-$_SERVER mapping every other header goes through.
if (strpos($_SERVER['REQUEST_URI'], '/cookie-and-auth') !== false) {
    echo "COOKIE_FOO=" . ($_COOKIE['foo'] ?? 'MISSING') . "\n";
    echo "COOKIE_BAZ=" . ($_COOKIE['baz'] ?? 'MISSING') . "\n";
    echo "AUTH_USER=" . ($_SERVER['PHP_AUTH_USER'] ?? 'MISSING') . "\n";
    echo "AUTH_PW=" . ($_SERVER['PHP_AUTH_PW'] ?? 'MISSING') . "\n";
    exit;
}

// The directive must reach the engine, not merely make ini_get() report the
// right string with the function still present.
if (strpos($_SERVER['REQUEST_URI'], '/disabled-function-check') !== false) {
    echo "EXEC_EXISTS=" . var_export(function_exists('exec'), true) . "\n";
    echo "PHP_VERSION_ID=" . PHP_VERSION_ID . "\n";
    exit;
}

// 8.0 drops a disabled function from the table, so calling it is an uncaught
// Error. 7.4 leaves a stub that warns and returns false, so the script goes on
// - either way the command itself must not run.
if (strpos($_SERVER['REQUEST_URI'], '/call-disabled-function') !== false) {
    $output = [];
    exec('echo hi', $output);
    echo "EXEC_OUTPUT_LINES=" . count($output) . "\n";
    exit;
}

// php-mod renames the SAPI to `cli-server` purely so OPcache's accel_find_sapi()
// admits it. Nothing else can tell whether that worked.
if (strpos($_SERVER['REQUEST_URI'], '/opcache-status') !== false) {
    $present = function_exists('opcache_get_status');
    echo "OPCACHE_EXT=" . var_export($present, true) . "\n";
    if ($present) {
        // false: the per-script list is large and nothing here reads it.
        $status = opcache_get_status(false);
        echo "OPCACHE_ENABLED=" . var_export($status['opcache_enabled'] ?? false, true) . "\n";
        echo "OPCACHE_HITS=" . ($status['opcache_statistics']['hits'] ?? -1) . "\n";
    }
    exit;
}

// A lost client only surfaces through a write, so this keeps writing well past
// the point the caller hangs up. The marker is reached only if PHP did not
// abort, which is ignore_user_abort()'s call to make.
if (strpos($_SERVER['REQUEST_URI'], '/abort-check') !== false) {
    ignore_user_abort(($_GET['ignore'] ?? '0') === '1');
    $marker = '/tmp/proteus_abort_marker_' . ($_GET['marker'] ?? 'none') . '.txt';
    @unlink($marker);
    echo str_repeat('x', 512) . "\n";
    for ($i = 0; $i < 25; $i++) {
        usleep(20000);
        echo "chunk $i\n";
    }
    file_put_contents($marker, 'completed');
    exit;
}

// Admin is applied after user and so wins a collision, and its value is
// locked, so a script's own ini_set() on that key must fail.
if (strpos($_SERVER['REQUEST_URI'], '/ini-check') !== false) {
    echo "MEMORY_LIMIT=" . ini_get('memory_limit') . "\n";
    $set_ok = ini_set('memory_limit', '999M');
    echo "INI_SET_RESULT=" . var_export($set_ok, true) . "\n";
    echo "MEMORY_LIMIT_AFTER_SET=" . ini_get('memory_limit') . "\n";
    exit;
}

// Holds a worker busy for a controlled duration while still returning the
// same pid-bearing body as an undelayed request.
if (isset($_GET['delay_ms'])) {
    usleep((int) $_GET['delay_ms'] * 1000);
}

static $n = 0;
$n++;
echo "PHP response, worker pid=" . getmypid() . ", in-process call #" . $n . "\n";
echo "METHOD=" . $_SERVER['REQUEST_METHOD'] . "\n";
echo "URI=" . $_SERVER['REQUEST_URI'] . "\n";
echo "HEADER_X_TEST=" . ($_SERVER['HTTP_X_TEST'] ?? 'MISSING') . "\n";
// httpoxy (CVE-2016-5385) - a client-sent `Proxy:` header must never
// land here, or PHP HTTP clients would treat it as their upstream proxy.
echo "HTTP_PROXY=" . ($_SERVER['HTTP_PROXY'] ?? 'MISSING') . "\n";
echo "REMOTE_ADDR=" . ($_SERVER['REMOTE_ADDR'] ?? 'MISSING') . "\n";
// Standard CGI vars.
echo "SERVER_NAME=" . ($_SERVER['SERVER_NAME'] ?? 'MISSING') . "\n";
echo "SERVER_ADDR=" . ($_SERVER['SERVER_ADDR'] ?? 'MISSING') . "\n";
echo "SERVER_SOFTWARE=" . ($_SERVER['SERVER_SOFTWARE'] ?? 'MISSING') . "\n";
echo "SERVER_PORT=" . ($_SERVER['SERVER_PORT'] ?? 'MISSING') . "\n";
echo "SERVER_PROTOCOL=" . ($_SERVER['SERVER_PROTOCOL'] ?? 'MISSING') . "\n";
echo "REQUEST_SCHEME=" . ($_SERVER['REQUEST_SCHEME'] ?? 'MISSING') . "\n";
echo "GATEWAY_INTERFACE=" . ($_SERVER['GATEWAY_INTERFACE'] ?? 'MISSING') . "\n";
echo "DOCUMENT_ROOT=" . ($_SERVER['DOCUMENT_ROOT'] ?? 'MISSING') . "\n";
echo "SCRIPT_FILENAME=" . ($_SERVER['SCRIPT_FILENAME'] ?? 'MISSING') . "\n";
echo "SCRIPT_NAME=" . ($_SERVER['SCRIPT_NAME'] ?? 'MISSING') . "\n";
echo "PATH_INFO=" . ($_SERVER['PATH_INFO'] ?? 'MISSING') . "\n";
echo "PHP_SELF=" . ($_SERVER['PHP_SELF'] ?? 'MISSING') . "\n";
echo "HTTPS=" . ($_SERVER['HTTPS'] ?? 'MISSING') . "\n";
echo "REQUEST_TIME_SET=" . (($_SERVER['REQUEST_TIME'] ?? 0) > 0 ? 'yes' : 'no') . "\n";
// getenv() falls back to the real OS environment unconditionally; $_ENV sees
// it only where the operator opted in through variables_order.
echo "ENV_GETENV=" . (getenv('TEST_ENV_VAR') ?: 'MISSING') . "\n";
echo "ENV_SUPERGLOBAL=" . ($_ENV['TEST_ENV_VAR'] ?? 'MISSING') . "\n";
if (($_SERVER['REQUEST_METHOD'] ?? '') === 'POST') {
    echo "BODY=" . file_get_contents('php://input') . "\n";
}
// Single front-controller: REQUEST_URI decides behaviour, with no per-path
// script resolution on the server side.
//
// strpos, not str_contains: this fixture must run on the whole supported PHP
// range, and str_contains is 8.0+.
if (strpos($_SERVER['REQUEST_URI'], '/not-found') !== false) {
    http_response_code(404);
    echo "not found on purpose\n";
}
if (strpos($_SERVER['REQUEST_URI'], '/fatal-error') !== false) {
    undefined_function_call_to_trigger_a_fatal_error();
}
