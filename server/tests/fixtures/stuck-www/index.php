<?php
// Simulates a hung request (runaway app code) for watchdog/queue tests.
// Deliberately never returns.
$x = 0;
while (true) {
    $x++;
}
