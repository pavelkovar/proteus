<?php
// Targets ("index" mode) - PATH_INFO split off a real request beyond this
// script (e.g. /legacy/sub/handler.php/extra/path).
echo "legacy handler pid=" . getmypid() . "\n";
echo "SCRIPT_NAME=" . $_SERVER['SCRIPT_NAME'] . "\n";
echo "PATH_INFO=" . ($_SERVER['PATH_INFO'] ?? 'MISSING') . "\n";
