<?php
// Targets ("index" mode) - direct URL-to-file match, no PATH_INFO.
echo "legacy foo pid=" . getmypid() . "\n";
echo "SCRIPT_NAME=" . $_SERVER['SCRIPT_NAME'] . "\n";
echo "PATH_INFO=" . ($_SERVER['PATH_INFO'] ?? 'MISSING') . "\n";
