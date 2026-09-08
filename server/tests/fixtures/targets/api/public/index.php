<?php
// Targets ("script" mode) - single front-controller for the whole "api"
// target, same convention the default (no-target) entrypoint already used.
echo "api target pid=" . getmypid() . "\n";
echo "SCRIPT_NAME=" . $_SERVER['SCRIPT_NAME'] . "\n";
echo "PATH_INFO=" . ($_SERVER['PATH_INFO'] ?? 'MISSING') . "\n";
echo "DOCUMENT_ROOT=" . $_SERVER['DOCUMENT_ROOT'] . "\n";
