<?php
// Targets ("index" mode) - directory-style request, `index` appended.
echo "legacy dir index pid=" . getmypid() . "\n";
echo "SCRIPT_NAME=" . $_SERVER['SCRIPT_NAME'] . "\n";
