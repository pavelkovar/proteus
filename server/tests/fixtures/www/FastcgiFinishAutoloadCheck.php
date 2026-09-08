<?php
// Must be reached through autoload and never require'd: the Optimizer crash
// this reproduces happens only when function_exists() is constant-folded
// during autoload-driven compilation, not when compiling the primary script.
class FastcgiFinishAutoloadCheck
{
    public static function hasFastcgiFinishRequest(): bool
    {
        return function_exists('fastcgi_finish_request');
    }
}
