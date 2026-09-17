pub mod control;
pub mod data;
pub mod shm;

/// Where the prototype's end of the control channel is dup2'd before exec:
/// the first fd after stdio, as systemd socket activation also does.
pub const CONTROL_FD: std::os::fd::RawFd = 3;

/// Where the prototype's config arrives. Over a pipe rather than argv,
/// because `php.environment` may hold secrets and argv stays visible in
/// `/proc/<pid>/cmdline` for the process's whole life.
pub const CONFIG_FD: std::os::fd::RawFd = 4;
