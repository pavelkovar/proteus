//! The master process: Tokio, HTTP, pool manager, status endpoint, access
//! log. Never forks raw - workers are reached only through the prototype.

pub mod http;
mod idle_stack;
pub mod pool_manager;
mod prototype_launch;
mod worker_channel;
