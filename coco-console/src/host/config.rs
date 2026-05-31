use std::net::SocketAddr;

#[derive(Debug, Clone, Copy)]
pub struct ConsoleConfig {
    pub addr: SocketAddr,
}

impl Default for ConsoleConfig {
    fn default() -> Self {
        Self {
            addr: default_console_addr(),
        }
    }
}

fn default_console_addr() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 17667))
}
