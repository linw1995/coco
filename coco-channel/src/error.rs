use snafu::prelude::*;

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Channel handler failed: {source}"))]
    Handler { source: BoxError },

    #[snafu(display("Channel transport failed: {source}"))]
    Transport { source: BoxError },

    #[cfg(feature = "telegram")]
    #[snafu(display("Telegram channel transport failed: {source}"))]
    TelegramTransport {
        source: crate::telegram::TelegramError,
    },

    #[snafu(display("Invalid channel input: {message}"))]
    InvalidInput { message: String },
}

impl Error {
    pub fn handler(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Handler {
            source: Box::new(source),
        }
    }

    pub fn transport(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Transport {
            source: Box::new(source),
        }
    }

    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::InvalidInput {
            message: message.into(),
        }
    }

    pub(crate) fn is_transport_failure(&self) -> bool {
        match self {
            Self::Transport { .. } => true,
            #[cfg(feature = "telegram")]
            Self::TelegramTransport { .. } => true,
            Self::Handler { .. } | Self::InvalidInput { .. } => false,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
