use snafu::prelude::*;

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Channel handler failed: {source}"))]
    Handler { source: BoxError },

    #[snafu(display("Channel transport failed: {source}"))]
    Transport { source: BoxError },

    #[snafu(display("Invalid channel input: {message}"))]
    InvalidInput { message: String },
}

pub type Result<T> = std::result::Result<T, Error>;
