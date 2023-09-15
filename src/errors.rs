use color_eyre::eyre::Result;
use reqwest::StatusCode;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum GCSObjectURIError {
    #[error("Invalid GCS URI prefix: {0}")]
    InvalidPrefix(String),
    #[error("Invalid GCS URI: missing object")]
    MissingObject,
    #[error("Invalid GCS URI: missing bucket")]
    MissingBucket,
}

#[derive(Error, Debug)]
pub(crate) enum GCSReaderError {
    #[error("GET failed. Status: {0}. Response:\n{1}")]
    GetError(StatusCode, String),
    #[error("Failed to parse object size for URI: {0}.")]
    GetSizeError(String),
}

impl GCSReaderError {
    pub(crate) fn from_response(res: reqwest::blocking::Response) -> Result<Self> {
        let status = res.status();
        let body = res.text()?;
        Ok(Self::GetError(status, body))
    }

    pub(crate) async fn from_async_response(res: reqwest::Response) -> Result<Self> {
        let status = res.status();
        let body = res.text().await?;
        Ok(Self::GetError(status, body))
    }
}
