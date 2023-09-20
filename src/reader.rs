use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use color_eyre::eyre::{ensure, eyre, Result};
use gcp_auth::AuthenticationManager;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, RANGE};
use reqwest::{Client, ClientBuilder};
use serde_json::Value;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::runtime::Runtime;

use crate::errors::GCSReaderError;
use crate::uri::GCSObjectURI;

macro_rules! bearer {
    ($token:expr) => {
        format!("Bearer {}", $token.as_str())
    };
}

pub enum Auth {
    Auto,
    Token(String),
}

impl Default for Auth {
    fn default() -> Self {
        Self::Auto
    }
}

impl Auth {
    async fn gcp_auth_token() -> Result<String> {
        let authentication_manager = AuthenticationManager::new().await?;
        let scopes = &["https://www.googleapis.com/auth/devstorage.read_only"];
        let token = authentication_manager.get_token(scopes).await?;
        Ok(token.as_str().to_string())
    }

    async fn token(&self) -> Result<String> {
        match self {
            Self::Auto => Self::gcp_auth_token().await,
            Self::Token(token) => Ok(token.to_string()),
        }
    }
}

#[derive(Debug)]
pub struct GCSReader {
    client: Client,
    uri: GCSObjectURI,
    pos: u64,
    len: u64,
}

impl GCSReader {
    pub fn open(uri: GCSObjectURI, auth: Auth) -> Result<Self> {
        let token = Runtime::new()?.block_on(auth.token())?;

        let md_res = reqwest::blocking::Client::new()
            .get(uri.endpoint())
            .header(AUTHORIZATION, HeaderValue::from_str(&bearer!(token))?)
            .send()?;

        ensure!(md_res.status().is_success(), GCSReaderError::from_response(md_res)?);
        let len = md_res
            .json::<HashMap<String, Value>>()?
            .get("size")
            .ok_or(GCSReaderError::GetSizeError(uri.uri()))?
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap();

        let mut header = HeaderMap::new();
        header.insert(AUTHORIZATION, HeaderValue::from_str(&bearer!(token))?);
        let client = ClientBuilder::new().default_headers(header).build()?;

        Ok(Self {
            client,
            uri,
            pos: 0,
            len,
        })
    }

    pub fn from_uri(uri: &str, auth: Auth) -> Result<Self> {
        let uri = GCSObjectURI::new(uri)?;
        Self::open(uri, auth)
    }

    pub async fn read_range(&self, start: u64, end: u64) -> Result<Bytes> {
        let range = format!("bytes={}-{}", start, end - 1);
        let mut header = HeaderMap::new();
        header.insert(RANGE, HeaderValue::from_str(&range)?);

        let mut params = HashMap::new();
        params.insert("alt", "media");

        let res = self
            .client
            .get(self.uri.endpoint())
            .headers(header)
            .query(&params)
            .send()
            .await?;

        ensure!(
            res.status().is_success(),
            GCSReaderError::from_async_response(res).await?
        );
        res.bytes().await.map_err(|e| eyre!(e))
    }
}

impl Read for GCSReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let start = self.pos;
        let end = std::cmp::min(self.pos + (buf.len() as u64), self.len);
        if start == end {
            return Ok(0);
        }
        let bytes = Runtime::new()?
            .block_on(self.read_range(start, end))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let len = bytes.len() as u64;
        buf.clone_from_slice(&bytes);
        self.pos += len;
        Ok(len as usize)
    }
}

impl AsyncRead for GCSReader {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        todo!()
    }
}

impl Seek for GCSReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(pos) => {
                pos as i64
            }
            SeekFrom::End(pos) => {
                self.len as i64 + pos
            }
            SeekFrom::Current(pos) => {
                self.pos as i64 + pos
            }
        };
        if new_pos < 0 && new_pos >= self.len as i64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Invalid seek position: {}", new_pos),
            ));
        }
        self.pos = new_pos as u64;
        Ok(self.pos)
    }
}
