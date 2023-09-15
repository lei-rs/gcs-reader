use color_eyre::eyre::{ensure, Result};

use crate::errors::GCSObjectURIError;

#[derive(Debug)]
pub struct GCSObjectURI {
    bucket: String,
    object: String,
}

impl GCSObjectURI {
    pub fn new(uri: &str) -> Result<Self> {
        ensure!(&uri[..5] == "gs://", GCSObjectURIError::InvalidPrefix(uri.to_string()));
        let mut parts = uri.splitn(2, "://");
        let _ = parts.next().unwrap();
        let path = parts.next().unwrap();
        let mut parts = path.splitn(2, '/');
        let bucket = parts.next().ok_or(GCSObjectURIError::MissingBucket)?;
        let object = parts.next().ok_or(GCSObjectURIError::MissingObject)?;
        ensure!(!bucket.is_empty(), GCSObjectURIError::MissingBucket);
        ensure!(!object.is_empty(), GCSObjectURIError::MissingObject);
        Ok(Self {
            bucket: bucket.to_string(),
            object: object.to_string(),
        })
    }

    pub(crate) fn bucket(&self) -> &str {
        &self.bucket
    }

    pub(crate) fn object(&self) -> &str {
        &self.object
    }

    pub(crate) fn endpoint(&self) -> String {
        let object = self.object.replace('/', "%2F");
        format!(
            "https://storage.googleapis.com/storage/v1/b/{}/o/{}",
            self.bucket, object
        )
    }

    pub(crate) fn uri(&self) -> String {
        format!("gs://{}/{}", self.bucket, self.object)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::setup;

    #[test]
    fn test_valid_uri() {
        setup();
        let uri = "gs://my_bucket/my_object";
        let gcs_obj = GCSObjectURI::new(uri).unwrap();
        assert_eq!(gcs_obj.bucket(), "my_bucket");
        assert_eq!(gcs_obj.object(), "my_object");
    }

    #[test]
    fn test_invalid_prefix() {
        setup();
        let uri = "s3://my_bucket/my_object";
        let gcs_obj = GCSObjectURI::new(uri);
        assert!(gcs_obj.is_err());
    }

    #[test]
    fn test_missing_object() {
        setup();
        let uri = "gs://my_bucket/";
        let gcs_obj = GCSObjectURI::new(uri);
        assert!(gcs_obj.is_err());
    }

    #[test]
    fn test_missing_bucket_and_object() {
        setup();
        let uri = "gs://";
        let gcs_obj = GCSObjectURI::new(uri);
        assert!(gcs_obj.is_err());
    }

    #[test]
    fn test_uri_method() {
        setup();
        let uri = "gs://my_bucket/my_object/folder";
        let gcs_obj = GCSObjectURI::new(uri).unwrap();
        assert_eq!(gcs_obj.uri(), "gs://my_bucket/my_object/folder");
    }

    #[test]
    fn test_endpoint_method() {
        setup();
        let uri = "gs://my_bucket/my_object/folder";
        let gcs_obj = GCSObjectURI::new(uri).unwrap();
        assert_eq!(
            gcs_obj.endpoint(),
            "https://storage.googleapis.com/storage/v1/b/my_bucket/o/my_object%2Ffolder"
        );
    }
}
