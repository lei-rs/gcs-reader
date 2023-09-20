use std::io::{Read, Seek, SeekFrom};

use gcs_reader::{Auth, GCSReader};

#[test]
fn test_seek_start() {
    dotenv::dotenv().ok();
    let uri = std::env::var("URI").unwrap();
    let mut reader = GCSReader::from_uri(&uri, Auth::default()).unwrap();
    reader.seek(SeekFrom::Start(10)).unwrap();
    let mut buf1 = [0u8; 10];
    reader.read_exact(&mut buf1).unwrap();

    let mut file = std::fs::File::open("tests/dummy.file").unwrap();
    file.seek(SeekFrom::Start(10)).unwrap();
    let mut buf2 = [0u8; 10];
    file.read_exact(&mut buf2).unwrap();

    assert_eq!(buf1, buf2);
}

#[test]
fn test_seek_end() {
    dotenv::dotenv().ok();
    let uri = std::env::var("URI").unwrap();
    let mut reader = GCSReader::from_uri(&uri, Auth::default()).unwrap();
    reader.seek(SeekFrom::End(-10)).unwrap();
    let mut buf1 = [0u8; 10];
    reader.read_exact(&mut buf1).unwrap();

    let mut file = std::fs::File::open("tests/dummy.file").unwrap();
    file.seek(SeekFrom::End(-10)).unwrap();
    let mut buf2 = [0u8; 10];
    file.read_exact(&mut buf2).unwrap();

    assert_eq!(buf1, buf2);
}

#[test]
fn test_read_to_end() {
    dotenv::dotenv().ok();
    let uri = std::env::var("URI").unwrap();
    let mut reader = GCSReader::from_uri(&uri, Auth::default()).unwrap();
    let mut buf1 = Vec::new();
    reader.read_to_end(&mut buf1).unwrap();

    let mut file = std::fs::File::open("tests/dummy.file").unwrap();
    let mut buf2 = Vec::new();
    file.read_to_end(&mut buf2).unwrap();

    assert_eq!(buf1, buf2);
}
