#[test]
fn inner_error_reported_as_source() {
    use batch_aint_one::error::BatchError;
    use std::error::Error;
    let e = BatchError::<std::io::Error>::BatchFailed(std::io::Error::new(
        std::io::ErrorKind::Other,
        "underlying error",
    ));
    let display = format!("{}", e);
    assert_eq!(display, "The entire batch failed");
    let source = e.source().unwrap();
    assert_eq!(source.to_string(), "underlying error");
}
