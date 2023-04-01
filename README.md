# bytes-stream

Utility functions to work with
[Streams](https://docs.rs/futures/latest/futures/stream/index.html) of
[Bytes](https://docs.rs/bytes/latest/bytes/struct.Bytes.html).

## Examples

Split a stream of bytes into chunks:

```rust
use bytes::Bytes;
use bytes_stream::BytesStream;
use futures::StreamExt;

fn main() {
    futures::executor::block_on(async {
        let stream = futures::stream::iter(vec![
            Bytes::from_static(&[1, 2, 3]),
            Bytes::from_static(&[4, 5, 6]),
            Bytes::from_static(&[7, 8, 9]),
        ]);

        let mut stream = stream.bytes_chunks(4);

        assert_eq!(stream.next().await, Some(Bytes::from_static(&[1, 2, 3, 4])));
        assert_eq!(stream.next().await, Some(Bytes::from_static(&[5, 6, 7, 8])));
        assert_eq!(stream.next().await, Some(Bytes::from_static(&[9])));
        assert_eq!(stream.next().await, None);
    });
}
```
