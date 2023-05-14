#![doc = include_str!("../README.md")]

use std::{
    fmt,
    marker::PhantomData,
    mem,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Bytes, BytesMut};
use futures_core::{FusedStream, Stream};
use pin_project_lite::pin_project;

pin_project! {
    #[derive(Debug)]
    pub struct BytesChunks<St: Stream, P> {
        #[pin]
        stream: St,
        buffer: BytesMut,
        capacity: usize,
        marker: PhantomData<P>,
    }
}

type TryBytesChunksResult<T, E> = Result<Bytes, TryBytesChunksError<T, E>>;
type TryBytesChunks<St, T, E> = BytesChunks<St, TryBytesChunksResult<T, E>>;

#[derive(PartialEq, Eq)]
pub struct TryBytesChunksError<T, E>(pub T, pub E);

impl<St: Stream, B> BytesChunks<St, B> {
    pub fn with_capacity(capacity: usize, stream: St) -> Self {
        Self {
            stream,
            buffer: BytesMut::with_capacity(capacity),
            capacity,
            marker: PhantomData,
        }
    }

    pub fn buffer(&self) -> &[u8] {
        self.buffer.as_ref()
    }
}

impl<St: Stream> BytesChunks<St, Bytes> {
    fn take(self: Pin<&mut Self>) -> Bytes {
        let cap = self.capacity;
        self.project().buffer.split_to(cap).freeze()
    }
}

impl<St: Stream> BytesChunks<St, Vec<u8>> {
    fn take(self: Pin<&mut Self>) -> Vec<u8> {
        let cap = self.capacity;
        Vec::from(&self.project().buffer.split_to(cap).freeze()[..])
    }
}

impl<St: Stream, E> BytesChunks<St, TryBytesChunksResult<Bytes, E>> {
    fn take(self: Pin<&mut Self>) -> Bytes {
        let cap = self.capacity.clamp(0, self.buffer.len());
        self.project().buffer.split_to(cap).freeze()
    }
}

impl<St: Stream, E> BytesChunks<St, TryBytesChunksResult<Vec<u8>, E>> {
    fn take(self: Pin<&mut Self>) -> Vec<u8> {
        let cap = self.capacity.clamp(0, self.buffer.len());
        Vec::from(&self.project().buffer.split_to(cap).freeze()[..])
    }
}

impl<T, E: fmt::Debug> fmt::Debug for TryBytesChunksError<T, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.1.fmt(f)
    }
}

impl<T, E: fmt::Display> fmt::Display for TryBytesChunksError<T, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.1.fmt(f)
    }
}

impl<T, E: fmt::Debug + fmt::Display> std::error::Error for TryBytesChunksError<T, E> {}

impl<T, E> TryBytesChunksError<T, E> {
    /// Returns the buffered data stored before the error.
    /// ```
    /// # use std::{convert::Infallible, vec::IntoIter};
    /// # use bytes::Bytes;
    /// # use futures::{
    /// #     executor::block_on,
    /// #     stream::{self, StreamExt},
    /// # };
    /// # use bytes_stream::BytesStream;
    /// # fn main() {
    /// # block_on(async {
    /// let stream: stream::Iter<IntoIter<Result<Bytes, &'static str>>> =
    ///     stream::iter(vec![
    ///         Ok(Bytes::from_static(&[1, 2, 3])),
    ///         Ok(Bytes::from_static(&[4, 5, 6])),
    ///         Err("failure"),
    ///     ]);
    ///
    /// let mut stream = stream.try_bytes_chunks(4);
    ///
    /// assert_eq!(stream.next().await, Some(Ok(Bytes::from_static(&[1, 2, 3, 4]))));
    ///
    /// let err = stream.next().await.unwrap().err().unwrap();
    /// assert_eq!(err.into_inner(), Bytes::from_static(&[5, 6]));
    /// # });
    /// # }
    /// ```
    pub fn into_inner(self) -> T {
        self.0
    }
}

pub trait BytesStream: Stream {
    /// Group bytes in chunks of `capacity`.
    /// ```
    /// # use bytes::Bytes;
    /// # use futures::{
    /// #     executor::block_on,
    /// #     stream::{self, StreamExt},
    /// # };
    /// # use bytes_stream::BytesStream;
    /// # fn main() {
    /// # block_on(async {
    /// let stream = futures::stream::iter(vec![
    ///     Bytes::from_static(&[1, 2, 3]),
    ///     Bytes::from_static(&[4, 5, 6]),
    ///     Bytes::from_static(&[7, 8, 9]),
    /// ]);
    ///
    /// let mut stream = stream.bytes_chunks(4);
    ///
    /// assert_eq!(stream.next().await, Some(Bytes::from_static(&[1, 2, 3, 4])));
    /// assert_eq!(stream.next().await, Some(Bytes::from_static(&[5, 6, 7, 8])));
    /// assert_eq!(stream.next().await, Some(Bytes::from_static(&[9])));
    /// assert_eq!(stream.next().await, None);
    /// # });
    /// # }
    /// ```
    fn bytes_chunks<T>(self, capacity: usize) -> BytesChunks<Self, T>
    where
        Self: Sized,
    {
        BytesChunks::with_capacity(capacity, self)
    }

    /// Group result of bytes in chunks of `capacity`.
    /// ```
    /// # use std::convert::Infallible;
    /// # use bytes::Bytes;
    /// # use futures::{
    /// #     executor::block_on,
    /// #     stream::{self, StreamExt},
    /// # };
    /// # use bytes_stream::BytesStream;
    /// # fn main() {
    /// # block_on(async {
    /// let stream = futures::stream::iter(vec![
    ///     Ok::<_, Infallible>(Bytes::from_static(&[1, 2, 3])),
    ///     Ok::<_, Infallible>(Bytes::from_static(&[4, 5, 6])),
    ///     Ok::<_, Infallible>(Bytes::from_static(&[7, 8, 9])),
    /// ]);
    ///
    /// let mut stream = stream.try_bytes_chunks(4);
    ///
    /// assert_eq!(stream.next().await, Some(Ok(Bytes::from_static(&[1, 2, 3, 4]))));
    /// assert_eq!(stream.next().await, Some(Ok(Bytes::from_static(&[5, 6, 7, 8]))));
    /// assert_eq!(stream.next().await, Some(Ok(Bytes::from_static(&[9]))));
    /// assert_eq!(stream.next().await, None);
    /// # });
    /// # }
    /// ```
    fn try_bytes_chunks<T, E>(self, capacity: usize) -> TryBytesChunks<Self, T, E>
    where
        Self: Sized,
    {
        BytesChunks::with_capacity(capacity, self)
    }
}

impl<T> BytesStream for T where T: Stream {}

impl<E, St: Stream<Item = Result<Bytes, E>>> Stream for TryBytesChunks<St, Bytes, E> {
    type Item = Result<Bytes, TryBytesChunksError<Bytes, E>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();

        if this.buffer.len() >= *this.capacity {
            return Poll::Ready(Some(Ok(self.take())));
        }

        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,

                Poll::Ready(Some(item)) => match item {
                    Ok(item) => {
                        this.buffer.extend_from_slice(&item[..]);

                        if this.buffer.len() >= *this.capacity {
                            return Poll::Ready(Some(Ok(self.take())));
                        }
                    }
                    Err(err) => {
                        let err = TryBytesChunksError(self.take(), err);
                        return Poll::Ready(Some(Err(err)));
                    }
                },

                Poll::Ready(None) => {
                    let last = if this.buffer.is_empty() {
                        None
                    } else {
                        Some(Ok(Bytes::from(mem::take(this.buffer))))
                    };

                    return Poll::Ready(last);
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let chunk_len = if self.buffer.is_empty() { 0 } else { 1 };
        let (lower, upper) = self.stream.size_hint();
        let lower = lower.saturating_add(chunk_len);
        let upper = upper.and_then(|x| x.checked_add(chunk_len));
        (lower, upper)
    }
}

impl<St: Stream<Item = Bytes>> Stream for BytesChunks<St, Bytes> {
    type Item = Bytes;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();

        if this.buffer.len() >= *this.capacity {
            return Poll::Ready(Some(self.take()));
        }

        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,

                Poll::Ready(Some(item)) => {
                    this.buffer.extend_from_slice(&item[..]);

                    if this.buffer.len() >= *this.capacity {
                        return Poll::Ready(Some(self.take()));
                    }
                }

                Poll::Ready(None) => {
                    let last = if this.buffer.is_empty() {
                        None
                    } else {
                        Some(Bytes::from(mem::take(this.buffer)))
                    };

                    return Poll::Ready(last);
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let chunk_len = if self.buffer.is_empty() { 0 } else { 1 };
        let (lower, upper) = self.stream.size_hint();
        let lower = lower.saturating_add(chunk_len);
        let upper = upper.and_then(|x| x.checked_add(chunk_len));
        (lower, upper)
    }
}

impl<St: Stream<Item = Vec<u8>>> Stream for BytesChunks<St, Vec<u8>> {
    type Item = Vec<u8>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();

        if this.buffer.len() >= *this.capacity {
            return Poll::Ready(Some(self.take()));
        }

        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,

                Poll::Ready(Some(item)) => {
                    this.buffer.extend_from_slice(&item[..]);

                    if this.buffer.len() >= *this.capacity {
                        return Poll::Ready(Some(self.take()));
                    }
                }

                Poll::Ready(None) => {
                    let last = if this.buffer.is_empty() {
                        None
                    } else {
                        let buf = mem::take(this.buffer);
                        Some(Vec::from(&buf[..]))
                    };

                    return Poll::Ready(last);
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let chunk_len = if self.buffer.is_empty() { 0 } else { 1 };
        let (lower, upper) = self.stream.size_hint();
        let lower = lower.saturating_add(chunk_len);
        let upper = upper.and_then(|x| x.checked_add(chunk_len));
        (lower, upper)
    }
}

impl<E, St: Stream<Item = Result<Vec<u8>, E>>> Stream for TryBytesChunks<St, Vec<u8>, E> {
    type Item = Result<Vec<u8>, TryBytesChunksError<Vec<u8>, E>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();

        if this.buffer.len() >= *this.capacity {
            return Poll::Ready(Some(Ok(self.take())));
        }

        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,

                Poll::Ready(Some(item)) => match item {
                    Ok(item) => {
                        this.buffer.extend_from_slice(&item[..]);

                        if this.buffer.len() >= *this.capacity {
                            return Poll::Ready(Some(Ok(self.take())));
                        }
                    }
                    Err(err) => {
                        let err = TryBytesChunksError(self.take(), err);
                        return Poll::Ready(Some(Err(err)));
                    }
                },

                Poll::Ready(None) => {
                    let last = if this.buffer.is_empty() {
                        None
                    } else {
                        let buf = mem::take(this.buffer);
                        Some(Ok(Vec::from(&buf[..])))
                    };

                    return Poll::Ready(last);
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let chunk_len = if self.buffer.is_empty() { 0 } else { 1 };
        let (lower, upper) = self.stream.size_hint();
        let lower = lower.saturating_add(chunk_len);
        let upper = upper.and_then(|x| x.checked_add(chunk_len));
        (lower, upper)
    }
}

impl<St: FusedStream<Item = Bytes>> FusedStream for BytesChunks<St, Bytes> {
    fn is_terminated(&self) -> bool {
        self.stream.is_terminated() && self.buffer.is_empty()
    }
}

impl<E, St: FusedStream<Item = Result<Bytes, E>>> FusedStream for TryBytesChunks<St, Bytes, E> {
    fn is_terminated(&self) -> bool {
        self.stream.is_terminated() && self.buffer.is_empty()
    }
}

#[cfg(test)]
mod test {
    use std::convert::Infallible;

    use bytes::Bytes;
    use futures::{
        executor::block_on,
        stream::{self, StreamExt},
    };
    use futures_test::{assert_stream_done, assert_stream_next};

    use super::BytesStream;

    #[test]
    fn test_bytes_chunks_lengthen() {
        block_on(async {
            let stream = futures::stream::iter(vec![
                Bytes::from_static(&[1, 2, 3]),
                Bytes::from_static(&[4, 5, 6]),
                Bytes::from_static(&[7, 8, 9]),
            ]);

            let mut stream = stream.bytes_chunks(4);

            assert_stream_next!(stream, Bytes::from_static(&[1, 2, 3, 4]));
            assert_stream_next!(stream, Bytes::from_static(&[5, 6, 7, 8]));
            assert_stream_next!(stream, Bytes::from_static(&[9]));
            assert_stream_done!(stream);
        });
    }

    #[test]
    fn test_bytes_chunks_shorten() {
        block_on(async {
            let stream = futures::stream::iter(vec![
                Bytes::from_static(&[1, 2, 3]),
                Bytes::from_static(&[4, 5, 6]),
                Bytes::from_static(&[7, 8, 9]),
            ]);

            let mut stream = stream.bytes_chunks(2);

            assert_stream_next!(stream, Bytes::from_static(&[1, 2]));
            assert_stream_next!(stream, Bytes::from_static(&[3, 4]));
            assert_stream_next!(stream, Bytes::from_static(&[5, 6]));
            assert_stream_next!(stream, Bytes::from_static(&[7, 8]));
            assert_stream_next!(stream, Bytes::from_static(&[9]));
            assert_stream_done!(stream);
        });
    }

    #[test]
    fn test_vec_chunks_lengthen() {
        block_on(async {
            #[rustfmt::skip]
            let stream = futures::stream::iter(vec![
                vec![1, 2, 3],
                vec![4, 5, 6],
                vec![7, 8, 9],
            ]);

            let mut stream = stream.bytes_chunks(4);

            assert_stream_next!(stream, vec![1, 2, 3, 4]);
            assert_stream_next!(stream, vec![5, 6, 7, 8]);
            assert_stream_next!(stream, vec![9]);
            assert_stream_done!(stream);
        });
    }

    #[test]
    fn test_vec_chunks_shorten() {
        block_on(async {
            #[rustfmt::skip]
            let stream = futures::stream::iter(vec![
                vec![1, 2, 3],
                vec![4, 5, 6],
                vec![7, 8, 9],
            ]);

            let mut stream = stream.bytes_chunks(2);

            assert_stream_next!(stream, vec![1, 2]);
            assert_stream_next!(stream, vec![3, 4]);
            assert_stream_next!(stream, vec![5, 6]);
            assert_stream_next!(stream, vec![7, 8]);
            assert_stream_next!(stream, vec![9]);
            assert_stream_done!(stream);
        });
    }

    #[test]
    fn test_try_bytes_chunks_lengthen() {
        block_on(async {
            let stream: stream::Iter<std::vec::IntoIter<Result<Bytes, Infallible>>> =
                stream::iter(vec![
                    Ok(Bytes::from_static(&[1, 2, 3])),
                    Ok(Bytes::from_static(&[4, 5, 6])),
                    Ok(Bytes::from_static(&[7, 8, 9])),
                ]);

            let mut stream = stream.try_bytes_chunks(4);

            assert_stream_next!(stream, Ok(Bytes::from_static(&[1, 2, 3, 4])));
            assert_stream_next!(stream, Ok(Bytes::from_static(&[5, 6, 7, 8])));
            assert_stream_next!(stream, Ok(Bytes::from_static(&[9])));
            assert_stream_done!(stream);
        });
    }

    #[test]
    fn test_try_bytes_chunks_shorten() {
        block_on(async {
            let stream: stream::Iter<std::vec::IntoIter<Result<Bytes, Infallible>>> =
                stream::iter(vec![
                    Ok(Bytes::from_static(&[1, 2, 3])),
                    Ok(Bytes::from_static(&[4, 5, 6])),
                    Ok(Bytes::from_static(&[7, 8, 9])),
                ]);

            let mut stream = stream.try_bytes_chunks(2);

            assert_stream_next!(stream, Ok(Bytes::from_static(&[1, 2])));
            assert_stream_next!(stream, Ok(Bytes::from_static(&[3, 4])));
            assert_stream_next!(stream, Ok(Bytes::from_static(&[5, 6])));
            assert_stream_next!(stream, Ok(Bytes::from_static(&[7, 8])));
            assert_stream_next!(stream, Ok(Bytes::from_static(&[9])));
            assert_stream_done!(stream);
        });
    }

    #[test]
    fn test_try_bytes_chunks_leftovers() {
        block_on(async {
            let stream: stream::Iter<std::vec::IntoIter<Result<Bytes, &'static str>>> =
                stream::iter(vec![
                    Ok(Bytes::from_static(&[1, 2, 3])),
                    Ok(Bytes::from_static(&[4, 5, 6])),
                    Err("error"),
                ]);

            let mut stream = stream.try_bytes_chunks(4);

            assert_stream_next!(stream, Ok(Bytes::from_static(&[1, 2, 3, 4])));

            let err = stream.next().await.unwrap();
            assert!(err.is_err());
            let err = err.err().unwrap();
            assert_eq!(err.into_inner(), Bytes::from_static(&[5, 6]));
        });
    }
}
