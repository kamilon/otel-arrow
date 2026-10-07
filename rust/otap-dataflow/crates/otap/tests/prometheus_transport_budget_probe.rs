// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Loopback probes for the HTTP limits used by the scrape receiver.

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use hyper::{Request, body::Incoming};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};

const HTTP1_BUFFER_LIMIT: usize = 8 * 1024;

fn bounded_client() -> Client<HttpConnector, Empty<Bytes>> {
    let mut builder = Client::builder(TokioExecutor::new());
    let _ = builder.http1_max_buf_size(HTTP1_BUFFER_LIMIT);
    let _ = builder.http1_max_headers(64);
    let _ = builder.pool_max_idle_per_host(0);
    builder.build(HttpConnector::new())
}

async fn get(
    client: &Client<HttpConnector, Empty<Bytes>>,
    url: String,
) -> Result<hyper::Response<Incoming>, hyper_util::client::legacy::Error> {
    let request = Request::builder()
        .method("GET")
        .uri(url)
        .body(Empty::new())
        .expect("valid probe request");
    client.request(request).await
}

async fn serve_once(response: Vec<u8>) -> (String, tokio::task::JoinHandle<std::io::Result<()>>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback listener");
    let address = listener.local_addr().expect("listener address");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept client");
        let mut request = [0_u8; 1024];
        let request_bytes = socket
            .read(&mut request)
            .await
            .expect("read client request");
        assert!(request_bytes > 0, "client sent an HTTP request");
        socket.write_all(&response).await
    });
    (format!("http://{address}/metrics"), server)
}

fn response_with_header_count(count: usize) -> Vec<u8> {
    let mut response = b"HTTP/1.1 200 OK\r\n".to_vec();
    for index in 0..count {
        response.extend_from_slice(format!("X-Probe-{index}: value\r\n").as_bytes());
    }
    response.extend_from_slice(b"Content-Length: 0\r\nConnection: close\r\n\r\n");
    response
}

#[tokio::test]
async fn hyper_client_honors_header_count_and_byte_bounds() {
    let client = bounded_client();
    let (url, server) = serve_once(response_with_header_count(65)).await;
    let response = get(&client, url).await;
    assert!(response.is_err(), "65 response headers must exceed the cap");
    server
        .await
        .expect("header-count server task")
        .expect("write complete header-count response");

    let mut response = b"HTTP/1.1 200 OK\r\nX-Large: ".to_vec();
    response.extend(std::iter::repeat_n(b'x', 512 * 1024));
    response.extend_from_slice(b"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
    let (url, server) = serve_once(response).await;
    let response = get(&client, url).await;
    assert!(
        response.is_err(),
        "header block larger than the configured Hyper buffer must be rejected"
    );
    let _ = server.await.expect("large-header server task");
}

#[tokio::test]
async fn streamed_body_frames_stay_within_transport_bound() {
    const BODY_LEN: usize = 1024 * 1024;
    let mut response =
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n".to_vec();
    response.extend_from_slice(format!("{BODY_LEN:x}\r\n").as_bytes());
    response.resize(response.len() + BODY_LEN, b'x');
    response.extend_from_slice(b"\r\n0\r\n\r\n");
    let (url, server) = serve_once(response).await;
    let response = get(&bounded_client(), url).await.expect("receive response");
    let mut body = response.into_body();
    let mut total = 0;
    let mut largest_frame = 0;
    while let Some(frame) = body.frame().await {
        let frame = frame.expect("valid body frame");
        if let Ok(data) = frame.into_data() {
            largest_frame = largest_frame.max(data.len());
            total += data.len();
        }
    }
    server
        .await
        .expect("body server task")
        .expect("write complete body response");

    println!(
        "body_bytes={total} largest_frame_bytes={largest_frame} \
         configured_http1_buffer_limit_bytes={HTTP1_BUFFER_LIMIT}"
    );
    assert_eq!(total, BODY_LEN);
    assert!(largest_frame > 0);
    assert!(
        largest_frame <= HTTP1_BUFFER_LIMIT,
        "frame must not exceed the configured HTTP/1 buffer limit"
    );
}
