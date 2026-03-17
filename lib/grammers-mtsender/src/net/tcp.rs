// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use log::info;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

use super::ServerAddr;

#[cfg(feature = "websocket")]
use futures_util::{Sink, Stream};

/// Wraps a WebSocket connection to provide AsyncRead + AsyncWrite over binary messages.
#[cfg(feature = "websocket")]
pub(crate) struct WsByteStream {
    ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<TcpStream>,
    >,
    read_buf: Vec<u8>,
    read_pos: usize,
}

pub type ReadHalf<'a> = tokio::io::ReadHalf<&'a mut NetStream>;
pub type WriteHalf<'a> = tokio::io::WriteHalf<&'a mut NetStream>;

pub enum NetStream {
    Tcp(TcpStream),
    #[cfg(feature = "proxy")]
    ProxySocks5(tokio_socks::tcp::Socks5Stream<TcpStream>),
    #[cfg(feature = "websocket")]
    WebSocket(WsByteStream),
}

impl NetStream {
    pub(crate) fn split(&mut self) -> (ReadHalf<'_>, WriteHalf<'_>) {
        tokio::io::split(self)
    }

    pub(crate) async fn connect(addr: &ServerAddr) -> Result<Self, io::Error> {
        info!("connecting...");
        match addr {
            ServerAddr::Tcp { address } => Ok(NetStream::Tcp(TcpStream::connect(address).await?)),
            #[cfg(feature = "proxy")]
            ServerAddr::Proxied { address, proxy } => {
                Self::connect_proxy_stream(address, proxy).await
            }
            #[cfg(feature = "websocket")]
            ServerAddr::Ws { address } => {
                use tokio_tungstenite::tungstenite::http::Request;
                use tokio_tungstenite::tungstenite::client::IntoClientRequest;

                info!("connecting via WebSocket to {}", address);
                let mut request = address.as_str().into_client_request().map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidInput, format!("bad WS URL: {}", e))
                })?;
                // Telegram requires the "binary" subprotocol — without it the server returns 404.
                request.headers_mut().insert(
                    "Sec-WebSocket-Protocol",
                    "binary".parse().unwrap(),
                );
                let (ws_stream, _) = tokio_tungstenite::connect_async(request)
                    .await
                    .map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::ConnectionRefused,
                            format!("WebSocket connection failed: {}", e),
                        )
                    })?;
                info!("WebSocket connected successfully");
                Ok(NetStream::WebSocket(WsByteStream {
                    ws: ws_stream,
                    read_buf: Vec::new(),
                    read_pos: 0,
                }))
            }
        }
    }

    #[cfg(feature = "proxy")]
    async fn connect_proxy_stream(
        addr: &std::net::SocketAddr,
        proxy_url: &str,
    ) -> Result<NetStream, io::Error> {
        use std::{
            io::{self, ErrorKind},
            net::{IpAddr, SocketAddr},
        };

        use hickory_resolver::{
            AsyncResolver,
            config::{ResolverConfig, ResolverOpts},
        };
        use url::Host;

        let proxy = url::Url::parse(proxy_url)
            .map_err(|err| io::Error::new(ErrorKind::InvalidData, err))?;
        let scheme = proxy.scheme();
        let host = proxy.host().ok_or(io::Error::new(
            ErrorKind::NotFound,
            format!("proxy host is missing from url: {}", proxy_url),
        ))?;
        let port = proxy.port().ok_or(io::Error::new(
            ErrorKind::NotFound,
            format!("proxy port is missing from url: {}", proxy_url),
        ))?;
        let username = proxy.username();
        let password = proxy.password().unwrap_or("");
        let socks_addr = match host {
            Host::Domain(domain) => {
                let resolver =
                    AsyncResolver::tokio(ResolverConfig::default(), ResolverOpts::default());
                let response = resolver.lookup_ip(domain).await?;
                let socks_ip_addr = response.into_iter().next().ok_or(io::Error::new(
                    ErrorKind::NotFound,
                    format!("proxy host did not return any ip address: {}", domain),
                ))?;
                SocketAddr::new(socks_ip_addr, port)
            }
            Host::Ipv4(v4) => SocketAddr::new(IpAddr::from(v4), port),
            Host::Ipv6(v6) => SocketAddr::new(IpAddr::from(v6), port),
        };

        match scheme {
            "socks5" => {
                if username.is_empty() {
                    Ok(NetStream::ProxySocks5(
                        tokio_socks::tcp::Socks5Stream::connect(socks_addr, addr)
                            .await
                            .map_err(|err| io::Error::new(ErrorKind::ConnectionAborted, err))?,
                    ))
                } else {
                    Ok(NetStream::ProxySocks5(
                        tokio_socks::tcp::Socks5Stream::connect_with_password(
                            socks_addr, addr, username, password,
                        )
                        .await
                        .map_err(|err| io::Error::new(ErrorKind::ConnectionAborted, err))?,
                    ))
                }
            }
            scheme => Err(io::Error::new(
                ErrorKind::ConnectionAborted,
                format!("proxy scheme not supported: {}", scheme),
            )),
        }
    }
}

// Implement AsyncRead for NetStream, dispatching to inner stream type.
impl AsyncRead for NetStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            NetStream::Tcp(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(feature = "proxy")]
            NetStream::ProxySocks5(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(feature = "websocket")]
            NetStream::WebSocket(ws) => ws.poll_read_ws(cx, buf),
        }
    }
}

// Implement AsyncWrite for NetStream, dispatching to inner stream type.
impl AsyncWrite for NetStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            NetStream::Tcp(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(feature = "proxy")]
            NetStream::ProxySocks5(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(feature = "websocket")]
            NetStream::WebSocket(ws) => ws.poll_write_ws(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            NetStream::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "proxy")]
            NetStream::ProxySocks5(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "websocket")]
            NetStream::WebSocket(ws) => {
                Pin::new(&mut ws.ws)
                    .poll_flush(cx)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
            }
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            NetStream::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(feature = "proxy")]
            NetStream::ProxySocks5(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(feature = "websocket")]
            NetStream::WebSocket(ws) => {
                Pin::new(&mut ws.ws)
                    .poll_close(cx)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
            }
        }
    }
}

#[cfg(feature = "websocket")]
impl WsByteStream {
    /// Read bytes from the WebSocket. Binary messages are buffered and served byte-by-byte.
    fn poll_read_ws(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // Return buffered data from a previous WebSocket message first.
        if self.read_pos < self.read_buf.len() {
            let n = std::cmp::min(buf.remaining(), self.read_buf.len() - self.read_pos);
            buf.put_slice(&self.read_buf[self.read_pos..self.read_pos + n]);
            self.read_pos += n;
            if self.read_pos >= self.read_buf.len() {
                self.read_buf.clear();
                self.read_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }

        // Poll for the next WebSocket message.
        match Pin::new(&mut self.ws).poll_next(cx) {
            Poll::Ready(Some(Ok(msg))) => {
                let data = msg.into_data();
                if data.is_empty() {
                    // Empty message (ping/pong/close) — re-poll.
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                let n = std::cmp::min(buf.remaining(), data.len());
                buf.put_slice(&data[..n]);
                if n < data.len() {
                    self.read_buf = data;
                    self.read_pos = n;
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Some(Err(e))) => {
                Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e)))
            }
            Poll::Ready(None) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "WebSocket closed",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }

    /// Write bytes as a single binary WebSocket message.
    fn poll_write_ws(
        &mut self,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        // Check if the sink is ready to accept a message.
        match Pin::new(&mut self.ws).poll_ready(cx) {
            Poll::Ready(Ok(())) => {
                let msg = tokio_tungstenite::tungstenite::Message::Binary(buf.to_vec().into());
                match Pin::new(&mut self.ws).start_send(msg) {
                    Ok(()) => Poll::Ready(Ok(buf.len())),
                    Err(e) => Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e))),
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e))),
            Poll::Pending => Poll::Pending,
        }
    }
}
