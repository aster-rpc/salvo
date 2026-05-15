//! `QuinnListener` and utils.
use std::fmt::{self, Debug, Formatter};
use std::future::{Ready, ready};
use std::io::Result as IoResult;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use futures_util::future::{BoxFuture, FutureExt};
use futures_util::stream::{Once, once};
pub use quinn::ServerConfig;
use salvo_http3::quinn as http3_quinn;
use tokio_util::sync::CancellationToken;

use crate::conn::{Coupler, HttpBuilder, IntoConfigStream};
use crate::fuse::ArcFusewire;

use crate::service::HyperHandler;

mod builder;
pub use builder::Builder;
mod listener;
pub use listener::{QuinnAcceptor, QuinnListener};

/// Http3 Connection.
#[allow(dead_code)]
pub struct QuinnConnection {
    inner: http3_quinn::Connection,
    /// Clone of the underlying `quinn::Connection` (cheap — quinn's
    /// Connection is `Arc` internally). Held alongside `inner` so
    /// handlers can read QUIC-level stats (RTT, congestion window,
    /// lost packets) for adaptive bandwidth control. The h3-quinn
    /// wrapper hides the raw connection behind a private field, so
    /// without this we'd have no way to call `stats()`.
    raw_quinn: ::quinn::Connection,
    fusewire: Option<ArcFusewire>,
}
impl QuinnConnection {
    pub(crate) fn new(
        inner: http3_quinn::Connection,
        raw_quinn: ::quinn::Connection,
        fusewire: Option<ArcFusewire>,
    ) -> Self {
        Self { inner, raw_quinn, fusewire }
    }
    /// Get inner quinn connection.
    #[must_use]
    pub fn into_inner(self) -> http3_quinn::Connection {
        self.inner
    }
    /// Underlying `quinn::Connection`. Use for `stats()` (smoothed
    /// RTT, congestion window, lost packets, bytes sent/received)
    /// and other QUIC-level state that h3-quinn doesn't surface.
    #[must_use]
    pub fn quinn(&self) -> &::quinn::Connection {
        &self.raw_quinn
    }
}
impl Debug for QuinnConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuinnConnection").finish()
    }
}
impl Deref for QuinnConnection {
    type Target = http3_quinn::Connection;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl DerefMut for QuinnConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// QUIC connection coupler.
pub struct QuinnCoupler;
impl Coupler for QuinnCoupler {
    type Stream = QuinnConnection;

     fn couple(
        &self,
        stream: Self::Stream,
        handler: HyperHandler,
        builder: Arc<HttpBuilder>,
        graceful_stop_token: Option<CancellationToken>,
    ) -> BoxFuture<'static, IoResult<()>> {
        async move {
        builder
            .quinn
            .serve_connection(stream, handler, graceful_stop_token)
            .await
        }.boxed()
    }
}
impl Debug for QuinnCoupler {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuinnCoupler").finish()
    }
}

impl IntoConfigStream<Self> for ServerConfig {
    type Stream = Once<Ready<Self>>;

    fn into_stream(self) -> Self::Stream {
        once(ready(self))
    }
}

impl IntoConfigStream<ServerConfig> for quinn::crypto::rustls::QuicServerConfig {
    type Stream = Once<Ready<ServerConfig>>;

    fn into_stream(self) -> Self::Stream {
        once(ready(ServerConfig::with_crypto(Arc::new(self))))
    }
}
