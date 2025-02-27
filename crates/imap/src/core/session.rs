/*
 * Copyright (c) 2023 Stalwart Labs Ltd.
 *
 * This file is part of Stalwart Mail Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

use std::{borrow::Cow, sync::Arc};

use imap_proto::{protocol::ProtocolVersion, receiver::Receiver};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::server::TlsStream;
use utils::listener::{stream::NullIo, SessionManager, SessionStream};

use super::{ImapSessionManager, Session, State};

impl SessionManager for ImapSessionManager {
    #[allow(clippy::manual_async_fn)]
    fn handle<T: utils::listener::SessionStream>(
        self,
        session: utils::listener::SessionData<T>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async move {
            if let Ok(mut session) = Session::new(session, self).await {
                if session.handle_conn().await && session.instance.acceptor.is_tls() {
                    if let Ok(mut session) = session.into_tls().await {
                        session.handle_conn().await;
                    }
                }
            }
        }
    }

    #[allow(clippy::manual_async_fn)]
    fn shutdown(&self) -> impl std::future::Future<Output = ()> + Send {
        async {}
    }
}

impl<T: SessionStream> Session<T> {
    pub async fn handle_conn(&mut self) -> bool {
        let mut buf = vec![0; 8192];
        let mut shutdown_rx = self.instance.shutdown_rx.clone();

        loop {
            tokio::select! {
                result = tokio::time::timeout(
                    if !matches!(self.state, State::NotAuthenticated {..}) {
                        self.imap.timeout_auth
                    } else {
                        self.imap.timeout_unauth
                    },
                    self.stream_rx.read(&mut buf)) => {
                    match result {
                        Ok(Ok(bytes_read)) => {
                            if bytes_read > 0 {
                                match self.ingest(&buf[..bytes_read]).await {
                                    Ok(false) => (),
                                    Ok(true) => {
                                        return true;
                                    }
                                    Err(_) => {
                                        tracing::debug!(parent: &self.span, event = "disconnect", "Disconnecting client.");
                                        break;
                                    }
                                }
                            } else {
                                tracing::debug!(parent: &self.span, event = "close", "IMAP connection closed by client.");
                                break;
                            }
                        },
                        Ok(Err(err)) => {
                            tracing::debug!(parent: &self.span, event = "error", reason = %err, "IMAP connection error.");
                            break;
                        },
                        Err(_) => {
                            self.write_bytes(&b"* BYE Connection timed out.\r\n"[..]).await.ok();
                            tracing::debug!(parent: &self.span, "IMAP connection timed out.");
                            break;
                        }
                    }
                },
                _ = shutdown_rx.changed() => {
                    self.write_bytes(&b"* BYE Server shutting down.\r\n"[..]).await.ok();
                    tracing::debug!(parent: &self.span, event = "shutdown", "IMAP server shutting down.");
                    break;
                }
            };
        }

        false
    }

    pub async fn new(
        mut session: utils::listener::SessionData<T>,
        manager: ImapSessionManager,
    ) -> Result<Session<T>, ()> {
        // Write greeting
        let (is_tls, greeting) = if session.stream.is_tls() {
            (true, &manager.imap.greeting_tls)
        } else {
            (false, &manager.imap.greeting_plain)
        };
        if let Err(err) = session.stream.write_all(greeting).await {
            tracing::debug!(parent: &session.span, event = "error", reason = %err, "Failed to write greeting.");
            return Err(());
        }
        let _ = session.stream.flush().await;

        // Split stream into read and write halves
        let (stream_rx, stream_tx) = tokio::io::split(session.stream);

        Ok(Session {
            receiver: Receiver::with_max_request_size(manager.imap.max_request_size),
            version: ProtocolVersion::Rev1,
            state: State::NotAuthenticated { auth_failures: 0 },
            is_tls,
            is_condstore: false,
            is_qresync: false,
            imap: manager.imap,
            jmap: manager.jmap,
            instance: session.instance,
            span: session.span,
            in_flight: session.in_flight,
            remote_addr: session.remote_ip,
            stream_rx,
            stream_tx: Arc::new(tokio::sync::Mutex::new(stream_tx)),
        })
    }

    pub async fn into_tls(self) -> Result<Session<TlsStream<T>>, ()> {
        // Drop references to write half from state
        let state = if let Some(state) =
            self.state
                .try_replace_stream_tx(Arc::new(tokio::sync::Mutex::new(
                    tokio::io::split(NullIo::default()).1,
                ))) {
            state
        } else {
            tracing::debug!("Failed to obtain write half state.");
            return Err(());
        };

        // Take ownership of WriteHalf and unsplit it from ReadHalf
        let stream = if let Ok(stream_tx) =
            Arc::try_unwrap(self.stream_tx).map(|mutex| mutex.into_inner())
        {
            self.stream_rx.unsplit(stream_tx)
        } else {
            tracing::debug!("Failed to take ownership of write half.");
            return Err(());
        };

        // Upgrade to TLS
        let (stream_rx, stream_tx) =
            tokio::io::split(self.instance.tls_accept(stream, &self.span).await?);
        let stream_tx = Arc::new(tokio::sync::Mutex::new(stream_tx));

        Ok(Session {
            jmap: self.jmap,
            imap: self.imap,
            instance: self.instance,
            receiver: self.receiver,
            version: self.version,
            state: state.try_replace_stream_tx(stream_tx.clone()).unwrap(),
            is_tls: true,
            is_condstore: self.is_condstore,
            is_qresync: self.is_qresync,
            span: self.span,
            in_flight: self.in_flight,
            remote_addr: self.remote_addr,
            stream_rx,
            stream_tx,
        })
    }
}

impl<T: SessionStream> Session<T> {
    pub async fn write_bytes(&self, bytes: impl Into<Cow<'static, [u8]>>) -> crate::OpResult {
        let bytes = bytes.into();
        /*for line in String::from_utf8_lossy(bytes.as_ref()).split("\r\n") {
            let c = println!("{}", line);
        }*/
        tracing::trace!(
            parent: &self.span,
            event = "write",
            data = std::str::from_utf8(bytes.as_ref()).unwrap_or_default(),
            size = bytes.len()
        );

        let mut stream = self.stream_tx.lock().await;
        if let Err(err) = stream.write_all(bytes.as_ref()).await {
            tracing::trace!(parent: &self.span, "Failed to write to stream: {}", err);
            Err(())
        } else {
            let _ = stream.flush().await;
            Ok(())
        }
    }
}

impl<T: SessionStream> super::SessionData<T> {
    pub async fn write_bytes(&self, bytes: impl Into<Cow<'static, [u8]>>) -> bool {
        let bytes = bytes.into();
        /*for line in String::from_utf8_lossy(bytes.as_ref()).split("\r\n") {
            let c = println!("{}", line);
        }*/
        tracing::trace!(
            parent: &self.span,
            event = "write",
            data = std::str::from_utf8(bytes.as_ref()).unwrap_or_default(),
            size = bytes.len()
        );

        let mut stream = self.stream_tx.lock().await;
        if let Err(err) = stream.write_all(bytes.as_ref()).await {
            tracing::trace!(parent: &self.span, "Failed to write to stream: {}", err);
            false
        } else {
            let _ = stream.flush().await;
            true
        }
    }
}
