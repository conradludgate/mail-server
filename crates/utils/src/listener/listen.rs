/*
 * Copyright (c) 2023 Stalwart Labs Ltd.
 *
 * This file is part of the Stalwart Mail Server.
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

use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use proxy_header::io::ProxiedStream;
use rustls::crypto::ring::cipher_suite::TLS13_AES_128_GCM_SHA256;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::watch,
};
use tokio_rustls::server::TlsStream;
use tracing::Span;

use crate::{
    acme::SpawnAcme,
    config::{Config, Listener, Server, ServerProtocol, Servers},
    failed,
    listener::SessionData,
    UnwrapFailure,
};

use super::{
    limiter::ConcurrencyLimiter, ServerInstance, SessionManager, SessionStream, TcpAcceptorResult,
};

impl Server {
    pub fn spawn(self, manager: impl SessionManager, shutdown_rx: watch::Receiver<bool>) {
        // Prepare instance
        let instance = Arc::new(ServerInstance {
            data: if matches!(self.protocol, ServerProtocol::Smtp | ServerProtocol::Lmtp) {
                format!("220 {} {}\r\n", self.hostname, self.data)
            } else {
                self.data
            },
            id: self.id,
            listener_id: self.internal_id,
            protocol: self.protocol,
            hostname: self.hostname,
            acceptor: self.acceptor,
            proxy_networks: self.proxy_networks,
            blocked_ips: self.blocked_ips,
            limiter: ConcurrencyLimiter::new(self.max_connections),
            shutdown_rx,
        });
        let is_tls = self.tls_implicit;
        let has_proxies = !instance.proxy_networks.is_empty();

        // Spawn listeners
        for listener in self.listeners {
            tracing::info!(
                id = instance.id,
                protocol = ?instance.protocol,
                bind.ip = listener.addr.ip().to_string(),
                bind.port = listener.addr.port(),
                tls = is_tls,
                "Starting listener"
            );
            let local_ip = listener.addr.ip();

            // Obtain TCP options
            let opts = SocketOpts {
                nodelay: listener.nodelay,
                ttl: listener.ttl,
                linger: listener.linger,
            };

            // Bind socket
            let listener = listener.listen();

            // Spawn listener
            let mut shutdown_rx = instance.shutdown_rx.clone();
            let manager = manager.clone();
            let instance = instance.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        stream = listener.accept() => {
                            match stream {
                                Ok((stream, remote_addr)) => {
                                    if has_proxies && instance.proxy_networks.iter().any(|network| network.matches(&remote_addr.ip())) {
                                        let instance = instance.clone();
                                        let manager = manager.clone();

                                        // Set socket options
                                        opts.apply(&stream);

                                        tokio::spawn(async move {
                                            match ProxiedStream::create_from_tokio(stream, Default::default()).await {
                                                Ok(stream) =>{
                                                    let remote_addr = stream.proxy_header()
                                                                            .proxied_address()
                                                                            .map(|addr| addr.source)
                                                                            .unwrap_or(remote_addr);
                                                    if let Some(session) = instance.build_session(stream, local_ip, remote_addr) {
                                                        // Spawn session
                                                        manager.spawn(session, is_tls);
                                                    }
                                                }
                                                Err(err) => {
                                                    tracing::trace!(context = "io",
                                                                    event = "error",
                                                                    instance = instance.id,
                                                                    protocol = ?instance.protocol,
                                                                    reason = %err,
                                                                    "Failed to accept proxied TCP connection");
                                                }
                                            }
                                        });
                                    } else if let Some(session) = instance.build_session(stream, local_ip, remote_addr) {
                                        // Set socket options
                                        opts.apply(&session.stream);

                                        // Spawn session
                                        manager.spawn(session, is_tls);
                                    }
                                }
                                Err(err) => {
                                    tracing::trace!(context = "io",
                                                    event = "error",
                                                    instance = instance.id,
                                                    protocol = ?instance.protocol,
                                                    "Failed to accept TCP connection: {}", err);
                                }
                            }
                        },
                        _ = shutdown_rx.changed() => {
                            tracing::debug!(
                                event = "shutdown",
                                instance = instance.id,
                                protocol = ?instance.protocol,
                                "Listener shutting down.");
                            manager.shutdown();
                            break;
                        }
                    };
                }
            });
        }
    }
}

trait BuildSession {
    fn build_session<T: SessionStream>(
        &self,
        stream: T,
        local_ip: IpAddr,
        remote_addr: SocketAddr,
    ) -> Option<SessionData<T>>;
}

impl BuildSession for Arc<ServerInstance> {
    fn build_session<T: SessionStream>(
        &self,
        stream: T,
        local_ip: IpAddr,
        remote_addr: SocketAddr,
    ) -> Option<SessionData<T>> {
        // Convert mapped IPv6 addresses to IPv4
        let remote_ip = match remote_addr.ip() {
            IpAddr::V6(ip) => ip
                .to_ipv4_mapped()
                .map(IpAddr::V4)
                .unwrap_or(IpAddr::V6(ip)),
            remote_ip => remote_ip,
        };
        let remote_port = remote_addr.port();

        // Check if blocked
        if self.blocked_ips.is_blocked(&remote_ip) {
            tracing::debug!(
                context = "listener",
                event = "blocked",
                instance = self.id,
                protocol = ?self.protocol,
                remote.ip = remote_ip.to_string(),
                remote.port = remote_port,
                "Dropping connection from blocked IP."
            );
            None
        } else if let Some(in_flight) = self.limiter.is_allowed() {
            // Enforce concurrency
            SessionData {
                stream,
                in_flight,
                span: tracing::info_span!(
                    "session",
                    instance = self.id,
                    protocol = ?self.protocol,
                    remote.ip = remote_ip.to_string(),
                    remote.port = remote_port,
                ),
                local_ip,
                remote_ip,
                remote_port,
                instance: self.clone(),
            }
            .into()
        } else {
            tracing::info!(
                context = "throttle",
                event = "too-many-requests",
                instance = self.id,
                protocol = ?self.protocol,
                remote.ip = remote_ip.to_string(),
                remote.port = remote_port,
                max_concurrent = self.limiter.max_concurrent,
                "Too many concurrent connections."
            );
            None
        }
    }
}

pub struct SocketOpts {
    pub nodelay: bool,
    pub ttl: Option<u32>,
    pub linger: Option<Duration>,
}

impl SocketOpts {
    pub fn apply(&self, stream: &TcpStream) {
        // Set TCP options
        if let Err(err) = stream.set_nodelay(self.nodelay) {
            tracing::warn!(
                context = "tcp",
                event = "error",
                "Failed to set no-delay: {}",
                err
            );
        }
        if let Some(ttl) = self.ttl {
            if let Err(err) = stream.set_ttl(ttl) {
                tracing::warn!(
                    context = "tcp",
                    event = "error",
                    "Failed to set TTL: {}",
                    err
                );
            }
        }
        if self.linger.is_some() {
            if let Err(err) = stream.set_linger(self.linger) {
                tracing::warn!(
                    context = "tcp",
                    event = "error",
                    "Failed to set linger: {}",
                    err
                );
            }
        }
    }
}

impl Servers {
    pub fn bind(&self, config: &Config) {
        // Bind as root
        for server in &self.inner {
            for listener in &server.listeners {
                listener
                    .socket
                    .bind(listener.addr)
                    .failed(&format!("Failed to bind to {}", listener.addr));
            }
        }

        // Drop privileges
        #[cfg(not(target_env = "msvc"))]
        {
            if let Some(run_as_user) = config.value("server.run-as.user") {
                let mut pd = privdrop::PrivDrop::default().user(run_as_user);
                if let Some(run_as_group) = config.value("server.run-as.group") {
                    pd = pd.group(run_as_group);
                }
                pd.apply().failed("Failed to drop privileges");
            }
        }
    }

    pub fn spawn(
        self,
        spawn: impl Fn(Server, watch::Receiver<bool>),
    ) -> (watch::Sender<bool>, watch::Receiver<bool>) {
        // Spawn listeners
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        for server in self.inner {
            spawn(server, shutdown_rx.clone());
        }

        // Spawn ACME managers
        for acme_manager in self.acme_managers {
            acme_manager.spawn(shutdown_rx.clone());
        }

        (shutdown_tx, shutdown_rx)
    }
}

impl Listener {
    pub fn listen(self) -> TcpListener {
        self.socket
            .listen(self.backlog.unwrap_or(1024))
            .unwrap_or_else(|err| failed(&format!("Failed to listen on {}: {}", self.addr, err)))
    }
}

impl ServerInstance {
    pub async fn tls_accept<T: SessionStream>(
        &self,
        stream: T,
        span: &Span,
    ) -> Result<TlsStream<T>, ()> {
        match self.acceptor.accept(stream).await {
            TcpAcceptorResult::Tls(accept) => match accept.await {
                Ok(stream) => {
                    tracing::info!(
                        parent: span,
                        context = "tls",
                        event = "handshake",
                        version = ?stream.get_ref().1.protocol_version().unwrap_or(rustls::ProtocolVersion::TLSv1_3),
                        cipher = ?stream.get_ref().1.negotiated_cipher_suite().unwrap_or(TLS13_AES_128_GCM_SHA256),
                    );
                    Ok(stream)
                }
                Err(err) => {
                    tracing::debug!(
                        parent: span,
                        context = "tls",
                        event = "error",
                        "Failed to accept TLS connection: {}",
                        err
                    );
                    Err(())
                }
            },
            TcpAcceptorResult::Plain(_) | TcpAcceptorResult::Close => {
                tracing::debug!(
                    parent: span,
                    context = "tls",
                    event = "error",
                    "Failed to accept TLS connection: {}",
                    "TLS is not configured for this server."
                );
                Err(())
            }
        }
    }
}
