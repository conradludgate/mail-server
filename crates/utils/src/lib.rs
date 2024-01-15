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

use std::{collections::HashMap, sync::Arc};

use config::Config;

pub mod codec;
pub mod config;
pub mod ipc;
pub mod listener;
pub mod map;
pub mod snowflake;
pub mod suffixlist;

use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    trace::{self, Sampler},
    Resource,
};
use opentelemetry_semantic_conventions::resource::{SERVICE_NAME, SERVICE_VERSION};
use rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    ClientConfig, RootCertStore, SignatureScheme,
};
use rustls_pki_types::TrustAnchor;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{prelude::__tracing_subscriber_SubscriberExt, EnvFilter};

pub trait UnwrapFailure<T> {
    fn failed(self, action: &str) -> T;
}

impl<T> UnwrapFailure<T> for Option<T> {
    fn failed(self, message: &str) -> T {
        match self {
            Some(result) => result,
            None => {
                eprintln!("{message}");
                std::process::exit(1);
            }
        }
    }
}

impl<T, E: std::fmt::Display> UnwrapFailure<T> for Result<T, E> {
    fn failed(self, message: &str) -> T {
        match self {
            Ok(result) => result,
            Err(err) => {
                #[cfg(feature = "test_mode")]
                panic!("{message}: {err}");

                #[cfg(not(feature = "test_mode"))]
                {
                    eprintln!("{message}: {err}");
                    std::process::exit(1);
                }
            }
        }
    }
}

pub fn failed(message: &str) -> ! {
    eprintln!("{message}");
    std::process::exit(1);
}

pub fn enable_tracing(
    config: &Config,
    map_filter: impl FnOnce(EnvFilter) -> EnvFilter,
    message: &str,
) -> config::Result<Option<WorkerGuard>> {
    let level = config.value("global.tracing.level").unwrap_or("info");
    let env_filter = EnvFilter::builder()
        .parse(format!(
            "smtp={level},imap={level},jmap={level},store={level},utils={level},directory={level}"
        ))
        .failed("Failed to log level");
    let env_filter = map_filter(env_filter);

    let result = match config.value("global.tracing.method").unwrap_or_default() {
        "log" => {
            let path = config.value_require("global.tracing.path")?;
            let prefix = config.value_require("global.tracing.prefix")?;
            let file_appender = match config.value("global.tracing.rotate").unwrap_or("daily") {
                "daily" => tracing_appender::rolling::daily(path, prefix),
                "hourly" => tracing_appender::rolling::hourly(path, prefix),
                "minutely" => tracing_appender::rolling::minutely(path, prefix),
                "never" => tracing_appender::rolling::never(path, prefix),
                rotate => {
                    return Err(format!("Unsupported log rotation strategy {rotate:?}"));
                }
            };

            let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
            tracing::subscriber::set_global_default(
                tracing_subscriber::FmtSubscriber::builder()
                    .with_env_filter(env_filter)
                    .with_writer(non_blocking)
                    .with_ansi(config.property_or_static("global.tracing.ansi", "true")?)
                    .finish(),
            )
            .failed("Failed to set subscriber");
            Ok(guard.into())
        }
        "stdout" => {
            tracing::subscriber::set_global_default(
                tracing_subscriber::FmtSubscriber::builder()
                    .with_env_filter(env_filter)
                    .with_ansi(config.property_or_static("global.tracing.ansi", "true")?)
                    .finish(),
            )
            .failed("Failed to set subscriber");

            Ok(None)
        }
        "otel" | "open-telemetry" => {
            let tracer = match config.value_require("global.tracing.transport")? {
                "grpc" => {
                    let mut exporter = opentelemetry_otlp::new_exporter().tonic();
                    if let Some(endpoint) = config.value("global.tracing.endpoint") {
                        exporter = exporter.with_endpoint(endpoint);
                    }
                    opentelemetry_otlp::new_pipeline()
                        .tracing()
                        .with_exporter(exporter)
                }
                "http" => {
                    let mut headers = HashMap::new();
                    for (_, value) in config.values("global.tracing.headers") {
                        if let Some((key, value)) = value.split_once(':') {
                            headers.insert(key.trim().to_string(), value.trim().to_string());
                        } else {
                            return Err(format!("Invalid open-telemetry header {value:?}"));
                        }
                    }
                    let mut exporter = opentelemetry_otlp::new_exporter()
                        .http()
                        .with_endpoint(config.value_require("global.tracing.endpoint")?);
                    if !headers.is_empty() {
                        exporter = exporter.with_headers(headers);
                    }
                    opentelemetry_otlp::new_pipeline()
                        .tracing()
                        .with_exporter(exporter)
                }
                transport => {
                    return Err(format!(
                        "Unsupported open-telemetry transport {transport:?}"
                    ));
                }
            }
            .with_trace_config(
                trace::config()
                    .with_resource(Resource::new(vec![
                        KeyValue::new(SERVICE_NAME, "stalwart-smtp".to_string()),
                        KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION").to_string()),
                    ]))
                    .with_sampler(Sampler::AlwaysOn),
            )
            .install_batch(opentelemetry_sdk::runtime::Tokio)
            .failed("Failed to create tracer");

            tracing::subscriber::set_global_default(
                tracing_subscriber::Registry::default()
                    .with(tracing_opentelemetry::layer().with_tracer(tracer))
                    .with(env_filter),
            )
            .failed("Failed to set subscriber");

            Ok(None)
        }
        #[cfg(unix)]
        "journal" => {
            tracing::subscriber::set_global_default(
                tracing_subscriber::Registry::default()
                    .with(tracing_journald::layer().failed("Failed to configure journal"))
                    .with(env_filter),
            )
            .failed("Failed to set subscriber");

            Ok(None)
        }
        _ => Ok(None),
    };

    tracing::info!(message);

    result
}

pub async fn wait_for_shutdown(message: &str) {
    #[cfg(not(target_env = "msvc"))]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut h_term = signal(SignalKind::terminate()).failed("start signal handler");
        let mut h_int = signal(SignalKind::interrupt()).failed("start signal handler");

        tokio::select! {
            _ = h_term.recv() => tracing::debug!("Received SIGTERM."),
            _ = h_int.recv() => tracing::debug!("Received SIGINT."),
        };
    }

    #[cfg(target_env = "msvc")]
    {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {}
            Err(err) => {
                eprintln!("Unable to listen for shutdown signal: {}", err);
            }
        }
    }

    tracing::info!(message);
}

pub fn rustls_client_config(allow_invalid_certs: bool) -> ClientConfig {
    let config = ClientConfig::builder();

    if !allow_invalid_certs {
        let mut root_cert_store = RootCertStore::empty();

        root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().map(|ta| TrustAnchor {
            subject: ta.subject.clone(),
            subject_public_key_info: ta.subject_public_key_info.clone(),
            name_constraints: ta.name_constraints.clone(),
        }));

        config
            .with_root_certificates(root_cert_store)
            .with_no_client_auth()
    } else {
        config
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(DummyVerifier {}))
            .with_no_client_auth()
    }
}

#[derive(Debug)]
struct DummyVerifier;

impl ServerCertVerifier for DummyVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls_pki_types::CertificateDer<'_>,
        _intermediates: &[rustls_pki_types::CertificateDer<'_>],
        _server_name: &rustls_pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls_pki_types::UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA1,
            SignatureScheme::ECDSA_SHA1_Legacy,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
            SignatureScheme::ED448,
        ]
    }
}
