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

use std::{
    collections::hash_map::RandomState, fmt::Display, net::IpAddr, sync::Arc, time::Duration,
};

use ::sieve::{Compiler, Runtime};
use api::session::BaseCapabilities;
use auth::{
    oauth::OAuthCode,
    rate_limit::{AnonymousLimiter, AuthenticatedLimiter},
    AccessToken,
};
use dashmap::DashMap;
use directory::{Directories, Directory, QueryBy};
use jmap_proto::{
    error::method::MethodError,
    method::{
        query::{QueryRequest, QueryResponse},
        set::{SetRequest, SetResponse},
    },
    types::{collection::Collection, property::Property},
};
use mail_parser::HeaderName;
use nlp::language::Language;
use services::{
    delivery::spawn_delivery_manager,
    housekeeper::{self, init_housekeeper, spawn_housekeeper},
    state::{self, init_state_manager, spawn_state_manager},
};
use smtp::core::SMTP;
use store::{
    fts::FtsFilter,
    query::{sort::Pagination, Comparator, Filter, ResultSet, SortedResultSet},
    roaring::RoaringBitmap,
    write::{BatchBuilder, BitmapClass, DirectoryClass, TagValue, ToBitmaps, ValueClass},
    BitmapKey, BlobStore, Deserialize, FtsStore, Serialize, Store, Stores, ValueKey,
};
use tokio::sync::mpsc;
use utils::{
    config::{Rate, Servers},
    ipc::DeliveryEvent,
    map::ttl_dashmap::{TtlDashMap, TtlMap},
    snowflake::SnowflakeIdGenerator,
    UnwrapFailure,
};

pub mod api;
pub mod auth;
pub mod blob;
pub mod changes;
pub mod email;
pub mod identity;
pub mod mailbox;
pub mod principal;
pub mod push;
pub mod quota;
pub mod services;
pub mod sieve;
pub mod submission;
pub mod thread;
pub mod vacation;
pub mod websocket;

pub const LONG_SLUMBER: Duration = Duration::from_secs(60 * 60 * 24);

pub struct JMAP {
    pub store: Store,
    pub blob_store: BlobStore,
    pub fts_store: FtsStore,
    pub config: Config,
    pub directory: Arc<Directory>,

    pub sessions: TtlDashMap<String, u32>,
    pub access_tokens: TtlDashMap<u32, Arc<AccessToken>>,
    pub snowflake_id: SnowflakeIdGenerator,

    pub rate_limit_auth: DashMap<u32, Arc<AuthenticatedLimiter>>,
    pub rate_limit_unauth: DashMap<IpAddr, Arc<AnonymousLimiter>>,

    pub oauth_codes: TtlDashMap<String, Arc<OAuthCode>>,

    pub state_tx: mpsc::Sender<state::Event>,
    pub housekeeper_tx: mpsc::Sender<housekeeper::Event>,
    pub smtp: Arc<SMTP>,

    pub sieve_compiler: Compiler,
    pub sieve_runtime: Runtime<()>,
}

pub struct Config {
    pub default_language: Language,
    pub query_max_results: usize,
    pub changes_max_results: usize,
    pub snippet_max_results: usize,

    pub request_max_size: usize,
    pub request_max_calls: usize,
    pub request_max_concurrent: u64,

    pub get_max_objects: usize,
    pub set_max_objects: usize,

    pub upload_max_size: usize,
    pub upload_max_concurrent: u64,

    pub upload_tmp_quota_size: usize,
    pub upload_tmp_quota_amount: usize,
    pub upload_tmp_ttl: u64,

    pub mailbox_max_depth: usize,
    pub mailbox_name_max_len: usize,
    pub mail_attachments_max_size: usize,
    pub mail_parse_max_items: usize,
    pub mail_max_size: usize,

    pub sieve_max_script_name: usize,
    pub sieve_max_scripts: usize,

    pub session_cache_ttl: Duration,
    pub rate_authenticated: Rate,
    pub rate_authenticate_req: Rate,
    pub rate_anonymous: Rate,
    pub rate_use_forwarded: bool,

    pub event_source_throttle: Duration,
    pub push_max_total: usize,

    pub web_socket_throttle: Duration,
    pub web_socket_timeout: Duration,
    pub web_socket_heartbeat: Duration,

    pub oauth_key: String,
    pub oauth_expiry_user_code: u64,
    pub oauth_expiry_auth_code: u64,
    pub oauth_expiry_token: u64,
    pub oauth_expiry_refresh_token: u64,
    pub oauth_expiry_refresh_token_renew: u64,
    pub oauth_max_auth_attempts: u32,

    pub spam_header: Option<(HeaderName<'static>, String)>,

    pub http_headers: Vec<(hyper::header::HeaderName, hyper::header::HeaderValue)>,

    pub encrypt: bool,
    pub encrypt_append: bool,

    pub principal_allow_lookups: bool,

    pub capabilities: BaseCapabilities,
}

pub struct Bincode<T: serde::Serialize + serde::de::DeserializeOwned> {
    pub inner: T,
}

#[derive(Debug)]
pub enum IngestError {
    Temporary,
    OverQuota,
    Permanent { code: [u8; 3], reason: String },
}

impl JMAP {
    pub async fn init(
        config: &utils::config::Config,
        stores: &Stores,
        directories: &Directories,
        servers: &mut Servers,
        delivery_rx: mpsc::Receiver<DeliveryEvent>,
        smtp: Arc<SMTP>,
    ) -> Result<Arc<Self>, String> {
        // Init state manager and housekeeper
        let (state_tx, state_rx) = init_state_manager();
        let (housekeeper_tx, housekeeper_rx) = init_housekeeper();
        let shard_amount = config
            .property::<u64>("global.shared-map.shard")?
            .unwrap_or(32)
            .next_power_of_two() as usize;

        let jmap_server = Arc::new(JMAP {
            directory: directories
                .directories
                .get(config.value_require("storage.directory")?)
                .failed(&format!(
                    "Unable to find directory '{}'",
                    config.value_require("storage.directory")?
                ))
                .clone(),
            snowflake_id: config
                .property::<u64>("storage.cluster.node-id")?
                .map(SnowflakeIdGenerator::with_node_id)
                .unwrap_or_else(SnowflakeIdGenerator::new),
            store: stores.get_store(config, "storage.data")?,
            fts_store: stores.get_fts_store(config, "storage.fts")?,
            blob_store: stores.get_blob_store(config, "storage.blob")?,
            config: Config::new(config).failed("Invalid configuration file"),
            sessions: TtlDashMap::with_capacity(
                config.property("jmap.session.cache.size")?.unwrap_or(100),
                shard_amount,
            ),
            access_tokens: TtlDashMap::with_capacity(
                config.property("jmap.session.cache.size")?.unwrap_or(100),
                shard_amount,
            ),
            rate_limit_auth: DashMap::with_capacity_and_hasher_and_shard_amount(
                config
                    .property("jmap.rate-limit.cache.size")?
                    .unwrap_or(1024),
                RandomState::default(),
                shard_amount,
            ),
            rate_limit_unauth: DashMap::with_capacity_and_hasher_and_shard_amount(
                config
                    .property("jmap.rate-limit.cache.size")?
                    .unwrap_or(1024),
                RandomState::default(),
                shard_amount,
            ),
            oauth_codes: TtlDashMap::with_capacity(
                config.property("oauth.cache.size")?.unwrap_or(128),
                shard_amount,
            ),
            state_tx,
            housekeeper_tx,
            smtp,
            sieve_compiler: Compiler::new()
                .with_max_script_size(
                    config
                        .property("sieve.untrusted.limits.script-size")?
                        .unwrap_or(1024 * 1024),
                )
                .with_max_string_size(
                    config
                        .property("sieve.untrusted.limits.string-length")?
                        .unwrap_or(4096),
                )
                .with_max_variable_name_size(
                    config
                        .property("sieve.untrusted.limits.variable-name-length")?
                        .unwrap_or(32),
                )
                .with_max_nested_blocks(
                    config
                        .property("sieve.untrusted.limits.nested-blocks")?
                        .unwrap_or(15),
                )
                .with_max_nested_tests(
                    config
                        .property("sieve.untrusted.limits.nested-tests")?
                        .unwrap_or(15),
                )
                .with_max_nested_foreverypart(
                    config
                        .property("sieve.untrusted.limits.nested-foreverypart")?
                        .unwrap_or(3),
                )
                .with_max_match_variables(
                    config
                        .property("sieve.untrusted.limits.match-variables")?
                        .unwrap_or(30),
                )
                .with_max_local_variables(
                    config
                        .property("sieve.untrusted.limits.local-variables")?
                        .unwrap_or(128),
                )
                .with_max_header_size(
                    config
                        .property("sieve.untrusted.limits.header-size")?
                        .unwrap_or(1024),
                )
                .with_max_includes(
                    config
                        .property("sieve.untrusted.limits.includes")?
                        .unwrap_or(3),
                ),
            sieve_runtime: Runtime::new()
                .with_max_nested_includes(
                    config
                        .property("sieve.untrusted.limits.nested-includes")?
                        .unwrap_or(3),
                )
                .with_cpu_limit(
                    config
                        .property("sieve.untrusted.limits.cpu")?
                        .unwrap_or(5000),
                )
                .with_max_variable_size(
                    config
                        .property("sieve.untrusted.limits.variable-size")?
                        .unwrap_or(4096),
                )
                .with_max_redirects(
                    config
                        .property("sieve.untrusted.limits.redirects")?
                        .unwrap_or(1),
                )
                .with_max_received_headers(
                    config
                        .property("sieve.untrusted.limits.received-headers")?
                        .unwrap_or(10),
                )
                .with_max_header_size(
                    config
                        .property("sieve.untrusted.limits.header-size")?
                        .unwrap_or(1024),
                )
                .with_max_out_messages(
                    config
                        .property("sieve.untrusted.limits.outgoing-messages")?
                        .unwrap_or(3),
                )
                .with_default_vacation_expiry(
                    config
                        .property::<Duration>("sieve.untrusted.default-expiry.vacation")?
                        .unwrap_or(Duration::from_secs(30 * 86400))
                        .as_secs(),
                )
                .with_default_duplicate_expiry(
                    config
                        .property::<Duration>("sieve.untrusted.default-expiry.duplicate")?
                        .unwrap_or(Duration::from_secs(7 * 86400))
                        .as_secs(),
                )
                .without_capabilities(
                    config
                        .values("sieve.untrusted.disable-capabilities")
                        .map(|(_, v)| v),
                )
                .with_valid_notification_uris({
                    let values = config
                        .values("sieve.untrusted.notification-uris")
                        .map(|(_, v)| v.to_string())
                        .collect::<Vec<_>>();
                    if !values.is_empty() {
                        values
                    } else {
                        vec!["mailto".to_string()]
                    }
                })
                .with_protected_headers({
                    let values = config
                        .values("sieve.untrusted.protected-headers")
                        .map(|(_, v)| v.to_string())
                        .collect::<Vec<_>>();
                    if !values.is_empty() {
                        values
                    } else {
                        vec![
                            "Original-Subject".to_string(),
                            "Original-From".to_string(),
                            "Received".to_string(),
                            "Auto-Submitted".to_string(),
                        ]
                    }
                })
                .with_vacation_default_subject(
                    config
                        .value("sieve.untrusted.vacation.default-subject")
                        .unwrap_or("Automated reply")
                        .to_string(),
                )
                .with_vacation_subject_prefix(
                    config
                        .value("sieve.untrusted.vacation.subject-prefix")
                        .unwrap_or("Auto: ")
                        .to_string(),
                )
                .with_env_variable("name", "Stalwart JMAP")
                .with_env_variable("version", env!("CARGO_PKG_VERSION"))
                .with_env_variable("location", "MS")
                .with_env_variable("phase", "during"),
        });

        // Spawn delivery manager
        spawn_delivery_manager(jmap_server.clone(), delivery_rx);

        // Spawn state manager
        spawn_state_manager(jmap_server.clone(), config, state_rx);

        // Spawn housekeeper
        spawn_housekeeper(jmap_server.clone(), config, servers, housekeeper_rx);

        Ok(jmap_server)
    }

    pub async fn assign_document_id(
        &self,
        account_id: u32,
        collection: Collection,
    ) -> Result<u32, MethodError> {
        self.store
            .assign_document_id(account_id, collection)
            .await
            .map_err(|err| {
                tracing::error!(
                    event = "error",
                    context = "assign_document_id",
                    error = ?err,
                    "Failed to assign documentId.");
                MethodError::ServerPartialFail
            })
    }

    pub async fn get_property<U>(
        &self,
        account_id: u32,
        collection: Collection,
        document_id: u32,
        property: impl AsRef<Property>,
    ) -> Result<Option<U>, MethodError>
    where
        U: Deserialize + 'static,
    {
        let property = property.as_ref();
        match self
            .store
            .get_value::<U>(ValueKey {
                account_id,
                collection: collection.into(),
                document_id,
                class: ValueClass::Property(property.into()),
            })
            .await
        {
            Ok(value) => Ok(value),
            Err(err) => {
                tracing::error!(event = "error",
                                context = "store",
                                account_id = account_id,
                                collection = ?collection,
                                document_id = document_id,
                                property = ?property,
                                error = ?err,
                                "Failed to retrieve property");
                Err(MethodError::ServerPartialFail)
            }
        }
    }

    pub async fn get_properties<U>(
        &self,
        account_id: u32,
        collection: Collection,
        document_ids: impl Iterator<Item = u32>,
        property: impl AsRef<Property>,
    ) -> Result<Vec<Option<U>>, MethodError>
    where
        U: Deserialize + 'static,
    {
        let property = property.as_ref();

        match self
            .store
            .get_values::<U>(
                document_ids
                    .map(|document_id| ValueKey {
                        account_id,
                        collection: collection.into(),
                        document_id,
                        class: ValueClass::Property(property.into()),
                    })
                    .collect(),
            )
            .await
        {
            Ok(value) => Ok(value),
            Err(err) => {
                tracing::error!(event = "error",
                                context = "store",
                                account_id = account_id,
                                collection = ?collection,
                                property = ?property,
                                error = ?err,
                                "Failed to retrieve properties");
                Err(MethodError::ServerPartialFail)
            }
        }
    }

    pub async fn get_document_ids(
        &self,
        account_id: u32,
        collection: Collection,
    ) -> Result<Option<RoaringBitmap>, MethodError> {
        match self
            .store
            .get_bitmap(BitmapKey::document_ids(account_id, collection))
            .await
        {
            Ok(value) => Ok(value),
            Err(err) => {
                tracing::error!(event = "error",
                                context = "store",
                                account_id = account_id,
                                collection = ?collection,
                                error = ?err,
                                "Failed to retrieve document ids bitmap");
                Err(MethodError::ServerPartialFail)
            }
        }
    }

    pub async fn get_tag(
        &self,
        account_id: u32,
        collection: Collection,
        property: impl AsRef<Property>,
        value: impl Into<TagValue>,
    ) -> Result<Option<RoaringBitmap>, MethodError> {
        let property = property.as_ref();
        match self
            .store
            .get_bitmap(BitmapKey {
                account_id,
                collection: collection.into(),
                class: BitmapClass::Tag {
                    field: property.into(),
                    value: value.into(),
                },
                block_num: 0,
            })
            .await
        {
            Ok(value) => Ok(value),
            Err(err) => {
                tracing::error!(event = "error",
                                context = "store",
                                account_id = account_id,
                                collection = ?collection,
                                property = ?property,
                                error = ?err,
                                "Failed to retrieve tag bitmap");
                Err(MethodError::ServerPartialFail)
            }
        }
    }

    pub async fn prepare_set_response<T>(
        &self,
        request: &SetRequest<T>,
        collection: Collection,
    ) -> Result<SetResponse, MethodError> {
        Ok(
            SetResponse::from_request(request, self.config.set_max_objects)?.with_state(
                self.assert_state(
                    request.account_id.document_id(),
                    collection,
                    &request.if_in_state,
                )
                .await?,
            ),
        )
    }

    pub async fn get_quota(
        &self,
        access_token: &AccessToken,
        account_id: u32,
    ) -> Result<i64, MethodError> {
        Ok(if access_token.primary_id == account_id {
            access_token.quota as i64
        } else {
            self.directory
                .query(QueryBy::Id(account_id), false)
                .await
                .map_err(|err| {
                    tracing::error!(
                        event = "error",
                        context = "get_quota",
                        account_id = account_id,
                        error = ?err,
                        "Failed to obtain disk quota for account.");
                    MethodError::ServerPartialFail
                })?
                .map(|p| p.quota as i64)
                .unwrap_or_default()
        })
    }

    pub async fn get_used_quota(&self, account_id: u32) -> Result<i64, MethodError> {
        self.store
            .get_counter(DirectoryClass::UsedQuota(account_id))
            .await
            .map_err(|err| {
                tracing::error!(
                event = "error",
                context = "get_used_quota",
                account_id = account_id,
                error = ?err,
                "Failed to obtain used disk quota for account.");
                MethodError::ServerPartialFail
            })
    }

    pub async fn filter(
        &self,
        account_id: u32,
        collection: Collection,
        filters: Vec<Filter>,
    ) -> Result<ResultSet, MethodError> {
        self.store
            .filter(account_id, collection, filters)
            .await
            .map_err(|err| {
                tracing::error!(event = "error",
                                context = "filter",
                                account_id = account_id,
                                collection = ?collection,
                                error = ?err,
                                "Failed to execute filter.");

                MethodError::ServerPartialFail
            })
    }

    pub async fn fts_filter<T: Into<u8> + Display + Clone + std::fmt::Debug>(
        &self,
        account_id: u32,
        collection: Collection,
        filters: Vec<FtsFilter<T>>,
    ) -> Result<RoaringBitmap, MethodError> {
        self.fts_store
            .query(account_id, collection, filters)
            .await
            .map_err(|err| {
                tracing::error!(event = "error",
                                context = "fts-filter",
                                account_id = account_id,
                                collection = ?collection,
                                error = ?err,
                                "Failed to execute filter.");

                MethodError::ServerPartialFail
            })
    }

    pub async fn build_query_response<T>(
        &self,
        result_set: &ResultSet,
        request: &QueryRequest<T>,
    ) -> Result<(QueryResponse, Option<Pagination>), MethodError> {
        let total = result_set.results.len() as usize;
        let (limit_total, limit) = if let Some(limit) = request.limit {
            if limit > 0 {
                let limit = std::cmp::min(limit, self.config.query_max_results);
                (std::cmp::min(limit, total), limit)
            } else {
                (0, 0)
            }
        } else {
            (
                std::cmp::min(self.config.query_max_results, total),
                self.config.query_max_results,
            )
        };
        Ok((
            QueryResponse {
                account_id: request.account_id,
                query_state: self
                    .get_state(result_set.account_id, result_set.collection)
                    .await?,
                can_calculate_changes: true,
                position: 0,
                ids: vec![],
                total: if request.calculate_total.unwrap_or(false) {
                    Some(total)
                } else {
                    None
                },
                limit: if total > limit { Some(limit) } else { None },
            },
            if limit_total > 0 {
                Pagination::new(
                    limit_total,
                    request.position.unwrap_or(0),
                    request.anchor.map(|a| a.document_id()),
                    request.anchor_offset.unwrap_or(0),
                )
                .into()
            } else {
                None
            },
        ))
    }

    pub async fn sort(
        &self,
        result_set: ResultSet,
        comparators: Vec<Comparator>,
        paginate: Pagination,
        mut response: QueryResponse,
    ) -> Result<QueryResponse, MethodError> {
        // Sort results
        let collection = result_set.collection;
        let account_id = result_set.account_id;
        response.update_results(
            match self.store.sort(result_set, comparators, paginate).await {
                Ok(result) => result,
                Err(err) => {
                    tracing::error!(event = "error",
                                context = "store",
                                account_id = account_id,
                                collection = ?collection,
                                error = ?err,
                                "Sort failed");
                    return Err(MethodError::ServerPartialFail);
                }
            },
        )?;

        Ok(response)
    }

    pub async fn write_batch(&self, batch: BatchBuilder) -> Result<(), MethodError> {
        self.store.write(batch.build()).await.map_err(|err| {
            match err {
                store::Error::InternalError(err) => {
                    tracing::error!(
                        event = "error",
                        context = "write_batch",
                        error = ?err,
                        "Failed to write batch.");
                    MethodError::ServerPartialFail
                }
                store::Error::AssertValueFailed => {
                    // This should not occur, as we are not using assertions.
                    tracing::debug!(
                        event = "assert_failed",
                        context = "write_batch",
                        "Failed to assert value."
                    );
                    MethodError::ServerUnavailable
                }
            }
        })
    }
}

impl<T: serde::Serialize + serde::de::DeserializeOwned> Bincode<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T: serde::Serialize + serde::de::DeserializeOwned> Serialize for &Bincode<T> {
    fn serialize(self) -> Vec<u8> {
        lz4_flex::compress_prepend_size(&bincode::serialize(&self.inner).unwrap_or_default())
    }
}

impl<T: serde::Serialize + serde::de::DeserializeOwned> Serialize for Bincode<T> {
    fn serialize(self) -> Vec<u8> {
        lz4_flex::compress_prepend_size(&bincode::serialize(&self.inner).unwrap_or_default())
    }
}

impl<T: serde::Serialize + serde::de::DeserializeOwned + Sized + Sync + Send> Deserialize
    for Bincode<T>
{
    fn deserialize(bytes: &[u8]) -> store::Result<Self> {
        lz4_flex::decompress_size_prepended(bytes)
            .map_err(|err| {
                store::Error::InternalError(format!("Bincode decompression failed: {err:?}"))
            })
            .and_then(|result| {
                bincode::deserialize(&result).map_err(|err| {
                    store::Error::InternalError(format!(
                        "Bincode deserialization failed (len {}): {err:?}",
                        result.len()
                    ))
                })
            })
            .map(|inner| Self { inner })
    }
}

impl<T: serde::Serialize + serde::de::DeserializeOwned> ToBitmaps for Bincode<T> {
    fn to_bitmaps(&self, _ops: &mut Vec<store::write::Operation>, _field: u8, _set: bool) {
        unreachable!()
    }
}

impl<T: serde::Serialize + serde::de::DeserializeOwned> ToBitmaps for &Bincode<T> {
    fn to_bitmaps(&self, _ops: &mut Vec<store::write::Operation>, _field: u8, _set: bool) {
        unreachable!()
    }
}

trait UpdateResults: Sized {
    fn update_results(&mut self, sorted_results: SortedResultSet) -> Result<(), MethodError>;
}

impl UpdateResults for QueryResponse {
    fn update_results(&mut self, sorted_results: SortedResultSet) -> Result<(), MethodError> {
        // Prepare response
        if sorted_results.found_anchor {
            self.position = sorted_results.position;
            self.ids = sorted_results
                .ids
                .into_iter()
                .map(|id| id.into())
                .collect::<Vec<_>>();
            Ok(())
        } else {
            Err(MethodError::AnchorNotFound)
        }
    }
}
