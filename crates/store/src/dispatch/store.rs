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

use std::ops::{BitAndAssign, Range};

use roaring::RoaringBitmap;

use crate::{
    write::{key::KeySerializer, AnyKey, Batch, BitmapClass, ValueClass},
    BitmapKey, Deserialize, IterateParams, Key, Store, ValueKey, SUBSPACE_BITMAPS,
    SUBSPACE_INDEXES, SUBSPACE_LOGS, U32_LEN,
};

#[cfg(feature = "test_mode")]
lazy_static::lazy_static! {
pub static ref BITMAPS: std::sync::Arc<parking_lot::Mutex<std::collections::HashMap<Vec<u8>, std::collections::HashSet<u32>>>> =
                    std::sync::Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new()));
}

impl Store {
    pub async fn get_value<U>(&self, key: impl Key) -> crate::Result<Option<U>>
    where
        U: Deserialize + 'static,
    {
        match self {
            #[cfg(feature = "sqlite")]
            Self::SQLite(store) => store.get_value(key).await,
            #[cfg(feature = "foundation")]
            Self::FoundationDb(store) => store.get_value(key).await,
            #[cfg(feature = "postgres")]
            Self::PostgreSQL(store) => store.get_value(key).await,
            #[cfg(feature = "mysql")]
            Self::MySQL(store) => store.get_value(key).await,
            #[cfg(feature = "rocks")]
            Self::RocksDb(store) => store.get_value(key).await,
        }
    }

    pub async fn get_values<U>(&self, key: Vec<impl Key>) -> crate::Result<Vec<Option<U>>>
    where
        U: Deserialize + 'static,
    {
        let mut results = Vec::with_capacity(key.len());

        for key in key {
            results.push(self.get_value(key).await?);
        }

        Ok(results)
    }

    pub async fn get_bitmap(
        &self,
        key: BitmapKey<BitmapClass>,
    ) -> crate::Result<Option<RoaringBitmap>> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::SQLite(store) => store.get_bitmap(key).await,
            #[cfg(feature = "foundation")]
            Self::FoundationDb(store) => store.get_bitmap(key).await,
            #[cfg(feature = "postgres")]
            Self::PostgreSQL(store) => store.get_bitmap(key).await,
            #[cfg(feature = "mysql")]
            Self::MySQL(store) => store.get_bitmap(key).await,
            #[cfg(feature = "rocks")]
            Self::RocksDb(store) => store.get_bitmap(key).await,
        }
    }

    pub async fn get_bitmaps_intersection(
        &self,
        keys: Vec<BitmapKey<BitmapClass>>,
    ) -> crate::Result<Option<RoaringBitmap>> {
        let mut result: Option<RoaringBitmap> = None;
        for key in keys {
            if let Some(bitmap) = self.get_bitmap(key).await? {
                if let Some(result) = &mut result {
                    result.bitand_assign(&bitmap);
                    if result.is_empty() {
                        break;
                    }
                } else {
                    result = Some(bitmap);
                }
            } else {
                return Ok(None);
            }
        }
        Ok(result)
    }

    pub async fn iterate<T: Key>(
        &self,
        params: IterateParams<T>,
        cb: impl for<'x> FnMut(&'x [u8], &'x [u8]) -> crate::Result<bool> + Sync + Send,
    ) -> crate::Result<()> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::SQLite(store) => store.iterate(params, cb).await,
            #[cfg(feature = "foundation")]
            Self::FoundationDb(store) => store.iterate(params, cb).await,
            #[cfg(feature = "postgres")]
            Self::PostgreSQL(store) => store.iterate(params, cb).await,
            #[cfg(feature = "mysql")]
            Self::MySQL(store) => store.iterate(params, cb).await,
            #[cfg(feature = "rocks")]
            Self::RocksDb(store) => store.iterate(params, cb).await,
        }
    }

    pub async fn get_counter(
        &self,
        key: impl Into<ValueKey<ValueClass>> + Sync + Send,
    ) -> crate::Result<i64> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::SQLite(store) => store.get_counter(key).await,
            #[cfg(feature = "foundation")]
            Self::FoundationDb(store) => store.get_counter(key).await,
            #[cfg(feature = "postgres")]
            Self::PostgreSQL(store) => store.get_counter(key).await,
            #[cfg(feature = "mysql")]
            Self::MySQL(store) => store.get_counter(key).await,
            #[cfg(feature = "rocks")]
            Self::RocksDb(store) => store.get_counter(key).await,
        }
    }

    pub async fn write(&self, batch: Batch) -> crate::Result<()> {
        #[cfg(feature = "test_mode")]
        if std::env::var("PARANOID_WRITE").map_or(false, |v| v == "1") {
            use crate::write::Operation;
            let mut account_id = u32::MAX;
            let mut collection = u8::MAX;
            let mut document_id = u32::MAX;

            let mut bitmaps = Vec::new();

            for op in &batch.ops {
                match op {
                    Operation::AccountId {
                        account_id: account_id_,
                    } => {
                        account_id = *account_id_;
                    }
                    Operation::Collection {
                        collection: collection_,
                    } => {
                        collection = *collection_;
                    }
                    Operation::DocumentId {
                        document_id: document_id_,
                    } => {
                        document_id = *document_id_;
                    }
                    Operation::Bitmap { class, set } => {
                        let key = BitmapKey {
                            account_id,
                            collection,
                            block_num: 0,
                            class,
                        }
                        .serialize(0);
                        bitmaps.push((key, class.clone(), document_id, *set));
                    }
                    _ => {}
                }
            }

            match self {
                #[cfg(feature = "sqlite")]
                Self::SQLite(store) => store.write(batch).await,
                #[cfg(feature = "foundation")]
                Self::FoundationDb(store) => store.write(batch).await,
                #[cfg(feature = "postgres")]
                Self::PostgreSQL(store) => store.write(batch).await,
                #[cfg(feature = "mysql")]
                Self::MySQL(store) => store.write(batch).await,
                #[cfg(feature = "rocks")]
                Self::RocksDb(store) => store.write(batch).await,
            }?;

            for (key, class, document_id, set) in bitmaps {
                let mut bitmaps = BITMAPS.lock();
                let map = bitmaps.entry(key).or_default();
                if set {
                    if !map.insert(document_id) {
                        println!(
                            concat!(
                                "WARNING: key {:?} already contains document {} for account ",
                                "{}, collection {}"
                            ),
                            class, document_id, account_id, collection
                        );
                    }
                } else if !map.remove(&document_id) {
                    println!(
                        concat!(
                            "WARNING: key {:?} does not contain document {} for account ",
                            "{}, collection {}"
                        ),
                        class, document_id, account_id, collection
                    );
                }
            }

            return Ok(());
        }

        match self {
            #[cfg(feature = "sqlite")]
            Self::SQLite(store) => store.write(batch).await,
            #[cfg(feature = "foundation")]
            Self::FoundationDb(store) => store.write(batch).await,
            #[cfg(feature = "postgres")]
            Self::PostgreSQL(store) => store.write(batch).await,
            #[cfg(feature = "mysql")]
            Self::MySQL(store) => store.write(batch).await,
            #[cfg(feature = "rocks")]
            Self::RocksDb(store) => store.write(batch).await,
        }
    }

    pub async fn purge_bitmaps(&self) -> crate::Result<()> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::SQLite(store) => store.purge_bitmaps().await,
            #[cfg(feature = "foundation")]
            Self::FoundationDb(store) => store.purge_bitmaps().await,
            #[cfg(feature = "postgres")]
            Self::PostgreSQL(store) => store.purge_bitmaps().await,
            #[cfg(feature = "mysql")]
            Self::MySQL(store) => store.purge_bitmaps().await,
            #[cfg(feature = "rocks")]
            Self::RocksDb(store) => store.purge_bitmaps().await,
        }
    }
    pub(crate) async fn delete_range(&self, from: impl Key, to: impl Key) -> crate::Result<()> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::SQLite(store) => store.delete_range(from, to).await,
            #[cfg(feature = "foundation")]
            Self::FoundationDb(store) => store.delete_range(from, to).await,
            #[cfg(feature = "postgres")]
            Self::PostgreSQL(store) => store.delete_range(from, to).await,
            #[cfg(feature = "mysql")]
            Self::MySQL(store) => store.delete_range(from, to).await,
            #[cfg(feature = "rocks")]
            Self::RocksDb(store) => store.delete_range(from, to).await,
        }
    }

    pub async fn purge_account(&self, account_id: u32) -> crate::Result<()> {
        for subspace in [SUBSPACE_BITMAPS, SUBSPACE_LOGS, SUBSPACE_INDEXES] {
            self.delete_range(
                AnyKey {
                    subspace,
                    key: KeySerializer::new(U32_LEN).write(account_id).finalize(),
                },
                AnyKey {
                    subspace,
                    key: KeySerializer::new(U32_LEN).write(account_id + 1).finalize(),
                },
            )
            .await?;
        }

        for (from_class, to_class) in [
            (ValueClass::Acl(account_id), ValueClass::Acl(account_id + 1)),
            (ValueClass::ReservedId, ValueClass::ReservedId),
            (ValueClass::Property(0), ValueClass::Property(0)),
            (ValueClass::TermIndex, ValueClass::TermIndex),
        ] {
            self.delete_range(
                ValueKey {
                    account_id,
                    collection: 0,
                    document_id: 0,
                    class: from_class,
                },
                ValueKey {
                    account_id: account_id + 1,
                    collection: 0,
                    document_id: 0,
                    class: to_class,
                },
            )
            .await?;
        }

        Ok(())
    }

    pub async fn get_blob(&self, key: &[u8], range: Range<u32>) -> crate::Result<Option<Vec<u8>>> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::SQLite(store) => store.get_blob(key, range).await,
            #[cfg(feature = "foundation")]
            Self::FoundationDb(store) => store.get_blob(key, range).await,
            #[cfg(feature = "postgres")]
            Self::PostgreSQL(store) => store.get_blob(key, range).await,
            #[cfg(feature = "mysql")]
            Self::MySQL(store) => store.get_blob(key, range).await,
            #[cfg(feature = "rocks")]
            Self::RocksDb(store) => store.get_blob(key, range).await,
        }
    }

    pub async fn put_blob(&self, key: &[u8], data: &[u8]) -> crate::Result<()> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::SQLite(store) => store.put_blob(key, data).await,
            #[cfg(feature = "foundation")]
            Self::FoundationDb(store) => store.put_blob(key, data).await,
            #[cfg(feature = "postgres")]
            Self::PostgreSQL(store) => store.put_blob(key, data).await,
            #[cfg(feature = "mysql")]
            Self::MySQL(store) => store.put_blob(key, data).await,
            #[cfg(feature = "rocks")]
            Self::RocksDb(store) => store.put_blob(key, data).await,
        }
    }

    pub async fn delete_blob(&self, key: &[u8]) -> crate::Result<bool> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::SQLite(store) => store.delete_blob(key).await,
            #[cfg(feature = "foundation")]
            Self::FoundationDb(store) => store.delete_blob(key).await,
            #[cfg(feature = "postgres")]
            Self::PostgreSQL(store) => store.delete_blob(key).await,
            #[cfg(feature = "mysql")]
            Self::MySQL(store) => store.delete_blob(key).await,
            #[cfg(feature = "rocks")]
            Self::RocksDb(store) => store.delete_blob(key).await,
        }
    }

    #[cfg(feature = "test_mode")]
    pub async fn destroy(&self) {
        use crate::{SUBSPACE_BLOBS, SUBSPACE_COUNTERS, SUBSPACE_VALUES};

        for subspace in [
            SUBSPACE_VALUES,
            SUBSPACE_LOGS,
            SUBSPACE_BITMAPS,
            SUBSPACE_INDEXES,
            SUBSPACE_COUNTERS,
            SUBSPACE_BLOBS,
        ] {
            self.delete_range(
                AnyKey {
                    subspace,
                    key: &[0u8],
                },
                AnyKey {
                    subspace,
                    key: &[
                        u8::MAX,
                        u8::MAX,
                        u8::MAX,
                        u8::MAX,
                        u8::MAX,
                        u8::MAX,
                        u8::MAX,
                    ],
                },
            )
            .await
            .unwrap();
        }

        BITMAPS.lock().clear();
    }

    #[cfg(feature = "test_mode")]
    pub async fn blob_expire_all(&self) {
        use crate::{
            write::{key::DeserializeBigEndian, BatchBuilder, BlobOp, Operation, ValueOp},
            BlobHash, BLOB_HASH_LEN, U64_LEN,
        };

        // Delete all temporary hashes
        let from_key = ValueKey {
            account_id: 0,
            collection: 0,
            document_id: 0,
            class: ValueClass::Blob(BlobOp::Reserve {
                hash: BlobHash::default(),
                until: 0,
            }),
        };
        let to_key = ValueKey {
            account_id: u32::MAX,
            collection: 0,
            document_id: 0,
            class: ValueClass::Blob(BlobOp::Reserve {
                hash: BlobHash::default(),
                until: 0,
            }),
        };
        let mut batch = BatchBuilder::new();
        let mut last_account_id = u32::MAX;
        self.iterate(
            IterateParams::new(from_key, to_key).ascending().no_values(),
            |key, _| {
                let account_id = key.deserialize_be_u32(1)?;
                if account_id != last_account_id {
                    last_account_id = account_id;
                    batch.with_account_id(account_id);
                }

                batch.ops.push(Operation::Value {
                    class: ValueClass::Blob(BlobOp::Reserve {
                        hash: BlobHash::try_from_hash_slice(
                            key.get(1 + U32_LEN..1 + U32_LEN + BLOB_HASH_LEN).unwrap(),
                        )
                        .unwrap(),
                        until: key.deserialize_be_u64(key.len() - U64_LEN)?,
                    }),
                    op: ValueOp::Clear,
                });

                Ok(true)
            },
        )
        .await
        .unwrap();
        self.write(batch.build()).await.unwrap();
    }

    #[cfg(feature = "test_mode")]
    #[allow(unused_variables)]

    pub async fn assert_is_empty(&self, blob_store: crate::BlobStore) {
        use utils::codec::leb128::Leb128Iterator;

        use crate::{SUBSPACE_BLOBS, SUBSPACE_COUNTERS, SUBSPACE_VALUES};

        self.blob_expire_all().await;
        self.purge_blobs(blob_store).await.unwrap();
        self.purge_bitmaps().await.unwrap();

        let store = self.clone();
        let mut failed = false;

        for (subspace, with_values) in [
            (SUBSPACE_VALUES, true),
            (SUBSPACE_COUNTERS, false),
            (SUBSPACE_BLOBS, true),
            (SUBSPACE_BITMAPS, false),
            (SUBSPACE_INDEXES, false),
        ] {
            let from_key = crate::write::AnyKey {
                subspace,
                key: vec![0u8],
            };
            let to_key = crate::write::AnyKey {
                subspace,
                key: vec![u8::MAX; 10],
            };

            self.iterate(
                IterateParams::new(from_key, to_key).set_values(with_values),
                |key, value| {
                    match subspace {
                        SUBSPACE_BITMAPS => {
                            if key.get(0..4).unwrap_or_default() == u32::MAX.to_be_bytes() {
                                return Ok(true);
                            }

                            #[cfg(feature = "rocks")]
                            if matches!(store, Self::RocksDb(_))
                                && RoaringBitmap::deserialize(value).unwrap().is_empty()
                            {
                                return Ok(true);
                            }

                            const BM_DOCUMENT_IDS: u8 = 0;
                            const BM_TAG: u8 = 1 << 6;
                            const BM_TEXT: u8 = 1 << 7;
                            const TAG_TEXT: u8 = 1 << 0;
                            const TAG_STATIC: u8 = 1 << 1;

                            match key[5] {
                                BM_DOCUMENT_IDS => {
                                    eprint!("Found document ids bitmap");
                                }
                                BM_TAG => {
                                    eprint!(
                                        "Found tagged id {} bitmap",
                                        key[7..].iter().next_leb128::<u32>().unwrap()
                                    );
                                }
                                TAG_TEXT => {
                                    eprint!(
                                        "Found tagged text {:?} bitmap",
                                        String::from_utf8_lossy(&key[7..])
                                    );
                                }
                                TAG_STATIC => {
                                    eprint!("Found tagged static {} bitmap", key[7]);
                                }
                                other => {
                                    if other & BM_TEXT == BM_TEXT {
                                        eprint!(
                                            "Found text hash {:?} bitmap",
                                            String::from_utf8_lossy(&key[7..])
                                        );
                                    } else {
                                        eprint!("Found unknown bitmap");
                                    }
                                }
                            }

                            println!(
                                concat!(
                                    ", account {}, collection {},",
                                    " family {}, field {}, key {:?}: {:?}"
                                ),
                                u32::from_be_bytes(key[0..4].try_into().unwrap()),
                                key[4],
                                key[5],
                                key[6],
                                key,
                                value
                            );
                        }
                        SUBSPACE_VALUES
                            if key[0] == 3
                                || key[0] >= 20
                                || key.get(1..5).unwrap_or_default() == u32::MAX.to_be_bytes() =>
                        {
                            // Ignore lastId counter and ID mappings
                            return Ok(true);
                        }
                        SUBSPACE_COUNTERS if key.len() <= 4 => {
                            // Ignore named keys
                            return Ok(true);
                        }
                        SUBSPACE_INDEXES => {
                            println!(
                                concat!(
                                    "Found index key, account {}, collection {}, ",
                                    "document {}, property {}, value {:?}: {:?}"
                                ),
                                u32::from_be_bytes(key[0..4].try_into().unwrap()),
                                key[4],
                                u32::from_be_bytes(key[key.len() - 4..].try_into().unwrap()),
                                key[5],
                                String::from_utf8_lossy(&key[6..key.len() - 4]),
                                key
                            );
                        }
                        _ => {
                            println!(
                                "Found key in {:?}: {:?} {:?}",
                                char::from(subspace),
                                key,
                                value
                            );
                        }
                    }
                    failed = true;

                    Ok(true)
                },
            )
            .await
            .unwrap();
        }

        // Delete logs
        self.delete_range(
            AnyKey {
                subspace: SUBSPACE_LOGS,
                key: &[0u8],
            },
            AnyKey {
                subspace: SUBSPACE_LOGS,
                key: &[
                    u8::MAX,
                    u8::MAX,
                    u8::MAX,
                    u8::MAX,
                    u8::MAX,
                    u8::MAX,
                    u8::MAX,
                ],
            },
        )
        .await
        .unwrap();

        if failed {
            panic!("Store is not empty.");
        }
    }
}
