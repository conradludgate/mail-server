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

use ahash::AHashSet;

use crate::{
    write::BatchBuilder, BlobClass, BlobHash, BlobStore, Deserialize, IterateParams, Store,
    ValueKey, BLOB_HASH_LEN, U32_LEN, U64_LEN,
};

use super::{key::DeserializeBigEndian, now, BlobOp, Operation, ValueClass, ValueOp};

#[derive(Debug, PartialEq, Eq)]
pub struct BlobQuota {
    pub bytes: usize,
    pub count: usize,
}

impl Store {
    pub async fn blob_exists(
        &self,
        hash: impl AsRef<BlobHash> + Sync + Send,
    ) -> crate::Result<bool> {
        self.get_value::<()>(ValueKey {
            account_id: 0,
            collection: 0,
            document_id: 0,
            class: ValueClass::Blob(BlobOp::Commit {
                hash: hash.as_ref().clone(),
            }),
        })
        .await
        .map(|v| v.is_some())
    }

    pub async fn blob_quota(&self, account_id: u32) -> crate::Result<BlobQuota> {
        let from_key = ValueKey {
            account_id,
            collection: 0,
            document_id: 0,
            class: ValueClass::Blob(BlobOp::Reserve {
                hash: BlobHash::default(),
                until: 0,
            }),
        };
        let to_key = ValueKey {
            account_id: account_id + 1,
            collection: 0,
            document_id: 0,
            class: ValueClass::Blob(BlobOp::Reserve {
                hash: BlobHash::default(),
                until: 0,
            }),
        };

        let now = now();
        let mut quota = BlobQuota { bytes: 0, count: 0 };

        self.iterate(
            IterateParams::new(from_key, to_key).ascending(),
            |key, value| {
                let until = key.deserialize_be_u64(key.len() - U64_LEN)?;
                if until > now {
                    let bytes = u32::deserialize(value)?;
                    if bytes > 0 {
                        quota.bytes += bytes as usize;
                        quota.count += 1;
                    }
                }
                Ok(true)
            },
        )
        .await?;

        Ok(quota)
    }

    pub async fn blob_has_access(
        &self,
        hash: impl AsRef<BlobHash> + Sync + Send,
        class: impl AsRef<BlobClass> + Sync + Send,
    ) -> crate::Result<bool> {
        let key = match class.as_ref() {
            BlobClass::Reserved {
                account_id,
                expires,
            } if *expires > now() => ValueKey {
                account_id: *account_id,
                collection: 0,
                document_id: 0,
                class: ValueClass::Blob(BlobOp::Reserve {
                    hash: hash.as_ref().clone(),
                    until: *expires,
                }),
            },
            BlobClass::Linked {
                account_id,
                collection,
                document_id,
            } => ValueKey {
                account_id: *account_id,
                collection: *collection,
                document_id: *document_id,
                class: ValueClass::Blob(BlobOp::Link {
                    hash: hash.as_ref().clone(),
                }),
            },
            _ => return Ok(false),
        };

        self.get_value::<()>(key).await.map(|v| v.is_some())
    }

    pub async fn purge_blobs(&self, blob_store: BlobStore) -> crate::Result<()> {
        // Remove expired temporary blobs
        let from_key = ValueKey {
            account_id: 0,
            collection: 0,
            document_id: 0,
            class: ValueClass::Blob(BlobOp::Reserve {
                until: 0,
                hash: BlobHash::default(),
            }),
        };
        let to_key = ValueKey {
            account_id: u32::MAX,
            collection: 0,
            document_id: 0,
            class: ValueClass::Blob(BlobOp::Reserve {
                until: 0,
                hash: BlobHash::default(),
            }),
        };
        let mut delete_keys = Vec::new();
        let mut active_hashes = AHashSet::new();
        let now = now();
        self.iterate(
            IterateParams::new(from_key, to_key).ascending().no_values(),
            |key, _| {
                let hash = BlobHash::try_from_hash_slice(
                    key.get(1 + U32_LEN..1 + U32_LEN + BLOB_HASH_LEN)
                        .ok_or_else(|| {
                            crate::Error::InternalError(format!(
                                "Invalid key {key:?} in blob hash tables"
                            ))
                        })?,
                )
                .unwrap();
                let until = key.deserialize_be_u64(key.len() - U64_LEN)?;
                if until <= now {
                    delete_keys.push(ValueKey {
                        account_id: key.deserialize_be_u32(1)?,
                        collection: 0,
                        document_id: 0,
                        class: ValueClass::Blob(BlobOp::Reserve { until, hash }),
                    });
                } else {
                    active_hashes.insert(hash);
                }
                Ok(true)
            },
        )
        .await?;

        // Validate linked blobs
        let from_key = ValueKey {
            account_id: 0,
            collection: 0,
            document_id: 0,
            class: ValueClass::Blob(BlobOp::Link {
                hash: BlobHash::default(),
            }),
        };
        let to_key = ValueKey {
            account_id: u32::MAX,
            collection: u8::MAX,
            document_id: u32::MAX,
            class: ValueClass::Blob(BlobOp::Link {
                hash: BlobHash::new_max(),
            }),
        };
        let mut last_hash = BlobHash::default();
        self.iterate(
            IterateParams::new(from_key, to_key).ascending().no_values(),
            |key, _| {
                let hash = BlobHash::try_from_hash_slice(
                    key.get(1..1 + BLOB_HASH_LEN).ok_or_else(|| {
                        crate::Error::InternalError(format!(
                            "Invalid key {key:?} in blob hash tables"
                        ))
                    })?,
                )
                .unwrap();
                let document_id = key.deserialize_be_u32(key.len() - U32_LEN)?;

                if document_id != u32::MAX {
                    if last_hash != hash {
                        last_hash = hash;
                    }
                } else if last_hash != hash && !active_hashes.contains(&hash) {
                    // Unlinked or expired blob, delete.
                    delete_keys.push(ValueKey {
                        account_id: 0,
                        collection: 0,
                        document_id: 0,
                        class: ValueClass::Blob(BlobOp::Commit { hash }),
                    });
                }

                Ok(true)
            },
        )
        .await?;

        // Delete expired or unlinked blobs
        for key in &delete_keys {
            if let ValueClass::Blob(BlobOp::Commit { hash }) = &key.class {
                blob_store.delete_blob(hash.as_ref()).await?;
            }
        }

        // Delete hashes
        let mut batch = BatchBuilder::new();
        let mut last_account_id = u32::MAX;
        for key in delete_keys.into_iter() {
            if batch.ops.len() >= 1000 {
                last_account_id = u32::MAX;
                self.write(batch.build()).await?;
                batch = BatchBuilder::new();
            }
            if matches!(key.class, ValueClass::Blob(BlobOp::Reserve { .. }))
                && key.account_id != last_account_id
            {
                batch.with_account_id(key.account_id);
                last_account_id = key.account_id;
            }
            batch.ops.push(Operation::Value {
                class: key.class,
                op: ValueOp::Clear,
            })
        }
        if !batch.is_empty() {
            self.write(batch.build()).await?;
        }

        Ok(())
    }

    pub async fn blob_hash_unlink_account(&self, account_id: u32) -> crate::Result<()> {
        // Validate linked blobs
        let from_key = ValueKey {
            account_id: 0,
            collection: 0,
            document_id: 0,
            class: ValueClass::Blob(BlobOp::Link {
                hash: BlobHash::default(),
            }),
        };
        let to_key = ValueKey {
            account_id: u32::MAX,
            collection: u8::MAX,
            document_id: u32::MAX,
            class: ValueClass::Blob(BlobOp::Link {
                hash: BlobHash::new_max(),
            }),
        };
        let mut delete_keys = Vec::new();
        self.iterate(
            IterateParams::new(from_key, to_key).ascending().no_values(),
            |key, _| {
                let document_id = key.deserialize_be_u32(key.len() - U32_LEN)?;

                if document_id != u32::MAX
                    && key.deserialize_be_u32(1 + BLOB_HASH_LEN)? == account_id
                {
                    delete_keys.push(ValueKey {
                        account_id,
                        collection: key[1 + BLOB_HASH_LEN + U32_LEN],
                        document_id,
                        class: ValueClass::Blob(BlobOp::Link {
                            hash: BlobHash::try_from_hash_slice(
                                key.get(1..1 + BLOB_HASH_LEN).ok_or_else(|| {
                                    crate::Error::InternalError(format!(
                                        "Invalid key {key:?} in blob hash tables"
                                    ))
                                })?,
                            )
                            .unwrap(),
                        }),
                    });
                }

                Ok(true)
            },
        )
        .await?;

        // Unlink blobs
        let mut batch = BatchBuilder::new();
        batch.with_account_id(account_id);
        let mut last_collection = u8::MAX;
        for key in delete_keys.into_iter() {
            if batch.ops.len() >= 1000 {
                self.write(batch.build()).await?;
                batch = BatchBuilder::new();
                batch.with_account_id(account_id);
                last_collection = u8::MAX;
            }
            if key.collection != last_collection {
                batch.with_collection(key.collection);
                last_collection = key.collection;
            }
            batch.update_document(key.document_id);
            batch.ops.push(Operation::Value {
                class: key.class,
                op: ValueOp::Clear,
            });
        }
        if !batch.is_empty() {
            self.write(batch.build()).await?;
        }

        Ok(())
    }
}
