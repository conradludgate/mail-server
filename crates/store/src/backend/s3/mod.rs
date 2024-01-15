/*
 * Copyright (c) 2023, Stalwart Labs Ltd.
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

use std::{ops::Range, time::Duration};

use s3::{
    creds::{error::CredentialsError, Credentials},
    error::S3Error,
    Bucket, Region,
};
use utils::{
    codec::base32_custom::Base32Writer,
    config::{utils::AsKey, Config},
};

pub struct S3Store {
    bucket: Bucket,
    prefix: String,
}

impl S3Store {
    fn get_path(&self, key: &[u8]) -> String {
        let key = Base32Writer::from_bytes(key).finalize();
        if self.prefix.is_empty() {
            key
        } else {
            format!("{}/{key}", self.prefix)
        }
    }

    pub async fn open(config: &Config, prefix: impl AsKey) -> crate::Result<Self> {
        // Obtain region and endpoint from config
        let prefix = prefix.as_key();
        let region = config.value_require((&prefix, "region"))?;
        let region = if let Some(endpoint) = config.value((&prefix, "endpoint")) {
            Region::Custom {
                region: region.to_string(),
                endpoint: endpoint.to_string(),
            }
        } else {
            region.parse().unwrap()
        };
        let credentials = Credentials::new(
            config.value((&prefix, "access-key")),
            config.value((&prefix, "secret-key")),
            config.value((&prefix, "security-token")),
            config.value((&prefix, "session-token")),
            config.value((&prefix, "profile")),
        )?;
        let timeout = config.property_or_static::<Duration>((&prefix, "timeout"), "30s")?;

        Ok(S3Store {
            prefix: config
                .value((&prefix, "prefix"))
                .unwrap_or_default()
                .to_owned(),
            bucket: Bucket::new(
                config.value_require((&prefix, "bucket"))?,
                region,
                credentials,
            )?
            .with_path_style()
            .with_request_timeout(timeout),
        })
    }

    pub(crate) async fn get_blob(
        &self,
        key: &[u8],
        range: Range<u32>,
    ) -> crate::Result<Option<Vec<u8>>> {
        let path = self.get_path(key);
        let response = if range.start != 0 || range.end != u32::MAX {
            self.bucket
                .get_object_range(
                    path,
                    range.start as u64,
                    Some(range.end.saturating_sub(1) as u64),
                )
                .await
        } else {
            self.bucket.get_object(path).await
        };
        match response {
            Ok(response) if (200..300).contains(&response.status_code()) => {
                Ok(Some(response.to_vec()))
            }
            Ok(response) if response.status_code() == 404 => Ok(None),
            Ok(response) => Err(crate::Error::InternalError(format!(
                "S3 error code {}: {}",
                response.status_code(),
                String::from_utf8_lossy(response.as_slice())
            ))),
            Err(err) => Err(err.into()),
        }
    }

    pub(crate) async fn put_blob(&self, key: &[u8], data: &[u8]) -> crate::Result<()> {
        let path = self.get_path(key);
        match self.bucket.put_object(path, data).await {
            Ok(response) if (200..300).contains(&response.status_code()) => Ok(()),
            Ok(response) => Err(crate::Error::InternalError(format!(
                "S3 error code {}: {}",
                response.status_code(),
                String::from_utf8_lossy(response.as_slice())
            ))),
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) async fn delete_blob(&self, key: &[u8]) -> crate::Result<bool> {
        let path = self.get_path(key);
        self.bucket
            .delete_object(path)
            .await
            .map(|response| (200..300).contains(&response.status_code()))
            .map_err(|e| e.into())
    }
}

impl From<S3Error> for crate::Error {
    fn from(err: S3Error) -> Self {
        Self::InternalError(format!("S3 error: {}", err))
    }
}

impl From<CredentialsError> for crate::Error {
    fn from(err: CredentialsError) -> Self {
        Self::InternalError(format!("S3 Credentials error: {}", err))
    }
}
