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

use ldap3::LdapConnSettings;
use store::Store;
use utils::config::{utils::AsKey, Config};

use crate::core::config::build_pool;

use super::{Bind, LdapConnectionManager, LdapDirectory, LdapFilter, LdapMappings};

impl LdapDirectory {
    pub fn from_config(
        config: &Config,
        prefix: impl AsKey,
        data_store: Store,
    ) -> utils::config::Result<Self> {
        let prefix = prefix.as_key();
        let bind_dn = if let Some(dn) = config.value((&prefix, "bind.dn")) {
            Bind::new(
                dn.to_string(),
                config.value_require((&prefix, "bind.secret"))?.to_string(),
            )
            .into()
        } else {
            None
        };

        let manager = LdapConnectionManager::new(
            config.value_require((&prefix, "address"))?.to_string(),
            LdapConnSettings::new()
                .set_conn_timeout(config.property_or_static((&prefix, "timeout"), "30s")?)
                .set_starttls(config.property_or_static((&prefix, "tls.enable"), "false")?)
                .set_no_tls_verify(
                    config.property_or_static((&prefix, "tls.allow-invalid-certs"), "false")?,
                ),
            bind_dn,
        );

        let mut mappings = LdapMappings {
            base_dn: config.value_require((&prefix, "base-dn"))?.to_string(),
            filter_name: LdapFilter::from_config(config, (&prefix, "filter.name"))?,
            filter_email: LdapFilter::from_config(config, (&prefix, "filter.email"))?,
            filter_verify: LdapFilter::from_config(config, (&prefix, "filter.verify"))?,
            filter_expand: LdapFilter::from_config(config, (&prefix, "filter.expand"))?,
            filter_domains: LdapFilter::from_config(config, (&prefix, "filter.domains"))?,
            attr_name: config
                .values((&prefix, "attributes.name"))
                .map(|(_, v)| v.to_string())
                .collect(),
            attr_groups: config
                .values((&prefix, "attributes.groups"))
                .map(|(_, v)| v.to_string())
                .collect(),
            attr_type: config
                .values((&prefix, "attributes.type"))
                .map(|(_, v)| v.to_string())
                .collect(),
            attr_description: config
                .values((&prefix, "attributes.description"))
                .map(|(_, v)| v.to_string())
                .collect(),
            attr_secret: config
                .values((&prefix, "attributes.secret"))
                .map(|(_, v)| v.to_string())
                .collect(),
            attr_email_address: config
                .values((&prefix, "attributes.email"))
                .map(|(_, v)| v.to_string())
                .collect(),
            attr_quota: config
                .values((&prefix, "attributes.quota"))
                .map(|(_, v)| v.to_string())
                .collect(),
            attr_email_alias: config
                .values((&prefix, "attributes.email-alias"))
                .map(|(_, v)| v.to_string())
                .collect(),
            attrs_principal: vec!["objectClass".to_string()],
        };

        for attr in [
            &mappings.attr_name,
            &mappings.attr_type,
            &mappings.attr_description,
            &mappings.attr_secret,
            &mappings.attr_quota,
            &mappings.attr_groups,
            &mappings.attr_email_address,
            &mappings.attr_email_alias,
        ] {
            mappings.attrs_principal.extend(attr.iter().cloned());
        }

        let auth_bind =
            if config.property_or_static::<bool>((&prefix, "bind.auth.enable"), "false")? {
                LdapFilter::from_config(config, (&prefix, "bind.auth.dn"))?.into()
            } else {
                None
            };

        Ok(LdapDirectory {
            mappings,
            pool: build_pool(config, &prefix, manager)?,
            auth_bind,
            data_store,
        })
    }
}

impl LdapFilter {
    fn from_config(config: &Config, key: impl AsKey) -> utils::config::Result<Self> {
        if let Some(value) = config.value(key.clone()) {
            let filter = LdapFilter {
                filter: value.split('?').map(|s| s.to_string()).collect(),
            };
            if filter.filter.len() >= 2 {
                Ok(filter)
            } else {
                Err(format!(
                    "Missing '?' parameter placeholder in filter {:?} with value {:?}",
                    key.as_key(),
                    value
                ))
            }
        } else {
            Ok(Self::default())
        }
    }
}
