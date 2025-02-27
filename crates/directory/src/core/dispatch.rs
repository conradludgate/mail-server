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

use std::net::IpAddr;

use mail_send::Credentials;
use store::Store;

use crate::{
    backend::internal::lookup::DirectoryStore, AuthResult, Directory, DirectoryInner, Principal,
    QueryBy,
};

impl Directory {
    pub async fn authenticate(
        &self,
        credentials: &Credentials<String>,
        remote_ip: IpAddr,
        return_member_of: bool,
    ) -> crate::Result<AuthResult<Principal<u32>>> {
        if let Some(principal) = self
            .query(QueryBy::Credentials(credentials), return_member_of)
            .await?
        {
            Ok(AuthResult::Success(principal))
        } else if self.blocked_ips.has_fail2ban() {
            let login = match credentials {
                Credentials::Plain { username, .. }
                | Credentials::XOauth2 { username, .. }
                | Credentials::OAuthBearer { token: username } => username,
            };
            if let Some(banned) = self
                .blocked_ips
                .is_fail2banned(remote_ip, login.to_string())
            {
                tracing::info!(
                    context = "directory",
                    event = "fail2ban",
                    remote_ip = ?remote_ip,
                    login = ?login,
                    "IP address blocked after too many failed login attempts",
                );

                // Write blocked address to config
                self.store().config_set(vec![banned].into_iter()).await?;

                Ok(AuthResult::Banned)
            } else {
                Ok(AuthResult::Failure)
            }
        } else {
            Ok(AuthResult::Failure)
        }
    }

    pub async fn query(
        &self,
        by: QueryBy<'_>,
        return_member_of: bool,
    ) -> crate::Result<Option<Principal<u32>>> {
        match &self.store {
            DirectoryInner::Internal(store) => store.query(by, return_member_of).await,
            DirectoryInner::Ldap(store) => store.query(by, return_member_of).await,
            DirectoryInner::Sql(store) => store.query(by, return_member_of).await,
            DirectoryInner::Imap(store) => store.query(by).await,
            DirectoryInner::Smtp(store) => store.query(by).await,
            DirectoryInner::Memory(store) => store.query(by).await,
        }
    }

    pub async fn email_to_ids(&self, email: &str) -> crate::Result<Vec<u32>> {
        let mut address = self.subaddressing.to_subaddress(email);
        for _ in 0..2 {
            let result = match &self.store {
                DirectoryInner::Internal(store) => store.email_to_ids(address.as_ref()).await,
                DirectoryInner::Ldap(store) => store.email_to_ids(address.as_ref()).await,
                DirectoryInner::Sql(store) => store.email_to_ids(address.as_ref()).await,
                DirectoryInner::Imap(store) => store.email_to_ids(address.as_ref()).await,
                DirectoryInner::Smtp(store) => store.email_to_ids(address.as_ref()).await,
                DirectoryInner::Memory(store) => store.email_to_ids(address.as_ref()).await,
            }?;

            if !result.is_empty() {
                return Ok(result);
            } else if let Some(catch_all) = self.catch_all.to_catch_all(email) {
                address = catch_all;
            } else {
                break;
            }
        }

        Ok(vec![])
    }

    pub async fn is_local_domain(&self, domain: &str) -> crate::Result<bool> {
        // Check cache
        if let Some(cache) = &self.cache {
            if let Some(result) = cache.get_domain(domain) {
                return Ok(result);
            }
        }

        let result = match &self.store {
            DirectoryInner::Internal(store) => store.is_local_domain(domain).await,
            DirectoryInner::Ldap(store) => store.is_local_domain(domain).await,
            DirectoryInner::Sql(store) => store.is_local_domain(domain).await,
            DirectoryInner::Imap(store) => store.is_local_domain(domain).await,
            DirectoryInner::Smtp(store) => store.is_local_domain(domain).await,
            DirectoryInner::Memory(store) => store.is_local_domain(domain).await,
        }?;

        // Update cache
        if let Some(cache) = &self.cache {
            cache.set_domain(domain, result);
        }

        Ok(result)
    }

    pub async fn rcpt(&self, email: &str) -> crate::Result<bool> {
        // Expand subaddress
        let mut address = self.subaddressing.to_subaddress(email);

        // Check cache
        if let Some(cache) = &self.cache {
            if let Some(result) = cache.get_rcpt(address.as_ref()) {
                return Ok(result);
            }
        }

        for _ in 0..2 {
            let result = match &self.store {
                DirectoryInner::Internal(store) => store.rcpt(address.as_ref()).await,
                DirectoryInner::Ldap(store) => store.rcpt(address.as_ref()).await,
                DirectoryInner::Sql(store) => store.rcpt(address.as_ref()).await,
                DirectoryInner::Imap(store) => store.rcpt(address.as_ref()).await,
                DirectoryInner::Smtp(store) => store.rcpt(address.as_ref()).await,
                DirectoryInner::Memory(store) => store.rcpt(address.as_ref()).await,
            }?;

            if result {
                // Update cache
                if let Some(cache) = &self.cache {
                    cache.set_rcpt(address.as_ref(), true);
                }
                return Ok(true);
            } else if let Some(catch_all) = self.catch_all.to_catch_all(email) {
                // Check cache
                if let Some(cache) = &self.cache {
                    if let Some(result) = cache.get_rcpt(catch_all.as_ref()) {
                        return Ok(result);
                    }
                }
                address = catch_all;
            } else {
                break;
            }
        }

        // Update cache
        if let Some(cache) = &self.cache {
            cache.set_rcpt(address.as_ref(), false);
        }

        Ok(false)
    }

    pub async fn vrfy(&self, address: &str) -> crate::Result<Vec<String>> {
        let address = self.subaddressing.to_subaddress(address);
        match &self.store {
            DirectoryInner::Internal(store) => store.vrfy(address.as_ref()).await,
            DirectoryInner::Ldap(store) => store.vrfy(address.as_ref()).await,
            DirectoryInner::Sql(store) => store.vrfy(address.as_ref()).await,
            DirectoryInner::Imap(store) => store.vrfy(address.as_ref()).await,
            DirectoryInner::Smtp(store) => store.vrfy(address.as_ref()).await,
            DirectoryInner::Memory(store) => store.vrfy(address.as_ref()).await,
        }
    }

    pub async fn expn(&self, address: &str) -> crate::Result<Vec<String>> {
        let address = self.subaddressing.to_subaddress(address);
        match &self.store {
            DirectoryInner::Internal(store) => store.expn(address.as_ref()).await,
            DirectoryInner::Ldap(store) => store.expn(address.as_ref()).await,
            DirectoryInner::Sql(store) => store.expn(address.as_ref()).await,
            DirectoryInner::Imap(store) => store.expn(address.as_ref()).await,
            DirectoryInner::Smtp(store) => store.expn(address.as_ref()).await,
            DirectoryInner::Memory(store) => store.expn(address.as_ref()).await,
        }
    }

    fn store(&self) -> &Store {
        match &self.store {
            DirectoryInner::Internal(store) => store,
            DirectoryInner::Ldap(store) => &store.data_store,
            DirectoryInner::Sql(store) => &store.data_store,
            DirectoryInner::Imap(store) => &store.data_store,
            DirectoryInner::Smtp(store) => &store.data_store,
            DirectoryInner::Memory(store) => &store.data_store,
        }
    }
}
