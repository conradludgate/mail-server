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

use ::utils::listener::limiter::{ConcurrencyLimiter, RateLimiter};
use dashmap::mapref::entry::Entry;
use tokio::io::{AsyncRead, AsyncWrite};
use utils::config::{KeyLookup, Rate};

use std::{
    hash::{BuildHasher, Hash, Hasher},
    net::IpAddr,
};

use crate::config::*;

use super::Session;

#[derive(Debug)]
pub struct Limiter {
    pub rate: Option<RateLimiter>,
    pub concurrency: Option<ConcurrencyLimiter>,
}

#[derive(Debug, Clone, Eq)]
pub struct ThrottleKey {
    hash: [u8; 32],
}

impl PartialEq for ThrottleKey {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Hash for ThrottleKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}

#[derive(Default)]
pub struct ThrottleKeyHasher {
    hash: u64,
}

impl Hasher for ThrottleKeyHasher {
    fn finish(&self) -> u64 {
        self.hash
    }

    fn write(&mut self, bytes: &[u8]) {
        self.hash = u64::from_ne_bytes((&bytes[..std::mem::size_of::<u64>()]).try_into().unwrap());
    }
}

#[derive(Clone, Default)]
pub struct ThrottleKeyHasherBuilder {}

impl BuildHasher for ThrottleKeyHasherBuilder {
    type Hasher = ThrottleKeyHasher;

    fn build_hasher(&self) -> Self::Hasher {
        ThrottleKeyHasher::default()
    }
}

impl QueueQuota {
    pub fn new_key(&self, e: &impl KeyLookup<Key = EnvelopeKey>) -> ThrottleKey {
        let mut hasher = blake3::Hasher::new();

        if (self.keys & THROTTLE_RCPT) != 0 {
            hasher.update(e.key(&EnvelopeKey::Recipient).as_bytes());
        }
        if (self.keys & THROTTLE_RCPT_DOMAIN) != 0 {
            hasher.update(e.key(&EnvelopeKey::RecipientDomain).as_bytes());
        }
        if (self.keys & THROTTLE_SENDER) != 0 {
            let sender = e.key(&EnvelopeKey::Sender);
            hasher.update(
                if !sender.is_empty() {
                    sender.as_ref()
                } else {
                    "<>"
                }
                .as_bytes(),
            );
        }
        if (self.keys & THROTTLE_SENDER_DOMAIN) != 0 {
            let sender_domain = e.key(&EnvelopeKey::SenderDomain);
            hasher.update(
                if !sender_domain.is_empty() {
                    sender_domain.as_ref()
                } else {
                    "<>"
                }
                .as_bytes(),
            );
        }

        if let Some(messages) = &self.messages {
            hasher.update(&messages.to_ne_bytes()[..]);
        }

        if let Some(size) = &self.size {
            hasher.update(&size.to_ne_bytes()[..]);
        }

        ThrottleKey {
            hash: hasher.finalize().into(),
        }
    }
}

impl Throttle {
    pub fn new_key(&self, e: &impl KeyLookup<Key = EnvelopeKey>) -> ThrottleKey {
        let mut hasher = blake3::Hasher::new();

        if (self.keys & THROTTLE_RCPT) != 0 {
            hasher.update(e.key(&EnvelopeKey::Recipient).as_bytes());
        }
        if (self.keys & THROTTLE_RCPT_DOMAIN) != 0 {
            hasher.update(e.key(&EnvelopeKey::RecipientDomain).as_bytes());
        }
        if (self.keys & THROTTLE_SENDER) != 0 {
            let sender = e.key(&EnvelopeKey::Sender);
            hasher.update(
                if !sender.is_empty() {
                    sender.as_ref()
                } else {
                    "<>"
                }
                .as_bytes(),
            );
        }
        if (self.keys & THROTTLE_SENDER_DOMAIN) != 0 {
            let sender_domain = e.key(&EnvelopeKey::SenderDomain);
            hasher.update(
                if !sender_domain.is_empty() {
                    sender_domain.as_ref()
                } else {
                    "<>"
                }
                .as_bytes(),
            );
        }
        if (self.keys & THROTTLE_HELO_DOMAIN) != 0 {
            hasher.update(e.key(&EnvelopeKey::HeloDomain).as_bytes());
        }
        if (self.keys & THROTTLE_AUTH_AS) != 0 {
            hasher.update(e.key(&EnvelopeKey::AuthenticatedAs).as_bytes());
        }
        if (self.keys & THROTTLE_LISTENER) != 0 {
            hasher.update(&e.key_as_int(&EnvelopeKey::Listener).to_ne_bytes()[..]);
        }
        if (self.keys & THROTTLE_MX) != 0 {
            hasher.update(e.key(&EnvelopeKey::Mx).as_bytes());
        }
        if (self.keys & THROTTLE_REMOTE_IP) != 0 {
            match &e.key_as_ip(&EnvelopeKey::RemoteIp) {
                IpAddr::V4(ip) => {
                    hasher.update(&ip.octets()[..]);
                }
                IpAddr::V6(ip) => {
                    hasher.update(&ip.octets()[..]);
                }
            }
        }
        if (self.keys & THROTTLE_LOCAL_IP) != 0 {
            match &e.key_as_ip(&EnvelopeKey::LocalIp) {
                IpAddr::V4(ip) => {
                    hasher.update(&ip.octets()[..]);
                }
                IpAddr::V6(ip) => {
                    hasher.update(&ip.octets()[..]);
                }
            }
        }
        if let Some(rate_limit) = &self.rate {
            hasher.update(&rate_limit.period.as_secs().to_ne_bytes()[..]);
            hasher.update(&rate_limit.requests.to_ne_bytes()[..]);
        }
        if let Some(concurrency) = &self.concurrency {
            hasher.update(&concurrency.to_ne_bytes()[..]);
        }

        ThrottleKey {
            hash: hasher.finalize().into(),
        }
    }
}

impl<T: AsyncRead + AsyncWrite> Session<T> {
    pub async fn is_allowed(&mut self) -> bool {
        let throttles = if !self.data.rcpt_to.is_empty() {
            &self.core.session.config.throttle.rcpt_to
        } else if self.data.mail_from.is_some() {
            &self.core.session.config.throttle.mail_from
        } else {
            &self.core.session.config.throttle.connect
        };

        for t in throttles {
            if t.conditions.conditions.is_empty() || t.conditions.eval(self).await {
                if (t.keys & THROTTLE_RCPT_DOMAIN) != 0 {
                    let d = self
                        .data
                        .rcpt_to
                        .last()
                        .map(|r| r.domain.as_str())
                        .unwrap_or_default();

                    if self.data.rcpt_to.iter().filter(|p| p.domain == d).count() > 1 {
                        continue;
                    }
                }

                // Build throttle key
                match self.core.session.throttle.entry(t.new_key(self)) {
                    Entry::Occupied(mut e) => {
                        let limiter = e.get_mut();
                        if let Some(limiter) = &limiter.concurrency {
                            if let Some(inflight) = limiter.is_allowed() {
                                self.in_flight.push(inflight);
                            } else {
                                tracing::debug!(
                                    parent: &self.span,
                                    context = "throttle",
                                    event = "too-many-requests",
                                    max_concurrent = limiter.max_concurrent,
                                    "Too many concurrent requests."
                                );
                                return false;
                            }
                        }
                        if let (Some(limiter), Some(rate)) = (&mut limiter.rate, &t.rate) {
                            if !limiter.is_allowed(rate) {
                                tracing::debug!(
                                    parent: &self.span,
                                    context = "throttle",
                                    event = "rate-limit-exceeded",
                                    max_requests = rate.requests,
                                    max_interval = rate.period.as_secs(),
                                    "Rate limit exceeded."
                                );
                                return false;
                            }
                        }
                    }
                    Entry::Vacant(e) => {
                        let concurrency = t.concurrency.map(|concurrency| {
                            let limiter = ConcurrencyLimiter::new(concurrency);
                            if let Some(inflight) = limiter.is_allowed() {
                                self.in_flight.push(inflight);
                            }
                            limiter
                        });
                        let rate = t.rate.as_ref().map(|rate| {
                            let r = RateLimiter::new(rate);
                            r.is_allowed(rate);
                            r
                        });

                        e.insert(Limiter { rate, concurrency });
                    }
                }
            }
        }

        true
    }

    pub fn throttle_rcpt(&self, rcpt: &str, rate: &Rate, ctx: &str) -> bool {
        let mut hasher = blake3::Hasher::new();
        hasher.update(rcpt.as_bytes());
        hasher.update(ctx.as_bytes());
        hasher.update(&rate.period.as_secs().to_ne_bytes()[..]);
        hasher.update(&rate.requests.to_ne_bytes()[..]);
        let key = ThrottleKey {
            hash: hasher.finalize().into(),
        };

        match self.core.session.throttle.entry(key) {
            Entry::Occupied(mut e) => {
                if let Some(limiter) = &mut e.get_mut().rate {
                    limiter.is_allowed(rate)
                } else {
                    false
                }
            }
            Entry::Vacant(e) => {
                let limiter = RateLimiter::new(rate);
                limiter.is_allowed(rate);
                e.insert(Limiter {
                    rate: limiter.into(),
                    concurrency: None,
                });
                true
            }
        }
    }
}
