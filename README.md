<h2 align="center">
    <a href="https://stalw.art">
    <img src="https://stalw.art/home/apple-touch-icon.png" height="60">
    </a>
    <br>
    Stalwart Mail Server
</h1>

<p align="center">
  <i align="center">Secure & Modern All-in-One Mail Server (IMAP, JMAP, SMTP)</i> 🛡️
</p>

<h4 align="center">
  <a href="https://github.com/stalwartlabs/mail-server/actions/workflows/build.yml">
    <img src="https://img.shields.io/github/actions/workflow/status/stalwartlabs/mail-server/build.yml?style=flat-square" alt="continuous integration">
  </a>
  <a href="https://www.gnu.org/licenses/agpl-3.0">
    <img src="https://img.shields.io/badge/License-AGPL_v3-blue.svg?label=license&style=flat-square" alt="License: AGPL v3">
  </a>
  <a href="https://stalw.art/docs/get-started/">
    <img src="https://img.shields.io/badge/read_the-docs-red?style=flat-square" alt="Documentation">
  </a>
  <br>
  <a href="https://mastodon.social/@stalwartlabs">
    <img src="https://img.shields.io/mastodon/follow/109929667531941122?style=flat-square&logo=mastodon&color=%236364ff" alt="Mastodon">
  </a>
  <a href="https://twitter.com/stalwartlabs">
    <img src="https://img.shields.io/twitter/follow/stalwartlabs?style=flat-square&logo=twitter" alt="Twitter">
  </a>
  <br>
  <a href="https://discord.gg/jtgtCNj66U">
    <img src="https://img.shields.io/discord/923615863037390889?label=discord&style=flat-square" alt="Discord">
  </a>
  <a href="https://matrix.to/#/#stalwart:matrix.org">
    <img src="https://img.shields.io/matrix/stalwartmail%3Amatrix.org?label=matrix&style=flat-square" alt="Matrix">
  </a>
</h4>


**Stalwart Mail Server** is an open-source mail server solution with JMAP, IMAP4, and SMTP support and a wide range of modern features. It is written in Rust and designed to be secure, fast, robust and scalable.

Key features:

- **JMAP** server:
  - JMAP Core ([RFC 8620](https://datatracker.ietf.org/doc/html/rfc8620))
  - JMAP Mail ([RFC 8621](https://datatracker.ietf.org/doc/html/rfc8621))
  - JMAP for Sieve Scripts ([DRAFT-SIEVE-15](https://www.ietf.org/archive/id/draft-ietf-jmap-sieve-15.html))
  - JMAP over WebSocket ([RFC 8887](https://datatracker.ietf.org/doc/html/rfc8887)), JMAP Blob Management ([RFC9404](https://www.rfc-editor.org/rfc/rfc9404.html)) and JMAP for Quotas ([RFC9425](https://www.rfc-editor.org/rfc/rfc9425.html)) extensions.
- **IMAP4** server:
  - IMAP4rev2 ([RFC 9051](https://datatracker.ietf.org/doc/html/rfc9051)) full compliance.
  - IMAP4rev1 ([RFC 3501](https://datatracker.ietf.org/doc/html/rfc3501)) backwards compatible.
  - ManageSieve ([RFC 5804](https://datatracker.ietf.org/doc/html/rfc5804)) server.
  - Numerous [extensions](https://stalw.art/docs/development/rfcs#imap4-and-extensions) supported.
- **SMTP** server:
  - Built-in [DMARC](https://datatracker.ietf.org/doc/html/rfc7489), [DKIM](https://datatracker.ietf.org/doc/html/rfc6376), [SPF](https://datatracker.ietf.org/doc/html/rfc7208) and [ARC](https://datatracker.ietf.org/doc/html/rfc8617) support for message authentication.
  - Strong transport security through [DANE](https://datatracker.ietf.org/doc/html/rfc6698), [MTA-STS](https://datatracker.ietf.org/doc/html/rfc8461) and [SMTP TLS](https://datatracker.ietf.org/doc/html/rfc8460) reporting.
  - Inbound throttling and filtering with granular configuration rules, sieve scripting and milter integration.
  - Virtual queues with delayed delivery, priority delivery, quotas, routing rules and throttling support.
  - Envelope rewriting and message modification.
- **Spam and Phishing** filter:
  - Comprehensive set of filtering **rules** on par with popular solutions.
  - Statistical **spam classifier** with automatic training capabilities.
  - DNS Blocklists (**DNSBLs**) checking of IP addresses, domains, and hashes.
  - Collaborative digest-based spam filtering with **Pyzor**.
  - **Phishing** protection against homographic URL attacks, sender spoofing and other techniques.
  - Trusted **reply** tracking to recognize and prioritize genuine e-mail replies.
  - Sender **reputation** monitoring by IP address, ASN, domain and email address.
  - **Greylisting** to temporarily defer unknown senders.
  - **Spam traps** to set up decoy email addresses that catch and analyze spam.
- **Flexible and scalable**:
  - Pluggable storage backends with **RocksDB**, **FoundationDB**, **PostgreSQL**, **mySQL**, **SQLite**, **S3-Compatible**, **Redis** and **ElasticSearch** support.
  - Built-in, **LDAP** or **SQL** authentication backend support.
  - Full-text search available in 17 languages.
  - Disk quotas.
  - Sieve scripting language with support for all [registered extensions](https://www.iana.org/assignments/sieve-extensions/sieve-extensions.xhtml).
  - Email aliases, mailing lists, subaddressing and catch-all addresses support.
  - Integration with **OpenTelemetry** to enable monitoring, tracing, and performance analysis.
- **Secure and robust**:
  - Encryption at rest with **S/MIME** or **OpenPGP**.
  - Automatic TLS certificate provisioning with [ACME](https://datatracker.ietf.org/doc/html/rfc8555).
  - OAuth 2.0 [authorization code](https://www.rfc-editor.org/rfc/rfc8628) and [device authorization](https://www.rfc-editor.org/rfc/rfc8628) flows.
  - Automated blocking of hosts that cause multiple authentication errors (aka **fail2ban**).
  - Access Control Lists (ACLs).
  - Rate limiting.
  - Security audited (read the [report](https://stalw.art/blog/security-audit)).
  - Memory safe (thanks to Rust).

## Get Started

Install Stalwart Mail Server on your server by following the instructions for your platform:

- [Linux / MacOS](https://stalw.art/docs/install/linux)
- [Windows](https://stalw.art/docs/install/windows)
- [Docker](https://stalw.art/docs/install/docker)

All documentation is available at [stalw.art/docs/get-started](https://stalw.art/docs/get-started).

## Support

If you are having problems running Stalwart Mail Server, you found a bug or just have a question,
do not hesitate to reach us on [Github Discussions](https://github.com/stalwartlabs/mail-server/discussions),
[Reddit](https://www.reddit.com/r/stalwartlabs), [Discord](https://discord.gg/aVQr3jF8jd) or [Matrix](https://matrix.to/#/#stalwart:matrix.org).
Additionally you may become a sponsor to obtain priority support from Stalwart Labs Ltd.

## Roadmap

- [x] Performance enhancements
- [ ] Web-based admin panel
- [ ] JMAP Calendar, Contacts and Tasks support
- [ ] Brand Indicators for Message Identification (BIMI) support
- [ ] Distributed SMTP queues

See the [open issues](https://github.com/stalwartlabs/mail-server/issues) for a full list of proposed features (and known issues).

## Funding

Part of the development of this project was funded through the [NGI0 Entrust Fund](https://nlnet.nl/entrust), a fund established by [NLnet](https://nlnet.nl/) with financial support from the European Commission's [Next Generation Internet](https://ngi.eu/) programme, under the aegis of DG Communications Networks, Content and Technology under grant agreement No 101069594.

If you find the project useful you can help by [becoming a sponsor](https://github.com/sponsors/stalwartlabs). Thank you!

## License

Licensed under the terms of the [GNU Affero General Public License](https://www.gnu.org/licenses/agpl-3.0.en.html) as published by
the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
See [LICENSE](LICENSE) for more details.

You can be released from the requirements of the AGPLv3 license by purchasing
a commercial license. Please contact licensing@stalw.art for more details.
  
## Copyright

Copyright (C) 2024, Stalwart Labs Ltd.
