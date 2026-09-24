---
title: Survey of sensitive metadata and external secret storage
summary: Inventory of persistently stored keys, tokens, and certificates in Ozone and guidance on HashiCorp Vault vs Hadoop KMS
date: 2026-09-24
status: draft
author: Wei-Chiu Chuang
---
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Abstract

This document surveys **persistently stored** cryptographic and credential material
in Apache Ozone (as of the current `master` codebase) and evaluates which assets
are good candidates for **remote secret stores** (for example HashiCorp Vault) versus
**Hadoop KMS with Apache Ranger** versus **local protection only** (permissions,
disk encryption, backup policy).

It complements [THREAT_MODEL.md](../../../../THREAT_MODEL.md) (operator responsibilities
for SCM CA keys and service metadata at rest), [symmetric-token-signatures.md](symmetric-token-signatures.md)
(HDDS-7733), [secure-s3.md](secure-s3.md) (HDDS-8132), [ozone-sts.md](ozone-sts.md),
and [tde.md](tde.md).

This is a **survey**, not an enhancement proposal. It does not change product behavior.

**See also:** [Secure persistence of sensitive metadata](secure-sensitive-metadata-persistence.md)
— design proposal for pluggable local, Vault, and Ranger KMS backends.

# Scope

**In scope**

* Secrets, keys, tokens, and private key material written to **disk** (files, RocksDB,
  Ratis storage, checkpoints) or to an **external store** already integrated with Ozone.
* Recommendations for Vault-style secret management vs Ranger KMS vs at-rest encryption.

**Out of scope**

* Ephemeral bearer tokens held only in client memory or verified without durable OM rows.
* Kerberos KDC, Ranger policy content, network perimeter, and SCM CA compromise scenarios
  already disclaimed in the threat model.
* Non-secure mode (`ozone.security.enabled=false`).

**Path conventions**

File-based material uses `hdds.metadata.dirs` or `ozone.metadata.dirs` as `{metadata}`:

* Component keys: `{metadata}/{component}/keys/` (default `private.pem`, `public.pem`).
* Component certs: `{metadata}/{component}/certs/` (default `certificate.crt`).
* SCM symmetric signing keys: `{metadata}/scm/keys/secret_keys.json` (configurable via
  `hdds.secret.key.file.name`).

Component names include `om`, `scm`, `dn` (datanode), and `recon`.

# 1. Persistent inventory by service

## 1.1 Storage Container Manager (SCM)

| Asset | Persistence | Sensitivity | Code / config anchors |
| --- | --- | --- | --- |
| Token-signing symmetric keys | `{metadata}/scm/keys/secret_keys.json` (JSON, Base64-encoded key bytes) | **Critical** | `LocalSecretKeyStore`, `SecretKeyManagerService` |
| SCM root / sub-CA private key | PEM under `{metadata}/scm/…` (`hdds.x509.rootca.private.key.file`, `scm/ca` layout) | **Critical** | `SecurityConfig`, `SCMCertificateClient` |
| SCM service private key + cert | `{metadata}/scm/keys/`, `{metadata}/scm/certs/` | **High** | PKI client for SCM |
| Issued certificate registry | RocksDB `ozone.scm.db.dirs`: `validCerts`, `validSCMCerts` (**public** X.509 only) | Medium | `SCMDBDefinition` |
| SCM HA Ratis | `ozone.scm.ha.ratis.storage.dir` (+ snapshots) | **High** | Replicates secret-key state; each node still writes `secret_keys.json` |

OM and datanodes **fetch** signing keys from SCM over RPC and cache them **in memory**;
they do not persist `secret_keys.json`.

## 1.2 Ozone Manager (OM)

### RocksDB (`ozone.om.db.dirs`)

| Column family | Content | Sensitivity |
| --- | --- | --- |
| `s3SecretTable` | access key id → `S3SecretValue` (**plaintext** `awsSecret`) | **Critical** (default store) |
| `dTokenTable` | `OzoneTokenIdentifier` → renew time | **High** (identifier + renewal metadata) |
| `s3RevokedStsTokenTable` | original access key id → revocation cutoff (ms) | Low (policy metadata) |
| `keyTable`, `fileTable`, `openKeyTable`, … | `OmKeyInfo` may include `FileEncryptionInfo` (**EDEK**, IV) | **High** when TDE enabled |
| `bucketTable` | `BucketEncryptionKeyInfo` (KMS key **name**, suite) | Medium (reference only) |
| `tenantAccessIdTable` | tenant / principal / admin flags (**no** secret) | Low |

See `OMDBDefinition`.

### OM HA Ratis (`ozone.om.ratis.storage.dir`)

Ratis logs and snapshots hold applied transaction payloads. **`UpdateAssumeRoleRequest`**
(sts HA replication) includes leader-generated **`secretAccessKey`** and **`sessionToken`**
even though AssumeRole does **not** write active STS credentials to RocksDB
(`S3AssumeRoleResponse` is stateless). Treat OM Ratis storage as **sensitive**.

### Checkpoints and snapshots

OM DB checkpoints (HA bootstrap, snapshot feature) are full RocksDB copies at a point
in time, including `s3SecretTable`, `dTokenTable`, and EDEKs when present.

### OM PKI

`{metadata}/om/keys/private.pem` and `{metadata}/om/certs/certificate.crt`
(`OMCertificateClient`).

## 1.3 Datanode

| Asset | Persistence | Sensitivity |
| --- | --- | --- |
| DN private key + cert | `{metadata}/dn/keys/`, `{metadata}/dn/certs/` | **High** |
| User data blocks | Datanode volumes | TDE-dependent (DEKs from KMS at read/write time) |

No durable copy of SCM signing keys or S3 secrets on datanodes in the normal design.

## 1.4 Recon

| Asset | Persistence | Sensitivity |
| --- | --- | --- |
| OM snapshot DB | `ozone.recon.om.snapshot.db.dir` / recon OM DB | **Same classes as OM** when local S3 store is used |
| SCM snapshot DB | under `ozone.recon.db.dir` | Medium (mostly public certs + container metadata) |
| Recon PKI | `{metadata}/recon/keys/`, `{metadata}/recon/certs/` | **High** |

Recon tasks treat `s3SecretTable` and `dTokenTable` as security-relevant OM tables.

## 1.5 S3 Gateway

* **S3 user secrets:** not stored locally; OM resolves via `s3SecretTable` or Vault.
* **TLS:** keystore/truststore files and passwords in config when HTTPS is enabled.

## 1.6 External: HashiCorp Vault (optional S3 backend)

When `ozone.secret.s3.store.provider` is `VaultS3SecretStorageProvider`, long-lived S3
secrets persist in Vault KV paths (`VaultS3SecretStore`, keys under
`ozone.secret.s3.store.remote.vault.*`). OM `s3SecretTable` is bypassed for those operations.

## 1.7 External: Hadoop KMS / Ranger (TDE)

Ozone does **not** store KMS master keys. OM persists **wrapped** data keys (EDEKs) and
encryption key **names**; KEKs live in KMS. OM uses `hadoop.security.key.provider.path`
(`OzoneManager` / `OzoneKMSUtil`).

# 2. What is *not* persistently stored in OM

Understanding boundaries avoids misplaced vault effort:

| Item | Behavior |
| --- | --- |
| Active STS session tokens | Returned to clients; validation is cryptographic. Revocation cutoffs only in `s3RevokedStsTokenTable`. |
| Block / container tokens | Minted on demand; verified with SCM symmetric keys; no per-token OM table. |
| S3 SigV4 (non-STS) | Per-request; secret resolved from `s3SecretTable` or Vault. |
| SCM signing keys on OM/DN | In-memory cache after RPC fetch. |

# 3. Remote Vault vs Ranger KMS vs local protection

## 3.1 Strong candidates for a remote vault

| Asset | Rationale | Practical pattern |
| --- | --- | --- |
| **S3 long-lived secrets** | Many static credentials; admin-readable RocksDB today; audit and rotation | **Implemented:** `VaultS3SecretStore` (see [secure-s3.md](secure-s3.md)) |
| **SCM CA private key** | Cluster root of trust; low churn; HSM/PKI fits enterprise policy | Vault PKI, external CA, or offline root; avoid plaintext PEM on shared disks |
| **SCM `secret_keys.json`** | Forges delegation, block/container, and STS tokens if leaked | Vault as **source of truth** for generation/rotation; **envelope-encrypt** local file with Transit; working keys in **RAM** on SCM (not per-request Vault calls) |
| **TLS / integration secrets** | Keystore passwords, Vault AppRole secrets often live in `ozone-site.xml` | Vault Agent, env injection, short-lived certs |

**Rule:** Vault backs **birth, rotation, and wrapping**. Hot-path token signing and
verification stay on SCM/OM/DN using keys already in memory.

## 3.2 Envelope encryption (Vault Transit or cloud KMS), not KV per object

| Surface | Recommendation |
| --- | --- |
| OM / SCM / Recon **RocksDB** directories | Volume or filesystem encryption; restrict permissions; protect backups |
| OM / SCM **Ratis** directories | Encrypted volumes; treat as sensitive because of STS replication payloads |
| OM **checkpoints** and Recon **OM snapshots** | Encrypted backup policy; reducing `s3SecretTable` use (Vault) shrinks secret duplication |

Moving entire RocksDB or Ratis logs into Vault KV is the wrong abstraction.

## 3.3 Ranger KMS / Hadoop KMS (not Vault KV)

| Asset | Tool |
| --- | --- |
| Bucket encryption key names in OM | Ranger policies on KMS |
| EDEKs in `OmKeyInfo` | KMS wrap/unwrap (`KeyProvider`) |
| KEKs | KMS backend (HSM/cloud); optional Vault as KMS backend in some deployments |

Ranger governs **who may use which encryption key** for **data at rest**; it does not
replace SCM signing keys or S3 SigV4 secrets.

## 3.4 Poor return on vault-specific integration

| Asset | Why |
| --- | --- |
| `s3RevokedStsTokenTable`, `tenantAccessIdTable` | Metadata, not secrets |
| `dTokenTable` rows | Identifiers + renew times; forgery requires SCM signing keys |
| SCM `validCerts` / public `certificate.crt` | Public material |
| Ephemeral block/container/STS bearer tokens | Not OM durable state |
| Per-row Vault storage for AssumeRole fields in Ratis | Fix replication or encrypt Ratis; do not mirror each AssumeRole into KV |

# 4. Suggested operator priority (persistent surface)

1. SCM **CA private key** and **`secret_keys.json`** on every SCM node.
2. OM **`s3SecretTable`** (or migrate to **Vault** provider).
3. OM **RocksDB**, **Ratis**, **checkpoints**, and Recon **OM snapshot** (aggregate exposure).
4. **KMS/Ranger** for TDE production deployments.
5. Per-service **`private.pem`** under `{metadata}/{component}/keys/`.
6. **Config-file secrets** (Vault auth, SSL passwords).

# 5. Gaps and possible follow-up work

These are not commitments; they are logical extensions suggested by the survey:

* Pluggable `SecretKeyStore` beyond `LocalSecretKeyStore` (Vault envelope or HSM).
* Encrypted or redacted **OM Ratis** payloads for STS `UpdateAssumeRoleRequest`.
* Documented production checklist tying THREAT_MODEL §10 to concrete paths and config keys.
* Recon hardening options (snapshot scope, access isolation) when OM still uses local S3 secrets.

# 6. References

| Topic | Location |
| --- | --- |
| OM DB layout | `hadoop-ozone/ozone-manager/.../OMDBDefinition.java` |
| SCM secret key file | `hadoop-hdds/framework/.../LocalSecretKeyStore.java` |
| Vault S3 store | `hadoop-ozone/s3-secret-store/.../VaultS3SecretStore.java` |
| S3 store config | `ozone.secret.s3.store.provider`, `ozone.secret.s3.store.remote.vault.*` |
| STS stateless AssumeRole | `S3AssumeRoleResponse`, `UpdateAssumeRoleRequest` in `OmClientProtocol.proto` |
| Threat model (operator) | `THREAT_MODEL.md` §3, §5a, §10 |
