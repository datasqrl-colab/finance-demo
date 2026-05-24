# Customer Enriched — Pipeline Requirements

## 1. Purpose

Produce the two silver tables defined in `customer/customer_data/customer_enriched.sqrl` from bronze sources. These tables are the bank's authoritative "current view of the customer" and the basis for household-level analytics, marketing, risk, and compliance use cases. They feed downstream silver and gold layers (segmentation, CLV, exposure aggregation), and they front-end customer-facing applications that need a single coherent customer record.

This document covers:
- Output contracts for `customer_profile` and `customer_household`.
- Source contracts and required bronze columns.
- Transformation logic and edge cases.
- Pipeline execution semantics: freshness, idempotency, ordering, recovery.
- Data quality, observability, and operational requirements.

It does **not** cover storage layout, the choice of streaming vs. micro-batch engine, or the deployment topology — those are implementation choices that must satisfy the freshness and semantic requirements below.

This pipeline must use the existing `.sqrl` files defined in the `data-catalog/customer/customer_data/`. Those file cannot be modified.

## 2. Output Contracts

### 2.1 customer_profile

**Grain.** One row per active `customer_id`. Customers whose `customer.status` is `CLOSED` for more than the retention window (Section 8) are excluded; closed customers within the window remain with `status = 'closed'` and last-known values frozen.

**Required columns** (illustrative; full tagging per the catalog's Section 7):

| Column | Type | Source / derivation |
|---|---|---|
| `customer_id` | STRING PK | `customer_master.customer.customer_id` |
| `customer_type` | STRING | `customer.customer_type` (person / organization) |
| `legal_name` | STRING | Conformed full name (see 4.1.1) |
| `date_of_birth` | DATE | `customer.date_of_birth` (persons only) |
| `tax_id_token` | STRING | Tokenized reference to SSN/EIN; raw never lands here |
| `primary_email` | STRING | Latest verified email (see 4.1.3) |
| `primary_phone` | STRING | Latest verified mobile phone (see 4.1.3) |
| `residential_address` | STRUCT | Normalized current residential address (see 4.1.2) |
| `mailing_address` | STRUCT | Normalized current mailing address (see 4.1.2) |
| `address_match` | BOOLEAN | True if mailing and residential addresses are equivalent post-normalization |
| `aml_risk_rating` | STRING | Latest effective rating from `kyc_aml.customer_risk_rating` |
| `aml_risk_rated_at` | TIMESTAMP | Effective timestamp of the rating in use |
| `customer_open_date` | DATE | `customer.open_date` |
| `customer_status` | STRING | `customer.status` |
| `tenure_months` | INT | Months between `customer_open_date` and `as_of_date` (closed: to close date) |
| `household_id` | STRING | FK to `customer_household.household_id` (see 4.2) |
| `as_of_date` | DATE | Date the row reflects |
| `last_updated_at` | TIMESTAMP | When the row was last materially changed |

**Stability.** `customer_id` is permanent. `household_id` is stable across runs subject to the household identity rules in 4.2.4.

### 2.2 customer_household

**Grain.** One row per household. A household is a set of customers connected by current shared residential address and/or declared family/joint relationships.

**Required columns:**

| Column | Type | Source / derivation |
|---|---|---|
| `household_id` | STRING PK | Stable identifier (see 4.2.4) |
| `primary_address` | STRUCT | Normalized residential address shared by the household (see 4.2.5) |
| `member_count` | INT | Count of customers in the household |
| `members` | ARRAY<STRUCT> | One element per customer; each element has `customer_id`, `joined_household_at`, `relationship_to_household` (head / spouse / dependent / co-resident / declared-relation) |
| `formed_at` | TIMESTAMP | First time this household_id existed |
| `last_changed_at` | TIMESTAMP | Last membership or address change |

**Stability.** See 4.2.4. Member changes do not change the `household_id` unless the household dissolves to a single member or merges with another household.

## 3. Source Contracts

The pipeline reads from these bronze tables. For each, the listed columns are required; the pipeline must fail closed if any required column is missing or has unexpected nullability.

| Bronze table | Required columns |
|---|---|
| `customer_master.customer` | `customer_id`, `customer_type`, `first_name`, `middle_name`, `last_name`, `legal_name` (where present), `date_of_birth`, `status`, `open_date`, `close_date`, `source_updated_at`, `ingested_at` |
| `customer_master.customer_address` | `customer_id`, `address_type`, `line1`, `line2`, `city`, `state_region`, `postal_code`, `country`, `effective_from`, `effective_to`, `source_updated_at` |
| `customer_master.customer_contact` | `customer_id`, `contact_type` (email / phone), `contact_subtype` (mobile, home, work, primary, secondary), `value`, `verification_status`, `verified_at`, `effective_from`, `effective_to`, `source_updated_at` |
| `customer_master.customer_relationship` | `customer_id_a`, `customer_id_b`, `relationship_type`, `effective_from`, `effective_to`, `source_updated_at` |
| `kyc_aml.customer_risk_rating` | `customer_id`, `rating`, `effective_from`, `effective_to`, `rated_by`, `source_updated_at` |

The pipeline must treat each bronze table as an append-only changelog keyed by its natural key plus `source_updated_at`. Out-of-order arrivals are normal; see Section 6.

## 4. Transformation Logic

### 4.1 customer_profile

#### 4.1.1 Name conformance

- `legal_name` is taken from `customer.legal_name` when present.
- Otherwise it is composed as `TRIM(first_name + ' ' + middle_name + ' ' + last_name)` with collapsed whitespace.
- Names are not case-normalized (preserve source casing); leading/trailing whitespace is stripped.
- For organizations, `customer.legal_name` is the legal entity name as provided.

#### 4.1.2 Current address selection

For each `customer_id` and `address_type IN ('residential', 'mailing')`:

1. Filter to rows where `effective_from <= as_of_date` and (`effective_to IS NULL` or `effective_to > as_of_date`).
2. If multiple rows qualify, choose the one with the latest `effective_from`; ties broken by latest `source_updated_at`.
3. If no row qualifies for `residential`, fall back to the latest historical residential address and flag it (`residential_address.is_stale = true`).
4. If no row qualifies for `mailing`, copy `residential_address` into `mailing_address` and set `address_match = true`.

Address normalization (applied before storage and before `address_match` comparison):
- Trim and uppercase all components.
- Postal code: preserve as-is but right-trim and uppercase (UK / Canada).
- `address_match` is computed on normalized components, excluding `line2` (apartment / unit) — a customer in the same building with different units does not auto-match.

#### 4.1.3 Current contact selection

For each `customer_id`:
- `primary_email`: the latest currently-effective `customer_contact` row with `contact_type = 'email'` and `verification_status = 'verified'`. If none verified, the latest effective unverified row; flag with `primary_email_verified = false` (column added if not already present).
- `primary_phone`: same rule, with `contact_type = 'phone'` and preferring `contact_subtype = 'mobile'`, then `home`, then `work`.
- Effective-window logic identical to 4.1.2.

#### 4.1.4 AML risk rating

For each `customer_id`, select the row from `customer_risk_rating` with `effective_from <= as_of_date` and (`effective_to IS NULL` or `effective_to > as_of_date`). If multiple, latest `effective_from` wins. If none, `aml_risk_rating = 'unrated'`, `aml_risk_rated_at = NULL`.

#### 4.1.5 Tenure

`tenure_months = months_between(coalesce(close_date, as_of_date), open_date)`, floor to whole months, minimum zero.

#### 4.1.6 Customer scope

- Include all customers with `status IN ('active', 'dormant', 'restricted')`.
- Include customers with `status = 'closed'` and `close_date >= as_of_date - <closed_customer_retention>` (default 90 days; see Section 8).
- Customers with `status = 'closed'` outside the window are excluded from the output.
- Customers without a current `customer` row but referenced from address/contact/relationship are not included (orphan records logged as a DQ issue).

### 4.2 customer_household

Household formation runs **before** `customer_profile` materialization in each pipeline cycle so that `customer_profile.household_id` is populated from the current run's household assignments.

#### 4.2.1 Eligible customers and addresses

- Use only currently-effective rows (per 4.1.2 windowing) from `customer_address` and `customer_relationship`.
- Use only `customer_address` rows with `address_type = 'residential'`.
- Use only `customer_relationship` rows where `relationship_type` is in the household-forming set: `spouse`, `domestic_partner`, `parent_of`, `child_of`, `sibling_of`, `joint_account_with`. The set is closed and updates require updating this document.
- Exclude customers with `status = 'closed'`.

#### 4.2.2 Edge construction

Two customers share a household edge if **either** holds:

- **Address edge.** They share a current residential address (normalized per 4.1.2 including `line2`/unit). Building-only matches without unit number do not form edges — `line2` must match (both NULL counts as match for single-unit dwellings, but with the limit in 4.2.3 applied).
- **Relationship edge.** They are connected by a household-forming relationship per 4.2.1.

#### 4.2.3 Building / shared-address sanity limits

To prevent apartment buildings, dorms, group homes, and PO boxes from being collapsed into a single mega-household:

- An address (with `line2`) shared by **more than 8 customers** is treated as a probable multi-occupancy address. Edges through that address are not formed; the address is logged for review.
- An address where `line2 IS NULL` and is shared by **more than 4 customers** is treated similarly.
- PO boxes (detected by `line1` matching documented PO-box patterns) never form address edges.
- Business addresses (residential address attached to an organization customer) never form address edges into person customers.

These thresholds are configuration, not hardcoded; the default values above must be adjustable via pipeline config.

#### 4.2.4 Household identity and stability

Households are the connected components of the edge graph from 4.2.2. The `household_id` assigned to each component must be **stable**: a household with the same membership in two consecutive runs must have the same `household_id`. The recommended scheme:

- On first formation: generate a new `household_id` (UUID).
- On subsequent runs, match this run's components against the previous run's by computing the Jaccard overlap of member sets between each new component and each previous component.
    - If exactly one previous component overlaps a new component by ≥ 50% of the smaller set, the new component inherits its `household_id`.
    - If a new component overlaps two or more previous components each by ≥ 50%, treat as a **merge**: assign the `household_id` of the larger previous component; emit a household-merge event for the others.
    - If a previous component splits into two or more new components each below the 50% threshold, treat as a **split**: the largest new component inherits the previous `household_id`; the others get new IDs.
    - Otherwise, assign a new `household_id`.

The previous run's household snapshot is part of the pipeline state and must be retained at least as long as the longest feasible gap between runs.

A merge or split must produce auditable events (household_id transitions) on a dedicated DQ/lineage stream, since they have downstream impact on aggregations and CLV.

#### 4.2.5 Household primary address

The household's `primary_address` is the residential address shared by the largest subset of household members. Ties broken by the address most recently `effective_from`. For relationship-only households (no shared address), `primary_address = NULL` and a DQ flag is raised so ops can review.

#### 4.2.6 Single-member households

A customer with no household-forming edges to any other customer still receives a household_id (a singleton household). This guarantees every active customer has a non-null `household_id` in `customer_profile`.

## 5. Pipeline Semantics

### Ordering

For a given `customer_id`, the order of materialization must respect the source `source_updated_at` ordering. The pipeline must not regress a customer's row to an earlier state because of out-of-order arrivals.

## 6. Data Quality

Capture those as dedicated tables that can be observed.

### 6.1 Input checks (run continuously on bronze)

- **Effective-window sanity.** `effective_from <= effective_to` where both are present.
- **Freshness check.** `max(source_updated_at)` per bronze table is within the source system's expected lag; staleness exceeding configured thresholds triggers an alert.

### 6.2 Output checks (run after each materialization)

| Check | Threshold (default; tunable) |
|---|---|
| Every active customer has exactly one `customer_profile` row | 100% |
| Every `customer_profile.household_id` resolves to a `customer_household` row | 100% |
| Households are non-empty (`member_count >= 1`) | 100% |
| Each customer appears in exactly one household | 100% |
| Households formed only by relationship (no shared address) | < 5% of households (alert above) |
| Household merges in a single run | < 0.1% of households (alert above) |
| Household splits in a single run | < 0.1% of households (alert above) |
| Customers with `primary_email_verified = false` | < 30% (warn; not block) |
| Customers with `aml_risk_rating = 'unrated'` and `status = 'active'` | < 0.5% (alert; compliance-sensitive) |
| Row-count delta vs. previous run | within ±2% (warn above; not block) |

Threshold breaches at "alert" level page on-call; "warn" level emits a metric only.

### 6.3 Reconciliation

- `count(distinct customer_id)` in `customer_profile` vs. `count(active + within-window-closed customers)` in `customer_master.customer`.
- `sum(member_count)` across `customer_household` vs. row count of `customer_profile`.


## 7. Lineage, Observability, and Audit

- Each output row carries `last_updated_at`; this is the watermark for downstream consumers.
- The pipeline emits per-batch lineage records to a lineage topic: bronze input ranges (by `source_updated_at`), output row counts, DQ check results, and household merge/split events.
- All transformations are version-controlled; the running pipeline version is reported in lineage metadata.
- A change to the household-forming relationship set, the building thresholds in 4.2.3, or the address normalization map is a **definitional change** that requires a full reprocess and a release note.

## 8. Compliance and PII Handling

Classification, regulatory scope, and column tags are inherited from the catalog conventions; this section covers what the pipeline must enforce at runtime.

- **No raw `ssn` or other `@spii` value lands in `customer_profile` or anywhere in this pipeline's intermediate state.** The pipeline reads only the tokenized form (`tax_id_token`) from bronze. If bronze still carries the raw value alongside the token, the pipeline must drop the raw column in the first stage; any code path that materializes the raw value is a defect.
- **Right-to-erasure.** When a customer's record is erased upstream (GDPR Art. 17 / CCPA delete), the pipeline must propagate the erasure: the customer's row in `customer_profile` is removed (or tombstoned per platform policy) and the customer is removed from their household. If removing them dissolves the household to a singleton, the household is updated accordingly. Erasure must be processed within **24 hours** of the upstream signal.
- **`closed_customer_retention`** (Section 4.1.6) is set to 90 days by default. Closed customers' rows must be removed from `customer_profile` after this window unless held under legal hold.
- **Audit log.** Every erasure and every household merge/split is logged with timestamp, triggering event, and affected `customer_id`s. The log is itself classified `Restricted` and retained per the bank's records policy.
- **Test data isolation.** The pipeline must never read non-production bronze into the production silver tables, and vice versa. Environment is part of the dataset identity.

## 9. Configuration Surface

These parameters must be externally configurable in the package.json under `source.config`:

| Parameter | Default |
|---|---|
| `closed_customer_retention_days` | 90 |
| `orphan_grace_window_hours` | 24 |
| `household_forming_relationships` | `[spouse, domestic_partner, parent_of, child_of, sibling_of, joint_account_with]` |
| `address_threshold_with_unit` | 8 |
| `address_threshold_without_unit` | 4 |
| `household_jaccard_match_threshold` | 0.50 |
| `freshness_profile_target_p95_minutes` | 5 |
| `freshness_household_target_p95_minutes` | 30 |
| DQ check thresholds (Section 6.2) | as listed |

Changes to `household_forming_relationships`, the address thresholds, or the Jaccard threshold are definitional changes requiring full reprocessing (Section 7).

## 11. Deliverable Checklist

- [ ] `customer_enriched.sqrl` defines `customer_profile` and `customer_household` with the columns in Section 2 and the catalog's required `/** */` and `--` tagging.
- [ ] Pipeline reads only the bronze columns listed in Section 3 and fails closed on contract drift.
- [ ] All transformation rules in Section 4 implemented and unit-tested with documented test fixtures, including each edge case (no current address, multi-occupancy threshold, merge, split).
- [ ] Freshness, idempotency, ordering, and late-data handling per Section 5 verified with integration tests.
- [ ] Input and output DQ checks per Section 6 are running and wired to alerting.
- [ ] Lineage events per Section 7 are emitted and queryable.
- [ ] Erasure propagation tested end-to-end within the 24-hour SLA.
- [ ] Configuration surface in Section 9 exposed via the platform's config mechanism, no hardcoded values.
