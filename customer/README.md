# Customer Enrichment Pipeline

A real-time streaming pipeline that produces silver-layer customer data products from bronze-layer source streams. It consolidates customer identity, contact, compliance, and relationship data into queryable `Customer_Profile` and `Customer_Household` tables exposed via GraphQL, REST, and MCP APIs.

## Motivation and Requirements

Banks and financial institutions maintain customer data across multiple authoritative systems — a core customer master (CDC stream), KYC verification records, AML risk ratings, and PEP screening results. These systems use append-only or CDC event streams, meaning the same customer can appear multiple times as records are updated.

Downstream consumers (analytics, marketing, relationship management, compliance reporting) need:

- **A single, deduplicated customer view** — one row per customer with the latest identity, contact, and address information.
- **Compliance enrichment** — KYC verification status, AML risk rating, and PEP flag attached to each customer record.
- **Household groupings** — customers linked via SPOUSE, JOINT_OWNER, or DEPENDENT relationships grouped into households for relationship-level analytics.
- **Data quality signals** — observable indicators of source data contract violations (invalid address windows, active customers with no AML rating).
- **Ingestion observability** — per-source record counts and source timestamp ranges for pipeline monitoring.

Regulatory scope: **GLBA, GDPR, CCPA** (customer PII) and **BSA, USA PATRIOT Act, OFAC** (KYC/AML data).

## Architecture and Implementation

### Data Sources (Bronze Layer)

| Source | Description | Delivery |
|--------|-------------|----------|
| `customer_master` | Core identity, addresses, contacts, relationships | CDC append stream |
| `kyc_aml` | KYC verifications, AML risk ratings, PEP screenings | CDC append stream |

Both sources are parameterized by environment (`test` uses filesystem files, `prod` reads from Apache Iceberg tables via `${CUSTOMER_DATA_ICEBERG_WAREHOUSE}`).

Source definitions come from the shared [`data-catalog`](../data-catalog) submodule at the repository root, wired in via `script.include` in `customer_enriched_pipeline-shared-package.json`.

### Outputs (Silver Layer)

| Table | Grain | Sink |
|-------|-------|------|
| `Customer_Profile` | One row per active/inactive customer | Iceberg (upsert by `customer_id`) |
| `Customer_Household` | One row per customer household assignment | Iceberg (upsert by `household_id, customer_id`) |

### Key Processing Decisions

**Deduplication** — All source streams use `DISTINCT ON ... ORDER BY source_updated_at DESC` (latest-record-wins). SCD Type 2 tables (risk ratings) use the `is_current` flag before deduplication to select the active row. The output tables `Customer_Profile` and `Customer_Household` are likewise `DISTINCT`-keyed by `customer_id`, which gives the upsert sinks an explicit upsert key (required by DataSQRL 0.11+) and lets a later record for the same customer overwrite any earlier transient row.

**Contact resolution** — Rather than picking any contact record, priority rules select the single best contact: verified email before unverified; phone ranked MOBILE > HOME_PHONE > WORK_PHONE. This ensures a deterministic, highest-quality contact per customer.

**Age and tenure computation in Flink** — `TIMESTAMPDIFF` and date cast expressions cannot be translated to Postgres SQL. An intermediate `_CustomerComputed` table (annotated `/*+engine(flink)*/`) pre-computes all time-based fields before the result is written to the serving layer, keeping `Customer_Profile` free of untranslatable expressions.

**Household formation** — Only SPOUSE, JOINT_OWNER, and DEPENDENT relationships form multi-member households. Relationship pairs are normalized (smaller `customer_id` always becomes `household_id`) to eliminate duplicate bidirectional entries. Oldest effective date wins for household ID stability. Customers without qualifying relationships become singleton households (`household_id = customer_id`).

**Data quality tables** — `DQ_Address_Window_Issues` and `DQ_Unrated_Active_Customers` are exposed as queryable API endpoints. Non-empty results indicate source data contract violations requiring remediation.

### Pipeline Engines

| Engine | Role |
|--------|------|
| Apache Flink | Streaming computation (dedup, enrichment, aggregation) |
| Apache Iceberg | Silver-layer sink storage |
| DuckDB | Interactive query serving |
| Vert.x | API server (GraphQL / REST / MCP) |

## API

All three protocols are enabled. The API exposes:

- `CustomerProfile` — query by `customer_id`
- `CustomerHousehold` — query by `household_id` or `customer_id`
- `SourceIngestionStats` — query by `source_name`
- `DQAddressWindowIssues` — data quality: address effective window violations
- `DQUnratedActiveCustomers` — data quality: active customers missing AML rating

| Protocol | URL |
|----------|-----|
| GraphiQL | http://localhost:8888/v1/graphiql/ |
| REST Swagger | http://localhost:8888/v1/swagger-ui |
| MCP | http://localhost:8888/v1/mcp/ |

## Commands

Run all commands from the **repository root**: the whole repository is mounted so the shared `data-catalog` submodule is visible, and `-r customer` selects this project.

### Compile

Validate the pipeline compiles without errors:

```bash
# Test configuration
docker run -it --rm -p 8888:8888 -p 8081:8081 -v $PWD:/workspace datasqrl/cmd \
  compile customer_enriched_pipeline-shared-package.json customer_enriched_pipeline-test-package.json -r customer

# Production configuration
docker run -it --rm -p 8888:8888 -p 8081:8081 -v $PWD:/workspace datasqrl/cmd \
  compile customer_enriched_pipeline-shared-package.json customer_enriched_pipeline-prod-package.json -r customer
```

### Test

Run all tests and validate snapshots:

```bash
docker run -it --rm -p 8888:8888 -p 8081:8081 -v $PWD:/workspace datasqrl/cmd \
  test customer_enriched_pipeline-shared-package.json customer_enriched_pipeline-test-package.json -r customer
```

Test snapshots are stored in `snapshots/customer_enriched_pipeline/`. The test suite covers:

- `CustomerProfileTest` — verifies identity, contact, status, and compliance fields per customer
- `CustomerHouseholdTest` — verifies household assignments, roles, and match method
- `SourceIngestionStatsTests` — verifies hourly windowed ingestion statistics

### Run

Start the full pipeline locally (Flink + Iceberg + DuckDB + API server):

```bash
docker run -it --rm -p 8888:8888 -p 8081:8081 -v $PWD:/workspace datasqrl/cmd \
  run customer_enriched_pipeline-shared-package.json customer_enriched_pipeline-test-package.json -r customer
```

- Flink WebUI: http://localhost:8081/
- GraphiQL: http://localhost:8888/v1/graphiql/
- REST Swagger: http://localhost:8888/v1/swagger-ui
- MCP endpoint: http://localhost:8888/v1/mcp/

## Production Configuration

The prod package (`customer_enriched_pipeline-prod-package.json`) reads from and writes to Apache Iceberg tables. Set the environment variable before running:

```bash
export CUSTOMER_DATA_ICEBERG_WAREHOUSE=s3://your-bucket/warehouse
```

Iceberg catalog name: `customer`, database: `customer_data`, tables: `customer_profile`, `customer_household`.

## Not Yet Implemented

- **Customer segmentation** — `Customer_Segment` schema is defined in the data catalog but segmentation logic is not yet implemented in the pipeline.
- **Customer Lifetime Value (CLV)** — `Customer_Lifetime_Value` schema is defined but CLV computation is pending downstream revenue data integration.
- **Digital enrollment and preferred channel** — `is_digital_enrolled` and `preferred_channel` fields are stubbed to `FALSE`/`''` pending digital banking system integration.
- **Household income and balance aggregation** — `household_income_band`, `household_balance_cents`, and `household_product_count` are `NULL` pending cross-product enrichment.
