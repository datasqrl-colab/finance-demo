# Loan Payments

A customer-facing data application for **personal loans**. It reads the shared finance
[`data-catalog`](../data-catalog) sources `Personal_Loan` and `Loan_Payment`, builds a payment
history and a per-loan payment summary, and serves them through a JWT-authorized GraphQL, REST and
MCP API. Each customer sees only the loans of the `customer_id` in their JWT.

The sub-project is `loan-payments`. The table tests run as a second sub-project,
`loan-payments_table_test`.

## Motivation and Requirements

Personal-loan customers sign in through the bank SSO and need to look up their own loans:

- `LoanPayments`: one row per payment of a personal loan, with the customer and purpose of the
  loan. It keeps payments in every status, so a customer still sees a returned payment.
- `LoanPaymentSummary`: one row per personal loan, with payment counts, payment totals and the last
  payment date. Only `APPLIED` payments contribute to the totals; a loan without payments still
  appears with zero counts and a null last payment date.
- Three lookups, all filtered by the JWT claim `customer_id`: a loan's summary, a loan's payments,
  and every personal loan of the signed-in customer with its summary.

Auto loans, mortgages and credit cards are out of scope, as are writes: the API has no mutation.
The pipeline manages no data of its own — it reuses the catalog connectors and its test data.

## Data Sources

Both sources come from the shared `data-catalog` module, declared under `script.include` in
`loan-payments-shared-package.json` and imported in `loan-payments.sqrl`. The project never copies or
redeclares them.

| Source | Test (`environment: test`) | Production (`environment: prod`) |
|---|---|---|
| `Personal_Loan` | `testdata/consumer_loans-personal_loan.jsonl` | Kafka topic `consumer_loans.personal_loan` |
| `Loan_Payment` | `testdata/consumer_loans-loan_payment.jsonl` | Kafka topic `consumer_loans.loan_payment` |

Both are CDC streams with `ingested_at` watermarks (10 s in test, 30 s in prod). The project uses
the catalog watermarks as they are. The `dev` overlay sets `environment: test`, so a local
development run reads the same filesystem test data as the test suites. The catalog's other consumer-loan tables — `Auto_Loan`,
`Loan_Application` and `Consumer_Loan_Delinquency_Event` — are intentionally out of scope: the
project neither reads nor tests them.

## Architecture and Implementation Choices

| Table | What it does | In the API |
|---|---|---|
| `_LatestPersonalLoan` | latest version of each `Personal_Loan` by `source_updated_at`, key `loan_id`, all statuses | no |
| `_LatestLoanPayment` → `_LatestPersonalLoanPayment` | latest version of each `Loan_Payment` by `source_updated_at`, key `payment_id`, filtered to `loan_type = 'PERSONAL'` | no |
| `LoanPaymentsSource` | personal payments inner-joined to their loan record, enriched with `customer_id` and `loan_purpose` | as the `LoanPayments` lookup |
| `_AppliedPaymentAgg` / `_ReturnedPaymentAgg` | applied and returned payment counts and sums per loan | no |
| `LoanPaymentSummarySource` | every latest personal loan left-joined to both aggregates | as the `LoanPaymentSummary` lookup |
| `CustomerLoansSource` | the summary plus the loan details the client shows | as the `CustomerLoans` lookup |
| `LoanPayments`, `LoanPaymentSummary`, `CustomerLoans` | claim-injected table functions over the sources above | yes |

- **CDC dedup and no windows.** `DISTINCT ... ORDER BY source_updated_at DESC` keeps the latest
  version per key, and the summary is an unwindowed, unbounded re-aggregation. A late or changed
  payment updates the API within the freshness target (about one minute) instead of waiting for a
  window boundary. The Flink state is bounded by key cardinality, not by time: the CDC dedup keeps
  one state entry per loan and per payment, and the sources retain all payments, so state grows with
  the number of loans and payments rather than with the stream length. At the assumed production
  workload (up to 1,000,000 personal loans and 50,000 payments per day) the retained keys are large
  but finite, and the "no expiry" retention requirement means state must grow with the retained
  data rather than with the event rate. Windows would contradict the latest-status requirement,
  which is why none are used. The state tables instead carry a derived event timestamp `_event_time`
  (the greatest `ingested_at` of the rows that produced the state), so they keep valid event-time
  semantics without a window.
- **Loans without payments.** `LoanPaymentSummarySource` is anchored on the latest personal loans
  and left-joins the applied-payment aggregate, so `PL-011` and the other payment-free loans appear
  with zero counts and sums and a null `last_payment_date`. Grouping only the payments would omit
  them.
- **Flink + Postgres + Vert.x, no Kafka engine.** The dedup, enrichment and aggregation run in
  Flink; the three lookups run in Postgres over indexed tables; Vert.x serves the API. There is no
  mutation or subscription, so the `kafka` engine is not enabled — the catalog's Kafka sources are
  ordinary external Flink connectors.
- **Custom GraphQL schema.** `loan-payments-api/schema.v1.graphqls` fixes the exposed type and query
  names, and `loan-payments-api/operations.v1.graphql` adds REST URIs and MCP tools. The claim
  argument never appears as a caller argument.

## API

| Query | Arguments | Returns |
|---|---|---|
| `LoanPaymentSummary(loan_id)` | `loan_id` | one loan's summary, or `null` when the loan is not the caller's |
| `LoanPayments(loan_id, limit, offset)` | `loan_id`, paging | the loan's payments, newest due date first |
| `CustomerLoans(limit, offset)` | paging | every personal loan of the caller, each with its summary |

`customer_id` is **not** an argument. It is read from the JWT and every query filters on it, so a
request for another customer's loan returns no rows. GraphQL, REST and MCP are all exposed. Explicit
operations: `LoanSummaryByLoan`, `LoanPaymentsByLoan`, `MyLoans`.

The API leaves out `loan_number` (`@npi @mask`), the account ids, the credit score and the DTI
ratio. Amounts stay in cents (`BIGINT`).

## Authorization

Every query needs a valid JWT with the top-level claim `customer_id`; a request without a valid
token is rejected (`JWT auth failed` / `Unauthorized`). The claim is mapped to a required function
argument with `METADATA FROM 'auth.customer_id'`.

The auth provider is configured under `engines.vertx.config.auth` in each test, dev and prod
overlay:

- **test**: issuer `loan-payments-test-issuer`, audience `loan-payments-test-audience`, inline HS256
  key from the fixed secret `dummy-super-secret-jwt-signing-string` (base64url without padding:
  `ZHVtbXktc3VwZXItc2VjcmV0LWp3dC1zaWduaW5nLXN0cmluZw`).
- **dev**: issuer `loan-payments-issuer`, audience `loan-payments-api`, inline HS256 key from the
  fixed dev secret `loan-payments-test-secret-signing-string` (base64url without padding:
  `bG9hbi1wYXltZW50cy10ZXN0LXNlY3JldC1zaWduaW5nLXN0cmluZw`).
- **prod**: issuer `${AUTH_ISSUER}`, audience `${AUTH_AUDIENCE}`, key `${JWT_SECRET}` (base64url).

Mint a test token for any customer id:

```bash
jwt encode --secret 'dummy-super-secret-jwt-signing-string' --alg HS256 \
  '{"iss": "loan-payments-test-issuer", "aud": ["loan-payments-test-audience"], "exp": 9999999999, "customer_id": "CUST-060"}'
```

## How FR-8 Is Covered

FR-8 says that when a payment changes status (for example `APPLIED` to `RETURNED`), the summary uses
only the latest status. It is covered **by construction**, not by a snapshot: `_LatestLoanPayment`
deduplicates on `payment_id` by `source_updated_at`, so only one version of each payment reaches
`LoanPaymentsSource`; the unbounded aggregation then reads that single latest version. A changed
payment therefore replaces its contribution to the counts and sums. No catalog payment changes
status in the test data, and the scope forbids adding synthetic source data, so there is no
snapshot for it.

## Data Quality and Observability

This project adds **no** pipeline-local `DQ_` or `OB_` tables:

- The one credible silent-loss point is the inner join from a `PERSONAL` payment to
  `Personal_Loan`: a payment whose loan record never arrives would vanish. The catalog already
  asserts referential integrity (`LoanPaymentReferencesValidPersonalLoan_Test` in
  `data-catalog/lending/lending-ontology.sqrl`), so no capture is warranted.
- The two sources are not independent senders, so ingestion counters add no signal; the catalog's
  root-ontology snapshot queries already surface content changes.
- The auto-loan exclusion is a required filter, not an anomaly, so it is not monitored.

Existing coverage is reused from the catalog's lending-ontology assertions and root-ontology
snapshot queries.

## Tests

The tests read the catalog test data through the catalog test connectors; the project adds no
source data. API tests and table tests live in two packages because each `sqrl test` run uses
one schema. The three public functions (`LoanPaymentSummary`, `LoanPayments`, `CustomerLoans`) are
exercised through the custom schema by the API tests below, so their claim filter and ordering are
covered even though a `/*+test*/` table cannot inject the JWT claim:

| Sub-project | Test package | Schema | Runs |
|---|---|---|---|
| `loan-payments` | `loan-payments-api-test-package.json` | custom (`use-inferred-schema: false`) | the API tests in `loan-payments-api/tests/` |
| `loan-payments_table_test` | `loan-payments-table-test-package.json` | inferred (`use-inferred-schema: true`) | the `/*+test*/` tables in `loan-payments.sqrl` |

API tests (each `X.graphql` with an optional `X.properties` token override):

| Test | Token | Checks |
|---|---|---|
| `payment-returned` | `CUST-439` | `LoanPayments("PL-115")` includes `LPMT-046` with status `RETURNED` and reason `R01` |
| `loan-summary-returned` | `CUST-439` | `PL-115`: `returned_payment_count = 1`, `total_paid_cents = 635360` (the returned 57,760 is excluded) |
| `loan-late-fees` | `CUST-061` | `PL-035`: `late_fees_paid_cents = 7500` |
| `zero-payment-loan` | `CUST-066` | `PL-011`: zero counts and sums, null `last_payment_date` |
| `customer-loans` | `CUST-060` | the four loans `PL-006`, `PL-007`, `PL-022`, `PL-033`, each with its summary |
| `wrong-customer-summary` | `CUST-060` | `LoanPaymentSummary("PL-115")` returns `null` |
| `wrong-customer-payments` | `CUST-060` | `LoanPayments("PL-115")` returns no rows |
| `missing-token` | none | the request is rejected with an authorization error |

Table tests:

| Test | Checks |
|---|---|
| `LoanPaymentsCountTest` | 1,010 personal payment rows over 135 loans |
| `AutoLoanLeakTest` (`no_rows`) | no `AL-` loan id leaked in |
| `DuplicateLoanPaymentsTest` (`no_rows`) | one row per `payment_id` |
| `DuplicateLoanSummaryTest` (`no_rows`) | one row per `loan_id` |
| `ZeroPaymentSummaryTest` | a payment-free loan has zero counts/sums and a null last payment date |
| `CustomerLoansSourceTest` | the four-loan customer `CUST-060` appears with one row per loan and its summary |

Expected results are in `snapshots/loan-payments/` and `snapshots/loan-payments_table_test/`.

## Project Structure

```
loan-payments/
├── loan-payments.sqrl                        # processing, lookups and table tests
├── loan-payments-shared-package.json         # base config: engines, catalog include, API
├── loan-payments-api-test-package.json       # custom-schema API tests
├── loan-payments-table-test-package.json     # inferred-schema table tests
├── loan-payments-dev-package.json            # local development auth (test catalog data)
├── loan-payments-prod-package.json           # production auth
├── loan-payments-api/
│   ├── schema.v1.graphqls                    # GraphQL schema
│   ├── operations.v1.graphql                 # REST/MCP operations
│   └── tests/                                # API test queries and tokens
├── snapshots/
│   ├── loan-payments/                        # API snapshots
│   └── loan-payments_table_test/             # table snapshots
├── run-tests.sh
└── README.md
```

## Commands

The `./run-tests.sh` commands run from the project folder, which the `cd` line enters from the root
of the repository.

```bash
cd loan-payments                             # the project folder, from the repository root
./run-tests.sh                               # run the API tests and the table tests
./run-tests.sh loan-payments                 # run only the API tests
./run-tests.sh loan-payments_table_test      # run only the table tests
./run-tests.sh --compile --env dev loan-payments    # compile the local dev pipeline
./run-tests.sh --compile --env prod loan-payments   # compile the production pipeline
```

The `docker run` commands make the same DataSQRL calls and work from any folder inside the git
repository.

```bash
docker run --rm -v "$(git rev-parse --show-toplevel)":/workspace datasqrl/cmd:0.12.5 \
  compile -r loan-payments loan-payments-shared-package.json loan-payments-dev-package.json -b loan-payments
docker run --rm -v "$(git rev-parse --show-toplevel)":/workspace datasqrl/cmd:0.12.5 \
  compile -r loan-payments loan-payments-shared-package.json loan-payments-prod-package.json -b loan-payments
docker run -it --rm -v "$(git rev-parse --show-toplevel)":/workspace datasqrl/cmd:0.12.5 \
  test -r loan-payments loan-payments-shared-package.json loan-payments-api-test-package.json -b loan-payments
docker run -it --rm -v "$(git rev-parse --show-toplevel)":/workspace datasqrl/cmd:0.12.5 \
  test -r loan-payments loan-payments-shared-package.json loan-payments-table-test-package.json -b loan-payments_table_test
```

`-b <name>` writes the output to `build/<name>`, and it is optional.

The `dev` overlay is the local development environment. It reads the catalog's filesystem test data
(`environment: test`) and signs with a fixed dev issuer, audience and HS256 key, so no production
credentials are needed. Layer it over the base for a local run:

```bash
docker run -it --rm -p 8888:8888 -p 8081:8081 -v "$(git rev-parse --show-toplevel)":/workspace datasqrl/cmd:0.12.5 \
  run -r loan-payments loan-payments-shared-package.json loan-payments-dev-package.json
```

While the pipeline runs: the Flink WebUI is at http://localhost:8081/, the GraphQL API and GraphiQL
at http://localhost:8888/v1/graphiql/, the REST Swagger UI at http://localhost:8888/v1/swagger-ui,
the OpenAPI JSON at http://localhost:8888/v1/openapi, and the MCP API at
`http://localhost:8888/v1/mcp/`. Every request needs a token from the [Authorization](#authorization)
section.

A production deployment needs `AUTH_ISSUER`, `AUTH_AUDIENCE` and `JWT_SECRET`, and the catalog
requires `LENDING_KAFKA_BROKERS`.
