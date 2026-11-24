# Loan Payments

A customer-facing, JWT-restricted data product that shows a retail-bank customer the payments
on their **personal** loans and a per-loan payment summary. The pipeline reads personal loans and
loan payments from the shared [`data-catalog`](../data-catalog) consumer-lending tables, joins them,
aggregates the payments per loan, and serves the result over GraphQL, REST and MCP.

## Motivation and requirements

Customers need self-service visibility into their personal-loan payments without contacting the
bank. The SSO issues a JWT, and the API must return only the loans that belong to the customer in
the token. The requirements:

- Read the personal loans (`consumer_loans.Personal_Loan`) and payments (`consumer_loans.Loan_Payment`)
  from the shared catalog through its environment-specific connectors.
- Deduplicate both CDC streams on their key (`loan_id`, `payment_id`), keeping the latest
  `source_updated_at` version. Deduplication is the reason this project is a **streaming** pipeline:
  a changed payment status and a new payment both update the view within the 1-minute freshness
  target.
- Keep only payments of personal loans (the inner join drops auto loans and any payment whose loan
  record is missing).
- Produce `LoanPayments` (one row per payment) and `LoanPaymentSummary` (one row per personal loan,
  including loans with no payments, counting and summing `APPLIED` payments only).
- Require a valid JWT on every request and filter every lookup by the token's `customer_id` claim.
  A lookup of another customer's loan returns no data.

Auto loans, mortgages, credit cards, loan applications, delinquency events and all writes are out
of scope. The shared catalog is read-only; this project never modifies it.

## Architecture

```
data-catalog (read-only, included via script.include)
  Personal_Loan (CDC stream) ──DISTINCT on loan_id──────────────┐
  Loan_Payment  (CDC stream) ──DISTINCT on payment_id──┐        │
                                                       │        │
                              inner join on loan_id ───┴────────┴──► LoanPayments
                                                                        │
                     per-loan aggregate from the deduplicated payments   │
                     (applied/returned counts, applied sums, last date) │
                                          │                              │
             _LatestPersonalLoan LEFT JOIN _LoanPaymentAgg ──► LoanPaymentSummary
```

Engines: **Flink** processes the streaming deduplication, join and unbounded per-loan aggregate;
**Postgres** stores the served tables; **Vert.x** serves the API. Kafka is not enabled because the
project has no mutations or subscriptions — the production catalog sources are plain `kafka-safe`
connector tables.

The custom GraphQL schema (`loan-payments-api/schema.v1.graphqls`) is authoritative. `LoanPayments`
and `LoanPaymentSummary` carry the `no_query` hint, so they have no direct query endpoint: every
lookup goes through a table function that reads `customer_id` from the `auth.customer_id` claim.
There is therefore no unfiltered endpoint over customer data.

## API

Every request requires a valid `Authorization: Bearer <jwt>` header. The server rejects a request
without a token (HTTP 401). All customer queries filter on `customer_id = auth.customer_id`.

| Query | Arguments | REST operation |
|---|---|---|
| `LoanPaymentSummaryByLoanId` | `loan_id` | `GET /v1/rest/loans/{loan_id}/payment-summary` |
| `LoanPaymentsByLoanId` | `loan_id`, `limit`, `offset` | `GET /v1/rest/loans/{loan_id}/payments` |
| `CustomerLoanSummaries` | `limit`, `offset` | `GET /v1/rest/customer/loans` |
| `DQ_Dropped_Payment` (operations, admin only) | `loan_type` | `GET /v1/rest/data-quality/dropped-payments` |
| `OB_SourceIngestionStats` (operations, admin only) | `source_name` | `GET /v1/rest/observability/source-ingestion` |

`CustomerLoanSummaries` embeds the loan's payments as the `payments` relationship. The API exposes
only the fields the customer needs: `customer_id` and `loan_purpose` from the loan record, and the
payment fields. Loan number, credit score, DTI and account ids stay out of the API.

MCP tools are generated for the same queries (`GetLoanPaymentSummary`, `GetLoanPayments`,
`GetCustomerLoans`, `GetDroppedPayments`, `GetSourceIngestionStats`).

The data-quality and observability queries are **admin only**. They read the `is_admin` claim
from the token and return no rows unless it is `true`; a customer token cannot see them.

### Test tokens

The `test` and `dev` configs use an inline HS256 key with issuer `loan-payments-issuer` and audience
`loan-payments-api`. Generate a token for any customer with:

```bash
jwt encode --secret 'loan-payments-test-secret-signing-string' --alg HS256 \
  '{"iss":"loan-payments-issuer","aud":["loan-payments-api"],"exp":9999999999,"customer_id":"CUST-439"}'
```

For the admin-only data-quality and observability queries, mint a token that also carries
`"is_admin":true`:

```bash
jwt encode --secret 'loan-payments-test-secret-signing-string' --alg HS256 \
  '{"iss":"loan-payments-issuer","aud":["loan-payments-api"],"exp":9999999999,"customer_id":"CUST-001","is_admin":true}'
```

The `prod` config reads the issuer, audience and JWKS URI from `AUTH_ISSUER`, `AUTH_AUDIENCE` and
`AUTH_JWKS_URI`.

## Data quality and observability

- **`DQ_Dropped_Payment`** — payments dropped by the personal-loan inner join, counted by source
  `loan_type`. `AUTO` rows are expected (auto loans are out of scope). A `PERSONAL` row, or a rising
  count, means the personal-loan feed is missing or late and real personal payments were dropped.
  Admin only: the query reads the `is_admin` claim and is empty for a non-admin token.
- **`OB_SourceIngestionStats`** — monthly record counts per consumer-lending source stream. A
  missing month or a lower count can indicate a stalled source feed.
  Admin only: the query reads the `is_admin` claim and is empty for a non-admin token.

The catalog's own column guarantees (NOT NULL keys, amount types, the payment-status enum) are
treated as upstream coverage and are not re-checked here. `LoanPayments` and `LoanPaymentSummary`
are covered by the catalog test-time data; per-customer masking is enforced by the JWT filter and
the API schema.

## Catalog dependency

This project consumes the shared `data-catalog` module read-only. It is declared under
`script.include` in `loan-payments-shared-package.json` under the namespace `data_catalog`, and the
main script imports the environment-specific connector file:

```sqrl
IMPORT data_catalog.lending.cards_consumer_credit.consumer_loans-{{environment}} AS cons_loans;
```

The catalog is never copied or modified. Its test data is used directly as the test fixture.

## Project structure

```
loan-payments/
├── loan_payments.sqrl                          # Pipeline + API functions
├── loan-payments-shared-package.json           # Base config (engines, include, API)
├── loan-payments-api-test-package.json         # API test config (custom schema)
├── loan-payments-table-test-package.json       # Table test config (inferred schema)
├── loan-payments-dev-package.json              # Local dev config
├── loan-payments-prod-package.json             # Production config (auth from env)
├── loan-payments-api/
│   ├── schema.v1.graphqls                      # Custom GraphQL schema
│   ├── operations.v1.graphql                   # REST/MCP operations
│   └── tests/                                  # API tests + tokens
├── snapshots/
│   ├── loan_payments_api/
│   └── loan_payments_table/
└── run-tests.sh
```

## Commands

The `./run-tests.sh` commands run from the project folder. The `docker run` commands work from any
folder inside the git repository and mount the repository root so the sibling `../data-catalog`
module is available.

### Run the tests

```bash
cd loan-payments
./run-tests.sh                                       # every sub-project, test environment
./run-tests.sh loan_payments                         # only the API test suite
./run-tests.sh loan_payments_table_test              # only the table test suite
./run-tests.sh --compile --env dev loan_payments     # compile a deployed environment
```

### Compile, test and run with Docker

```bash
docker run --rm -v "$(git rev-parse --show-toplevel)":/workspace datasqrl/cmd:0.12.5 \
  compile -r loan-payments loan-payments-shared-package.json loan-payments-dev-package.json -b loan_payments

docker run -it --rm -v "$(git rev-parse --show-toplevel)":/workspace datasqrl/cmd:0.12.5 \
  test -r loan-payments loan-payments-shared-package.json loan-payments-api-test-package.json -b loan_payments

docker run -it --rm -p 8888:8888 -p 8081:8081 -v "$(git rev-parse --show-toplevel)":/workspace datasqrl/cmd:0.12.5 \
  run -r loan-payments loan-payments-shared-package.json loan-payments-dev-package.json
```

`-b <name>` writes the build output to `build/<name>` and is optional; keep it so the two test
suites and the dev compile do not overwrite each other's artifacts.

A local `run` exposes:

- GraphQL / GraphiQL: <http://localhost:8888/v1/graphiql/>
- REST Swagger UI: <http://localhost:8888/v1/swagger-ui>
- OpenAPI JSON: <http://localhost:8888/v1/openapi>
- MCP: <http://localhost:8888/v1/mcp/>
- Flink WebUI: <http://localhost:8081/>

## Tests

- `loan-payments-api-test-package.json` runs the API tests against the **custom** schema
  (`use-inferred-schema: false`) using per-test JWT tokens in the `.properties` files.
- `loan-payments-table-test-package.json` runs the `/*+test*/` table tests against the inferred
  schema. It carries the checks that must not be in the customer API: the total `LoanPayments` row
  count, the absence of auto-loan rows, and the data-quality and observability values. The latter
  two read the hidden aggregation tables directly, because the public queries are admin only and a
  table test has no auth context.

The API tests cover the acceptance criteria: the PL-115 summary and its 12 payments for CUST-439,
the two CUST-383 loans (including the zero-payment summary) with nested payments, a cross-customer
denial, a request without a token, late fees for PL-035, the LPMT-002 payment, the admin-only data
quality and observability tables (with an admin token), and their denial for a non-admin token. The CDC deduplication itself has no snapshot because the catalog test data
contains no repeated keys.
