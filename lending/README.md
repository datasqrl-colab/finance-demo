# Lending 360

A customer-facing data application that provides a unified view of all lending instruments a customer holds, including mortgages, consumer loans (personal and auto), and credit cards. Customers can see active balances, terms, and a consolidated payment history across all products.

## Features

- **Mortgage**: Active mortgage loans with key terms (rate, balance, payment, maturity)
- **CreditCard**: Active credit card accounts with balance, credit limit, APR, and rewards
- **ConsumerLoan**: Active personal and auto loans in a unified view
- **Customer**: Basic customer profile without sensitive PII (no SSN, TIN, or date of birth)
- **ConsolidatedLoanPayments**: All payments across mortgages, consumer loans, and credit cards in one table

## Data Sources

Data is sourced from the internal data catalog:
- `data-catalog/lending/mortgages` — mortgage originations and servicing
- `data-catalog/lending/cards_consumer_credit` — consumer loans and credit cards
- `data-catalog/customer/customer_data` — customer master records

## Project Structure

```
lending360/
├── lending360.sqrl                  # Main processing logic
├── lending360-test-package.json     # Test configuration
├── lending360-api/
│   ├── schema.v1.graphqls           # GraphQL API schema
│   ├── operations.v1.graphql        # API endpoints (GraphQL, REST, MCP)
│   └── tests/                       # GraphQL test queries
│       ├── customer-lending.graphql
│       └── payment-history.graphql
├── snapshots/lending360/            # Test snapshots (auto-generated)
└── README.md
```

## API Endpoints

| Operation | Description |
|-----------|-------------|
| `GetCustomerLendingOverview` | Full lending overview for a customer including all active instruments |
| `GetCustomerPaymentHistory` | Consolidated payment history across all lending instruments |
| `GetMortgage` | Mortgage loan details by loan_id or customer_id |
| `GetCreditCard` | Credit card account details by account_id or customer_id |
| `GetConsumerLoan` | Consumer loan details by loan_id or customer_id |

## Running Tests

```bash
/opt/agent/cmd.sh test lending360-test-package.json
```

## Design Notes

- CDC streams are deduplicated with `DISTINCT ... ORDER BY source_updated_at DESC` to retain the latest record per entity
- ConsumerLoan unifies personal and auto loans via UNION ALL; `loan_purpose` is null for auto loans (they use `loan_type` for vehicle category instead)
- ConsolidatedLoanPayments joins payment records with their respective loan/account tables to enrich with `customer_id`, which payments do not carry directly
- The Customer table excludes SSN, TIN, and date of birth to avoid exposing sensitive PII
- `instrument_type` values in ConsolidatedLoanPayments: `MORTGAGE`, `PERSONAL_LOAN`, `AUTO_LOAN`, `CREDIT_CARD`
