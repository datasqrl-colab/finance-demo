# Finance Demo

Real-time streaming data products for financial services built with DataSQRL.

Both data products consume the shared [`data-catalog`](data-catalog) — a git submodule at the repository root — via `script.include` in their shared package files. Clone with submodules:

```bash
git clone --recurse-submodules <repo-url>
# or, in an existing checkout:
git submodule update --init
```

## Data Products

### [Customer Enrichment Pipeline](customer/README.md)

Produces silver-layer customer data from bronze-layer CDC streams. Consolidates identity, contact, compliance (KYC/AML/PEP), and relationship data into deduplicated `Customer_Profile` and `Customer_Household` tables with household grouping and data quality monitoring.

### [Lending 360](lending/README.md)

Customer-facing unified view of all lending instruments — mortgages, consumer loans (personal and auto), and credit cards. Provides active balances, terms, and consolidated payment history across all products.