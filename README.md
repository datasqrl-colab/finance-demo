# Finance Demo

Real-time streaming data products for financial services built with DataSQRL.

## Data Products

### [Customer Enrichment Pipeline](customer/README.md)

Produces silver-layer customer data from bronze-layer CDC streams. Consolidates identity, contact, compliance (KYC/AML/PEP), and relationship data into deduplicated `Customer_Profile` and `Customer_Household` tables with household grouping and data quality monitoring.

### [Lending 360](lending/README.md)

Customer-facing unified view of all lending instruments — mortgages, consumer loans (personal and auto), and credit cards. Provides active balances, terms, and consolidated payment history across all products.