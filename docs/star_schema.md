# Star Schema — Gold Layer

## fact_transactions
Grain: one row = one transaction

| Column | Type | Description |
|---|---|---|
| transaction_id | int | PK (trans_id) |
| date_key | int | FK → dim_date (YYYYMMDD) |
| account_id | int | FK → dim_account |
| client_id | int | FK → dim_client (account OWNER, via disp) |
| district_id | int | FK → dim_district (account's district) |
| transaction_type | string | CREDIT / DEBIT (VYBER mapped to DEBIT) |
| operation | string | CASH_DEPOSIT, CASH_WITHDRAWAL, TRANSFER_IN, TRANSFER_OUT, CARD_WITHDRAWAL, INTEREST_CREDIT |
| category | string | INTEREST, STATEMENT_FEE, HOUSEHOLD, PENSION, INSURANCE, LOAN_PAYMENT, PENALTY_INTEREST, NULL |
| amount | decimal | Absolute amount (always ≥ 0) |
| signed_amount | decimal | Negative for debits, positive for credits |
| balance_after | decimal | Account balance after the transaction |
| is_overdraft | boolean | True if balance_after < 0 |
| partner_bank | string | Counterparty bank code (NULL if none) |
| partner_account | string | Counterparty account number (NULL if none) |

## fact_loans
Grain: one row = one loan

| Column | Type | Description |
|---|---|---|
| loan_id | int | PK — unique loan identifier |
| account_id | int | FK → dim_account |
| client_id | int | FK → dim_client (account owner, via disp) |
| district_id | int | FK → dim_district (account's district) |
| date_key | int | FK → dim_date (loan date, YYYYMMDD) |
| loan_amount | decimal | Total amount of the loan |
| duration_months | int | 12, 24, 36, 48 or 60 |
| monthly_payment | decimal | Monthly installment |
| loan_state | string | FINISHED / RUNNING |
| is_default | boolean | True if status B or D |

## dim_client
Grain: one row = one client

| Column | Type | Description |
|---|---|---|
| client_id | int | PK — unique client identifier |
| gender | string | M / F (decoded from birth_number) |
| birth_date | date | Decoded from birth_number |
| age | int | Age at 1998-12-31 (end of dataset) |
| age_group | string | <18, 18-25, 26-35, 36-50, 51-65, 65+ |
| district_id | int | FK → dim_district (where the client lives) |

## dim_account
Grain: one row = one account

| Column | Type | Description |
|---|---|---|
| account_id | int | PK — unique account identifier |
| statement_frequency | string | MONTHLY / WEEKLY / AFTER_TRANSACTION |
| open_date | date | Account opening date |
| district_id | int | FK → dim_district (branch where the account was opened) |
| owner_client_id | int | FK → dim_client (account owner, from disp) |
| is_joint_account | boolean | True if 2 people share the account (from disp) |
| card_type | string | classic / junior / gold / NONE (from card) |
| card_issued_date | date | Card issue date, NULL if no card |

## dim_district
Grain: one row = one district

| Column | Type | Description |
|---|---|---|
| district_id | int | PK (A1) |
| district_name | string | District name (A2) |
| region | string | One of the 8 regions (A3) |
| inhabitants | int | Number of inhabitants (A4) |
| nb_municipalities_lt_500 | int | Municipalities < 500 inhabitants (A5) |
| nb_municipalities_500_1999 | int | Municipalities 500–1,999 inhabitants (A6) |
| nb_municipalities_2000_9999 | int | Municipalities 2,000–9,999 inhabitants (A7) |
| nb_municipalities_gte_10000 | int | Municipalities ≥ 10,000 inhabitants (A8) |
| nb_cities | int | Number of cities (A9) |
| urban_ratio | decimal | % of urban inhabitants (A10) |
| avg_salary | int | Average salary (A11) |
| unemployment_rate_95 | decimal | Unemployment rate 1995 (A12, '?' → NULL) |
| unemployment_rate_96 | decimal | Unemployment rate 1996 (A13) |
| entrepreneurs_per_1000 | int | Entrepreneurs per 1,000 inhabitants (A14) |
| crimes_95 | int | Committed crimes 1995 (A15, '?' → NULL) |
| crimes_96 | int | Committed crimes 1996 (A16) |
| crime_rate_96_per_1000 | decimal | crimes_96 / inhabitants × 1,000 (derived) |

## dim_date
Grain: one row = one calendar day (1993-01-01 → 1998-12-31)

| Column | Type | Description |
|---|---|---|
| date_key | int | PK (YYYYMMDD, e.g. 19960715) |
| full_date | date | Calendar date |
| year | int | e.g. 1996 |
| quarter | string | Q1 / Q2 / Q3 / Q4 |
| month | int | 1–12 |
| month_name | string | January … December |
| year_month | string | e.g. 1996-07 (for monthly trends) |
| day | int | 1–31 |
| day_of_week | string | Monday … Sunday |
| is_weekend | boolean | True on Saturday and Sunday |