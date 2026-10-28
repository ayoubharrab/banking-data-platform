# Data Quality Report — Berka Dataset (PKDD'99)

> Exploratory analysis performed locally with Pandas (`notebooks/01_exploration.ipynb`).
> Every issue listed here maps to a transformation rule in the **Silver** layer.

## Overview

| Table | Rows | Primary key | Referential integrity | Main issues |
|---|---:|---|---|---|
| account | 4,500 | `account_id` ✅ | district ✅ | integer `YYMMDD` dates, Czech labels |
| client | 5,369 | `client_id` ✅ | district ✅ | `birth_number` encodes both birth date and gender |
| disp | 5,369 | `disp_id` ✅ | client ✅ account ✅ | — |
| order | 6,471 | `order_id` ✅ | account ✅ | blank `k_symbol` values (spaces), Czech labels |
| loan | 682 | `loan_id` ✅ | account ✅ | `status` coded A/B/C/D |
| card | 892 | `card_id` ✅ | disp ✅ | `issued` stored as text in a different format |
| district | 77 | `A1` ✅ | — | cryptic column names A1–A16, `?` in A12 / A15 |
| trans | 1,056,320 | `trans_id` ✅ | account ✅ | unexpected 3rd `type` value, ~51% empty `k_symbol`, unsigned amounts |

**Time range: 1993-01-01 → 1998-12-31** (bounds of `dim_date`).
No full duplicates and no orphan foreign keys across the whole dataset.

---

## Cross-table issues

| # | Issue | Tables | Silver rule |
|---|---|---|---|
| T1 | Dates stored as integers `YYMMDD` | account, loan, trans | `to_date(cast(date as string), 'yyMMdd')` |
| T2 | Date stored as text `YYMMDD 00:00:00` | card | keep the first 6 characters, then apply T1 |
| T3 | Labels in Czech | account, order, trans | mapping tables → English labels |
| T4 | Disguised empty values (`" "`) not detected by `isna()` | order, trans | `trim()`, then empty string → `NULL` |
| T5 | Partner account numbers stored as numbers | order, trans | cast to `string` (identifier, not a quantity) |

---

## Table-level findings

### account
- `frequency`: `POPLATEK MESICNE` → `MONTHLY` (4,167), `POPLATEK TYDNE` → `WEEKLY` (240), `POPLATEK PO OBRATU` → `AFTER_TRANSACTION` (93).
- Accounts opened between 1993-01-01 and 1997-12-29.

### client
- `birth_number` = `YYMMDD`, with **month + 50 for women** (observed months: 1 → 62).
- Silver: derive `gender` (F: 2,645 / M: 2,724) and `birth_date`, drop `birth_number`.
- Cross-validated: ages are consistent with card types (junior cards = 12–20 years old).

### disp
- 4,500 `OWNER` + 869 `DISPONENT`; every account has **exactly one OWNER**; every client belongs to **one account only**.
- 869 joint accounts (2 people).
- Gold: `owner_client_id` and `is_joint_account` in `dim_account`.

### order
- `k_symbol`: `SIPO` → `HOUSEHOLD`, `UVER` → `LOAN_PAYMENT`, `POJISTNE` → `INSURANCE`, `LEASING`; **1,379 blanks** (spaces) → `NULL`.
- Amounts of 1–2 (mostly insurance / blank category): **kept** and flagged with `is_suspicious_amount`.
- Consistency with `loan`: all 682 loans have a matching `UVER` standing order ✅; 35 `UVER` orders without a loan (most likely loans held at another bank).

### loan
- Cleanest table: at most one loan per account, no loan granted before the account was opened.
- `amount = duration × payments` holds for 100% of rows.
- `status` → `loan_state` + `is_default`:

| status | loan_state | is_default | Count |
|---|---|---|---:|
| A | FINISHED | false | 203 |
| B | FINISHED | true | 31 |
| C | RUNNING | false | 403 |
| D | RUNNING | true | 45 |

- **Overall default rate: 11.1%** (76 / 682).

### card
- `classic` 659, `junior` 145, `gold` 88; issued to OWNERs only, at most one card per account (~20% of accounts).
- No card issued before the account opening date; issued between 1993-11-07 and 1998-12-29.
- Gold: no fact table needed → `card_type` and `card_issued_date` in `dim_account`.

### district
- Renaming: A1 `district_id`, A2 `district_name`, A3 `region`, A4 `inhabitants`, A5–A8 `nb_municipalities_*`, A9 `nb_cities`, A10 `urban_ratio`, A11 `avg_salary`, A12 `unemployment_rate_95`, A13 `unemployment_rate_96`, A14 `entrepreneurs_per_1000`, A15 `crimes_95`, A16 `crimes_96`.
- **A12 and A15 contain `?`** (1 district), so both columns were read as text. Decision: cast to numeric, `?` → `NULL` *(alternative: impute with the 1996 value, documented)*.
- Prague (1.2M inhabitants) is an outlier → Gold: use rates per 1,000 inhabitants instead of raw counts.

### trans
- **`type` has 3 values**: `PRIJEM` (credit, 405,083), `VYDAJ` (debit, 634,571) and **`VYBER` (16,666)**. Verified: 100% of `VYBER` rows are cash withdrawals (`operation = VYBER`) → mapped to `DEBIT`.
- **Null `operation` (183,114) matches the `UROK` interest rows exactly** (verified with a crosstab) → `INTEREST_CREDIT`.
- `operation`: `VYBER` → `CASH_WITHDRAWAL`, `VKLAD` → `CASH_DEPOSIT`, `PREVOD NA UCET` → `TRANSFER_OUT`, `PREVOD Z UCTU` → `TRANSFER_IN`, `VYBER KARTOU` → `CARD_WITHDRAWAL`.
- `k_symbol`: 481,881 NaN + 53,433 spaces (**~51% empty**); `UROK` → `INTEREST`, `SLUZBY` → `STATEMENT_FEE`, `SIPO` → `HOUSEHOLD`, `DUCHOD` → `PENSION`, `POJISTNE` → `INSURANCE`, `UVER` → `LOAN_PAYMENT`, `SANKC. UROK` → `PENALTY_INTEREST`.
- `amount` is always positive → derive **`signed_amount`** (negative for debits).
- 14 zero amounts → kept, quality flag.
- 2,999 negative balances (min −41,126): overdrafts. 264 accounts were charged `SANKC. UROK` penalty interest, 255 of them (97%) went into overdraft → consistent.
- 21,881 rows have a partner account **without a bank code** → source inconsistency, flagged.

---

## Design decisions

1. **No business record is deleted**: suspicious values are kept and flagged with `is_*` columns; filtering happens at analysis time.
2. **Empty values are normalized to `NULL`** (no empty strings in Silver).
3. **All labels are translated to English** through mapping tables versioned with the code.
4. **The checks above will become automated data quality tests** in the Silver notebooks (row counts, key uniqueness, referential integrity).
