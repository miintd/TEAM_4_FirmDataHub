# FirmDataHub - ETL Pipeline for Firm Financial Data Management

**A comprehensive Python ETL (Extract, Transform, Load) solution for importing, validating, and exporting firm-level financial panel data from Excel to MySQL.**

## 📋 Quick Overview

- **Data Source:** Excel files with firm information and 37-variable financial panel data
- **Database:** MySQL with snapshot versioning system
- **Processing:** Automated Python-based data pipeline with quality controls
- **Output:** Clean, validated panel data in CSV format
- **Versioning:** Multi-snapshot support for data version control (1-20 snapshots per batch)

---

## 📁 Project Structure

```
TEAM_4_FirmDataHub/
├── README.md                    # Project documentation (English)
├── requirements.txt             # Python dependencies (pandas, mysql-connector-python)
│
├── data/                        # Input data directory
│   ├── team_ticker.csv         # Reference ticker list
│   └── (Excel files to import)
│
├── etl/                         # ETL processing scripts
│   ├── db_config.py            # MySQL connection settings  
│   ├── create_snapshot.py      # Create data snapshots for versioning (snapshot_id 1-20)
│   ├── import_firms.py         # Import firm dimension table
│   ├── import_panel.py         # Import 37-variable panel data with snapshot_id
│   ├── qc_checks.py            # Data quality validation (6 check types) - 222 lines
│   ├── export_panel.py         # Export clean panel using MAX(snapshot_id)
│   └── __pycache__/            # Python cache (auto-generated)
│
└── outputs/                     # Generated output files
    ├── panel_latest.csv        # Exported 38-variable panel dataset
    └── qc_report.csv           # Quality control validation report
```

---

## 🚀 Getting Started

### Prerequisites

- **Python** 3.8+
- **MySQL Server** 5.7+ with database `firm_data_hub`
- **Dependencies:** pandas, mysql-connector-python

### Installation

1. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure MySQL connection** (`etl/db_config.py`):
   ```python
   DB_CONFIG = {
       'host': 'localhost',
       'database': 'firm_data_hub',
       'port': '3306',
       'user': 'root',
       'password': 'your_password'  # ← Update with your credentials
   }
   ```

3. **Verify MySQL database tables exist:**
   - Dimension: `dim_firm`, `dim_data_source`
   - Fact tables: `fact_ownership_year`, `fact_market_year`, `fact_financial_year`, `fact_cashflow_year`, `fact_innovation_year`, `fact_firm_year_meta`
   - Snapshot: `fact_data_snapshot`

4. **Prepare input data** - Place Excel files in `data/` folder

---

## 📊 ETL Scripts Reference

### 1️⃣ `db_config.py` — Database Configuration

**Purpose:** Central MySQL connection configuration

```python
DB_CONFIG = {
    'host': 'localhost',           # MySQL server hostname
    'database': 'firm_data_hub',   # Database name
    'port': '3306',                # MySQL port
    'user': 'root',                # MySQL user
    'password': 'your_password'    # MySQL password ← UPDATE THIS
}
```

**Action Required:** Update credentials before running any ETL scripts.

---

### 2️⃣ `create_snapshot.py` — Create Data Snapshots

**Purpose:** Create dated snapshot records for data version control and tracking

**Features:**
- Creates records in `fact_data_snapshot` table
- Auto-increments `snapshot_id` (1, 2, 3, ...)
- Records timestamp, description, and statistics
- Batch mode: Create 20 snapshots (4 sources × 5 fiscal years)

**Usage — Single snapshot:**
```bash
python etl/create_snapshot.py --description "Data batch 1"
```

**Usage — Batch mode (4 sources × 5 years):**
```bash
python etl/create_snapshot.py --batch-default 2020 2024
# Creates 20 snapshots (snapshot_id: 1-20)
# For: 4 data sources × fiscal years 2020-2024
```

**Output:**
- Record in `fact_data_snapshot` with auto-incremented `snapshot_id`

---

### 3️⃣ `import_firms.py` — Import Firm Dimension

**Purpose:** Import firm master data into `dim_firm` dimension table

**Features:**
- Reads firm information from Excel
- Validates ticker format and uniqueness
- Maps industry and country classifications
- Handles duplicates gracefully

**Usage:**
```bash
python etl/import_firms.py data/firms.xlsx
```

**Input:** Excel with columns:
- ticker, firm_name, industry, country, ...

**Output:** Populated `dim_firm` dimension table

---

### 4️⃣ `import_panel.py` — Import 37-Variable Panel Data

**Purpose:** Import financial panel data into 6 fact tables with snapshot versioning

**Features:**
- Reads 37-variable panel data from Excel
- Validates `snapshot_id` exists in `fact_data_snapshot`
- Distributes data into 6 fact tables:
  - `fact_ownership_year` (4 variables)
  - `fact_market_year` (4 variables)
  - `fact_financial_year` (23 variables)
  - `fact_cashflow_year` (3 variables)
  - `fact_innovation_year` (2 variables)
  - `fact_firm_year_meta` (2 variables)
- Supports selective module import and multiple snapshot ranges
- Snapshot_id links each record to its data version

**Usage — Single snapshot:**
```bash
python etl/import_panel.py data/panel.xlsx --snapshots 1
```

**Usage — Multiple modules:**
```bash
python etl/import_panel.py data/panel.xlsx --modules financial,ownership --snapshots 1-5
```

**Usage — Multiple snapshot ranges:**
```bash
# Import snapshots 1-5 and 6-10
python etl/import_panel.py data/panel.xlsx --snapshots 1-5,6-10
```

**Supported modules:**
- `financial` - Financial statement data (net_sales, total_assets, net_income, ...)
- `ownership` - Ownership structure (managerial, state, institutional, foreign ownership)
- `market` - Market data (shares outstanding, market cap, dividend, EPS)
- `cashflow` - Cash flow statement (CFO, CFI, CAPEX)
- `innovation` - Innovation indicators (product/process innovation: 0 or 1)
- `meta` - Metadata (employees_count, firm_age)

**Input format:**
```
ticker | fiscal_year | snapshot_id | variable_name | value
AAPL   | 2024        | 1           | net_sales     | 383285
```

**Output:**
- 6 fact tables populated with data linked to `snapshot_id`

---

### 5️⃣ `qc_checks.py` — Data Quality Validation

**Purpose:** Comprehensive data quality checks using `MAX(snapshot_id)` per firm-year

**Quality Checks (6 types):**

| Check | Field(s) | Rule | Output |
|-------|---------|------|--------|
| **Missing Values** | Critical fields in all tables | NOT NULL | missing_value |
| **Ownership Ratios** | managerial/state/institutional/foreign_own | ∈ [0, 1] | out_of_range |
| **Shares Outstanding** | shares_outstanding | > 0 | invalid_value |
| **Total Assets** | total_assets | ≥ 0 | negative_value |
| **Current Liabilities** | current_liabilities | ≥ 0 | negative_value |
| **Growth Ratios** | growth_ratio | ∈ [-0.95, 5.0] | extreme_growth |

**Key Features:**
- Uses `MAX(snapshot_id)` per firm_id + fiscal_year (latest version selection)
- Optimized code: 222 lines (57% reduction from original)
- Helper methods to eliminate SQL duplication
- Detailed CSV error report with error summaries

**Usage:**
```bash
# Validate all data (uses MAX snapshot_id per firm-year)
python etl/qc_checks.py
```

**Output:** `outputs/qc_report.csv`
```
ticker | fiscal_year | field_name          | error_type     | message
AAPL   | 2024        | total_assets        | negative_value | total_assets=-1000 should be >= 0
MSFT   | 2023        | shares_outstanding | invalid_value  | shares_outstanding=0 should be > 0
```

**Report Summary:**
- Total error count
- Breakdown by error_type
- Lists affected firms and time ranges

---

### 6️⃣ `export_panel.py` — Export Clean Panel Data

**Purpose:** Export validated 37-variable panel dataset using latest snapshot versions

**Features:**
- Selects `MAX(snapshot_id)` per firm_id + fiscal_year for each fact table
- Performs LEFT JOINs across all 6 fact tables
- Preserves all 37 variables structured by module
- Handles missing values gracefully (NULLs from unmatched LEFT JOINs)

**Usage:**
```bash
python etl/export_panel.py
# Output: outputs/panel_latest.csv
```

**Output:** `outputs/panel_latest.csv`

**Structure (38 variables):**
```csv
ticker,fiscal_year,managerial_inside_own,state_own,institutional_own,foreign_own,...
AAPL,2024,0.15,0.20,0.30,0.35,...
MSFT,2024,0.10,0.15,0.40,0.35,...
```

---

## 🔄 Complete ETL Workflow

### Recommended Execution Order

```
STEP 1: Setup & Configuration
├─ Update db_config.py with MySQL credentials
├─ Verify MySQL database exists with required tables
└─ Install dependencies: pip install -r requirements.txt

STEP 2: Initialize Snapshots (version management)
└─ python etl/create_snapshot.py --batch-default 2020 2024
   → Creates 20 snapshots (snapshot_id: 1-20)
   → For: 4 data sources × fiscal years 2020-2024

STEP 3: Import Master Firm Data
└─ python etl/import_firms.py data/firms.xlsx
   → Populates dim_firm table
   → Reference for all panel data imports

STEP 4: Import Panel Data
└─ python etl/import_panel.py data/panel.xlsx --snapshots 1-20
   → Loads 37 variables across 6 fact tables
   → Each record linked to snapshot_id

STEP 5: Validate Data Quality
└─ python etl/qc_checks.py
   → Generates outputs/qc_report.csv
   → Uses MAX(snapshot_id) per firm-year
   → Reports 6 types of validation errors

STEP 6: Export Clean Data
└─ python etl/export_panel.py
   → Generates outputs/panel_latest.csv
   → 37 variables with latest snapshot selection
   → Ready for analysis
```

### Example: Complete Batch Import

```bash
# 1. Create 20 snapshots for batch
python etl/create_snapshot.py --batch-default 2020 2024

# 2. Import firms
python etl/import_firms.py data/firms.xlsx

# 3. Import snapshots 1-10 (4 sources × 2 years)
python etl/import_panel.py data/panel_batch1.xlsx --snapshots 1-10

# 4. Validate imported data
python etl/qc_checks.py

# 5. Export clean panel
python etl/export_panel.py

# Check results
cat outputs/qc_report.csv | head -20
ls -lh outputs/panel_latest.csv
```

---

## 📊 Database Schema

### Dimension Tables

**`dim_firm`** — Company master data
```sql
firm_id (PK) | ticker | firm_name | industry | country | ...
```

**`dim_data_source`** — Data source reference
```sql
source_id (PK) | source_name | description | ...
```

### Fact Tables (each row linked to snapshot_id)

**Template structure:**
```sql
fact_id (PK) | firm_id (FK) | fiscal_year | snapshot_id (FK) | ... variables ...
```

**`fact_ownership_year`** — 4 variables
- managerial_inside_own, state_own, institutional_own, foreign_own

**`fact_market_year`** — 4 variables
- shares_outstanding, market_value_equity, dividend_cash_paid, eps_basic

**`fact_financial_year`** — 23 variables
- Revenue: net_sales
- Assets: total_assets, current_assets, inventory, cash_and_equivalents, intangible_assets_net, net_ppe
- Expenses: selling_expenses, general_admin_expenses, manufacturing_overhead, raw_material_consumption, merchandise_purchase_year, wip_goods_purchase, outside_manufacturing_expenses, production_cost, rnd_expenses
- Profitability: net_income, net_operating_income
- Structure: total_equity, total_liabilities, long_term_debt, current_liabilities
- Growth: growth_ratio

**`fact_cashflow_year`** — 3 variables
- net_cfo (Operating Cash Flow)
- net_cfi (Investment Cash Flow)
- capex (Capital Expenditure)

**`fact_innovation_year`** — 2 variables
- product_innovation (0 or 1)
- process_innovation (0 or 1)

**`fact_firm_year_meta`** — 2 variables
- employees_count
- firm_age

### Snapshot Table

**`fact_data_snapshot`** — Version control and audit trail
```sql
snapshot_id (PK) | source_id (FK) | fiscal_year | created_at | description | record_count
```

---

## 37-Variable Panel Data Overview

| Module | Variables | Count |
|--------|-----------|-------|
| **Ownership** | managerial_inside_own, state_own, institutional_own, foreign_own | 4 |
| **Market** | shares_outstanding, market_value_equity, dividend_cash_paid, eps_basic | 4 |
| **Financial** | net_sales, total_assets, selling_expenses, general_admin_expenses, intangible_assets_net, manufacturing_overhead, net_operating_income, raw_material_consumption, merchandise_purchase_year, wip_goods_purchase, outside_manufacturing_expenses, production_cost, rnd_expenses, net_income, total_equity, total_liabilities, cash_and_equivalents, long_term_debt, current_assets, current_liabilities, growth_ratio, inventory, net_ppe | 23 |
| **Cashflow** | net_cfo, net_cfi, capex | 3 |
| **Innovation** | product_innovation, process_innovation | 2 |
| **Metadata** | employees_count, firm_age | 2 |
| **TOTAL** | | **37** |

---

## 🔧 Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| MySQL connection error | Wrong credentials or server offline | Update `db_config.py`, verify MySQL is running |
| "Snapshot ID not found" | snapshot_id doesn't exist | Run `create_snapshot.py` first with matching ID |
| "Duplicate entry" error | Record already exists | Check for existing data, update or skip |
| QC errors in report | Data validation failures | Review `qc_report.csv`, verify source data |
| Missing columns in export | Schema mismatch | Verify fact table columns match expected variable names |
| "No rows" in output | Mismatched firm_id or fiscal_year | Check Excel data matches dim_firm records |

---

## ✨ Key Features & Architecture

✅ **Multi-version data management** — Up to 20 concurrent snapshots (snapshot_id 1-20)  
✅ **Automated ETL pipeline** — Complete workflow in Python scripts  
✅ **Data quality validation** — 6 comprehensive check types using MAX(snapshot_id)  
✅ **Selective import** — Choose specific modules and snapshot ranges  
✅ **Code optimization** — 222-line QC module (57% reduction) with helper methods  
✅ **Flexible export** — 37-variable panel with latest snapshot selection per firm-year  
✅ **Error tracking** — Detailed CSV reports for all validation failures  
✅ **Version control** — Snapshot-based approach allows data lineage tracking  

---

## 📝 Notes

- **Snapshot Workflow:** Each import operation assigns a `snapshot_id`, enabling multiple data versions and audit trails
- **Latest Data Selection:** Queries use `MAX(snapshot_id)` per firm_id + fiscal_year to automatically select newest version
- **Data Integrity:** All fact table records linked to `fact_data_snapshot` ensuring data traceability
- **Batch Operations:** Supports `snapshot_id` ranges (e.g., `1-5,6-10`) for efficient multi-version imports

---

## 📄 File Dependencies

```
db_config.py
    ↓
create_snapshot.py → fact_data_snapshot
    ↓
import_firms.py → dim_firm
    ↓
import_panel.py → 6 fact tables (all link to fact_data_snapshot)
    ↓
qc_checks.py → outputs/qc_report.csv
    ↓
export_panel.py → outputs/panel_latest.csv
```

---

**Last Updated:** March 2026  
**Project:** TEAM_4_FirmDataHub  
**Version:** 1.0

