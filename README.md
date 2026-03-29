# FirmDataHub - ETL Pipeline

**A comprehensive Python ETL (Extract, Transform, Load) solution for importing, validating, and exporting firm-level financial panel data from Excel to MySQL.**

## 📋 Quick Overview

- **Data Source:** Excel files with firm information and 38-variable financial panel data
- **Database:** MySQL with snapshot versioning system
- **Processing:** Automated Python-based data pipeline with quality controls
- **Output:** Clean, validated panel data in CSV format
- **Versioning:** Multi-snapshot support for data version control

---

## 📁 Project Structure

```
TEAM_4_FirmDataHub/
├── README.md                    # Project documentation 
├── requirements.txt             # Python dependencies 
│
├── data/                        # Input data directory
│   ├── firms.xlsx              # List of firms
│   ├── panel_2020_2024.xlsx    # 38-variable financial panel data
│   └── team_ticker.csv         # Reference ticker list
│
├── sql/                         # Database schema and initialization
│   └── schema_and_seed.sql     # MySQL schema definitions and seed data
│
├── etl/                         # ETL processing scripts
│   ├── db_config.py            # MySQL connection settings  
│   ├── create_snapshot.py      # Create data snapshots for versioning
│   ├── import_firms.py         # Import firm dimension table
│   ├── import_panel.py         # Import 38-variable panel data
│   ├── qc_checks.py            # Data quality validation 
│   └── export_panel.py         # Export clean panel 
│
└── outputs/                     # Generated output files
    ├── panel_latest.csv        # Exported 38-variable panel dataset
    └── qc_report.csv           # Quality control validation report
```

---

## 🚀 Getting Started

### Prerequisites

- **Python** 3.8+
- **MySQL Server** 5.7+ with database `vn_firm_panel`
- **Dependencies:** pandas, mysql-connector-python

### Installation

1. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Initialize MySQL database:**
   ```bash
   # Create database schema and load seed data (REQUIRED)
   mysql -u root -p < sql/schema_and_seed.sql
   ```
   This creates all required tables:
   - Dimension: `dim_firm`, `dim_data_source`, `dim_exchange`, `dim_industry_l2`
   - Fact tables: `fact_ownership_year`, `fact_market_year`, `fact_financial_year`, `fact_cashflow_year`, `fact_innovation_year`, `fact_firm_year_meta`
   - Snapshot: `fact_data_snapshot`

3. **Configure MySQL connection** (`etl/db_config.py`):
   ```python
   DB_CONFIG = {
       'host': 'localhost',
       'database': 'vn_firm_panel',
       'port': '3306',
       'user': 'root',
       'password': 'your_password'  # ← Update with your credentials
   }
   ```

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

### 2️⃣ `import_firms.py` — Import Firm Dimension

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
- ticker, company_name, Exchange, Industry

**Output:** Populated `dim_firm` dimension table

---

### 3️⃣ `create_snapshot.py` — Create Data Snapshots

**Purpose:** Create dated snapshot records for data version control and tracking

**Input:**
- `source_name` (required) - Data source identifier (e.g., 'ACB', 'TPB', 'Bloomberg', 'Internal')
- `fiscal_year` (required) - Fiscal year (e.g., 2020, 2021, 2024)
- `snapshot_date` (optional) - Date snapshot was created (YYYY-MM-DD, defaults to today)
- `version_tag` (optional) - Version identifier (e.g., 'v1', 'v2', 'final')

**Features:**
- Creates records in `fact_data_snapshot` table
- Auto-increments `snapshot_id` (1, 2, 3, ...)
- Each snapshot uniquely characterized by source_name + fiscal_year combination
- Records timestamp, description, and statistics

**Usage — Single snapshot:**
```bash
python etl/create_snapshot.py <source_name> <fiscal_year> [snapshot_date] [version_tag]
```

**Usage — Multiple snapshots:**
```bash
python etl/create_snapshot.py --batch-default 2020 2024
# Creates 20 snapshots for: 4 data sources × fiscal years 2020-2024
```

**Output:**
- Record in `fact_data_snapshot` with auto-incremented `snapshot_id`

---

### 4️⃣ `import_panel.py` — Import 38-Variable Panel Data

**Purpose:** Import financial panel data into 6 fact tables with snapshot versioning

**Features:**
- Reads 38-variable panel data from Excel
- Validates `snapshot_id` exists in `fact_data_snapshot`
- Distributes data into 6 fact tables:
  - `fact_ownership_year` (4 variables)
  - `fact_market_year` (4 variables)
  - `fact_financial_year` (23 variables)
  - `fact_cashflow_year` (3 variables)
  - `fact_innovation_year` (2 variables)
  - `fact_firm_year_meta` (2 variables)
- Each data row linked to correct snapshot_id via source_name + fiscal_year

**Usage — Single snapshot:**
```bash
python etl/import_panel.py data/panel.xlsx --snapshots 1
```

**Usage — Multiple modules:**
```bash
# Multiple modules with snapshots
python etl/import_panel.py data/panel.xlsx --modules financial,ownership --snapshots 1-5
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
- Helper methods to eliminate SQL duplication
- Detailed CSV error report with error summaries

**Usage:**
```bash
python etl/qc_checks.py
```

**Output:** `outputs/qc_report.csv`
```
ticker | fiscal_year | field_name          | error_type     | message
AAPL   | 2024        | total_assets        | negative_value | total_assets=-1000 should be >= 0
MSFT   | 2023        | shares_outstanding | invalid_value  | shares_outstanding=0 should be > 0
```
---

### 6️⃣ `export_panel.py` — Export Clean Panel Data

**Purpose:** Export validated 38-variable panel dataset using latest snapshot versions

**Features:**
- Selects `MAX(snapshot_id)` per firm_id + fiscal_year for each fact table
- Performs LEFT JOINs across all 6 fact tables
- Preserves all 38 variables structured by module
- Handles missing values gracefully (NULLs from unmatched LEFT JOINs)

**Usage:**
```bash
python etl/export_panel.py
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

STEP 2: Import Master Firm Data
└─ python etl/import_firms.py data/firms.xlsx
   → Populates dim_firm table
   → Reference for all panel data imports

STEP 3: Initialize Snapshots (version management)
└─ python etl/create_snapshot.py --batch-default 2020 2024
   → Creates 20 snapshots (snapshot_id: 1-20) for 4 data sources × fiscal years 2020-2024
   → Or creates a single snapshot for each pair of sources and fiscal years 

STEP 4: Import Panel Data
└─ python etl/import_panel.py data/panel.xlsx --snapshots 1-20
   → Loads 38 variables across 6 fact tables
   → Each record linked by snapshot_id

STEP 5: Validate Data Quality
└─ python etl/qc_checks.py
   → Generates outputs/qc_report.csv
   → Uses MAX(snapshot_id) per firm-year
   → Reports 6 types of validation errors

STEP 6: Export Clean Data
└─ python etl/export_panel.py
   → Generates outputs/panel_latest.csv combining 38 variables with latest snapshot selection
   → Ready for analysis
```

### Reproducible Scripts

**IMPORTANT: Database Initialization Required**

Before running the ETL pipeline, you must first initialize the MySQL database with schema and seed data:

```bash
# 1. Initialize MySQL database (MUST RUN FIRST!)
mysql -u root -p < sql/schema_and_seed.sql
# This creates all required tables (dim_firm, fact_data_snapshot, etc.) and seed data
```

After database initialization is complete, execute the ETL pipeline in order:

```bash
# 2. Import firms
python etl/import_firms.py data/firms.xlsx

# 3. Create 20 snapshots for batch
python etl/create_snapshot.py --setup
python etl/create_snapshot.py --batch-default 2020 2024

# 4. Import data 
python etl/import_panel.py data/panel_2020_2024.xlsx --modules financial --snapshots 1-5
python etl/import_panel.py data/panel_2020_2024.xlsx --modules cashflow --snapshots 6-10
python etl/import_panel.py data/panel_2020_2024.xlsx --modules market  --snapshots 11-15
python etl/import_panel.py data/panel_2020_2024.xlsx --modules ownership,innovation,meta  --snapshots 16-20

# 5. Validate imported data
python etl/qc_checks.py

# 6. Export clean panel
python etl/export_panel.py

# Check results
cat outputs/qc_report.csv | head -20
ls -lh outputs/panel_latest.csv
```

---

## 📝 Notes

- **Snapshot Workflow:** Each import operation assigns a `snapshot_id`, enabling multiple data versions and audit trails
- **Latest Data Selection:** Queries use `MAX(snapshot_id)` per firm_id + fiscal_year to automatically select newest version
- **Data Integrity:** All fact table records linked to `fact_data_snapshot` ensuring data traceability
- **Batch Operations:** Supports `snapshot_id` ranges (e.g., `1-5,6-10`) for efficient multi-version imports

---

**Last Updated:** March 2026  
**Project:** TEAM_4_FirmDataHub  

