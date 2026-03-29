/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
 /*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
 /*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
 /*!50503 SET NAMES utf8mb4 */;
 /*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
 /*!40103 SET TIME_ZONE='+00:00' */;
 /*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
 /*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
 /*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
 /*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

DROP DATABASE IF EXISTS vn_firm_panel;
CREATE DATABASE vn_firm_panel CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE vn_firm_panel;

-- ================= DIMENSION TABLES =================

CREATE TABLE dim_data_source (
    source_id SMALLINT NOT NULL AUTO_INCREMENT,
    source_name VARCHAR(100) NOT NULL,
    source_type ENUM('python_pkg', 'financial_statement', 'market', 'text_report') NOT NULL,
    provider VARCHAR(150),
    note VARCHAR(255),
    PRIMARY KEY (source_id),
    UNIQUE KEY (source_name)
) ENGINE=InnoDB;

CREATE TABLE dim_exchange (
    exchange_id TINYINT NOT NULL AUTO_INCREMENT,
    exchange_code VARCHAR(10) NOT NULL,
    exchange_name VARCHAR(100),
    PRIMARY KEY (exchange_id),
    UNIQUE KEY (exchange_code)
) ENGINE=InnoDB;

CREATE TABLE dim_industry_l2 (
    industry_l2_id SMALLINT NOT NULL AUTO_INCREMENT,
    industry_l2_name VARCHAR(150) NOT NULL,
    PRIMARY KEY (industry_l2_id),
    UNIQUE KEY (industry_l2_name)
) ENGINE=InnoDB;

CREATE TABLE dim_firm (
    firm_id BIGINT NOT NULL AUTO_INCREMENT,
    ticker VARCHAR(20) NOT NULL,
    company_name VARCHAR(255) NOT NULL,
    exchange_id TINYINT NOT NULL,
    industry_l2_id SMALLINT,
    founded_year SMALLINT,
    listed_year SMALLINT,
    status ENUM('active','delisted','inactive') DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (firm_id),
    UNIQUE KEY (ticker)
) ENGINE=InnoDB;

-- ================= SNAPSHOT TABLE =================

CREATE TABLE fact_data_snapshot (
    snapshot_id BIGINT NOT NULL AUTO_INCREMENT,
    snapshot_date DATE NOT NULL,
    period_from DATE,
    period_to DATE,
    fiscal_year SMALLINT NOT NULL,
    source_id SMALLINT NOT NULL,
    version_tag VARCHAR(50) DEFAULT 'v1',
    created_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (snapshot_id),
    FOREIGN KEY (source_id) REFERENCES dim_data_source(source_id)
) ENGINE=InnoDB;

-- ================= FACT TABLES =================

CREATE TABLE fact_financial_year (
    firm_id BIGINT NOT NULL,
    fiscal_year SMALLINT NOT NULL,
    snapshot_id BIGINT NOT NULL,
    unit_scale INT DEFAULT 1,
    currency_code VARCHAR(10) DEFAULT 'VND',

    net_sales DECIMAL(20,2),
    total_assets DECIMAL(20,2),
    selling_expenses DECIMAL(20,2),
    general_admin_expenses DECIMAL(20,2),
    intangible_assets_net DECIMAL(20,2),
    manufacturing_overhead DECIMAL(20,2),
    net_operating_income DECIMAL(20,2),
    raw_material_consumption DECIMAL(20,2),
    merchandise_purchase_year DECIMAL(20,2),
    wip_goods_purchase DECIMAL(20,2),
    outside_manufacturing_expenses DECIMAL(20,2),
    production_cost DECIMAL(20,2),
    rnd_expenses DECIMAL(20,2),
    net_income DECIMAL(20,2),
    total_equity DECIMAL(20,2),
    total_liabilities DECIMAL(20,2),
    cash_and_equivalents DECIMAL(20,2),
    long_term_debt DECIMAL(20,2),
    current_assets DECIMAL(20,2),
    current_liabilities DECIMAL(20,2),
    growth_ratio DECIMAL(10,6),
    inventory DECIMAL(20,2),
    net_ppe DECIMAL(20,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
    FOREIGN KEY (firm_id) REFERENCES dim_firm(firm_id),
    FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
) ENGINE=InnoDB;

CREATE TABLE fact_market_year (
    firm_id BIGINT NOT NULL,
    fiscal_year SMALLINT NOT NULL,
    snapshot_id BIGINT NOT NULL,

    shares_outstanding BIGINT,
    price_reference VARCHAR(50) DEFAULT 'close_year_end',
    share_price DECIMAL(20,4),
    market_value_equity DECIMAL(20,2),
    dividend_cash_paid DECIMAL(20,2),
    eps_basic DECIMAL(20,6),
    currency_code VARCHAR(10) DEFAULT 'VND',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
    FOREIGN KEY (firm_id) REFERENCES dim_firm(firm_id),
    FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
) ENGINE=InnoDB;

CREATE TABLE fact_ownership_year (
    firm_id BIGINT NOT NULL,
    fiscal_year SMALLINT NOT NULL,
    snapshot_id BIGINT NOT NULL,

    managerial_inside_own DECIMAL(10,6),
    state_own DECIMAL(10,6),
    institutional_own DECIMAL(10,6),
    foreign_own DECIMAL(10,6),

    note VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
    FOREIGN KEY (firm_id) REFERENCES dim_firm(firm_id),
    FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
) ENGINE=InnoDB;

CREATE TABLE fact_cashflow_year (
    firm_id BIGINT NOT NULL,
    fiscal_year SMALLINT NOT NULL,
    snapshot_id BIGINT NOT NULL,
	unit_scale INT DEFAULT 1,
    currency_code VARCHAR(10) DEFAULT 'VND',
    net_cfo DECIMAL(20,2),
    capex DECIMAL(20,2),
    net_cfi DECIMAL(20,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
    FOREIGN KEY (firm_id) REFERENCES dim_firm(firm_id),
    FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
) ENGINE=InnoDB;

CREATE TABLE fact_firm_year_meta (
    firm_id BIGINT NOT NULL,
    fiscal_year SMALLINT NOT NULL,
    snapshot_id BIGINT NOT NULL,

    employees_count INT,
    firm_age SMALLINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
    FOREIGN KEY (firm_id) REFERENCES dim_firm(firm_id),
    FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
) ENGINE=InnoDB;

CREATE TABLE fact_innovation_year (
    firm_id BIGINT NOT NULL,
    fiscal_year SMALLINT NOT NULL,
    snapshot_id BIGINT NOT NULL,

    product_innovation TINYINT,
    process_innovation TINYINT,

    evidence_source_id SMALLINT,
    evidence_note VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
    FOREIGN KEY (firm_id) REFERENCES dim_firm(firm_id),
    FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id),
    FOREIGN KEY (evidence_source_id) REFERENCES dim_data_source(source_id)
) ENGINE=InnoDB;

-- ================= LOG TABLE =================

CREATE TABLE fact_value_override_log (
    override_id BIGINT NOT NULL AUTO_INCREMENT,
    firm_id BIGINT NOT NULL,
    fiscal_year SMALLINT NOT NULL,
    table_name VARCHAR(80) NOT NULL,
    column_name VARCHAR(80) NOT NULL,
    old_value VARCHAR(255),
    new_value VARCHAR(255),
    reason VARCHAR(255),
    changed_by VARCHAR(80),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (override_id)
) ENGINE=InnoDB;

-- ================= VIEW =================
CREATE OR REPLACE VIEW vw_firm_panel_latest AS
WITH 
-- Lấy latest snapshot cho từng bảng fact
latest_ownership AS (
    SELECT firm_id, fiscal_year, MAX(snapshot_id) as snapshot_id
    FROM fact_ownership_year
    GROUP BY firm_id, fiscal_year
),
latest_market AS (
    SELECT firm_id, fiscal_year, MAX(snapshot_id) as snapshot_id
    FROM fact_market_year
    GROUP BY firm_id, fiscal_year
),
latest_financial AS (
    SELECT firm_id, fiscal_year, MAX(snapshot_id) as snapshot_id
    FROM fact_financial_year
    GROUP BY firm_id, fiscal_year
),
latest_cashflow AS (
    SELECT firm_id, fiscal_year, MAX(snapshot_id) as snapshot_id
    FROM fact_cashflow_year
    GROUP BY firm_id, fiscal_year
),
latest_innovation AS (
    SELECT firm_id, fiscal_year, MAX(snapshot_id) as snapshot_id
    FROM fact_innovation_year
    GROUP BY firm_id, fiscal_year
),
latest_meta AS (
    SELECT firm_id, fiscal_year, MAX(snapshot_id) as snapshot_id
    FROM fact_firm_year_meta
    GROUP BY firm_id, fiscal_year
),
-- Lấy tất cả các firm-year có trong bất kỳ bảng nào
all_firm_years AS (
    SELECT DISTINCT firm_id, fiscal_year FROM (
        SELECT firm_id, fiscal_year FROM latest_ownership
        UNION
        SELECT firm_id, fiscal_year FROM latest_market
        UNION
        SELECT firm_id, fiscal_year FROM latest_financial
        UNION
        SELECT firm_id, fiscal_year FROM latest_cashflow
        UNION
        SELECT firm_id, fiscal_year FROM latest_innovation
        UNION
        SELECT firm_id, fiscal_year FROM latest_meta
    ) t
)
SELECT 
    f.ticker,
    afy.fiscal_year,
    s.snapshot_date,
    s.version_tag,
    
    -- Ownership (4 variables)
    o.managerial_inside_own, 
    o.state_own, 
    o.institutional_own, 
    o.foreign_own,
    
    -- Market (4 variables)
    m.shares_outstanding,
    m.market_value_equity,
    m.dividend_cash_paid,
    m.eps_basic,
    
    -- Financial statements (23 variables)
    fn.net_sales, fn.total_assets, fn.selling_expenses, fn.general_admin_expenses,
    fn.intangible_assets_net, fn.manufacturing_overhead, fn.net_operating_income,
    fn.raw_material_consumption, fn.merchandise_purchase_year, fn.wip_goods_purchase,
    fn.outside_manufacturing_expenses, fn.production_cost, fn.rnd_expenses, fn.net_income,
    fn.total_equity, fn.total_liabilities, fn.cash_and_equivalents, fn.long_term_debt,
    fn.current_assets, fn.current_liabilities, fn.growth_ratio, fn.inventory, fn.net_ppe,
    
    -- Cashflow (3 variables)
    cf.net_cfo,
    cf.net_cfi,
    cf.capex,
    
    -- Innovation (2 variables)
    inv.product_innovation,
    inv.process_innovation,
    
    -- Metadata (2 variables)
    meta.employees_count,
    meta.firm_age
    
FROM all_firm_years afy
INNER JOIN dim_firm f ON afy.firm_id = f.firm_id

-- Join ownership với latest snapshot riêng
LEFT JOIN fact_ownership_year o 
    ON o.firm_id = afy.firm_id 
    AND o.fiscal_year = afy.fiscal_year
    AND o.snapshot_id = (SELECT snapshot_id FROM latest_ownership l 
                         WHERE l.firm_id = afy.firm_id AND l.fiscal_year = afy.fiscal_year)

-- Join market với latest snapshot riêng
LEFT JOIN fact_market_year m 
    ON m.firm_id = afy.firm_id 
    AND m.fiscal_year = afy.fiscal_year
    AND m.snapshot_id = (SELECT snapshot_id FROM latest_market l 
                         WHERE l.firm_id = afy.firm_id AND l.fiscal_year = afy.fiscal_year)

-- Join financial với latest snapshot riêng
LEFT JOIN fact_financial_year fn 
    ON fn.firm_id = afy.firm_id 
    AND fn.fiscal_year = afy.fiscal_year
    AND fn.snapshot_id = (SELECT snapshot_id FROM latest_financial l 
                          WHERE l.firm_id = afy.firm_id AND l.fiscal_year = afy.fiscal_year)

-- Join cashflow với latest snapshot riêng
LEFT JOIN fact_cashflow_year cf 
    ON cf.firm_id = afy.firm_id 
    AND cf.fiscal_year = afy.fiscal_year
    AND cf.snapshot_id = (SELECT snapshot_id FROM latest_cashflow l 
                          WHERE l.firm_id = afy.firm_id AND l.fiscal_year = afy.fiscal_year)

-- Join innovation với latest snapshot riêng
LEFT JOIN fact_innovation_year inv 
    ON inv.firm_id = afy.firm_id 
    AND inv.fiscal_year = afy.fiscal_year
    AND inv.snapshot_id = (SELECT snapshot_id FROM latest_innovation l 
                           WHERE l.firm_id = afy.firm_id AND l.fiscal_year = afy.fiscal_year)

-- Join metadata với latest snapshot riêng
LEFT JOIN fact_firm_year_meta meta 
    ON meta.firm_id = afy.firm_id 
    AND meta.fiscal_year = afy.fiscal_year
    AND meta.snapshot_id = (SELECT snapshot_id FROM latest_meta l 
                            WHERE l.firm_id = afy.firm_id AND l.fiscal_year = afy.fiscal_year)

-- Join snapshot info (lấy từ bất kỳ snapshot_id nào, ưu tiên lớn nhất)
LEFT JOIN fact_data_snapshot s 
    ON s.snapshot_id = (
        SELECT MAX(snapshot_id) FROM (
            SELECT snapshot_id FROM latest_ownership l WHERE l.firm_id = afy.firm_id AND l.fiscal_year = afy.fiscal_year
            UNION ALL
            SELECT snapshot_id FROM latest_market l WHERE l.firm_id = afy.firm_id AND l.fiscal_year = afy.fiscal_year
            UNION ALL
            SELECT snapshot_id FROM latest_financial l WHERE l.firm_id = afy.firm_id AND l.fiscal_year = afy.fiscal_year
        ) t
    )
ORDER BY f.ticker, afy.fiscal_year;


-- ================= INSERT DATA =================

INSERT INTO dim_data_source (source_name, source_type, provider, note) VALUES
('vnstock_v2', 'python_pkg', 'Thinh Vu (VNstock Team)', 'A specialized Python library for Vietnam stock market, providing real-time price feeds, corporate finanical ratios, and historical OHLC data.'),
('BCTC_Audited', 'financial_statement', 'Company/Exchange', 'Audited financial statements'),
('Vietstock', 'market', 'Vietstock', 'Market fields (price, shares, dividend, EPS)'),
('AnnualReport', 'text_report', 'Company', 'Annual report / disclosures for ownership, innovation & health');

INSERT INTO dim_exchange (exchange_code, exchange_name) VALUES
('HOSE', 'Ho Chi Minh Stock Exchange'),
('HNX', 'Hanoi Stock Exchange');

INSERT INTO dim_industry_l2 (industry_l2_id, industry_l2_name) VALUES
(1, 'Tài nguyên Cơ bản'),
(2, 'Thực phẩm và đồ uống'),
(3, 'Hóa chất'),
(4, 'Dầu khí'),
(5, 'Hàng & Dịch vụ Công nghiệp'),
(6, 'Hàng cá nhân & Gia dụng'),
(7, 'Xây dựng và Vật liệu'),
(8, 'Ô tô và phụ tùng'),
(9, 'Y tế');

INSERT INTO dim_firm (ticker, company_name, exchange_id, industry_l2_id, status) VALUES
('AAA', 'An Phát Bioplastics', '1', '1', 'active'),
('ANV', 'Thủy sản Nam Việt', '1', '2', 'active'),
('BFC', 'Phân bón Bình Điền', '1', '1', 'active'),
('CTD', 'Xây dựng Coteccons', '1', '3', 'active'),
('DHC', 'Đông Hải Bến Tre', '1', '4', 'active'),
('FMC', 'Thực phẩm Sao Ta', '1', '2', 'active'),
('HAX', 'Ô tô Hàng Xanh', '1', '5', 'active'),
('IMP', 'IMEXPHARM', '1', '6', 'active'),
('NCT', 'DV Hàng hóa Nội Bài', '1', '7', 'active'),
('NTP', 'Nhựa Tiền Phong', '2', '3', 'active'),
('PAN', 'Tập đoàn PAN', '1', '2', 'active'),
('PHR', 'Cao su Phước Hòa', '1', '1', 'active'),
('PTB', 'Công ty Cổ phần Phú Tài', '1', '4', 'active'),
('PVT', 'Vận tải Dầu khí PVTrans', '1', '7', 'active'),
('SBT', 'Mía đường Thành Thành Công - Biên Hòa', '1', '2', 'active'),
('SGN', 'Phục vụ mặt đất Sài Gòn', '1', '7', 'active'),
('STK', 'Sợi Thế Kỷ', '1', '8', 'active'),
('TCM', 'Dệt may Thành Công', '1', '8', 'active'),
('TLG', 'Tập đoàn Thiên Long', '1', '8', 'active'),
('VCS', 'VICOSTONE', '2', '3', 'active');


/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;
 /*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
 /*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
 /*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
 /*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
 /*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
 /*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
 /*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;