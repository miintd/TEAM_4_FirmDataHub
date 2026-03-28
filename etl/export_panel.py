"""
export_panel.py
Export clean panel dataset for analysis

Usage: 
python etl/export_panel.py                    # Export latest panel
"""

import pandas as pd
import mysql.connector
from mysql.connector import Error
import sys
from datetime import datetime
import os
from db_config import DB_CONFIG

class PanelExporter:
    def __init__(self, host='localhost', database='firm_data_hub', port='3306',
                 user='root', password='your_password'):
        self.connection = None
        try:
            self.connection = mysql.connector.connect(
                host=host,
                database=database,
                port=port,
                user=user,
                password=password
            )
        except Error as e:
            print(f"Error connecting to MySQL: {e}")
            sys.exit(1) 
    
    def export_latest_panel(self, output_file='outputs/panel_latest.csv'):
        try:
            print("\n Exporting panel dataset...")
            
            cursor = self.connection.cursor(dictionary=True)

            query = """
                -- Get all distinct firm_id + fiscal_year combinations from all fact tables
                WITH all_firm_years AS (
                    SELECT DISTINCT firm_id, fiscal_year FROM fact_ownership_year
                    UNION ALL
                    SELECT DISTINCT firm_id, fiscal_year FROM fact_market_year
                    UNION ALL
                    SELECT DISTINCT firm_id, fiscal_year FROM fact_financial_year
                    UNION ALL
                    SELECT DISTINCT firm_id, fiscal_year FROM fact_cashflow_year
                    UNION ALL
                    SELECT DISTINCT firm_id, fiscal_year FROM fact_innovation_year
                    UNION ALL
                    SELECT DISTINCT firm_id, fiscal_year FROM fact_firm_year_meta
                ),
                -- Get latest ownership data for each firm_id + fiscal_year
                ownership_latest AS (
                    SELECT o.* FROM fact_ownership_year o
                    INNER JOIN (
                        SELECT firm_id, fiscal_year, MAX(snapshot_id) as max_snapshot_id
                        FROM fact_ownership_year
                        GROUP BY firm_id, fiscal_year
                    ) latest ON o.firm_id = latest.firm_id 
                            AND o.fiscal_year = latest.fiscal_year 
                            AND o.snapshot_id = latest.max_snapshot_id
                ),
                -- Get latest market data for each firm_id + fiscal_year
                market_latest AS (
                    SELECT m.* FROM fact_market_year m
                    INNER JOIN (
                        SELECT firm_id, fiscal_year, MAX(snapshot_id) as max_snapshot_id
                        FROM fact_market_year
                        GROUP BY firm_id, fiscal_year
                    ) latest ON m.firm_id = latest.firm_id 
                            AND m.fiscal_year = latest.fiscal_year 
                            AND m.snapshot_id = latest.max_snapshot_id
                ),
                -- Get latest financial data for each firm_id + fiscal_year
                financial_latest AS (
                    SELECT fn.* FROM fact_financial_year fn
                    INNER JOIN (
                        SELECT firm_id, fiscal_year, MAX(snapshot_id) as max_snapshot_id
                        FROM fact_financial_year
                        GROUP BY firm_id, fiscal_year
                    ) latest ON fn.firm_id = latest.firm_id 
                            AND fn.fiscal_year = latest.fiscal_year 
                            AND fn.snapshot_id = latest.max_snapshot_id
                ),
                -- Get latest cashflow data for each firm_id + fiscal_year
                cashflow_latest AS (
                    SELECT cf.* FROM fact_cashflow_year cf
                    INNER JOIN (
                        SELECT firm_id, fiscal_year, MAX(snapshot_id) as max_snapshot_id
                        FROM fact_cashflow_year
                        GROUP BY firm_id, fiscal_year
                    ) latest ON cf.firm_id = latest.firm_id 
                            AND cf.fiscal_year = latest.fiscal_year 
                            AND cf.snapshot_id = latest.max_snapshot_id
                ),
                -- Get latest innovation data for each firm_id + fiscal_year
                innovation_latest AS (
                    SELECT inv.* FROM fact_innovation_year inv
                    INNER JOIN (
                        SELECT firm_id, fiscal_year, MAX(snapshot_id) as max_snapshot_id
                        FROM fact_innovation_year
                        GROUP BY firm_id, fiscal_year
                    ) latest ON inv.firm_id = latest.firm_id 
                            AND inv.fiscal_year = latest.fiscal_year 
                            AND inv.snapshot_id = latest.max_snapshot_id
                ),
                -- Get latest metadata for each firm_id + fiscal_year
                meta_latest AS (
                    SELECT meta.* FROM fact_firm_year_meta meta
                    INNER JOIN (
                        SELECT firm_id, fiscal_year, MAX(snapshot_id) as max_snapshot_id
                        FROM fact_firm_year_meta
                        GROUP BY firm_id, fiscal_year
                    ) latest ON meta.firm_id = latest.firm_id 
                            AND meta.fiscal_year = latest.fiscal_year 
                            AND meta.snapshot_id = latest.max_snapshot_id
                )
                -- Main query: join all latest data
                SELECT 
                    f.ticker,
                    
                    -- Year and snapshot info
                    afy.fiscal_year,
                    
                    -- Ownership (4 variables)
                    COALESCE(o.managerial_inside_own, 0) as managerial_inside_own, 
                    COALESCE(o.state_own, 0) as state_own, 
                    COALESCE(o.institutional_own, 0) as institutional_own, 
                    COALESCE(o.foreign_own, 0) as foreign_own,
                    
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
                    
                FROM (
                    SELECT DISTINCT firm_id, fiscal_year FROM all_firm_years
                ) afy
                
                -- Join dimension tables
                LEFT JOIN dim_firm f ON afy.firm_id = f.firm_id
                
                -- Join ownership (get latest snapshot only)
                LEFT JOIN ownership_latest o 
                    ON afy.firm_id = o.firm_id 
                    AND afy.fiscal_year = o.fiscal_year
                
                -- Join market (get latest snapshot only)
                LEFT JOIN market_latest m 
                    ON afy.firm_id = m.firm_id 
                    AND afy.fiscal_year = m.fiscal_year
                    
                -- Join financial (get latest snapshot only)
                LEFT JOIN financial_latest fn 
                    ON afy.firm_id = fn.firm_id 
                    AND afy.fiscal_year = fn.fiscal_year
                    
                -- Join cashflow (get latest snapshot only)
                LEFT JOIN cashflow_latest cf 
                    ON afy.firm_id = cf.firm_id 
                    AND afy.fiscal_year = cf.fiscal_year
                    
                -- Join innovation (get latest snapshot only)
                LEFT JOIN innovation_latest inv 
                    ON afy.firm_id = inv.firm_id 
                    AND afy.fiscal_year = inv.fiscal_year
                    
                -- Join metadata (get latest snapshot only)
                LEFT JOIN meta_latest meta 
                    ON afy.firm_id = meta.firm_id 
                    AND afy.fiscal_year = meta.fiscal_year
                    
                ORDER BY f.ticker, afy.fiscal_year
            """
            
            cursor.execute(query)
            rows = cursor.fetchall()
            
            if not rows:
                print("No data found in database!")
                cursor.close()
                return
            
            df = pd.DataFrame(rows)
            
            print(f"\nRetrieved {len(df)} firm-year records")
            print(f"  - Unique tickers: {df['ticker'].nunique()}")
            print(f"  - Years covered: {df['fiscal_year'].min()} - {df['fiscal_year'].max()}")

            os.makedirs(os.path.dirname(output_file), exist_ok=True)

            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            
            print(f"\nPanel dataset exported to: {output_file}")
            print(f"  - Total columns: {len(df.columns)}")
            print(f"  - Total rows: {len(df)}")

            print("\nData Completeness Summary:")
            
            key_vars = [
                'state_own', 'foreign_own', 'shares_outstanding', 
                'market_value_equity', 'net_sales', 'total_assets', 'net_income',
                'net_cfo', 'capex', 'employees_count'
            ]
            
            for var in key_vars:
                if var in df.columns:
                    non_null = df[var].notna().sum()
                    pct = (non_null / len(df)) * 100
                    print(f"  - {var}: {non_null}/{len(df)} ({pct:.1f}%)")
            
            cursor.close()
            
            return df
            
        except Exception as e:
            print(f"Error exporting panel: {e}")
            raise
    
    def close(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("\nDatabase connection closed")


def main():
    exporter = PanelExporter(**DB_CONFIG)
    try:
        output_file = 'outputs/panel_latest.csv'
        
        exporter.export_latest_panel(output_file)
    
    finally:
        exporter.close()


if __name__ == "__main__":
    main()
