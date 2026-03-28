"""
import_panel.py
Import panel data (38 variables) for firm-years

Usage: python etl/import_panel.py <excel_file> [--modules financial,ownership,...] [--snapshots <snapshot_ids>]
Examples: 
    # Single snapshot
    python etl/import_panel.py data/panel_2020_2024.xlsx --modules [] --snapshots 1
    
    # Multiple snapshots with ranges
    python etl/import_panel.py data/panel_2020_2024.xlsx --modules [] --snapshots 1-5,6-10
    
    # Multiple snapshots without ranges
    python etl/import_panel.py data/panel_2020_2024.xlsx --modules [] --snapshots 1,2,3,4,5,6,7,8,9,10
    
    # All modules, multiple snapshots
    python etl/import_panel.py data/panel_2020_2024.xlsx --snapshots 1-20
"""

import pandas as pd
import mysql.connector
from mysql.connector import Error
import argparse
import sys
from datetime import datetime
from db_config import DB_CONFIG  

ALL_MODULES = ['financial', 'ownership', 'market', 'cashflow', 'innovation', 'meta']

class PanelImporter:
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
    
    def import_panel_data(self, excel_file, snapshot_id, modules=None):
        if modules is None:
            modules = ALL_MODULES
        else:
            invalid = [m for m in modules if m not in ALL_MODULES]
            if invalid:
                raise ValueError(f"Unknown module(s): {invalid}. Valid choices: {ALL_MODULES}")
        try:
            cursor = self.connection.cursor()
            cursor.execute(
                "SELECT fiscal_year FROM fact_data_snapshot WHERE snapshot_id = %s",
                (snapshot_id,)
            )
            result = cursor.fetchone()
            if not result:
                raise ValueError(f"Snapshot ID {snapshot_id} does not exist!")
            
            target_year = int(result[0])

            df = pd.read_excel(excel_file)
            
            if 'ticker' not in df.columns or 'fiscal_year' not in df.columns:
                raise ValueError("Excel must have 'ticker' and 'fiscal_year' columns")
            
            df["fiscal_year"] = df["fiscal_year"].astype(int)
            df = df[df["fiscal_year"] == target_year]
            
            cursor.execute(
                "SELECT snapshot_id FROM fact_data_snapshot WHERE snapshot_id = %s",
                (snapshot_id,)
            )
            if not cursor.fetchone():
                raise ValueError(f"Snapshot ID {snapshot_id} does not exist!")
            
            imported = 0
            skipped = 0
            
            module_dispatch = {
                'ownership':  self._import_ownership,
                'market':     self._import_market,
                'financial':  self._import_financial,
                'cashflow':   self._import_cashflow,
                'innovation': self._import_innovation,
                'meta':       self._import_meta,
            }

            for idx, row in df.iterrows():
                try:
                    ticker = str(row['ticker']).strip().upper()
                    fiscal_year = int(row['fiscal_year'])
                    
                    firm_id = self._get_firm_id(cursor, ticker)
                    if firm_id is None:
                        print(f"Skipped: {ticker} not found in dim_firm")
                        skipped += 1
                        continue
                    
                    for module in modules:
                        module_dispatch[module](cursor, firm_id, fiscal_year, snapshot_id, row)

                    self.connection.commit()
                    imported += 1
                    print(f"Imported: {ticker} - {fiscal_year}")
                    
                except Exception as e:
                    print(f"Error importing {ticker} ({fiscal_year}): {e}")
                    self.connection.rollback()
                    skipped += 1
            
            print(f"  - Successfully imported: {imported}")
            print(f"  - Skipped/Errors: {skipped}")
            
            cursor.close()
            
        except Exception as e:
            print(f"Fatal error during import: {e}")
            if self.connection:
                self.connection.rollback()
            raise
    
    def _get_firm_id(self, cursor, ticker):
        cursor.execute(
            "SELECT firm_id FROM dim_firm WHERE ticker = %s",
            (ticker,)
        )
        result = cursor.fetchone()
        return result[0] if result else None
    
    def _safe_float(self, value):
        if pd.isna(value) or value is None or value == '':
            return None
        try:
            return float(value)
        except:
            return None
    
    def _safe_int(self, value):
        if pd.isna(value) or value is None or value == '':
            return None
        try:
            return int(value)
        except:
            return None
    
    def _import_ownership(self, cursor, firm_id, fiscal_year, snapshot_id, row):
        cursor.execute("""
            DELETE FROM fact_ownership_year 
            WHERE firm_id = %s AND fiscal_year = %s AND snapshot_id = %s
        """, (firm_id, fiscal_year, snapshot_id))
        
        cursor.execute("""
            INSERT INTO fact_ownership_year 
            (firm_id, fiscal_year, snapshot_id, 
             managerial_inside_own, state_own, institutional_own, foreign_own)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            firm_id, fiscal_year, snapshot_id,
            self._safe_float(row.get('managerial_inside_own')),
            self._safe_float(row.get('state_own')),
            self._safe_float(row.get('institutional_own')),
            self._safe_float(row.get('foreign_own'))
        ))
    
    def _import_market(self, cursor, firm_id, fiscal_year, snapshot_id, row):
        cursor.execute("""
            DELETE FROM fact_market_year 
            WHERE firm_id = %s AND fiscal_year = %s AND snapshot_id = %s
        """, (firm_id, fiscal_year, snapshot_id))
        
        cursor.execute("""
            INSERT INTO fact_market_year 
            (firm_id, fiscal_year, snapshot_id, 
             shares_outstanding, market_value_equity, dividend_cash_paid, eps_basic)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            firm_id, fiscal_year, snapshot_id,
            self._safe_float(row.get('shares_outstanding')),
            self._safe_float(row.get('market_value_equity')),
            self._safe_float(row.get('dividend_cash_paid')),
            self._safe_float(row.get('eps_basic'))
        ))
    
    def _import_financial(self, cursor, firm_id, fiscal_year, snapshot_id, row):
        cursor.execute("""
            DELETE FROM fact_financial_year 
            WHERE firm_id = %s AND fiscal_year = %s AND snapshot_id = %s
        """, (firm_id, fiscal_year, snapshot_id))
        
        cursor.execute("""
            INSERT INTO fact_financial_year 
            (firm_id, fiscal_year, snapshot_id,
             net_sales, total_assets, selling_expenses, general_admin_expenses,
             intangible_assets_net, manufacturing_overhead, net_operating_income,
             raw_material_consumption, merchandise_purchase_year, wip_goods_purchase,
             outside_manufacturing_expenses, production_cost, rnd_expenses, net_income,
             total_equity, total_liabilities, cash_and_equivalents, long_term_debt,
             current_assets, current_liabilities, growth_ratio, inventory, net_ppe)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            firm_id, fiscal_year, snapshot_id,
            self._safe_float(row.get('net_sales')),
            self._safe_float(row.get('total_assets')),
            self._safe_float(row.get('selling_expenses')),
            self._safe_float(row.get('general_admin_expenses')),
            self._safe_float(row.get('intangible_assets_net')),
            self._safe_float(row.get('manufacturing_overhead')),
            self._safe_float(row.get('net_operating_income')),
            self._safe_float(row.get('raw_material_consumption')),
            self._safe_float(row.get('merchandise_purchase_year')),
            self._safe_float(row.get('wip_goods_purchase')),
            self._safe_float(row.get('outside_manufacturing_expenses')),
            self._safe_float(row.get('production_cost')),
            self._safe_float(row.get('rnd_expenses')),
            self._safe_float(row.get('net_income')),
            self._safe_float(row.get('total_equity')),
            self._safe_float(row.get('total_liabilities')),
            self._safe_float(row.get('cash_and_equivalents')),
            self._safe_float(row.get('long_term_debt')),
            self._safe_float(row.get('current_assets')),
            self._safe_float(row.get('current_liabilities')),
            self._safe_float(row.get('growth_ratio')),
            self._safe_float(row.get('inventory')),
            self._safe_float(row.get('net_ppe')),
        ))
    
    def _import_cashflow(self, cursor, firm_id, fiscal_year, snapshot_id, row):
        cursor.execute("""
            DELETE FROM fact_cashflow_year 
            WHERE firm_id = %s AND fiscal_year = %s AND snapshot_id = %s
        """, (firm_id, fiscal_year, snapshot_id))
        
        cursor.execute("""
            INSERT INTO fact_cashflow_year 
            (firm_id, fiscal_year, snapshot_id, net_cfo, capex, net_cfi)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            firm_id, fiscal_year, snapshot_id,
            self._safe_float(row.get('net_cfo')),
            self._safe_float(row.get('capex')),
            self._safe_float(row.get('net_cfi'))
        ))
    
    def _import_innovation(self, cursor, firm_id, fiscal_year, snapshot_id, row):
        cursor.execute("""
            DELETE FROM fact_innovation_year 
            WHERE firm_id = %s AND fiscal_year = %s AND snapshot_id = %s
        """, (firm_id, fiscal_year, snapshot_id))
        
        cursor.execute("""
            INSERT INTO fact_innovation_year 
            (firm_id, fiscal_year, snapshot_id, 
             product_innovation, process_innovation)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            firm_id, fiscal_year, snapshot_id,
            self._safe_int(row.get('product_innovation')),
            self._safe_int(row.get('process_innovation')),
        ))
    
    def _import_meta(self, cursor, firm_id, fiscal_year, snapshot_id, row):
        cursor.execute("""
            DELETE FROM fact_firm_year_meta 
            WHERE firm_id = %s AND fiscal_year = %s AND snapshot_id = %s
        """, (firm_id, fiscal_year, snapshot_id))
        
        cursor.execute("""
            INSERT INTO fact_firm_year_meta 
            (firm_id, fiscal_year, snapshot_id, employees_count, firm_age)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            firm_id, fiscal_year, snapshot_id,
            self._safe_int(row.get('employees_count')),
            self._safe_int(row.get('firm_age'))
        ))
    
    def parse_snapshot_ids(self, snapshot_spec):
        snapshot_ids = []
        parts = snapshot_spec.split(',')
        
        for part in parts:
            part = part.strip()
            if '-' in part:
                # It's a range
                try:
                    start, end = part.split('-')
                    start, end = int(start.strip()), int(end.strip())
                    snapshot_ids.extend(range(start, end + 1))
                except:
                    print(f"Invalid range format: {part}")
                    raise ValueError(f"Invalid range format: {part}")
            else:
                # It's a single ID
                try:
                    snapshot_ids.append(int(part))
                except:
                    print(f"Invalid snapshot ID: {part}")
                    raise ValueError(f"Invalid snapshot ID: {part}")
        
        # Remove duplicates and sort
        snapshot_ids = sorted(set(snapshot_ids))
        return snapshot_ids
    
    def close(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("\n Database connection closed")

def main():
    parser = argparse.ArgumentParser(
        description="Import panel data into firm_data_hub",
        epilog=__doc__,
    )
    parser.add_argument("excel_file")
    parser.add_argument("--snapshots", required=True, help="Snapshot IDs to import (e.g., '1-5,6-10' or '1,2,3')")
    parser.add_argument("--modules", default=None)

    args = parser.parse_args()

    modules = (
        [m.strip().lower() for m in args.modules.split(",") if m.strip()]
        if args.modules else None
    )

    importer = PanelImporter(**DB_CONFIG)
    try:
        # Parse snapshot IDs
        snapshot_ids = importer.parse_snapshot_ids(args.snapshots)
        print(f"Processing {len(snapshot_ids)} snapshot(s): {snapshot_ids}\n")
        
        # Import data for each snapshot
        for snapshot_id in snapshot_ids:
            print(f"\n{'='*60}")
            print(f"Importing for snapshot_id: {snapshot_id}")
            print(f"{'='*60}")
            importer.import_panel_data(
                excel_file=args.excel_file,
                snapshot_id=snapshot_id,
                modules=modules,
            )
    finally:
        importer.close()


if __name__ == "__main__":
    main()