"""
qc_checks.py - Data Quality Check
Usage: python etl/qc_checks.py
"""

import pandas as pd
import mysql.connector
from mysql.connector import Error
import sys
import os
from db_config import DB_CONFIG

class DataQualityChecker:
    def __init__(self, host='localhost', database='firm_data_hub', port='3306',
                 user='root', password='your_password'):
        self.connection = None
        try:
            self.connection = mysql.connector.connect(
                host=host, database=database, port=port, user=user, password=password
            )
        except Error as e:
            print(f"Error connecting to MySQL: {e}")
            sys.exit(1)
        self.qc_errors = []
    
    def _add_error(self, ticker, fiscal_year, field_name, error_type, message):
        """Helper to add error"""
        self.qc_errors.append({
            'ticker': ticker, 'fiscal_year': fiscal_year,
            'field_name': field_name, 'error_type': error_type, 'message': message
        })
    
    def _get_latest_data(self, table_name, field_list, join_firm=True):
        """Helper: Get MAX(snapshot_id) data for each firm_id + fiscal_year"""
        cursor = self.connection.cursor(dictionary=True)
        join_clause = "JOIN dim_firm f ON t.firm_id = f.firm_id" if join_firm else ""
        fields = f"{field_list}, f.ticker" if join_firm else field_list
        
        query = f"""
            SELECT {fields}, t.fiscal_year
            FROM {table_name} t {join_clause}
            INNER JOIN (
                SELECT firm_id, fiscal_year, MAX(snapshot_id) as max_sid
                FROM {table_name}
                GROUP BY firm_id, fiscal_year
            ) latest ON t.firm_id = latest.firm_id 
                    AND t.fiscal_year = latest.fiscal_year
                    AND t.snapshot_id = latest.max_sid
        """
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return results
    
    def run_all_checks(self):
        print("\n=== Starting Quality Checks ===")
        self.qc_errors = []
        
        self.check_missing_values()
        self.check_ownership_ratios()
        self.check_shares_outstanding()
        self.check_total_assets()
        self.check_current_liabilities()
        self.check_growth_ratios()
        
        print(f"\nQuality checks completed. Found {len(self.qc_errors)} errors")
        return self.qc_errors
    
    def check_missing_values(self):
        print("\n Checking missing values...")
        
        # Ownership fields
        rows = self._get_latest_data('fact_ownership_year',
            't.managerial_inside_own, t.state_own, t.institutional_own, t.foreign_own')
        for row in rows:
            for field in ['managerial_inside_own', 'state_own', 'institutional_own', 'foreign_own']:
                if row[field] is None:
                    self._add_error(row['ticker'], row['fiscal_year'], f'ownership.{field}',
                        'missing_value', f"Missing {field}")
        
        # Market fields
        rows = self._get_latest_data('fact_market_year',
            't.shares_outstanding, t.market_value_equity, t.dividend_cash_paid, t.eps_basic')
        for row in rows:
            for field in ['shares_outstanding', 'market_value_equity', 'dividend_cash_paid', 'eps_basic']:
                if row[field] is None:
                    self._add_error(row['ticker'], row['fiscal_year'], f'market.{field}',
                        'missing_value', f"Missing {field}")
        
        # Financial fields
        rows = self._get_latest_data('fact_financial_year',
            't.net_sales, t.total_assets, t.total_equity, t.total_liabilities, t.net_income')
        for row in rows:
            for field in ['net_sales', 'total_assets', 'total_equity', 'total_liabilities', 'net_income']:
                if row[field] is None:
                    self._add_error(row['ticker'], row['fiscal_year'], f'financial.{field}',
                        'missing_value', f"Missing {field}")
    
    def check_ownership_ratios(self):
        print(" Checking ownership ratios [0,1]...")
        
        rows = self._get_latest_data('fact_ownership_year',
            't.managerial_inside_own, t.state_own, t.institutional_own, t.foreign_own')
        
        for row in rows:
            for field in ['managerial_inside_own', 'state_own', 'institutional_own', 'foreign_own']:
                value = row[field]
                if value is not None and (value < 0 or value > 1):
                    self._add_error(row['ticker'], row['fiscal_year'], field,
                        'out_of_range', f"{field}={value} outside [0,1]")
    
    def check_shares_outstanding(self):
        print(" Checking shares outstanding > 0...")
        
        cursor = self.connection.cursor(dictionary=True)
        cursor.execute("""
            SELECT f.ticker, m.fiscal_year, m.shares_outstanding
            FROM fact_market_year m
            JOIN dim_firm f ON m.firm_id = f.firm_id
            INNER JOIN (
                SELECT firm_id, fiscal_year, MAX(snapshot_id) as max_sid
                FROM fact_market_year
                GROUP BY firm_id, fiscal_year
            ) latest ON m.firm_id = latest.firm_id AND m.fiscal_year = latest.fiscal_year
                    AND m.snapshot_id = latest.max_sid
            WHERE m.shares_outstanding IS NOT NULL AND m.shares_outstanding <= 0
        """)
        
        for row in cursor.fetchall():
            self._add_error(row['ticker'], row['fiscal_year'], 'shares_outstanding',
                'invalid_value', f"shares_outstanding={row['shares_outstanding']} should be > 0")
        cursor.close()
    
    def check_total_assets(self):
        print(" Checking total assets >= 0...")
        
        cursor = self.connection.cursor(dictionary=True)
        cursor.execute("""
            SELECT f.ticker, fn.fiscal_year, fn.total_assets
            FROM fact_financial_year fn
            JOIN dim_firm f ON fn.firm_id = f.firm_id
            INNER JOIN (
                SELECT firm_id, fiscal_year, MAX(snapshot_id) as max_sid
                FROM fact_financial_year
                GROUP BY firm_id, fiscal_year
            ) latest ON fn.firm_id = latest.firm_id AND fn.fiscal_year = latest.fiscal_year
                    AND fn.snapshot_id = latest.max_sid
            WHERE fn.total_assets IS NOT NULL AND fn.total_assets < 0
        """)
        
        for row in cursor.fetchall():
            self._add_error(row['ticker'], row['fiscal_year'], 'total_assets',
                'negative_value', f"total_assets={row['total_assets']} should be >= 0")
        cursor.close()
    
    def check_current_liabilities(self):
        print(" Checking current liabilities >= 0...")
        
        cursor = self.connection.cursor(dictionary=True)
        cursor.execute("""
            SELECT f.ticker, fn.fiscal_year, fn.current_liabilities
            FROM fact_financial_year fn
            JOIN dim_firm f ON fn.firm_id = f.firm_id
            INNER JOIN (
                SELECT firm_id, fiscal_year, MAX(snapshot_id) as max_sid
                FROM fact_financial_year
                GROUP BY firm_id, fiscal_year
            ) latest ON fn.firm_id = latest.firm_id AND fn.fiscal_year = latest.fiscal_year
                    AND fn.snapshot_id = latest.max_sid
            WHERE fn.current_liabilities IS NOT NULL AND fn.current_liabilities < 0
        """)
        
        for row in cursor.fetchall():
            self._add_error(row['ticker'], row['fiscal_year'], 'current_liabilities',
                'negative_value', f"current_liabilities={row['current_liabilities']} should be >= 0")
        cursor.close()
    
    def check_growth_ratios(self, min_growth=-0.95, max_growth=5.0):
        print(" Checking growth ratios (range: [-0.95, 5.0])...")
        
        cursor = self.connection.cursor(dictionary=True)
        cursor.execute("""
            SELECT f.ticker, fn.fiscal_year, fn.growth_ratio
            FROM fact_financial_year fn
            JOIN dim_firm f ON fn.firm_id = f.firm_id
            INNER JOIN (
                SELECT firm_id, fiscal_year, MAX(snapshot_id) as max_sid
                FROM fact_financial_year
                GROUP BY firm_id, fiscal_year
            ) latest ON fn.firm_id = latest.firm_id AND fn.fiscal_year = latest.fiscal_year
                    AND fn.snapshot_id = latest.max_sid
            WHERE fn.growth_ratio IS NOT NULL
        """)
        
        for row in cursor.fetchall():
            growth = row['growth_ratio']
            if growth < min_growth or growth > max_growth:
                self._add_error(row['ticker'], row['fiscal_year'], 'growth_ratio',
                    'extreme_growth', f"growth_ratio={growth:.2%} outside [{min_growth:.1%}, {max_growth:.1%}]")
        cursor.close()
    
    def export_qc_report(self, output_file='outputs/qc_report.csv'):
        df = pd.DataFrame(self.qc_errors) if self.qc_errors else \
             pd.DataFrame(columns=['ticker', 'fiscal_year', 'field_name', 'error_type', 'message'])
        
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"\n QC report: {output_file}")
        print(f"  Errors found: {len(self.qc_errors)}")
        
        if self.qc_errors:
            print("\n  Error Summary:")
            for error_type, count in df['error_type'].value_counts().items():
                print(f"    - {error_type}: {count}")
    
    def close(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()


if __name__ == '__main__':
    checker = DataQualityChecker(**DB_CONFIG)
    checker.run_all_checks()
    checker.export_qc_report()
    checker.close()
