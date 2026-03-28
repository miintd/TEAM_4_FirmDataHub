"""
import_firms.py
Import company directory from Excel into dimension tables

Usage: python etl/import_firms.py data/firms.xlsx
"""

import pandas as pd
import mysql.connector
from mysql.connector import Error
import sys
from db_config import DB_CONFIG
from datetime import datetime

class FirmImporter:
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
    
    def import_from_excel(self, excel_file):
        try:
            print(f"\nReading Excel file: {excel_file}")
            df = pd.read_excel(excel_file)
            
            required_cols = ['ticker', 'company_name', 'Exchange', 'Industry']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Missing columns: {missing_cols}")
            
            print(f"Found {len(df)} firms to import")
            
            cursor = self.connection.cursor()
 
            imported = 0
            updated = 0
            
            for idx, row in df.iterrows():
                ticker = str(row['ticker']).strip().upper()
                company_name = str(row['company_name']).strip()
                exchange = str(row['Exchange']).strip().upper()
                industry_l2 = str(row['Industry']).strip()
                
                exchange_id = self._get_or_create_exchange(cursor, exchange)
                industry_id = self._get_or_create_industry(cursor, industry_l2)

                result = self._insert_or_update_firm(
                    cursor, ticker, company_name, exchange_id, industry_id
                )
                
                if result == 'inserted':
                    imported += 1
                elif result == 'updated':
                    updated += 1

                self.connection.commit()
            
            print(f"\nImport completed!")
            print(f"  - New firms imported: {imported}")
            print(f"  - Existing firms updated: {updated}")
            
            cursor.close()
            
        except Exception as e:
            print(f"Error during import: {e}")
            if self.connection:
                self.connection.rollback()
            raise
    
    def _get_or_create_exchange(self, cursor, exchange_name):
        cursor.execute(
            "SELECT exchange_id FROM dim_exchange WHERE exchange_code = %s",
            (exchange_name,)
        )
        result = cursor.fetchone()
        
        if result:
            return result[0]
        else:
            cursor.execute(
                "INSERT INTO dim_exchange (exchange_code, exchange_name) VALUES (%s, %s)",
                (exchange_name, exchange_name)
            )
            return cursor.lastrowid
    
    def _get_or_create_industry(self, cursor, industry_name):
        cursor.execute(
            "SELECT industry_l2_id FROM dim_industry_l2 WHERE industry_l2_name = %s",
            (industry_name,)
        )
        result = cursor.fetchone()
        
        if result:
            return result[0]
        else:
            cursor.execute(
                "INSERT INTO dim_industry_l2 (industry_l2_name) VALUES (%s)",
                (industry_name,)
            )
            return cursor.lastrowid
    
    def _insert_or_update_firm(self, cursor, ticker, company_name, 
                               exchange_id, industry_id):
        cursor.execute(
            "SELECT firm_id FROM dim_firm WHERE ticker = %s",
            (ticker,)
        )
        result = cursor.fetchone()
        
        if result:
            cursor.execute("""
                UPDATE dim_firm 
                SET company_name = %s, 
                    exchange_id = %s, 
                    industry_l2_id = %s,
                    updated_at = %s
                WHERE ticker = %s
            """, (company_name, exchange_id, industry_id, datetime.now(), ticker))
            print(f"  Updated: {ticker} - {company_name}")
            return 'updated'
        else:
            cursor.execute("""
                INSERT INTO dim_firm 
                (ticker, company_name, exchange_id, industry_l2_id, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (ticker, company_name, exchange_id, industry_id, 
                  datetime.now(), datetime.now()))
            print(f"  Imported: {ticker} - {company_name}")
            return 'inserted'
    
    def close(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("\nDatabase connection closed")


def main():
    if len(sys.argv) < 2:
        print("Usage: python import_firms.py <excel_file>")
        sys.exit(1)
    
    excel_file = sys.argv[1]

    importer = FirmImporter(**DB_CONFIG)
    try:
        importer.import_from_excel(excel_file)
    finally:
        importer.close()


if __name__ == "__main__":
    main()