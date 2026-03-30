"""
create_snapshot.py
Create data snapshots and manage data sources

Usage:
    # Setup data source
    python etl/create_snapshot.py --setup 

    # Create single snapshot
    python etl/create_snapshot.py <source_name> <fiscal_year> 
    
    # Create batch snapshots (4 sources x 5 years = 20 snapshots with auto-incrementing snapshot_id)
    python etl/create_snapshot.py --batch-default 2020 2024
"""

import mysql.connector
from mysql.connector import Error
import sys
from datetime import datetime
from db_config import DB_CONFIG

class SnapshotCreator:
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
    
    def create_snapshot(self, source_name, fiscal_year, snapshot_date=None, version_tag=None):
        try:
            cursor = self.connection.cursor()
            
            cursor.execute("SELECT source_id FROM dim_data_source WHERE source_name = %s", (source_name,))
            result = cursor.fetchone()
            if not result:
                print(f"Error: Source '{source_name}' not found. Use --setup first.")
                cursor.close()
                return None
            
            source_id = result[0]
            if snapshot_date:
                snapshot_date = datetime.strptime(snapshot_date, "%Y-%m-%d").date()
            else:
                snapshot_date = datetime.now().date()

            if version_tag is None:
                cursor.execute(
                    "SELECT COUNT(*) FROM fact_data_snapshot WHERE source_id = %s AND fiscal_year = %s", (source_id, fiscal_year)
                )
                count = cursor.fetchone()[0] + 1
                version_tag = f"v{count}"
            
            cursor.execute("""
                INSERT INTO fact_data_snapshot 
                (source_id, fiscal_year, snapshot_date, version_tag, created_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (source_id, fiscal_year, snapshot_date, version_tag, datetime.now()))
            
            snapshot_id = cursor.lastrowid
            self.connection.commit()
            
            print(f"Snapshot created: ID={snapshot_id}, Source={source_name}, Year={fiscal_year}, Tag={version_tag}")
            cursor.close()
            return snapshot_id
            
        except Exception as e:
            print(f"Error: {e}")
            if self.connection:
                self.connection.rollback()
            raise
    
    def setup_source(self, source_name, source_type, provider, note):
        try:
            cursor = self.connection.cursor()

            cursor.execute("SELECT source_id FROM dim_data_source WHERE source_name = %s", (source_name,))
            result = cursor.fetchone()
            
            if result:
                cursor.execute("""
                    UPDATE dim_data_source 
                    SET source_type = %s, provider = %s, note = %s
                    WHERE source_id = %s
                """, (source_type, provider, note, result[0]))
                source_id = result[0]
            else:
                cursor.execute("""
                    INSERT INTO dim_data_source (source_name, source_type, provider, note)
                    VALUES (%s, %s, %s, %s)
                """, (source_name, source_type, provider, note))
                source_id = cursor.lastrowid
            
            self.connection.commit()
            print(f"\n Source updated: ID={source_id}, Name={source_name}, Type={source_type}, Provider={provider}")
            cursor.close()
            
        except Exception as e:
            print(f"Error: {e}")
            if self.connection:
                self.connection.rollback()
            raise
    
    def create_batch_snapshots(self, sources, start_year, end_year):
        snapshot_ids = []
        try:
            cursor = self.connection.cursor()
            
            # Get the latest snapshot_id to start from
            cursor.execute("SELECT MAX(snapshot_id) FROM fact_data_snapshot")
            max_id_result = cursor.fetchone()
            next_snapshot_id = (max_id_result[0] or 0) + 1
            
            # Verify all sources exist
            for source_name in sources:
                cursor.execute("SELECT source_id FROM dim_data_source WHERE source_name = %s", (source_name,))
                if not cursor.fetchone():
                    print(f"Error: Source '{source_name}' not found. Use --setup first.")
                    cursor.close()
                    return []
            
            print(f"\nCreating snapshots for {len(sources)} sources, years {start_year}-{end_year}...")
            print(f"Total snapshots to create: {len(sources) * (end_year - start_year + 1)}")
            print(f"Starting snapshot_id: {next_snapshot_id}\n")
            
            snapshot_date = datetime.now().date()
            
            # Create snapshots in order: source1-year1, source1-year2, ..., source2-year1, etc.
            for source_name in sources:
                cursor.execute("SELECT source_id FROM dim_data_source WHERE source_name = %s", (source_name,))
                source_id = cursor.fetchone()[0]
                
                for year in range(start_year, end_year + 1):
                    # Check if snapshot already exists
                    cursor.execute("""
                        SELECT COUNT(*) FROM fact_data_snapshot 
                        WHERE source_id = %s AND fiscal_year = %s
                    """, (source_id, year))
                    count = cursor.fetchone()[0]
                    version_tag = f"v{count + 1}"
                    
                    cursor.execute("""
                        INSERT INTO fact_data_snapshot 
                        (source_id, fiscal_year, snapshot_date, version_tag, created_at)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (source_id, year, snapshot_date, version_tag, datetime.now()))
                    
                    snapshot_id = cursor.lastrowid
                    snapshot_ids.append(snapshot_id)
                    print(f"Created: snapshot_id={snapshot_id:2d}, source={source_name:20s}, year={year}, tag={version_tag}")
            
            self.connection.commit()
            cursor.close()
            print(f"\n✓ Successfully created {len(snapshot_ids)} snapshots!")
            
        except Exception as e:
            print(f"Error: {e}")
            if self.connection:
                self.connection.rollback()
            raise
        
        return snapshot_ids
    
    def close(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("Database connection closed")


def main():
    creator = SnapshotCreator(**DB_CONFIG)
    try:
        if len(sys.argv) < 2:
            print(__doc__)
            return
    
        if sys.argv[1] == "--setup":
            creator.setup_source("vnstock_v2", "python_pkg", "Thinh Vu (VNstock Team)", "A specialized Python library for Vietnam stock market, providing real-time price feeds, corporate finanical ratios, and historical OHLC data.")
            creator.setup_source("BCTC_Audited", "financial_statement", "Company/Exchange", "Audited financial statements")
            creator.setup_source("Vietstock", "market", "Vietstock", "Market fields (price, shares, dividend, EPS)")
            creator.setup_source("AnnualReport", "text_report", "Company", "Annual report / disclosures for ownership, innovation & health")
        elif sys.argv[1] == "--batch-default":
            # Create batch snapshots for 3 default sources
            if len(sys.argv) < 4:
                print("Usage: python create_snapshot.py --batch-default <start_year> <end_year>")
                return
            start_year = int(sys.argv[2])
            end_year = int(sys.argv[3])
            default_sources = ["vnstock_v2", "BCTC_Audited", "AnnualReport"]
            creator.create_batch_snapshots(default_sources, start_year, end_year)
        else:
            if len(sys.argv) < 3:
                print("Usage: python create_snapshot.py <source_name> <fiscal_year> [snapshot_date] [version_tag]")
                return
            source_name = sys.argv[1]
            fiscal_year = int(sys.argv[2])
            snapshot_date = sys.argv[3] if len(sys.argv) > 3 else None
            version_tag = sys.argv[4] if len(sys.argv) > 4 else None
            creator.create_snapshot(source_name, fiscal_year, snapshot_date, version_tag)
    finally:
        creator.close()


if __name__ == "__main__":
    main()
