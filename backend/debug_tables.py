#!/usr/bin/env python3
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.databricks_config import db_config
from databricks import sql
import logging

logging.basicConfig(level=logging.INFO)

def debug_tables():
    print("🔍 Debugging Databricks Tables Connection...")
    
    if not db_config.is_configured():
        print("❌ Databricks not configured properly")
        return
    
    try:
        # Test connection and list tables
        connection = sql.connect(
            server_hostname=db_config.server_hostname,
            http_path=db_config.http_path,
            access_token=db_config.access_token
        )
        
        cursor = connection.cursor()
        
        # List all tables in the current schema
        print("\n📋 Listing all tables:")
        cursor.tables(schema_name=db_config.schema)
        tables = cursor.fetchall()
        for table in tables:
            print(f"  - {table[2]}")
        
        # Check if our specific tables exist
        target_tables = ['fact_sales', 'dim_product', 'dim_customer', 'dim_date']
        print(f"\n🔎 Checking for target tables in schema '{db_config.schema}':")
        
        for table_name in target_tables:
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            result = cursor.fetchall()
            if result:
                print(f"  ✅ {table_name}: EXISTS")
                # Show sample data
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 2")
                sample = cursor.fetchall()
                print(f"     Sample columns: {[desc[0] for desc in cursor.description]}")
                print(f"     Sample rows: {sample}")
            else:
                print(f"  ❌ {table_name}: NOT FOUND")
        
        # Test the actual queries we're using
        print(f"\n🧪 Testing insights queries:")
        
        # Test top products query
        try:
            cursor.execute("""
                SELECT dp.Description, SUM(fs.quantity) as total_quantity_sold
                FROM fact_sales fs
                JOIN dim_product dp ON fs.StockCode = dp.StockCode
                GROUP BY dp.Description
                ORDER BY total_quantity_sold DESC
                LIMIT 5
            """)
            top_products = cursor.fetchall()
            print(f"  Top products query: {len(top_products)} results")
            for product in top_products:
                print(f"    - {product[0]}: {product[1]}")
        except Exception as e:
            print(f"  ❌ Top products query failed: {e}")
        
        # Test sales by country query
        try:
            cursor.execute("""
                SELECT dc.Country, SUM(fs.total_price) as total_sales_revenue
                FROM fact_sales fs
                JOIN dim_customer dc ON fs.CustomerID = dc.CustomerID
                GROUP BY dc.Country
                ORDER BY total_sales_revenue DESC
                LIMIT 5
            """)
            sales_by_country = cursor.fetchall()
            print(f"  Sales by country query: {len(sales_by_country)} results")
            for country in sales_by_country:
                print(f"    - {country[0]}: {country[1]}")
        except Exception as e:
            print(f"  ❌ Sales by country query failed: {e}")
        
        cursor.close()
        connection.close()
        
    except Exception as e:
        print(f"❌ Connection error: {e}")

if __name__ == "__main__":
    debug_tables()
