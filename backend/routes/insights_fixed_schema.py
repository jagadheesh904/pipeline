from flask import Blueprint, jsonify
import logging
from config.databricks_config_fixed import db_config
from databricks import sql
import pandas as pd

insights_bp = Blueprint('insights', __name__)

def execute_query(query):
    """Execute SQL query on Databricks and return results"""
    try:
        if not db_config.is_configured():
            return {"error": "Databricks not configured"}
            
        with sql.connect(
            server_hostname=db_config.server_hostname,
            http_path=db_config.http_path,
            access_token=db_config.access_token
        ) as connection:
            with connection.cursor() as cursor:
                # Use the correct schema
                cursor.execute(f"USE SCHEMA {db_config.schema}")
                cursor.execute(query)
                result = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                # Convert to list of dictionaries
                results_list = []
                for row in result:
                    results_list.append(dict(zip(columns, row)))
                
                return results_list
                
    except Exception as e:
        logging.error(f"Query execution failed: {str(e)}")
        return {"error": str(e)}

@insights_bp.route('/top-products', methods=['GET'])
def get_top_products():
    """Get top 10 products by quantity sold"""
    try:
        query = """
        SELECT 
            dp.Description, 
            SUM(fs.quantity) as total_quantity_sold
        FROM fact_sales fs
        JOIN dim_product dp ON fs.StockCode = dp.StockCode 
        GROUP BY dp.Description
        ORDER BY total_quantity_sold DESC 
        LIMIT 10
        """
        
        results = execute_query(query)
        
        if "error" in results:
            logging.error(f"Top products query failed: {results['error']}")
            return jsonify({"status": "error", "message": results["error"]}), 500
            
        logging.info(f"Top products query returned {len(results)} results")
        return jsonify({
            "status": "success",
            "data": results,
            "insight": "Top 10 Best-Selling Products"
        })
        
    except Exception as e:
        logging.error(f"Failed to get top products: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@insights_bp.route('/sales-by-country', methods=['GET'])
def get_sales_by_country():
    """Get sales revenue by country"""
    try:
        query = """
        SELECT 
            dc.Country, 
            SUM(fs.total_price) as total_sales_revenue
        FROM fact_sales fs
        JOIN dim_customer dc ON fs.CustomerID = dc.CustomerID
        GROUP BY dc.Country
        ORDER BY total_sales_revenue DESC
        LIMIT 10
        """
        
        results = execute_query(query)
        
        if "error" in results:
            logging.error(f"Sales by country query failed: {results['error']}")
            return jsonify({"status": "error", "message": results["error"]}), 500
            
        logging.info(f"Sales by country query returned {len(results)} results")
        return jsonify({
            "status": "success", 
            "data": results,
            "insight": "Top 10 Countries by Sales Revenue"
        })
        
    except Exception as e:
        logging.error(f"Failed to get sales by country: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@insights_bp.route('/recent-sales', methods=['GET'])
def get_recent_sales():
    """Get recent sales trends"""
    try:
        query = """
        SELECT 
            dd.date_key,
            dd.year,
            dd.month, 
            dd.day,
            SUM(fs.total_price) as daily_revenue,
            SUM(fs.quantity) as daily_quantity
        FROM fact_sales fs
        JOIN dim_date dd ON fs.date_key = dd.date_key
        GROUP BY dd.date_key, dd.year, dd.month, dd.day
        ORDER BY dd.date_key DESC
        LIMIT 30
        """
        
        results = execute_query(query)
        
        if "error" in results:
            logging.error(f"Recent sales query failed: {results['error']}")
            return jsonify({"status": "error", "message": results["error"]}), 500
            
        return jsonify({
            "status": "success",
            "data": results,
            "insight": "Recent Sales Trends (Last 30 Days)"
        })
        
    except Exception as e:
        logging.error(f"Failed to get recent sales: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@insights_bp.route('/check-tables', methods=['GET'])
def check_tables():
    """Debug endpoint to check if tables exist and have data"""
    try:
        # Check if tables exist and have data
        tables_to_check = ['fact_sales', 'dim_product', 'dim_customer', 'dim_date']
        table_status = {}
        
        with sql.connect(
            server_hostname=db_config.server_hostname,
            http_path=db_config.http_path,
            access_token=db_config.access_token
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"USE SCHEMA {db_config.schema}")
                
                for table in tables_to_check:
                    try:
                        # Check if table exists and get row count
                        cursor.execute(f"SELECT COUNT(*) as row_count FROM {table}")
                        count_result = cursor.fetchone()
                        table_status[table] = {
                            "exists": True,
                            "row_count": count_result[0] if count_result else 0
                        }
                    except Exception as e:
                        table_status[table] = {
                            "exists": False,
                            "error": str(e)
                        }
        
        return jsonify({
            "status": "success",
            "schema": db_config.schema,
            "tables": table_status
        })
        
    except Exception as e:
        logging.error(f"Table check failed: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500
