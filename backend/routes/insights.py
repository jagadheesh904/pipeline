from flask import Blueprint, jsonify
import logging
from config.databricks_config import db_config
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
        SELECT Description, total_quantity_sold 
        FROM fact_sales 
        JOIN dim_product ON fact_sales.StockCode = dim_product.StockCode 
        ORDER BY total_quantity_sold DESC 
        LIMIT 10
        """
        
        results = execute_query(query)
        
        if "error" in results:
            return jsonify({"status": "error", "message": results["error"]}), 500
            
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
        SELECT Country, SUM(total_price) as total_sales_revenue
        FROM fact_sales
        JOIN dim_customer ON fact_sales.CustomerID = dim_customer.CustomerID
        GROUP BY Country
        ORDER BY total_sales_revenue DESC
        LIMIT 10
        """
        
        results = execute_query(query)
        
        if "error" in results:
            return jsonify({"status": "error", "message": results["error"]}), 500
            
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
            date_key,
            SUM(total_price) as daily_revenue,
            SUM(quantity) as daily_quantity
        FROM fact_sales
        GROUP BY date_key
        ORDER BY date_key DESC
        LIMIT 30
        """
        
        results = execute_query(query)
        
        if "error" in results:
            return jsonify({"status": "error", "message": results["error"]}), 500
            
        return jsonify({
            "status": "success",
            "data": results,
            "insight": "Recent Sales Trends (Last 30 Days)"
        })
        
    except Exception as e:
        logging.error(f"Failed to get recent sales: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500
