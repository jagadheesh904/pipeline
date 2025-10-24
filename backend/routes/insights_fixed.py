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
        # Fixed query - using SUM(quantity) instead of non-existent total_quantity_sold
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
        # Fixed query - using SUM(total_price) and proper joins
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
        # Fixed query - using date_key for grouping
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
            return jsonify({"status": "error", "message": results["error"]}), 500
            
        return jsonify({
            "status": "success",
            "data": results,
            "insight": "Recent Sales Trends (Last 30 Days)"
        })
        
    except Exception as e:
        logging.error(f"Failed to get recent sales: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@insights_bp.route('/customer-insights', methods=['GET'])
def get_customer_insights():
    """Get customer spending insights"""
    try:
        query = """
        SELECT 
            dc.CustomerID,
            dc.Country,
            COUNT(DISTINCT fs.invoice_id) as total_orders,
            SUM(fs.total_price) as total_spent,
            AVG(fs.total_price) as avg_order_value
        FROM fact_sales fs
        JOIN dim_customer dc ON fs.CustomerID = dc.CustomerID
        GROUP BY dc.CustomerID, dc.Country
        ORDER BY total_spent DESC
        LIMIT 15
        """
        
        results = execute_query(query)
        
        if "error" in results:
            return jsonify({"status": "error", "message": results["error"]}), 500
            
        return jsonify({
            "status": "success",
            "data": results,
            "insight": "Top Customers by Spending"
        })
        
    except Exception as e:
        logging.error(f"Failed to get customer insights: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@insights_bp.route('/product-performance', methods=['GET'])
def get_product_performance():
    """Get product revenue performance"""
    try:
        query = """
        SELECT 
            dp.StockCode,
            dp.Description,
            COUNT(DISTINCT fs.invoice_id) as times_ordered,
            SUM(fs.quantity) as total_quantity,
            SUM(fs.total_price) as total_revenue,
            AVG(fs.unit_price) as avg_unit_price
        FROM fact_sales fs
        JOIN dim_product dp ON fs.StockCode = dp.StockCode
        GROUP BY dp.StockCode, dp.Description
        ORDER BY total_revenue DESC
        LIMIT 15
        """
        
        results = execute_query(query)
        
        if "error" in results:
            return jsonify({"status": "error", "message": results["error"]}), 500
            
        return jsonify({
            "status": "success",
            "data": results,
            "insight": "Product Performance by Revenue"
        })
        
    except Exception as e:
        logging.error(f"Failed to get product performance: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500
