from flask import Blueprint, jsonify, request
import requests
import json
import time
import logging
from config.databricks_config_fixed import db_config
from databricks import sql

pipeline_bp = Blueprint('pipeline', __name__)

class DatabricksJobRunner:
    def __init__(self, config):
        self.config = config
        self.base_url = f"https://{config.server_hostname}/api/2.0"
        self.headers = {
            "Authorization": f"Bearer {config.access_token}",
            "Content-Type": "application/json"
        }

    def run_notebook(self, notebook_path, timeout=600):
        """Execute a Databricks notebook via Jobs API"""
        try:
            # Create a one-time job to run the notebook
            job_payload = {
                "run_name": f"Insights-Notebook-{int(time.time())}",
                "tasks": [
                    {
                        "task_key": "run-insights-notebook",
                        "notebook_task": {
                            "notebook_path": notebook_path,
                            "base_parameters": {}
                        },
                        "timeout_seconds": timeout
                    }
                ]
            }
            
            logging.info(f"Submitting job for notebook: {notebook_path}")
            response = requests.post(
                f"{self.base_url}/jobs/runs/submit",
                headers=self.headers,
                json=job_payload,
                timeout=30
            )
            
            if response.status_code == 200:
                run_data = response.json()
                run_id = run_data['run_id']
                logging.info(f"Notebook execution started. Run ID: {run_id}")
                return run_id
            else:
                logging.error(f"Failed to start notebook: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logging.error(f"Error running notebook: {str(e)}")
            return None

    def get_run_status(self, run_id):
        """Get the status of a job run"""
        try:
            response = requests.get(
                f"{self.base_url}/jobs/runs/get?run_id={run_id}",
                headers=self.headers,
                timeout=30
            )
            
            if response.status_code == 200:
                run_data = response.json()
                state = run_data.get('state', {})
                logging.info(f"Run {run_id} status: {state.get('life_cycle_state')} - {state.get('result_state')}")
                return run_data
            else:
                logging.error(f"Failed to get run status: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logging.error(f"Error getting run status: {str(e)}")
            return None

    def query_insights_from_tables(self):
        """Query the actual tables for insights"""
        try:
            logging.info(f"Querying insights from schema: {self.config.schema}")
            
            connection = sql.connect(
                server_hostname=self.config.server_hostname,
                http_path=self.config.http_path,
                access_token=self.config.access_token
            )
            
            cursor = connection.cursor()
            
            # Use the correct schema
            cursor.execute(f"USE SCHEMA {self.config.schema}")
            
            # Get top products
            cursor.execute("""
                SELECT dp.Description, SUM(fs.quantity) as total_quantity_sold
                FROM fact_sales fs
                JOIN dim_product dp ON fs.StockCode = dp.StockCode
                GROUP BY dp.Description
                ORDER BY total_quantity_sold DESC
                LIMIT 10
            """)
            top_products = cursor.fetchall()
            top_products_list = [{"Description": row[0], "total_quantity_sold": int(row[1])} for row in top_products]
            
            # Get sales by country
            cursor.execute("""
                SELECT dc.Country, SUM(fs.total_price) as total_sales_revenue
                FROM fact_sales fs
                JOIN dim_customer dc ON fs.CustomerID = dc.CustomerID
                GROUP BY dc.Country
                ORDER BY total_sales_revenue DESC
                LIMIT 10
            """)
            sales_by_country = cursor.fetchall()
            sales_by_country_list = [{"Country": row[0], "total_sales_revenue": float(row[1])} for row in sales_by_country]
            
            cursor.close()
            connection.close()
            
            logging.info(f"Retrieved {len(top_products_list)} products and {len(sales_by_country_list)} countries")
            
            return {
                "top_products": top_products_list,
                "sales_by_country": sales_by_country_list,
                "source": "direct_table_query"
            }
            
        except Exception as e:
            logging.error(f"Error querying insights from tables: {str(e)}")
            return None

# Create job runner instance
job_runner = DatabricksJobRunner(db_config)

@pipeline_bp.route('/trigger-insights-notebook', methods=['POST'])
def trigger_insights_notebook():
    """
    Trigger the insights notebook in Databricks
    """
    try:
        logging.info("Starting Insights Notebook execution...")
        
        # Your insights notebook path in Databricks workspace
        notebook_path = "/Workspace/Users/jagadheeshnaidu1@gmail.com/online_ratail_project_pipeline/05_Insights_and_Queries"
        
        # Execute the insights notebook
        run_id = job_runner.run_notebook(notebook_path)
        
        if run_id:
            return jsonify({
                "status": "success",
                "message": "Insights notebook execution started",
                "run_id": run_id,
                "notebook_path": notebook_path
            })
        else:
            return jsonify({
                "status": "error", 
                "message": "Failed to start insights notebook"
            }), 500
            
    except Exception as e:
        logging.error(f"Insights notebook trigger failed: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@pipeline_bp.route('/get-insights-output/<run_id>', methods=['GET'])
def get_insights_output(run_id):
    """
    Get insights after notebook execution
    """
    try:
        # First check if the run is completed
        run_status = job_runner.get_run_status(run_id)
        
        if not run_status:
            return jsonify({
                "status": "error",
                "message": "Could not retrieve run status"
            }), 500
        
        state = run_status.get('state', {})
        life_cycle_state = state.get('life_cycle_state')
        result_state = state.get('result_state')
        
        # If job is still running, return current status
        if life_cycle_state in ['PENDING', 'RUNNING']:
            return jsonify({
                "status": "running",
                "life_cycle_state": life_cycle_state,
                "result_state": result_state,
                "message": f"Job is still {life_cycle_state.lower()}"
            })
        
        # If job completed (successfully or not)
        if life_cycle_state == 'TERMINATED':
            if result_state == 'SUCCESS':
                # Query tables directly for insights
                logging.info("Notebook completed successfully, querying tables for insights...")
                insights_data = job_runner.query_insights_from_tables()
                
                if insights_data:
                    return jsonify({
                        "status": "success", 
                        "run_id": run_id,
                        "insights": insights_data,
                        "source": "table_query_after_notebook"
                    })
                else:
                    return jsonify({
                        "status": "error",
                        "message": "Could not retrieve insights data from tables"
                    }), 500
                    
            else:
                # Job failed
                return jsonify({
                    "status": "error",
                    "message": f"Notebook execution failed: {state.get('state_message', 'Unknown error')}"
                }), 500
        else:
            # Unexpected state
            return jsonify({
                "status": "error", 
                "message": f"Unexpected job state: {life_cycle_state}"
            }), 500
            
    except Exception as e:
        logging.error(f"Failed to get insights output: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@pipeline_bp.route('/refresh-insights', methods=['POST'])
def refresh_insights():
    """
    Refresh insights by querying the tables directly (without running notebook)
    """
    try:
        logging.info("Refreshing insights from tables...")
        
        insights_data = job_runner.query_insights_from_tables()
        
        if insights_data:
            return jsonify({
                "status": "success",
                "insights": insights_data,
                "source": "table_refresh"
            })
        else:
            return jsonify({
                "status": "error",
                "message": "Failed to refresh insights from tables"
            }), 500
            
    except Exception as e:
        logging.error(f"Failed to refresh insights: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

# Keep existing pipeline functions
@pipeline_bp.route('/trigger-real', methods=['POST'])
def trigger_real_pipeline():
    """Trigger the main pipeline notebook"""
    try:
        data = request.get_json()
        pipeline_type = data.get('pipeline_type', 'full')
        
        logging.info("Starting REAL Databricks pipeline execution...")
        
        notebook_path = "/Workspace/Users/jagadheeshnaidu1@gmail.com/online_ratail_project_pipeline/04_RUN_PIPELINE"
        
        run_id = job_runner.run_notebook(notebook_path)
        
        if run_id:
            return jsonify({
                "status": "success",
                "message": "Databricks pipeline execution started",
                "run_id": run_id,
                "pipeline_type": pipeline_type,
                "notebook_path": notebook_path
            })
        else:
            return jsonify({
                "status": "error", 
                "message": "Failed to start Databricks job"
            }), 500
            
    except Exception as e:
        logging.error(f"Pipeline trigger failed: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@pipeline_bp.route('/status-real/<run_id>', methods=['GET'])
def get_real_pipeline_status(run_id):
    """Get status of any notebook run"""
    try:
        run_status = job_runner.get_run_status(run_id)
        
        if run_status:
            state = run_status.get('state', {})
            return jsonify({
                "status": "success",
                "run_id": run_id,
                "life_cycle_state": state.get('life_cycle_state'),
                "result_state": state.get('result_state'),
                "state_message": state.get('state_message'),
                "start_time": run_status.get('start_time'),
                "end_time": run_status.get('end_time')
            })
        else:
            return jsonify({
                "status": "error",
                "message": "Could not retrieve run status"
            }), 500
            
    except Exception as e:
        logging.error(f"Status check failed: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500
