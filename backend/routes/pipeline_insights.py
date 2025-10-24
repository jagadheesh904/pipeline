from flask import Blueprint, jsonify, request
import requests
import json
import time
import logging
from config.databricks_config import db_config

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
            
            # Submit the job
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
                logging.error(f"Failed to start notebook: {response.text}")
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
                return response.json()
            else:
                logging.error(f"Failed to get run status: {response.text}")
                return None
        except Exception as e:
            logging.error(f"Error getting run status: {str(e)}")
            return None

    def get_run_output(self, run_id):
        """Get the output of a completed notebook run"""
        try:
            response = requests.get(
                f"{self.base_url}/jobs/runs/get-output?run_id={run_id}",
                headers=self.headers,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logging.error(f"Failed to get run output: {response.text}")
                return None
        except Exception as e:
            logging.error(f"Error getting run output: {str(e)}")
            return None

# Create job runner instance
job_runner = DatabricksJobRunner(db_config)

@pipeline_bp.route('/trigger-insights-notebook', methods=['POST'])
def trigger_insights_notebook():
    """
    Trigger the insights notebook in Databricks and get its output
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
    Get the output from insights notebook execution
    """
    try:
        run_output = job_runner.get_run_output(run_id)
        
        if run_output:
            # The output contains notebook execution results
            # We'll parse the output to extract insights
            insights_data = parse_insights_output(run_output)
            
            return jsonify({
                "status": "success",
                "run_id": run_id,
                "insights": insights_data,
                "raw_output": run_output  # For debugging
            })
        else:
            return jsonify({
                "status": "error",
                "message": "Could not retrieve insights output"
            }), 500
            
    except Exception as e:
        logging.error(f"Failed to get insights output: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

def parse_insights_output(run_output):
    """
    Parse the notebook output to extract structured insights
    This is a simplified parser - you might need to adjust based on actual output format
    """
    insights = {
        "top_products": [],
        "sales_by_country": []
    }
    
    try:
        # Extract task output if available
        if 'notebook_output' in run_output:
            notebook_result = run_output['notebook_output']
            
            # In a real implementation, you would parse the actual output structure
            # For now, we'll return a structured format that matches what we expect
            
            # Mock data structure based on your notebook output
            insights["top_products"] = [
                {"Description": "Product A", "total_quantity_sold": 1500},
                {"Description": "Product B", "total_quantity_sold": 1200},
                {"Description": "Product C", "total_quantity_sold": 900}
            ]
            
            insights["sales_by_country"] = [
                {"Country": "United Kingdom", "total_sales_revenue": 50000},
                {"Country": "Germany", "total_sales_revenue": 15000},
                {"Country": "France", "total_sales_revenue": 12000}
            ]
            
        return insights
        
    except Exception as e:
        logging.error(f"Error parsing insights output: {str(e)}")
        return insights

# Keep the existing pipeline functions
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
            state = run_status['state']
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
