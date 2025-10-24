from flask import Flask, jsonify, request
from flask_cors import CORS
from routes.pipeline_insights import pipeline_bp
from routes.insights_fixed import insights_bp
import logging
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
CORS(app)

# Register blueprints
app.register_blueprint(pipeline_bp, url_prefix='/api/pipeline')
app.register_blueprint(insights_bp, url_prefix='/api/insights')

@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "message": "Retail Pipeline Dashboard API is running"})

@app.route('/api/databricks-config', methods=['GET'])
def get_databricks_config():
    """Check if Databricks is properly configured"""
    from config.databricks_config import db_config
    return jsonify({
        "configured": db_config.is_configured(),
        "server_hostname": db_config.server_hostname,
        "has_http_path": bool(db_config.http_path),
        "has_access_token": bool(db_config.access_token)
    })

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    app.run(debug=True, host='0.0.0.0', port=5000)
