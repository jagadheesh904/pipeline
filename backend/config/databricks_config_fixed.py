import os
from dotenv import load_dotenv

load_dotenv()

class DatabricksConfig:
    def __init__(self):
        self.server_hostname = os.getenv('DATABRICKS_SERVER_HOSTNAME')
        self.http_path = os.getenv('DATABRICKS_HTTP_PATH') 
        self.access_token = os.getenv('DATABRICKS_ACCESS_TOKEN')
        self.catalog = os.getenv('DATABRICKS_CATALOG', 'main')
        
        # Use the same database name as your pipeline
        # From your 00_Setup_and_Config.py, it's "retail_dev_jaggu_db"
        self.schema = os.getenv('DATABRICKS_SCHEMA', 'retail_dev_jaggu_db')
    
    def is_configured(self):
        return all([self.server_hostname, self.http_path, self.access_token])

# Global config instance
db_config = DatabricksConfig()
