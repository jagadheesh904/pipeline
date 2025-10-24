import os
from dotenv import load_dotenv

load_dotenv()

class DatabricksConfig:
    def __init__(self):
        self.server_hostname = os.getenv('DATABRICKS_SERVER_HOSTNAME')
        self.http_path = os.getenv('DATABRICKS_HTTP_PATH') 
        self.access_token = os.getenv('DATABRICKS_ACCESS_TOKEN')
        self.catalog = os.getenv('DATABRICKS_CATALOG', 'main')
        self.schema = os.getenv('DATABRICKS_SCHEMA', 'default')
    
    def is_configured(self):
        return all([self.server_hostname, self.http_path, self.access_token])

# Global config instance
db_config = DatabricksConfig()
