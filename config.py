import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 数据库配置
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', 'stock_data'),
    'port': int(os.getenv('DB_PORT', 3306))
}

# 应用配置
DEBUG = True
SECRET_KEY = 'your-secret-key-here'