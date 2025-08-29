import mysql.connector
from mysql.connector import errorcode
import pandas as pd
from datetime import datetime
from config import DB_CONFIG

class StockDatabase:
    def __init__(self):
        self.connection = None
        self.connect()
        self.create_tables()
    
    def connect(self):
        """连接到MySQL数据库"""
        try:
            self.connection = mysql.connector.connect(**DB_CONFIG)
            print("数据库连接成功")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("用户名或密码错误")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("数据库不存在，正在创建...")
                # 创建数据库
                temp_conn = mysql.connector.connect(
                    host=DB_CONFIG['host'],
                    user=DB_CONFIG['user'],
                    password=DB_CONFIG['password'],
                    port=DB_CONFIG['port']
                )
                cursor = temp_conn.cursor()
                cursor.execute(f"CREATE DATABASE {DB_CONFIG['database']}")
                temp_conn.close()
                self.connect()  # 重新连接
            else:
                print(err)
    
    def create_tables(self):
        """创建必要的数据库表"""
        if not self.connection:
            self.connect()
            
        cursor = self.connection.cursor()
        
        # 股票列表 table
        stock_list_table = """
        CREATE TABLE IF NOT EXISTS stock_list (
            code VARCHAR(20) PRIMARY KEY,
            name VARCHAR(100),
            market VARCHAR(10),
            last_updated DATETIME
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        
        # 股票历史数据 table
        stock_history_table = """
        CREATE TABLE IF NOT EXISTS stock_history (
            id INT AUTO_INCREMENT PRIMARY KEY,
            code VARCHAR(20),
            time DATETIME,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume FLOAT,
            frequency VARCHAR(10),
            UNIQUE KEY unique_record (code, time, frequency),
            FOREIGN KEY (code) REFERENCES stock_list(code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        
        try:
            cursor.execute(stock_list_table)
            cursor.execute(stock_history_table)
            self.connection.commit()
            print("数据表创建成功")
        except mysql.connector.Error as err:
            print(f"创建表时出错: {err}")
        finally:
            cursor.close()
    
    def insert_stock_list(self, code, name, market):
        """插入股票基本信息"""
        if not self.connection:
            self.connect()
            
        cursor = self.connection.cursor()
        try:
            query = """
            INSERT INTO stock_list (code, name, market, last_updated)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            market = VALUES(market),
            last_updated = VALUES(last_updated)
            """
            cursor.execute(query, (code, name, market, datetime.now()))
            self.connection.commit()
        except mysql.connector.Error as err:
            print(f"插入股票列表出错: {err}")
        finally:
            cursor.close()
    
    def insert_history_data(self, code, df, frequency):
        """插入股票历史数据"""
        if not self.connection:
            self.connect()
            
        cursor = self.connection.cursor()
        try:
            # 准备插入数据
            data = []
            for index, row in df.iterrows():
                data.append((
                    code,
                    index,
                    row['open'],
                    row['high'],
                    row['low'],
                    row['close'],
                    row['volume'],
                    frequency
                ))
            
            # 批量插入
            query = """
            INSERT IGNORE INTO stock_history 
            (code, time, open, high, low, close, volume, frequency)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(query, data)
            self.connection.commit()
            print(f"插入 {cursor.rowcount} 条数据到 {code} 的 {frequency} 历史记录")
        except mysql.connector.Error as err:
            print(f"插入历史数据出错: {err}")
        finally:
            cursor.close()
    
    def get_stock_list(self):
        """获取所有股票列表"""
        if not self.connection:
            self.connect()
            
        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute("SELECT * FROM stock_list")
            return cursor.fetchall()
        except mysql.connector.Error as err:
            print(f"获取股票列表出错: {err}")
            return []
        finally:
            cursor.close()
    
    def get_stock_history(self, code, frequency, start_date=None, end_date=None, limit=100):
        """获取股票历史数据"""
        if not self.connection:
            self.connect()
            
        cursor = self.connection.cursor(dictionary=True)
        try:
            query = """
            SELECT time, open, high, low, close, volume 
            FROM stock_history 
            WHERE code = %s AND frequency = %s
            """
            params = [code, frequency]
            
            if start_date:
                query += " AND time >= %s"
                params.append(start_date)
            
            if end_date:
                query += " AND time <= %s"
                params.append(end_date)
                
            query += " ORDER BY time DESC LIMIT %s"
            params.append(limit)
            
            cursor.execute(query, params)
            result = cursor.fetchall()
            
            # 转换为DataFrame
            if result:
                df = pd.DataFrame(result)
                df['time'] = pd.to_datetime(df['time'])
                df.set_index('time', inplace=True)
                return df.sort_index()
            return None
        except mysql.connector.Error as err:
            print(f"获取历史数据出错: {err}")
            return None
        finally:
            cursor.close()
    
    def filter_stocks(self, conditions):
        """根据条件筛选股票"""
        if not self.connection:
            self.connect()
            
        cursor = self.connection.cursor(dictionary=True)
        try:
            # 基础查询，获取最新价格
            query = """
            SELECT h.code, s.name, h.close, h.time
            FROM stock_history h
            JOIN stock_list s ON h.code = s.code
            WHERE h.frequency = '1d'
            AND h.time = (
                SELECT MAX(time) 
                FROM stock_history 
                WHERE code = h.code AND frequency = '1d'
            )
            """
            
            # 添加筛选条件
            params = []
            if conditions.get('min_price'):
                query += " AND h.close >= %s"
                params.append(conditions['min_price'])
            
            if conditions.get('max_price'):
                query += " AND h.close <= %s"
                params.append(conditions['max_price'])
                
            # 可以根据需要添加更多筛选条件
            
            cursor.execute(query, params)
            return cursor.fetchall()
        except mysql.connector.Error as err:
            print(f"筛选股票出错: {err}")
            return []
        finally:
            cursor.close()
    
    def close(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            print("数据库连接已关闭")