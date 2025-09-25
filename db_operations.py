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
        
        # 股票列表 table - 存储全部股票
        stock_list_table = """
        CREATE TABLE IF NOT EXISTS stock_list (
            code VARCHAR(20) PRIMARY KEY,
            name VARCHAR(100),
            market VARCHAR(10),
            industry VARCHAR(100),  -- 新增：行业
            pe FLOAT,  -- 新增：市盈率
            pb FLOAT,  -- 新增：市净率
            total_market_cap FLOAT,  -- 新增：总市值
            last_updated DATETIME
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        
        # 自选股票 table - 存储用户自选股票
        favorite_stocks_table = """
        CREATE TABLE IF NOT EXISTS favorite_stocks (
            id INT AUTO_INCREMENT PRIMARY KEY,
            code VARCHAR(20) NOT NULL,
            added_time DATETIME NOT NULL,
            notes TEXT,  -- 备注
            FOREIGN KEY (code) REFERENCES stock_list(code),
            UNIQUE KEY unique_favorite (code)
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
            amount FLOAT,  -- 新增：成交额
            frequency VARCHAR(10),
            UNIQUE KEY unique_record (code, time, frequency),
            FOREIGN KEY (code) REFERENCES stock_list(code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        
        # 新增：股票技术指标表
        stock_indicators_table = """
        CREATE TABLE IF NOT EXISTS stock_indicators (
            id INT AUTO_INCREMENT PRIMARY KEY,
            code VARCHAR(20),
            time DATETIME,
            ma5 FLOAT,     -- 5日均线
            ma10 FLOAT,    -- 10日均线
            ma20 FLOAT,    -- 20日均线
            ma60 FLOAT,    -- 60日均线
            macd FLOAT,    -- MACD
            macd_diff FLOAT, -- MACD差离值
            macd_dea FLOAT,  -- MACD信号线
            rsi FLOAT,     -- RSI相对强弱指数
            kdj_k FLOAT,   -- KDJ-K值
            kdj_d FLOAT,   -- KDJ-D值
            kdj_j FLOAT,   -- KDJ-J值
            frequency VARCHAR(10),
            UNIQUE KEY unique_indicator (code, time, frequency),
            FOREIGN KEY (code) REFERENCES stock_list(code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        
        try:
            cursor.execute(stock_list_table)
            cursor.execute(favorite_stocks_table)
            cursor.execute(stock_history_table)
            cursor.execute(stock_indicators_table)
            self.connection.commit()
            print("数据表创建成功")
        except mysql.connector.Error as err:
            print(f"创建表时出错: {err}")
        finally:
            cursor.close()
    
    # 股票列表相关操作
    def insert_stock_list(self, code, name, market, industry=None, pe=None, pb=None, total_market_cap=None):
        """插入或更新股票基本信息"""
        if not self.connection:
            self.connect()
            
        cursor = self.connection.cursor()
        try:
            query = """
            INSERT INTO stock_list 
            (code, name, market, industry, pe, pb, total_market_cap, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            market = VALUES(market),
            industry = VALUES(industry),
            pe = VALUES(pe),
            pb = VALUES(pb),
            total_market_cap = VALUES(total_market_cap),
            last_updated = VALUES(last_updated)
            """
            cursor.execute(query, (code, name, market, industry, pe, pb, total_market_cap, datetime.now()))
            self.connection.commit()
        except mysql.connector.Error as err:
            print(f"插入股票列表出错: {err}")
        finally:
            cursor.close()
    
    def get_all_stocks(self, page=1, page_size=20):
        """分页获取全部股票列表"""
        if not self.connection:
            self.connect()
            
        offset = (page - 1) * page_size
        cursor = self.connection.cursor(dictionary=True)
        try:
            # 获取总数
            cursor.execute("SELECT COUNT(*) as total FROM stock_list")
            total = cursor.fetchone()['total']
            
            # 获取分页数据
            cursor.execute("""
                SELECT * FROM stock_list 
                ORDER BY code 
                LIMIT %s OFFSET %s
            """, (page_size, offset))
            stocks = cursor.fetchall()
            
            return {
                'stocks': stocks,
                'total': total,
                'page': page,
                'page_size': page_size,
                'total_pages': (total + page_size - 1) // page_size
            }
        except mysql.connector.Error as err:
            print(f"获取全部股票出错: {err}")
            return {'stocks': [], 'total': 0, 'page': page, 'page_size': page_size, 'total_pages': 0}
        finally:
            cursor.close()
    
    # 自选股票相关操作
    def add_favorite(self, code, notes=""):
        """添加股票到自选列表"""
        if not self.connection:
            self.connect()
            
        cursor = self.connection.cursor()
        try:
            query = """
            INSERT INTO favorite_stocks (code, added_time, notes)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
            added_time = VALUES(added_time),
            notes = VALUES(notes)
            """
            cursor.execute(query, (code, datetime.now(), notes))
            self.connection.commit()
            return True
        except mysql.connector.Error as err:
            print(f"添加自选股票出错: {err}")
            return False
        finally:
            cursor.close()
    
    def remove_favorite(self, code):
        """从自选列表移除股票"""
        if not self.connection:
            self.connect()
            
        cursor = self.connection.cursor()
        try:
            query = "DELETE FROM favorite_stocks WHERE code = %s"
            cursor.execute(query, (code,))
            self.connection.commit()
            return cursor.rowcount > 0
        except mysql.connector.Error as err:
            print(f"移除自选股票出错: {err}")
            return False
        finally:
            cursor.close()
    
    def get_favorite_stocks(self):
        """获取所有自选股票"""
        if not self.connection:
            self.connect()
            
        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute("""
                SELECT s.*, f.added_time, f.notes 
                FROM favorite_stocks f
                JOIN stock_list s ON f.code = s.code
                ORDER BY f.added_time DESC
            """)
            return cursor.fetchall()
        except mysql.connector.Error as err:
            print(f"获取自选股票出错: {err}")
            return []
        finally:
            cursor.close()
    
    def is_favorite(self, code):
        """检查股票是否在自选列表中"""
        if not self.connection:
            self.connect()
            
        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute("SELECT * FROM favorite_stocks WHERE code = %s", (code,))
            return cursor.fetchone() is not None
        except mysql.connector.Error as err:
            print(f"检查自选股票出错: {err}")
            return False
        finally:
            cursor.close()
    
    # 历史数据相关操作
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
                    row.get('open'),
                    row.get('high'),
                    row.get('low'),
                    row.get('close'),
                    row.get('volume'),
                    row.get('amount'),  # 新增成交额
                    frequency
                ))
            
            # 批量插入
            query = """
            INSERT IGNORE INTO stock_history 
            (code, time, open, high, low, close, volume, amount, frequency)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(query, data)
            self.connection.commit()
            print(f"插入 {cursor.rowcount} 条数据到 {code} 的 {frequency} 历史记录")
        except mysql.connector.Error as err:
            print(f"插入历史数据出错: {err}")
        finally:
            cursor.close()
    
    # 新增：获取股票历史数据方法
    def get_stock_history(self, code, frequency, limit=100):
        """从数据库获取股票历史数据"""
        if not self.connection:
            self.connect()
            
        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute("""
                SELECT time, open, high, low, close, volume, amount 
                FROM stock_history 
                WHERE code = %s AND frequency = %s
                ORDER BY time DESC
                LIMIT %s
            """, (code, frequency, limit))
            
            data = cursor.fetchall()
            if not data:
                return None
                
            # 转换为DataFrame并按时间排序
            df = pd.DataFrame(data)
            df['time'] = pd.to_datetime(df['time'])
            df = df.set_index('time')
            df = df.sort_index()  # 按时间升序排列
            
            return df
        except mysql.connector.Error as err:
            print(f"获取股票历史数据出错: {err}")
            return None
        finally:
            cursor.close()
    
    # 技术指标相关操作
    def insert_indicators(self, code, df, frequency):
        if not self.connection:
            self.connect()
        cursor = self.connection.cursor()
        try:
            data = []
            for index, row in df.iterrows():
                # 关键：将NaN替换为None（MySQL会识别为NULL）
                def safe_null(value):
                    return None if pd.isna(value) else value
                
                data.append((
                    code,
                    index,
                    safe_null(row.get('ma5')),    # 处理NaN
                    safe_null(row.get('ma10')),   # 处理NaN
                    safe_null(row.get('ma20')),   # 处理NaN
                    safe_null(row.get('ma60')),   # 处理NaN
                    safe_null(row.get('macd')),   # 处理NaN
                    safe_null(row.get('macd_diff')),
                    safe_null(row.get('macd_dea')),
                    safe_null(row.get('rsi')),
                    safe_null(row.get('kdj_k')),
                    safe_null(row.get('kdj_d')),
                    safe_null(row.get('kdj_j')),
                    frequency
                ))
            # 插入SQL（不变）
            query = """INSERT IGNORE INTO stock_indicators 
                    (code, time, ma5, ma10, ma20, ma60, macd, macd_diff, macd_dea,
                    rsi, kdj_k, kdj_d, kdj_j, frequency)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            cursor.executemany(query, data)
            self.connection.commit()
        except Exception as err:
            print(f"插入技术指标出错: {err}")
        finally:
            cursor.close()
    
    def close(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            print("数据库连接已关闭")
