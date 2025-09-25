import pymysql
import pandas as pd
import time
from datetime import datetime
import os
import logging
import io
import sys
import random
import multiprocessing
from dotenv import load_dotenv

# 加载.env文件
load_dotenv()

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def create_database_connection(max_attempts=5, base_delay=5):
    """创建MySQL数据库连接，失败重试"""
    attempt = 0
    last_exception = None
    while attempt < max_attempts:
        try:
            conn = pymysql.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                user=os.getenv('DB_USER', 'stoker'),
                password=os.getenv('DB_PASSWORD', '!QAZ2wsx'),
                database=os.getenv('DB_NAME', 'instockdb'),
                port = int(os.getenv('DB_PORT', 3306)),
                charset='utf8mb4',
                connect_timeout=10
            )
            if attempt > 0:
                logger.info(f"成功连接到数据库，尝试次数：{attempt+1}")
            return conn
        except pymysql.err.OperationalError as e:
            attempt += 1
            last_exception = e
            delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 3)
            logger.warning(f"数据库连接失败，{delay:.2f}秒后重试 ({attempt}/{max_attempts})：{e}")
            time.sleep(delay)
    logger.error(f"无法连接到数据库，已达到最大重试次数：{max_attempts}")
    raise last_exception

def upsert_batch(args):
    batch_data, columns, db_params = args
    try:
        # 关键：将所有 NaN 替换为 None，防止 NaN 写入 MySQL
        batch_data = batch_data.where(pd.notnull(batch_data), None)
        # 用 .values.tolist() 生成插入数据
        rows = batch_data.values.tolist()
        # 再次确保所有 NaN 都是 None
        import math
        def nan_to_none(x):
            return None if isinstance(x, float) and math.isnan(x) else x
        rows = [[nan_to_none(cell) for cell in row] for row in rows]
        conn = pymysql.connect(**db_params)
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TEMPORARY TABLE IF NOT EXISTS tmp_stock_data (
                    ts_code VARCHAR(10),
                    trade_date DATE,
                    cycle VARCHAR(10),
                    open FLOAT,
                    high FLOAT,
                    low FLOAT,
                    close FLOAT,
                    pre_close FLOAT,
                    `change` FLOAT,
                    pct_chg FLOAT,
                    vol FLOAT,
                    amount FLOAT,
                    turnover_rate FLOAT,
                    turnover_rate_f FLOAT,
                    volume_ratio FLOAT,
                    pe FLOAT,
                    pe_ttm FLOAT,
                    pb FLOAT,
                    ps FLOAT,
                    ps_ttm FLOAT,
                    dv_ratio FLOAT,
                    dv_ttm FLOAT,
                    total_share FLOAT,
                    float_share FLOAT,
                    free_share FLOAT,
                    total_mv FLOAT,
                    circ_mv FLOAT,
                    PRIMARY KEY (ts_code, trade_date, cycle)
                );
            """)
            # 用 values.tolist() 生成插入数据，避免空字符串
            sql_columns = [f"`{col}`" if col == "change" else col for col in columns]
            insert_sql = f"INSERT INTO tmp_stock_data ({', '.join(sql_columns)}) VALUES ({', '.join(['%s']*len(columns))})"
            cursor.executemany(insert_sql, rows)
            upsert_sql = f"""
                INSERT INTO stock_data ({', '.join(sql_columns)})
                SELECT {', '.join(sql_columns)} FROM tmp_stock_data
                ON DUPLICATE KEY UPDATE
                    open=VALUES(open),
                    high=VALUES(high),
                    low=VALUES(low),
                    close=VALUES(close),
                    pre_close=VALUES(pre_close),
                    `change`=VALUES(`change`),
                    pct_chg=VALUES(pct_chg),
                    vol=VALUES(vol),
                    amount=VALUES(amount),
                    turnover_rate=VALUES(turnover_rate),
                    turnover_rate_f=VALUES(turnover_rate_f),
                    volume_ratio=VALUES(volume_ratio),
                    pe=VALUES(pe),
                    pe_ttm=VALUES(pe_ttm),
                    pb=VALUES(pb),
                    ps=VALUES(ps),
                    ps_ttm=VALUES(ps_ttm),
                    dv_ratio=VALUES(dv_ratio),
                    dv_ttm=VALUES(dv_ttm),
                    total_share=VALUES(total_share),
                    float_share=VALUES(float_share),
                    free_share=VALUES(free_share),
                    total_mv=VALUES(total_mv),
                    circ_mv=VALUES(circ_mv);

            """
            cursor.execute(upsert_sql)
            conn.commit()
        conn.close()
        return len(batch_data)
    except Exception as e:
        logger.error(f"批次上传失败: {e}")
        return 0

def main():
    start_time = time.time()
    today = datetime.today().strftime('%Y%m%d')
    csv_file_path = f'./data/merged_stocks_data_{today}.csv'
    try:
        data = pd.read_csv(csv_file_path)
        logger.info(f"读取CSV文件，共{len(data)}条数据")
    except Exception as e:
        logger.error(f"读取CSV文件失败: {e}")
        return
    columns = ['ts_code', 'trade_date', 'cycle', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount', 
               'turnover_rate', 'turnover_rate_f', 'volume_ratio', 'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm', 
               'dv_ratio', 'dv_ttm', 'total_share', 'float_share', 'free_share', 'total_mv', 'circ_mv']
    data = data[columns]
    data.replace('', pd.NA, inplace=True)
    # 强制所有数值型字段为 float，无法转换的变为 NaN
    float_cols = ['open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount',
                  'turnover_rate', 'turnover_rate_f', 'volume_ratio', 'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm',
                  'dv_ratio', 'dv_ttm', 'total_share', 'float_share', 'free_share', 'total_mv', 'circ_mv']
    for col in float_cols:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors='coerce')
    batch_size = 100000
    num_rows = len(data)
    num_batches = (num_rows + batch_size - 1) // batch_size
    db_params = dict(
        host=os.getenv('DB_HOST', 'localhost'),
        user=os.getenv('DB_USER', 'stoker'),
        password=os.getenv('DB_PASSWORD', '!QAZ2wsx'),
        database=os.getenv('DB_NAME', 'instockdb'),
        port = int(os.getenv('DB_PORT', 3306)),
        charset='utf8mb4',
        connect_timeout=10
    )
    conn = None
    try:
        conn = create_database_connection()
        with conn.cursor() as cursor:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS stock_data (
                ts_code VARCHAR(10) NOT NULL,
                trade_date DATE NOT NULL,
                cycle VARCHAR(10) NOT NULL,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                pre_close FLOAT,
                `change` FLOAT,
                pct_chg FLOAT,
                vol FLOAT,
                amount FLOAT,
                turnover_rate FLOAT,
                turnover_rate_f FLOAT,
                volume_ratio FLOAT,
                pe FLOAT,
                pe_ttm FLOAT,
                pb FLOAT,
                ps FLOAT,
                ps_ttm FLOAT,
                dv_ratio FLOAT,
                dv_ttm FLOAT,
                total_share FLOAT,
                float_share FLOAT,
                free_share FLOAT,
                total_mv FLOAT,
                circ_mv FLOAT,
                PRIMARY KEY (ts_code, trade_date, cycle)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
            cursor.execute(create_table_sql)
        conn.close()
        pool = multiprocessing.Pool(processes=min(4, num_batches))
        batches = [(data.iloc[i*batch_size:(i+1)*batch_size], columns, db_params) for i in range(num_batches)]
        results = []
        for i, res in enumerate(pool.imap_unordered(upsert_batch, batches), 1):
            results.append(res)
            rows_processed = sum(results)
            percent = int((rows_processed/num_rows)*100)
            progress_msg = f"数据库导入进度: {percent}% ({rows_processed}/{num_rows})"
            sys.stdout.write('\r' + progress_msg)
            sys.stdout.flush()
        pool.close()
        pool.join()
        print()
        conn = create_database_connection()
        with conn.cursor() as cursor:
            cursor.execute("ANALYZE TABLE stock_data;")
        logger.info(f"成功导入 {num_rows} 条数据！")
    except Exception as e:
        logger.error(f"数据库操作失败: {e}")
        if conn:
            conn.close()
    elapsed_time = time.time() - start_time
    logger.info(f"任务完成，总耗时: {elapsed_time:.2f} 秒")

if __name__ == "__main__":
    main()