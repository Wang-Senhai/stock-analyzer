import glob  
import tushare as ts  
import pandas as pd  
import os  
import time  
import sys  
import threading  
from multiprocessing import Pool  
from datetime import datetime  
from dotenv import load_dotenv  
  
# 加载.env环境变量  
load_dotenv()  
TUSHARE_TOKEN = os.getenv('TUSHARE_TOKEN')  
if not TUSHARE_TOKEN:  
    raise ValueError('请在.env文件中设置TUSHARE_TOKEN')  
  
# 全局频率控制变量  
api_lock = threading.Lock()  
last_api_call = 0
  
def fetch_and_save_single_stock(args):  
    """  
    拉取单只股票的历史数据并返回 DataFrame。  
    """  
    global last_api_call  
    code, start_date, end_date, token = args
    req_frequency = 90/60  # 每次请求间隔至少1.5秒  
      
    try:  
        # 全局频率控制，确保API调用间隔至少1.5秒  
        with api_lock:  
            current_time = time.time()  
            time_since_last = current_time - last_api_call  
            if time_since_last < req_frequency:  
                time.sleep(req_frequency - time_since_last)  
            last_api_call = time.time()  
          
        ts.set_token(token)  
        pro = ts.pro_api()  
  
        # 获取股票数据  
        data_daily = pro.daily(ts_code=code, start_date=start_date, end_date=end_date)
        data_basic = pro.daily_basic(ts_code=code, start_date=start_date, end_date=end_date)
        data_basic = data_basic.drop(columns=['close'])
          
        data = pd.merge(data_daily, data_basic, on=['ts_code', 'trade_date'], how='left')
        
  
        # 检查数据是否为空  
        if data is not None and not data.empty:  
            data['ts_code'] = code  
            return data  
        else:  
            return None  
    except Exception as e:    
        print(f"拉取股票 {code} 失败: {str(e)}")  
        return str(e)  
  
def fetch_and_save_stock_data_parallel(stock_codes, start_date, end_date, token, output_file, num_processes):  
    """  
    使用多进程拉取股票数据并保存为单个 CSV 文件。  
    """  
    # 为每只股票构建参数元组  
    args_list = [(code, start_date, end_date, token) for code in stock_codes] 
    args_list_retry = [] 
      
    with Pool(num_processes) as pool:  
        total_stocks = len(stock_codes)  
        completed = 0  
        failed = 0  
        successful = 0  
        start_time = time.time()  
  
        all_data = []  
          
        # 使用 imap_unordered 来获得各个进程返回的数据  
        for data in pool.imap_unordered(fetch_and_save_single_stock, args_list):  
            if isinstance(data, pd.DataFrame):  
                all_data.append(data)  
                successful += 1  
            else:  
                if completed < len(args_list):  
                    print(f"股票 {args_list[completed][0]} 拉取失败: {data}") 
                    args_list_retry.append(args_list[completed]) 
                failed += 1  
  
            completed += 1  
              
            # 计算当前进度百分比  
            percent_complete = (completed / total_stocks) * 100  
            elapsed_time = time.time() - start_time  
              
            # 每次更新进度时，使用 '\r' 让光标回到行首并更新所有信息  
            current_stock = args_list[completed-1][0] if completed <= len(args_list) else "未知"  
            sys.stdout.write(f"\r拉取进度：{completed}/{total_stocks} ({percent_complete:.1f}%)，成功：{successful}，失败：{failed}，当前拉取：{current_stock}，已耗时：{elapsed_time:.2f} 秒, API频率: {successful*2/elapsed_time:.2f} 次/秒")  
            sys.stdout.flush()  
  
        # 输出最终换行  
        print()  

        # 对失败的股票进行三次重试
        for attempt in range(3):
            if not args_list_retry:
                break  # 如果没有失败的股票，跳出循环
            print(f"\n开始第 {attempt + 1} 次重试，共 {len(args_list_retry)} 只股票")
            current_retry_list = args_list_retry
            args_list_retry = []  # 重置失败列表
            for data in pool.imap_unordered(fetch_and_save_single_stock, current_retry_list):
                if isinstance(data, pd.DataFrame):
                    all_data.append(data)
                    successful += 1
                else:
                    if completed < len(current_retry_list):
                        print(f"股票 {current_retry_list[completed][0]} 重试失败: {data}")
                        args_list_retry.append(current_retry_list[completed])
                    failed += 1

                completed += 1

                # 更新进度显示
                percent_complete = (completed / total_stocks) * 100
                elapsed_time = time.time() - start_time
                current_stock = current_retry_list[completed-1][0] if completed <= len(current_retry_list) else "未知"
                sys.stdout.write(f"\r拉取进度：{completed}/{total_stocks} ({percent_complete:.1f}%)，成功：{successful}，失败：{failed}，当前拉取：{current_stock}，已耗时：{elapsed_time:.2f} 秒, API频率: {successful*2/elapsed_time:.2f} 次/秒")
                sys.stdout.flush()
            print()  # 每次重试结束后换行
        # 打印失败列表
        if args_list_retry:
            print(f"\n以下股票拉取失败，建议检查代码或稍后重试：")
            for args in args_list_retry:
                print(f"股票代码: {args[0]}")
        # 合并所有数据  
        if all_data:  
            final_data = pd.concat(all_data, ignore_index=True)  
            output_file_with_date = os.path.join(output_file, f"merged_stocks_data_{end_date}.csv")  
            final_data.to_csv(output_file_with_date, index=False)  
            print(f"所有数据已保存到 {output_file_with_date}")  
        else:  
            print("没有数据可以保存。")  
  
        total_elapsed = time.time() - start_time  
        print(f"\n最终结果：成功 {successful}，失败 {failed}，总共 {total_stocks} 只股票。总耗时：{total_elapsed:.2f} 秒")  
  
if __name__ == "__main__":  
    directory = './data/'  
    file_pattern = os.path.join(directory, "基础数据_预处理*.csv")  
    files = glob.glob(file_pattern)  
    if not files:  
        print("没有找到符合条件的文件")  
        sys.exit(1)  
    else:  
        latest_file = max(files, key=lambda x: os.path.basename(x).split('_')[-1].replace('.csv', ''))  
        stock_list = pd.read_csv(latest_file)  
        print(f"读取的文件是: {latest_file}")  
      
    stock_codes = stock_list['ts_code'].values      
    selected_token = TUSHARE_TOKEN  
    start_date = '20200101'  
    end_date = datetime.today().strftime('%Y%m%d')  
    output_file = './data'  
    num_processes = 2  # 单进程避免频率限制  
  
    print(f"准备拉取 {len(stock_codes)} 只股票的数据，预计耗时约 {len(stock_codes)  / 90:.1f} 分钟")  
      
    # 开始拉取并保存数据  
    fetch_and_save_stock_data_parallel(stock_codes, start_date, end_date, selected_token, output_file, num_processes)