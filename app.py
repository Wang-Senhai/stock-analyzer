from flask import Flask, render_template, request, jsonify
import pandas as pd
from datetime import datetime, timedelta
from Ashare import get_price
from db_operations import StockDatabase
import config

app = Flask(__name__)
app.config.from_object(config)

# 初始化数据库连接
db = StockDatabase()

# 常用股票列表（可以扩展）
STOCK_CODES = {
    'sh000001': '上证指数',
    'sz399001': '深证成指',
    'sz000858': '五 粮 液',
    'sh600519': '贵州茅台',
    'sz000002': '万 科Ａ',
    'sh601318': '中国平安'
}

# 初始化股票列表到数据库
for code, name in STOCK_CODES.items():
    market = 'SH' if code.startswith('sh') else 'SZ'
    db.insert_stock_list(code, name, market)

@app.route('/')
def index():
    """首页"""
    stocks = db.get_stock_list()
    return render_template('index.html', stocks=stocks)

@app.route('/realtime')
def realtime():
    """实时数据页面"""
    stock_code = request.args.get('code', 'sh000001')
    # 获取实时数据（最近1条）
    try:
        df = get_price(stock_code, count=1, frequency='1m')
        if not df.empty:
            data = df.iloc[-1].to_dict()
            data['time'] = df.index[-1].strftime('%Y-%m-%d %H:%M:%S')
            data['code'] = stock_code
            data['name'] = STOCK_CODES.get(stock_code, stock_code)
        else:
            data = {'error': '无法获取数据'}
    except Exception as e:
        data = {'error': str(e)}
    
    stocks = db.get_stock_list()
    return render_template('realtime.html', data=data, stocks=stocks, selected_code=stock_code)

@app.route('/api/realtime/<code>')
def api_realtime(code):
    """获取实时数据的API"""
    try:
        df = get_price(code, count=1, frequency='1m')
        if not df.empty:
            data = df.iloc[-1].to_dict()
            data['time'] = df.index[-1].strftime('%Y-%m-%d %H:%M:%S')
            return jsonify(data)
        return jsonify({'error': '无数据'})
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/history')
def history():
    """历史数据页面"""
    stock_code = request.args.get('code', 'sh000001')
    frequency = request.args.get('frequency', '1d')
    count = int(request.args.get('count', 30))
    
    # 尝试从数据库获取，没有则从API获取并保存
    df = db.get_stock_history(stock_code, frequency, limit=count)
    if df is None or df.empty:
        try:
            df = get_price(stock_code, count=count, frequency=frequency)
            # 保存到数据库
            db.insert_history_data(stock_code, df, frequency)
        except Exception as e:
            return render_template('history.html', error=str(e), stocks=db.get_stock_list())
    
    # 转换为前端可用的格式
    if not df.empty:
        history_data = []
        for index, row in df.iterrows():
            history_data.append({
                'time': index.strftime('%Y-%m-%d %H:%M:%S'),
                'open': round(float(row['open']), 2),
                'high': round(float(row['high']), 2),
                'low': round(float(row['low']), 2),
                'close': round(float(row['close']), 2),
                'volume': int(row['volume'])
            })
    else:
        history_data = []
    
    stocks = db.get_stock_list()
    return render_template('history.html', 
                          data=history_data, 
                          stocks=stocks, 
                          selected_code=stock_code,
                          selected_freq=frequency)

@app.route('/kline')
def kline():
    """K线图页面"""
    stock_code = request.args.get('code', 'sh000001')
    frequency = request.args.get('frequency', '1d')
    count = int(request.args.get('count', 60))
    
    # 获取K线数据
    try:
        df = get_price(stock_code, count=count, frequency=frequency)
        # 同时保存到数据库
        db.insert_history_data(stock_code, df, frequency)
        
        # 转换为K线图所需格式
        kline_data = []
        for index, row in df.iterrows():
            kline_data.append({
                'time': index.strftime('%Y-%m-%d %H:%M:%S'),
                'open': round(float(row['open']), 2),
                'high': round(float(row['high']), 2),
                'low': round(float(row['low']), 2),
                'close': round(float(row['close']), 2),
                'volume': int(row['volume'])
            })
    except Exception as e:
        return render_template('kline.html', error=str(e), stocks=db.get_stock_list())
    
    stocks = db.get_stock_list()
    return render_template('kline.html', 
                          data=kline_data, 
                          stock_code=stock_code,
                          stock_name=STOCK_CODES.get(stock_code, stock_code),
                          selected_freq=frequency,
                          stocks=stocks)

@app.route('/filter')
def filter_stocks():
    """股票筛选页面"""
    # 获取筛选条件
    min_price = request.args.get('min_price')
    max_price = request.args.get('max_price')
    
    conditions = {}
    if min_price and min_price.strip():
        conditions['min_price'] = float(min_price)
    if max_price and max_price.strip():
        conditions['max_price'] = float(max_price)
    
    # 执行筛选
    results = db.filter_stocks(conditions) if conditions else []
    
    return render_template('filter.html', 
                          results=results,
                          min_price=min_price or '',
                          max_price=max_price or '',
                          stocks=db.get_stock_list())

@app.route('/update/<code>')
def update_stock(code):
    """更新股票数据"""
    frequencies = ['1d', '1w', '1M', '60m', '30m', '15m', '5m']
    for freq in frequencies:
        try:
            # 对于日线及以上周期，获取更多数据
            count = 365 if freq in ['1d', '1w', '1M'] else 100
            df = get_price(code, count=count, frequency=freq)
            db.insert_history_data(code, df, freq)
        except Exception as e:
            print(f"更新 {code} {freq} 数据出错: {e}")
    
    return jsonify({'status': 'success', 'message': f'股票 {code} 数据已更新'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)