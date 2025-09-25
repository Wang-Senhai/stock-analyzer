from flask import Flask, render_template, request, jsonify
import pandas as pd
from datetime import datetime
from Ashare import get_price, get_stock_basic, calculate_indicators
from db_operations import StockDatabase
import config

# 初始化Flask应用
app = Flask(__name__)
app.config.from_object(config)

# 初始化数据库连接
db = StockDatabase()

# 首页路由
@app.route('/')
def index():
    """首页 - 显示股票分析平台主页面"""
    # 获取热门股票列表（前20名）
    stocks_data = db.get_all_stocks(page=1, page_size=20)
    return render_template('index.html', 
                          stocks=stocks_data['stocks'],
                          total_stocks=stocks_data['total'])

# 全部股票列表页面
@app.route('/all-stocks')
def all_stocks():
    """全部股票列表页面，支持分页和搜索"""
    page = int(request.args.get('page', 1))
    stocks_data = db.get_all_stocks(page=page, page_size=30)
    
    # 获取每个股票是否在自选列表中
    for stock in stocks_data['stocks']:
        stock['is_favorite'] = db.is_favorite(stock['code'])
    
    return render_template('all_stocks.html', 
                          stocks_data=stocks_data,
                          current_page=page)

# 自选股票页面
@app.route('/favorites')
def favorites():
    """自选股票页面，显示用户添加的自选股票"""
    favorite_stocks = db.get_favorite_stocks()
    
    # 获取自选股票的最新价格和涨跌幅
    for stock in favorite_stocks:
        try:
            # 获取最新价格
            df = db.get_stock_history(stock['code'], '1d', limit=1)
            if df is not None and not df.empty:
                stock['latest_price'] = round(float(df.iloc[-1]['close']), 2)
                stock['change'] = round(
                    (stock['latest_price'] - float(df.iloc[-1]['open'])) / float(df.iloc[-1]['open']) * 100, 
                    2
                )
            else:
                stock['latest_price'] = 'N/A'
                stock['change'] = 0
        except Exception as e:
            print(f"获取 {stock['code']} 最新价格出错: {e}")
            stock['latest_price'] = 'N/A'
            stock['change'] = 0
    
    return render_template('favorites.html', 
                          stocks=favorite_stocks)

# 实时数据页面
@app.route('/realtime')
def realtime():
    """股票实时数据页面"""
    stock_code = request.args.get('code', 'sh000001')
    
    # 获取股票信息
    stock_info = None
    stocks_data = db.get_all_stocks()['stocks']
    for stock in stocks_data:
        if stock['code'] == stock_code:
            stock_info = stock
            break
    
    # 获取最新数据
    try:
        df = get_price(stock_code, count=1, frequency='1d')
        latest_data = {
            'code': stock_code,
            'name': stock_info['name'] if stock_info else stock_code,
            'price': round(float(df.iloc[-1]['close']), 2),
            'open': round(float(df.iloc[-1]['open']), 2),
            'high': round(float(df.iloc[-1]['high']), 2),
            'low': round(float(df.iloc[-1]['low']), 2),
            'volume': int(df.iloc[-1]['volume']),
            'time': df.index[-1].strftime('%Y-%m-%d %H:%M:%S')
        }
        # 计算涨跌幅
        latest_data['change'] = round(
            (latest_data['price'] - latest_data['open']) / latest_data['open'] * 100, 
            2
        )
    except Exception as e:
        return render_template('realtime.html', 
                              error=str(e), 
                              stocks=stocks_data,
                              selected_code=stock_code)
    
    return render_template('realtime.html', 
                          data=latest_data, 
                          stocks=stocks_data,
                          selected_code=stock_code)

# 历史数据页面
@app.route('/history')
def history():
    """股票历史数据页面"""
    stock_code = request.args.get('code', 'sh000001')
    frequency = request.args.get('frequency', '1d')
    count = int(request.args.get('count', 30))
    
    # 获取股票列表用于下拉选择
    stocks_data = db.get_all_stocks()['stocks']
    
    # 获取历史数据
    try:
        # 先从数据库获取
        df = db.get_stock_history(stock_code, frequency, limit=count)
        
        # 数据库没有则从接口获取并保存
        if df is None or df.empty:
            df = get_price(stock_code, count=count, frequency=frequency)
            db.insert_history_data(stock_code, df, frequency)
        
        # 转换数据格式
        history_data = []
        for index, row in df.iterrows():
            history_data.append({
                'time': index.strftime('%Y-%m-%d %H:%M:%S'),
                'open': round(float(row['open']), 2),
                'high': round(float(row['high']), 2),
                'low': round(float(row['low']), 2),
                'close': round(float(row['close']), 2),
                'volume': int(row['volume']),
                'amount': round(float(row.get('amount', 0)), 2)
            })
    except Exception as e:
        return render_template('history.html', 
                              error=str(e), 
                              stocks=stocks_data,
                              selected_code=stock_code,
                              selected_freq=frequency)
    
    # 获取股票名称
    stock_name = stock_code
    for stock in stocks_data:
        if stock['code'] == stock_code:
            stock_name = f"{stock['name']} ({stock_code})"
            break
    
    return render_template('history.html', 
                          data=history_data, 
                          stocks=stocks_data, 
                          selected_code=stock_code,
                          selected_freq=frequency,
                          stock_name=stock_name)

# K线图页面
@app.route('/kline')
def kline():
    """高级K线图分析页面"""
    stock_code = request.args.get('code', 'sh000001')
    frequency = request.args.get('frequency', '1d')
    count = int(request.args.get('count', 60))
    
    # 获取股票列表用于下拉选择
    stocks_data = db.get_all_stocks()['stocks']
    
    # 获取股票名称
    stock_name = stock_code
    for stock in stocks_data:
        if stock['code'] == stock_code:
            stock_name = stock['name']
            break
    
    # 获取K线数据和技术指标
    try:
        # 获取价格数据
        df = get_price(stock_code, count=count, frequency=frequency)
        # 计算技术指标
        df = calculate_indicators(df)
        # 保存到数据库
        db.insert_history_data(stock_code, df, frequency)
        db.insert_indicators(stock_code, df, frequency)
        
        # 转换为前端所需格式
        kline_data = []
        for index, row in df.iterrows():
            kline_data.append({
                'time': index.strftime('%Y-%m-%d %H:%M:%S'),
                'open': round(float(row['open']), 2),
                'high': round(float(row['high']), 2),
                'low': round(float(row['low']), 2),
                'close': round(float(row['close']), 2),
                'volume': int(row['volume']),
                'amount': round(float(row.get('amount', 0)), 2),
                'ma5': round(float(row.get('ma5', 0)), 2) if not pd.isna(row.get('ma5')) else None,
                'ma10': round(float(row.get('ma10', 0)), 2) if not pd.isna(row.get('ma10')) else None,
                'ma20': round(float(row.get('ma20', 0)), 2) if not pd.isna(row.get('ma20')) else None,
                'ma60': round(float(row.get('ma60', 0)), 2) if not pd.isna(row.get('ma60')) else None,
                'macd': round(float(row.get('macd', 0)), 4) if not pd.isna(row.get('macd')) else None,
                'macd_diff': round(float(row.get('macd_diff', 0)), 4) if not pd.isna(row.get('macd_diff')) else None,
                'macd_dea': round(float(row.get('macd_dea', 0)), 4) if not pd.isna(row.get('macd_dea')) else None,
                'rsi': round(float(row.get('rsi', 0)), 2) if not pd.isna(row.get('rsi')) else None,
            })
    except Exception as e:
        return render_template('kline.html', 
                              error=str(e), 
                              stocks=stocks_data,
                              selected_code=stock_code)
    
    return render_template('kline.html', 
                          data=kline_data, 
                          stock_code=stock_code,
                          stock_name=stock_name,
                          selected_freq=frequency,
                          stocks=stocks_data)

# 股票筛选页面
@app.route('/filter')
def filter_stocks():
    """股票筛选页面"""
    # 获取筛选参数
    min_pe = request.args.get('min_pe', None)
    max_pe = request.args.get('max_pe', None)
    min_pb = request.args.get('min_pb', None)
    max_pb = request.args.get('max_pb', None)
    industry = request.args.get('industry', None)
    
    # 获取符合条件的股票（实际实现中应在数据库层实现筛选）
    all_stocks = db.get_all_stocks(page=1, page_size=1000)['stocks']
    filtered_stocks = []
    
    for stock in all_stocks:
        # 筛选逻辑
        match = True
        
        if min_pe and stock.get('pe') and float(stock['pe']) < float(min_pe):
            match = False
        if max_pe and stock.get('pe') and float(stock['pe']) > float(max_pe):
            match = False
        if min_pb and stock.get('pb') and float(stock['pb']) < float(min_pb):
            match = False
        if max_pb and stock.get('pb') and float(stock['pb']) > float(max_pb):
            match = False
        if industry and stock.get('industry') != industry:
            match = False
            
        if match:
            filtered_stocks.append(stock)
    
    # 获取所有行业用于筛选下拉框
    industries = list(set(stock.get('industry', '未知') for stock in all_stocks))
    industries.sort()
    
    return render_template('filter.html', 
                          stocks=filtered_stocks,
                          industries=industries,
                          filters={
                              'min_pe': min_pe,
                              'max_pe': max_pe,
                              'min_pb': min_pb,
                              'max_pb': max_pb,
                              'industry': industry
                          })

# 自选股票API - 添加
@app.route('/api/favorite/add/<code>')
def add_favorite(code):
    """添加股票到自选列表"""
    result = db.add_favorite(code)
    return jsonify({'status': 'success' if result else 'error'})

# 自选股票API - 移除
@app.route('/api/favorite/remove/<code>')
def remove_favorite(code):
    """从自选列表移除股票"""
    result = db.remove_favorite(code)
    return jsonify({'status': 'success' if result else 'error'})

# 更新股票列表API
@app.route('/update-stock-list')
def update_stock_list():
    """更新全部股票列表数据"""
    try:
        # 从Ashare获取股票基本信息
        stocks = get_stock_basic()
        for stock in stocks:
            db.insert_stock_list(
                code=stock['code'],
                name=stock['name'],
                market=stock['market'],
                industry=stock.get('industry'),
                pe=stock.get('pe'),
                pb=stock.get('pb'),
                total_market_cap=stock.get('total_market_cap')
            )
        return jsonify({'status': 'success', 'message': f'更新了 {len(stocks)} 只股票信息'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

# 应用入口
if __name__ == '__main__':
    # 启动Flask应用，允许外部访问
    app.run(host='0.0.0.0', port=5000, debug=True)
