// 实时数据更新
function updateRealtimeData(code) {
    fetch(`/api/realtime/${code}`)
        .then(response => response.json())
        .then(data => {
            if (data.error) {
                document.getElementById('realtime-error').textContent = data.error;
                return;
            }
            
            document.getElementById('realtime-price').textContent = data.close.toFixed(2);
            document.getElementById('realtime-open').textContent = data.open.toFixed(2);
            document.getElementById('realtime-high').textContent = data.high.toFixed(2);
            document.getElementById('realtime-low').textContent = data.low.toFixed(2);
            document.getElementById('realtime-volume').textContent = data.volume.toLocaleString();
            document.getElementById('realtime-time').textContent = data.time;
            
            // 更新涨跌状态
            const priceElement = document.getElementById('realtime-price');
            if (data.close > data.open) {
                priceElement.style.color = 'red';
            } else if (data.close < data.open) {
                priceElement.style.color = 'green';
            } else {
                priceElement.style.color = 'black';
            }
        })
        .catch(error => {
            console.error('获取实时数据失败:', error);
        });
}

// 页面加载完成后执行
document.addEventListener('DOMContentLoaded', function() {
    // 如果是实时数据页面，设置定时刷新
    if (document.getElementById('realtime-container')) {
        const code = document.getElementById('stock-code').value;
        // 立即更新一次
        updateRealtimeData(code);
        // 每30秒更新一次
        setInterval(() => updateRealtimeData(code), 30000);
    }
    
    // 如果是K线图页面，绘制K线
    if (document.getElementById('kline-data')) {
        const klineData = JSON.parse(document.getElementById('kline-data').textContent);
        drawKline(klineData, 'kline-chart');
    }
    
    // 股票代码选择变化时提交表单
    const stockSelect = document.getElementById('stock-select');
    if (stockSelect) {
        stockSelect.addEventListener('change', function() {
            this.form.submit();
        });
    }
    
    // 周期选择变化时提交表单
    const freqSelect = document.getElementById('frequency-select');
    if (freqSelect) {
        freqSelect.addEventListener('change', function() {
            this.form.submit();
        });
    }
    
    // 更新数据按钮
    const updateButtons = document.querySelectorAll('.update-btn');
    updateButtons.forEach(button => {
        button.addEventListener('click', function(e) {
            e.preventDefault();
            const code = this.getAttribute('data-code');
            const buttonText = this.textContent;
            this.textContent = '更新中...';
            this.disabled = true;
            
            fetch(`/update/${code}`)
                .then(response => response.json())
                .then(data => {
                    alert(data.message);
                    this.textContent = buttonText;
                    this.disabled = false;
                    // 刷新页面
                    location.reload();
                })
                .catch(error => {
                    console.error('更新数据失败:', error);
                    this.textContent = buttonText;
                    this.disabled = false;
                });
        });
    });
});