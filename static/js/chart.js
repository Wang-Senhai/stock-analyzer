// 绘制K线图
function drawKline(data, containerId) {
    const ctx = document.getElementById(containerId).getContext('2d');
    
    // 销毁已存在的图表
    if (window.klineChart) {
        window.klineChart.destroy();
    }
    
    // 准备数据
    const labels = data.map(item => item.time);
    const openData = data.map(item => item.open);
    const highData = data.map(item => item.high);
    const lowData = data.map(item => item.low);
    const closeData = data.map(item => item.close);
    
    // 计算涨跌幅颜色
    const colors = data.map((item, index) => {
        if (index === 0) return item.open >= item.close ? 'red' : 'green';
        return item.close >= data[index-1].close ? 'red' : 'green';
    });
    
    // 创建K线图
    window.klineChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: '开盘价',
                data: openData,
                type: 'line',
                borderColor: 'blue',
                backgroundColor: 'transparent',
                borderWidth: 1,
                yAxisID: 'y'
            }, {
                label: 'K线',
                data: closeData,
                backgroundColor: colors,
                borderColor: colors,
                borderWidth: 1,
                yAxisID: 'y',
                barPercentage: 0.6
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    grid: {
                        display: false
                    }
                },
                y: {
                    position: 'right',
                    title: {
                        display: true,
                        text: '价格'
                    }
                }
            },
            plugins: {
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const index = context.dataIndex;
                            return [
                                `时间: ${data[index].time}`,
                                `开盘: ${data[index].open}`,
                                `最高: ${data[index].high}`,
                                `最低: ${data[index].low}`,
                                `收盘: ${data[index].close}`,
                                `成交量: ${data[index].volume}`
                            ];
                        }
                    }
                }
            }
        }
    });
}