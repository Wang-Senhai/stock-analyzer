// 通用工具函数
function formatNumber(num, decimals = 2) {
    if (isNaN(num)) return '0.00';
    return num.toFixed(decimals);
}

// 页面加载完成后的通用初始化
document.addEventListener('DOMContentLoaded', function() {
    // 移除错误的drawKline()调用（这是导致第一个错误的原因）
    // 保留其他通用逻辑，如导航栏高亮、表单提交等
    
    // 导航栏当前页面高亮
    highlightCurrentNav();
    
    // 股票选择下拉框联动
    initStockSelect();
});

// 导航栏当前页面高亮
function highlightCurrentNav() {
    const currentPath = window.location.pathname;
    const navLinks = document.querySelectorAll('nav a');
    
    navLinks.forEach(link => {
        if (link.getAttribute('href') === currentPath) {
            link.classList.add('active');
        }
    });
}

// 股票选择下拉框初始化
function initStockSelect() {
    const stockSelect = document.getElementById('stock-select');
    if (stockSelect) {
        stockSelect.addEventListener('change', function() {
            // 自动提交表单或跳转
            const form = this.closest('form');
            if (form) form.submit();
        });
    }
}

// 通用的图表工具函数（如需）
function formatDate(dateString) {
    const date = new Date(dateString);
    return date.toLocaleDateString('zh-CN');
}
