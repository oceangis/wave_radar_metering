/**
 * 工具函数库
 * Utility Functions for Wave Monitoring System
 */

const Utils = {
    /**
     * 格式化时间戳
     * @param {string|Date} timestamp - ISO时间戳或Date对象
     * @param {boolean} includeSeconds - 是否包含秒
     * @returns {string} 格式化的时间字符串
     */
    formatTimestamp(timestamp, includeSeconds = true) {
        if (!timestamp) return '--';

        const date = typeof timestamp === 'string' ? new Date(timestamp) : timestamp;

        if (isNaN(date.getTime())) return '--';

        const options = {
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
        };

        if (includeSeconds) {
            options.second = '2-digit';
        }

        return date.toLocaleString('zh-CN', options);
    },

    /**
     * 格式化相对时间（多久之前）
     * @param {string|Date} timestamp
     * @returns {string}
     */
    formatRelativeTime(timestamp) {
        if (!timestamp) return '--';

        const date = typeof timestamp === 'string' ? new Date(timestamp) : timestamp;
        const now = new Date();
        const seconds = Math.floor((now - date) / 1000);

        if (seconds < 60) return `${seconds}秒前`;
        if (seconds < 3600) return `${Math.floor(seconds / 60)}分钟前`;
        if (seconds < 86400) return `${Math.floor(seconds / 3600)}小时前`;
        return `${Math.floor(seconds / 86400)}天前`;
    },

    /**
     * 格式化数值
     * @param {number} value
     * @param {number} decimals
     * @returns {string}
     */
    formatNumber(value, decimals = 2) {
        if (value === null || value === undefined || isNaN(value)) return '--';
        return Number(value).toFixed(decimals);
    },

    /**
     * 安全获取嵌套对象属性
     * @param {object} obj
     * @param {string} path - 例如 'data.results.Hm0'
     * @param {*} defaultValue
     * @returns {*}
     */
    get(obj, path, defaultValue = null) {
        const keys = path.split('.');
        let result = obj;

        for (const key of keys) {
            if (result === null || result === undefined) {
                return defaultValue;
            }
            result = result[key];
        }

        return result !== undefined ? result : defaultValue;
    },

    /**
     * 防抖函数
     * @param {Function} func
     * @param {number} wait
     * @returns {Function}
     */
    debounce(func, wait = 300) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    },

    /**
     * 节流函数
     * @param {Function} func
     * @param {number} limit
     * @returns {Function}
     */
    throttle(func, limit = 1000) {
        let inThrottle;
        return function(...args) {
            if (!inThrottle) {
                func.apply(this, args);
                inThrottle = true;
                setTimeout(() => inThrottle = false, limit);
            }
        };
    },

    /**
     * 显示Toast提示
     * @param {string} message
     * @param {string} type - 'success', 'error', 'warning', 'info'
     * @param {number} duration - 显示时长(ms)
     */
    showToast(message, type = 'info', duration = 3000) {
        // 创建toast容器（如果不存在）
        let container = document.getElementById('toast-container');
        if (!container) {
            container = document.createElement('div');
            container.id = 'toast-container';
            container.style.cssText = `
                position: fixed;
                top: 20px;
                right: 20px;
                z-index: 10000;
                display: flex;
                flex-direction: column;
                gap: 10px;
            `;
            document.body.appendChild(container);
        }

        // 创建toast元素
        const toast = document.createElement('div');
        toast.className = `toast toast-${type}`;

        const colors = {
            success: '#10b981',
            error: '#ef4444',
            warning: '#f59e0b',
            info: '#3b82f6'
        };

        const icons = {
            success: '✓',
            error: '✕',
            warning: '⚠',
            info: 'ℹ'
        };

        toast.style.cssText = `
            background: white;
            border-left: 4px solid ${colors[type]};
            padding: 15px 20px;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            display: flex;
            align-items: center;
            gap: 10px;
            min-width: 300px;
            max-width: 500px;
            animation: slideIn 0.3s ease-out;
        `;

        toast.innerHTML = `
            <span style="
                width: 24px;
                height: 24px;
                border-radius: 50%;
                background: ${colors[type]};
                color: white;
                display: flex;
                align-items: center;
                justify-content: center;
                font-weight: bold;
                flex-shrink: 0;
            ">${icons[type]}</span>
            <span style="flex: 1; color: #333;">${message}</span>
        `;

        container.appendChild(toast);

        // 自动移除
        setTimeout(() => {
            toast.style.animation = 'slideOut 0.3s ease-out';
            setTimeout(() => toast.remove(), 300);
        }, duration);
    },

    /**
     * 下载数据为文件
     * @param {string} data - 文件内容
     * @param {string} filename - 文件名
     * @param {string} type - MIME类型
     */
    downloadFile(data, filename, type = 'text/plain') {
        const blob = new Blob([data], { type });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    },

    /**
     * 导出为CSV
     * @param {Array} data - 数据数组
     * @param {string} filename - 文件名
     */
    exportToCSV(data, filename = 'data.csv') {
        if (!data || data.length === 0) {
            this.showToast('没有数据可导出', 'warning');
            return;
        }

        // 获取表头
        const headers = Object.keys(data[0]);

        // 构建CSV内容
        let csv = headers.join(',') + '\n';

        data.forEach(row => {
            const values = headers.map(header => {
                let value = row[header];

                // 处理null/undefined
                if (value === null || value === undefined) {
                    value = '';
                }

                // 处理包含逗号或引号的值
                if (typeof value === 'string' && (value.includes(',') || value.includes('"'))) {
                    value = `"${value.replace(/"/g, '""')}"`;
                }

                return value;
            });

            csv += values.join(',') + '\n';
        });

        this.downloadFile(csv, filename, 'text/csv;charset=utf-8;');
        this.showToast('数据已导出', 'success');
    },

    /**
     * 导出为JSON
     * @param {*} data
     * @param {string} filename
     */
    exportToJSON(data, filename = 'data.json') {
        const json = JSON.stringify(data, null, 2);
        this.downloadFile(json, filename, 'application/json');
        this.showToast('数据已导出', 'success');
    },

    /**
     * 生成UUID
     * @returns {string}
     */
    generateUUID() {
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
            const r = Math.random() * 16 | 0;
            const v = c === 'x' ? r : (r & 0x3 | 0x8);
            return v.toString(16);
        });
    },

    /**
     * 深拷贝对象
     * @param {*} obj
     * @returns {*}
     */
    deepClone(obj) {
        if (obj === null || typeof obj !== 'object') return obj;
        if (obj instanceof Date) return new Date(obj);
        if (obj instanceof Array) return obj.map(item => this.deepClone(item));

        const clonedObj = {};
        for (const key in obj) {
            if (obj.hasOwnProperty(key)) {
                clonedObj[key] = this.deepClone(obj[key]);
            }
        }
        return clonedObj;
    },

    /**
     * 本地存储操作
     */
    storage: {
        set(key, value) {
            try {
                localStorage.setItem(key, JSON.stringify(value));
                return true;
            } catch (e) {
                console.error('localStorage.setItem error:', e);
                return false;
            }
        },

        get(key, defaultValue = null) {
            try {
                const item = localStorage.getItem(key);
                return item ? JSON.parse(item) : defaultValue;
            } catch (e) {
                console.error('localStorage.getItem error:', e);
                return defaultValue;
            }
        },

        remove(key) {
            try {
                localStorage.removeItem(key);
                return true;
            } catch (e) {
                console.error('localStorage.removeItem error:', e);
                return false;
            }
        },

        clear() {
            try {
                localStorage.clear();
                return true;
            } catch (e) {
                console.error('localStorage.clear error:', e);
                return false;
            }
        }
    }
};

// 添加CSS动画
const style = document.createElement('style');
style.textContent = `
    @keyframes slideIn {
        from {
            transform: translateX(100%);
            opacity: 0;
        }
        to {
            transform: translateX(0);
            opacity: 1;
        }
    }

    @keyframes slideOut {
        from {
            transform: translateX(0);
            opacity: 1;
        }
        to {
            transform: translateX(100%);
            opacity: 0;
        }
    }
`;
document.head.appendChild(style);

// 导出到全局
window.Utils = Utils;
