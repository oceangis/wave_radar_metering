/**
 * API 接口封装
 * REST API Client for Wave Monitoring System
 */

class API {
    constructor(baseUrl = '') {
        this.baseUrl = baseUrl;
        this.defaultTimeout = 10000;  // 10秒超时
    }

    /**
     * 通用请求方法（带超时）
     * @private
     */
    async request(endpoint, options = {}) {
        const url = `${this.baseUrl}${endpoint}`;
        const timeout = options.timeout || this.defaultTimeout;

        const defaultOptions = {
            headers: {
                'Content-Type': 'application/json',
            },
        };

        // 移除自定义的timeout选项，因为fetch不支持
        const { timeout: _, ...restOptions } = options;
        const config = { ...defaultOptions, ...restOptions };

        // 创建AbortController用于超时控制
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), timeout);

        try {
            const response = await fetch(url, {
                ...config,
                signal: controller.signal
            });

            clearTimeout(timeoutId);

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const data = await response.json();
            return data;

        } catch (error) {
            clearTimeout(timeoutId);

            if (error.name === 'AbortError') {
                console.warn(`API Request timeout: ${endpoint}`);
                throw new Error(`Request timeout after ${timeout}ms`);
            }

            console.error(`API Request failed: ${endpoint}`, error);
            throw error;
        }
    }

    /**
     * GET 请求
     */
    async get(endpoint, params = {}) {
        const queryString = new URLSearchParams(params).toString();
        const url = queryString ? `${endpoint}?${queryString}` : endpoint;

        return this.request(url, {
            method: 'GET',
        });
    }

    /**
     * POST 请求
     */
    async post(endpoint, data = {}) {
        return this.request(endpoint, {
            method: 'POST',
            body: JSON.stringify(data),
        });
    }

    /**
     * PUT 请求
     */
    async put(endpoint, data = {}) {
        return this.request(endpoint, {
            method: 'PUT',
            body: JSON.stringify(data),
        });
    }

    /**
     * DELETE 请求
     */
    async delete(endpoint) {
        return this.request(endpoint, {
            method: 'DELETE',
        });
    }

    // ==================== 具体API方法 ====================

    /**
     * 获取最新数据
     * @returns {Promise<Object>}
     */
    async getLatest() {
        return this.get('/api/latest');
    }

    /**
     * 获取原始数据历史
     * @param {Object} params - { hours, limit }
     * @returns {Promise<Object>}
     */
    async getRawHistory(params = {}) {
        const defaultParams = {
            hours: 1,
            limit: 1000,
        };
        return this.get('/api/history/raw', { ...defaultParams, ...params });
    }

    /**
     * 获取分析结果历史
     * @param {Object} params - { days, limit }
     * @returns {Promise<Object>}
     */
    async getAnalysisHistory(params = {}) {
        const defaultParams = {
            days: 1,
            limit: 100,
        };
        return this.get('/api/history/analysis', { ...defaultParams, ...params });
    }

    /**
     * 获取统计信息
     * @returns {Promise<Object>}
     */
    async getStatistics() {
        return this.get('/api/statistics');
    }

    /**
     * 获取系统配置
     * @returns {Promise<Object>}
     */
    async getConfig() {
        return this.get('/api/config');
    }

    /**
     * 更新系统配置
     * @param {Object} config - 配置对象
     * @returns {Promise<Object>}
     */
    async updateConfig(config) {
        return this.post('/api/config', config);
    }

    /**
     * 获取系统状态
     * @returns {Promise<Object>}
     */
    async getSystemStatus() {
        return this.get('/api/system/status');
    }

    /**
     * 发送系统命令
     * @param {Object} command - 命令对象
     * @returns {Promise<Object>}
     */
    async sendSystemCommand(command) {
        return this.post('/api/system/command', command);
    }

    /**
     * 获取雷达历史数据（自定义时间范围）
     * @param {Object} params - { start, end, radar_id }
     * @returns {Promise<Object>}
     */
    async getCustomHistory(params) {
        return this.get('/api/history', params);
    }
}

// 创建全局API实例
window.api = new API();
