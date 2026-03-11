/**
 * 图表管理器
 * Chart Manager using Chart.js for Wave Monitoring System
 */

class ChartManager {
    constructor() {
        this.charts = new Map();
        this.maxDataPoints = 600; // 最多保存10分钟数据 (600秒 @ 1Hz)
    }

    /**
     * 创建实时波浪时序图
     * @param {string} canvasId - Canvas元素ID
     * @param {Object} options - 配置选项
     * @returns {Chart}
     */
    createTimeSeriesChart(canvasId, options = {}) {
        const ctx = document.getElementById(canvasId);
        if (!ctx) {
            console.error(`Canvas element not found: ${canvasId}`);
            return null;
        }

        const defaultOptions = {
            title: '实时波浪数据',
            yAxisLabel: '高度 (m)',
            datasets: [
                { label: '雷达 1', color: '#3b82f6' },
                { label: '雷达 2', color: '#10b981' },
                { label: '雷达 3', color: '#f59e0b' }
            ]
        };

        const config = { ...defaultOptions, ...options };

        const chart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: [],
                datasets: config.datasets.map((ds, index) => ({
                    label: ds.label,
                    data: [],
                    borderColor: ds.color,
                    backgroundColor: ds.color + '20',
                    borderWidth: 2,
                    fill: false,
                    tension: 0.4,
                    pointRadius: 0,
                    pointHoverRadius: 4,
                }))
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: {
                    duration: 0 // 禁用动画以提高性能
                },
                interaction: {
                    mode: 'index',
                    intersect: false,
                },
                plugins: {
                    title: {
                        display: true,
                        text: config.title,
                        font: { size: 16, weight: 'bold' }
                    },
                    legend: {
                        display: true,
                        position: 'top',
                    },
                    tooltip: {
                        enabled: true,
                        mode: 'index',
                        intersect: false,
                    }
                },
                scales: {
                    x: {
                        type: 'time',
                        time: {
                            displayFormats: {
                                second: 'HH:mm:ss',
                                minute: 'HH:mm',
                            },
                            tooltipFormat: 'yyyy-MM-dd HH:mm:ss'
                        },
                        title: {
                            display: true,
                            text: '时间'
                        },
                        ticks: {
                            maxRotation: 0,
                            autoSkipPadding: 50,
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: config.yAxisLabel
                        },
                        beginAtZero: false
                    }
                }
            }
        });

        this.charts.set(canvasId, chart);
        return chart;
    }

    /**
     * 创建频谱图
     * @param {string} canvasId
     * @param {Object} options
     * @returns {Chart}
     */
    createSpectrumChart(canvasId, options = {}) {
        const ctx = document.getElementById(canvasId);
        if (!ctx) {
            console.error(`Canvas element not found: ${canvasId}`);
            return null;
        }

        const defaultOptions = {
            title: '波浪能量谱',
            xAxisLabel: '频率 (Hz)',
            yAxisLabel: '能量谱密度 (m²/Hz)'
        };

        const config = { ...defaultOptions, ...options };

        const chart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: '能量谱',
                    data: [],
                    borderColor: '#667eea',
                    backgroundColor: '#667eea40',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.4,
                    pointRadius: 0,
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    title: {
                        display: true,
                        text: config.title,
                        font: { size: 16, weight: 'bold' }
                    },
                    legend: {
                        display: false
                    }
                },
                scales: {
                    x: {
                        title: {
                            display: true,
                            text: config.xAxisLabel
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: config.yAxisLabel
                        },
                        beginAtZero: true
                    }
                }
            }
        });

        this.charts.set(canvasId, chart);
        return chart;
    }

    /**
     * 创建波浪参数历史趋势图
     * @param {string} canvasId
     * @param {Object} options
     * @returns {Chart}
     */
    createTrendChart(canvasId, options = {}) {
        const ctx = document.getElementById(canvasId);
        if (!ctx) {
            console.error(`Canvas element not found: ${canvasId}`);
            return null;
        }

        const defaultOptions = {
            title: '波浪参数趋势',
            parameter: 'hs',
            label: '有效波高 (m)',
            color: '#3b82f6'
        };

        const config = { ...defaultOptions, ...options };

        const chart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: config.label,
                    data: [],
                    borderColor: config.color,
                    backgroundColor: config.color + '20',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.4,
                    pointRadius: 3,
                    pointHoverRadius: 6,
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    title: {
                        display: true,
                        text: config.title,
                        font: { size: 16, weight: 'bold' }
                    },
                    legend: {
                        display: true,
                        position: 'top',
                    }
                },
                scales: {
                    x: {
                        type: 'time',
                        time: {
                            displayFormats: {
                                hour: 'MM-dd HH:mm',
                                day: 'MM-dd'
                            }
                        },
                        title: {
                            display: true,
                            text: '时间'
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: config.label
                        },
                        beginAtZero: false
                    }
                }
            }
        });

        this.charts.set(canvasId, chart);
        return chart;
    }

    /**
     * 更新时序图数据
     * @param {string} chartId
     * @param {Array} heights - [eta1, eta2, eta3]
     * @param {Date|string} timestamp
     */
    updateTimeSeriesChart(chartId, heights, timestamp) {
        const chart = this.charts.get(chartId);
        if (!chart) return;

        const time = timestamp instanceof Date ? timestamp : new Date(timestamp);

        // 添加新数据点
        chart.data.labels.push(time);

        heights.forEach((height, index) => {
            if (chart.data.datasets[index]) {
                chart.data.datasets[index].data.push(height);
            }
        });

        // 限制数据点数量（滑动窗口）
        if (chart.data.labels.length > this.maxDataPoints) {
            chart.data.labels.shift();
            chart.data.datasets.forEach(dataset => {
                dataset.data.shift();
            });
        }

        chart.update('none'); // 'none' mode for better performance
    }

    /**
     * 更新频谱图
     * @param {string} chartId
     * @param {Array} frequencies
     * @param {Array} spectrum
     */
    updateSpectrumChart(chartId, frequencies, spectrum) {
        const chart = this.charts.get(chartId);
        if (!chart) return;

        chart.data.labels = frequencies.map(f => f.toFixed(3));
        chart.data.datasets[0].data = spectrum;

        chart.update();
    }

    /**
     * 更新趋势图
     * @param {string} chartId
     * @param {Array} data - [{ timestamp, value }, ...]
     */
    updateTrendChart(chartId, data) {
        const chart = this.charts.get(chartId);
        if (!chart) return;

        chart.data.labels = data.map(d => new Date(d.timestamp || d.start_time));
        chart.data.datasets[0].data = data.map(d => d.value || d.hs);

        chart.update();
    }

    /**
     * 清空图表数据
     * @param {string} chartId
     */
    clearChart(chartId) {
        const chart = this.charts.get(chartId);
        if (!chart) return;

        chart.data.labels = [];
        chart.data.datasets.forEach(dataset => {
            dataset.data = [];
        });

        chart.update();
    }

    /**
     * 销毁图表
     * @param {string} chartId
     */
    destroyChart(chartId) {
        const chart = this.charts.get(chartId);
        if (chart) {
            chart.destroy();
            this.charts.delete(chartId);
        }
    }

    /**
     * 获取图表实例
     * @param {string} chartId
     * @returns {Chart|null}
     */
    getChart(chartId) {
        return this.charts.get(chartId) || null;
    }

    /**
     * 批量更新时序图（优化性能）
     * @param {string} chartId
     * @param {Array} dataPoints - [{ timestamp, heights: [eta1, eta2, eta3] }, ...]
     */
    batchUpdateTimeSeries(chartId, dataPoints) {
        const chart = this.charts.get(chartId);
        if (!chart) return;

        dataPoints.forEach(point => {
            const time = point.timestamp instanceof Date ? point.timestamp : new Date(point.timestamp);

            chart.data.labels.push(time);

            point.heights.forEach((height, index) => {
                if (chart.data.datasets[index]) {
                    chart.data.datasets[index].data.push(height);
                }
            });
        });

        // 限制数据点数量
        while (chart.data.labels.length > this.maxDataPoints) {
            chart.data.labels.shift();
            chart.data.datasets.forEach(dataset => {
                dataset.data.shift();
            });
        }

        chart.update();
    }

    /**
     * 设置图表最大数据点数
     * @param {number} maxPoints
     */
    setMaxDataPoints(maxPoints) {
        this.maxDataPoints = maxPoints;
    }
}

// 创建全局图表管理器实例
window.chartManager = new ChartManager();
