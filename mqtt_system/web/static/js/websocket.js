/**
 * WebSocket 连接管理
 * WebSocket Client for Real-time Data Streaming
 */

class WebSocketClient {
    constructor() {
        this.ws = null;
        this.reconnectInterval = null;
        this.reconnectDelay = 3000;  // Reduced from 5000ms
        this.maxReconnectDelay = 15000;  // Reduced from 30000ms
        this.currentReconnectDelay = this.reconnectDelay;
        this.messageHandlers = new Map();
        this.connectionStateHandlers = [];
        this.isManualClose = false;

        // Keepalive ping/pong
        this.pingInterval = null;
        this.pingIntervalMs = 25000;  // Send ping every 25 seconds
        this.pongTimeout = null;
        this.pongTimeoutMs = 10000;  // Wait 10 seconds for pong
    }

    /**
     * 连接到WebSocket服务器
     * @param {string} url - WebSocket URL（可选，自动检测）
     */
    connect(url = null) {
        // 自动构建WebSocket URL
        if (!url) {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            url = `${protocol}//${window.location.host}/ws`;
        }

        console.log(`[WebSocket] Connecting to ${url}...`);

        try {
            this.ws = new WebSocket(url);
            this.setupEventHandlers();
        } catch (error) {
            console.error('[WebSocket] Connection error:', error);
            this.handleConnectionState('error', error);
            this.scheduleReconnect();
        }
    }

    /**
     * 设置WebSocket事件处理器
     * @private
     */
    setupEventHandlers() {
        this.ws.onopen = () => {
            console.log('[WebSocket] Connected');
            this.currentReconnectDelay = this.reconnectDelay;
            this.clearReconnectTimer();
            this.startPingInterval();
            this.handleConnectionState('connected');
        };

        this.ws.onmessage = (event) => {
            // Reset pong timeout on any message (server is alive)
            this.clearPongTimeout();

            try {
                const message = JSON.parse(event.data);

                // Handle pong response
                if (message.type === 'pong') {
                    console.log('[WebSocket] Pong received');
                    return;
                }

                this.handleMessage(message);
            } catch (error) {
                console.error('[WebSocket] Failed to parse message:', error);
            }
        };

        this.ws.onerror = (error) => {
            console.error('[WebSocket] Error:', error);
            this.handleConnectionState('error', error);
        };

        this.ws.onclose = (event) => {
            console.log('[WebSocket] Disconnected', event.code, event.reason);
            this.stopPingInterval();
            this.clearPongTimeout();
            this.handleConnectionState('disconnected', event);

            // 如果不是手动关闭，则尝试重连
            if (!this.isManualClose) {
                this.scheduleReconnect();
            }
        };
    }

    /**
     * 启动心跳ping定时器
     * @private
     */
    startPingInterval() {
        this.stopPingInterval();
        this.pingInterval = setInterval(() => {
            this.sendPing();
        }, this.pingIntervalMs);
    }

    /**
     * 停止心跳ping定时器
     * @private
     */
    stopPingInterval() {
        if (this.pingInterval) {
            clearInterval(this.pingInterval);
            this.pingInterval = null;
        }
    }

    /**
     * 发送ping消息
     * @private
     */
    sendPing() {
        if (this.isConnected()) {
            console.log('[WebSocket] Sending ping...');
            this.send({ type: 'ping', timestamp: Date.now() });

            // Set pong timeout
            this.pongTimeout = setTimeout(() => {
                console.warn('[WebSocket] Pong timeout, reconnecting...');
                this.ws.close(4000, 'Pong timeout');
            }, this.pongTimeoutMs);
        }
    }

    /**
     * 清除pong超时定时器
     * @private
     */
    clearPongTimeout() {
        if (this.pongTimeout) {
            clearTimeout(this.pongTimeout);
            this.pongTimeout = null;
        }
    }

    /**
     * 处理接收到的消息
     * @private
     */
    handleMessage(message) {
        const { type, data } = message;

        // 调用注册的处理器
        if (this.messageHandlers.has(type)) {
            const handlers = this.messageHandlers.get(type);
            handlers.forEach(handler => {
                try {
                    handler(data, message);
                } catch (error) {
                    console.error(`[WebSocket] Handler error for type '${type}':`, error);
                }
            });
        }

        // 调用通用处理器
        if (this.messageHandlers.has('*')) {
            const handlers = this.messageHandlers.get('*');
            handlers.forEach(handler => {
                try {
                    handler(data, message);
                } catch (error) {
                    console.error('[WebSocket] Universal handler error:', error);
                }
            });
        }
    }

    /**
     * 处理连接状态变化
     * @private
     */
    handleConnectionState(state, event = null) {
        this.connectionStateHandlers.forEach(handler => {
            try {
                handler(state, event);
            } catch (error) {
                console.error('[WebSocket] State handler error:', error);
            }
        });
    }

    /**
     * 注册消息处理器
     * @param {string} type - 消息类型（'*' 表示所有消息）
     * @param {Function} handler - 处理函数
     */
    on(type, handler) {
        if (!this.messageHandlers.has(type)) {
            this.messageHandlers.set(type, []);
        }
        this.messageHandlers.get(type).push(handler);
    }

    /**
     * 移除消息处理器
     * @param {string} type - 消息类型
     * @param {Function} handler - 处理函数
     */
    off(type, handler) {
        if (!this.messageHandlers.has(type)) return;

        const handlers = this.messageHandlers.get(type);
        const index = handlers.indexOf(handler);

        if (index > -1) {
            handlers.splice(index, 1);
        }
    }

    /**
     * 注册连接状态处理器
     * @param {Function} handler - 处理函数 (state, event) => {}
     */
    onConnectionStateChange(handler) {
        this.connectionStateHandlers.push(handler);
    }

    /**
     * 发送消息
     * @param {Object} message - 消息对象
     */
    send(message) {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            try {
                this.ws.send(JSON.stringify(message));
            } catch (error) {
                console.error('[WebSocket] Failed to send message:', error);
            }
        } else {
            console.warn('[WebSocket] Cannot send message: connection not open');
        }
    }

    /**
     * 计划重连
     * @private
     */
    scheduleReconnect() {
        if (this.reconnectInterval) return;

        console.log(`[WebSocket] Reconnecting in ${this.currentReconnectDelay / 1000}s...`);

        this.reconnectInterval = setTimeout(() => {
            this.reconnectInterval = null;
            this.connect();

            // 指数退避
            this.currentReconnectDelay = Math.min(
                this.currentReconnectDelay * 1.5,
                this.maxReconnectDelay
            );
        }, this.currentReconnectDelay);
    }

    /**
     * 清除重连定时器
     * @private
     */
    clearReconnectTimer() {
        if (this.reconnectInterval) {
            clearTimeout(this.reconnectInterval);
            this.reconnectInterval = null;
        }
    }

    /**
     * 关闭连接
     * @param {boolean} manual - 是否手动关闭
     */
    close(manual = true) {
        this.isManualClose = manual;
        this.clearReconnectTimer();
        this.stopPingInterval();
        this.clearPongTimeout();

        if (this.ws) {
            this.ws.close();
            this.ws = null;
        }
    }

    /**
     * 获取连接状态
     * @returns {string} 'connecting' | 'connected' | 'disconnected'
     */
    getState() {
        if (!this.ws) return 'disconnected';

        switch (this.ws.readyState) {
            case WebSocket.CONNECTING:
                return 'connecting';
            case WebSocket.OPEN:
                return 'connected';
            case WebSocket.CLOSING:
            case WebSocket.CLOSED:
            default:
                return 'disconnected';
        }
    }

    /**
     * 检查是否已连接
     * @returns {boolean}
     */
    isConnected() {
        return this.ws && this.ws.readyState === WebSocket.OPEN;
    }
}

// 创建全局WebSocket实例
window.wsClient = new WebSocketClient();
