/**
 * 雷达数据显示辅助函数
 */

// 存储最近的雷达数据
window.radarDataCache = {
    1: null,
    2: null,
    3: null
};

/**
 * 更新单个雷达的显示
 */
function updateSingleRadar(radarData) {
    if (!radarData || !radarData.radar_id) return;
    
    const radarId = radarData.radar_id;
    const distance = radarData.distance;
    const quality = radarData.quality || 0;
    
    // 更新缓存
    window.radarDataCache[radarId] = radarData;
    
    // 更新显示
    const heightElem = document.getElementById(`radar${radarId}Height`);
    const statusElem = document.getElementById(`radar${radarId}Status`);
    const radarElem = document.getElementById(`radar${radarId}`);
    
    if (heightElem) {
        heightElem.textContent = distance !== null && distance !== undefined ? 
            distance.toFixed(3) + ' m' : '-- m';
    }
    
    if (statusElem) {
        if (quality >= 80) {
            statusElem.textContent = '在线';
            statusElem.className = 'radar-status online';
        } else if (quality >= 50) {
            statusElem.textContent = '信号弱';
            statusElem.className = 'radar-status warning';
        } else {
            statusElem.textContent = '离线';
            statusElem.className = 'radar-status offline';
        }
    }
    
    if (radarElem) {
        radarElem.classList.remove('offline');
        if (quality >= 50) {
            radarElem.classList.add('online');
        }
    }
}

/**
 * 更新所有雷达的显示
 */
function refreshAllRadars() {
    for (let i = 1; i <= 3; i++) {
        const data = window.radarDataCache[i];
        if (data) {
            updateSingleRadar(data);
        }
    }
}

// 添加样式
const style = document.createElement('style');
style.textContent = `
    .radar-status.online { color: #10b981; font-weight: bold; }
    .radar-status.warning { color: #f59e0b; font-weight: bold; }
    .radar-status.offline { color: #ef4444; font-weight: bold; }
    .radar-item.online { border-left: 3px solid #10b981; }
`;
document.head.appendChild(style);

console.log('✅ 雷达显示模块已加载');
