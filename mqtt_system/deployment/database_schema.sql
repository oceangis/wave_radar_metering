-- =====================================================
-- 波浪监测系统 - 完整数据库建表脚本
-- 在新机器上执行: sudo -u postgres psql wave_monitoring < database_schema.sql
-- =====================================================

-- ==================== 原始测量数据 ====================
CREATE TABLE IF NOT EXISTS wave_measurements (
    id BIGSERIAL PRIMARY KEY,
    "timestamp" TIMESTAMP WITH TIME ZONE NOT NULL,
    radar_id INTEGER NOT NULL,
    distance REAL NOT NULL,
    quality INTEGER DEFAULT 100,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_measurements_timestamp ON wave_measurements("timestamp");
CREATE INDEX IF NOT EXISTS idx_measurements_radar_id ON wave_measurements(radar_id);
CREATE INDEX IF NOT EXISTS idx_measurements_timestamp_radar ON wave_measurements("timestamp", radar_id);
CREATE INDEX IF NOT EXISTS idx_measurements_created_at ON wave_measurements(created_at);

-- ==================== 波浪分析结果 ====================
CREATE TABLE IF NOT EXISTS wave_analysis (
    id SERIAL PRIMARY KEY,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time TIMESTAMP WITH TIME ZONE NOT NULL,
    collection_start_time TIMESTAMP WITH TIME ZONE,
    collection_end_time TIMESTAMP WITH TIME ZONE,
    duration_seconds INTEGER NOT NULL,
    sample_count INTEGER NOT NULL,
    sample_rate REAL,

    -- 频谱分析参数
    hs REAL,                    -- 有效波高 Hm0 (m)
    tp REAL,                    -- 峰值周期 (s)
    tz REAL,                    -- 平均跨零周期 (s)
    theta REAL,                 -- 波向 (°)
    fp REAL,                    -- 峰值频率 (Hz)

    -- 各雷达波高
    hs_radar1 REAL,
    hs_radar2 REAL,
    hs_radar3 REAL,
    phase_diff_12 REAL,
    phase_diff_13 REAL,

    -- 零交叉分析
    hs_zc REAL,                 -- 零交叉有效波高 (m)
    hmax DOUBLE PRECISION,      -- 最大波高 (m)
    h1_10 DOUBLE PRECISION,     -- 1/10波高 (m)
    h_mean DOUBLE PRECISION,    -- 平均波高 (m)
    tmax DOUBLE PRECISION,      -- 最大波周期 (s)
    t1_10 DOUBLE PRECISION,     -- 1/10波周期 (s)
    ts DOUBLE PRECISION,        -- 有效波周期 (s)
    tmean DOUBLE PRECISION,     -- 平均波周期 (s)
    wave_count INTEGER,         -- 波浪数量
    mean_level DOUBLE PRECISION,-- 潮位/平均水位 (cm)

    -- 谱矩
    m_minus1 DOUBLE PRECISION,
    m0 DOUBLE PRECISION,
    m1 DOUBLE PRECISION,
    m2 DOUBLE PRECISION,
    m4 DOUBLE PRECISION,

    -- 谱参数
    tm01 DOUBLE PRECISION,
    te DOUBLE PRECISION,
    fm DOUBLE PRECISION,
    fz DOUBLE PRECISION,
    fe DOUBLE PRECISION,
    df DOUBLE PRECISION,
    f_min DOUBLE PRECISION,
    f_max DOUBLE PRECISION,
    nf INTEGER,
    epsilon_0 DOUBLE PRECISION,

    -- 方向分析
    wave_direction REAL,        -- 峰值波向 Dp (°)
    mean_direction REAL,        -- 平均波向 (°)
    directional_spread REAL,    -- 方向展宽 (°)
    direction_at_peak REAL,     -- 峰值周期处波向 DTp (°)
    diwasp_method VARCHAR(20),
    diwasp_success BOOLEAN DEFAULT FALSE,
    directional_spectrum JSONB,

    -- 谱数据
    spectrum_data JSONB,
    time_domain_data JSONB,

    -- 元数据
    analysis_version VARCHAR(50),
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_analysis_start_time ON wave_analysis(start_time);
CREATE INDEX IF NOT EXISTS idx_analysis_end_time ON wave_analysis(end_time);
CREATE INDEX IF NOT EXISTS idx_analysis_created_at ON wave_analysis(created_at);
CREATE INDEX IF NOT EXISTS idx_analysis_wave_direction ON wave_analysis(wave_direction);
CREATE INDEX IF NOT EXISTS idx_analysis_directional ON wave_analysis(start_time, wave_direction, mean_direction);

-- ==================== 采集器状态 ====================
CREATE TABLE IF NOT EXISTS collector_status (
    id SERIAL PRIMARY KEY,
    "timestamp" TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    radar1_online BOOLEAN,
    radar2_online BOOLEAN,
    radar3_online BOOLEAN,
    sample_rate_actual REAL,
    error_count INTEGER,
    uptime_seconds BIGINT
);

CREATE INDEX IF NOT EXISTS idx_status_timestamp ON collector_status("timestamp");

-- ==================== 系统日志 ====================
CREATE TABLE IF NOT EXISTS system_logs (
    id SERIAL PRIMARY KEY,
    "timestamp" TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    level VARCHAR(20),
    component VARCHAR(50),
    message TEXT,
    details JSONB
);

CREATE INDEX IF NOT EXISTS idx_log_timestamp ON system_logs("timestamp");
CREATE INDEX IF NOT EXISTS idx_logs_level ON system_logs(level);

-- ==================== 潮汐分析 ====================
CREATE TABLE IF NOT EXISTS tide_analysis (
    id SERIAL PRIMARY KEY,
    analysis_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    data_start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    data_end_time TIMESTAMP WITH TIME ZONE NOT NULL,
    data_duration_hours REAL NOT NULL,
    sample_count INTEGER NOT NULL,
    latitude REAL NOT NULL,
    array_height REAL NOT NULL,
    method VARCHAR(20) DEFAULT 'ols',
    M2_amplitude DOUBLE PRECISION,
    S2_amplitude DOUBLE PRECISION,
    K1_amplitude DOUBLE PRECISION,
    O1_amplitude DOUBLE PRECISION,
    N2_amplitude DOUBLE PRECISION,
    K2_amplitude DOUBLE PRECISION,
    P1_amplitude DOUBLE PRECISION,
    Q1_amplitude DOUBLE PRECISION,
    M2_phase DOUBLE PRECISION,
    S2_phase DOUBLE PRECISION,
    K1_phase DOUBLE PRECISION,
    O1_phase DOUBLE PRECISION,
    N2_phase DOUBLE PRECISION,
    K2_phase DOUBLE PRECISION,
    P1_phase DOUBLE PRECISION,
    Q1_phase DOUBLE PRECISION,
    constituents_count INTEGER,
    mean_tide_level DOUBLE PRECISION,
    tide_range DOUBLE PRECISION,
    residual_std DOUBLE PRECISION,
    coefficients JSONB,
    quality_flag INTEGER DEFAULT 0,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tide_analysis_time ON tide_analysis(analysis_time);
CREATE INDEX IF NOT EXISTS idx_tide_analysis_data_range ON tide_analysis(data_start_time, data_end_time);

-- ==================== 潮位预测 ====================
CREATE TABLE IF NOT EXISTS tide_predictions (
    id SERIAL PRIMARY KEY,
    analysis_id INTEGER REFERENCES tide_analysis(id) ON DELETE CASCADE,
    prediction_time TIMESTAMP WITH TIME ZONE NOT NULL,
    predicted_tide_level DOUBLE PRECISION NOT NULL,
    tide_type VARCHAR(20) DEFAULT 'astronomical',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(analysis_id, prediction_time)
);

CREATE INDEX IF NOT EXISTS idx_tide_predictions_time ON tide_predictions(prediction_time);
CREATE INDEX IF NOT EXISTS idx_tide_predictions_analysis ON tide_predictions(analysis_id);

-- ==================== 潮位观测 ====================
CREATE TABLE IF NOT EXISTS tide_observations (
    id SERIAL PRIMARY KEY,
    observation_time TIMESTAMP WITH TIME ZONE NOT NULL,
    observed_tide_level DOUBLE PRECISION NOT NULL,
    radar1_distance DOUBLE PRECISION NOT NULL,
    array_height DOUBLE PRECISION NOT NULL,
    quality_flag INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(observation_time)
);

CREATE INDEX IF NOT EXISTS idx_tide_observations_time ON tide_observations(observation_time);

-- ==================== 统计视图 ====================
CREATE OR REPLACE VIEW recent_wave_stats AS
SELECT date_trunc('hour', start_time) AS hour,
       count(*) AS analysis_count,
       avg(hs) AS avg_hs,
       max(hs) AS max_hs,
       avg(tp) AS avg_tp,
       avg(theta) AS avg_theta
FROM wave_analysis
WHERE start_time > (now() - INTERVAL '1 hour')
GROUP BY date_trunc('hour', start_time)
ORDER BY date_trunc('hour', start_time) DESC;

CREATE OR REPLACE VIEW directional_wave_stats AS
SELECT date_trunc('hour', start_time) AS hour,
       count(*) AS analysis_count,
       avg(hs) AS avg_hs,
       avg(tp) AS avg_tp,
       avg(wave_direction) AS avg_direction,
       avg(mean_direction) AS avg_mean_direction,
       avg(directional_spread) AS avg_spread,
       count(CASE WHEN diwasp_success THEN 1 END) AS successful_diwasp_count
FROM wave_analysis
WHERE start_time > (now() - INTERVAL '24 hours')
GROUP BY date_trunc('hour', start_time)
ORDER BY date_trunc('hour', start_time) DESC;

-- ==================== 数据清理函数 ====================
CREATE OR REPLACE FUNCTION cleanup_old_measurements() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM wave_measurements WHERE created_at < NOW() - INTERVAL '30 days';
END;
$$;

CREATE OR REPLACE FUNCTION cleanup_old_analysis() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM wave_analysis WHERE created_at < NOW() - INTERVAL '1 year';
END;
$$;

-- ==================== 授权 ====================
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO wave_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO wave_user;
