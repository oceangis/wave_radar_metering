-- 潮汐分析调和常数表
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

    -- 主要调和常数振幅 (m)
    M2_amplitude DOUBLE PRECISION,
    S2_amplitude DOUBLE PRECISION,
    K1_amplitude DOUBLE PRECISION,
    O1_amplitude DOUBLE PRECISION,
    N2_amplitude DOUBLE PRECISION,
    K2_amplitude DOUBLE PRECISION,
    P1_amplitude DOUBLE PRECISION,
    Q1_amplitude DOUBLE PRECISION,

    -- 主要调和常数相位 (度)
    M2_phase DOUBLE PRECISION,
    S2_phase DOUBLE PRECISION,
    K1_phase DOUBLE PRECISION,
    O1_phase DOUBLE PRECISION,
    N2_phase DOUBLE PRECISION,
    K2_phase DOUBLE PRECISION,
    P1_phase DOUBLE PRECISION,
    Q1_phase DOUBLE PRECISION,

    -- 统计信息
    constituents_count INTEGER,
    mean_tide_level DOUBLE PRECISION,
    tide_range DOUBLE PRECISION,
    residual_std DOUBLE PRECISION,

    -- 完整调和常数 (JSONB格式)
    coefficients JSONB,

    -- 分析质量
    quality_flag INTEGER DEFAULT 0,
    notes TEXT,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 潮位预测表
CREATE TABLE IF NOT EXISTS tide_predictions (
    id SERIAL PRIMARY KEY,
    analysis_id INTEGER REFERENCES tide_analysis(id) ON DELETE CASCADE,
    prediction_time TIMESTAMP WITH TIME ZONE NOT NULL,

    -- 预测潮位 (相对基准面, m)
    predicted_tide_level DOUBLE PRECISION NOT NULL,

    -- 潮位类型 (astronomical: 天文潮, residual: 余潮)
    tide_type VARCHAR(20) DEFAULT 'astronomical',

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    UNIQUE(analysis_id, prediction_time)
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_tide_analysis_time ON tide_analysis(analysis_time);
CREATE INDEX IF NOT EXISTS idx_tide_analysis_data_range ON tide_analysis(data_start_time, data_end_time);
CREATE INDEX IF NOT EXISTS idx_tide_predictions_time ON tide_predictions(prediction_time);
CREATE INDEX IF NOT EXISTS idx_tide_predictions_analysis ON tide_predictions(analysis_id);

-- 潮汐实时监测表 (存储每分钟的潮位观测值)
CREATE TABLE IF NOT EXISTS tide_observations (
    id SERIAL PRIMARY KEY,
    observation_time TIMESTAMP WITH TIME ZONE NOT NULL,

    -- 观测潮位 (array_height - radar1_distance, m)
    observed_tide_level DOUBLE PRECISION NOT NULL,

    -- 雷达1原始测距 (m)
    radar1_distance DOUBLE PRECISION NOT NULL,

    -- 使用的阵列高度 (m)
    array_height DOUBLE PRECISION NOT NULL,

    -- 数据质量标志 (0: good, 1: suspect, 2: bad)
    quality_flag INTEGER DEFAULT 0,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    UNIQUE(observation_time)
);

CREATE INDEX IF NOT EXISTS idx_tide_observations_time ON tide_observations(observation_time);

COMMENT ON TABLE tide_analysis IS '潮汐调和分析结果表，存储UTide分析得到的调和常数';
COMMENT ON TABLE tide_predictions IS '潮位预测表，基于调和常数生成的未来潮位预测';
COMMENT ON TABLE tide_observations IS '潮位实时观测表，每分钟记录一次实测潮位';
