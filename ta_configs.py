import ta

# =====================================
# Column Aliases
# =====================================
COL_OPEN, COL_HIGH, COL_LOW, COL_CLOSE, COL_VOLUME = (
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
)

TA_CONFIG = [
    # ---- Momentum ----
    {
        "code": "crma_ta_mom_c_rsi_14d",
        "name": "RSI (14-day)",
        "group": "Momentum",
        "class": ta.momentum.RSIIndicator,
        "inputs": [COL_CLOSE],
        "params": {"window": 14},
        "methods": ["rsi"],
    },
    {
        "code": ["crma_ta_mom_c_stochk_14d", "crma_ta_mom_c_stochd_14d"],
        "name": "Stochastic",
        "group": "Momentum",
        "class": ta.momentum.StochasticOscillator,
        "inputs": [COL_HIGH, COL_LOW, COL_CLOSE],
        "params": {"window": 14, "smooth_window": 3},
        "methods": ["stoch", "stoch_signal"],
    },
    {
        "code": "crma_ta_mom_c_wr_14d",
        "name": "Williams %R",
        "group": "Momentum",
        "class": ta.momentum.WilliamsRIndicator,
        "inputs": [COL_HIGH, COL_LOW, COL_CLOSE],
        "params": {"lbp": 14},
        "methods": ["williams_r"],
    },
    {
        "code": [
            "crma_ta_mom_c_macd_12_26",
            "crma_ta_mom_c_macds_9d",
            "crma_ta_mom_c_macdh_12_26_9",
        ],
        "name": "MACD",
        "group": "Momentum",
        "class": ta.trend.MACD,
        "inputs": [COL_CLOSE],
        "params": {"window_fast": 12, "window_slow": 26, "window_sign": 9},
        "methods": ["macd", "macd_signal", "macd_diff"],
    },
    {
        "code": "crma_ta_mom_c_roc_12d",
        "name": "Rate of Change (ROC)",
        "group": "Momentum",
        "class": ta.momentum.ROCIndicator,
        "inputs": [COL_CLOSE],
        "params": {"window": 12},
        "methods": ["roc"],
    },
    # ---- Volume ----
    {
        "code": "crma_ta_vol_c_obv",
        "name": "On-Balance Volume (OBV)",
        "group": "Volume",
        "class": ta.volume.OnBalanceVolumeIndicator,
        "inputs": [COL_CLOSE, COL_VOLUME],
        "params": {},
        "methods": ["on_balance_volume"],
    },
    {
        "code": "crma_ta_vol_cmf_20d",
        "name": "Chaikin Money Flow (CMF)",
        "group": "Volume",
        "class": ta.volume.ChaikinMoneyFlowIndicator,
        "inputs": [COL_HIGH, COL_LOW, COL_CLOSE, COL_VOLUME],
        "params": {"window": 20},
        "methods": ["chaikin_money_flow"],
    },
    {
        "code": "crma_ta_vol_adi",
        "name": "Accumulation/Distribution Index (ADI)",
        "group": "Volume",
        "class": ta.volume.AccDistIndexIndicator,
        "inputs": [COL_HIGH, COL_LOW, COL_CLOSE, COL_VOLUME],
        "params": {},
        "methods": ["acc_dist_index"],
    },
    {
        "code": "crma_ta_vol_eom_14d",
        "name": "Ease of Movement (EoM)",
        "group": "Volume",
        "class": ta.volume.EaseOfMovementIndicator,
        "inputs": [COL_HIGH, COL_LOW, COL_VOLUME],
        "params": {"window": 14},
        "methods": ["ease_of_movement"],
    },
    {
        "code": "crma_ta_vol_vwap_20d",
        "name": "VWAP",
        "group": "Volume",
        "class": ta.volume.VolumeWeightedAveragePrice,
        "inputs": [COL_HIGH, COL_LOW, COL_CLOSE, COL_VOLUME],
        "params": {"window": 20},
        "methods": ["volume_weighted_average_price"],
    },
    # ---- Volatility ----
    {
        "code": [
            "crma_ta_vlt_c_bbh_20d",
            "crma_ta_vlt_c_bbl_20d",
            "crma_ta_vlt_c_bbw_20d",
        ],
        "name": "Bollinger Bands",
        "group": "Volatility",
        "class": ta.volatility.BollingerBands,
        "inputs": [COL_CLOSE],
        "params": {"window": 20, "window_dev": 2},
        "methods": ["bollinger_hband", "bollinger_lband", "bollinger_wband"],
    },
    {
        "code": "crma_ta_vlt_c_atr_14d",
        "name": "ATR",
        "group": "Volatility",
        "class": ta.volatility.AverageTrueRange,
        "inputs": [COL_HIGH, COL_LOW, COL_CLOSE],
        "params": {"window": 14},
        "methods": ["average_true_range"],
    }
]