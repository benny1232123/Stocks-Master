// 持仓建议三维评分配置 —— 由 smcore/config/defaults.py 的 RECOMMENDATION_CONFIG 生成。
//
// ⚠️ 单一真源：评分阈值与权重的真源在**后端**（smcore/config/defaults.py）。
// 本文件只是「后端不可达时的兜底快照」，由 scripts/sync_scoring_config.py 同步生成，
// 请勿手工编辑。运行时 useScoringConfig() 会拉取 /api/config/recommendation 覆盖它。
//
// 背景（2026-09-09）：此前 App.jsx 里硬编码复刻了整套分段阈值与 0.40/0.35/0.25 权重，
// 与后端各写一份，只能靠 verify_panel_alignment.py 事后校验，改一处漏一处。

export const DEFAULT_SCORING_CONFIG = {
  "enable_technical": true,
  "enable_fundamental": true,
  "enable_capital": true,
  "face_weights": {
    "technical": 0.4,
    "fundamental": 0.35,
    "capital": 0.25
  },
  "tech_base": 50,
  "tech_step": 6,
  "technical": {
    "rsi": [
      {
        "gt": 80,
        "score": -2,
        "label": "严重超买"
      },
      {
        "gt": 70,
        "score": -1,
        "label": "高位"
      },
      {
        "lt": 20,
        "score": 2,
        "label": "严重超卖"
      },
      {
        "lt": 30,
        "score": 1,
        "label": "超卖"
      },
      {
        "gt": 55,
        "score": 1,
        "label": "偏强"
      },
      {
        "lt": 45,
        "score": -1,
        "label": "偏弱"
      }
    ],
    "macd_golden_red": 2,
    "macd_dead_green": -2,
    "kdj_j_over": 100,
    "kdj_j_over_score": -2,
    "kdj_j_under": 0,
    "kdj_j_under_score": 2,
    "kdj_k_gt_d": 1,
    "kdj_k_lt_d": -1,
    "ma_bull": 2,
    "ma_bear": -2,
    "ma5_gt_ma20": 1,
    "ma5_lt_ma20": -1,
    "boll_below_lower": 1,
    "boll_near_lower_dist": 2.0,
    "boll_near_lower": 1,
    "boll_near_upper_dist": -2.0,
    "boll_near_upper": -1
  },
  "technical_cls": {
    "good": 70,
    "bad": 30
  },
  "fundamental": {
    "pe": [
      {
        "lt": 0,
        "score": 38,
        "label": "亏损"
      },
      {
        "lt": 15,
        "score": 90,
        "label": "偏低·有吸引力"
      },
      {
        "lt": 25,
        "score": 76,
        "label": "中性合理"
      },
      {
        "lt": 35,
        "score": 62,
        "label": "偏高"
      },
      {
        "lt": 50,
        "score": 46,
        "label": "高估值"
      },
      {
        "score": 32,
        "label": "高估值"
      }
    ],
    "pb": [
      {
        "lt": 1,
        "score": 90,
        "label": "破净·低估值"
      },
      {
        "lt": 3,
        "score": 76,
        "label": "偏低"
      },
      {
        "lt": 6,
        "score": 62,
        "label": "合理"
      },
      {
        "lt": 10,
        "score": 46,
        "label": "偏高"
      },
      {
        "score": 32,
        "label": "高PB"
      }
    ],
    "roe": [
      {
        "gt": 0.2,
        "score": 92,
        "label": "优秀"
      },
      {
        "gt": 0.15,
        "score": 82,
        "label": "良好"
      },
      {
        "gt": 0.1,
        "score": 66,
        "label": "一般"
      },
      {
        "gt": 0,
        "score": 50,
        "label": "偏低"
      },
      {
        "score": 28,
        "label": "亏损"
      }
    ],
    "gm": [
      {
        "gt": 0.5,
        "score": 92,
        "label": "高毛利"
      },
      {
        "gt": 0.4,
        "score": 82,
        "label": "较高"
      },
      {
        "gt": 0.3,
        "score": 66,
        "label": "中等"
      },
      {
        "gt": 0.2,
        "score": 54,
        "label": "较低"
      },
      {
        "score": 42,
        "label": "低毛利"
      }
    ],
    "rg": [
      {
        "gt": 0.3,
        "score": 92,
        "label": "高增长"
      },
      {
        "gt": 0.2,
        "score": 82,
        "label": "稳健增长"
      },
      {
        "gt": 0.1,
        "score": 66,
        "label": "微增"
      },
      {
        "gt": 0,
        "score": 54,
        "label": "持平"
      },
      {
        "score": 32,
        "label": "负增长"
      }
    ],
    "missing": 50
  },
  "capital": {
    "liq_amt": [
      {
        "gt": 5,
        "score": 92,
        "label": "流动性充裕"
      },
      {
        "gt": 2,
        "score": 76,
        "label": "流动性较好"
      },
      {
        "gt": 1,
        "score": 62,
        "label": "流动性中等"
      },
      {
        "gt": 0.3,
        "score": 48,
        "label": "成交偏清淡"
      },
      {
        "score": 32,
        "label": "成交清淡"
      }
    ],
    "turnover": [
      {
        "gt": 5,
        "score": 90,
        "label": "高度活跃"
      },
      {
        "gt": 2,
        "score": 78,
        "label": "活跃"
      },
      {
        "gt": 1,
        "score": 64,
        "label": "一般"
      },
      {
        "gt": 0.3,
        "score": 52,
        "label": "偏低"
      },
      {
        "lt": 0.1,
        "score": 34,
        "label": "低迷"
      },
      {
        "score": 46,
        "label": "一般"
      }
    ],
    "missing": 50
  },
  "fund_cap_cls": {
    "good": 65,
    "bad": 45
  },
  "rating": [
    {
      "gte": 70,
      "label": "推荐关注"
    },
    {
      "gte": 58,
      "label": "偏积极"
    },
    {
      "gte": 45,
      "label": "中性观望"
    },
    {
      "gte": 35,
      "label": "偏谨慎"
    },
    {
      "label": "回避"
    }
  ],
  "action_map": {
    "推荐关注": "加仓",
    "偏积极": "持有偏多",
    "中性观望": "持有观望",
    "偏谨慎": "减仓偏空",
    "回避": "减仓"
  }
}

export default DEFAULT_SCORING_CONFIG
