from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
STOCK_DATA_DIR = PROJECT_ROOT / "stock_data"
PLOT_DIR = STOCK_DATA_DIR / "plots"

DEFAULT_DAYS_BACK = 180
DEFAULT_WINDOW = 20
DEFAULT_K = 1.645
DEFAULT_NEAR_RATIO = 1.015
DEFAULT_ADJUST = "qfq"
DEFAULT_PRICE_UPPER_LIMIT = 35.0
DEFAULT_DEBT_ASSET_RATIO_LIMIT = 70.0
DEFAULT_EXCLUDE_GEM_SCI = True

DEFAULT_FUND_FLOW_PERIODS = ("3日排行", "5日排行", "10日排行")
IMPORTANT_SHAREHOLDERS = (
	"香港中央结算有限公司",
	"中央汇金资产管理有限公司",
	"中央汇金投资有限责任公司",
	"香港中央结算（代理人）有限公司",
	"中国证券金融股份有限公司",
)
IMPORTANT_SHAREHOLDER_TYPES = ("社保基金",)

CSV_ENCODING = "utf-8-sig"
