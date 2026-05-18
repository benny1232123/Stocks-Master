from importlib import util
from pathlib import Path
import sys
p = Path(__file__).resolve().parent / 'Frequently-Used-Program' / 'auto_notify_boll.py'
spec = util.spec_from_file_location('auto_notify_boll', str(p))
mod = util.module_from_spec(spec)
spec.loader.exec_module(mod)
from datetime import datetime

import sys

today = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y%m%d')
print('RUNNING MACRO-NEWS ONLY FOR', today)
print('\n--- TREND SUMMARY ---\n')
# If argument is a path to a news CSV, use it; otherwise try archived news for the date.
arg = today
path = None
try:
	p = Path(arg)
	if p.exists() and p.is_file():
		path = p
except Exception:
	path = None

if path is None:
	# try archive location
	archive_dir = Path('stock_data') / 'archive' / today[:6] / 'news'
	if archive_dir.exists():
		# find the most recent file that contains the date
		candidates = sorted(list(archive_dir.glob(f"*{today}*.csv")), key=lambda p: p.stat().st_mtime, reverse=True)
		if candidates:
			path = candidates[0]

if path is None:
	print(mod._build_macro_news_trend_summary(window_days=3, top_n=0, auto_fetch=False))
	print('\n--- RISK SUMMARY ---\n')
	print(mod._build_macro_risk_summary(today, window_days=3, top_n=0, auto_fetch=False))
	sys.exit(0)

print(f"Using news file: {path}")
news_files = [(path, today)]
rules, easing, triggers = mod._load_macro_risk_config()
burst_tokens = mod._extract_burst_tokens(news_files, min_count=1, top_n=50)
events, _ = mod._collect_macro_risk_events(news_files, rules, easing, triggers, burst_tokens, trigger_mode=os.getenv('MACRO_RISK_TRIGGER_MODE','burst'))
print('\n--- RAW EVENTS ---\n')
for e in events:
	print(e)
print('\n--- TREND SUMMARY ---\n')
print(mod._build_macro_news_trend_summary(window_days=3, top_n=0, auto_fetch=False))
print('\n--- RISK SUMMARY ---\n')
print(mod._build_macro_risk_summary(today, window_days=3, top_n=0, auto_fetch=False))
