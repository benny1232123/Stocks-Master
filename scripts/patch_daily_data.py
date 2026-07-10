"""补丁：补名称 + 补买入价 + 清洗策略字段"""
import csv, glob, os
import akshare as ak

STOCK_DATA_DIR = 'stock_data'

# ====== 1. 名称映射 ======
print("1/3 加载名称映射...")
name_map = {}
try:
    df = ak.stock_info_a_code_name()
    for _, r in df.iterrows():
        c, n = str(r['code']).strip(), str(r['name']).strip()
        if c and n: name_map[c] = n
except Exception as e:
    print(f"  失败: {e}")
print(f"  {len(name_map)} 条")

# ====== 2. 价格缓存（逐只拉取，带重试） ======
print("2/3 构建价格表（按需拉取）...")
_price_cache = {}

def get_price(code):
    if code in _price_cache:
        return _price_cache[code]
    # 尝试历史日K
    for attempt in range(2):
        try:
            df = ak.stock_zh_a_hist(
                symbol=code, period="daily",
                start_date="20260709", end_date="20260711",
                adjust="qfq"
            )
            if not df.empty:
                p = float(df.iloc[0]['收盘'])
                _price_cache[code] = p
                return p
        except Exception:
            pass
    _price_cache[code] = None
    return None

# ====== 3. 遍历修复 ======
print("3/3 修复日报CSV...")
fixed = t_name = t_price = t_strat = 0

for f in sorted(glob.glob(os.path.join(STOCK_DATA_DIR, 'Daily-Action-List-*.csv'))):
    with open(f, encoding='utf-8-sig') as fh:
        reader = csv.DictReader(fh)
        fields = list(reader.fieldnames)
        rows = list(reader)   # ← 正确：DictReader直接迭代得dict

    bn = os.path.basename(f)
    n_cnt = p_cnt = s_cnt = 0
    need_codes = set()

    # 先收集需要价格的代码
    for r in rows:
        code = str(r.get('股票代码', '')).strip()
        if code and not (r.get('建议买入价') or '').strip():
            need_codes.add(code)

    # 批量预加载价格
    for code in need_codes:
        get_price(code)

    for r in rows:
        code = str(r.get('股票代码', '')).strip()

        if code and not (r.get('股票名称') or '').strip() and code in name_map:
            r['股票名称'] = name_map[code]; n_cnt += 1

        if code and not (r.get('建议买入价') or '').strip():
            p = _price_cache.get(code)
            if p:
                r['建议买入价'] = f"{p:.2f}"
                r['最新价'] = f"{p:.2f}"
                r['止损价(下轨)'] = f"{p * 0.92:.2f}"
                r['止盈价(上轨)'] = f"{p * 1.08:.2f}"
                p_cnt += 1

        raw = r.get('来源策略', '')
        if raw:
            clean = '/'.join(s.strip() for s in raw.split('/') if s.strip())
            if clean != raw:
                r['来源策略'] = clean; s_cnt += 1

    if n_cnt or p_cnt or s_cnt:
        with open(f, 'w', encoding='utf-8-sig', newline='') as fh:
            w = csv.DictWriter(fh, fieldnames=fields)
            w.writeheader(); w.writerows(rows)
        fixed += 1
        parts = []
        if n_cnt: parts.append(f"名+{n_cnt}")
        if p_cnt: parts.append(f"价+{p_cnt}")
        if s_cnt: parts.append(f"策+{s_cnt}")
        print(f"  ✅ {bn}: {' · '.join(parts)}")

    t_name += n_cnt; t_price += p_cnt; t_strat += s_cnt

print(f"\n✅ 完成: {fixed}/23 文件 | 名称={t_name} 价格={t_price} 策略={t_strat}")
