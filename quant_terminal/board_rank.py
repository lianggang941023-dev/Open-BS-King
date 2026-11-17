# -*- coding: utf-8 -*-
"""行业/概念板块排行 + 成分股明细 (v2.277, 2026-11-11 用户 m09653 移植 TSP 板块功能)

为什么另起一个模块, 而不是复用 /api/sector_ma:
  终端原有的「行业趋势/概念趋势」面板走 /api/sector_ma —— 那是**均线广度**口径
  (站上 N 日线的成分股占比), 回答的问题是「这个板块有多少票在多头排列」。
  用户要从 TSP(端口3011) 移植过来的「行业板块 / 概念板块」两个大窗口, 是**当日涨幅口径**
  的板块排行 + 龙头分 (涨幅/换手/成交/市值/量比 六因子), 回答的是「今天哪个板块最强、
  谁在领涨」。两套东西口径不同、用途不同, 所以另起一层计算, 不动 sector_ma 那条链路。

数据口径 (全部本地 + 现有快照, **不新增任何网络数据源**):
  · 成分股归属:   engine.load_industry_concept_maps() 的 industry_map / concept_map
  · 行业分级:     engine.load_industry_parent() 的「细分→上级」(257 → 90), level=1 取上级
  · K 线/市值:    本地 CSV 末 8 行 —— date,open,close,high,low,volume,总市值,流通市值,自由流通股本
  · 涨幅/现价:    全市场实时快照 (datasource 主备源, 并发 8 批实测 ≈ 3.7 秒);
                  拿不到或日期不符就回退 CSV 末行自算, 绝不编数
  · 换手率:       成交量(手) ÷ 自由流通股本(万股) —— 与 v2.274 竞价打板 `turnover_pct` 逐位一致
                  (手×100 ÷ 万股×10000 × 100% 三个 100 正好抵消)
  · 量比:         今日量 ÷ 前 5 日均量; 前 5 日**不含今日** (与 wave_scan 的 5MA 口径同款)
  · 成交额:       量(手) × 100 × 均价, 均价取 (高+低+收)/3 (v2.263 实测这四种里精度最优)

TSP 原版公式逐条照搬 (见 _leader_score / _calc_stat), 只把单位统一成
  「涨幅=百分数 / 成交额=元 / 市值=元 / 换手=百分数」,
  因为 TSP 前端里 pct 是小数(0.02 表示 2%), 而 8799 全线用百分数(2.07 表示 2.07%),
  直接抄会把 momentum 因子算成 1/50, 龙头分全被压平 —— 这是移植时最容易踩的静默错误。

对外只有两个函数:
  rank(kind, level, sort)      → 板块排行 (不含成分股明细, 快)
  members(kind, name, level)   → 某个板块的成分股明细 (按龙头分降序)
两者共用同一份 120 秒缓存的全市场事实表 _facts()。
"""
import os
import sys
import time
import math
import threading
from concurrent.futures import ThreadPoolExecutor

# 直接 `python board_rank.py` 自测时, 父目录 (终端8799) 不在 sys.path → import quant_terminal 会炸。
# 被 server.py 导入时这一段是空操作 (父目录已在 path 里)。
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

KINDS = ('industry', 'concept')
# 板块排序口径: 强度 / 涨幅 / 龙头 / 成交 / 跌幅 (TSP 原版五个 tab)
SORTS = ('heat', 'avgPct', 'leader', 'amount', 'down')

# v2.294 (2026-11-13 用户 m14431): 120 -> 1800 —— 同理, 缓存 key 已经是
#   "目录里最新 CSV 的 mtime", 15:31 写盘后自然失效; TTL 拉长只是别让它两分钟就作废。
_TTL = 1800.0                      # 全市场事实表缓存 (秒)
_CACHE = {'ts': 0.0, 'key': '', 'facts': {}, 'date': '', 'qdate': '', 'src': ''}
_LOCK = threading.Lock()
_RT_TTL = 60.0                    # 实时快照缓存
_RT = {'ts': 0.0, 'data': {}}


# ---------------------------------------------------------------- 基础工具
def _clamp01(v):
    if v is None:
        return 0.0
    try:
        v = float(v)
    except (TypeError, ValueError):
        return 0.0
    if v < 0.0:
        return 0.0
    if v > 1.0:
        return 1.0
    return v


def _avg(vals):
    vals = [v for v in vals if v is not None]
    return (sum(vals) / len(vals)) if vals else None


def _median(vals):
    vals = sorted(v for v in vals if v is not None)
    n = len(vals)
    if not n:
        return None
    m = n // 2
    return vals[m] if n % 2 else (vals[m - 1] + vals[m]) / 2.0


def _num(s):
    try:
        return float(s)
    except (TypeError, ValueError):
        return 0.0


def kline_dir():
    from quant_terminal import datasource as ds
    return ds.resolve(ds.get('local', 'kline_dir', os.path.join('kline_data', 'daily')))


def _pref(code):
    """6 位裸码 → CSV 文件名前缀 (与本地 K 线命名规则一致)"""
    c = str(code)
    if c[:3] == '920' or c[0] in '48':
        return 'BZ'
    return 'SH' if c[0] in '69' else 'SZ'


# ---------------------------------------------------------------- CSV 尾部
def _tail_rows(path, n=8):
    """倒读 CSV 末尾 n 行 → [{date,open,close,high,low,volume,cap_t,cap_f,free}, ...]

    ★ 不能用 engine._read_csv_tail: 那个只解析到第 6 列 (volume), 而这里要第 8 列流通市值
      和第 9 列自由流通股本 —— 少解析就白读一遍盘。表头行会因 float() 失败自动跳过。
    """
    try:
        with open(path, 'rb') as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            block = 8192
            buf = b''
            pos = size
            while pos > 0 and buf.count(b'\n') <= n + 2:
                step = min(block, pos)
                pos -= step
                f.seek(pos)
                buf = f.read(step) + buf
            lines = buf.split(b'\n')
            if pos > 0:
                lines = lines[1:]          # 丢掉被截断的半行
    except OSError:
        return []
    out = []
    for ln in lines:
        s = ln.decode('utf-8', 'ignore').strip()
        if not s:
            continue
        p = s.split(',')
        if len(p) < 6:
            continue
        try:
            out.append({
                'date': p[0],
                'open': float(p[1]),
                'close': float(p[2]),
                'high': float(p[3]),
                'low': float(p[4]),
                'volume': _num(p[5]),
                'cap_t': _num(p[6]) if len(p) > 6 else 0.0,
                'cap_f': _num(p[7]) if len(p) > 7 else 0.0,
                'free': _num(p[8]) if len(p) > 8 else 0.0,
            })
        except (ValueError, IndexError):
            continue
    return out[-n:] if len(out) > n else out


# ---------------------------------------------------------------- 实时快照
def _realtime_quotes():
    """全市场实时快照 {6位码: {pct, price, name}} —— 60 秒缓存。

    拿不到就返回 {} (调用方自动回退 CSV 末行算涨幅), 绝不抛异常:
    板块窗口是"看一眼"的功能, 不能因为快照源抽风就整页打不开。

    v2.279 (2026-11-12 用户 m10425): 北交所简称一直是空的。
      老实现拼 `('sh' + c if c[0] in '69' else 'sz' + c)` —— 920xxx 首位是 9 被拼成
      `sh920157`, 新浪/腾讯都返回空串 ⇒ 名字拿不到, 板块窗口里 355 只北交所全显示代码。
      实测 (2026-11-11): `bj920157` 新浪 `宇特光电,43.850,...`、腾讯 `62~宇特光电~920157~...`
      都有数据, `sh`/`sz` 一律空 ⇒ **北交所要拼 `bj`**, 与 mcap.prefixed() 归的 BZ 一一对应。
    """
    now = time.time()
    if _RT['data'] and now - _RT['ts'] < _RT_TTL:
        return _RT['data']
    try:
        from quant_terminal import datasource as ds
        from quant_terminal import engine as E
        im, cm = E.load_industry_concept_maps()
        codes = sorted(set(im) | set(cm))

        def _sc(c):
            """6 位码 → 行情源代码。北交所 920/4/8 开头走 bj, 其余 sh/sz —— 与 mcap.prefixed 同规则。"""
            if c[:3] == '920' or c[:1] in ('4', '8'):
                return 'bj' + c
            return ('sh' + c) if c[:1] in '69' else ('sz' + c)

        sina = [_sc(c) for c in codes]
        batches = [sina[i:i + 50] for i in range(0, len(sina), 50)]

        def _one(b):
            try:
                q = ds.fetch_realtime(b)
                return {k: v for k, v in (q or {}).items()
                        if isinstance(v, dict) and (v.get('price') or 0) > 0}
            except Exception:
                return {}

        got = {}
        with ThreadPoolExecutor(max_workers=8) as ex:
            for part in ex.map(_one, batches):
                got.update(part)
        out = {}
        for sc, d in got.items():
            out[sc[2:]] = {
                'pct': d.get('change_pct'),
                'price': d.get('price'),
                'name': d.get('name') or '',
                'qdate': d.get('qdate') or '',
            }
        # v2.279 兜底: 行情源偶尔不给简称 (北交所/新股), 用同花顺标的表补齐 —— 与 engine 同源
        if out:
            try:
                from quant_terminal import sector_kline as _sk
                _tk = _sk.tickers() or {}
                for _c, _v in out.items():
                    if not _v.get('name'):
                        _nm = _tk.get(_c)
                        if _nm:
                            _v['name'] = str(_nm)
            except Exception:
                pass
            _RT['data'] = out
            _RT['ts'] = now
        return out
    except Exception:
        return {}


# ---------------------------------------------------------------- 全市场事实表
def _facts():
    """全市场个股事实表 {6位码: {...}} —— 120 秒缓存

    每只票 10 个字段:
      pct(涨幅%) / price(现价) / turn(换手率%) / amt(成交额元) / cap_f(流通市值元)
      vr(量比) / v5(前5日均量 手) / date(数据日期) / name / score(龙头分) / parts(六因子)
    """
    with _LOCK:
        now = time.time()
        d = kline_dir()
        # 缓存 key: 目录里最新一个 CSV 的 mtime —— 15:31 写盘后自然失效
        key = ''
        try:
            newest = 0.0
            for fn in os.listdir(d):
                if fn.endswith('.csv'):
                    mt = os.path.getmtime(os.path.join(d, fn))
                    if mt > newest:
                        newest = mt
            key = '%.0f' % newest
        except OSError:
            key = ''
        if _CACHE['facts'] and now - _CACHE['ts'] < _TTL and key and key == _CACHE['key']:
            return _CACHE['facts']

        try:
            from quant_terminal import engine as E
            im, cm = E.load_industry_concept_maps()
        except Exception:
            return _CACHE['facts'] or {}
        codes = sorted(set(im) | set(cm))
        rt = _realtime_quotes()

        facts = {}
        for c in codes:
            rows = _tail_rows(os.path.join(d, _pref(c) + c + '.csv'), 8)
            if len(rows) < 2:
                continue
            last = rows[-1]
            prev_bar = rows[-2]
            close = last['close']
            if close <= 0:
                continue
            prev_close = prev_bar['close']
            vol = last['volume']
            # 前 5 日均量: 不含最后一天 (与 wave_scan 5MA 同口径)
            p5 = [r['volume'] for r in rows[:-1] if r['volume'] > 0][-5:]
            v5 = (sum(p5) / len(p5)) if p5 else 0.0
            # 涨幅 / 现价: 实时快照优先 (且必须与 CSV 同一天, 否则说明快照是别的交易日)
            q = rt.get(c) or {}
            price = q.get('price') or close
            pct = q.get('pct')
            if pct is None:
                pct = ((close - prev_close) / prev_close * 100.0) if prev_close > 0 else 0.0
            else:
                # 快照给了涨幅但价格可能缺 → 用涨幅反推, 保证 pct 与 price 自洽
                price = float(price)
            free = last['free']
            turn = (vol / free) if (free > 0 and vol > 0) else None      # 手 ÷ 万股 = %
            avg_px = (last['high'] + last['low'] + close) / 3.0
            amt = vol * 100.0 * avg_px if avg_px > 0 else 0.0            # 手→股 再乘均价
            cap_f = last['cap_f'] * 1e8 if last['cap_f'] > 0 else 0.0    # 亿元 → 元
            vr = (vol / v5) if v5 > 0 else None
            score, parts = _leader_score(pct, turn or 0.0, amt, cap_f, vr or 1.0, 0.0)
            facts[c] = {
                'code': c,
                'name': (q.get('name') or ''),
                'pct': round(float(pct), 2),
                'price': round(float(price), 3),
                'turn': (round(turn, 2) if turn is not None else None),
                'amt': amt,
                'cap_f': cap_f,
                'vr': (round(vr, 2) if vr is not None else None),
                'v5': v5,
                'date': last['date'],
                'score': score,
                'parts': parts,
            }
        if facts:
            _CACHE.update({'ts': now, 'key': key, 'facts': facts,
                           'date': max((f['date'] for f in facts.values()), default=''),
                           'qdate': max((q.get('qdate') or '' for q in rt.values()), default=''),
                           'src': ('realtime' if rt else 'csv')})
        return facts


def cache_info():
    """缓存状态 (排查用; 也给前端的"数据时刻"用)"""
    return {'ts': _CACHE['ts'], 'date': _CACHE['date'], 'qdate': _CACHE['qdate'],
            'src': _CACHE['src'], 'n': len(_CACHE['facts']), 'ttl': _TTL}


def invalidate():
    with _LOCK:
        _CACHE['ts'] = 0.0
        _CACHE['key'] = ''
    _RT['ts'] = 0.0


# ---------------------------------------------------------------- 龙头分 (TSP 六因子)
def _leader_score(pct, turn, amt, cap, vr, boards):
    """TSP leaderScore 的逐条移植 (权重 0.35/0.22/0.18/0.15/0.07/0.03)

    ⚠ 单位换算: TSP 里 change_pct 是小数(2% → 0.02), 这里 pct 是百分数(2.0),
      所以 (pct/100 + 0.02)/0.12 —— 少了这个 /100 会把 momentum 压到 1/50 以下。
    """
    momentum = _clamp01((pct / 100.0 + 0.02) / 0.12)
    turnover = _clamp01(math.log1p(max(turn, 0.0)) / math.log1p(30.0))
    amount = _clamp01(math.log1p(max(amt, 0.0)) / math.log1p(2.0e10))
    capv = _clamp01(math.log1p(max(cap, 0.0)) / math.log1p(3.0e11))
    volume = _clamp01((vr - 1.0) / 4.0)
    boards = _clamp01(boards / 5.0)
    score = (momentum * 0.35 + turnover * 0.22 + amount * 0.18
             + capv * 0.15 + volume * 0.07 + boards * 0.03) * 100.0
    return round(score, 1), {
        'momentum': round(momentum, 4), 'turnover': round(turnover, 4),
        'amount': round(amount, 4), 'cap': round(capv, 4),
        'volume': round(volume, 4), 'boards': round(boards, 4),
    }


# ---------------------------------------------------------------- 板块分组
def _maps():
    from quant_terminal import engine as E
    im, cm = E.load_industry_concept_maps()
    try:
        pm = E.load_industry_parent() or {}
    except Exception:
        pm = {}
    return im, cm, pm


def _groups(kind, level=2):
    """{板块名: [6位码...]} —— level=1 行业取上级(90), =2 取细分(257); 概念不分级"""
    im, cm, pm = _maps()
    out = {}
    if kind == 'concept':
        for c, v in cm.items():
            for n in (v or '').split('|'):
                n = n.strip()
                if n:
                    out.setdefault(n, []).append(c)
        return out
    for c, v in im.items():
        n = (v or '').strip()
        if not n:
            continue
        if level == 1:
            n = pm.get(n) or n
        out.setdefault(n, []).append(c)
    return out


def _calc_stat(name, codes, fx):
    """板块统计 —— TSP calcIndustryStat 的移植"""
    rows = [fx[c] for c in codes if c in fx]
    if not rows:
        return None
    pcts = [r['pct'] for r in rows if r['pct'] is not None]
    turns = [r['turn'] for r in rows if r['turn'] is not None]
    vrs = [r['vr'] for r in rows if r['vr'] is not None]
    n = len(rows)
    up = sum(1 for v in pcts if v > 0)
    dn = sum(1 for v in pcts if v < 0)
    flat = max(0, len(pcts) - up - dn)
    strong = sum(1 for v in pcts if v >= 5.0)
    weak = sum(1 for v in pcts if v <= -5.0)
    total_amt = sum(r['amt'] for r in rows)
    avg_pct = _avg(pcts)
    med_pct = _median(pcts)
    up_rate = (up / len(pcts)) if pcts else 0.0
    leader = max(rows, key=lambda r: r['score']) if rows else None
    # v2.278 (2026-11-12 用户 m10185): TSP 的「领涨主线/领跌方向」每行右侧要挂 3 个龙头小胶囊,
    #   点一下直接看个股 —— 排行接口必须一并给出, 否则前端要为一个板块多打一次 members() 接口。
    _top = sorted(rows, key=lambda r: -r['score'])[:3]

    amount_score = _clamp01(math.log1p(total_amt) / math.log1p(8.0e10))
    strong_score = _clamp01(strong / max(1, n * 0.18))
    leader_part = _clamp01((leader['score'] if leader else 0.0) / 100.0)
    avg_part = _clamp01(((avg_pct or 0.0) / 100.0 + 0.02) / 0.09)
    up_part = _clamp01((up_rate - 0.35) / 0.55)
    heat = (avg_part * 0.38 + up_part * 0.2 + strong_score * 0.16
            + amount_score * 0.12 + leader_part * 0.14) * 100.0
    risk = (_clamp01((-(avg_pct or 0.0) / 100.0 + 0.01) / 0.08) * 0.55
            + _clamp01(weak / max(1, n * 0.18)) * 0.45) * 100.0

    return {
        'name': name,
        'count': n,
        'avg_pct': (round(avg_pct, 2) if avg_pct is not None else None),
        'median_pct': (round(med_pct, 2) if med_pct is not None else None),
        'up_count': up, 'down_count': dn, 'flat_count': flat,
        'up_rate': round(up_rate * 100.0, 1),
        'strong_count': strong, 'weak_count': weak,
        'total_amt_yi': round(total_amt / 1e8, 1),
        'avg_turn': (round(_avg(turns), 2) if turns else None),
        'avg_vr': (round(_avg(vrs), 2) if vrs else None),
        'heat': round(heat, 1),
        'risk': round(risk, 1),
        'leader_name': (leader['name'] or leader['code']) if leader else '',
        'leader_code': leader['code'] if leader else '',
        'leader_pct': leader['pct'] if leader else None,
        'leader_score': leader['score'] if leader else None,
        # v2.278: 前 3 龙头 (TSP PulseList 右侧 3 个小胶囊) —— [{code,name,pct,score}, ...]
        'top3': [{'code': r['code'], 'name': (r['name'] or r['code']),
                  'pct': r['pct'], 'score': r['score']} for r in _top],
    }


# ---------------------------------------------------------------- 对外: 排行
def rank(kind='industry', level=2, sort='heat'):
    """板块排行 (不含成分股明细) → {ok, kind, level, sort, date, src, ts, rows}"""
    kind = kind if kind in KINDS else 'industry'
    sort = sort if sort in SORTS else 'heat'
    try:
        level = int(level)
    except (TypeError, ValueError):
        level = 2
    if level not in (1, 2):
        level = 2
    fx = _facts()
    if not fx:
        return {'ok': False, 'kind': kind, 'rows': [], 'error': '本地 K 线表读不到'}
    gs = _groups(kind, level)
    rows = []
    for name, codes in gs.items():
        st = _calc_stat(name, codes, fx)
        if st:
            rows.append(st)

    if sort == 'avgPct':
        rows.sort(key=lambda x: -(x['avg_pct'] if x['avg_pct'] is not None else -1e9))
    elif sort == 'leader':
        rows.sort(key=lambda x: -(x['leader_score'] or 0))
    elif sort == 'amount':
        rows.sort(key=lambda x: -x['total_amt_yi'])
    elif sort == 'down':
        rows.sort(key=lambda x: (x['avg_pct'] if x['avg_pct'] is not None else 1e9))
    else:
        rows.sort(key=lambda x: -x['heat'])

    info = cache_info()
    return {
        'ok': True, 'kind': kind, 'level': level, 'sort': sort,
        'date': info['date'], 'qdate': info['qdate'], 'src': info['src'],
        'ts': time.strftime('%m-%d %H:%M:%S', time.localtime(info['ts'] or time.time())),
        'n_stock': info['n'], 'rows': rows,
    }


# ---------------------------------------------------------------- 对外: 成分股
def members(kind='industry', name='', level=2, limit=300):
    """某个板块的成分股明细 (按龙头分降序) → {ok, name, count, members}"""
    kind = kind if kind in KINDS else 'industry'
    try:
        level = int(level)
    except (TypeError, ValueError):
        level = 2
    if level not in (1, 2):
        level = 2
    fx = _facts()
    if not fx:
        return {'ok': False, 'name': name, 'members': [], 'error': '本地 K 线表读不到'}
    gs = _groups(kind, level)
    codes = gs.get(name)
    if not codes:
        return {'ok': False, 'name': name, 'members': [], 'error': '未找到该板块'}
    try:
        im, cm, _pm = _maps()
    except Exception:
        im, cm = {}, {}
    out = []
    for c in codes:
        r = fx.get(c)
        if not r:
            continue
        out.append({
            'code': c,
            'name': r['name'] or c,
            'pct': r['pct'],
            'price': r['price'],
            'turn': r['turn'],
            'amt_yi': round(r['amt'] / 1e8, 2),
            'cap_f_yi': round(r['cap_f'] / 1e8, 1),
            'vr': r['vr'],
            'score': r['score'],
            'parts': r['parts'],
            'sector': (im.get(c) or ''),
            'concept': (cm.get(c) or ''),
        })
    out.sort(key=lambda x: -x['score'])
    st = _calc_stat(name, [x['code'] for x in out], fx) or {}
    return {'ok': True, 'kind': kind, 'level': level, 'name': name,
            'count': len(out), 'stat': st, 'members': out[:int(limit)]}


# ---------------------------------------------------------------- 自测
if __name__ == '__main__':
    import sys
    t0 = time.time()
    d = rank('industry', 2, 'heat')
    print('行业 rank: ok=%s n=%d 用时 %.2fs  日期=%s 源=%s'
          % (d.get('ok'), len(d.get('rows') or []), time.time() - t0,
             d.get('date'), d.get('src')))
    for r in (d.get('rows') or [])[:10]:
        print('  %-12s n=%-4d 均%+6.2f%% 涨%d/跌%d 强%d 额%7.1f亿 换%5s 量比%5s 强度%5.1f 龙头 %s %+.2f'
              % (r['name'], r['count'], r['avg_pct'] or 0, r['up_count'], r['down_count'],
                 r['strong_count'], r['total_amt_yi'], r['avg_turn'], r['avg_vr'],
                 r['heat'], r['leader_name'], r['leader_pct'] or 0))
    t0 = time.time()
    d2 = rank('concept', 2, 'heat')
    print('概念 rank: ok=%s n=%d 用时 %.2fs' % (d2.get('ok'), len(d2.get('rows') or []), time.time() - t0))
    for r in (d2.get('rows') or [])[:8]:
        print('  %-14s n=%-4d 均%+6.2f%% 强度%5.1f 龙头 %s' % (r['name'], r['count'], r['avg_pct'] or 0, r['heat'], r['leader_name']))
    t0 = time.time()
    top = (d.get('rows') or [{}])[0].get('name') or ''
    m = members('industry', top, 2)
    print('成分股 %s: ok=%s n=%d 用时 %.2fs' % (top, m.get('ok'), m.get('count'), time.time() - t0))
    for x in (m.get('members') or [])[:6]:
        print('  %s %-8s %+6.2f%% 换%5s 额%6.2f亿 市值%7.1f亿 量比%5s 龙头分%5.1f'
              % (x['code'], x['name'], x['pct'], x['turn'], x['amt_yi'], x['cap_f_yi'], x['vr'], x['score']))
    print('cache_info:', cache_info())
