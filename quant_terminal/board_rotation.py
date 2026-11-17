# -*- coding: utf-8 -*-
"""v2.281 (2026-11-12 用户 m10425 第⑤项 + m10906): 板块涨幅轮动矩阵（RPS 轮动分析）
   位置: 板块大窗口右上角「涨幅RPS轮动分析」按钮 → 弹一个居中的矩阵窗

【数据从哪来 —— 关键结论，不用攒历史】
   用户 m10906 问「TSP 里面不是有 30 日的数据吗，不能从那边取历史数据吗？」
   ⇒ 看过 TSP 的实现（backend/app/services/rps_rotation.py）后确认：
     TSP 也**不存任何快照**，它是每次现算的：
       ① 取最近 N 天所有个股的 change_pct（它有自己的 enriched 缓存）
       ② 个股 → 板块（概念/行业）映射，一只票拆成多行
       ③ 按 (日期, 板块) 求平均涨幅
       ④ 每个日期内按平均涨幅降序
   我们的本地 K 线 CSV 里**每一天的价格都有** ⇒ 涨幅能直接算出来，
   板块成分映射也是现成的（engine.load_industry_concept_maps，5,584 只 / 257 行业 / 351 概念）。
   **所以第一打开就有完整 30 天，不用等积累。**

【口径】
   个股日涨幅 = (今收 - 昨收) / 昨收 * 100   （百分数，与全终端一致）
   板块日涨幅 = 成分股日涨幅的**简单平均**（TSP 同款，停牌/无行情的票不参与）
   行业层级: level=1 取上级行业(90 类) / level=2 取细分行业(257 类)
   排序: 每个日期列内降序，只保留所有日期都出现的板块（避免某天缺票导致矩阵错位）
"""
import os
import sys
import time
import threading

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
for _p in (ROOT, HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ---------------------------------------------------------------- 常量
KINDS = ('industry', 'concept')
_LEVEL_OK = (1, 2)
DAYS_MIN, DAYS_MAX, DAYS_DEF = 7, 30, 30

_CACHE = {}          # {(kind, level, days, sig): (ts, payload)}
_CACHE_TTL = 120.0
_LOCK = threading.Lock()


# ---------------------------------------------------------------- 基础
def kline_dir():
    try:
        from quant_terminal import datasource
        d = datasource.resolve(datasource.get('local', 'kline_dir', ''))
        if d and os.path.isdir(d):
            return d
    except Exception:
        pass
    d = os.path.join(ROOT, 'kline_data', 'daily')
    return d


def _read_tail(path, n):
    """倒读文件尾 n 行（不用 engine._read_csv_tail: 它只解析到第 6 列）"""
    try:
        with open(path, 'rb') as f:
            f.seek(0, 2)
            sz = f.tell()
            pos = sz
            buf = b''
            while pos > 0 and buf.count(b'\n') <= n + 1:
                step = min(65536, pos)
                pos -= step
                f.seek(pos)
                buf = f.read(step) + buf
            txt = buf.decode('utf-8', 'ignore')
    except Exception:
        return []
    lines = [l for l in txt.split('\n') if l.strip()]
    if pos > 0 and lines:
        lines = lines[1:]          # 丢掉被截断的半行
    return lines[-n:]


def _num(s):
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def _parse(line):
    """date,open,close,high,low,volume[,总市值,流通市值,自由流通股本]"""
    p = line.split(',')
    if len(p) < 6:
        return None
    c = _num(p[2])
    if c is None or c <= 0:
        return None
    return {'date': p[0].strip(), 'close': c}


def _pref(code):
    c = str(code or '').strip()
    if c[:3] == '920' or c[:1] in ('4', '8'):
        return 'BZ'
    if c[:1] in ('6', '9'):
        return 'SH'
    return 'SZ'


def _maps():
    from quant_terminal import engine
    im, cm = engine.load_industry_concept_maps()
    return im or {}, cm or {}


def _industry_parent():
    try:
        from quant_terminal import engine
        return engine.load_industry_parent() or {}
    except Exception:
        return {}


# ---------------------------------------------------------------- 核心
def _stock_closes(n_days):
    """{code: [(date, close), ...]} —— 每只票最近 n_days+1 根（多一根算第一天涨幅）"""
    d = kline_dir()
    if not os.path.isdir(d):
        return {}, ''
    out = {}
    latest = ''
    want = n_days + 1
    try:
        names = [x for x in os.listdir(d) if x.endswith('.csv')]
    except Exception:
        return {}, ''
    for fn in names:
        base = fn[:-4]
        if base[:2] not in ('SH', 'SZ', 'BZ'):
            continue
        code = base[2:]
        if not code.isdigit():
            continue
        lines = _read_tail(os.path.join(d, fn), want)
        rows = []
        for ln in lines:
            r = _parse(ln)
            if r:
                rows.append((r['date'], r['close']))
        if len(rows) < 2:
            continue
        out[code] = rows
        if rows[-1][0] > latest:
            latest = rows[-1][0]
    return out, latest


def _pct_by_date(rows, dates):
    """某只票 → {日期: 涨幅%}（只在 dates 里的日期上算，缺前一根就跳过）"""
    m = {}
    for i in range(1, len(rows)):
        d, c = rows[i]
        if d not in dates:
            continue
        pc = rows[i - 1][1]
        if pc and pc > 0:
            m[d] = (c - pc) / pc * 100.0
    return m


def rotation(kind='industry', level=2, days=DAYS_DEF, force=False):
    """板块涨幅轮动矩阵。

    返回: {ok, kind, level, days, dates:[最新在前], columns:{日期:[[板块, 涨幅%], ...]},
           count, n_stock, latest, elapsed}
    """
    t0 = time.time()
    kind = kind if kind in KINDS else 'industry'
    try:
        level = int(level)
    except (TypeError, ValueError):
        level = 2
    if level not in _LEVEL_OK:
        level = 2
    try:
        days = int(days)
    except (TypeError, ValueError):
        days = DAYS_DEF
    days = max(DAYS_MIN, min(DAYS_MAX, days))

    im, cm = _maps()
    if not im:
        return {'ok': False, 'error': '行业/概念映射读不到', 'dates': [], 'columns': {}, 'count': 0}

    # 需要多少个交易日：多读 5 根余量（停牌票的尾巴会短）
    closes, latest = _stock_closes(days + 5)
    if not closes:
        return {'ok': False, 'error': '本地 K 线读不到', 'dates': [], 'columns': {}, 'count': 0}

    sig = (len(closes), latest)
    ck = (kind, level, days, sig)
    now = time.time()
    if not force:
        with _LOCK:
            got = _CACHE.get(ck)
        if got and (now - got[0]) < _CACHE_TTL:
            return got[1]

    # 交易日列表 = 取所有票日期并集的最后 days 个
    alld = set()
    for rows in closes.values():
        alld.add(rows[-1][0])
    dset = set()
    for rows in closes.values():
        for dd, _c in rows[-days - 1:]:
            dset.add(dd)
    dates = sorted(dset)[-days:]
    if not dates:
        return {'ok': False, 'error': '没有可用交易日', 'dates': [], 'columns': {}, 'count': 0}
    dset = set(dates)

    # 板块 → 累计
    pm = _industry_parent() if kind == 'industry' else {}
    agg = {}       # {(date, name): [sum, cnt]}
    for code, rows in closes.items():
        # 取板块名
        if kind == 'industry':
            nm = im.get(code)
            if not nm:
                continue
            if level == 1:
                nm = pm.get(nm) or nm
        else:
            raw = cm.get(code)
            if not raw:
                continue
            # 概念是 "A|B|C"，一只票属于多个概念 → 每个都累一次
            pm2 = _pct_by_date(rows, dset)
            if not pm2:
                continue
            for cname in [x for x in raw.split('|') if x.strip()]:
                for dd, pv in pm2.items():
                    k = (dd, cname)
                    a = agg.get(k)
                    if a is None:
                        agg[k] = [pv, 1]
                    else:
                        a[0] += pv
                        a[1] += 1
            continue
        pm2 = _pct_by_date(rows, dset)
        if not pm2:
            continue
        for dd, pv in pm2.items():
            k = (dd, nm)
            a = agg.get(k)
            if a is None:
                agg[k] = [pv, 1]
            else:
                a[0] += pv
                a[1] += 1

    # 拆成 {date: [(name, avg)]}
    byd = {}
    names_all = set()
    for (dd, nm), (s, n) in agg.items():
        if n <= 0:
            continue
        byd.setdefault(dd, []).append((nm, s / n))
        names_all.add(nm)

    # 只保留「所有日期都出现」的板块：矩阵要求每列对齐（某天缺票会让行错位）
    keep = None
    for dd in dates:
        cur = set(x[0] for x in byd.get(dd) or [])
        keep = cur if keep is None else (keep & cur)
    keep = keep or set()

    columns = {}
    for dd in dates:
        lst = [(nm, round(v, 4)) for nm, v in (byd.get(dd) or []) if nm in keep]
        lst.sort(key=lambda x: -x[1])
        columns[dd] = lst

    payload = {
        'ok': True, 'kind': kind, 'level': level, 'days': len(dates),
        'dates': list(reversed(dates)),      # 最新在最前（与 TSP 一致）
        'columns': columns,
        'count': len(keep),
        'n_stock': len(closes),
        'latest': latest,
        'elapsed': round(time.time() - t0, 2),
    }
    with _LOCK:
        _CACHE[ck] = (now, payload)
    return payload


def invalidate():
    with _LOCK:
        _CACHE.clear()


if __name__ == '__main__':
    t = time.time()
    for kind, lv in (('industry', 2), ('industry', 1), ('concept', 2)):
        d = rotation(kind, lv, 30)
        print('%-9s L%d  ok=%s 板块=%d 日期=%d 股票=%d  %.2fs' % (
            kind, lv, d.get('ok'), d.get('count', 0), len(d.get('dates') or []),
            d.get('n_stock', 0), d.get('elapsed', 0)))
        ds = d.get('dates') or []
        if ds:
            for dd in ds[:3]:
                top = (d['columns'].get(dd) or [])[:4]
                print('   %s  ' % dd + ' | '.join('%s %+.2f%%' % (n, v) for n, v in top))
    print('total %.2fs' % (time.time() - t))
