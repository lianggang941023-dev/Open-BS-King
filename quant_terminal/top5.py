# -*- coding: utf-8 -*-
"""v2.242 (2026-10-30 用户 m20347/m20454): 三个趋势窗口的「成分股最大市值多空」与「topN 多空比」。

用户定义的口径（必须逐字遵守）：
  · 现价 > MA5 > MA10 > MA21(> MA55 ...)   ⇒ **多**
  · 现价 ≤ MA5 但均线本身仍严格递减         ⇒ **中性**
  · 均线排列不按顺序                        ⇒ **空**
  · 均线条数 = 当前档位（3线=5/10/21, 4线=+55, 5线=+120, 6线=+250）
  · topN 计分：多=1, 中性=0.5, 空=0，格式 `top5 2.5/5`
  · N = min(5, 能算出状态的只数)  ⇒ 方案A：按总市值降序**跳过算不出状态的**，能算几个取几个
    （不足 5 家真实存在：行业里有 top4/top3/top2，所以 N 必须动态）

数据来源（零新依赖，全部本地）：
  · 成分股  sector_kline.members()  710 个板块 / 0.02s
  · 市值    个股日线 CSV 的**第 7 列**（每一行都带，不只末行）⇒ 读尾即可
  · 均线    同一次读尾就能算，不需要再读一遍
  · 名称    engine.load_stock_name_map()
  · 现价    盘中叠实时价（与板块档位同口径），盘后用末根收盘

性能：全市场 5585 只读尾一次 ~2s，719 个板块（细分 257 + 概念 351 + 上级 90）一次算完；
      结果按 (档位, 数据日, 是否叠实时) 落内存缓存，二次开窗 0ms。
"""
import os
import threading
import time

from quant_terminal import sector_kline as SK

MAXN = 5
# 读尾字节数按档位算：6 线要 250 根收盘价，每行 ~60 字节 ⇒ 250*70+800 ≈ 18KB 足够。
# 原来写死 200KB，实测 710 个板块要 9.08s（5584 个文件每个多读 10 倍数据）；
# 改成按需读之后同样的机器 ~2s。
_ROW_BYTES = 70
_MIN_TAIL = 12000
# 缓存按 (档位|口径) 分槽 —— 原来只有一格，从 4 线切到 6 线再切回来就得重算 4.5 秒
_CACHE = {}
# 基础缓存：{code: {'mcap','date','closes'(最近 250 根)}，与档位无关，TTL 60 秒。
#   实测：4线冷启 1.04s、5线 1.40s、6线 4.47s —— 差别全在「要不要多读那几根」，
#   而每只票读盘的成本几乎恒定 ⇒ 一次读满 250 根，四个档位共用，换档只重算状态。
_BASE = {'ts': 0.0, 'date': '', 'd': {}}
_LOCK = threading.Lock()


def _tail_bytes(periods):
    return max(_MIN_TAIL, (max(periods) + 8) * _ROW_BYTES)


def _csv_path(code):
    return SK._csv_path(code)


def _tail_rows(path, nbytes=None):
    """读 CSV 尾部 → [[date, open, close, high, low, volume, total, float], ...]

    ⚠ 必须二进制读 + errors='ignore'：直接 seek 到中间会在半行处切出非法字节，
      utf-8 解码会抛 UnicodeDecodeError（勘察时踩过）。被截断的首行直接丢掉。
    """
    if not nbytes:
        nbytes = _MIN_TAIL
    with open(path, 'rb') as f:
        f.seek(0, os.SEEK_END)
        fs = f.tell()
        start = fs - nbytes if fs > nbytes else 0
        # ⚠ 必须无条件 seek 回去 —— 文件比 nbytes 小的时候 start==0，
        #   而 seek(0,SEEK_END) 已经把指针留在文件末尾了；少了这一步就读出空字节
        #   （2026-10-30 实测：rows 恒为 0，all-codes 全部落空）。
        f.seek(start)
        raw = f.read()
    txt = raw.decode('utf-8', 'ignore')
    lines = txt.split('\n')
    if start:
        lines = lines[1:]                     # 丢掉被切一半的首行
    out = []
    for ln in lines:
        if not ln or ln.startswith('date'):
            continue
        p = ln.split(',')
        if len(p) < 6:
            continue
        out.append(p)
    return out


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _status(closes, periods, price):
    """三态判定。返回 '多' / '中' / '空'，K 线不够长返回 None。"""
    need = max(periods)
    if len(closes) < need or price <= 0:
        return None
    mas = [sum(closes[-p:]) / p for p in periods]
    ordered = all(mas[i] > mas[i + 1] for i in range(len(mas) - 1))
    if not ordered:
        return '空'
    return '多' if price > mas[0] else '中'


def _load_base():
    """读一遍全市场 CSV 尾部 → {code: {'mcap','date','closes'}} + 最新交易日。

    ⚠ 读满 **250 根**（6 线档的最长均线），四个档位共用这一份，换档不重读盘。
      实测：只读 30 根是 1.0s、读 250 根是 4.5s，但读一次管四个档位，比每次重读划算。
    """
    now = time.time()
    if _BASE['d'] and (now - _BASE['ts']) < 60:
        return _BASE['d'], _BASE['date']
    try:
        trades = SK.tickers()
    except Exception:
        trades = []
    names_ok = set(trades) if trades else None
    ddir = SK.DAILY_DIR
    try:
        files = os.listdir(ddir)
    except OSError:
        return {}, ''
    # 250 根 × 70 字节 + 余量
    tbn = _tail_bytes((250,))
    out = {}
    latest = ''
    for fn in files:
        if not fn.lower().endswith('.csv'):
            continue
        code = fn[:-4]
        # 文件名是 BZ920000 / SH600519 / SZ000001 —— 前两个字母是市场前缀，必须剥掉，
        # 否则 bare 变成 'BZ920000' 这种非数字串，tickers() 比对全部落空
        # （2026-10-30 实测：stocks 恒为 0；勘察时按老代码写的是 split('_')，那是别的命名口径）。
        bare = code[2:] if len(code) > 2 and code[:2].isalpha() else code
        if not bare.isdigit():
            continue
        if names_ok is not None and bare not in names_ok:
            continue
        fp = os.path.join(ddir, fn)
        try:
            rows = _tail_rows(fp, tbn)
        except OSError:
            continue
        if not rows:
            continue
        last = rows[-1]
        d = last[0][:10]
        if d > latest:
            latest = d
        try:
            closes = [float(r[2]) for r in rows[-255:]]
        except (ValueError, IndexError):
            continue
        if len(closes) < 6:
            continue
        mcap = _f(last[6]) if len(last) > 6 else 0.0
        out[bare] = {'mcap': mcap, 'date': d, 'closes': closes}
    # 第二遍：把不是最新交易日的票剔掉（停牌/退市的老数据不参与排序，
    #   否则一个三年前退市的巨无霸会永远霸占 top1）
    if latest:
        out = dict((k, v) for k, v in out.items() if v['date'] == latest)
    _BASE['ts'] = now
    _BASE['date'] = latest
    _BASE['d'] = out
    return out, latest


def _scan_stocks(periods, rt=None):
    """{code: {'mcap','st','date','close'}} —— 从基础缓存算当前档位的三态。"""
    base, latest = _load_base()
    out = {}
    need = max(periods)
    for code, v in base.items():
        closes = v['closes']
        if len(closes) < need:
            out[code] = {'mcap': v['mcap'], 'st': None, 'date': v['date'], 'close': closes[-1]}
            continue
        price = closes[-1]
        if rt:
            p = rt.get(code)
            if p:
                price = float(p)
        out[code] = {'mcap': v['mcap'], 'st': _status(closes, periods, price),
                     'date': v['date'], 'close': closes[-1]}
    return out, latest


def _board_members():
    """{thscode: [code, ...]} —— 710 个板块（含 881 上级 / 884 细分 / 885-886 概念）"""
    try:
        return SK.members()
    except Exception:
        return {}


def build(lines='4', rt=None, force=False):
    """算一份完整的 {板块名: {'max_name','max_code','max_st','top_n','top_score'}}"""
    periods = SK_ENGINE_LINES.get(str(lines)) or (5, 10, 21, 55)
    key = '%s|%s' % (lines, 'rt' if rt else 'close')
    now = time.time()
    if not force:
        c = _CACHE.get(key) or {}
        # v2.294 (2026-11-13 用户 m14431): 60 -> 1800 —— 上级行业趋势窗 5.5s 冷启
        #   就是因为这个 60 秒过期; 数据本身按 (lines, rt/close) 分 key, 交易日变化会自然重算。
        if c.get('data') and (now - c.get('ts', 0)) < 1800:
            return c['data']
    with _LOCK:
        stocks, latest = _scan_stocks(periods, rt)
        if not stocks:
            return {}
        try:
            from quant_terminal import engine as E
            nmap = E.load_stock_name_map()
        except Exception:
            nmap = {}
        mem = _board_members()
        try:
            nms = SK.names()
        except Exception:
            nms = {}
        data = {}
        for tc, codes in mem.items():
            info = nms.get(tc) or {}
            bname = info.get('name') or ''
            if not bname:
                continue
            cand = []
            for c in codes:
                s = stocks.get(c)
                if not s or s['mcap'] <= 0:
                    continue
                cand.append((s['mcap'], c, s))
            if not cand:
                continue
            cand.sort(key=lambda x: -x[0])
            picked = []
            for mcap, c, s in cand:
                if s['st'] is None:
                    continue                 # 方案A: 算不出状态的跳过, 不当 0 分
                picked.append((mcap, c, s))
                if len(picked) >= MAXN:
                    break
            if not picked:
                continue
            top_mcap, top_code, top_s = picked[0]
            score = 0.0
            for _m, _c, s in picked:
                score += 1.0 if s['st'] == '多' else (0.5 if s['st'] == '中' else 0.0)
            data[bname] = {
                'max_name': nmap.get(top_code) or top_code,
                'max_code': top_code,
                'max_st': top_s['st'],
                'top_n': len(picked),
                'top_score': score,
                'top_cnt': ''.join([{'多': '多', '中': '中', '空': '空'}[x[2]['st']] for x in picked]),
            }
        _CACHE[key] = {'ts': now, 'data': data, 'date': latest}
        return data


def attach(rows, lines='4', rt=None, force=False):
    """给 rows（原地）补五个字段：mcap_max_name / mcap_max_code / mcap_max_st / mcap_top_n / mcap_top_score"""
    if not rows:
        return rows
    d = build(lines, rt, force)
    if not d:
        return rows
    for r in rows:
        nm = r.get('name') or ''
        v = d.get(nm)
        if v:
            r['mcap_max_name'] = v['max_name']
            r['mcap_max_code'] = v.get('max_code') or ''
            r['mcap_max_st'] = v['max_st']
            r['mcap_top_n'] = v['top_n']
            r['mcap_top_score'] = v['top_score']
        else:
            r['mcap_max_name'] = ''
            r['mcap_max_code'] = ''
            r['mcap_max_st'] = ''
            r['mcap_top_n'] = 0
            r['mcap_top_score'] = None
    return rows


def status():
    out = {}
    now = time.time()
    for k, c in _CACHE.items():
        out[k] = {'age': round(now - c.get('ts', 0), 1),
                  'boards': len(c.get('data') or {}), 'date': c.get('date', '')}
    return out


SK_ENGINE_LINES = {'3': (5, 10, 21), '4': (5, 10, 21, 55),
                   '5': (5, 10, 21, 55, 120), '6': (5, 10, 21, 55, 120, 250)}
