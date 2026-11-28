# -*- coding: utf-8 -*-
"""海外指数取数 (v2.409, 用户 m11025)

为什么单独一个模块
------------------
A 股指数走 engine 的腾讯/新浪/同花顺/东财四条通路, 代码是 6 位数字。海外指数代码是
带点前缀的 (us.DJI / hk.HSI ...), 源也完全不同; 塞进 engine._fetch_index_full 会把那个
函数变成一个大分派体。这里独立一份, engine / idx_store 只在 src_of(code)=='overseas'
时**按需 import** (与 ths_index / etf_src 同一手法, 避免循环 import)。

四条日K源 (全部实测, 2026-11-28)
---------------------------------
- kind='us'      新浪美股 JSONP  US_MinKService.getDailyK?symbol=.DJI
                 道指 5765 根(2004-01-02 起) / 纳指 5763 / 纳100 3214 / 标普 5766。
                 **不需要 py_mini_racer** —— akshare 的 index_us_stock_sina 才要 (它去解
                 finance.sina.com.cn/staticdata/us 那段 JS 混淆), 直连这个 JSONP 就够。
- kind='gi'      新浪国际指数   gi.finance.sina.com.cn/hq/daily?symbol=NKY&num=1000
                 日经/韩国/台湾/欧洲/印度/澳洲, **各 1000 根(2022-10 起)**。
                 ⚠ num 硬上限 1000: num=2000/5000、page/start/offset 全部无效(实测同样 1000 根)。
- kind='tencent' 腾讯 fqkline   hkHSI -> 1000 根; 列序与 engine._norm_tencent_bars 完全一致。
- kind='a50'     新浪期货 JSONP getGlobalFuturesDailyKLine?symbol=CHA50CFD
                 2594 根(2016-11-28 起), 字段 date/open/high/low/close/volume/position。

分时源 (只有一条)
-----------------
东财 trends2 —— **必须 http, 不能 https**:
    http://push2his.eastmoney.com/api/qt/stock/trends2/get?secid=100.DJIA&ndays=1&iscr=0...
    -> data.trends 每行 "YYYY-MM-DD HH:MM,价格,均价"; data.preClose / data.name
    实测 12 个海外指数全通 (211~511 点), A50 期货用 104.CN00Y (751 点, 16:45~次日 05:15)。
    🔴 东财 stock/kline/get (日K) **https 与 http 都 RemoteDisconnected** —— 日K 绝不用东财。

🔴 东财 secid 100.NDX 的坑 (实测确认)
-------------------------------------
它 `f58` 报的 name 是「纳斯达克」, 但数值 24129.50 / 昨收 23585.80 与新浪 `gb_$ixic`
(纳斯达克**综合**指数) 完全一致 —— 也就是说 **100.NDX 实际就是纳斯达克综合指数**,
而真正的纳斯达克100 (26726.39) 东财没有分时。所以:
    us.IXIC (纳指综合) -> secid 100.NDX  ✅ 有分时
    us.NDX  (纳指100)  -> 无 secid      ❌ 无分时 (只能看日K)

实时盘中 bar
------------
本轮**不做** live_bar。理由: ① 海外各市场交易时段与 A 股完全不同, engine 的
_is_trade_window() 只对 A 股有意义; ② 硬造一个「今天」的 bar 会在周末/休市日往
CSV 里写假行; ③ /api/idx_list 的设计前提是「只读本地存档零网络」。分时的实时性由
mini_minute 那条路承担, 列表页显示上一交易日收盘与涨跌幅 (与各家行情软件休市时一致)。
"""

import io
import json
import os
import re
import time

# 代码 -> 取数描述。key 一律小写 (idx_registry.norm() 只做 strip().lower() 查表)。
#   kind : us / gi / tencent / a50 —— 决定日K走哪条源
#   sym  : 该源里的代码
#   secid: 东财分时 secid (空 = 没有分时源)
#   group: 'us' / 'asia' / 'eu' / 'other' —— 只用于日志分组, 不参与逻辑
CODES = {
    # ── 美股 ────────────────────────────────────────────────────────────
    'us.dji':   {'name': '道琼斯',        'short': '道指',    'kind': 'us', 'sym': '.DJI',
                 'secid': '100.DJIA',  'group': 'us'},
    'us.ixic':  {'name': '纳斯达克',      'short': '纳指',    'kind': 'us', 'sym': '.IXIC',
                 'secid': '100.NDX',   'group': 'us'},
    'us.ndx':   {'name': '纳斯达克100',   'short': '纳100',   'kind': 'us', 'sym': '.NDX',
                 'secid': '',          'group': 'us'},
    'us.inx':   {'name': '标普500',       'short': '标普',    'kind': 'us', 'sym': '.INX',
                 'secid': '100.SPX',   'group': 'us'},

    # ── 亚太 ────────────────────────────────────────────────────────────
    'hk.hsi':   {'name': '恒生指数',      'short': '恒生',    'kind': 'tencent', 'sym': 'hkHSI',
                 'secid': '100.HSI',   'group': 'asia'},
    'jp.n225':  {'name': '日经225',       'short': '日经',    'kind': 'gi', 'sym': 'NKY',
                 'secid': '100.N225',  'group': 'asia'},
    'kr.ks11':  {'name': '韩国KOSPI',     'short': 'KOSPI',   'kind': 'gi', 'sym': 'KOSPI',
                 'secid': '100.KS11',  'group': 'asia'},
    'tw.twii':  {'name': '台湾加权',      'short': '台湾',    'kind': 'gi', 'sym': 'TWJQ',
                 'secid': '100.TWII',  'group': 'asia'},
    'au.as51':  {'name': '澳大利亚标普200', 'short': 'AS51',  'kind': 'gi', 'sym': 'AS51',
                 'secid': '100.AS51',  'group': 'asia'},

    # ── 欧洲 ────────────────────────────────────────────────────────────
    'uk.ftse':  {'name': '英国富时100',   'short': '富时100', 'kind': 'gi', 'sym': 'UKX',
                 'secid': '100.FTSE',  'group': 'eu'},
    'de.gdaxi': {'name': '德国DAX30',     'short': 'DAX',     'kind': 'gi', 'sym': 'DAX',
                 'secid': '100.GDAXI', 'group': 'eu'},
    'fr.fchi':  {'name': '法国CAC40',     'short': 'CAC40',   'kind': 'gi', 'sym': 'CAC',
                 'secid': '100.FCHI',  'group': 'eu'},

    # ── 其它 ────────────────────────────────────────────────────────────
    'in.sensex': {'name': '印度孟买SENSEX', 'short': 'SENSEX', 'kind': 'gi', 'sym': 'SENSEX',
                  'secid': '100.SENSEX', 'group': 'other'},
    'sg.cn00y': {'name': '富时中国A50期货', 'short': 'A50',    'kind': 'a50', 'sym': 'CHA50CFD',
                 'secid': '104.CN00Y', 'group': 'other', 'div': 10},
}

ORDER = ('us.dji', 'us.ixic', 'us.ndx', 'us.inx',
         'hk.hsi', 'jp.n225', 'kr.ks11', 'tw.twii', 'au.as51',
         'uk.ftse', 'de.gdaxi', 'fr.fchi', 'in.sensex', 'sg.cn00y')

_UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
       '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
_TIMEOUT = 20

# 分时（东财 trends2）—— 只报给前端做「数据来源」提示用，取数在 minute() 里。
#   🔴 必须 http, https 那条 RemoteDisconnected。
MINUTE_URL = 'http://push2his.eastmoney.com/api/qt/stock/trends2/get?secid={secid}'


# ── 基础 HTTP ──────────────────────────────────────────────────────────────
def _http(url, ref, timeout=None, enc='utf-8'):
    """直连 GET (绕开系统代理)。

    为什么要自己写而不是复用 engine._req_get: 本函数只打境内外这几家
    (新浪/腾讯/东财), 直连永远比走 SSR 代理快 4~11 倍; engine._req_get 会
    在失败时退回系统代理, 对海外源反而是"越退越慢"。这里只直连, 失败就失败。
    """
    import urllib.request
    req = urllib.request.Request(url, headers={'User-Agent': _UA, 'Referer': ref})
    r = urllib.request.urlopen(req, timeout=timeout or _TIMEOUT)
    raw = r.read()
    if (r.headers.get('Content-Encoding') or '').lower() == 'gzip':
        import gzip
        raw = gzip.decompress(raw)
    return raw.decode(enc, 'replace')


def _jsonp(txt):
    """剥掉 `/*<script>...*/` 前缀 + `var _=(` ... `);` 包装, 返回 python 对象。"""
    t = (txt or '').strip()
    t = re.sub(r'^/\*.*?\*/', '', t, flags=re.S).strip()
    i = t.find('=')
    if t.startswith('var _') and i > 0:
        t = t[i + 1:].strip()
    t = t.strip().rstrip(';').strip()
    if t.startswith('('):
        t = t[1:]
    if t.endswith(')'):
        t = t[:-1]
    return json.loads(t)


def _norm(rows):
    """按日期去重 + 升序 + 剔坏行。返回 [{date,open,close,high,low,volume}]。"""
    seen = {}
    for r in rows or []:
        d = str(r.get('date') or '')[:10]
        if len(d) != 10 or d[4] != '-':
            continue
        try:
            seen[d] = {'date': d,
                       'open': float(r['open']), 'close': float(r['close']),
                       'high': float(r['high']), 'low': float(r['low']),
                       'volume': float(r.get('volume') or 0)}
        except (KeyError, TypeError, ValueError):
            continue
    return [seen[k] for k in sorted(seen)]


# ── 四条日K源 ─────────────────────────────────────────────────────────────
def _us_daily(sym):
    """新浪美股日K JSONP。道指/纳指/纳100/标普 各 3000~5800 根, 2004 年起。"""
    url = ('https://stock.finance.sina.com.cn/usstock/api/jsonp.php/var%20_=/'
           'US_MinKService.getDailyK?symbol=' + sym + '&___qn=3')
    arr = _jsonp(_http(url, 'https://finance.sina.com.cn/'))
    out = []
    for r in arr or []:
        out.append({'date': str(r.get('d') or '')[:10],
                    'open': r.get('o'), 'close': r.get('c'),
                    'high': r.get('h'), 'low': r.get('l'),
                    'volume': r.get('v') or 0})
    return _norm(out)


def _gi_daily(sym, num=1000):
    """新浪国际指数日K。⚠ num 硬上限 1000(更早的翻页参数全部无效)。"""
    url = ('https://gi.finance.sina.com.cn/hq/daily?symbol=' + sym
           + '&num=' + str(int(num)))
    j = json.loads(_http(url, 'https://finance.sina.com.cn/'))
    data = ((j or {}).get('result') or {}).get('data') or []
    out = []
    for r in data:
        out.append({'date': str(r.get('d') or '')[:10],
                    'open': r.get('o'), 'close': r.get('c'),
                    'high': r.get('h'), 'low': r.get('l'),
                    'volume': r.get('v') or 0})
    return _norm(out)


def _tencent_daily(sym, count='1000'):
    """腾讯 fqkline (港股/其它)。解析直接复用 engine._norm_tencent_bars。"""
    from quant_terminal import engine as _E
    j = _E._fqkline_req(sym, count=str(count))
    return _norm(_E._norm_tencent_bars(j, sym) or [])


def _a50_daily():
    """新浪期货日K JSONP。2594 根 / 2016-11-28 起。"""
    url = ('https://stock.finance.sina.com.cn/futures/api/jsonp.php/var%20_=/'
           'GlobalFuturesService.getGlobalFuturesDailyKLine?symbol=CHA50CFD')
    arr = _jsonp(_http(url, 'https://finance.sina.com.cn/'))
    out = []
    for r in arr or []:
        out.append({'date': str(r.get('date') or '')[:10],
                    'open': r.get('open'), 'close': r.get('close'),
                    'high': r.get('high'), 'low': r.get('low'),
                    'volume': r.get('volume') or 0})
    return _norm(out)


# ── 对外接口 ──────────────────────────────────────────────────────────────
def is_code(code):
    return str(code or '').strip().lower() in CODES


def info(code):
    return dict(CODES.get(str(code or '').strip().lower()) or {})


def full_bars(code):
    """该指数**尽可能早**的完整日线。失败返回 []（不抛异常）。"""
    c = str(code or '').strip().lower()
    it = CODES.get(c)
    if not it:
        return []
    k = it['kind']
    try:
        if k == 'us':
            return _us_daily(it['sym'])
        if k == 'gi':
            return _gi_daily(it['sym'])
        if k == 'tencent':
            return _tencent_daily(it['sym'])
        if k == 'a50':
            return _a50_daily()
    except Exception:
        return []
    return []


def tail_bars(code, n=10):
    """最近 n 根。取数策略: 美/港走腾讯 count=n(只回 n 根, 省流量);
    国际指数/ A50 走全量再切片(源本身就没有 count 参数)。"""
    c = str(code or '').strip().lower()
    it = CODES.get(c)
    if not it:
        return []
    kind = it['kind']
    if kind in ('us', 'tencent') and n > 0:
        try:
            rows = _tencent_daily(it['sym'], count=str(int(n)))
            if rows:
                return rows[-int(n):]
        except Exception:
            pass
        # 腾讯没给就退回全量源切片
    rows = full_bars(c)
    return rows[-int(n):] if (n and rows) else rows


# ── 交易时段 / 盘中状态 (v2.410, 用户 m11576) ────────────────────────────
#   海外各市场的开收盘时间与 A 股完全不同, 欧洲/北美还有夏令时。
#   时区一律用 zoneinfo 换算(本机实测可用; 开源用户 Windows 上装了 tzdata 也行),
#   缺 tzdata 时退回固定时差 —— 夏令时会差 1 小时, 只影响「是否盘中」的提示,
#   不影响任何取数/落盘的正确性(落盘另有一道 is_settled 用同一套时间)。
from datetime import datetime as _dt, timedelta as _td, timezone as _tzfix

try:
    from zoneinfo import ZoneInfo as _ZI
except Exception:                                    # pragma: no cover
    _ZI = None

# 市场 -> (IANA 时区, [(开盘, 收盘) 当地分钟数]); 收盘 >1440 表示跨到次日凌晨。
#   一律按常规交易时段, 不含盘前/盘后。
_SESS = {
    'us': ('America/New_York', [(570, 960)]),                   # 09:30-16:00
    'hk': ('Asia/Hong_Kong',   [(570, 720), (780, 960)]),       # 09:30-12:00 13:00-16:00
    'jp': ('Asia/Tokyo',       [(540, 690), (750, 930)]),       # 09:00-11:30 12:30-15:30
    'kr': ('Asia/Seoul',       [(540, 930)]),                   # 09:00-15:30
    'tw': ('Asia/Taipei',      [(540, 810)]),                   # 09:00-13:30
    'au': ('Australia/Sydney', [(600, 960)]),                   # 10:00-16:00
    'uk': ('Europe/London',    [(480, 990)]),                   # 08:00-16:30
    'de': ('Europe/Berlin',    [(540, 1050)]),                  # 09:00-17:30
    'fr': ('Europe/Paris',     [(540, 1050)]),                  # 09:00-17:30
    'in': ('Asia/Kolkata',     [(555, 930)]),                   # 09:15-15:30
    'sg': ('Asia/Singapore',   [(540, 990), (1020, 1755)]),     # 09:00-16:30 17:00-05:15(+1)
}
_FALLBACK_OFF = {'us': -5, 'hk': 8, 'jp': 9, 'kr': 9, 'tw': 8, 'au': 10,
                 'uk': 0, 'de': 1, 'fr': 1, 'in': 5.5, 'sg': 8}
_TZC = {}

_CN = {'pre': '未开盘', 'open': '盘中', 'lunch': '午休', 'post': '已收盘',
       'weekend': '周末休市'}


def _tz_of(mkt):
    """市场 -> tzinfo (带缓存)。"""
    pair = _SESS.get(mkt)
    if not pair:
        return None
    name = pair[0]
    if name in _TZC:
        return _TZC[name]
    z = None
    if _ZI is not None:
        try:
            z = _ZI(name)
        except Exception:
            z = None
    if z is None:
        z = _tzfix(_td(hours=_FALLBACK_OFF.get(mkt, 0)))
    _TZC[name] = z
    return z


def session(code):
    """该指数所在市场**此刻**的交易状态。纯本地时间计算, 一次网络请求都不发。

    返回 {} 表示不是海外指数; 否则:
      {mkt, tz, local('YYYY-MM-DD HH:MM'), hm, state, state_cn, is_open, is_active}
    state: pre/open/lunch/post/weekend
    """
    c = str(code or '').strip().lower()
    mkt = c.split('.')[0]
    pair = _SESS.get(mkt)
    if not pair:
        return {}
    z = _tz_of(mkt)
    lc = _dt.now(z)
    mins = lc.hour * 60 + lc.minute
    sess = pair[1]
    op = False
    for a, b in sess:
        if b <= 1440:
            if a <= mins < b:
                op = True
        else:
            if mins >= a or mins < (b - 1440):
                op = True
    if lc.weekday() >= 5:
        state = 'weekend'
    elif op:
        state = 'open'
    elif mins < sess[0][0]:
        state = 'pre'
    else:
        state = 'post'
        for i in range(len(sess) - 1):
            a2, b2 = sess[i][1], sess[i + 1][0]
            if b2 <= 1440 and a2 <= mins < b2:
                state = 'lunch'
                break
    return {'mkt': mkt, 'tz': pair[0], 'local': lc.strftime('%Y-%m-%d %H:%M'),
            'hm': lc.strftime('%H:%M'), 'state': state,
            'state_cn': _CN.get(state, state),
            'is_open': state == 'open', 'is_active': state in ('open', 'lunch')}


def _settle_at(code, date_str):
    """某市场对 date_str 这一天的行情, 何时算「收盘确认」。"""
    mkt = str(code or '').strip().lower().split('.')[0]
    pair = _SESS.get(mkt)
    if not pair:
        return None
    z = _tz_of(mkt)
    try:
        d = _dt.strptime(str(date_str)[:10], '%Y-%m-%d')
    except Exception:
        return None
    best = None
    for a, b in pair[1]:
        if b <= 1440:
            cand = d.replace(hour=b // 60, minute=b % 60)
        else:
            cand = (d + _td(days=1)).replace(hour=(b - 1440) // 60, minute=(b - 1440) % 60)
        if best is None or cand > best:
            best = cand
    try:
        return best.replace(tzinfo=z)
    except Exception:
        return best


def is_settled(code, date_str):
    """该市场对 date_str 这一天是否已收盘确认(可以安全落盘)。不认识的市场返回 True(不拦)。"""
    t = _settle_at(code, date_str)
    if t is None:
        return True
    try:
        return _dt.now(t.tzinfo) >= t
    except Exception:
        return True


def settled_key(code):
    """该市场「最近一个已经收盘」的当地交易日(YYYY-MM-DD), 没有则 ''。

    给 server._index_guard_loop 做「每个市场每个交易日只同步一次」的键 ——
    原来看北京时间每天同步一次, 美股夜里开盘那一轮早就用掉了配额, 收盘后反而不补。
    """
    mkt = str(code or '').strip().lower().split('.')[0]
    if mkt not in _SESS:
        return ''
    lc = _dt.now(_tz_of(mkt))
    for back in range(0, 8):
        t = lc - _td(days=back)
        if t.weekday() >= 5:          # 当地时间周末没有行情, 不能当同步键
            continue
        d = t.strftime('%Y-%m-%d')
        if is_settled(code, d):
            return d
    return ''


def drop_unsettled(rows, code):
    """只保留「该市场当日已收盘确认」的 bar。

    🔴 为什么不能用 engine._drop_forming_today: 那条是按**北京 15:06** 判的。
    美股 22:30 才开盘, 那个点北京早已过 15:06 ⇒ 判「已收盘」⇒ 把刚开盘的半截
    日K当成收盘 bar 写进 CSV, 而增量同步只追加 date 更大的行, 以后永远覆盖不掉。
    """
    out = []
    for r in rows or []:
        if is_settled(code, r.get('date')):
            out.append(r)
    return out


def minute(code):
    """当日分时。返回 {ok, code, name, pre, date, times:[HHMM], prices:[], source}
    或 {ok:False, no_minute:'...'} —— 后者表示「这个指数没有分时源」, 不是取数失败。
    """
    c = str(code or '').strip().lower()
    it = CODES.get(c)
    if not it:
        return {'ok': False, 'error': '不是海外指数: %s' % code}
    sid = it.get('secid') or ''
    if not sid:
        return {'ok': False, 'no_minute': '%s 暂无分时数据源，可看日K' % it['short'],
                'code': c, 'name': it['name']}
    url = ('http://push2his.eastmoney.com/api/qt/stock/trends2/get?ndays=1&iscr=0'
           '&fields1=f1,f2,f3,f4,f5,f6,f7,f8&fields2=f51,f53&secid=' + sid)
    j = json.loads(_http(url, 'http://quote.eastmoney.com/'))
    d = ((j or {}).get('data') or {})
    tr = d.get('trends') or []
    times, prices = [], []
    for row in tr:
        p = str(row).split(',')
        if len(p) < 2:
            continue
        ts = p[0]                        # 'YYYY-MM-DD HH:MM'
        try:
            px = float(p[1])
        except (TypeError, ValueError):
            continue
        times.append(ts[11:16].replace(':', '') if len(ts) >= 16 else '')
        prices.append(px)
    if not prices:
        return {'ok': False, 'error': '东财分时无数据', 'code': c, 'name': it['name']}
    date = str(tr[0]).split(',')[0].split(' ')[0] if tr else ''
    return {'ok': True, 'code': c, 'name': it.get('name') or c,
            'pre': float(d.get('preClose') or 0), 'date': date,
            'times': times, 'prices': prices, 'source': '东财分时'}


def snapshot(code):
    """东财实时快照 (现价/昨收/涨跌幅/开高低/量)。给「以后想做盘中实时」留的接口,
    本轮没有消费方。返回 {'ok','name','price','pre','pct','open','high','low','volume'}"""
    c = str(code or '').strip().lower()
    it = CODES.get(c)
    if not it or not it.get('secid'):
        return {'ok': False, 'error': '无 secid'}
    url = ('http://push2.eastmoney.com/api/qt/stock/get?secid=' + it['secid']
           + '&fields=f43,f44,f45,f46,f47,f58,f60,f169,f170')
    j = json.loads(_http(url, 'http://quote.eastmoney.com/'))
    d = ((j or {}).get('data') or {})
    if not d:
        return {'ok': False, 'error': '无数据'}

    # 东财对不同品种的缩放不一样: 股票/指数 x100, 期货(104.CN00Y) 只 x10。
    #   实测 A50 期货 f43=130510 对应 13051.0 ⇒ 除 10, 不是 100。
    _dv = float(it.get('div') or 100.0)

    def _f(k, div=None):
        div = _dv if div is None else div
        v = d.get(k)
        try:
            return float(v) / div if v not in (None, '', '-') else None
        except (TypeError, ValueError):
            return None

    return {'ok': True, 'name': d.get('f58') or it['name'],
            'price': _f('f43'), 'pre': _f('f60'), 'pct': _f('f170'),
            'open': _f('f46'), 'high': _f('f44'), 'low': _f('f45'),
            'volume': _f('f47', 1.0)}


def snapshots(codes):
    """批量实时快照(线程池并发调 snapshot)。

    东财批量接口 ulist.np 对海外 secid 一律返回 0(实测), 不可用;
    但单只 push2 接口完全正常, 十几只并发约 0.3~0.6 秒, 和批量差不多。
    返回 {code: {'ok','name','price','pre','pct','open','high','low','volume'}}
    """
    out = {}
    want = []
    for c in (codes or []):
        _c = str(c or '').strip().lower()
        it = CODES.get(_c)
        if it and it.get('secid'):
            want.append(_c)
    if not want:
        return out
    try:
        from concurrent.futures import ThreadPoolExecutor

        def _one(_c):
            try:
                return _c, snapshot(_c)
            except Exception as _e:
                return _c, {'ok': False, 'error': str(_e)[:120]}

        with ThreadPoolExecutor(max_workers=min(8, len(want))) as ex:
            for _c, _v in ex.map(_one, want):
                out[_c] = _v
    except Exception:
        for _c in want:
            try:
                out[_c] = snapshot(_c)
            except Exception as _e:
                out[_c] = {'ok': False, 'error': str(_e)[:120]}
    return out


def selftest():
    print('=== 海外指数 selftest ===')
    bad = 0
    for c in ORDER:
        it = CODES[c]
        rows = full_bars(c)
        n = len(rows)
        if n <= 0:
            bad += 1
        print('  %-10s %-14s %-8s %5d 行  %s ~ %s'
              % (c, it['name'], it['kind'], n,
                 rows[0]['date'] if rows else '-', rows[-1]['date'] if rows else '-'))
    print('  ---- 分时 ----')
    for c in ORDER:
        it = CODES[c]
        if not it.get('secid'):
            print('  %-10s 无分时源' % c)
            continue
        m = minute(c)
        print('  %-10s %s  %d 点  pre=%s' % (c, 'OK' if m.get('ok') else 'FAIL',
                                             len(m.get('prices') or []), m.get('pre')))
    print('OK' if bad == 0 else ('FAIL (%d 只无数据)' % bad))
    return bad == 0


if __name__ == '__main__':
    import sys
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass
    raise SystemExit(0 if selftest() else 1)
