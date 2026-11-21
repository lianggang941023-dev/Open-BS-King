# -*- coding: utf-8 -*-
"""ETF 取数 + 「沪深300头部/全流通市值」指标（v2.367，2026-11-22 用户 m40047）

为什么单独一个文件
------------------
`ths_index.py` 解决「同花顺全A 883957」那一路（10jqka 网页接口），本文件解决 ETF 那一路
（东财 push2his / push2 / fundf10）。两条通路的数据结构与单位都不同，混进 engine 会把
`_fetch_index_full` 变成一团 if；所以按 src 分流，engine 只加三行分支。

用户 m40047 的四条要求（照抄）
------------------------------
1. 5 只 ETF 的 K 线进「指数一览」，和其它指数一样有 点数/涨跌幅/趋势描述
2. 前端短名：300华泰 / 300易方 / 300华夏 / 300嘉实 / 300天弘
3. 历史**截断到 2020 年第一个交易日**（2020-01-02），本地存档、每天增量
4. 新增特色指标「沪深300头部/全流通市值」＝ 这 5 只的规模合计 ÷ 沪深300 流通市值

东财接口要点（实测，别猜）
--------------------------
· 日K `http://push2his.eastmoney.com/api/qt/stock/kline/get`
    secid 前缀：沪市 `1.` / 深市 `0.`（510300→1.510300、159919→0.159919）
    每行 11 段：日期,开,收,高,低,成交量(手),成交额(元),振幅%,涨跌幅%,涨跌额,换手率%
    **顺序是 开/收/高/低**（不是 ohlc 那种 o,c,h,l 之外的排列，别搞反）
    `beg=20200101` 截断后 5 只均为 1668 根 / 2020-01-02 ~ 最新
· 实时 `http://push2.eastmoney.com/api/qt/stock/get?fields=f43,f57,f58,f84,f85,f116,f117`
    f84=份额(份) / f117=净值规模(元) / f43=现价×1000 / f116=总市值
    **必须带 Referer: http://quote.eastmoney.com/**，否则 RemoteDisconnected
· 季报份额 `http://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=gmbd&code=<6位>`
    **必须带 Referer: http://fundf10.eastmoney.com/**，返回 JSONP 里一段 <table class='...gmbd'>
    列序：日期 / 期间申购(亿份) / 期间赎回(亿份) / 期末总份额(亿份) / 期末净资产(亿元) / 净资产变动率
    `type=` **只认 gmbd**（jjgm/gm/gmbd_new/fsrq 全返回 12 字节空壳）
· 🔴 东财 **https 被封、http 可用**（2026-11-21 实测；https 报
    `Remote end closed connection without response`）⇒ 本文件一律 http。

为什么指标的「历史分母」是近似（必须写清楚，否则以后有人当成真值）
------------------------------------------------------------------
沪深300 的**流通市值只有实时值**（f117），拿不到历史序列：
中证官网 index-perf 只给 OHLC/成交额/成分数（没有市值）；东财数据中心
RPT_FUND_ETF_* / RPT_ETF_SCALE 等报表全部「报表配置不存在」；上交所日频份额公示
只返回最新一天；深交所 403。
所以分母按 `沪深300收盘点位 × K` 折算，`K = f117 / 收盘点位`（亿元/点）：
  · **从今天起**每天收盘存一份真快照（K 也是真的）⇒ 实测点
  · **历史段**用快照库里**该日期之前最近的那个 K**（没有更早的就用最早的）⇒ 近似点
好在分子分母都随行情同向变动、价格因子大部分相互抵消，**指标的主导变量是份额**
（5 只合计份额从 2020 年的几百亿涨到 2026-11 的 1251 亿份），所以近似分母不影响
「资金在流入还是流出」这个判断。前端会把近似点画成更淡的颜色并写明。
"""

import io
import json
import os
import re
import sys
import time
import urllib.request

from quant_terminal import engine as E
from quant_terminal import idx_registry as R

try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass

# ── 五只头部沪深300ETF（用户 m40047 点名，按规模降序）───────────────────────
#   code  六位代码 / secid 东财 / short 前端短名（用户指定）
ETFS = (
    ('510300', '1.510300', '300华泰'),
    ('510310', '1.510310', '300易方'),
    ('510330', '1.510330', '300华夏'),
    ('159919', '0.159919', '300嘉实'),
    ('515330', '1.515330', '300天弘'),
)
ETF_CODES = tuple('sh' + c if c[:1] in ('5', '6') else 'sz' + c for c, _, _ in ETFS)
#   注：510xxx/515xxx 是沪市 ⇒ sh；159xxx 是深市 ⇒ sz。与 idx_registry 的键一致。

ETFSET = frozenset(ETF_CODES)

BEG = '20200102'          # 用户要求：历史从 2020 年第一个交易日算起
_UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
       '(KHTML, like Gecko) Chrome/120.0 Safari/537.36')
_H_QUOTE = {'User-Agent': _UA, 'Referer': 'http://quote.eastmoney.com/'}
_H_FUND = {'User-Agent': _UA, 'Referer': 'http://fundf10.eastmoney.com/'}
_TIMEOUT = 15

_KL = 'http://push2his.eastmoney.com/api/qt/stock/kline/get'
_SN = 'http://push2.eastmoney.com/api/qt/stock/get'
_GM = 'http://fundf10.eastmoney.com/FundArchivesDatas.aspx'

_RATIO_FILE = os.path.join(E.INDEX_DAILY_DIR, '..', 'etf_head_ratio.json')
_K_LIVE = [0.0]        # 当前实时 K = 沪深300流通市值(亿元)/收盘；快照库为空时的兜底


def _interp(pts, d):
    """在 [(日期, 值)] 上线性插值；端点外取端点值。

    🔴 为什么必须插值而不是只做前向填充（第一版就只做前向填充，结果差 2.2 倍）：
      510300 的季报份额 2026-09-30 是 241.20 亿份，而实时 f84 已经是 515.41 亿份
      —— 一个半月里份额真的翻倍（F84 与 F117 净值规模互相印证，不是口径错）。
      只前向填充的话 2026-10~11 段会算出 2163 亿，而当天真实读数 4763.84 亿。
    所以时间轴 = 季报点 + 每日真值快照点，两者之间线性过渡。
    """
    if not pts:
        return 0.0
    # v2.367: pts 允许是「(序数, 值)」—— 如果第一项的第一元素是 int 就直接用，
    #   不再对每个点做 strptime（原来 1668 天 × 5 只 = 8340 次，占 0.4 秒）。
    def _o(x):
        if isinstance(x, int):
            return x
        try:
            import datetime as _dt2
            return _dt2.datetime.strptime(str(x), '%Y-%m-%d').toordinal()
        except Exception:
            return None
    _p = []
    for i in range(len(pts)):
        o = _o(pts[i][0])
        if o is not None:
            _p.append((o, pts[i][1]))
    if not _p:
        return 0.0
    if len(_p) == 1:
        return _p[0][1]
    if d <= pts[0][0]:
        return _p[0][1]
    if d >= pts[-1][0]:
        return _p[-1][1]
    od = _o(d)
    if od is None:
        return _p[-1][1]
    for i in range(1, len(_p)):
        t0, v0 = _p[i - 1]
        t1, v1 = _p[i]
        if t0 <= od <= t1:
            if t1 == t0:
                return v1
            return v0 + (v1 - v0) * (od - t0) / float(t1 - t0)
    return _p[-1][1]


def norm_code(code):
    """把 '510300' / 'sh510300' / 'SH510300' / ' 1.510300 ' 统一成 'sh510300'；不是这 5 只返回 ''。

    ⚠ 必须两种写法都认：engine 那边一律传带前缀的 'sh510300'，
      而顶层函数（比如指标里遍历 ETFS）拿到的是六位 '510300'。
      第一版只认带前缀，结果 full_bars('510300') 拿到空 secid ⇒ 日K 全 0 根。
    """
    c = str(code or '').strip().lower()
    if not c:
        return ''
    six = c[-6:]
    d0 = c.split('.')[-1] if '.' in c else six
    for s6, _sec, _sh in ETFS:
        if six == s6 and (len(c) == 6 or c[:2] in ('sh', 'sz', '1.', '0.')):
            return ('sh' if s6[:1] in ('5', '6') else 'sz') + s6
        if d0 == s6:
            return ('sh' if s6[:1] in ('5', '6') else 'sz') + s6
    return ''


def _secid(code):
    """'sh510300' / '510300' → '1.510300'；'sz159919' / '159919' → '0.159919'；认不出返回 ''。"""
    c = norm_code(code)
    if not c:
        return ''
    for six, sec, _sh in ETFS:
        if c.endswith(six):
            return sec
    return ''


def _short6(code):
    return str(code or '').strip().lower()[-6:]


def _get(url, headers, timeout=_TIMEOUT):
    """直连（不走系统代理 —— 用户梯子常开，走代理打东财会 502）。"""
    try:
        op = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        req = urllib.request.Request(url, headers=headers)
        return op.open(req, timeout=timeout).read()
    except Exception as e:
        print('[etf_src] GET 失败 %s: %r' % (url[:90], e), file=sys.stderr)
        return b''


# ══════════════════════ 日K ══════════════════════════════════════════════
def full_bars(code):
    """该 ETF 从 2020-01-02 起的日K → [{date,open,close,high,low,volume}]

    volume 单位 = **手**（东财给的就是手），与 engine 其它指数口径一致。
    返回 None 表示取不到（调用方不得用空表覆盖本地库）。
    """
    sec = _secid(code)
    if not sec:
        return None
    url = ('%s?secid=%s&fields1=f1,f2,f3,f4,f5,f6'
           '&fields2=f51,f52,f53,f54,f55,f56,f57,f58&klt=101&fqt=1'
           '&beg=%s&end=20500101') % (_KL, sec, BEG)
    raw = _get(url, _H_QUOTE)
    if not raw:
        return None
    try:
        kl = ((json.loads(raw.decode('utf-8', 'replace')).get('data') or {})
              .get('klines') or [])
    except Exception:
        return None
    out = []
    for r in kl:
        p = str(r).split(',')
        if len(p) < 6:
            continue
        try:
            out.append({'date': p[0], 'open': float(p[1]), 'close': float(p[2]),
                        'high': float(p[3]), 'low': float(p[4]),
                        'volume': float(p[5])})
        except ValueError:
            continue
    out.sort(key=lambda x: x['date'])
    return out or None


def tail_bars(code, n=10):
    """最近 n 根（增量比对用）。直接切全量末 n 根，避免再发一次请求。"""
    rows = full_bars(code) or []
    return rows[-max(1, int(n)):] or None


def live_bar(code):
    """盘中当日 live bar — 东财日K末根就是当日实时（未收盘为 forming），
    但更省事也更准的是用实时快照 f43(现价)；这里两者都试。"""
    import datetime as _dt
    if not E._is_trade_window():
        return None
    today = _dt.datetime.now().strftime('%Y-%m-%d')
    q = snap(code)
    if q and q.get('price', 0) > 0:
        rows = full_bars(code) or []
        last = rows[-1] if rows else None
        if last and last['date'] == today:
            return last
        return {'date': today, 'open': q.get('open') or q['price'],
                'close': q['price'], 'high': q.get('high') or q['price'],
                'low': q.get('low') or q['price'],
                'volume': q.get('volume') or 0}
    rows = full_bars(code) or []
    if rows and rows[-1]['date'] == today:
        return rows[-1]
    return None


# ══════════════════════ 实时快照 ══════════════════════════════════════════
def snap(code):
    """实时快照 → {'name','price','open','high','low','share'(份),'mcap'(元),'code'}

    ETF 的 f84 = 份额、f117 = 净值规模；指数（如 sh000300）的 f117 = 流通市值。
    两者字段同名不同含义 —— 调用方自己知道自己在要什么。
    """
    sec = _secid(code)
    if not sec:
        six = _short6(code)
        sec = '1.' + six if six[:1] in ('0', '5', '6') else '0.' + six
    url = ('%s?secid=%s&fields=f43,f44,f45,f46,f47,f57,f58,f84,f85,f116,f117'
           % (_SN, sec))
    raw = _get(url, _H_QUOTE)
    if not raw:
        return None
    try:
        d = json.loads(raw.decode('utf-8', 'replace')).get('data') or {}
    except Exception:
        return None
    if not d:
        return None

    def _n(k, div=1.0):
        v = d.get(k)
        try:
            return float(v) / div if v not in (None, '-', '') else 0.0
        except (TypeError, ValueError):
            return 0.0

    return {'code': str(code).lower(), 'name': d.get('f58') or '',
            'price': _n('f43', 1000.0), 'open': _n('f46', 1000.0),
            'high': _n('f44', 1000.0), 'low': _n('f45', 1000.0),
            'volume': _n('f47'), 'share': _n('f84'), 'mcap': _n('f117'),
            'mcap_tot': _n('f116')}


def hs300_fcap_yi():
    """沪深300 当前流通市值（**亿元**）。东财 f117 单位是元 ⇒ /1e8。"""
    q = snap('sh000300')
    if not q:
        return 0.0
    return (q.get('mcap') or 0.0) / 1e8


def hs300_close():
    rows = E.read_index_daily('sh000300') or []
    return rows[-1]['close'] if rows else 0.0


# ══════════════════════ 季报份额（历史规模的唯一真源）════════════════════
_GM_CACHE = {}


def gmbd(code):
    """季报份额 → [(日期'YYYY-MM-DD', 期末份额亿份, 期末净资产亿元)]

    只认 type=gmbd；返回按日期升序。结果内存缓存 1 小时。
    """
    six = _short6(code)
    c = _GM_CACHE.get(six)
    if c and (time.time() - c[0]) < 3600:
        return c[1]
    url = '%s?type=gmbd&code=%s&rt=0.1' % (_GM, six)
    raw = _get(url, _H_FUND)
    out = []
    if raw:
        txt = raw.decode('utf-8', 'replace')
        for tr in re.findall(r'<tr[^>]*>(.*?)</tr>', txt, re.S):
            tds = [re.sub(r'<[^>]+>', '', x).strip()
                   for x in re.findall(r'<td[^>]*>(.*?)</td>', tr, re.S)]
            if len(tds) < 5:
                continue
            dt = tds[0]
            if not re.match(r'^\d{4}-\d{2}-\d{2}$', dt):
                continue
            try:
                # 🔴 数字带千位逗号（'1,068.79'）—— 直接 float() 会 ValueError，
                #   第一版漏了 replace(',','') ⇒ 规模上亿的那些季全部被 skip，
                #   2022Q4 之后的份额全丢（指标算出来只有 0.40% 而不是 0.99%）。
                sh = float(tds[3].replace(',', ''))
                nav = float(tds[4].replace(',', ''))
            except ValueError:
                continue
            if sh <= 0:
                continue
            out.append((dt, sh, nav))
    out.sort()
    _GM_CACHE[six] = (time.time(), out)
    return out


# ══════════════════════ 指标：沪深300头部/全流通市值 ══════════════════════
HEAD_CODE = 'head300'
HEAD_NAME = '沪深300头部/全流通市值'


def _load_ratio():
    try:
        with io.open(_RATIO_FILE, 'r', encoding='utf-8') as f:
            d = json.load(f)
        if isinstance(d, dict) and isinstance(d.get('rows'), list):
            return d
    except Exception:
        pass
    return {'rows': [], 'note': ''}


def _save_ratio(d):
    try:
        os.makedirs(os.path.dirname(_RATIO_FILE), exist_ok=True)
        tmp = _RATIO_FILE + '.tmp'
        with io.open(tmp, 'w', encoding='utf-8') as f:
            f.write(json.dumps(d, ensure_ascii=False, separators=(',', ':')))
        os.replace(tmp, _RATIO_FILE)
        return True
    except Exception as e:
        print('[etf_src] 写 ratio 失败: %r' % (e,), file=sys.stderr)
        return False


def snapshot_today(force=False):
    """把今天的真值快照记进 json（每日一条，按日期覆盖）。

    ⚠ 只在收盘后（15:06 之后）才有意义 —— 盘中 f117 随时间变，会污染序列。
    """
    import datetime as _dt
    now = _dt.datetime.now()
    # 快照日期 = **最后一根日K的日期**，不是今天。
    #   为什么（踩过）：周六跑一次，`snap()` 拿到的是周五收盘价，若按 today
    #   记成周六，时间轴上就多一个周六点、而周五永远缺真值点，插值那段照样失真。
    #   日K 的末根日期就是这个快照真正的交易日。
    _tail = []
    for _c, _s, _sh in ETFS:
        # ⚠ ETFS 里的 code 是**六位**（'510300'），而本地日K文件名是 sh510300.csv
        #   ⇒ 读库必须过 norm_code，否则 read_index_daily 一律返回 0 根。
        _r = E.read_index_daily(norm_code(_c)) or []
        if _r:
            _tail.append(_r[-1]['date'])
    # 顺带把 closes/shares 也统一成规范码（同一个坑，别再踩第二次）

    if not _tail:
        return {'ok': False, 'msg': '本地还没有 ETF 日K，先 ensure 再记快照'}
    # 取众数（5 只同交易日，个别停牌会差一天）
    _cnt = {}
    for _d in _tail:
        _cnt[_d] = _cnt.get(_d, 0) + 1
    today = sorted(_cnt.items(), key=lambda x: (-x[1], x[0]))[0][0]
    # 只有当「这个交易日 == 今天」时才需要等收盘；补记历史日（周末/隔天跑）随时可以
    _same_day = (today == now.strftime('%Y-%m-%d'))
    if _same_day and not force and now.strftime('%H%M') < '1506':
        return {'ok': False, 'msg': '盘中不记快照（15:06 后再存）'}
    parts = []
    tot = 0.0
    for code, _sec, _sh in ETFS:
        q = snap(code)
        if not q or q.get('mcap', 0) <= 0:
            return {'ok': False, 'msg': '取不到 %s 的规模' % code}
        parts.append({'code': code, 'share': round(q['share'], 2),
                      'mcap': round(q['mcap'] / 1e8, 2),
                      'close': q['price']})
        tot += q['mcap'] / 1e8
    fcap = hs300_fcap_yi()
    close = hs300_close()
    if fcap <= 0:
        return {'ok': False, 'msg': '取不到沪深300流通市值'}
    row = {'date': today, 'head': round(tot, 2), 'fcap': round(fcap, 2),
           'close': round(close, 2), 'k': round(fcap / close, 6) if close else 0.0,
           'ratio': round(tot / fcap * 100.0, 4), 'parts': parts}
    d = _load_ratio()
    rows = [r for r in d['rows'] if r.get('date') != today]
    rows.append(row)
    rows.sort(key=lambda r: r.get('date') or '')
    d['rows'] = rows[-2000:]
    d['note'] = 'k = 流通市值(亿元)/沪深300收盘；历史段用它折算，只有快照日是实测'
    _save_ratio(d)
    return {'ok': True, 'row': row, 'n': len(d['rows'])}


_HEAD_CACHE = {'ts': 0.0, 'key': None, 'data': None}


def _head_cache_key():
    """缓存判据 = 5 只 ETF 日K + 沪深300 日K + ratio json 的 mtime/size。

    这条序列一天只变一次（收盘后快照写入），没必要每开一次页面重算 2 秒。
    """
    parts = []
    for _c, _s, _sh in ETFS:
        try:
            st = os.stat(E._idx_fpath(norm_code(_c)))
            parts.append('%d:%d' % (int(st.st_mtime), st.st_size))
        except OSError:
            parts.append('-')
    try:
        st2 = os.stat(E._idx_fpath('sh000300'))
        parts.append('%d:%d' % (int(st2.st_mtime), st2.st_size))
    except OSError:
        parts.append('-')
    try:
        st3 = os.stat(_RATIO_FILE)
        parts.append('%d:%d' % (int(st3.st_mtime), st3.st_size))
    except OSError:
        parts.append('-')
    return '|'.join(parts)


def head_ratio_series(limit=0):
    """「沪深300头部/全流通市值」日线 → {dates, head(亿元), fcap(亿元), ratio(%), snap_dates}

    · 分子 head[i] = Σ(5 只 ETF 的季报份额前向填充 × 当日收盘价)，单位 亿元
      （份额单位是**亿份**、价格是**元** ⇒ 相乘正好是亿元，不用再换算）
    · 分母 fcap[i] = 沪深300当日收盘 × K；K 取「快照库里 <= 当日的最近一个」，
      没有更早的就用最早的；**一个快照都没有时退回当前实时 K**（东财 f117/收盘）
    · snap_dates 里的日期是真快照日，前端把它画成实心点、其余淡一点
    """
    # v2.367: 缓存命中直接返回（key 覆盖「哪些文件变了」，比 TTL 更准）
    _k = _head_cache_key()
    _now = time.time()
    _cch = _HEAD_CACHE
    if _cch['data'] is not None and _cch['key'] == _k and (_now - _cch['ts']) < 600.0:
        _dd = _cch['data']
        if limit and len(_dd['dates']) > int(limit):
            _n = int(limit)
            return {'ok': True, 'code': _dd['code'], 'name': _dd['name'],
                    'dates': _dd['dates'][-_n:], 'head': _dd['head'][-_n:],
                    'fcap': _dd['fcap'][-_n:], 'ratio': _dd['ratio'][-_n:],
                    'n': _n, 'start': _dd['dates'][-_n], 'end': _dd['end'],
                    'snap_dates': _dd['snap_dates'], 'n_snap': _dd['n_snap'],
                    'k_now': _dd['k_now']}
        return _dd
    # 先抓一次实时 K 兜底（快照库为空时用；有快照就不会用到它）
    try:
        _c = hs300_close()
        _K_LIVE[0] = (hs300_fcap_yi() / _c) if _c > 0 else 0.0
    except Exception:
        _K_LIVE[0] = 0.0
    closes = {}
    for code, _sec, _sh in ETFS:
        rows = E.read_index_daily(code) or []
        if len(rows) < 30:
            rows = full_bars(code) or []
        closes[code] = {r['date']: r['close'] for r in rows}
    hs = {r['date']: r['close'] for r in (E.read_index_daily('sh000300') or [])}
    shares = {}
    for code, _sec, _sh in ETFS:
        shares[code] = gmbd(code)
    if not all(closes.values()) or not hs:
        return None

    snap = _load_ratio()
    ks = sorted([(r['date'], r.get('k') or 0.0) for r in snap['rows']
                 if r.get('k')], key=lambda x: x[0])
    snap_dates = set([r['date'] for r in snap['rows']])
    # v2.367: 把每日真值快照里的**份额**按代码整理成 [(date, 亿份)]，
    #   供 _share_at 与季报点一起插值（这是「越用越准」的那一半）。
    snap_share = {}
    for _r in snap['rows']:
        for _p in (_r.get('parts') or []):
            _c = (_p.get('code') or '').lower()
            _sh = _p.get('share')
            if _c and _sh:
                # 🔴 单位：快照存的是东财 f84 原值（**份**，510300 ≈ 515 亿份 = 5.15e10），
                #   而 gmbd() 给的是**亿份**（515.41）。两者要拼进同一条时间轴，
                #   这里必须 /1e8 对齐 —— 否则快照点比季报点大 1e8 倍，
                #   插值出来 head 是 4,584 亿亿、ratio = 95,032,774%（踩过）。
                snap_share.setdefault(_c, []).append((_r['date'], float(_sh) / 1e8))

    # 采样日期 = 沪深300 的交易日（2020 起）
    base = [d for d in sorted(hs) if d >= '2020-01-02']
    if limit and len(base) > int(limit):
        base = base[-int(limit):]
    if not base:
        return None

    def _share_at(rows, d, code=''):
        """季报点 + 每日真值快照点拼时间轴，线性插值（见模块级 _interp 的说明）。

        ⚠ 快照份额取自 `snap['rows'][i]['parts']`（下面已整理成 `snap_share`），
          **不要另设一个模块级 _SNAPS** —— 我之前就是写了个不存在的 _SNAPS，
          CLI selftest 走的是别的分支没暴露，服务里一调就 NameError，
          还被路由的 except 吞成「指标暂不可用」。
        """
        pts = [(x[0], x[1]) for x in rows]
        pts.extend(snap_share.get(code) or [])
        pts.sort()
        return _interp(pts, d)

    def _k_at(d):
        """K = 流通市值(亿元)/收盘点位。取快照库里 <= d 的最近一个；
        一个快照都没有（新装机器 / 刚上线）就退回**当前实时 K** —— 见模块 docstring
        那段「为什么历史分母是近似」。绝不能因为没快照就把 fcap 算成 0（会画空气）。"""
        k = 0.0
        for dt, kk in ks:
            if dt <= d:
                k = kk
            else:
                break
        if not k:
            k = ks[0][1] if ks else 0.0
        if not k:
            k = _K_LIVE[0]
        return k

    dates, heads, fcaps, ratios = [], [], [], []
    for d in base:
        tot = 0.0
        ok = True
        for code, _sec, _sh in ETFS:
            px = closes[code].get(d)
            if px is None:      # 该 ETF 当日停牌 → 用上一可得价（份额乘价格，价格缺失就 0 会让曲线断）
                px = 0.0
                for dd in sorted([x for x in closes[code] if x <= d], reverse=True)[:1]:
                    px = closes[code][dd]
            tot += _share_at(shares[code], d, code) * (px or 0.0)
        k = _k_at(d)
        c = hs.get(d) or 0.0
        fcap = c * k
        if tot <= 0 or fcap <= 0:
            ok = False
        dates.append(d)
        heads.append(round(tot, 2))
        fcaps.append(round(fcap, 2))
        ratios.append(round(tot / fcap * 100.0, 4) if ok else None)
    _out = {'ok': True, 'code': HEAD_CODE, 'name': HEAD_NAME, 'dates': dates,
            'head': heads, 'fcap': fcaps, 'ratio': ratios,
            'n': len(dates), 'start': dates[0] if dates else '',
            'end': dates[-1] if dates else '',
            'snap_dates': sorted(snap_dates),
            'n_snap': len(snap_dates),
            'k_now': round(hs300_fcap_yi() / max(1e-9, hs300_close()), 6) if hs300_close() else 0.0}
    _HEAD_CACHE['ts'] = time.time()
    _HEAD_CACHE['key'] = _k
    _HEAD_CACHE['data'] = _out
    return _out


def selftest():
    print('─' * 60)
    for code, sec, sh in ETFS:
        rows = full_bars(code) or []
        q = snap(code)
        gm = gmbd(code)
        print('%-8s %-6s secid=%-9s 日K %4d 根 %s~%s | 份额 %8.2f 亿份 规模 %8.2f 亿 | 季报 %2d 期 末期 %s'
              % (code, sh, sec, len(rows),
                 rows[0]['date'] if rows else '-', rows[-1]['date'] if rows else '-',
                 (q or {}).get('share', 0) / 1e8, (q or {}).get('mcap', 0) / 1e8,
                 len(gm), gm[-1][0] if gm else '-'))
    d = head_ratio_series()
    if d:
        print('指标 %s：%d 点 %s~%s  末值 ratio=%.4f%%  head=%.2f亿  fcap=%.2f亿  快照 %d 天'
              % (HEAD_NAME, d['n'], d['start'], d['end'],
                 (d['ratio'][-1] or 0), d['head'][-1], d['fcap'][-1], d['n_snap']))
    else:
        print('!! 指标取不到')
    print('─' * 60)


if __name__ == '__main__':
    selftest()
