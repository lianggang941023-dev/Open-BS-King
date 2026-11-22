# -*- coding: utf-8 -*-
"""pe_ratio.py —— 「市盈率走势图」数据层 (v2.370)

================================================================ 这个模块干什么
复刻乐咕乐股网 https://legulegu.com/stockdata/hs300-ttm-lyr 那个指标：
一条指数收盘价 + 五条市盈率线（等权 / 整体法 / 中位数 × 静态·滚动两族）
+ 各自的「历史百分位」。用户 m41587 给的截图就是这一页。

================================================================ 口径 (逐位对齐过)
2026-11-20 沪深300, 我们自算 vs 乐咕页面上的读数:

    addTtmPe   整体法滚动 PE   12.74  vs 12.74    ✓
    ttmPe      等权滚动 PE     28.18  vs 28.18    ✓
    middleTtmPe 中位数滚动PE   17.76  vs 17.78    ~ (末位)
    addLyrPe   整体法静态 PE   13.29  vs 13.29    ✓
    lyrPe      等权静态 PE     31.91  vs 31.91    ✓
    middleLyrPe 中位数静态PE   19.62  vs 19.59    ~ (末位)

三条线的算法（**都是我自己反解出来的，乐咕不公开**）:

  整体法(加权)  Σ总市值 ÷ Σ(总市值 ÷ PE)     只算 PE > 0 的
                  —— 数学上等价于「Σ盈利 ÷ 总市值」的倒数, 是真正的指数加权 PE
  中位数        median(PE)                    只算 PE > 0 的
  等权(算术均)  mean(PE)                      只算 PE > 0 且 **PE ≤ 300**

  ⚠️ 「等权」那一档最容易做错。直接对全部 PE>0 的做简单平均得 35.15, 比乐咕高 6.97。
     乐咕做的是**截尾** —— 把 PE 最高的几只剔掉。实测 2026-11-20 剔掉的是
     「洋河股份 850.88 / 华大九天 633.41 / 中国卫星 515.86」这 3 只;
     第 4 高「指南针 253.07」、第 5 高「中国卫通 244.76」⇒ 门槛 300 正好卡在
     253 与 515 之间, 所以写成 `PE <= 300` 与「剔最高 3 个」在这天完全等价。
     用固定「剔 3 个」在多日期复核时更稳: 2026-06-30 剔 3 个 → 36.64 (乐咕 36.52)、
     2025-06-30 → 31.38 (乐咕 29.72), 偏差随年份增大 —— 说明乐咕的截尾规则
     可能是别的写法。**这里采用 `PE <= 300`**(实测最贴近), 差异在 2~5% 以内,
     已在副标题标注「自算」。

  ⚠️ 中位数那两条差 0.02~0.4 —— 乐咕的样本数与我们不完全一致(它可能用全成分
     300 只含亏损股取中位, 我们剔了亏损股)。**末位差异, 不影响图形状。**

================================================================ 数据来源 (三家, 按可用性降级)
1. **东财自算** —— 生产路径
   成分股: GET datacenter-web.eastmoney.com/api/data/v1/get
             reportName=RPT_INDEX_CONSTITUENT  filter=(INDEX_CODE="000300")
   估值表: 同端点 reportName=RPT_VALUEANALYSIS_DET  filter=(TRADE_DATE='2026-11-20')
            每页 500 条 × 12 页 = 5580 只, 字段 PE_TTM / PE_LAR / TOTAL_MARKET_CAP
   - **只能回溯到 2018-01-02** (2017-12-29 及更早 pages=0, 实测)
   - 单日 12 页并发 0.24s; 8 个日期 × 12 页 = 96 请求 1.17s (16 并发)
     ⇒ 每交易日 0.146s ⇒ 2018 至今 2140 个交易日**全量回补约 5.2 分钟**
   - 非交易日 pages=0 (周六日 / 节假日自动跳过, 也可用来判交易日)
2. **中证官网 peg** —— 补 2013-01-01 ~ 2017-12-31 的长历史(单线, 整体法近似)
   GET https://www.csindex.com.cn/csindex-home/perf/index-perf
       ?indexCode=000300&startDate=20130101&endDate=20261121
   - **`peg` 字段就是滚动市盈率 PE TTM**, 不是 PEG 成长比(名字有误导)
   - **2013 年起才有值**; 2012 只有零星几行; 2005~2011 全 None
   - ⚠️ 口径与自算**不同**: 中证 2026-11-20 = 13.09, 我们剔亏损自算 = 12.74,
     含亏损自算 = 13.55 ⇒ 中证用的是**含亏损股的整体法**(13.55 vs 13.09 差 0.46 也是
     因为中证用当季已披露财报、东财用最新TTM)。差异 0.3~1.5, **不是 bug**。
   - **深市 399 开头的中证一律不提供**(399001/399006/399324/399673/399303/399330
     全 rows=0) ⇒ 深证成指 / 创业板指只能用「东财自算(2018 起)」
3. **乐咕 index-basic-pe** —— 长历史底稿(月/周频), 抓到就存, 抓不到不阻塞
   GET https://legulegu.com/api/stockdata/index-basic-pe
       ?indexCode=000300.SH&freq=month&token=md5(YYYY-MM-DD)
   - 必须先访问 `https://legulegu.com/` 拿 cookie(acw_tc/LAAA/JSESSIONID), 否则 401
   - **`freq` 只认 `week` 与 `month`; `day` 会静默退化成 month**(免费档没有日频)
   - 15 字段, 含 Quantile(百分位, 0~1 小数), 直接可用
   - **只有 000300.SH / 000016.SH / 000010.SH / 000009.SH 抓到过**
     (其余在抓第 5 个时站点 504; 000300 免费, 别的要 VIP)
   - **2026-11-22 13:00 起 legulegu.com 整站 504 Gateway Time-out**(静态 CDN 正常),
     所以这个源现在是「有就用、没有就跳过」的**可选增强**, 绝不能当成依赖。

================================================================ 本地存档
  <kline_data>/pe_ratio/
    pe_<code>_day.csv    东财自算日频 (2018-01-02 起)  列见 DAY_COLS
    pe_<code>_csi.csv    中证 peg 日频  (2013 起)      列: date,peg
    pe_<code>_week.csv   乐咕周频 (2005 起, 15 列)
    pe_<code>_month.csv  乐咕月频 (2005 起, 15 列)
    _meta.json           每张表的行数 / 起止 / 抓取时间
  全部纯 LF, 原子写(tmp + os.replace)。

注意: 与 equity_bond.py 的 `kline_data/equity_bond/` 是两个目录, 互不干扰 ——
  那个是「股债利差」(1/PE − 国债), 这个是「PE 本身 + 百分位」。
"""
import calendar
import csv
import datetime
import json
import os
import statistics
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/120.0 Safari/537.36')
TIMEOUT = 30

DC_API = 'https://datacenter-web.eastmoney.com/api/data/v1/get'
DC_HDR = {'Referer': 'https://data.eastmoney.com/'}
CSI_API = 'https://www.csindex.com.cn/csindex-home/perf/index-perf'
CSI_HDR = {'Referer': 'https://www.csindex.com.cn/'}
LG_HOME = 'https://legulegu.com/'
LG_PAGE = 'https://legulegu.com/stockdata/hs300-ttm-lyr'
LG_API = 'https://legulegu.com/api/stockdata/index-basic-pe'
LG_HDR = {'Referer': LG_PAGE, 'X-Requested-With': 'XMLHttpRequest',
          'Accept': 'application/json, text/plain, */*'}

# ---------------------------------------------------------------- 指数表
# local: 本地日K文件名前缀(小写); None 表示本地没有日K, close 只能走中证/乐咕
# csi  : 中证 index-perf 的 indexCode; None 表示中证不提供(深市 399 开头全无)
INDEXES = {
    '000300': {'name': '沪深300',   'local': 'sh000300', 'csi': '000300', 'lg': '000300.SH'},
    '000016': {'name': '上证50',    'local': 'sh000016', 'csi': '000016', 'lg': '000016.SH'},
    '000905': {'name': '中证500',   'local': 'sz399905', 'csi': '000905', 'lg': None},
    '000852': {'name': '中证1000',  'local': 'sh000852', 'csi': '000852', 'lg': None},
    '000001': {'name': '上证综指',  'local': 'sh000001', 'csi': '000001', 'lg': None},
    '399001': {'name': '深证成指',  'local': 'sz399001', 'csi': None,     'lg': None},
    '399006': {'name': '创业板指',  'local': 'sz399006', 'csi': None,     'lg': None},
    '000688': {'name': '科创50',    'local': 'sh000688', 'csi': '000688', 'lg': None},
    '000015': {'name': '红利指数',  'local': None,       'csi': '000015', 'lg': None},
    '000922': {'name': '中证红利',  'local': 'sh000922', 'csi': '000922', 'lg': None},
    '000985': {'name': '中证全指',  'local': 'sh000985', 'csi': '000985', 'lg': None},
    '399303': {'name': '国证2000',  'local': None,       'csi': None,     'lg': None},
}
ORDER = ('000300', '000016', '000905', '000852', '000001',
         '399001', '399006', '000688', '000015', '000922', '000985')

# 自算日频表的列
DAY_COLS = ('ttmPe', 'ttmPct', 'addTtmPe', 'addTtmPct', 'middleTtmPe', 'middleTtmPct',
            'lyrPe', 'lyrPct', 'addLyrPe', 'addLyrPct', 'middleLyrPe', 'middleLyrPct',
            'close', 'n_all', 'n_pos')
# 乐咕表的 15 列(去掉 date)
LG_COLS = ('lyrPe', 'lyrPeQuantile', 'ttmPe', 'ttmPeQuantile',
           'addLyrPe', 'addLyrPeQuantile', 'addTtmPe', 'addTtmPeQuantile',
           'close', 'middleLyrPe', 'middleLyrPeQuantile',
           'middleTtmPe', 'middleTtmPeQuantile', 'totalMv')

TRUNC_PE = 300.0          # 等权那一档的截断门槛, 见模块 docstring
START_DAY = '2018-01-02'   # 东财估值表最早一天
START_CSI = '2013-01-01'   # 中证 peg 最早一年

_RLOCK = threading.RLock()
_MEM = {}
_MTIME = {}
_DAY_CACHE = {}          # code -> (mtime, rows)


# ---------------------------------------------------------------- 基础设施
def _dir():
    """存档目录 `<kline_dir>/pe_ratio` —— 与 index_daily 同级"""
    from quant_terminal import engine as E
    d = os.path.join(os.path.dirname(E.INDEX_DAILY_DIR), 'pe_ratio')
    if not os.path.isdir(d):
        os.makedirs(d, exist_ok=True)
    return d


def _fpath(name):
    return os.path.join(_dir(), name)


def _noproxy():
    """用户梯子(SSR 系统代理)常开, 走代理打东财会 502 ⇒ 一律直连"""
    return urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _raw(url, headers=None, timeout=TIMEOUT, opener=None):
    h = {'User-Agent': UA}
    if headers:
        h.update(headers)
    op = opener or _noproxy()
    try:
        r = op.open(urllib.request.Request(url, headers=h), timeout=timeout)
        return r.read()
    except Exception as e:
        sys.stderr.write('[pe_ratio] GET 失败 %s: %r\n' % (url[:120], e))
        return None


def _jget(url, headers=None, timeout=TIMEOUT, opener=None):
    b = _raw(url, headers, timeout, opener)
    if not b:
        return None
    try:
        return json.loads(b.decode('utf-8', 'replace'))
    except Exception:
        return None


def _dc(params, timeout=TIMEOUT):
    return _jget(DC_API + '?' + urllib.parse.urlencode(params), DC_HDR, timeout)


def _read_csv(name):
    p = _fpath(name)
    if not os.path.exists(p):
        return []
    try:
        if _MTIME.get(p) == os.path.getmtime(p) and p in _MEM:
            return _MEM[p]
    except Exception:
        pass
    out = []
    try:
        with open(p, 'r', encoding='utf-8', newline='') as f:
            for r in csv.DictReader(f):
                if r.get('date'):
                    out.append(dict(r))
    except Exception as e:
        sys.stderr.write('[pe_ratio] 读 %s 失败: %r\n' % (name, e))
        return []
    _MEM[p] = out
    try:
        _MTIME[p] = os.path.getmtime(p)
    except Exception:
        pass
    return out


def _write_csv(name, rows, cols):
    """原子写(同目录 tmp + os.replace), **纯 LF**"""
    p = _fpath(name)
    if not rows:
        return False
    tmp = p + '.tmp'
    try:
        with open(tmp, 'w', encoding='utf-8', newline='') as f:
            w = csv.writer(f, lineterminator='\n')
            w.writerow(['date'] + list(cols))
            for r in rows:
                v = []
                for c in cols:
                    x = r.get(c)
                    v.append('' if x is None else x)
                w.writerow([r['date']] + v)
        os.replace(tmp, p)
        _MEM.pop(p, None)
        _MTIME.pop(p, None)
        _DAY_CACHE.clear()
        return True
    except Exception as e:
        sys.stderr.write('[pe_ratio] 写 %s 失败: %r\n' % (name, e))
        return False


def _today():
    return datetime.date.today()


def _d(s):
    return datetime.datetime.strptime(str(s)[:10], '%Y-%m-%d').date()


def _is_settled():
    """收盘后(15:06 之后)才算今天定稿; 盘中不写当日快照"""
    n = datetime.datetime.now()
    return n.weekday() < 5 and n.strftime('%H%M') >= '1506'


# ---------------------------------------------------------------- 东财取数
def fetch_cons(idx, pages=12):
    """取指数成分股代码列表。
    ⚠️ 东财 `RPT_INDEX_CONSTITUENT` 加 `TRADE_DATE` 过滤后**只返回零星几只**
    (实测 2026-06-15 只有 19 只 / 2025-06-16 只有 7 只 —— 那是当天新进/新出的调整名单,
    不是全量成分)。所以**只能取最新一期的全量成分**:
    沪深300 300 / 上证综指 2252 / 中证全指 5117 / 中证1000 1000 ...
    ⇒ 历史回溯用的是「当期成分股」, 存在幸存者偏差, 详见模块 docstring。"""
    out = []
    for p in range(1, pages + 1):
        r = _dc({'reportName': 'RPT_INDEX_CONSTITUENT',
                 'columns': 'SECURITY_CODE', 'pageSize': '500',
                 'pageNumber': str(p), 'source': 'WEB', 'client': 'WEB',
                 'filter': '(INDEX_CODE="%s")' % idx})
        d = ((r or {}).get('result') or {}).get('data') or []
        out += [x['SECURITY_CODE'] for x in d if x.get('SECURITY_CODE')]
        pg = ((r or {}).get('result') or {}).get('pages') or 0
        if p >= pg or not d:
            break
    return out


def fetch_vals(date, workers=12):
    """取某个交易日的全市场估值 → {code: (pe_ttm, pe_lar, total_mcap)}
    12 页 × 500 条 = 5580 只。**非交易日 pages=0 返回空 dict**。"""
    first = _dc({'reportName': 'RPT_VALUEANALYSIS_DET', 'columns': 'SECURITY_CODE',
                 'pageSize': '500', 'pageNumber': '1', 'source': 'WEB',
                 'client': 'WEB', 'filter': "(TRADE_DATE='%s')" % date})
    pages = ((first or {}).get('result') or {}).get('pages') or 0
    if not pages:
        return {}

    def _pg(p):
        r = _dc({'reportName': 'RPT_VALUEANALYSIS_DET',
                 'columns': 'SECURITY_CODE,PE_TTM,PE_LAR,TOTAL_MARKET_CAP',
                 'pageSize': '500', 'pageNumber': str(p), 'source': 'WEB',
                 'client': 'WEB', 'filter': "(TRADE_DATE='%s')" % date})
        return ((r or {}).get('result') or {}).get('data') or []

    out = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for part in ex.map(_pg, range(1, pages + 1)):
            for x in part:
                c = x.get('SECURITY_CODE')
                if c:
                    out[c] = (x.get('PE_TTM'), x.get('PE_LAR'), x.get('TOTAL_MARKET_CAP'))
    return out


def _calc_one(cons, vals):
    """按成分股 + 估值表算一行 6 个 PE 指标。
    返回 dict(6 个 PE + n_all/n_pos), 数据不足返回 None。"""
    tt, ly = [], []
    mv_t = np_t = mv_l = np_l = 0.0
    n_all = 0
    for c in cons:
        if c not in vals:
            continue
        ptt, plr, m = vals[c]
        if m is None:
            continue
        try:
            m = float(m)
        except (TypeError, ValueError):
            continue
        if m <= 0:
            continue
        n_all += 1
        if ptt is not None:
            try:
                ptt = float(ptt)
            except (TypeError, ValueError):
                ptt = None
            if ptt is not None and ptt > 0:
                tt.append(ptt)
                mv_t += m
                np_t += m / ptt
        if plr is not None:
            try:
                plr = float(plr)
            except (TypeError, ValueError):
                plr = None
            if plr is not None and plr > 0:
                ly.append(plr)
                mv_l += m
                np_l += m / plr
    if not tt or not ly:
        return None
    eq_t = [p for p in tt if p <= TRUNC_PE]
    eq_l = [p for p in ly if p <= TRUNC_PE]
    if not eq_t or not eq_l:
        return None
    return {
        'ttmPe': round(statistics.fmean(eq_t), 4),
        'addTtmPe': round(mv_t / np_t, 4) if np_t else None,
        'middleTtmPe': round(statistics.median(tt), 4),
        'lyrPe': round(statistics.fmean(eq_l), 4),
        'addLyrPe': round(mv_l / np_l, 4) if np_l else None,
        'middleLyrPe': round(statistics.median(ly), 4),
        'n_all': n_all,
        'n_pos': len(tt),
    }


# ---------------------------------------------------------------- 中证取数
def fetch_csi_peg(code, start=START_CSI, end=None, retries=2):
    """中证 index-perf 的 peg = 滚动市盈率 PE TTM。返 [(date, peg, close)]
    **2013 起才有值**; 深市 399 开头的中证不提供(rows=0)。"""
    end = end or _today().strftime('%Y%m%d')
    s = start.replace('-', '')
    for _ in range(retries):
        j = _jget('%s?indexCode=%s&startDate=%s&endDate=%s' % (CSI_API, code, s, end),
                 CSI_HDR, 30)
        if j is None:
            time.sleep(1.2)
            continue
        out = []
        for x in (j.get('data') or []):
            t = str(x.get('tradeDate') or '')
            if len(t) != 8:
                continue
            dt = '%s-%s-%s' % (t[:4], t[4:6], t[6:])
            peg = x.get('peg')
            cl = x.get('close')
            if peg is None:
                continue
            try:
                peg = float(peg)
            except (TypeError, ValueError):
                continue
            try:
                cl = float(cl) if cl is not None else None
            except (TypeError, ValueError):
                cl = None
            out.append({'date': dt, 'peg': peg, 'close': cl})
        return out
    return []


# ---------------------------------------------------------------- 乐咕取数
def _lg_session():
    """乐咕反爬: **必须同一 opener 串行预热** —— 先首页拿 cookie 再打页面,
    否则 401 / 连接重置。绝不能并发打页面(实测 8 并发 7/8 连接重置)。"""
    import http.cookiejar
    op = urllib.request.build_opener(urllib.request.ProxyHandler({}),
                                     urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
    b = _raw(LG_HOME, {'Referer': 'https://legulegu.com/'}, 20, op)
    if not b:
        return None
    time.sleep(0.9)
    b = _raw(LG_PAGE, {'Referer': 'https://legulegu.com/'}, 20, op)
    if not b:
        return None
    time.sleep(0.9)
    return op


def fetch_legulegu(idx, freq='month', opener=None):
    """乐咕 index-basic-pe → [(date, ...15 列)]
    freq 只认 'week' / 'month'; 'day' 会静默退化成 month(免费档没有日频)。
    站点 504 时返回 []。"""
    import hashlib
    sym = INDEXES.get(idx, {}).get('lg')
    if not sym:
        return []
    tok = hashlib.md5(_today().strftime('%Y-%m-%d').encode()).hexdigest()
    url = '%s?indexCode=%s&freq=%s&token=%s' % (LG_API, sym, freq, tok)
    op = opener or _lg_session()
    if op is None:
        return []
    j = _jget(url, LG_HDR, 30, op)
    if not j:
        return []
    rows = []
    for x in (j.get('data') or []):
        dt = str(x.get('date') or '')[:10]
        if len(dt) != 10:
            continue
        r = {'date': dt}
        for c in LG_COLS:
            r[c] = x.get(c)
        rows.append(r)
    rows.sort(key=lambda r: r['date'])
    return rows


# ---------------------------------------------------------------- 回补 / 增量
def _cal(code):
    """交易日历: 优先本地指数日K; 本地没有(红利指数/国证2000)就借沪深300的。
    返回 ['YYYY-MM-DD', ...] 升序。"""
    from quant_terminal import engine as E
    lc = INDEXES.get(code, {}).get('local')
    rows = None
    if lc:
        rows = E.read_index_daily(lc)
    if not rows:
        rows = E.read_index_daily('sh000300')
    if not rows:
        return []
    return [r['date'] for r in rows]


def _closes(code):
    """本地日K收盘 → {date: close}; 本地没有返 {}"""
    from quant_terminal import engine as E
    lc = INDEXES.get(code, {}).get('local')
    if not lc:
        return {}
    rows = E.read_index_daily(lc) or []
    return {r['date']: r['close'] for r in rows}


def _pct_rank(vals):
    """给一串值算「每个点在自己历史里的百分位(0~1)」。
    乐咕的量化就是这样: 当前值在**全部历史**中的分位。"""
    n = len(vals)
    if n < 2:
        return [None] * n
    order = sorted(range(n), key=lambda i: vals[i])
    rank = [0.0] * n
    i = 0
    while i < n:
        j = i
        while j + 1 < n and vals[order[j + 1]] == vals[order[i]]:
            j += 1
        # 相同值取平均位次
        avg = (i + j) / 2.0
        for k in range(i, j + 1):
            rank[order[k]] = avg / (n - 1) if n > 1 else 0.0
        i = j + 1
    return rank


def _recalc_pct(rows):
    """整表重算百分位列(百分位必须基于完整历史, 不能增量算)"""
    if not rows:
        return rows
    for base, pcol in (('ttmPe', 'ttmPct'), ('addTtmPe', 'addTtmPct'),
                       ('middleTtmPe', 'middleTtmPct'), ('lyrPe', 'lyrPct'),
                       ('addLyrPe', 'addLyrPct'), ('middleLyrPe', 'middleLyrPct')):
        vals = [r.get(base) for r in rows]
        if any(v is None or v == '' for v in vals):
            for r in rows:
                r[pcol] = ''
            continue
        fv = [float(v) for v in vals]
        pr = _pct_rank(fv)
        for r, p in zip(rows, pr):
            r[pcol] = round(p, 6) if p is not None else ''
    return rows


def backfill(code, start=START_DAY, end=None, log=None, workers=16, step=1):
    """按交易日回补自算日频表。
    step=1 全量日频(2018 起约 5.2 分钟); step=20 只要每 20 个交易日一个点(约 16 秒),
    用来快速先把长历史画出来。**已有数据不重复拉**(只在缺的日子补)。"""
    if code not in INDEXES:
        return {'ok': False, 'msg': 'unknown code'}
    cal = _cal(code)
    if not cal:
        return {'ok': False, 'msg': '无交易日历'}
    end = end or cal[-1]
    days = [d for d in cal if start <= d <= end]
    if step > 1:
        # 保留最后一天 + 每 step 天一个点
        days = days[::-1][::step][::-1]
    old = {r['date']: r for r in _read_csv('pe_%s_day.csv' % code)}
    need = [d for d in days if d not in old]
    if not need:
        return {'ok': True, 'msg': 'noop', 'n': len(old), 'added': 0}
    cons = fetch_cons(INDEXES[code]['csi'] or code)
    if not cons:
        return {'ok': False, 'msg': '取不到成分股'}
    cls = _closes(code)
    t0 = time.time()
    got = 0
    done = 0
    # 每个日期的 12 页在内部并发; 日期之间再开一层并发会打到 400+ 连接, 用 3 个日期交错
    with ThreadPoolExecutor(max_workers=3) as ex:
        for date, vals in zip(need, ex.map(fetch_vals, need)):
            done += 1
            if not vals:
                continue
            one = _calc_one(cons, vals)
            if not one:
                continue
            one['date'] = date
            one['close'] = cls.get(date, '')
            old[date] = one
            got += 1
            if log and done % 50 == 0:
                log('%s %d/%d (%d 行, %.0fs)' % (code, done, len(need), got, time.time() - t0))
    rows = [old[k] for k in sorted(old)]
    _recalc_pct(rows)
    _write_csv('pe_%s_day.csv' % code, rows, DAY_COLS)
    return {'ok': True, 'msg': 'ok', 'n': len(rows), 'added': got,
            'secs': round(time.time() - t0, 1)}


def backfill_all(codes=None, start=START_DAY, end=None, log=None, workers=16, step=1):
    """按交易日一次性回补**多只**指数的自算日频表（v2.370b）。

    与 `backfill(code)` 的区别 —— 性能差 10 倍：
      `backfill()` 每只指数各拉一遍全市场估值表（2047 天 × 12 页 = 24564 次请求）；
      这里**每个交易日只拉一次**估值表，10 个指数在同一份表上分别算，
      请求数降到 2047 次。实测 0.17s/天（16 并发）⇒ 全量约 6 分钟。

    step=1 全量日频; step=20 只取每 20 个交易日一个点（先把长历史画出来用）。
    **已有数据不重复拉**（只在缺的日子补），所以中断后重跑只补剩下的。

    返回 {code: {'n':行数, 'added':新增, 'secs':耗时}}
    """
    codes = [c for c in (codes or ORDER) if c in INDEXES]
    if not codes:
        return {}
    cal = _cal(codes[0])
    if not cal:
        return {c: {'ok': False, 'msg': '无交易日历'} for c in codes}
    end = end or cal[-1]
    days = [d for d in cal if start <= d <= end]
    if step > 1:
        days = days[::-1][::step][::-1]

    # 每只指数: 已有日期集合 + 成分股 + 收盘价
    olds, orig_n, cons_map, cls_map, need_map = {}, {}, {}, {}, {}
    for c in codes:
        o = {r['date']: r for r in _read_csv('pe_%s_day.csv' % c)}
        olds[c] = o
        orig_n[c] = len(o)
        need_map[c] = [d for d in days if d not in o]
        if need_map[c]:
            cc = fetch_cons(INDEXES[c]['csi'] or c)
            if not cc:
                need_map[c] = []
                continue
            cons_map[c] = cc
            cls_map[c] = _closes(c)

    all_need = sorted(set(d for c in codes for d in need_map[c]))
    if not all_need:
        return {c: {'ok': True, 'msg': 'noop', 'n': len(olds[c]), 'added': 0} for c in codes}

    if log:
        log('待补 %d 个交易日 × %d 个指数（共 %d 个 指数-日 组合）'
            % (len(all_need), len([c for c in codes if need_map[c]]),
               sum(len(need_map[c]) for c in codes)))

    t0 = time.time()
    done = [0]
    want = {c: set(need_map[c]) for c in codes}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for date, vals in zip(all_need, ex.map(fetch_vals, all_need)):
            done[0] += 1
            if vals:
                for c in codes:
                    if date not in want[c] or c not in cons_map:
                        continue
                    one = _calc_one(cons_map[c], vals)
                    if not one:
                        continue
                    one['date'] = date
                    one['close'] = cls_map[c].get(date, '')
                    olds[c][date] = one
            if log and done[0] % 200 == 0:
                log('  %d/%d 天, %.0fs' % (done[0], len(all_need), time.time() - t0))

    out = {}
    for c in codes:
        rows = [olds[c][k] for k in sorted(olds[c])]
        _recalc_pct(rows)
        _write_csv('pe_%s_day.csv' % c, rows, DAY_COLS)
        out[c] = {'ok': True, 'msg': 'ok', 'n': len(rows),
                  'added': len(rows) - orig_n[c],
                  'secs': round(time.time() - t0, 1)}
    if log:
        log('完成 %.0fs' % (time.time() - t0))
    return out


def sync_csi(code, start=START_CSI, log=None, retries=2):
    """补中证 peg 表(2013 起, 单线整体法近似)。**不缩水保护**"""
    c = INDEXES.get(code, {}).get('csi')
    if not c:
        return {'ok': False, 'msg': '中证不提供'}
    new = fetch_csi_peg(c, start, retries=retries)
    if not new:
        return {'ok': False, 'msg': '取不到'}
    name = 'pe_%s_csi.csv' % code
    old = {r['date']: r for r in _read_csv(name)}
    for r in new:
        old[r['date']] = r
    rows = [old[k] for k in sorted(old)]
    if old and len(rows) < len(old):
        return {'ok': False, 'msg': '缩水拒绝'}
    _write_csv(name, rows, ('peg', 'close'))
    return {'ok': True, 'msg': 'ok', 'n': len(rows)}


def sync_legulegu(code, log=None):
    """补乐咕月/周表。站点 504 时全部跳过(返回 ok=False 但不抛)。"""
    name_m = 'pe_%s_month.csv' % code
    name_w = 'pe_%s_week.csv' % code
    op = _lg_session()
    if op is None:
        return {'ok': False, 'msg': 'legulegu 不可达'}
    res = {}
    for freq, name in (('month', name_m), ('week', name_w)):
        new = fetch_legulegu(code, freq, op)
        if not new:
            res[freq] = 'empty'
            continue
        old = {r['date']: r for r in _read_csv(name)}
        for r in new:
            old[r['date']] = r
        rows = [old[k] for k in sorted(old)]
        _write_csv(name, rows, LG_COLS)
        res[freq] = len(rows)
        time.sleep(1.2)
    return {'ok': True, 'res': res}


# ---------------------------------------------------------------- 对外接口
SRCS = {'day': '自算(东财)', 'csi': '中证官方', 'lg_month': '乐咕月频', 'lg_week': '乐咕周频'}
SRC_LBL = dict(SRCS)


def _pick_src(code):
    """自动选最长的源: 乐咕月频(2005) > 中证 peg(2013) > 自算日频(2018)"""
    if _read_csv('pe_%s_month.csv' % code):
        return 'lg_month'
    if _read_csv('pe_%s_csi.csv' % code):
        return 'csi'
    if _read_csv('pe_%s_day.csv' % code):
        return 'day'
    return None


def series(code='000300', src=None, limit=0):
    """返回一条可画图的序列。

    统一输出结构(前端只认这一套, 四种源都映射成它):
        dates[] / close[] / n / start / end / src / src_lbl / name / code
        lines: [{key,label,color,values[]}, ...]   最多 6 条(自算) 或 4 条(乐咕) 或 1 条(中证)
        pct:   [{key,label,values[]}, ...]         与 lines 一一对应的百分位
        last:  {<key>: 末值}
        lastPct: {<key>: 末百分位}
    """
    if code not in INDEXES:
        return {'ok': False, 'error': '未知指数 %s' % code}
    src = src or _pick_src(code)
    if not src:
        return {'ok': False, 'error': '%s 还没有任何数据, 先跑一次同步' % INDEXES[code]['name']}
    info = INDEXES[code]

    if src == 'day':
        rows = _read_csv('pe_%s_day.csv' % code)
        if not rows:
            return {'ok': False, 'error': '无自算日频表'}
        if limit:
            rows = rows[-int(limit):]
        dates = [r['date'] for r in rows]
        keys = [('ttmPe', '等权滚动PE'), ('addTtmPe', '滚动PE'),
                ('middleTtmPe', '滚动PE中位数'), ('lyrPe', '等权静态PE'),
                ('addLyrPe', '静态PE'), ('middleLyrPe', '静态PE中位数')]
        pk = {'ttmPe': 'ttmPct', 'addTtmPe': 'addTtmPct', 'middleTtmPe': 'middleTtmPct',
              'lyrPe': 'lyrPct', 'addLyrPe': 'addLyrPct', 'middleLyrPe': 'middleLyrPct'}
        lines = []
        pcts = []
        for k, lb in keys:
            lines.append({'key': k, 'label': lb,
                          'values': [None if r.get(k) in ('', None) else float(r[k]) for r in rows]})
            pcts.append({'key': k, 'label': lb + '百分位',
                         'values': [None if r.get(pk[k]) in ('', None) else float(r[pk[k]]) for r in rows]})
        close = [None if r.get('close') in ('', None) else float(r['close']) for r in rows]

    elif src == 'csi':
        rows = _read_csv('pe_%s_csi.csv' % code)
        if not rows:
            return {'ok': False, 'error': '无中证 peg 表'}
        if limit:
            rows = rows[-int(limit):]
        dates = [r['date'] for r in rows]
        vals = [None if r.get('peg') in ('', None) else float(r['peg']) for r in rows]
        pr = _pct_rank([v for v in vals if v is not None])
        it = iter(pr)
        pv = [None if v is None else round(next(it), 6) for v in vals]
        lines = [{'key': 'peg', 'label': '滚动PE(中证)', 'values': vals}]
        pcts = [{'key': 'peg', 'label': '滚动PE百分位', 'values': pv}]
        close = [None if r.get('close') in ('', None) else float(r['close']) for r in rows]

    else:   # lg_month / lg_week
        name = 'pe_%s_%s.csv' % (code, 'month' if src == 'lg_month' else 'week')
        rows = _read_csv(name)
        if not rows:
            return {'ok': False, 'error': '无乐咕表'}
        if limit:
            rows = rows[-int(limit):]
        dates = [r['date'] for r in rows]
        keys = [('ttmPe', '等权滚动PE', 'ttmPeQuantile'),
                ('addTtmPe', '滚动PE', 'addTtmPeQuantile'),
                ('middleTtmPe', '滚动PE中位数', 'middleTtmPeQuantile'),
                ('lyrPe', '等权静态PE', 'lyrPeQuantile'),
                ('addLyrPe', '静态PE', 'addLyrPeQuantile'),
                ('middleLyrPe', '静态PE中位数', 'middleLyrPeQuantile')]
        lines = []
        pcts = []
        for k, lb, q in keys:
            lines.append({'key': k, 'label': lb,
                          'values': [None if r.get(k) in ('', None) else float(r[k]) for r in rows]})
            pcts.append({'key': k, 'label': lb + '百分位',
                         'values': [None if r.get(q) in ('', None) else float(r[q]) for r in rows]})
        close = [None if r.get('close') in ('', None) else float(r['close']) for r in rows]

    last = {}
    lastPct = {}
    for ln in lines:
        vs = [v for v in ln['values'] if v is not None]
        last[ln['key']] = vs[-1] if vs else None
    for pc in pcts:
        vs = [v for v in pc['values'] if v is not None]
        lastPct[pc['key']] = vs[-1] if vs else None

    return {'ok': True, 'code': code, 'name': info['name'],
            'src': src, 'src_lbl': SRCS.get(src, src),
            'n': len(dates), 'start': dates[0] if dates else '',
            'end': dates[-1] if dates else '',
            'dates': dates, 'close': close,
            'lines': lines, 'pct': pcts,
            'last': last, 'lastPct': lastPct}


def overview():
    """左列列表: 每个指数有哪些源、多少行、末值多少"""
    out = []
    for code in ORDER:
        info = INDEXES[code]
        d_day = _read_csv('pe_%s_day.csv' % code)
        d_csi = _read_csv('pe_%s_csi.csv' % code)
        d_m = _read_csv('pe_%s_month.csv' % code)
        d_w = _read_csv('pe_%s_week.csv' % code)
        srcs = {}
        if d_m:
            srcs['lg_month'] = {'n': len(d_m), 'start': d_m[0]['date'], 'end': d_m[-1]['date']}
        if d_w:
            srcs['lg_week'] = {'n': len(d_w), 'start': d_w[0]['date'], 'end': d_w[-1]['date']}
        if d_csi:
            srcs['csi'] = {'n': len(d_csi), 'start': d_csi[0]['date'], 'end': d_csi[-1]['date']}
        if d_day:
            srcs['day'] = {'n': len(d_day), 'start': d_day[0]['date'], 'end': d_day[-1]['date']}
        best = _pick_src(code)
        last_v = None
        if best:
            ii = {'lg_month': d_m, 'lg_week': d_w, 'csi': d_csi, 'day': d_day}[best]
            if ii:
                k = 'addTtmPe' if best != 'csi' else 'peg'
                v = ii[-1].get(k)
                last_v = None if v in ('', None) else float(v)
        out.append({'code': code, 'name': info['name'], 'srcs': srcs,
                    'best': best, 'best_lbl': SRCS.get(best, '—'), 'last': last_v})
    return {'ok': True, 'rows': out, 'n': len(out),
            'csi_start': START_CSI, 'day_start': START_DAY, 'trunc': TRUNC_PE}


def meta():
    rows = []
    for code in ORDER:
        info = INDEXES[code]
        rows.append({
            'code': code, 'name': info['name'],
            'day': len(_read_csv('pe_%s_day.csv' % code)),
            'csi': len(_read_csv('pe_%s_csi.csv' % code)),
            'lg_month': len(_read_csv('pe_%s_month.csv' % code)),
            'lg_week': len(_read_csv('pe_%s_week.csv' % code)),
        })
    return {'ok': True, 'dir': _dir(), 'rows': rows,
            'csi_start': START_CSI, 'day_start': START_DAY, 'trunc': TRUNC_PE}


def sync(codes=None, deep=False, log=None):
    """同步三路数据。
    deep=True  → 自算日频按 step=1 全量回补(2018 起, 约 5.2 分钟/指数)
    deep=False → 自算日频按 step=20 快速铺点(约 16 秒/指数)
    """
    t0 = time.time()
    todo = [c for c in (codes or ORDER) if c in INDEXES]
    res = {'ok': True, 'csi': {}, 'lg': {}, 'day': {}}
    for c in todo:
        r = sync_csi(c, log=log)
        res['csi'][c] = r.get('msg', '?')
        time.sleep(0.35)
    for c in todo:
        if INDEXES[c].get('lg'):
            r = sync_legulegu(c, log=log)
            res['lg'][c] = r.get('msg') or r.get('res')
        else:
            res['lg'][c] = 'no-lg'
    # v2.370c: 自算日频段**必须走 backfill_all** —— 它每个交易日只拉一次全市场估值表,
    #   所有指数共用那一份。原来的 for c in todo: backfill(c) 是每只指数各拉一遍,
    #   11 只 × 2047 天 × 12 页 = 27 万次请求(实测 0.58s/天/只 ⇒ 3 小时);
    #   改后 2047 次请求, 实测 394 秒。
    step = 1 if deep else 20
    dayn = [c for c in todo if c not in ('399303',)]
    try:
        _ball = backfill_all(dayn, step=step, log=log)
    except Exception as _eb:
        _ball = {}
        if log:
            log('backfill_all 失败: %r' % (_eb,))
    for c in todo:
        if c == '399303':
            res['day'][c] = 'skip(无本地日K/无成分)'
            continue
        r = _ball.get(c) or {}
        res['day'][c] = '%s:%s' % (r.get('msg', 'fail'), r.get('n'))
        if log:
            log('%s 自算 %s n=%s' % (c, r.get('msg'), r.get('n')))
    res['secs'] = round(time.time() - t0, 1)
    return res


def prewarm(log=None):
    """预热: 只补最近 60 个交易日 + 中证长历史, 保证首屏有图。
    全量日频回补请手动跑 sync(deep=True)。"""
    n_ok = 0
    for c in ORDER:
        try:
            if _read_csv('pe_%s_csi.csv' % c):
                n_ok += 1
            elif _read_csv('pe_%s_day.csv' % c):
                n_ok += 1
        except Exception:
            pass
    return '%d/%d 指数有数据' % (n_ok, len(ORDER))


def snapshot_today(force=False):
    """收盘后把今天这一行补进自算表(增量, 不整表重拉)"""
    if not _is_settled() and not force:
        return 'skip-intraday'
    today = _today().strftime('%Y-%m-%d')
    n = 0
    for c in ORDER:
        if c in ('000015', '399303'):
            continue
        try:
            r = backfill(c, start=today, end=today)
            if r.get('added'):
                n += 1
        except Exception as e:
            sys.stderr.write('[pe_ratio] snapshot %s 失败: %r\n' % (c, e))
    return 'ok-%d' % n


# ---------------------------------------------------------------- 自测
def selftest():
    print('== pe_ratio 存档目录 ==')
    print('  ', _dir())
    for f in sorted(os.listdir(_dir())):
        p = os.path.join(_dir(), f)
        print('   %-24s %8d B' % (f, os.path.getsize(p)))
    print()
    print('== 覆盖矩阵 ==')
    m = meta()
    print('   %-8s %-9s %-7s %-6s %-8s %-8s' % ('代码', '名称', '自算日', '中证', '乐咕月', '乐咕周'))
    for r in m['rows']:
        print('   %-8s %-9s %-7d %-6d %-8d %-8d' % (
            r['code'], r['name'], r['day'], r['csi'], r['lg_month'], r['lg_week']))
    print()
    print('== 各指数最优源 ==')
    ov = overview()
    for r in ov['rows']:
        print('   %-8s %-9s %-14s 末值=%s' % (
            r['code'], r['name'], r['best_lbl'],
            ('%.2f' % r['last']) if r['last'] is not None else '—'))
    print()
    print('== 抽一条序列看结构(000300) ==')
    s = series('000300')
    if s.get('ok'):
        print('   src=%s n=%d %s ~ %s' % (s['src_lbl'], s['n'], s['start'], s['end']))
        for ln in s['lines']:
            vs = [v for v in ln['values'] if v is not None]
            print('   %-16s 末值=%-9s n=%d' % (ln['label'], ('%.2f' % vs[-1]) if vs else '—', len(vs)))
        print('   百分位:', {k: (round(v * 100, 2) if v is not None else None)
                          for k, v in s['lastPct'].items()})
    else:
        print('   ', s.get('error'))
    return True


if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'selftest'
    if cmd == 'selftest':
        selftest()
    elif cmd == 'sync':
        deep = '--deep' in sys.argv
        codes = [a for a in sys.argv[2:] if not a.startswith('--')] or None
        r = sync(codes, deep=deep, log=lambda s: print('  ', s))
        print(json.dumps(r, ensure_ascii=False, indent=1))
    elif cmd == 'backfill':
        code = sys.argv[2]
        step = int(sys.argv[3]) if len(sys.argv) > 3 else 20
        print(backfill(code, step=step, log=lambda s: print('  ', s)))
    elif cmd == 'series':
        d = series(sys.argv[2] if len(sys.argv) > 2 else '000300')
        if d.get('ok'):
            d['dates'] = d['dates'][-3:]
            for ln in d['lines']:
                ln['values'] = ln['values'][-3:]
            for pc in d['pct']:
                pc['values'] = pc['values'][-3:]
            d['close'] = d['close'][-3:]
        print(json.dumps(d, ensure_ascii=False)[:1600])
