# -*- coding: utf-8 -*-
"""
quant_terminal/equity_bond.py —— 股债利差 (equity-bond spread)

v2.369 新建。复刻 legulegu.com/stockdata/equity-bond-spread 的「股债利差」指标。

## 三套公式（照乐咕官方说明，逐字对齐）
1. `pe`     : 股债利差 = (1 / 指数加权滚动市盈率TTM) − 10年期国债收益率(%) / 100
              ⇒ **小数**量纲, 0.0627 表示 6.27 个百分点
2. `dvttm`  : 股债利差 = 10年期国债收益率 − 指数等权股息率TTM        ⇒ **百分点**量纲
3. `dv`     : 股债利差 = 10年期国债收益率 − 指数静态股息率            ⇒ **百分点**量纲

注意量纲差异: `pe` 要除以 100 才是百分点, `dv`/`dvttm` 本身就是百分点。
乐咕返回的原始数据就是这个口径(`peSpread` 0.0627 / `dvSpread` -1.76), 前端画图时
按公式分别给 axisLabel 的 formatter, 不要统一乘 100 —— 会差两个数量级。

## 数据源(2026-11-22 实测, 全部 http 直连, 不走系统代理)
- **沪深300 全套(含三套公式 + 指数收盘)**: 乐咕自家 API
    GET https://legulegu.com/api/stockdata/equity-bond-spread?code=000300.SH&token=<md5(today)>
    - 必须先访问页面拿 cookie(acw_tc/LAAA/JSESSIONID), 否则 401
    - token = md5(YYYY-MM-DD)
    - 返回 5248 行 / 2005-04-08 起 / 12 字段:
      date, close, peSpread, dvSpread, dvTtmSpread,
      peSpreadAverage, dvSpreadAverage, dvTtmSpreadAverage,
      peSpreadStandardDeviation, dvSpreadStandardDeviation, dvTtmSpreadStandardDeviation, code
    - **只有 000300.SH 免费**; 000300.LG / 000016.SH / 000905.SH / 10.SH 全返 {"vip":false}
      (已验证 5 种 token 变体 / http / &vip=1 / 去点号 / 小写 .lg, 全是空)
- **上证50 / 中证500 的 PE TTM**: 中证指数官网
    GET https://www.csindex.com.cn/csindex-home/perf/index-perf?indexCode=000905&startDate=20130101&endDate=...
    - 返回的 **`peg` 字段就是滚动市盈率 PE TTM**(不是 PEG 成长比, 名字有误导)
    - **2013 年起才有值**(2012 只有 5 行有, 2005~2011 全 None)
    - **口径与乐咕不同**: 中证是官方加权 PE(沪深300 2026-11-20 = 13.09),
      乐咕是「成分股 Σ总市值 ÷ Σ(总市值/PE_TTM)、剔 PE≤0」的自算值(12.739)
      ⇒ 中证档的 1/PE 会比乐咕档低约 0.002, 这是**数据源差异不是 bug**, 前端要标 src。
- **10 年期国债收益率**: 东财数据中心
    GET https://datacenter-web.eastmoney.com/api/data/v1/get
        ?reportName=RPTA_WEB_TREASURYYIELD&columns=SOLAR_DATE,EMM00166466
        &pageSize=500&pageNumber=N&sortColumns=SOLAR_DATE&sortTypes=-1
    - **`EMM00166466` = 中国 10 年期国债收益率(%)**
    - total 9393 条 / 回溯到 1990 年 / 19 页

## 已确认做不了的(别再试)
- 乐咕非 000300.SH 的所有档 ⇒ VIP 锁定
- **沪深300(非金融)**: 唯一路子是「东财成分股 + 全市场估值自算」, 但
  **东财 `RPT_VALUEANALYSIS_DET` 只能回溯到 2018-01-02**(2017 及更早全空)
  ⇒ 只能做 2018 年起, 与乐咕图上 2005 起差 13 年, 已放弃
- 股息率(公式二/三)的其他指数: 东财 RPT_INDEX_DIVIDEND / RPT_INDEX_TS_DIVIDEND /
  RPT_VALUE_INDEX_DIV 三个报表全 `报表配置不存在(code 9501)`; 中证 div-yield /
  indicator-detail / perf-index-indicator 三条全 404; 申万挂; ⇒ 只有乐咕 000300.SH 有

## 布林线(通道) —— **不是固定交易日数, 是按「月份」回推, 这是关键**
乐咕图上「均线 + 上下轨」= N 期滚动均值 ± 2×滚动标准差。
**但它的 N 是「日历月」不是「交易日数」**: 实测把 5248 个点逐个反解,
唯一能让 `peSpreadAverage` 与 `peSpreadStandardDeviation` **5248/5248 全部零残差(<=5e-7)**
的规则是:

    窗口 = 从「当前日期往前推 W 个月」的那个月的**最后一天**开始(闭区间, 含当天)

即 `W=30`(2.5 年) 时: 起点 = `date(t.year, t.month-30 回退, 该月最后一天)`,
然后取所有 `date >= 起点` 的交易日。用固定交易根数(610/609/611...)永远对不上,
最多只有 20% 命中 —— **别再按交易日数试**。

实测对照(5248 点, 容差 5e-7):
| 规则 | avg 命中 | sd 命中 |
|---|---|---|
| W=30 月末对齐 闭区间 | **100.000%** | **100.000%** |
| W=30 按日回退(d28) 闭区间 | 99.104% | 99.104% |
| W=30 月末对齐 开区间 | 49.962% | 50.305% |
| W=29 / W=31 任意变体 | 11.3~11.6% | 11.3~11.7% |
| 固定 610/609/611 交易日 | <= 20.5% | <= 20.9% |

标准差用**总体标准差**(除以 n, 不是 n-1), 已由这 100% 命中确认。
本模块支持 30(2.5年) / 36(3年) / 60(5年) 三档; 乐咕官方档位名就叫 2.5年/3年/5年。
前 W 个月之前没有均线(值为 None), 前端 allowNull。

## 存档
`<kline_dir>/../equity_bond/` 下四类 CSV(纯 LF, 原子写):
- `eb_legulegu_000300.csv`: date,close,pe,dv,dv_ttm             (乐咕原样)
- `eb_treasury.csv`         : date,yld10                        (国债)
- `eb_peg_000300.csv` / `eb_peg_000016.csv` / `eb_peg_000905.csv`: date,peg,close
每日增量: 只拉最后一个已有日之后的, 不重拉全量(国债除外, 它一次能拿 500 天)。
"""
import csv
import datetime
import hashlib
import http.cookiejar
import json
import os
import sys
import threading
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/120.0 Safari/537.36')
TIMEOUT = 25
LG_PAGE = 'https://legulegu.com/stockdata/equity-bond-spread'
LG_API = 'https://legulegu.com/api/stockdata/equity-bond-spread'
LG_HDR = {'Referer': LG_PAGE, 'X-Requested-With': 'XMLHttpRequest',
          'Accept': 'application/json, text/plain, */*'}
CSI_API = 'https://www.csindex.com.cn/csindex-home/perf/index-perf'
CSI_HDR = {'Referer': 'https://www.csindex.com.cn/'}
DC_API = 'https://datacenter-web.eastmoney.com/api/data/v1/get'

# 指标支持的三个标的
TARGETS = {
    '000300': {'name': '沪深300', 'lg': '000300.SH', 'csi': '000300',
               'local': 'sh000300', 'formulas': ('pe', 'dvttm', 'dv')},
    '000016': {'name': '上证50', 'lg': None, 'csi': '000016',
               'local': 'sh000016', 'formulas': ('pe',)},
    '000905': {'name': '中证500', 'lg': None, 'csi': '000905',
               'local': 'sz399905', 'formulas': ('pe',)},
}
ORDER = ('000300', '000016', '000905')
FORMULA_LBL = {'pe': '1/市盈率', 'dvttm': '等权股息率TTM', 'dv': '静态股息率'}
# 均线档位(**月份数**, 不是交易日数) —— 30 与乐咕 2.5 年档 5248/5248 零残差
MA_WIN = {30: '2.5年', 36: '3年', 60: '5年'}
MA_DEF = 30
BOLL_K = 2.0

_RLOCK = threading.RLock()
_MEM = {}
_MTIME = {}


# ---------------------------------------------------------------- 基础设施
def _dir():
    """存档目录: <kline_dir>/../equity_bond —— 跟 index_daily 同级"""
    from quant_terminal import engine as E
    d = os.path.join(os.path.dirname(E.KLINE_DIR), 'equity_bond')
    if not os.path.isdir(d):
        try:
            os.makedirs(d, exist_ok=True)
        except Exception:
            pass
    return d


def _fpath(name):
    return os.path.join(_dir(), name + '.csv')


def _opener_with_cookies():
    jar = http.cookiejar.CookieJar()
    return urllib.request.build_opener(urllib.request.ProxyHandler({}),
                                       urllib.request.HTTPCookieProcessor(jar))


def _noproxy():
    return urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _raw(url, headers=None, timeout=TIMEOUT, opener=None):
    """直连 GET(系统代理常开, 走代理打这些站点会 502)"""
    op = opener or _noproxy()
    h = {'User-Agent': UA}
    if headers:
        h.update(headers)
    try:
        r = op.open(urllib.request.Request(url, headers=h), timeout=timeout)
        return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, b''
    except Exception as e:
        sys.stderr.write('[equity_bond] GET 失败 %s: %s\n' % (url[:110], repr(e)[:110]))
        return None, b''


def _jget(url, headers=None, timeout=TIMEOUT, opener=None):
    st, b = _raw(url, headers, timeout, opener)
    if st != 200 or not b:
        return None
    try:
        return json.loads(b.decode('utf-8', 'replace'))
    except Exception:
        return None


def _read_csv(name):
    """读存档 → [dict]; 文件不存在返 []; 坏行丢弃"""
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
            rd = csv.DictReader(f)
            for r in rd:
                try:
                    d = {'date': r['date']}
                    for k, v in r.items():
                        if k != 'date':
                            d[k] = float(v) if v not in (None, '') else None
                    out.append(d)
                except Exception:
                    continue
    except Exception as e:
        sys.stderr.write('[equity_bond] 读 %s 失败: %r\n' % (name, e))
        return []
    out.sort(key=lambda x: x['date'])
    try:
        _MEM[p] = out
        _MTIME[p] = os.path.getmtime(p)
    except Exception:
        pass
    return out


def _write_csv(name, rows, cols):
    """原子写(同目录 tmp + os.replace), 纯 LF"""
    p = _fpath(name)
    if not rows:
        return False
    tmp = p + '.tmp'
    try:
        with open(tmp, 'w', encoding='utf-8', newline='') as f:
            w = csv.writer(f, lineterminator='\n')
            w.writerow(['date'] + list(cols))
            for r in rows:
                w.writerow([r['date']] + ['' if r.get(c) is None else r.get(c) for c in cols])
        os.replace(tmp, p)
        _MEM.pop(p, None)
        _MTIME.pop(p, None)
        return True
    except Exception as e:
        sys.stderr.write('[equity_bond] 写 %s 失败: %r\n' % (name, e))
        try:
            os.remove(tmp)
        except Exception:
            pass
        return False


def _today():
    return datetime.date.today().strftime('%Y-%m-%d')


# ---------------------------------------------------------------- 一、乐咕(沪深300 全套)
def fetch_legulegu(retries=2):
    """拉乐咕 000300.SH 全量 → 写 eb_legulegu_000300.csv

    返回 (ok, n, msg)。ok=False 时**绝不覆盖已有存档**。
    """
    tok = hashlib.md5(_today().encode()).hexdigest()
    last = None
    for _i in range(retries + 1):
        op = _opener_with_cookies()
        _raw(LG_PAGE, {'Referer': 'https://legulegu.com/'}, opener=op)   # 拿 cookie
        d = _jget('%s?code=000300.SH&token=%s' % (LG_API, tok), LG_HDR, 60, op)
        rows = (d or {}).get('data') or []
        if rows:
            last = rows
            break
    if not last:
        return False, 0, '乐咕返回空(可能限流或 token 失效)'
    out = []
    for r in last:
        dt = str(r.get('date') or '')[:10]
        if not dt:
            continue
        try:
            out.append({
                'date': dt,
                'close': float(r['close']) if r.get('close') is not None else None,
                'pe': float(r['peSpread']) if r.get('peSpread') is not None else None,
                'dv': float(r['dvSpread']) if r.get('dvSpread') is not None else None,
                'dv_ttm': float(r['dvTtmSpread']) if r.get('dvTtmSpread') is not None else None,
            })
        except Exception:
            continue
    out.sort(key=lambda x: x['date'])
    if not out:
        return False, 0, '解析后 0 行'
    old = _read_csv('eb_legulegu_000300')
    # 不缩水保护(与 engine.ensure_index_daily 同一条铁律)
    if old and len(out) < len(old):
        return False, len(old), '新数据 %d 行 < 本地 %d 行, 拒绝覆盖' % (len(out), len(old))
    _write_csv('eb_legulegu_000300', out, ('close', 'pe', 'dv', 'dv_ttm'))
    return True, len(out), '%s ~ %s' % (out[0]['date'], out[-1]['date'])


# ---------------------------------------------------------------- 二、国债 10Y
def fetch_treasury(pages=3, retries=2):
    """拉东财中国 10 年期国债收益率 → 写 eb_treasury.csv

    pages=3 够覆盖最近 1500 条(约 6 年); 首次建库用 pages=0 表示拉全量。
    """
    out = {}
    pn = 1
    empty = 0
    while True:
        if pages and pn > pages:
            break
        u = ('%s?reportName=RPTA_WEB_TREASURYYIELD'
             '&columns=SOLAR_DATE,EMM00166466&pageSize=500&pageNumber=%d'
             '&sortColumns=SOLAR_DATE&sortTypes=-1' % (DC_API, pn))
        d = _jget(u)
        res = (d or {}).get('result') or {}
        rr = res.get('data') or []
        if not rr:
            empty += 1
            if empty >= 2:
                break
            pn += 1
            continue
        for r in rr:
            dt = str(r.get('SOLAR_DATE') or '')[:10]
            v = r.get('EMM00166466')
            if not dt or v in (None, ''):
                continue
            try:
                out[dt] = float(v)
            except Exception:
                continue
        total_pages = res.get('pages') or 0
        if total_pages and pn >= total_pages:
            break
        pn += 1
    if not out:
        return False, 0, '国债返回空'
    rows = [{'date': k, 'yld10': out[k]} for k in sorted(out)]
    old = _read_csv('eb_treasury')
    merged = {r['date']: r['yld10'] for r in old}
    for r in rows:
        merged[r['date']] = r['yld10']
    full = [{'date': k, 'yld10': merged[k]} for k in sorted(merged)]
    _write_csv('eb_treasury', full, ('yld10',))
    return True, len(full), '%s ~ %s' % (full[0]['date'], full[-1]['date'])


# ---------------------------------------------------------------- 三、中证 peg(三档 PE TTM)
def fetch_csi_peg(code, start='20130101', retries=2):
    """拉中证 index-perf 的 peg 字段(= PE TTM) → 写 eb_peg_<code>.csv

    中证接口支持区间查询, 一次给多行(约 47~59 行/季)。分年拉能覆盖全历史。
    """
    tgt = TARGETS.get(code)
    if not tgt:
        return False, 0, '未知标的 %s' % code
    got = {}
    # 从 2013 年到今天, 按 3 年一段拉(接口对长区间没限制, 但分段更稳)
    y0 = int(start[:4])
    y1 = int(_today()[:4])
    for y in range(y0, y1 + 1, 3):
        s = '%d0101' % y
        e = '%d1231' % min(y + 2, y1)
        u = '%s?indexCode=%s&startDate=%s&endDate=%s' % (CSI_API, tgt['csi'], s, e)
        d = _jget(u, CSI_HDR, 40)
        for r in ((d or {}).get('data') or []):
            td = str(r.get('tradeDate') or '')
            if len(td) != 8:
                continue
            dt = '%s-%s-%s' % (td[:4], td[4:6], td[6:])
            peg = r.get('peg')
            cl = r.get('close')
            if peg in (None, '', 0):
                continue
            try:
                got[dt] = {'date': dt, 'peg': float(peg),
                           'close': float(cl) if cl is not None else None}
            except Exception:
                continue
    if not got:
        return False, 0, '中证 peg 返回空'
    old = _read_csv('eb_peg_' + code)
    for r in old:
        if r['date'] not in got and r.get('peg'):
            got[r['date']] = r
    rows = [got[k] for k in sorted(got)]
    if old and len(rows) < len(old):
        return False, len(old), '新数据 %d 行 < 本地 %d 行, 拒绝覆盖' % (len(rows), len(old))
    _write_csv('eb_peg_' + code, rows, ('peg', 'close'))
    return True, len(rows), '%s ~ %s' % (rows[0]['date'], rows[-1]['date'])


# ---------------------------------------------------------------- 四、拼序列
def _yield_map():
    return {r['date']: r['yld10'] for r in _read_csv('eb_treasury') if r.get('yld10') is not None}


def _ffill_lookup(mp, dates, keys):
    """按日期前向填充: 返回 {date: 值} —— 国债在周末/节假日没有值"""
    out = {}
    last = None
    ks = sorted(keys)
    import bisect
    for d in dates:
        i = bisect.bisect_right(ks, d) - 1
        if i >= 0:
            last = mp.get(ks[i])
        out[d] = last
    return out


def _index_close_local(code):
    """本地指数日K收盘价(双轴用的红线)"""
    from quant_terminal import engine as E
    rows = E.read_index_daily(code) or []
    return {r['date']: r['close'] for r in rows}


def _d(s):
    """'YYYY-MM-DD' → date"""
    return datetime.date(int(s[0:4]), int(s[5:7]), int(s[8:10]))


def _back_months_eom(d, w):
    """往前推 w 个月, 取「同月同日」; 若目标月没有这天, 取该月**最后一天**

    等价于 dateutil.relativedelta(months=-w) 的行为 —— 也就是
    `date(y, m, min(d.day, 该月天数))`。**不是**取该月月末(那是另一条规则,
    实测命中 50% 不到)。w 可为负(往后推)。
    """
    import calendar
    y, m = d.year, d.month - w
    while m <= 0:
        m += 12
        y -= 1
    while m > 12:
        m -= 12
        y += 1
    return datetime.date(y, m, min(d.day, calendar.monthrange(y, m)[1]))


def series(code='000300', formula='pe', ma=MA_DEF, limit=0):
    """组装一条完整的股债利差序列

    返回 dict:
      ok, code, name, formula, formula_lbl, ma, ma_lbl, src,
      dates[], spread[], close[], avg[], upper[], lower[],
      n, start, end, last, last_avg, last_z, pct_rank, yld_last, pe_last
    """
    tgt = TARGETS.get(code)
    if not tgt:
        return {'ok': False, 'error': '未知标的 %s' % code}
    if formula not in tgt['formulas']:
        return {'ok': False, 'error': '%s 不支持 %s 公式' % (tgt['name'], FORMULA_LBL.get(formula, formula))}
    try:
        ma = int(ma)
    except (TypeError, ValueError):
        ma = MA_DEF
    if ma not in MA_WIN:
        ma = MA_DEF

    ymp = _yield_map()
    src = ''
    dated = []          # [(date, spread, close)]

    if tgt['lg']:
        # —— 沪深300: 乐咕现成(12 字段, 三套公式全有)
        rows = _read_csv('eb_legulegu_000300')
        if rows:
            src = 'legulegu'
            for r in rows:
                key = {'pe': 'pe', 'dv': 'dv', 'dvttm': 'dv_ttm'}[formula]
                sp = r.get(key)
                if sp is None:
                    continue
                dated.append((r['date'], float(sp), r.get('close')))
        # 乐咕挂了就退回「中证 PE + 国债自算」(只有 pe 公式)
        if not dated and formula == 'pe':
            rows = _read_csv('eb_peg_000300')
            if rows:
                src = 'csi+treasury'
                ym = _ffill_lookup(ymp, [r['date'] for r in rows], ymp.keys())
                for r in rows:
                    y = ym.get(r['date'])
                    if y is None or not r.get('peg'):
                        continue
                    dated.append((r['date'], 1.0 / float(r['peg']) - y / 100.0, r.get('close')))
    else:
        # —— 上证50 / 中证500: 中证 peg + 国债自算(只能算 pe 公式)
        rows = _read_csv('eb_peg_' + code)
        if rows:
            src = 'csi+treasury'
            ym = _ffill_lookup(ymp, [r['date'] for r in rows], ymp.keys())
            for r in rows:
                y = ym.get(r['date'])
                if y is None or not r.get('peg'):
                    continue
                dated.append((r['date'], 1.0 / float(r['peg']) - y / 100.0, r.get('close')))

    if not dated:
        return {'ok': False, 'error': '本地还没有 %s 的股债利差数据, 请先同步' % tgt['name']}

    dated.sort(key=lambda x: x[0])
    dates = [x[0] for x in dated]
    spread = [x[1] for x in dated]
    close = [x[2] for x in dated]

    # 指数收盘(双轴红线): 乐咕给了 close 就用它, 否则取本地指数日K
    if any(c is None for c in close):
        lc = _index_close_local(tgt['local'])
        close = [c if c is not None else lc.get(d) for d, c in zip(dates, close)]
    if all(c is None for c in close):
        lc = _index_close_local(tgt['local'])
        close = [lc.get(d) for d in dates]

    # —— 布林线(通道): 按**月份**回推(不是固定交易日数), 月末对齐 + 闭区间
    #    实测这条规则对乐咕 5248 个点 avg/sd 全部零残差(<=5e-7); 固定 610 根只有 20%
    #    注意: 数据首日之前没有更早的历史 ⇒ 前若干个点用「从首日到当期」的**扩张窗口**
    #    (乐咕首行 avg == spread / sd == 0.0, 就是扩张窗口到只剩一个点)
    avg = [None] * len(spread)
    up = [None] * len(spread)
    lo = [None] * len(spread)
    if dates:
        objs = [_d(s) for s in dates]
        P1 = [0.0] * (len(spread) + 1)
        P2 = [0.0] * (len(spread) + 1)
        for i, v in enumerate(spread):
            P1[i + 1] = P1[i] + v
            P2[i + 1] = P2[i] + v * v
        n_dates = len(objs)
        for i in range(n_dates):
            thr = _back_months_eom(objs[i], ma)
            j = i
            while j > 0 and objs[j - 1] >= thr:
                j -= 1
            n = i - j + 1
            s1 = P1[i + 1] - P1[j]
            s2 = P2[i + 1] - P2[j]
            m = s1 / n
            var = s2 / n - m * m
            if var < 0:
                var = 0.0
            sd = var ** 0.5
            avg[i] = m
            up[i] = m + BOLL_K * sd
            lo[i] = m - BOLL_K * sd

    last = spread[-1]
    last_avg = avg[-1]
    last_sd = None
    if avg[-1] is not None and up[-1] is not None:
        last_sd = (up[-1] - avg[-1]) / BOLL_K
    last_z = ((last - last_avg) / last_sd) if (last_sd and last_sd > 0) else None
    pct = sum(1 for v in spread if v <= last) * 100.0 / len(spread)

    ym_last = _ffill_lookup(ymp, [dates[-1]], ymp.keys()).get(dates[-1])
    pe_last = None
    if formula == 'pe' and ym_last is not None:
        pe_last = 1.0 / (last + ym_last / 100.0) if (last + ym_last / 100.0) > 0 else None

    n_cut = 0
    if limit:
        try:
            n_cut = max(0, len(dates) - int(limit))
        except (TypeError, ValueError):
            n_cut = 0

    def _cut(a):
        return a[n_cut:] if n_cut else a

    return {
        'ok': True,
        'code': code,
        'name': tgt['name'],
        'formula': formula,
        'formula_lbl': FORMULA_LBL.get(formula, formula),
        'ma': ma,
        'ma_lbl': MA_WIN.get(ma, '%d日' % ma),
        'src': src,
        'dates': _cut(dates),
        'spread': _cut(spread),
        'close': _cut(close),
        'avg': _cut(avg),
        'upper': _cut(up),
        'lower': _cut(lo),
        'n': len(dates),
        'start': dates[0],
        'end': dates[-1],
        'last': round(last, 6),
        'last_avg': None if last_avg is None else round(last_avg, 6),
        'last_z': None if last_z is None else round(last_z, 3),
        'pct_rank': round(pct, 1),
        'yld_last': None if ym_last is None else round(ym_last, 4),
        'pe_last': None if pe_last is None else round(pe_last, 3),
    }


def overview():
    """左列要的三行元信息(每个标的默认 pe 公式的最新值)"""
    out = []
    for c in ORDER:
        t = TARGETS[c]
        try:
            s = series(c, 'pe', MA_DEF, limit=1)
        except Exception:
            s = {'ok': False}
        out.append({
            'code': c, 'name': t['name'],
            'formulas': list(t['formulas']),
            'ok': bool(s.get('ok')),
            'last': s.get('last'), 'last_z': s.get('last_z'),
            'pct_rank': s.get('pct_rank'), 'end': s.get('end'),
            'start': s.get('start'), 'n': s.get('n'), 'src': s.get('src'),
        })
    return {'ok': True, 'rows': out, 'ma_def': MA_DEF,
            'ma_list': [{'v': k, 'lbl': v} for k, v in MA_WIN.items()],
            'formula_lbl': FORMULA_LBL}


# ---------------------------------------------------------------- 五、同步 / 预热
def sync(deep=False, log=None):
    """增量同步三张源表。deep=True 时国债拉全量(19 页)。

    返回 {'legulegu': (...), 'treasury': (...), 'peg': {code: (...)}, 'secs': ...}
    """
    import time
    t0 = time.time()
    out = {'ok': True}
    try:
        out['legulegu'] = fetch_legulegu()
    except Exception as e:
        out['legulegu'] = (False, 0, repr(e)[:90])
    if log:
        log('乐咕 沪深300: %s %d 行 %s' % out['legulegu'])
    try:
        out['treasury'] = fetch_treasury(pages=0 if deep else 3)
    except Exception as e:
        out['treasury'] = (False, 0, repr(e)[:90])
    if log:
        log('国债 10Y: %s %d 条 %s' % out['treasury'])
    peg = {}
    for c in ORDER:
        try:
            peg[c] = fetch_csi_peg(c)
        except Exception as e:
            peg[c] = (False, 0, repr(e)[:90])
        if log:
            _ok, _n, _m = peg[c]
            log('中证 peg %s: %s %d 行 %s' % (c, _ok, _n, _m))
    out['peg'] = peg
    out['secs'] = round(time.time() - t0, 1)
    return out


def prewarm(log=None):
    """启动预热: 拉一次源表 + 让每个标的都算一遍(填内存缓存)"""
    r = sync(deep=False, log=log)
    ok = 0
    for c in ORDER:
        try:
            s = series(c, 'pe', MA_DEF)
            if s.get('ok'):
                ok += 1
        except Exception:
            pass
    return '%d/%d 标的可用 (%ss)' % (ok, len(ORDER), r.get('secs'))


def snapshot_today(force=False):
    """每日增量归档: 调一次 sync, 把最新数据落到 CSV

    **盘中(<15:06)不写** —— 中证 peg 与乐咕都在收盘后才更新, 盘中拉到的是
    forming 值(乐咕是按实时价算的, 会随分时跳动)。
    """
    now = datetime.datetime.now()
    if not force:
        if now.weekday() >= 5:
            return 'skip-weekend'
        if now.strftime('%H%M') < '1506':
            return 'skip-intraday'
    try:
        r = sync(deep=False)
        return 'ok-%ss' % r.get('secs')
    except Exception as e:
        return 'fail-%s' % repr(e)[:60]


def meta():
    """三个标的各自的深度信息"""
    out = []
    for c in ORDER:
        t = TARGETS[c]
        lg = _read_csv('eb_legulegu_000300') if t['lg'] else []
        pg = _read_csv('eb_peg_' + c)
        tr = _read_csv('eb_treasury')
        out.append({
            'code': c, 'name': t['name'], 'src': 'legulegu' if lg else 'csi+treasury',
            'legulegu': len(lg) if t['lg'] else 0,
            'peg': len(pg),
            'peg_start': pg[0]['date'] if pg else '',
            'treasury': len(tr),
            'treasury_start': tr[0]['date'] if tr else '',
            'formulas': list(t['formulas']),
        })
    return {'ok': True, 'rows': out, 'dir': _dir()}


def selftest():
    print('=' * 92)
    print('存档目录: %s' % _dir())
    print('--- 源表 ---')
    for n in ('eb_legulegu_000300', 'eb_treasury', 'eb_peg_000300', 'eb_peg_000016', 'eb_peg_000905'):
        r = _read_csv(n)
        print('  %-22s %5d 行  %s' % (n, len(r),
                                      ('%s ~ %s' % (r[0]['date'], r[-1]['date'])) if r else '(空)'))
    print('--- 三档 × 各公式 ---')
    for c in ORDER:
        for f in TARGETS[c]['formulas']:
            s = series(c, f, MA_DEF)
            if not s.get('ok'):
                print('  %-8s %-6s  ✗ %s' % (c, f, s.get('error')))
                continue
            print('  %-8s %-6s  n=%4d %s~%s  last=%.6f avg=%s z=%s 分位=%s%% src=%s'
                  % (c, f, s['n'], s['start'], s['end'], s['last'],
                     ('%.6f' % s['last_avg']) if s['last_avg'] is not None else 'None',
                     s['last_z'], s['pct_rank'], s['src']))
    print('--- 三档均线档(单位=月份) ---')
    for ma in (30, 36, 60):
        s = series('000300', 'pe', ma)
        print('  ma=%-5d %-6s n=%d avg=%s up=%s lo=%s'
              % (ma, MA_WIN.get(ma), s['n'],
                 ('%.6f' % s['last_avg']) if s.get('last_avg') is not None else 'None',
                 ('%.6f' % s['upper'][-1]) if s.get('upper', [None])[-1] is not None else 'None',
                 ('%.6f' % s['lower'][-1]) if s.get('lower', [None])[-1] is not None else 'None'))
    print('--- 与乐咕原始字段对拍(最后一行) ---')
    lg = _read_csv('eb_legulegu_000300')
    if lg:
        l = lg[-1]
        print('  乐咕末行 %s: close=%.2f peSpread=%.6f dvSpread=%.4f dvTtmSpread=%.4f'
              % (l['date'], l['close'] or 0, l['pe'] or 0, l['dv'] or 0, l['dv_ttm'] or 0))
        for ma in (30,):
            s = series('000300', 'pe', ma)
            if s.get('ok') and s['last_avg'] is not None:
                print('  自算 ma=%d月: last=%.6f avg=%.6f (乐咕 peSpreadAverage=0.061726)'
                      % (ma, s['last'], s['last_avg']))
    print('--- overview() ---')
    ov = overview()
    for r in ov['rows']:
        print('  %-8s %-8s ok=%-5s last=%-10s z=%-8s 分位=%-6s %s~%s (%d)'
              % (r['code'], r['name'], r['ok'], r['last'], r['last_z'],
                 r['pct_rank'], r['start'], r['end'], r['n']))
    print('=' * 92)


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'sync':
        deep = '--deep' in sys.argv
        print(json.dumps(sync(deep=deep, log=lambda m: print('  ' + m)),
                         ensure_ascii=False, indent=1, default=str))
    elif len(sys.argv) > 1 and sys.argv[1] == 'meta':
        print(json.dumps(meta(), ensure_ascii=False, indent=1))
    else:
        selftest()
