# -*- coding: utf-8 -*-
"""宏观指标面板 (v2.388, 2026-11-25 用户 m05084)

需求来源
--------
用户：「可以做一个市场总览界面中，指数一览后面加一个『宏观指标』，具体怎么做我也不懂，
你先做出来我看看效果」

设计口径（与用户先前讨论确定的四条一致）
----------------------------------------
1. **本地存档优先**：所有序列落 `kline_data/macro/<key>.csv`，页面只读本地文件 ⇒ 秒开、
   断网可看、akshare 挂了也不影响。抓取是单独的「↻ 更新」动作，不在渲染路径上。
2. **月频一律 step 图**：两次发布之间真实值"未知且不变"，折线插值会造出不存在的中间走势。
3. **显示"数据截止"而不是"抓取时间"**：宏观数据发布滞后 1~2 个月很常见，拿旧值当现值
   是最容易犯的错。前端每张卡都标最后数据日 + 距今天数。
4. **每个源独立 try**：一个接口挂了只影响它自己，不拖垮整页。

数据源
------
· akshare（免费、国内直连为主）: bond_zh_us_rate / macro_china_* / macro_usa_cpi_yoy /
  macro_global_sox_index
· 东财 push2his 曾经能取到美元指数/纳指100, 但 2026-11-25 起对本机 IP 主动断连
  (RemoteDisconnected); 实测多镜像 x 多参数组合 10/10 全失败 ⇒ 不再依赖它。
  纳指100 改用 akshare index_us_stock_sina('.NDX')。
· 美元指数(DXY) 没找到任何可用的免费历史源: 新浪只有当日快照、腾讯 day 数组为空、
  FRED 超时、stooq 人机校验、yahoo 403 ⇒ 这一项换成**人民币汇率(中行美元牌价)**,
  对 A 股更直接(外资流入/流出压力)。

文件
----
kline_data/macro/<key>.csv   date,value   (纯 LF)
kline_data/macro/_meta.json  {fetched_at, items:{key:{name,...,last_date}}}
"""
import io
import json
import os
import re
import sys
import threading
import time
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAC_DIR = os.path.join(BASE_DIR, 'kline_data', 'macro')
META_FILE = os.path.join(MAC_DIR, '_meta.json')
HS300_FILE = os.path.join(BASE_DIR, 'kline_data', 'index_daily', 'SH000300.csv')

SCHEMA = 1
DAY_CAP = 2500          # 日频序列最多存这么多点 (约 10 年)
_LOCK = threading.RLock()
_CS = {}                # path -> (mtime, rows)
_FETCHING = {'on': False, 'started': 0.0, 'msg': ''}
_LAST_FETCH = {'at': '', 'ok': None, 'wrote': {}, 'errors': {}, 'secs': 0}

# ---------------------------------------------------------------------------
# 指标定义（顺序 = 前端展示顺序；good: 'up' 越高越好 / 'down' 越低越好 / '' 中性）
# ---------------------------------------------------------------------------
INDICATORS = [
    # ---- 利率 ----
    dict(key='cn_10y',   group='利率', name='中国10年期国债',   short='中债10Y',
         unit='%', freq='D', dp=2, good='down', src='akshare bond_zh_us_rate',
         note='国内无风险利率之锚 —— 它往下走，股票的相对吸引力就上升'),
    dict(key='us_10y',   group='利率', name='美国10年期国债',   short='美债10Y',
         unit='%', freq='D', dp=2, good='down', src='akshare bond_zh_us_rate',
         note='全球资产定价基准 —— 它往上冲，全世界股票估值一起被压'),
    dict(key='cn_us_spread', group='利率', name='中美利差(中-美)', short='中美利差',
         unit='%', freq='D', dp=2, good='up', src='计算 = 中债10Y - 美债10Y',
         note='负数越深，人民币资产越受压制、外资越容易流出'),
    dict(key='us_curve', group='利率', name='美债期限利差(10Y-2Y)', short='美债10-2',
         unit='%', freq='D', dp=2, good='', src='akshare bond_zh_us_rate',
         note='倒挂(负值)历史上是衰退的领先信号，但 2022-23 那次失效了 —— 只可参考'),
    dict(key='lpr1y',    group='利率', name='LPR 1年期',       short='LPR1Y',
         unit='%', freq='M', dp=2, good='down', src='akshare macro_china_lpr',
         note='贷款市场报价利率 —— 国内降息周期的直接刻度'),
    # ---- 通胀 ----
    dict(key='cn_cpi',   group='通胀', name='中国CPI同比',      short='CPI',
         unit='%', freq='M', dp=1, good='', src='akshare macro_china_cpi',
         note='决定央行能不能继续降息；长期低位=需求弱，也可能意味着政策空间大'),
    dict(key='cn_ppi',   group='通胀', name='中国PPI同比',      short='PPI',
         unit='%', freq='M', dp=1, good='', src='akshare macro_china_ppi',
         note='工业企业出厂价，领先 A 股（尤其上游周期）盈利约 1~2 个季度'),
    dict(key='us_cpi',   group='通胀', name='美国CPI同比',      short='US CPI',
         unit='%', freq='M', dp=1, good='down', src='akshare macro_usa_cpi_yoy',
         note='决定美联储降息节奏 —— 它是全球流动性的总闸门'),
    # ---- 货币 ----
    dict(key='cn_m2',    group='货币', name='M2同比',           short='M2',
         unit='%', freq='M', dp=1, good='up', src='akshare macro_china_money_supply',
         note='广义货币 —— 水位。持续下行说明在收水'),
    dict(key='cn_m1',    group='货币', name='M1同比',           short='M1',
         unit='%', freq='M', dp=1, good='up', src='akshare macro_china_money_supply',
         note='狭义货币 = 活钱。M1 抬头往往领先 A 股回暖'),
    dict(key='cn_m1m2',  group='货币', name='M1-M2 剪刀差',     short='M1-M2',
         unit='pp', freq='M', dp=1, good='up', src='计算 = M1同比 - M2同比',
         note='A 股最有效的流动性指标之一：差值回升 = 活钱变多、资金开始往外走'),
    # ---- 增长 ----
    dict(key='cn_pmi',   group='增长', name='中国制造业PMI',    short='PMI',
         unit='', freq='M', dp=1, good='up', src='akshare macro_china_pmi',
         note='50 是荣枯线。最灵敏的景气指标，领先企业盈利 1~2 个季度'),
    dict(key='cn_gdp',   group='增长', name='中国GDP累计同比',  short='GDP',
         unit='%', freq='Q', dp=1, good='up', src='akshare macro_china_gdp',
         note='季度累计同比 —— 慢变量，只用来判断大方向，不改仓'),
    # ---- 海外 / 科技周期 ----
    dict(key='usdcny',   group='海外', name='人民币汇率(中行)', short='USD/CNY',
         unit='元', freq='D', dp=4, good='down', src='akshare currency_boc_safe 中行美元牌价 / 100',
         note='美元兑人民币。数字变大 = 人民币贬值 ⇒ 外资流出压力大、A 股承压'),
    dict(key='sox',      group='海外', name='费城半导体指数',   short='SOX',
         unit='', freq='D', dp=1, good='up', src='akshare macro_global_sox_index',
         note='全球半导体周期最直接的读数 —— 科技行情真正的驱动变量'),
    dict(key='ndx',      group='海外', name='纳斯达克100',      short='NDX',
         unit='', freq='D', dp=2, good='up', src='akshare index_us_stock_sina(.NDX)',
         note='美股科技风向。A 股科技板块的中期节奏与它高度相关'),
]

BY_KEY = {d['key']: d for d in INDICATORS}
GROUPS = ['利率', '通胀', '货币', '增长', '海外']

# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------
def _d(s):
    """把各种日期写法统一成 'YYYY-MM-DD'"""
    s = str(s or '').strip()
    m = re.match(r'^(\d{4})[-/](\d{1,2})[-/](\d{1,2})', s)
    if m:
        return '%04d-%02d-%02d' % (int(m.group(1)), int(m.group(2)), int(m.group(3)))
    return ''


def _month_of(s):
    """'2026年10月份' / '202610' / '2026-10-01' → '2026-10-01'（归到当月 1 日）"""
    s = str(s or '').strip()
    m = re.search(r'(\d{4})\D{0,2}(\d{1,2})', s)
    if m and 1 <= int(m.group(2)) <= 12:
        return '%04d-%02d-01' % (int(m.group(1)), int(m.group(2)))
    return ''


def _quarter_of(s):
    """'2026年第1-3季度' → '2026-09-01'（归到累计期末月）"""
    s = str(s or '').strip()
    m = re.search(r'(\d{4})\D+?第\s*(\d)\s*(?:[-~至]\s*(\d)\s*)?季度', s)
    if not m:
        return ''
    q = int(m.group(3) or m.group(2))
    return '%04d-%02d-01' % (int(m.group(1)), min(12, q * 3))


def _num(v):
    try:
        f = float(v)
    except Exception:
        return None
    if f != f or f in (float('inf'), float('-inf')):   # NaN / inf
        return None
    return f


def _clean(rows):
    """[(date, value)] → 去空 / 去重 / 升序 / 裁剪"""
    out, seen = [], set()
    for dt, v in rows:
        dt = _d(dt)
        if not dt or dt in seen:
            continue
        f = _num(v)
        if f is None:
            continue
        seen.add(dt)
        out.append((dt, round(f, 6)))
    out.sort(key=lambda x: x[0])
    return out[-DAY_CAP:]


def _csv_write(path, rows):
    tmp = path + '.tmp'
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with io.open(tmp, 'w', encoding='utf-8', newline='') as fh:
        fh.write('date,value\n')
        for dt, v in rows:
            fh.write('%s,%s\n' % (dt, ('%g' % v)))
    os.replace(tmp, path)


def _csv_read(path):
    try:
        mt = os.path.getmtime(path)
    except OSError:
        return []
    hit = _CS.get(path)
    if hit and hit[0] == mt:
        return hit[1]
    rows = []
    try:
        with io.open(path, 'r', encoding='utf-8', newline='') as fh:
            head = fh.readline()
            if not head:
                return []
            for ln in fh:
                ln = ln.strip()
                if not ln:
                    continue
                p = ln.split(',')
                if len(p) < 2:
                    continue
                f = _num(p[1])
                if f is None:
                    continue
                rows.append((p[0], f))
    except Exception:
        return []
    _CS[path] = (mt, rows)
    return rows


def _path(key):
    return os.path.join(MAC_DIR, key + '.csv')


def _diff_series(a, b):
    """两条序列按日期求差（取交集）"""
    mb = dict(b)
    out = []
    for dt, v in a:
        w = mb.get(dt)
        if w is None:
            continue
        out.append((dt, v - w))
    return out


# ---------------------------------------------------------------------------
# 取数
# ---------------------------------------------------------------------------
def _ak():
    """延迟导入 akshare

    ⚠ akshare 底层依赖 py_mini_racer(Rust dll), **不是线程安全的** —— 两个线程同时调用
    不同 akshare 接口时, 实测 mini_racer.dll 会段错误, 把整个服务进程打死(无 traceback)。
    ⇒ 所有抓取必须串行执行(见 refresh)。这里不 extra 加锁, 靠 refresh 里的顺序循环保证。
    """
    import akshare as ak
    return ak


def _f_usdcny():
    """美元兑人民币 —— 中行外汇牌价(单位: 元/100 外币), 除以 100 得 元/美元"""
    df = _ak().currency_boc_safe()
    cm = {str(c): c for c in df.columns}
    out = []
    for _, r in df.iterrows():
        dt = _d(r[cm['日期']])
        v = _num(r[cm['美元']])
        if dt and v is not None:
            out.append((dt, v / 100.0))
    return {'usdcny': out}


def _f_bond():
    """中美国债收益率（一个源出 4 条序列）"""
    df = _ak().bond_zh_us_rate(start_date='20050101')
    cm = {str(c): c for c in df.columns}
    dcol = cm.get('日期')
    out = {'cn_10y': [], 'us_10y': [], 'us_curve': []}
    for _, r in df.iterrows():
        dt = _d(r[dcol])
        if not dt:
            continue
        for key, col in (('cn_10y', '中国国债收益率10年'),
                         ('us_10y', '美国国债收益率10年'),
                         ('us_curve', '美国国债收益率10年-2年')):
            if col in cm:
                out[key].append((dt, r[cm[col]]))
    return out


def _f_lpr():
    df = _ak().macro_china_lpr()
    cm = {str(c): c for c in df.columns}
    out = []
    for _, r in df.iterrows():
        dt = _d(r[cm['TRADE_DATE']])
        v = r.get(cm.get('LPR1Y'))
        if dt and _num(v) is not None:
            out.append((dt, v))
    return {'lpr1y': out}


def _f_cn_cpi():
    df = _ak().macro_china_cpi()
    cm = {str(c): c for c in df.columns}
    out = []
    for _, r in df.iterrows():
        dt = _month_of(r[cm['月份']])
        if dt:
            out.append((dt, r[cm['全国-同比增长']]))
    return {'cn_cpi': out}


def _f_cn_ppi():
    df = _ak().macro_china_ppi()
    cm = {str(c): c for c in df.columns}
    out = []
    for _, r in df.iterrows():
        dt = _month_of(r[cm['月份']])
        if dt:
            out.append((dt, r[cm['当月同比增长']]))
    return {'cn_ppi': out}


def _f_cn_pmi():
    df = _ak().macro_china_pmi()
    cm = {str(c): c for c in df.columns}
    out = []
    for _, r in df.iterrows():
        dt = _month_of(r[cm['月份']])
        if dt:
            out.append((dt, r[cm['制造业-指数']]))
    return {'cn_pmi': out}


def _f_cn_money():
    df = _ak().macro_china_money_supply()
    cm = {str(c): c for c in df.columns}
    c2 = cm.get('货币和准货币(M2)-同比增长')
    c1 = cm.get('货币(M1)-同比增长')
    m2, m1 = [], []
    for _, r in df.iterrows():
        dt = _month_of(r[cm['月份']])
        if not dt:
            continue
        if c2 is not None:
            m2.append((dt, r[c2]))
        if c1 is not None:
            m1.append((dt, r[c1]))
    return {'cn_m2': m2, 'cn_m1': m1}


def _f_cn_gdp():
    df = _ak().macro_china_gdp()
    cm = {str(c): c for c in df.columns}
    out = []
    for _, r in df.iterrows():
        dt = _quarter_of(r[cm['季度']])
        if dt:
            out.append((dt, r[cm['国内生产总值-同比增长']]))
    return {'cn_gdp': out}


def _f_us_cpi():
    df = _ak().macro_usa_cpi_yoy()
    cm = {str(c): c for c in df.columns}
    out = []
    for _, r in df.iterrows():
        dt = _month_of(r[cm['时间']])
        if dt:
            out.append((dt, r[cm['现值']]))
    return {'us_cpi': out}


def _f_sox():
    df = _ak().macro_global_sox_index()
    cm = {str(c): c for c in df.columns}
    out = []
    for _, r in df.iterrows():
        dt = _d(r[cm['日期']])
        if dt:
            out.append((dt, r[cm['最新值']]))
    return {'sox': out}


def _f_ndx():
    """纳指100 —— 新浪美股指数(akshare 封装), 实测 3212 行到 2026-11-24"""
    df = _ak().index_us_stock_sina(symbol='.NDX')
    out = []
    for _, r in df.iterrows():
        dt = _d(r['date'])
        v = _num(r['close'])
        if dt and v is not None:
            out.append((dt, v))
    return {'ndx': out}


FETCHERS = [
    ('中美国债', _f_bond),        # 慢 (6~20s) 但一个源出 3 条
    ('LPR', _f_lpr),
    ('中国CPI', _f_cn_cpi),
    ('中国PPI', _f_cn_ppi),
    ('中国PMI', _f_cn_pmi),
    ('货币供应', _f_cn_money),
    ('中国GDP', _f_cn_gdp),
    ('美国CPI', _f_us_cpi),
    ('费半', _f_sox),             # 最慢 (40s)
    ('人民币汇率', _f_usdcny),
    ('纳斯达克', _f_ndx),
]

COMBOS = [
    ('cn_us_spread', 'cn_10y', 'us_10y'),
    ('cn_m1m2', 'cn_m1', 'cn_m2'),
]


# ---------------------------------------------------------------------------
# 刷新
# ---------------------------------------------------------------------------
class _NullW(io.TextIOBase):
    """pythonw 下 sys.stdout/stderr 是 None, akshare 的进度条会炸 -> 给它一个黑洞。"""
    def write(self, s):
        return len(s) if s else 0

    def flush(self):
        pass


def refresh(keys=None, log=None):
    """抓全部（或指定）指标并落盘。返回 {ok, wrote:{key:n}, errors:{name:msg}, secs}"""
    t0 = time.time()
    got, errors = {}, {}
    _FETCHING.update({'on': True, 'started': t0, 'msg': 'running'})
    # v2.388: 服务由 pythonw 启动时 sys.stdout / sys.stderr 是 None,
    #   akshare 里有三个源带 tqdm 进度条会往 stdout 写 => AttributeError。
    #   抓取期间临时塞一个黑洞, 抓完还原(不能一直换, 别的模块可能要用真 stdout)。
    _sv = (sys.stdout, sys.stderr)
    try:
        if sys.stdout is None or sys.stderr is None:
            _nw = _NullW()
            sys.stdout = sys.stdout or _nw
            sys.stderr = sys.stderr or _nw
        return _refresh_inner(t0, keys, log, got, errors)
    finally:
        sys.stdout, sys.stderr = _sv


def _refresh_inner(t0, keys, log, got, errors):

    # ⚠ 必须串行: akshare(py_mini_racer) 并发调用会让整个进程段错误, 见 _ak() 注释
    for name, fn in FETCHERS:
        try:
            res = fn()
        except Exception as e:
            err = '%s: %s' % (type(e).__name__, str(e)[:180])
            errors[name] = err
            if log:
                log('x %s %s' % (name, err))
            continue
        got.update(res or {})
        if log:
            log('ok %s' % name)

    # 组合指标
    for key, ka, kb in COMBOS:
        if ka in got and kb in got:
            got[key] = _diff_series(got[ka], got[kb])

    wrote = {}
    with _LOCK:
        for d in INDICATORS:
            key = d['key']
            if key not in got:
                continue
            rows = _clean(got[key])
            if not rows:
                continue
            if len(rows) > DAY_CAP:
                rows = rows[-DAY_CAP:]
            _csv_write(_path(key), rows)
            _CS.pop(_path(key), None)
            wrote[key] = len(rows)
        _write_meta(wrote)

    _FETCHING.update({'on': False, 'msg': 'done'})
    _LAST_FETCH.update({'at': _now(), 'ok': bool(wrote), 'wrote': wrote,
                        'errors': errors, 'secs': round(time.time() - t0, 1)})
    return dict(ok=bool(wrote), wrote=wrote, errors=errors,
                secs=round(time.time() - t0, 1),
                fetched_at=_now())


def _now():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def _read_meta():
    try:
        with io.open(META_FILE, 'r', encoding='utf-8') as fh:
            o = json.load(fh)
        return o if isinstance(o, dict) else {}
    except Exception:
        return {}


def _write_meta(wrote=None):
    meta = _read_meta()
    meta['schema'] = SCHEMA
    meta['fetched_at'] = _now()
    items = meta.get('items') if isinstance(meta.get('items'), dict) else {}
    for d in INDICATORS:
        key = d['key']
        rows = _csv_read(_path(key))
        if not rows:
            continue
        items[key] = {'name': d['name'], 'unit': d['unit'], 'freq': d['freq'],
                      'last_date': rows[-1][0], 'n': len(rows)}
    meta['items'] = items
    tmp = META_FILE + '.tmp'
    os.makedirs(MAC_DIR, exist_ok=True)
    with io.open(tmp, 'w', encoding='utf-8', newline='') as fh:
        json.dump(meta, fh, ensure_ascii=False, indent=1)
    os.replace(tmp, META_FILE)


def _hs300(n=1400):
    """沪深300 收盘序列 —— 本地 CSV, 只读"""
    rows = []
    try:
        with io.open(HS300_FILE, 'r', encoding='utf-8', newline='') as fh:
            head = fh.readline()
            cols = [c.strip() for c in head.split(',')]
            ci = cols.index('close') if 'close' in cols else 2
            for ln in fh:
                ln = ln.strip()
                if not ln:
                    continue
                p = ln.split(',')
                if len(p) <= ci:
                    continue
                f = _num(p[ci])
                if f is None:
                    continue
                rows.append([_d(p[0]), round(f, 2)])
    except Exception:
        return []
    return rows[-n:]


def _days_since(iso):
    try:
        return (datetime.now() - datetime.strptime(iso, '%Y-%m-%d')).days
    except Exception:
        return None


def view(full=True):
    """给前端的面板数据。全本地读取, 不发网络请求。"""
    meta = _read_meta()
    items = []
    for d in INDICATORS:
        rows = _csv_read(_path(d['key']))
        if not rows:
            items.append(dict(d, n=0, last_date='', stale_days=None,
                              value=None, prev=None, chg=None, series=[]))
            continue
        last_date, last_v = rows[-1]
        prev_v = rows[-2][1] if len(rows) > 1 else None
        chg = (last_v - prev_v) if (prev_v is not None) else None
        items.append(dict(d, n=len(rows), last_date=last_date,
                          stale_days=_days_since(last_date),
                          value=round(last_v, 6), prev=prev_v,
                          chg=(round(chg, 6) if chg is not None else None),
                          series=[[dt, round(v, 6)] for dt, v in rows]))
    return {'ok': True, 'schema': SCHEMA, 'fetched_at': meta.get('fetched_at', ''),
            'groups': GROUPS, 'items': items, 'hs300': _hs300()}


FETCH_STUCK_SEC = 300.0     # 超过这个时长还 on=True 就当线程已死, 允许重新发起


def fetching_now():
    """真正的"正在抓" —— 卡死超过 FETCH_STUCK_SEC 的旧状态自动作废 (v2.388)。"""
    if not _FETCHING.get('on'):
        return False
    st = _FETCHING.get('started') or 0.0
    if st and (time.time() - st) > FETCH_STUCK_SEC:
        _FETCHING.update({'on': False, 'msg': 'stuck-reset'})
        try:
            _LAST_FETCH.update({'errors': dict(_LAST_FETCH.get('errors') or {},
                                               **{'?': '上一次抓取超时 (%d 秒), 已自动复位' % int(time.time() - st)})})
        except Exception:
            pass
        return False
    return True


def status():
    meta = _read_meta()
    return {'dir': MAC_DIR, 'fetched_at': meta.get('fetched_at', ''),
            'fetching': fetching_now(),
            'fetch_started': _FETCHING.get('started') or 0,
            'last_fetch': dict(_LAST_FETCH),
            'files': {d['key']: len(_csv_read(_path(d['key']))) for d in INDICATORS}}


if __name__ == '__main__':
    import sys
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    t = time.time()
    r = refresh(log=lambda s: print(s, flush=True))
    print('---')
    print('ok=%s secs=%s fetched_at=%s' % (r['ok'], r['secs'], r['fetched_at']))
    print('wrote:', json.dumps(r['wrote'], ensure_ascii=False))
    if r['errors']:
        print('errors:', json.dumps(r['errors'], ensure_ascii=False, indent=1))
    print('total wall %.1fs' % (time.time() - t))
    print(json.dumps(status(), ensure_ascii=False, indent=1))
