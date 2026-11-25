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
· 美联储政策利率: 纽约联储 EFFR 接口(2000-07 起, 带 targetRate 区间) —— v2.389 新增
· 叠加走势线: 上证/创业板/科创综/沪深300 读本地 index_daily;
  道指/纳指 走 akshare index_us_stock_sina('.DJI'/'.IXIC') —— v2.389 新增
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
    dict(key='us_fed_up', group='利率', name='美联储目标利率上限', short='Fed利率',
         unit='%', freq='D', dp=2, good='down', step=True,
         src='纽约联储 EFFR 接口 targetRateTo (2008-12 之前美联储只设单一目标利率)',
         note='全球资金的"出厂价"。加息周期压制所有风险资产估值, 降息周期反过来 —— '
              '阶梯线就是每次议息会议的决议, 台阶往上=加息、往下=降息'),
    dict(key='us_effr',  group='利率', name='有效联邦基金利率 EFFR', short='EFFR',
         unit='%', freq='D', dp=2, good='down',
         src='纽约联储 EFFR 接口 percentRate',
         note='市场真实成交的隔夜资金利率。它和上面那条目标利率的差 = 市场对政策松紧的实际感受'),
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
         unit='%', freq='M', dp=2, good='down', step=True, src='akshare macro_china_lpr',
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
# 可叠加的股市走势线（v2.389 用户 m05614：要选定了才显示）
#   rel   = 本地指数日线（终端自己维护, 不需要抓）
#   mac   = 落在 kline_data/macro/ 下的序列（由 refresh 抓）
# ---------------------------------------------------------------------------
BENCHES = [
    dict(key='us_dji',   name='道指',     mac='us_dji',  src='akshare index_us_stock_sina(.DJI)'),
    dict(key='us_ixic',  name='纳指',     mac='us_ixic', src='akshare index_us_stock_sina(.IXIC)'),
    dict(key='sh000001', name='上证指数', rel='kline_data/index_daily/SH000001.csv', src='本地 index_daily'),
    dict(key='sz399006', name='创业板指', rel='kline_data/index_daily/SZ399006.csv', src='本地 index_daily'),
    dict(key='sh000680', name='科创综指', rel='kline_data/index_daily/SH000680.csv', src='本地 index_daily'),
    dict(key='hs300',    name='沪深300',  rel='kline_data/index_daily/SH000300.csv', src='本地 index_daily'),
]

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
            # v2.389: %g 只有 6 位有效数字, 50018.6992 会被砍成 50018.7
            #   ⇒ 改成一个不会丢精度的写法(最多 6 位小数, 去掉尾巴的 0)
            _s = ('%.6f' % v).rstrip('0').rstrip('.')
            fh.write('%s,%s\n' % (dt, _s if _s else '0'))
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


def _f_us_idx():
    """美股指数(新浪, akshare 封装) —— 一次取三条

    .NDX  纳斯达克100  → 指标卡 ndx
    .DJI  道琼斯工业   → 叠加线 us_dji
    .IXIC 纳斯达克综合 → 叠加线 us_ixic (日常说的"纳指"就是这个)
    实测 .NDX 3212 行 / .DJI 5763 行 / .IXIC 5761 行, 都到 2026-11-24
    """
    ak = _ak()
    out = {}
    for sym, key in (('.NDX', 'ndx'), ('.DJI', 'us_dji'), ('.IXIC', 'us_ixic')):
        try:
            df = ak.index_us_stock_sina(symbol=sym)
        except Exception:
            continue                      # 单个失败不影响另外两条
        rows = []
        for _, r in df.iterrows():
            dt = _d(r['date'])
            v = _num(r['close'])
            if dt and v is not None:
                rows.append((dt, v))
        if rows:
            out[key] = rows
    return out


def _f_fed():
    """美联储政策利率 —— 纽约联储 EFFR 接口(免费, 无需 key, 不用梯子)

    实测 startDate=2000-01-01 返回 6633 条, 2000-07-03 ~ 2026-11-24, 约 1.5 MB / 1~3 秒。
    字段: effectiveDate / percentRate / targetRateFrom / targetRateTo / ...
      · targetRateTo 从 2008-12-16 才开始有 (那天美联储正式改用"区间"制度),
        之前只有单一目标利率 targetRateFrom ⇒ 回退用它, 保证 2000 年以来连续。
    返回:
      us_fed_up  目标利率上限 (%)  → 阶梯图, 台阶=每次议息决议
      us_effr    实际成交有效利率(%) → 平滑日线
    ⚠ 这是"政策利率"; 市场收益率(美债10Y)走 akshare bond_zh_us_rate, 别混
    """
    import requests
    s = requests.Session()
    s.trust_env = False               # 绕开用户的 SSR 系统代理(否则可能挂 100+ 秒)
    u = ('https://markets.newyorkfed.org/api/rates/unsecured/effr/search.json'
         '?startDate=2000-01-01&endDate=2099-12-31')
    r = s.get(u, timeout=40, headers={'User-Agent': 'Mozilla/5.0'})
    refs = (r.json() or {}).get('refRates') or []
    up, eff = [], []
    for x in refs:
        dt = _d(x.get('effectiveDate'))
        if not dt:
            continue
        v = _num(x.get('targetRateTo'))
        if v is None:
            v = _num(x.get('targetRateFrom'))
        if v is not None:
            up.append((dt, v))
        e = _num(x.get('percentRate'))
        if e is not None:
            eff.append((dt, e))
    return {'us_fed_up': up, 'us_effr': eff}


# (名字, 函数, 产出的 key) —— 第三个字段用于 refresh(keys=[...]) 的定点刷新
FETCHERS = [
    ('中美国债', _f_bond, ('cn_10y', 'us_10y', 'us_curve')),   # 慢 (6~20s) 但一个源出 3 条
    ('LPR', _f_lpr, ('lpr1y',)),
    ('中国CPI', _f_cn_cpi, ('cn_cpi',)),
    ('中国PPI', _f_cn_ppi, ('cn_ppi',)),
    ('中国PMI', _f_cn_pmi, ('cn_pmi',)),
    ('货币供应', _f_cn_money, ('cn_m2', 'cn_m1')),
    ('中国GDP', _f_cn_gdp, ('cn_gdp',)),
    ('美国CPI', _f_us_cpi, ('us_cpi',)),
    ('费半', _f_sox, ('sox',)),                                # 最慢 (40s)
    ('人民币汇率', _f_usdcny, ('usdcny',)),
    ('美股指数', _f_us_idx, ('ndx', 'us_dji', 'us_ixic')),
    ('美联储利率', _f_fed, ('us_fed_up', 'us_effr')),
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

    # v2.389: keys 给定时只跑「产出的 key 与它相交」的源 —— 抓两个新源 6 秒就够,
    #   不必为了它们把最慢的费半(40s)也跑一遍。keys=None = 全刷(默认)。
    _want = set(keys) if keys else None

    # ⚠ 必须串行: akshare(py_mini_racer) 并发调用会让整个进程段错误, 见 _ak() 注释
    for name, fn, oks in FETCHERS:
        if _want is not None and not (_want & set(oks)):
            continue
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

    # 组合指标（定点刷新时如果两个输入都没重新抓, 就从本地读出来重算, 保证不丢）
    for key, ka, kb in COMBOS:
        if ka in got and kb in got:
            got[key] = _diff_series(got[ka], got[kb])
        elif got:
            a, b = _csv_read(_path(ka)), _csv_read(_path(kb))
            if a and b:
                got[key] = _diff_series(a, b)

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
        # v2.389: 叠加走势线里的道指/纳指也归 _f_us_idx 抓, 但它们不是"指标卡",
        #   上面的 INDICATORS 循环不会写它们 ⇒ 这里补一遍, 否则永远是空的。
        for b in BENCHES:
            key = b.get('mac')
            if not key or key not in got:
                continue
            rows = _clean(got[key])
            if not rows:
                continue
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


def _idx_close(rel, n=DAY_CAP):
    """读本地指数日线 CSV 的收盘价 → [[date, close]]（升序, 取最后 n 根）"""
    rows = []
    p = os.path.join(BASE_DIR, rel)
    try:
        with io.open(p, 'r', encoding='utf-8', newline='') as fh:
            head = fh.readline()
            cols = [c.strip() for c in head.split(',')]
            ci = cols.index('close') if 'close' in cols else 2
            for ln in fh:
                ln = ln.strip()
                if not ln:
                    continue
                pp = ln.split(',')
                if len(pp) <= ci:
                    continue
                f = _num(pp[ci])
                dt = _d(pp[0])
                if dt and f is not None:
                    rows.append([dt, round(f, 2)])
    except Exception:
        return []
    return rows[-n:]


def _bench_series():
    """可叠加的 6 条走势线（本地优先, 缺的给空 series 让前端自己跳过）"""
    out = []
    for b in BENCHES:
        if b.get('mac'):
            rows = [[dt, round(v, 2)] for dt, v in _csv_read(_path(b['mac']))][-DAY_CAP:]
        else:
            rows = _idx_close(b.get('rel') or '')
        out.append({'key': b['key'], 'name': b['name'], 'src': b['src'],
                    'n': len(rows), 'last_date': (rows[-1][0] if rows else ''),
                    'series': rows})
    return out


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
        chg_date = last_date
        # v2.389: 政策利率(美联储目标利率 / LPR)是台阶, 一天一天看几乎永远是 0。
        #   改成「上一次真正变动」的口径(变动量 + 变动生效日), 这才是有意义的读数。
        if d.get('step') and len(rows) > 1:
            for k in range(len(rows) - 2, -1, -1):
                if rows[k][1] != last_v:
                    chg = last_v - rows[k][1]
                    chg_date = rows[k + 1][0]
                    break
            else:
                chg, chg_date = 0.0, rows[0][0]
        items.append(dict(d, n=len(rows), last_date=last_date,
                          stale_days=_days_since(last_date),
                          value=round(last_v, 6), prev=prev_v,
                          chg=(round(chg, 6) if chg is not None else None),
                          chg_date=chg_date,
                          series=[[dt, round(v, 6)] for dt, v in rows]))
    return {'ok': True, 'schema': SCHEMA, 'fetched_at': meta.get('fetched_at', ''),
            'groups': GROUPS, 'items': items,
            'benches': _bench_series(),      # v2.389: 6 条可叠加走势线
            'hs300': _hs300()}               # 保留旧字段, 老前端不会炸


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
