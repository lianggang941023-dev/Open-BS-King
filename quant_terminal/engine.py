"""
BS King Quant Terminal v2.1 — 三周期左峰突破框架
========================================================
M1 100日突破 (框架1·长期)  → 利弗莫尔方向判断
M2 55日突破  (框架7·中期)  → 底部反转/趋势确认
M3 20日突破  (框架8·短期)  → V型反转/入场时机

横切验证层 M4: A股适配 (板块共振/BIAS乖离/涨跌停/反转效应)
横切执行层:     海龟仓位 (ATR止损/单位仓位/金字塔加仓)

共识: 触发维度数 × M4通过数 → 共振越强信号越可靠

用法:
  from quant_terminal.engine import QuantEngine
  engine = QuantEngine()
  results = engine.scan()
"""

import os, sys, csv, time, json
import threading
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Tuple, Optional

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)
# v1 数据源设置 (2026-08-29): 本地路径从 data_sources.json 读取, 即时生效
from quant_terminal import datasource as ds
# v2.62 (2026-09-10): 两市成交额分钟落盘 + 全天成交预测 (纯读盘; 拉取由 server 侧 turnover.ensure 节流触发)
from quant_terminal import mcap as _mcap

# v2.279 (用户 m10425): 统一前缀拼法。
#   全仓只有 mcap.prefixed 能正确判北交所 (920xxx / 4 / 8 开头), 它返回 '大写前缀+6位'。
#   老判据 `9 开头 = 沪市` 会把 920xxx 拼成 SH920157 / sh920157,
#   而本地 K 线文件叫 BZ920157.csv —— 北交所票因此整片被跳过。
def _px(code):
    """→ SH600519 / SZ000001 / BZ920157 (mcap.prefixed 分市口径)"""
    return _mcap.prefixed(_mcap.bare(str(code or '').strip()))


def _sc(code):
    """→ sh600519 / sz000001 / bj920157 (新浪腾讯写法)"""
    _p = _px(code)
    return ('bj' if _p[:2] == 'BZ' else _p[:2].lower()) + _p[2:]


from quant_terminal import turnover
# v2.152 (2026-10-18): 盘中/盘后临时快照覆盖层 (通达信自动任务 11:35 / 15:35 拉的全市场
#   当日 OHLCV)。11:35 那次不写 CSV, 靠这层让「更新板块」用上最新数据。
#   模块内部按 mtime+size 缓存, 逐只调用也不会反复读盘。
from quant_terminal import intraday as _intraday
# v2.360 (2026-11-21 用户 m38439): 指数登记表 —— 全仓唯一一份指数代码表。
#   在这之前 INDEX_DAILY_CODES / INDEX_MAP / fetch() 的 codes 三处各写一份七码,
#   用户要求「顶栏两格可右键换指数」后再加就是四处, 改一个漏一个会静默出错。
from quant_terminal import idx_registry as _idxr
KLINE_DIR = ds.resolve(ds.get('local', 'kline_dir'))
POOL_DIR = ds.resolve(ds.get('local', 'snapshot_dir', '选股池'))


def refresh_dirs():
    """数据源设置保存后调用: 重新解析本地路径 (KLINE_DIR/POOL_DIR 即时生效)"""
    global KLINE_DIR, POOL_DIR, INDEX_DAILY_DIR
    KLINE_DIR = ds.resolve(ds.get('local', 'kline_dir'))
    POOL_DIR = ds.resolve(ds.get('local', 'snapshot_dir', '选股池'))
    INDEX_DAILY_DIR = os.path.join(os.path.dirname(KLINE_DIR), 'index_daily')


# ============================================================
# 指数日K本地化 (v2.57 2026-09-05): 七大指数日K落盘 kline_data/index_daily/
# 与股票CSV同构6列 (date,open,close,high,low,volume) utf-8无BOM, 单位=手。
# 数据源: 除 sh000680 (腾讯仅2025起, 走新浪) 外全部腾讯 fqkline (单次上限2000根≈2018-06起)。
# 职责: 初次全量回补 / 每日收盘后 guard 追加 / 读路径本地优先(盘中仅取当天live merge, 绝不写盘)
# ============================================================
# v2.360 (用户 m38512): 不再写死七码, 改为登记表全集 —— 用户明确要求「拉取指数的时候
#   也要增加这些指数, 不管它是否在主页显示」, 即实时刷新/盘后存档与其它指数同节奏。
INDEX_DAILY_CODES = tuple(_idxr.BAR_CODES)
INDEX_DAILY_SINA = frozenset({'sh000680'})          # 腾讯无全史 → 新浪 CN_MarketDataService
INDEX_DAILY_DIR = os.path.join(os.path.dirname(KLINE_DIR), 'index_daily')
_IDX_RLOCK = threading.RLock()                       # 读写/ensure/sync 共用 — Windows os.replace 原子性保障
_SINA_DAILY_URL = ('https://quotes.sina.cn/cn/api/json_v2.php/'
                   'CN_MarketDataService.getKLineData?symbol={code}&scale=240&ma=no&datalen={n}')


def _idx_fpath(code: str) -> str:
    return os.path.join(INDEX_DAILY_DIR, code.upper() + '.csv')


def _fmt_px(v: float) -> str:
    return f'{round(float(v), 3):.3f}'


def _fmt_vol(v: float) -> str:
    return str(int(round(float(v))))


def _norm_tencent_bars(j, sc: str) -> List[Dict]:
    """腾讯 fqkline response → [{date,open,close,high,low,volume}] (列序 b=[日期,O,C,H,L,量手])
    v2.57.3: j=None(域名全失败)防御 — 防 guard/sync 线程抛异常死亡"""
    if not j:
        return []
    d = (j.get('data') or {}).get(sc, {})
    bars = d.get('day') or d.get('qfqday') or []
    out = []
    for b in bars:
        try:
            out.append({'date': b[0], 'open': float(b[1]), 'close': float(b[2]),
                        'high': float(b[3]), 'low': float(b[4]),
                        'volume': float(b[5]) if len(b) > 5 else 0})
        except (ValueError, IndexError):
            continue
    out.sort(key=lambda x: x['date'])
    return out


def fetch_sina_daily(code: str, datalen: int = 1500) -> Optional[List[Dict]]:
    """新浪日K (仅 sh000680 全史源; datalen 上限1500=该指数全量). volume 股→手÷100"""
    try:
        resp = _req_get(_SINA_DAILY_URL.format(code=code, n=datalen), timeout=10)
        if resp is None or resp.status_code != 200:
            return None
        arr = resp.json()
        if not isinstance(arr, list) or not arr:
            return None
        out = []
        for r in arr:
            try:
                out.append({'date': str(r['day']), 'open': float(r['open']),
                            'close': float(r['close']), 'high': float(r['high']),
                            'low': float(r['low']),
                            'volume': int(round(float(r.get('volume', 0)) / 100))})  # 股→手
            except (ValueError, KeyError, TypeError):
                continue
        out.sort(key=lambda x: x['date'])
        return out or None
    except Exception:
        return None


def _fetch_index_full(code: str) -> Optional[List[Dict]]:
    """全量回补。三条通路, 由 idx_registry 的 src 决定(别按代码号猜):
      · src='ths'     → 同花顺网页接口(48_ 前缀)。**同花顺全A 883957 只有这一条**, 腾讯/新浪/东财全空。
      · 680            → 新浪 1500(腾讯只有 2025 起)
      · 其余 tencent   → 腾讯 2000 根(2018-06 起, 单次硬上限)
    v2.360 (用户 m38439): 加 ths 分支 —— 否则 883957 永远 bars=0。"""
    if _idxr.src_of(code) == 'ths':
        from quant_terminal import ths_index as _ths_i
        return _ths_i.full_bars(code) or None
    if code in INDEX_DAILY_SINA:
        return fetch_sina_daily(code, 1500)
    return _norm_tencent_bars(_fqkline_req(code, count='2000'), code) or None


def _fetch_index_tail(code: str, n: int = 10) -> Optional[List[Dict]]:
    """最近 n 根 (guard 每日增量比对用; 节假日返回旧bar, 由调用方比对判定)
    v2.360: ths 通路一次给全量, 切片即可(不额外请求第二个接口)。"""
    if _idxr.src_of(code) == 'ths':
        from quant_terminal import ths_index as _ths_i
        return _ths_i.tail_bars(code, n) or None
    if code in INDEX_DAILY_SINA:
        return fetch_sina_daily(code, n)
    return _norm_tencent_bars(_fqkline_req(code, count=str(n)), code) or None


def _is_settled() -> bool:
    """当日收盘已确认 (工作日过 15:06, 或周末) — 之后当日 bar 才允许落盘"""
    now = datetime.now()
    if now.weekday() >= 5:
        return True
    return now.hour * 100 + now.minute > 1506


def _drop_forming_today(rows: List[Dict]) -> List[Dict]:
    """当日 forming bar 一律不落盘, 直到收盘确认 _is_settled() 后才放行
    v2.57.3 (2026-09-07): 过滤窗原为 9:30-15:06 → 9:30 前漏网致事故:
    终端 09:26 开机 guard 首 sync 时腾讯已生成当日占位bar(四价全等+微成交),
    原窗 9:30 前不滤 → 占位行永久入库; 15:05 后 sync 见日期已同 → noop 滞留。
    现改: 收盘确认(15:06)前 9:00-15:06 全覆盖滤当日。"""
    if _is_settled():
        return rows
    today = datetime.now().strftime('%Y-%m-%d')
    return [r for r in rows if r['date'] != today]


def read_index_daily(code: str, n: Optional[int] = None) -> Optional[List[Dict]]:
    """读本地指数日K (持锁; 坏行容错滤; 有效<1行视为损坏→None)"""
    with _IDX_RLOCK:
        fpath = _idx_fpath(code)
        if not os.path.exists(fpath):
            return None
        rows = []
        try:
            with open(fpath, 'r', encoding='utf-8') as f:
                for r in csv.DictReader(f):
                    dt = r.get('date', '')
                    if not dt:
                        continue
                    try:
                        rows.append({'date': dt, 'open': float(r['open']),
                                     'close': float(r['close']), 'high': float(r['high']),
                                     'low': float(r['low']), 'volume': float(r.get('volume', 0))})
                    except (ValueError, KeyError):
                        continue
        except Exception:
            return None
        if not rows:
            return None
        rows.sort(key=lambda x: x['date'])
        return rows if n is None else rows[-n:]


# ── v2.128: K线新鲜度判据 —— 单位是**交易日**，不是日历日 ─────────────
# 🔴 病灶: 本文件与 breakout.py 原有 5 处独立的 `最新K线距今天数 > 3` 判停牌/滞后，数的是**日历日**。
#   09-25 中秋休市 + 周末 ⇒ 09-28(周一) 距 09-24 收盘差 4 个日历日、却只差 **0 个交易日** ⇒
#   5557 只票全被剔除 ⇒ 行业/概念面板 + 板块下钻 + 突破扫描的板块验证**同时全空**，且不报任何错。
#   而状态接口当天已改成数交易日 ⇒ 出现"报告说 0 天滞后、面板却空"的自相矛盾。**一个判据只能有一份实现。**
# ⚠️ fail-open: 日历读不到时**一律放行**。错杀是静默的整片空白；放行最多混进 ~20 只停牌股。
_CAL_CACHE = {'key': None, 'cal': []}


def _closed_trade_days() -> List[str]:
    """已收盘确认的交易日历 (升序 'YYYY-MM-DD')。读不到 → []

    与 /api/kline/status 同源 (同一份 index_daily) ⇒ 报告者与消费者口径物理同源。
    进程内按 (今天, 是否已收盘) 缓存 —— 一天最多读两次盘。
    ⚠️ 必须带缓存: 调用方在**逐板块/逐股循环**里问, 每次读盘会是 2014 行 × N 次。
    这份日历由服务端自己的 _index_guard_loop 写, **独立于用户手动导出的 kline_data/daily**
    (实测 mtime 15:10 vs 15:46 两条链路) ⇒ "忘了导出"时它照样准, 判定不会成环。"""
    global _CAL_CACHE
    today = datetime.now().strftime('%Y-%m-%d')
    key = (today, _is_settled())
    if _CAL_CACHE['key'] != key:
        cal = [r['date'] for r in (read_index_daily('sh000001') or [])]
        if cal and cal[-1] == today and not key[1]:
            cal = cal[:-1]      # 防御: 收盘确认前日历里不该有当日 forming bar
        _CAL_CACHE = {'key': key, 'cal': cal}
    return _CAL_CACHE['cal']


def trade_days_behind(last_date: str, as_of: Optional[str] = None) -> Optional[int]:
    """last_date 落后基准几个**交易日**。

    基准 = 日历里最后一个已收盘交易日 (给了 as_of 就截到 as_of, 供 v2.61 回填口径)。
    返回 None = 日历不可用 ⇒ **判不了** (调用方一律放行);
    返回 -1   = 日期不可解析 (原先靠 try/except ValueError 剔除, 现在把这个信号显式带出来)。
    ⚠️ 与 server 原式 `sum(1 for d in cal if d > latest)` **等价**: cal 升序时大于 latest 的
       元素恰是它的后缀, 从尾部倒着数到第一个 <= last_date 即可 (新鲜的只数 1 次)。"""
    try:
        datetime.strptime(last_date, '%Y-%m-%d')
        if as_of:
            datetime.strptime(as_of, '%Y-%m-%d')
    except (ValueError, TypeError):
        return -1
    cal = _closed_trade_days()
    if as_of:
        cal = [d for d in cal if d <= as_of]
    if not cal:
        return None
    n = 0
    for d in reversed(cal):
        if d > last_date:
            n += 1
        else:
            break
    return n


def with_intraday(tail: List[Dict], code: str, as_of: Optional[str] = None) -> List[Dict]:
    """v2.152: 把「临时快照」叠到某只股票的日K尾部 (不改入参)

    只在「最新口径」用 (as_of 为空)。回填口径 (as_of 有值) 一律不叠 ——
    回填是「按历史某天算」, 把今天的快照叠进去等于穿越。
    快照模块内部已处理三种情形 (追加/替换/过期不动) 与除权因子缩放。
    """
    if as_of:
        return tail
    try:
        return _intraday.apply(tail, code)
    except Exception:
        return tail          # 快照坏了绝不能把整块板块计算拖垮


def kline_is_fresh(last_date: str, as_of: Optional[str] = None, tol: int = 3) -> bool:
    """停牌/滞后过滤 —— 原 `(now - last_date).days > 3` 的**交易日**版。

    tol=3 与原文常量相同, 只把单位从日历日换成交易日 ⇒ **不会比旧行为更严**
      (旧行为在周一实际只容忍 0 个交易日滞后、周三容忍 1 个 —— 随星期几漂移, 是病灶的一部分)。
    🔴 日历不可用 ⇒ 恒 True。**别改成 False**: 那正是 v2.128 要修的"静默全空"。"""
    n = trade_days_behind(last_date, as_of)
    if n is None:
        return True
    if n < 0:
        return False
    return n <= tol

def write_index_daily(code: str, rows: List[Dict]) -> bool:
    """原子落盘 (tmp+os.replace; 持锁; 与读互斥防 Windows 句柄竞争)"""
    with _IDX_RLOCK:
        try:
            os.makedirs(INDEX_DAILY_DIR, exist_ok=True)
            tmp = _idx_fpath(code) + '.tmp'
            with open(tmp, 'w', encoding='utf-8', newline='') as f:
                f.write('date,open,close,high,low,volume\n')
                for r in rows:
                    f.write(','.join([r['date'], _fmt_px(r['open']), _fmt_px(r['close']),
                                      _fmt_px(r['high']), _fmt_px(r['low']),
                                      _fmt_vol(r['volume'])]) + '\n')
            os.replace(tmp, _idx_fpath(code))
            return True
        except Exception as e:
            print(f'[index_daily] 写 {code} 失败: {e!r}', file=sys.stderr)
            return False


def _live_index_bar(code: str) -> Optional[Dict]:
    """盘中当日 live bar (仅 _is_trade_window 真时调用): 腾讯fqkline末5根当日行为主
    (同源同单位手, 与历史同构); 失败→新浪实时兜底 (fields[8]量股÷100→手)
    v2.360 (用户 m38439): src='ths' 的指数(同花顺全A 883957)走同花顺分时 —— 腾讯/新浪都没有它。
    """
    if not _is_trade_window():
        return None
    if _idxr.src_of(code) == 'ths':
        from quant_terminal import ths_index as _ths_i
        return _ths_i.live_bar(code)
    today = datetime.now().strftime('%Y-%m-%d')
    try:
        j = _fqkline_req(code, count='5')
        rows = _norm_tencent_bars(j, code) if j else []
        if rows and rows[-1]['date'] == today:
            return rows[-1]
    except Exception:
        pass
    try:
        q = ds.fetch_realtime([code])
        v = q.get(code) or {}
        px = float(v.get('price', 0) or 0)
        if px <= 0:
            return None
        return {'date': today, 'open': float(v.get('open', 0) or 0), 'close': px,
                'high': float(v.get('high', 0) or px), 'low': float(v.get('low', 0) or px),
                'volume': float(v.get('volume', 0) or 0) / 100}  # 实时源volume=股
    except Exception:
        return None


def ensure_index_daily(code: str, need: int = 0) -> bool:
    """文件缺失/损坏/有效行不足 need → 全量回补落盘 (持锁跨网; 盘中建库滤当日 forming bar)
    返回是否本地文件已就绪"""
    with _IDX_RLOCK:
        cur = read_index_daily(code)  # reentrant RLock, 读=全量
        if cur is not None and (need <= 0 or len(cur) >= need):
            return True
        rows = _fetch_index_full(code)
        if not rows:
            return False
        rows = _drop_forming_today(rows)
        if not rows:
            return False
        ok = write_index_daily(code, rows)
        if not ok:
            return False
        return read_index_daily(code) is not None


def sync_index_daily(code: str, force: bool = False) -> str:
    """与远端比对补齐本地 (guard/CLI/手动端点共用). 返回:
    missing-rebuilt | appended:N | noop | gap-rebuilt | fail | trading-skip"""
    if _is_trade_window() and not force:
        return 'trading-skip'
    with _IDX_RLOCK:
        local = read_index_daily(code)
        if local is None:
            return 'missing-rebuilt' if ensure_index_daily(code) else 'fail'
        tail = _fetch_index_tail(code, 10)
        if not tail:
            return 'fail'
        local_last = local[-1]['date']
        if tail[-1]['date'] <= local_last:
            # v2.57.3 占位bar自愈: 盘前占位bar(四价全等+量<昨10%)曾滞留末日行,
            # 收盘确认后检测到该特征且远端当日行正常 → 覆盖末行修正
            if _is_settled() and local_last == datetime.now().strftime('%Y-%m-%d'):
                last = local[-1]
                if (last['open'] == last['close'] == last['high'] == last['low']
                        and last['volume'] > 0 and len(local) >= 2
                        and last['volume'] < local[-2]['volume'] * 0.10
                        and tail[-1]['date'] == local_last
                        and tail[-1]['volume'] > 0
                        and not (tail[-1]['open'] == tail[-1]['close']
                                 == tail[-1]['high'] == tail[-1]['low'])):
                    if write_index_daily(code, local[:-1] + [tail[-1]]):
                        return 'placeholder-fixed'
            return 'noop'
        seg = [r for r in tail if r['date'] > local_last]
        seg = _drop_forming_today(seg)  # settle宽限内不当日写入, 下轮tick重试
        if seg:
            if tail[0]['date'] > local_last:  # 缺口>tail窗(≈10交易日) → 全量重建
                full = _fetch_index_full(code)
                if not full:
                    return 'fail'
                if write_index_daily(code, _drop_forming_today(full)):
                    return 'gap-rebuilt'
                return 'fail'
            if write_index_daily(code, local + seg):
                return f'appended:{len(seg)}'
            return 'fail'
        return 'noop'


def get_index_bars(code: str, need: Optional[int] = None,
                   with_live: bool = False) -> List[Dict]:
    """统一读入口 (指数K线展示/RSRS/趋势判定三消费方):
    本地优先 → 缺失/不足 need 自动全量回补 → 盘中 with_live 当日live bar内存替换 (绝不写盘)"""
    full = read_index_daily(code)
    if full is None or (need and len(full) < need):
        if not ensure_index_daily(code, need or 0):
            full = read_index_daily(code)
        else:
            full = read_index_daily(code)
    if not full:
        return []
    rows = full if not need else full[-need:]
    if with_live and _is_trade_window():
        lb = _live_index_bar(code)
        if lb:
            if rows[-1]['date'] == lb['date']:
                rows[-1] = lb
            elif lb['date'] > rows[-1]['date']:
                rows.append(lb)
    return rows


# ============================================================
# 工具函数
# ============================================================

# 均线档位映射 (2026-08-05): 6线全多头 / 5线 / 4线 / 3线
# 行情越好用越严格的档位; 用户手动切换, 不做RSRS自动推荐
_SIDX_ALIGN_CACHE: Dict = {}


def _index_live_window():
    """板块指数该不该叠加「当天快照那一根」= 交易日 9:30~15:00（**含午休**）

    v2.180 (2026-10-22 用户定案): 与 _is_trade_window() 只差午休那 90 分钟 ——
    趋势面板午休 11:40 也要跑一轮，那会儿官方指数日线还停在昨天，不加这一根
    「偏多/偏空」就落后半天。周末/节假日照旧排除。
    """
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    try:
        if ds.is_holiday():
            return False
    except Exception:
        pass
    t = now.hour * 100 + now.minute
    return 930 <= t <= 1500


def official_index_aligned(ma_periods):
    """板块名 → **官方板块指数**是否多头排列（同花顺板块指数日线本地库，5 分钟缓存）

    为什么需要它（v2.173, 2026-10-22 用户定案）
    ------------------------------------------
    终端原来判定板块档位用的是「成分股涨幅等权合成的指数」再算均线，属于近似。
    同花顺自己有官方板块指数日线（本地库 quant_terminal/sector_kline.py 已建 710 个 ×
    5 年），用它判定更准。用户口径：**指数多头排列 ⇒ 至少偏多，再看成分股多头占比定强弱**。

    实测对照（2026-10-21 收盘，4 线档）：细分行业 257 个里 254 个一致、3 个改善
    （出版/股份制银行/电信运营商：合成口径误判偏空，官方指数其实多头排列）；
    上级行业 90/90、概念 346/350 完全一致。

    取不到（本地库没建 / 企稳失败）→ 返回空 dict，调用方自动回退到原来的合成口径。
    """
    import time as _t
    key = tuple(int(p) for p in ma_periods)
    # v2.180: 盘中/午休把当天指数快照当临时一根参与均线（只算不落盘）
    ov, ovd = {}, ''
    if _index_live_window():
        try:
            from quant_terminal import sector_kline as _sk0
            ov = _sk0.live_index_overlay() or {}
            ovd = datetime.now().strftime('%Y-%m-%d')
        except Exception:
            ov, ovd = {}, ''
    ck = (key, bool(ov))          # 叠加/不叠加分开缓存, 收盘后不会误用盘中那份
    ent = _SIDX_ALIGN_CACHE.get(ck)
    now = _t.time()
    if ent and (now - ent[0]) < 300:
        return ent[1]
    try:
        from quant_terminal import sector_kline as _sk
        sc = _sk.scan_all(periods=key, overlay=(ov or None), overlay_date=ovd)
        d = {r['name']: bool(r['aligned']) for r in (sc.get('rows') or []) if r.get('name')}
    except Exception:
        d = {}
    _SIDX_ALIGN_CACHE[ck] = (now, d)
    return d


LINES_MAP = {
    '3': (5, 10, 21),
    '4': (5, 10, 21, 55),
    '5': (5, 10, 21, 55, 120),
    '6': (5, 10, 21, 55, 120, 250),
}

# v2.97 (2026-09-16): 板块下钻**名单**口径 —— 现价站上这条均线即入选, 均线彼此**不要求有序**。
#   与面板的 占比/状态(bullish / bullish_pct / status) 彻底解耦: 面板口径一字未动。
DRILL_MA = 5
DRILL_MA_COLS = (5, 10, 21, 55, 120, 250)   # 下钻「均线」列的显示顺序

def _req_get(url: str, timeout: int = 8, headers: Optional[dict] = None):
    """HTTP GET —— **先直连, 失败再走系统代理** (v2.234 2026-10-27 用户 m10187 定案)

    历史: v2.97 之前是「先走代理、失败再直连」, 那是假设用户没开代理时 requests 会自己
      直连。但本机常年挂 SSR(127.0.0.1:1080), 实测同一接口走代理比直连慢 4~11 倍
      (同花顺 1491ms→149ms / 腾讯 qt 中位 1042ms→92ms), 代理链路还更容易被掐断。
      所以本函数反过来: **先绕代理直连, 直连失败才退回系统代理**。
    用到的全是境内源(新浪/腾讯/东财), 直连只会更快; 退回代理是给"直连被挡"留的退路。
    headers: 透传给两次请求(如新浪必带 Referer, 否则403)
    """
    import requests
    try:
        return requests.get(url, timeout=timeout, headers=headers,
                            proxies={'http': None, 'https': None})
    except Exception:
        return requests.get(url, timeout=timeout, headers=headers)


# ---- 腾讯 fqkline 域名自动切换 (2026-09-01) ----
# 腾讯风控在 ifzq.gtimg.cn(主域)/web.ifzq.gtimg.cn(子域) 间来回轮换(501):
# 8/31 主域可用→子域被501; 9/1 主域被501→子域可用。成功一次记住当前域名,
# 连续失败自动切下一个 (web子域 → 主域 → proxy代理), 全失败返回 None。
FQKLINE_HOSTS = ['web.ifzq.gtimg.cn', 'ifzq.gtimg.cn', 'proxy.finance.qq.com']
# v2.190: 🔴 腾讯把 `/appstock/app/fqkline/get` 这条 **path** 单独挡了 —— http 先 302 跳 https
#   （拿到的是 HTML），https 直接 501 跳 waf.tencent.com/501page.html。同域其他接口全 200
#   （qt.gtimg.cn 实时 / ifzq.../board/index 板块）⇒ **不是 IP 封**，是这条 path 被下架或加风控。
#   原实现只轮换**域名**、不换 path ⇒ 三个域名全撞 501、0.00 秒返回 None，连带在线 K 线补拉 /
#   指数 K 线 / RSRS / 盘中 live bar 一起失效。newfqkline 同域同结构（个股返 qfqday / 指数返 day /
#   分段 end= 也支持）⇒ 排前面优先；老 fqkline 留作兜底，腾讯哪天调回去自动切过来，不必改代码。
#   ⚠ 解析侧一行没动（day / qfqday 两种键都吃）；⚠ proxy.finance.qq.com 拼的是 /appstock/ 一直 404，
#     它从来只是理论兜底，本次不动。
FQKLINE_PATHS = ['newfqkline', 'fqkline']
_fqkline_cur = [0]  # 当前可用域名索引 (模块级, 进程内记忆)
_fqkline_pat = [0]  # 当前可用路径索引 (同上)


def _fqkline_req(code: str, count: str = '60', end: str = ''):
    """腾讯 K线请求 —— **路径 × 域名** 双维自动切换. code='sh000001'/'sh600053'.
    先试上次成功的 (path, host) 组合, 再按 FQKLINE_PATHS / FQKLINE_HOSTS 顺序展开。
    end 非空 → param={code},day,1990-01-01,{end},{count},qfq (分段拉取风格).
    成功(HTTP200+有数据)返回 response json, 并记住当前组合; 全失败返回 None."""
    p0 = _fqkline_pat[0]
    for path in FQKLINE_PATHS[p0:] + FQKLINE_PATHS[:p0]:
        h0 = _fqkline_cur[0]
        for host in FQKLINE_HOSTS[h0:] + FQKLINE_HOSTS[:h0]:
            if end:
                url = (f'https://{host}/appstock/app/{path}/get'
                       f'?param={code},day,1990-01-01,{end},{count},qfq')
            else:
                url = f'https://{host}/appstock/app/{path}/get?param={code},day,,,{count},qfq'
            try:
                resp = _req_get(url, timeout=10, headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                                  'Chrome/120.0 Safari/537.36',
                    'Referer': 'https://gu.qq.com/'})
                if resp.status_code != 200:
                    continue  # 501/403/5xx → 切下一个
                j = resp.json()
                d = (j.get('data') or {}).get(code)
                if d and (d.get('qfqday') or d.get('day')):
                    _fqkline_pat[0] = FQKLINE_PATHS.index(path)  # 记住当前可用组合
                    _fqkline_cur[0] = FQKLINE_HOSTS.index(host)
                    return j
            except Exception:
                continue
    return None


def load_csv_kline(code: str) -> List[Dict]:
    """加载个股日K线CSV

    v2.279 (2026-11-12 用户 m10425): 北交所补进前缀候选。
      老逻辑只认 `SZ`/`SH` 两个前缀, 而本地北交所文件叫 `BZ920157.csv` ——
      920xxx 首位是 9, 被当成沪市 B 股拼成 `SH920157.csv`, 永远找不到 ⇒ 北交所点进去没有 K 线。
      现在: ①`BZ`/`BJ` 8 位码原样认; ②裸 920xxx / 4xxxxx / 8xxxxx 一律先试 `BZ`;
      ③其余仍按老顺序 (精确前缀 → SZ → SH), 保证既有票行为一字不变。
    """
    c = (code or '').strip().upper()
    if len(c) > 2 and c[:2] in ('SZ', 'SH', 'BZ', 'BJ') and c[2:].isdigit():
        num, exact = c[2:], ('BZ' if c[:2] == 'BJ' else c[:2])
    else:
        num, exact = c, None
    prefixes = [exact] if exact else []
    # 北交所号段: 920xxx 是新号段, 4/8 开头是老三板/北交老号段 —— 本地一律存成 BZ
    if exact is None and (num.startswith('920') or num[:1] in ('4', '8')):
        prefixes.append('BZ')
    prefixes += [p for p in ['SZ', 'SH'] if p not in prefixes]
    fpath = None
    for prefix in prefixes:
        fp = os.path.join(KLINE_DIR, f'{prefix}{num}.csv')
        if os.path.exists(fp): fpath = fp; break
    if not fpath: return []

    rows = []
    try:
        with open(fpath, 'r', encoding='utf-8') as f:
            for r in csv.DictReader(f):
                dt = r.get('date', r.get('trade_date', ''))
                if not dt: continue
                try:
                    rows.append({'date': dt, 'open': float(r['open']),
                        'close': float(r['close']), 'high': float(r['high']),
                        'low': float(r['low']), 'volume': float(r.get('volume', 0))})
                except (ValueError, KeyError): continue
    except Exception: return []
    rows.sort(key=lambda x: x['date'])
    return rows


def calc_vol_price_sync(kline: List[Dict], window: int = 50) -> Optional[float]:
    """量价同步率 (2026-09-03 用户定案) — 最近 window 个有效同步对中满足同步的占比%

    每对 = 今 vs 昨, 同步判定:
      收涨(今收>昨收) + 量增      → 同步
      收跌(今收<昨收) + 量缩      → 同步
      收平(复权价差 < 半档0.005) + 量增减≤10% → 同步 (近似平量)
      其余(涨缩量/跌放量/平收放量) → 不同步
    说明:
      - 收平用 <0.005 判 (前复权3位小数, 真实平收有因子噪声, 不能浮点等值)
      - 量=0 的对(停牌/历史事故行)无法判增减 → 跳过并顺延, 防除零
      - 样本不足 window 按实际对数折算 (用户定案: 次新股几乎不会被选中, 有就折算)
      - 有效对 < 10 → None (样本太少不可信, 前端显示 '-')
    """
    closes = [r['close'] for r in kline]
    vols = [r['volume'] for r in kline]
    pairs = []
    i = len(closes) - 2
    while i >= 0 and len(pairs) < window:
        vp, vc = vols[i], vols[i + 1]
        if vp > 0 and vc > 0:
            pairs.append((closes[i], closes[i + 1], vp, vc))
        i -= 1
    n = len(pairs)
    if n < 10:
        return None
    sync = 0
    for pp, pc, vp, vc in pairs:  # pp=昨收 pc=今收
        if pc > pp and vc > vp:
            sync += 1
        elif pc < pp and vc < vp:
            sync += 1
        elif abs(pc - pp) < 0.005 and abs(vc - vp) / vp <= 0.10:
            sync += 1
    return round(sync / n * 100, 0)


def calc_entity_quality(kline: List[Dict], window: int = 50) -> Optional[float]:
    """实体量质 (v2.37 2026-09-03 用户定案) — 量能应配合K线实体有效推进, 0-100 分

    每对 = 今 vs 昨, 输入: 实体 e=(今收-今开)/昨收, 量比 q=今量/昨量-1
    量档: |q|≤10% 平量 | q>+10% 放量 | q<-10% 缩量 (边界含10%属平量, 与同步率一致)
    实体档 (十字=绝对口径, 用户定案): 大阳 e≥+1.5% | 小阳 [0.5,1.5)% | 十字 |e|<0.5%
                                          | 小阴 (-1.5,-0.5]% | 大阴 e≤-1.5%
    打分矩阵 (每对 0.2~2.0, 满分 2.0 同同步率每对2%刻度):
        大阳: 缩1.0(惜售) 平1.5 放2.0(推进)    小阳: 缩0.8 平1.2 放1.5
        十字: 缩1.2(静默) 平1.0 放0.3(放量推不动=分歧/板上换手)
        小阴: 缩1.2 平0.6 放0.2(放量跌)        大阴: 缩1.5(洗盘) 平0.5 放0.2(出货)
    设计要点 (用户裁决):
      - 实体方向用收vs开: 高开低走(相对昨收仍涨但收<开)按阴线处理 → 放量=出货嫌疑, 不会被误判成健康涨日
      - 一字板 C=O 实体=0 自动落十字档, 不特判: 缩量一字(买不到)=十字缩量分; 放量一字(T字/开板回封)=0.3 板上分歧
      - 跌停侧不设计: 池子全在多头趋势中, 跌停票进不了结果表 (兜底=矩阵阴线放量 0.2)
      - 天量不单独降温 (用户: 高位天量坏/低位天量好, 位置判断他自己看K线图)
      - 量=0 的对(停牌/事故行)跳过顺延; 有效对<10 → None; 不足 window 按实际对数折算
      - 纯展示列, 不参与评分
    """
    opens = [r['open'] for r in kline]
    closes = [r['close'] for r in kline]
    vols = [r['volume'] for r in kline]
    pairs = []
    i = len(closes) - 2
    while i >= 0 and len(pairs) < window:
        vp, vc = vols[i], vols[i + 1]
        if vp > 0 and vc > 0:
            pairs.append((opens[i], closes[i], opens[i + 1], closes[i + 1], vp, vc))
        i -= 1
    n = len(pairs)
    if n < 10:
        return None
    total = 0.0
    for _, pp, oc, pc, vp, vc in pairs:  # pp=昨收 oc=今开 pc=今收
        e = (pc - oc) / pp
        q = vc / vp - 1.0
        if q >= 0.10:      vk = 'v'   # 放量
        elif q <= -0.10:   vk = 's'   # 缩量
        else:              vk = 'f'   # 平量
        if e >= 0.015:      ek = 'BIGUP'
        elif e >= 0.005:    ek = 'UP'
        elif e > -0.005:    ek = 'X'      # 十字 |e|<0.5%
        elif e > -0.015:    ek = 'DN'
        else:               ek = 'BIGDN'
        total += {
            'BIGUP': {'v': 2.0, 'f': 1.5, 's': 1.0},
            'UP':    {'v': 1.5, 'f': 1.2, 's': 0.8},
            'X':     {'v': 0.3, 'f': 1.0, 's': 1.2},
            'DN':    {'v': 0.2, 'f': 0.6, 's': 1.2},
            'BIGDN': {'v': 0.2, 'f': 0.5, 's': 1.5},
        }[ek][vk]
    return round(total / n / 2.0 * 100, 0)


def load_pool(filename: str = '基础股票池.xlsx') -> Dict[str, Dict]:
    """加载选股池 (可指定文件名; v2.17: 支持绝对路径/含目录相对路径, 由配置驱动)"""
    pool = {}
    fpath = ds.file_path(filename, POOL_DIR)
    if not os.path.exists(fpath): return pool
    for enc in ['gbk', 'gb18030', 'utf-8']:
        try:
            with open(fpath, 'r', encoding=enc) as f:
                content = f.read()
            for line in content.replace('\r', '\n').split('\n'):
                parts = line.split('\t')
                code = parts[0].strip()
                if not (code.startswith('SZ') or code.startswith('SH')): continue
                code = code[:8]
                if code in pool: continue
                name = parts[1].strip() if len(parts) > 1 else ''
                # 自适应列结构: 基础池(33列) vs 新高池(34列)
                # 新高池: [5]=新高标记 [7]=上市板块 [8]=换手 [12]=总市值
                # 基础池: [5]=上市板块 [7]=MA21 [13]=总市值
                # 统一: 取代码+名称, 其余可选
                listing_board = parts[7].strip() if len(parts) > 7 and '板块' in str(parts[7]) else parts[5].strip() if len(parts) > 5 else ''
                ma21 = ma55 = ma120 = ma250 = 0
                # 尝试读MA21: 基础池在[7], 新高池无
                if len(parts) > 7 and parts[7].replace('.','').replace('-','').isdigit():
                    ma21 = float(parts[7])
                market_cap = parts[12].strip() if len(parts) > 12 and '亿' in str(parts[12]) else ''
                float_cap = parts[13].strip() if len(parts) > 13 and '亿' in str(parts[13]) else ''
                if not market_cap and len(parts) > 13:
                    market_cap = parts[13].strip() if len(parts) > 13 else ''
                    float_cap = parts[14].strip() if len(parts) > 14 else ''
                pool[code] = {'code': code, 'name': name, 'listing_board': listing_board,
                    'ma21': ma21, 'ma55': ma55, 'ma120': ma120, 'ma250': ma250,
                    'market_cap_str': market_cap, 'float_cap_str': float_cap}
            break
        except Exception: continue
    return pool


def build_pool_auto(progress_callback=None) -> Dict[str, Dict]:
    """从K线目录自动构建选股池 (MA21>MA55>MA120 AND close>MA21)"""
    pool = {}
    csv_files = sorted(os.listdir(KLINE_DIR))
    total = len(csv_files)
    for i, fname in enumerate(csv_files):
        if not fname.endswith('.csv'): continue
        code = fname.replace('.csv', '')
        if not (code.startswith('SZ') or code.startswith('SH')): continue
        fpath = os.path.join(KLINE_DIR, fname)
        try:
            tail = _read_csv_tail(fpath, 300)
            if len(tail) < 60: continue
            closes = [float(r['close']) for r in tail]
            ma21 = sum(closes[-21:]) / 21
            ma55 = sum(closes[-55:]) / 55 if len(closes) >= 55 else ma21
            ma120 = sum(closes[-120:]) / 120 if len(closes) >= 120 else ma55
            close = closes[-1]
            if not (ma21 > ma55 > ma120 and close > ma21): continue
            pool[code] = {'code': code, 'name': code, 'listing_board': '主板',
                'ma21': round(ma21,2), 'ma55': round(ma55,2), 'ma120': round(ma120,2),
                'ma250': 0, 'market_cap_str': '', 'float_cap_str': ''}
        except Exception: continue
        if progress_callback and i % 200 == 0: progress_callback(i, total, len(pool))
    return pool


# 本地条件建池（移植自 8801 朋友版 v2.19）：用户给的 6 个同花顺动态分组条件 → K 线全市场计算
#   20日新高  : 3 交易日内最高价创 20 日新高 + 收盘<MA250 + 收盘<MA120
#   55日新高  : 5 交易日内最高价创 55 日新高 + 收盘>MA21 + 收盘<MA250
#   百日新高  : 10 交易日内最高价创 100 日新高 + 收盘>MA21
#   趋势选股  : 收盘>MA21>MA55>MA120>MA250 + 收盘>MA50(10周) + 收盘>MA100(5月) + MA5>MA21
#   短期趋势股: MA5>MA10>MA21>MA55 + 收盘>MA21 + MA21 走平或向上（原条件带「流通市值<400亿」，
#               v2.79 用户拍板去掉 → 本系统只做均线形态）
#   寻找趋势龙: MA5>MA10>MA21>MA55 + 20 交易日内有涨停 + 上市>60 天
# 不排除 ST/板块（前端有展示筛选）；价格口径与前复权 CSV 对齐同花顺后复权结果（临界差异接受）。

def _pool_ma(closes: List[float], n: int) -> Optional[float]:
    if len(closes) < n:
        return None
    return sum(closes[-n:]) / n


def _pool_new_high(rows: List[Dict], ndays: int, within: int) -> bool:
    """within个交易日内任一天最高价创ndays日新高 (不含当日, 前ndays根)"""
    highs = [r['high'] for r in rows]
    for i in range(len(rows) - within, len(rows)):
        if i < ndays:
            continue
        if highs[i] > max(highs[i - ndays:i]):
            return True
    return False


def _cond_new_high_20(rows) -> bool:
    if len(rows) < 250:
        return False
    closes = [r['close'] for r in rows]
    c = closes[-1]
    if not (c < _pool_ma(closes, 250) and c < _pool_ma(closes, 120)):
        return False
    return _pool_new_high(rows, 20, 3)


def _cond_new_high_55(rows) -> bool:
    if len(rows) < 250:
        return False
    closes = [r['close'] for r in rows]
    c = closes[-1]
    if not (c > _pool_ma(closes, 21) and c < _pool_ma(closes, 250)):
        return False
    return _pool_new_high(rows, 55, 5)


def _cond_new_high_100(rows) -> bool:
    if len(rows) < 100:
        return False
    closes = [r['close'] for r in rows]
    if not (closes[-1] > _pool_ma(closes, 21)):
        return False
    return _pool_new_high(rows, 100, 10)


def _cond_trend(rows) -> bool:
    if len(rows) < 250:
        return False
    closes = [r['close'] for r in rows]
    c = closes[-1]
    vals = [_pool_ma(closes, n) for n in (5, 21, 50, 55, 100, 120, 250)]
    if None in vals:
        return False
    m5, m21, m50, m55, m100, m120, m250 = vals
    return (c > m21 > m55 > m120 > m250 and c > m50 and c > m100 and m5 > m21)


def _cond_short_trend(rows) -> bool:
    """短期趋势股 — 纯均线形态条件

    v2.79 (2026-09-12): 「流通市值<400亿」限制已按用户要求去掉, 本函数不做任何市值判断。"""
    if len(rows) < 55:
        return False
    closes = [r['close'] for r in rows]
    c = closes[-1]
    m5, m10, m21, m55 = (_pool_ma(closes, 5), _pool_ma(closes, 10),
                         _pool_ma(closes, 21), _pool_ma(closes, 55))
    if None in (m5, m10, m21, m55):
        return False
    if not (m5 > m10 > m21 > m55 and c > m21):
        return False
    # MA21 走平或向上: 今日>=昨日
    ma_today = sum(closes[-21:]) / 21
    ma_yest = sum(closes[-22:-1]) / 21
    return ma_today >= ma_yest


def _cond_dragon(rows) -> bool:
    """寻找趋势龙: MA排列 + 20交易日内有涨停 + 上市>60天"""
    if len(rows) <= 60:
        return False
    closes = [r['close'] for r in rows]
    m5, m10, m21, m55 = (_pool_ma(closes, 5), _pool_ma(closes, 10),
                         _pool_ma(closes, 21), _pool_ma(closes, 55))
    if None in (m5, m10, m21, m55):
        return False
    if not (m5 > m10 > m21 > m55):
        return False
    # 20交易日内涨停: 主板>=9.5% 创业/科创>=19.5% (主板含ST 5%不算涨停, 文件池非ST)
    for i in range(max(1, len(rows) - 20), len(rows)):
        prev_close = rows[i - 1]['close']
        if prev_close <= 0:
            continue
        pct = (rows[i]['close'] / prev_close - 1) * 100
        if pct >= 9.5:
            return True
    return False


CONDS_BY_MODE = {
    # v2.78 (2026-09-12): 20 模式补上「短期趋势股」— 与导出池 (pool_mode_20 = 20日新高 + 短期趋势) 对齐。
    # 此前自建池只模仿了 20日新高 一个条件, 漏了短期趋势股 → 池子口径比导出池窄
    '20': [('20日新高', _cond_new_high_20), ('短期趋势股', _cond_short_trend)],
    '55': [('55日新高', _cond_new_high_55), ('短期趋势股', _cond_short_trend)],
    '100': [('百日新高', _cond_new_high_100), ('趋势选股', _cond_trend), ('寻找趋势龙', _cond_dragon)],
}


def _fetch_float_caps(codes: List[str]) -> Dict[str, float]:
    """腾讯批量拉流通市值(亿) — v2.79 起 build_pool_conds 不再调用 (短期趋势股市值限制已取消), 保留备用

    qt.gtimg.cn 字段: [1]=名称 [2]=6位代码 [44]=流通市值(亿); 一次60只, 小写代码
    """
    caps = {}
    codes = [c.lower() for c in codes]
    for i in range(0, len(codes), 60):
        batch = codes[i:i + 60]
        url = ds.get('online', ('realtime', 'tencent_url'), 'http://qt.gtimg.cn/q={codes}')
        url = url.replace('{codes}', ','.join(batch))
        try:
            resp = _req_get(url, timeout=10)
            text = resp.text if hasattr(resp, 'text') else (resp or '')
            for line in (text or '').split(';'):
                if '="' not in line:
                    continue
                try:
                    var, payload = line.split('=', 1)
                    var = var.strip()
                    if not var.startswith('v_'):
                        continue
                    code = var[2:].upper()
                    parts = payload.strip().strip(';').strip('"').split('~')
                    if len(parts) > 44 and parts[44].replace('.', '').isdigit():
                        caps[code] = float(parts[44])
                except Exception:
                    continue
        except Exception:
            continue
    return caps


def build_pool_conds(mode: str = '100', progress_callback=None) -> Dict[str, Dict]:
    """本地条件建池: 按同花顺动态分组条件从K线全市场计算

    组合规则 (与文件池模式一致):
      '20'  = 20日新高 ∪ 短期趋势股   # v2.78: 原漏了短期趋势股
      '55'  = 55日新高 ∪ 短期趋势股
      '100' = 百日新高 ∪ 趋势选股 ∪ 寻找趋势龙
      v2.79: 短期趋势股 不再做「流通市值<400亿」过滤 (用户拍板)
    """
    pool = {}
    csv_files = sorted(os.listdir(KLINE_DIR))
    total = len(csv_files)
    conds = CONDS_BY_MODE.get(mode, [])
    for i, fname in enumerate(csv_files):
        if not fname.endswith('.csv'):
            continue
        code = fname.replace('.csv', '')
        if not (code.startswith('SZ') or code.startswith('SH')):
            continue
        try:
            rows = _read_csv_tail(os.path.join(KLINE_DIR, fname), 320)
            if len(rows) < 60:
                continue
            for _cname, cond in conds:
                if cond(rows):
                    if code not in pool:
                        pool[code] = {'code': code, 'name': '', 'listing_board': '主板',
                                      'ma21': 0, 'ma55': 0, 'ma120': 0, 'ma250': 0,
                                      'market_cap_str': '', 'float_cap_str': ''}
        except Exception:
            continue
        if progress_callback and i % 200 == 0:
            progress_callback(i, total, len(pool))
    # v2.79 (2026-09-12): 删掉「短期趋势股 流通市值<400亿」过滤 (用户拍板) → 不再拉腾讯市值, 建池更快
    return pool


def _read_csv_tail(fpath: str, n_lines: int) -> List[Dict]:
    """快速读取CSV文件末尾N行"""
    rows = []
    try:
        with open(fpath, 'r', encoding='utf-8') as f:
            header_line = f.readline().strip()
            header = header_line.replace('﻿', '').split(',')
            f.seek(0, 2); fsize = f.tell()
            if fsize > 20000:
                f.seek(max(0, fsize - 20000)); f.readline()
            else:
                # v2.97 (2026-09-16) 修复: 原实现在 fsize <= 20000 时**没有回退**, 文件位置停在
                #   上面 seek(0,2) 落下的 EOF → 下面的 for 一行都读不到, 对本类文件恒返回 []。
                #   全市场 5224 个K线文件里 152 个(2.9%)中招, 全是上市不满约2年的次新, 被下游
                #   `if len(tail) < 21: continue` 静默丢弃 —— 板块面板/概念面板/板块下钻/板块K线/
                #   龙头战法/涨停突破/涨停回调/自建池 统统看不见它们。
                f.seek(0); f.readline()      # 回到文件开头, 跳过表头行
            for line in f:
                line = line.strip()
                if not line: continue
                parts = line.split(',')
                if len(parts) < 5: continue
                try:
                    rows.append({'date': parts[0], 'open': float(parts[1]),
                        'close': float(parts[2]), 'high': float(parts[3]),
                        'low': float(parts[4]), 'volume': float(parts[5]) if len(parts) > 5 else 0})
                except (ValueError, IndexError): continue
            return rows[-n_lines:] if len(rows) > n_lines else rows
    except Exception: return rows


def load_multihead_sectors() -> Tuple[set, set]:
    """加载同花顺多头行业/概念指数名单 (用于选股输出过滤)
    v2.17: 文件路径由配置驱动 (bull_industry/bull_concept)"""
    ind_set, con_set = set(), set()
    for key, target in [('bull_industry', ind_set), ('bull_concept', con_set)]:
        fpath = ds.resolve(ds.get('local', key, ''))
        if not os.path.exists(fpath): continue
        try:
            with open(fpath, 'r', encoding='gbk') as f:
                content = f.read()
            for line in content.replace('\r', '\n').split('\n')[1:]:  # skip header
                parts = line.split('\t')
                if len(parts) >= 2:
                    name = parts[1].strip()
                    if name: target.add(name)
        except Exception: pass
    return ind_set, con_set


def load_sub_to_ind() -> Dict[str, str]:
    """加载 细分行业→一级行业 映射 (板块铁律双匹配用)

    个股map的行业是同花顺细分行业(443个, 如'国有大型银行'/'半导体设备'),
    多头名单含一级名(如'银行'/'半导体') → 匹配时细分不中再查一级
    """
    sub_map = {}
    fpath = ds.resolve(ds.get('local', 'sub_to_ind', 'kline_data/sub_to_ind.csv'))
    if os.path.exists(fpath):
        with open(fpath, 'r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                sub = (row.get('细分行业') or '').strip()
                ind1 = (row.get('一级行业') or '').strip()
                if sub and ind1:
                    sub_map.setdefault(sub, set()).add(ind1)
    return sub_map


# ---- v2.58 (2026-09-05): 概念屏蔽名单 — 终端读取层定向屏蔽 (map铁律: 绝不改 concept_map.csv) ----
# 名单文件 quant_terminal/concept_blocklist.txt, 每行一个概念【全名】一字不差精确匹配 —
# 绝不子串匹配 → 名单外新概念 (含相似名) 永不被误伤; 快照重导后读取层过滤依然生效, 被屏蔽概念不复活
CONCEPT_BLOCKLIST_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                      'concept_blocklist.txt')
_BLOCK_CACHE: Dict = {'mtime': -1.0, 'names': frozenset()}


def load_concept_blocklist() -> frozenset:
    """屏蔽概念名集合 (按 mtime 失效缓存; 文件缺失/异常 → 空集=不屏蔽, 安全默认)"""
    try:
        mt = os.path.getmtime(CONCEPT_BLOCKLIST_FILE)
        if mt == _BLOCK_CACHE['mtime']:
            return _BLOCK_CACHE['names']
    except Exception:
        _BLOCK_CACHE.update({'mtime': -1.0, 'names': frozenset()})
        return frozenset()
    names = set()
    try:
        with open(CONCEPT_BLOCKLIST_FILE, 'r', encoding='utf-8-sig') as f:
            for ln in f:
                s = ln.strip()
                if s and not s.startswith('#'):
                    names.add(s)
    except Exception:
        pass
    _BLOCK_CACHE.update({'mtime': mt, 'names': frozenset(names)})
    return _BLOCK_CACHE['names']


def _strip_blocked_concepts(val: str, bl: Optional[frozenset] = None) -> str:
    """概念 pipe 串过滤屏蔽名 (按 | 拆词, 全名精确 in 名单才剔除, 保序保量)
    bl: 外部已取的名单集合 (大批量遍历时传一次复用, 免每行 getmtime)"""
    if not val:
        return ''
    if bl is None:
        bl = load_concept_blocklist()
    if not bl:
        return val
    if '|' not in val:
        return '' if val in bl else val
    parts = val.split('|')
    return '|'.join(p for p in parts if p.strip() not in bl)


def load_industry_concept_maps() -> Tuple[Dict, Dict]:
    """加载行业/概念映射表 (v2.58: 概念值读取层过屏蔽名单 — 概念面板/列/验证/染色全链路生效)"""
    ind_map, con_map = {}, {}
    bl = load_concept_blocklist()  # 一次取名单复用, 免 5000+ 行各自 getmtime
    for fname, target in [('industry_map.csv', ind_map), ('concept_map.csv', con_map)]:
        cfg_key = fname.replace('.csv', '')  # industry_map / concept_map (配置键名)
        fpath = ds.resolve(ds.get('local', cfg_key, os.path.join('kline_data', fname)))
        if os.path.exists(fpath):
            with open(fpath, 'r', encoding='utf-8-sig') as f:
                for row in csv.DictReader(f):
                    code = row.get('代码', '').strip().upper()
                    val = row.get('行业' if 'industry' in fname else '概念', '').strip()
                    # v2.207b (2026-10-23): 北交所在 map 里写的是 BZ920285 这种**带前缀的 8 字符码**,
                    #   而旧代码 `code.isdigit()` 会把它们一律丢掉 ⇒ 353 只北交所股票在主表/热榜里拿不到行业概念。
                    #   统一归一到 6 位裸码(与 SZ/SH 一致), 消费方 `.get(num)` 全部受益, 不需改调用点。
                    bare = code[-6:] if (len(code) == 8 and code[:2] in ('SZ', 'SH', 'BZ', 'BJ')) else code
                    if bare and bare.isdigit():
                        target[bare] = val if 'industry' in fname else _strip_blocked_concepts(val, bl)
    return ind_map, con_map


# v5.0 (2026-09-12): 细分行业 → 上级(二级)行业名 — 前端表头悬停显示"上级所属-元件"
_PARENT_CACHE: Dict = {'mtime': -1.0, 'data': {}}


def load_industry_parent() -> Dict[str, str]:
    """industry_parent.csv → {细分行业: 上级行业}; mtime 未变则复用缓存

    由 import_ths_snapshot.py 的 rebuild_maps() 落盘; 文件缺失返回 {} (前端不显示上级, 不报错)。
    """
    fpath = ds.resolve(ds.get('local', 'industry_parent',
                              os.path.join('kline_data', 'industry_parent.csv')))
    if not os.path.exists(fpath):
        return {}
    mt = os.path.getmtime(fpath)
    if _PARENT_CACHE['mtime'] == mt:
        return _PARENT_CACHE['data']
    out = {}
    with open(fpath, 'r', encoding='utf-8-sig') as f:
        for row in csv.DictReader(f):
            sub = (row.get('细分行业') or '').strip()
            par = (row.get('上级行业') or '').strip()
            if sub and par:
                out[sub] = par
    _PARENT_CACHE['mtime'] = mt
    _PARENT_CACHE['data'] = out
    return out


# v2.100 (2026-09-16): 代码 → 上级行业名 (90 类口径) —— 上级趋势全量窗口的分组 map。
#   折叠 industry_map.csv(代码→细分) × industry_parent.csv(细分→上级)。
#   ⚠ 故意**不走 load_industry_concept_maps()**: 那会顺带读 concept_map.csv(952KB)+过屏蔽名单,
#     上级窗口用不到概念, 白白多读一份大文件。
#   ⚠ 缓存按**两个源文件 mtime 的最大值**失效 —— 只盯 industry_parent.csv 的话,
#     industry_map.csv 单独被重导(某只票改判细分)时缓存不失效, 要重启才更新。
_PARENT_MAP_CACHE: Dict = {'mtime': -1.0, 'data': {}}


def load_industry_parent_map() -> Dict[str, str]:
    """{6位代码: 上级行业名} — 90 类上级口径 (v2.100 2026-09-16)

    用途: 上级行业趋势窗口 (quant_terminal/parent_trend.py) 的分组 map。
    ★ 仅供查看, 不参与任何选股 —— 与 trend_data.json / 细分口径完全无关。

    两个源文件任一缺失、损坏或折叠结果为空 → 返回 {}。
    调用方(parent_trend.py force 路径)据此**拒绝刷新**, 绝不写空落盘覆盖旧数据。
    """
    try:
        p_ind = ds.resolve(ds.get('local', 'industry_map',
                                  os.path.join('kline_data', 'industry_map.csv')))
        p_par = ds.resolve(ds.get('local', 'industry_parent',
                                  os.path.join('kline_data', 'industry_parent.csv')))
        if not (os.path.exists(p_ind) and os.path.exists(p_par)):
            return {}
        mt = max(os.path.getmtime(p_ind), os.path.getmtime(p_par))
    except OSError:
        return {}
    if _PARENT_MAP_CACHE['mtime'] == mt:
        return _PARENT_MAP_CACHE['data']
    sub2par = load_industry_parent()
    if not sub2par:
        return {}
    out = {}
    try:
        with open(p_ind, 'r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                code = (row.get('代码') or '').strip().upper()
                par = sub2par.get((row.get('行业') or '').strip())
                # v2.207b: 同 load_industry_concept_maps —— 北交所在 map 里写 BZ920285,
                #   旧写 `code.isdigit()` 把 353 只北交所股票全丢 ⇒ 上级行业趋势的成分股统计/下钻列表少了它们。
                bare = code[-6:] if (len(code) == 8 and code[:2] in ('SZ', 'SH', 'BZ', 'BJ')) else code
                if par and bare.isdigit():
                    out[bare] = par
    except (OSError, UnicodeDecodeError, csv.Error):
        return {}
    _PARENT_MAP_CACHE['mtime'] = mt
    _PARENT_MAP_CACHE['data'] = out
    return out


# ---- v2.54 (2026-09-05): 板块实时双层缓存 — 盘中60s/盘后3600s, 自选/板块面板/扫描共用, 防重复拉腾讯 ----
# L1: 腾讯行业板块全表 (fetch_sector_realtime 结果)
# L2: calc_sector_returns 整表(含采样补充), key=池子行业集合 — 自选分组延迟优化 (原每次点击全量重算)
_SECTOR_RT_LOCK = threading.Lock()
_SECTOR_RT_CACHE: Dict = {'ts': 0.0, 'data': None}
_SECTOR_RET_LOCK = threading.Lock()
_SECTOR_RET_CACHE: Dict = {}


def _sector_rt_ttl() -> float:
    return 60.0 if _is_trade_window() else 3600.0


def clear_sector_rt_cache():
    """清空板块实时双层缓存 (更新板块/force重算后调用, 与 _sector_ma_cache_clear 配套, v2.54)"""
    with _SECTOR_RT_LOCK:
        _SECTOR_RT_CACHE['ts'] = 0.0
        _SECTOR_RT_CACHE['data'] = None
    with _SECTOR_RET_LOCK:
        _SECTOR_RET_CACHE.clear()


def fetch_sector_realtime(use_cache: bool = True) -> Dict[str, Dict]:
    """板块实时行情 — v2.173 (2026-10-22) 主源换成同花顺 fuyao。

    为什么换：原先只有腾讯 board/index 一个源，而那接口**实际只给 41 个板块**
    （`rank.plate` 6 条 + `fundflow.*` 每组 3 条的榜单 TopN），其余 669 个板块没数据。
    换成分三层：

      L1 同花顺 fuyao 指数快照  → **710 个 行业+概念** 全量覆盖，实测 1.2 秒
      L2 腾讯 board/index      → 只补它**独有**的资金面：主力净流入 / 涨跌家数 / 领涨股
                                  （同花顺指数快照没有这些字段）
      L3 本地板块日线 zdf.json → 5日 / 20日涨幅（腾讯只覆盖 41 个，本地能算全量）

    口径：同花顺板块指数 = 成分股涨跌幅的等权算术平均，与 `/api/sector_ma` 同口径
    （2026-10-22 用 8 个板块 8/8 精确复算验证：养殖业 36 只等权 1.98 = 官方 1.98 …）。
    所以切换后「板块热度涨跌幅」和「板块均线」是同一套数，不会再出现两套口径。
    """
    if use_cache:
        with _SECTOR_RT_LOCK:
            ent = _SECTOR_RT_CACHE
            if ent['data'] is not None and time.time() - ent['ts'] < _sector_rt_ttl():
                return dict(ent['data'])
    result = {}

    # ---------------- L1: 同花顺 fuyao —— 710 个 行业+概念 全量 ----------------
    _sk1 = None
    try:
        from quant_terminal import sector_kline as _sk1_
        _sk1 = _sk1_
        _snap, _snap_ts = _sk1.snapshot_indexes()
        _nm = _sk1.names()
        for _tc, _it in (_snap or {}).items():
            _info = _nm.get(_tc) or {}
            _name = _info.get('name') or ''
            if not _name:
                continue
            # 排除「特色统计指数」(883xxxx = 昨日涨停表现 / 昨日首板 / 近期复牌 …)：
            #   它们不是板块而是统计口径, 涨跌幅动辄 ±10% 会把 Top20 全占满
            if _info.get('tag') == 'tszs':
                continue
            result[_name] = {
                'change_pct': round(float(_it.get('pct') or 0), 2),
                'zdf5': 0.0, 'zdf20': 0.0,
                'up_count': -1, 'total_count': -1, 'net_flow_yi': 0.0,
                'lead_stock': '', 'lead_code': '', 'lead_pct': 0.0,
                'thscode': _tc, 'tag': _info.get('tag') or '', 'src': 'fuyao',
            }
    except Exception:
        pass

    # ---------------- L1b: 成分股广度（涨跌家数 + 领涨股）----------------
    # 同花顺指数快照不带这些字段，但它给的是「成分股等权平均」⇒ 拿成分股快照自己数一份
    if result and _sk1 is not None:
        try:
            from quant_terminal import fuyao as _fq1
            _stk, _ = _fq1.snapshot_all()
            _pct = {}
            for _c, _v in (_stk or {}).items():
                _pct[_c] = _v.get('pct')
            _br = _sk1.breadth(_pct, tags=())
            for _name, _row in result.items():
                _b = _br.get(_row.get('thscode') or '')
                if not _b:
                    continue
                if _row.get('total_count', -1) <= 0:
                    _row['up_count'] = _b['up']
                    _row['total_count'] = _b['total']
                if not _row.get('lead_code'):
                    _row['lead_stock'] = _b['lead_stock']
                    _row['lead_code'] = _b['lead_code']
                    _row['lead_pct'] = _b['lead_pct']
        except Exception:
            pass

    # ---------------- L2: 腾讯 —— 补齐 fuyao 缺的资金面字段 ----------------
    try:
        url = ds.get('online', 'sector_board', {}).get('url',
            'http://ifzq.gtimg.cn/appstock/app/board/index?board=industry')
        resp = _req_get(url, timeout=8)
        data = resp.json()
        for p in data.get('data', {}).get('rank', {}).get('plate', []):
            name = p.get('bd_name', '')
            if not name:
                continue
            _lead = {'lead_stock': p.get('nzg_name', ''),
                     'lead_pct': round(float(p.get('nzg_zdf', 0)), 2),
                     'lead_code': p.get('nzg_code', '')}
            if name in result:
                row = result[name]
                if not row.get('lead_code'):
                    row.update(_lead)
                # 5日/20日 只在「腾讯独有板」时才用腾讯的；有 thscode 的走 L3 官方口径
                if not row.get('thscode'):
                    if not row.get('zdf5'):
                        row['zdf5'] = round(float(p.get('bd_zdf5', 0)), 2)
                    if not row.get('zdf20'):
                        row['zdf20'] = round(float(p.get('bd_zdf20', 0)), 2)
            else:
                # 腾讯独有名字（如「电视广播Ⅱ」）—— 留着，总比没有强；
                # 但若去掉「Ⅱ」后同花顺那边已有同名板块，那就是同一个板块的重复行，跳过
                _base = name.replace('Ⅱ', '').replace('II', '').strip()
                if _base and _base != name and _base in result:
                    continue
                result[name] = {'change_pct': round(float(p.get('bd_zdf', 0)), 2),
                    'zdf5': round(float(p.get('bd_zdf5', 0)), 2),
                    'zdf20': round(float(p.get('bd_zdf20', 0)), 2),
                    'up_count': -1, 'total_count': -1, 'net_flow_yi': 0,
                    'thscode': '', 'tag': '', 'src': 'tencent'}
                result[name].update(_lead)
        for section in ['plate', 'concept', 'area']:
            ff = data.get('data', {}).get('fundflow', {}).get(section, {})
            for key in ['top', 'bottom', 'top_d5', 'bottom_d5', 'top_d20', 'bottom_d20']:
                for p in ff.get(key, []):
                    name = p.get('name', '')
                    if not name:
                        continue
                    zgb = p.get('zgb', '0/0')
                    parts = zgb.split('/')
                    up_cnt = int(parts[0]) if len(parts) > 0 and parts[0].isdigit() else 0
                    total_cnt = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
                    net_flow = round(float(p.get('zljlr', 0)) / 10000, 2)
                    lzg = p.get('lzg', {}) or {}
                    if name not in result:
                        _base2 = name.replace('Ⅱ', '').replace('II', '').strip()
                        if _base2 and _base2 != name and _base2 in result:
                            continue
                        result[name] = {'change_pct': round(float(p.get('zdf', 0)), 2),
                            'zdf5': 0, 'zdf20': 0, 'up_count': up_cnt, 'total_count': total_cnt,
                            'net_flow_yi': net_flow, 'lead_stock': lzg.get('name', ''),
                            'lead_code': lzg.get('code', ''),
                            'lead_pct': round(float(lzg.get('zdf', 0)), 2),
                            'thscode': '', 'tag': '', 'src': 'tencent'}
                    else:
                        row = result[name]
                        # 哨兵用 total_count<=0：-1=未知(fuyao)，0=板块全跌(合法状态)
                        if row.get('total_count', -1) <= 0 and total_cnt > 0:
                            row['up_count'] = up_cnt
                            row['total_count'] = total_cnt
                        if not row.get('net_flow_yi'):
                            row['net_flow_yi'] = net_flow
                        if not row.get('lead_code') and lzg.get('code'):
                            row['lead_code'] = lzg.get('code')
                            row['lead_stock'] = lzg.get('name', '')
                            row['lead_pct'] = round(float(lzg.get('zdf', 0)), 2)
    except Exception:
        pass

    # ---------------- L3: 本地板块日线补 5日/20日涨幅 ----------------
    # 有 thscode 的一律用官方日线算（与 change_pct 同源同口径），腾讯那个数口径不同不能混用
    try:
        from quant_terminal import sector_kline as _sk3
        _z = _sk3.zdf_map()
        for _name, _row in result.items():
            _tc = _row.get('thscode')
            if not _tc:
                continue
            _z1 = _z.get(_tc)
            if not _z1:
                continue
            _row['zdf5'] = round(float(_z1[0]), 2)
            _row['zdf20'] = round(float(_z1[1]), 2)
    except Exception:
        pass

    if result:  # 成功才落缓存 (空结果=网络失败, 下次重试)
        with _SECTOR_RT_LOCK:
            _SECTOR_RT_CACHE['ts'] = time.time()
            _SECTOR_RT_CACHE['data'] = result
    return result


# ============================================================
# 孤立左峰 (与实盘框架 kline_reader.find_left_peak 一致)
# 定义: ≥min_days_ago日前 + lookback日最高 + 前后no_new_high_days日均为最高
# 注意: 不能用滚动窗口max(highs[-N:]) 当左峰 — 突破日的高点会被当成峰,
#       导致"已突破"被误判为"未突破" (2026-08-02有研新材6/11突破5/11左峰33.34案例)
# ============================================================

def find_left_peak_list(highs: List[float], min_days_ago: int = 5,
                        lookback: int = 100, no_new_high_days: int = 5) -> Optional[Dict]:
    """孤立左峰: 窗口内按高度降序验证, 找第一个满足(峰后N日无新高+峰前N日无新高)的孤立峰"""
    n = len(highs)
    end_idx = n - min_days_ago - 1
    start_idx = max(0, end_idx - lookback)
    if end_idx <= start_idx or end_idx >= n: return None
    window_idx = sorted(range(start_idx, end_idx + 1),
                        key=lambda i: highs[i], reverse=True)
    for peak_idx in window_idx:
        peak_high = highs[peak_idx]
        check_end = min(n - 1, peak_idx + no_new_high_days)
        if any(highs[i] > peak_high for i in range(peak_idx + 1, check_end + 1)):
            continue
        check_start = max(0, peak_idx - no_new_high_days)
        if any(highs[i] >= peak_high for i in range(check_start, peak_idx)):
            continue
        return {'price': float(peak_high), 'idx': peak_idx}
    return None


def find_recent_two_peaks(highs: List[float], min_days_ago: int = 5,
                          no_new_high_days: int = 5,
                          max_lookback: int = 250) -> Dict:
    """最近两个清晰左峰 (2026-08-06, 画线用)

    从最近往回遍历找孤立峰(峰前后 no_new_high_days 日无更高), 取最近两个:
      peak2 = 最近的峰 (操作锚点) / peak1 = 稍远的峰 (参照标尺)
    返回 {'peak1': {price,idx}|None, 'peak2': {price,idx}|None}
    2026-08-06 用户定案:
      1. 免疫期 min_days_ago=5 必须保留 — 排除"突破后冲出的高点"(如突破日新高),
         峰的定义是突破前的左峰, 不是近期最高点
      2. 不做两峰间距过滤 — 峰①(窗口最高孤立峰)与峰②(最近孤立峰)重叠是正常
         现象 (承德露露55日: 两者同为8.58), 刻意拉开反而破坏定义
      3. 单边上涨无清晰孤立峰的票: 找最近的孤立峰即可, 位置低也接受 (上港集团案例)
    """
    n = len(highs)
    end_idx = n - min_days_ago - 1
    start_idx = max(0, end_idx - max_lookback)
    peaks = []
    for peak_idx in range(end_idx, start_idx - 1, -1):
        peak_high = highs[peak_idx]
        check_end = min(n - 1, peak_idx + no_new_high_days)
        if any(highs[i] > peak_high for i in range(peak_idx + 1, check_end + 1)):
            continue
        check_start = max(0, peak_idx - no_new_high_days)
        if any(highs[i] >= peak_high for i in range(check_start, peak_idx)):
            continue
        peaks.append({'price': float(peak_high), 'idx': peak_idx})
        if len(peaks) >= 2:
            break
    return {'peak1': peaks[1] if len(peaks) > 1 else None,
            'peak2': peaks[0] if peaks else None}


# ============================================================
# 技术指标
# ============================================================

class Indicators:
    """纯Python技术指标计算"""

    @staticmethod
    def sma(values: List[float], period: int) -> List[Optional[float]]:
        result = []
        for i in range(len(values)):
            if i < period - 1: result.append(None)
            else: result.append(sum(values[i - period + 1:i + 1]) / period)
        return result

    @staticmethod
    def atr(data: List[Dict], period: int = 20) -> List[Optional[float]]:
        tr_values = []
        for i, d in enumerate(data):
            if i == 0: tr = d['high'] - d['low']
            else: tr = max(d['high'] - d['low'], abs(d['high'] - data[i - 1]['close']), abs(d['low'] - data[i - 1]['close']))
            tr_values.append(tr)
        return Indicators.sma(tr_values, period)

    @staticmethod
    def adx(data: List[Dict], period: int = 14) -> Tuple[List[Optional[float]], List[Optional[float]], List[Optional[float]]]:
        n = len(data)
        if n < period + 1: return ([None] * n, [None] * n, [None] * n)
        tr_list, plus_dm, minus_dm = [0] * n, [0] * n, [0] * n
        for i in range(1, n):
            tr_list[i] = max(data[i]['high'] - data[i]['low'], abs(data[i]['high'] - data[i-1]['close']), abs(data[i]['low'] - data[i-1]['close']))
            up_move = data[i]['high'] - data[i-1]['high']
            down_move = data[i-1]['low'] - data[i]['low']
            plus_dm[i] = up_move if up_move > down_move and up_move > 0 else 0
            minus_dm[i] = down_move if down_move > up_move and down_move > 0 else 0
        tr_smooth = Indicators.sma(tr_list, period)
        plus_di = Indicators.sma(plus_dm, period)
        minus_di = Indicators.sma(minus_dm, period)
        adx_vals = [None] * n
        prev_adx = None
        for i in range(period * 2 - 1, n):
            if tr_smooth[i] and tr_smooth[i] > 0:
                pdi = plus_di[i] or 0; mdi = minus_di[i] or 0
                if pdi + mdi > 0:
                    dx = abs(pdi - mdi) / (pdi + mdi) * 100
                    if prev_adx is None:
                        valid_dxs = []
                        for j in range(i - period + 1, i + 1):
                            if j >= period*2-1 and tr_smooth[j] and tr_smooth[j] > 0:
                                pdj = plus_di[j] or 0; mdj = minus_di[j] or 0
                                if pdj + mdj > 0: valid_dxs.append(abs(pdj - mdj) / (pdj + mdj) * 100)
                        prev_adx = sum(valid_dxs) / len(valid_dxs) if valid_dxs else dx
                    else: prev_adx = (prev_adx * (period - 1) + dx) / period
                    adx_vals[i] = prev_adx
        return adx_vals, plus_di, minus_di

    @staticmethod
    def bias(data: List[Dict], period: int = 20) -> List[Optional[float]]:
        closes = [d['close'] for d in data]
        ma = Indicators.sma(closes, period)
        result = [None] * len(data)
        for i in range(len(data)):
            if ma[i] is not None and ma[i] > 0: result[i] = (closes[i] - ma[i]) / ma[i] * 100
        return result

    @staticmethod
    def slope(values: List[Optional[float]], lookback: int = 5) -> Optional[float]:
        valid = [(i, v) for i, v in enumerate(values) if v is not None]
        if len(valid) < lookback: return None
        pts = valid[-lookback:]
        n = len(pts)
        sum_x = sum(p[0] for p in pts); sum_y = sum(p[1] for p in pts)
        sum_xy = sum(p[0] * p[1] for p in pts); sum_x2 = sum(p[0] * p[0] for p in pts)
        denom = n * sum_x2 - sum_x * sum_x
        if denom == 0: return 0
        return (n * sum_xy - sum_x * sum_y) / denom

# ============================================================
# 模块: 三周期左峰突破引擎
# ============================================================

class LeftPeakEngine:
    """
    左峰突破引擎 (周期最高峰为锚点)
    =======================================
    参数化: 100日(长期方向) / 55日(中期趋势) / 20日(短期入场)

    海龟原文:
      系统一: 20日高点突破入场, 10日低点突破离场
      系统二: 55日高点突破入场, 20日低点突破离场
    """

    def __init__(self, kline_data: List[Dict], period: int, label: str):
        self.data = kline_data
        self.period = period
        self.label = label  # '100', '55', '20'
        self.n = len(kline_data)
        self.closes = [d['close'] for d in kline_data]
        self.opens = [d['open'] for d in kline_data]
        self.highs = [d['high'] for d in kline_data]
        self.lows = [d['low'] for d in kline_data]
        self.volumes = [d['volume'] for d in kline_data]
        self.atr20 = Indicators.atr(kline_data, 20)
        self.ma21 = Indicators.sma(self.closes, 21)
        self.ma55 = Indicators.sma(self.closes, 55)
        self.ma250 = Indicators.sma(self.closes, 250)
        self.vol_ma20 = Indicators.sma(self.volumes, 20)
        # v2.3左峰锚点回归: 池子票已突破周期高点 → 左峰 = 前period日窗口(≥5日前)内最高
        # 逻辑: 突破日(最近5日内)被排除在窗口外, 窗口内最高 = 被突破的周期高点 = 左峰
        # 不做孤立验证(突破本身已确认其存在); 不再fallback唐奇安滚动高(2026-08-02用户决策)
        # 设计(用户2026-08-02定): 绿线=周期最高峰(参照, 不孤立也没关系);
        #   操作锚点用蓝线=近孤立峰(find_left_peak_list, 孤立验证拒绝非真峰)
        #   窗口边界效应(峰前高点被截断)由蓝线兜底 — 不扩大窗口(治标不治本)
        if self.n >= period + 6:
            end = self.n - 6            # 排除当日+最近5日
            start = max(0, end - period)
            win = self.highs[start:end + 1]
            maxv = max(win)
            self.left_peak = {'price': float(maxv), 'idx': start + win.index(maxv)}
        else:
            self.left_peak = None

    @staticmethod
    def quick_left_peak(kline: List[Dict], period: int) -> float:
        """轻量左峰: 只算周期窗口最高价 (与构造left_peak口径完全一致), 不重算ATR/MA指标

        P3-3 fix: mode 55/20 的多周期比较原来整体重建第二套LeftPeakEngine, 每票多算一遍指标
        """
        highs = [d['high'] for d in kline]
        n = len(highs)
        if n >= period + 6:
            end = n - 6
            start = max(0, end - period)
            win = highs[start:end + 1]
            return round(float(max(win)), 2)
        if n < 2: return round(float(max(highs)), 2)
        return round(float(max(highs[-period - 1:-1])), 2)

    def _get_left_peak_price(self) -> float:
        """左峰 = 孤立左峰 (与实盘框架1一致)

        2026-08-02修正: 原为滚动前N日最高(唐奇安), 突破日高点会进窗口当"峰",
        导致已突破被误判未突破(有研新材6/11突破5/11左峰案例)。
        孤立左峰: 前后5日均为最高, 才是真正的峰。
        """
        if self.left_peak:
            return self.left_peak['price']
        if self.n < 2: return max(self.highs)
        return max(self.highs[-self.period - 1:-1])

    def _get_recent_low(self) -> float:
        """前N日最低价 (近期低点, 海龟止损用) — 不含当日"""
        if self.n < 2: return min(self.lows)
        return min(self.lows[-self.period - 1:-1])

    def check_breakout(self) -> Dict:
        """
        左峰突破检测
        海龟原文: 收盘价 > N日最高价 → 突破
        """
        d_high = self._get_left_peak_price()
        d_low = self._get_recent_low()
        close = self.closes[-1]
        vol = self.volumes[-1]
        vol_ma = self.vol_ma20[-1] if self.vol_ma20[-1] else vol
        atr = self.atr20[-1] if self.atr20[-1] else close * 0.03

        is_breakout = close >= d_high  # 允许等于(刚好在左峰)
        vol_ok = vol > vol_ma * 1.5  # Minervini: 1.5-2倍
        decisive = close > d_high + atr * 0.5  # 至少超过通道0.5ATR

        # 距左峰的距离 (ATR单位)
        dist_to_high = (close - d_high) / atr if atr > 0 else 0

        # 价格在通道内的相对位置 (0=最低, 1=最高)
        channel_width = d_high - d_low
        position_in_channel = (close - d_low) / channel_width if channel_width > 0 else 0.5

        return {
            'is_breakout': is_breakout and vol_ok,
            'is_decisive': decisive and vol_ok,
            'left_peak': round(d_high, 2),
            'recent_low': round(d_low, 2),
            'close': round(close, 2),
            'dist_atr': round(dist_to_high, 2),
            'position_pct': round(position_in_channel * 100, 1),
            'volume_ratio': round(vol / vol_ma, 2) if vol_ma > 0 else 0,
        }

    def check_pullback(self, left_peak: float) -> Dict:
        """
        回踩左峰 (突破后的缩量回踩)
        框架7底部反转/框架8 V型反转 核心逻辑:
          价格在左峰附近缩量 → 支撑确认
        """
        low = self.lows[-1]
        close = self.closes[-1]
        atr = self.atr20[-1] if self.atr20[-1] else close * 0.03
        vol = self.volumes[-1]
        vol_ma = self.vol_ma20[-1] if self.vol_ma20[-1] else vol

        # 回踩: 最低价在左峰上方1ATR内, 缩量
        near_high = low > left_peak and (low - left_peak) <= atr
        vol_dry = vol < vol_ma * 0.7

        return {
            'is_pullback': near_high and vol_dry,
            'low': round(low, 2),
            'dist_atr': round((low - left_peak) / atr, 2) if atr > 0 else 0,
            'volume_ratio': round(vol / vol_ma, 2),
        }

    def check_trend(self) -> Dict:
        """趋势方向: MA 排列 + 价格位置"""
        c = self.closes[-1]
        a21 = self.ma21[-1]; a55 = self.ma55[-1]; a250 = self.ma250[-1]

        aligned = a21 is not None and a55 is not None and a21 > a55
        above_ma21 = a21 is not None and c > a21
        ma21_rising = Indicators.slope(self.ma21, 5)
        ma21_up = ma21_rising is not None and ma21_rising > 0
        above_ma250 = a250 is not None and c > a250

        if aligned and above_ma21 and ma21_up:
            direction = 'UP'
        elif c < a21 and ma21_rising is not None and ma21_rising < 0:
            direction = 'DOWN'
        else:
            direction = 'NEUTRAL'

        return {
            'direction': direction,
            'aligned': aligned,
            'above_ma21': above_ma21,
            'ma21_rising': ma21_up,
            'above_ma250': above_ma250,
        }

    def run(self) -> Dict:
        """运行 左峰突破检测"""
        bo = self.check_breakout()
        trend = self.check_trend()
        pb = self.check_pullback(bo['left_peak'])
        atr = self.atr20[-1] if self.atr20[-1] else self.closes[-1] * 0.03

        # 信号判定 (海龟 + 利弗莫尔结合)
        if bo['is_decisive'] and trend['direction'] == 'UP':
            signal = 'BREAKOUT'
            confidence = 4
        elif bo['is_breakout'] and trend['direction'] == 'UP':
            signal = 'BREAKOUT'
            confidence = 3
        elif pb['is_pullback'] and trend['direction'] == 'UP':
            signal = 'PULLBACK'
            confidence = 5
        elif bo['is_breakout']:
            signal = 'BREAKOUT_WEAK'
            confidence = 2
        elif bo['position_pct'] > 70 and trend['above_ma21']:
            signal = 'NEAR_HIGH'
            confidence = 1
        elif trend['direction'] == 'DOWN':
            signal = 'DOWNTREND'
            confidence = 0
        else:
            signal = 'NONE'
            confidence = 0

        # 止损: 近期低点 或 2ATR
        stop_recent = bo['recent_low']
        stop_atr = self.closes[-1] - 2 * atr
        stop_loss = max(stop_atr, stop_recent)  # 取较紧的

        return {
            'period': self.period,
            'label': self.label,
            'signal': signal,
            'confidence': confidence,
            'channel': bo,
            'pullback': pb,
            'trend': trend,
            'stop_loss': round(stop_loss, 2),
            'atr': round(atr, 2),
        }

    def quality_score(self) -> Dict:
        """
        突破质量评分 (价格3分 + 成交量3分 = 6分制)
        价格维度: 上引线试探左峰的行为质量
        成交量维度: 突破前后量价配合质量
        """
        if self.n < 10: return {'price': 0, 'vol': 0, 'total': 0}
        d_high = self._get_left_peak_price()
        atr = self.atr20[-1] if self.atr20[-1] else self.closes[-1] * 0.03
        close = self.closes[-1]

        # === 价格维度 (0-3) ===
        # 基础分: 现价在峰下5%内或5天内有试探→1分, 否则0分
        dist_pct = (close - d_high) / d_high * 100 if d_high > 0 else 0
        near_enough = dist_pct >= -5  # 距峰5%以内
        stood_above = any(self.closes[i] > d_high for i in range(-5, 0))
        price_score = 1 if (near_enough or stood_above) else 0

        if price_score >= 1:
            # 检查上引线试探行为 (最近5天)
            wick_tests = []  # 记录试探日
            for i in range(-5, 0):
                oi = self.opens[i]; ci = self.closes[i]; hi = self.highs[i]
                body_high = max(oi, ci)
                upper_wick = hi - body_high
                if upper_wick > 0:
                    gap_to_peak = d_high - hi
                    if gap_to_peak <= 0.3 * atr:  # 上引线打到左峰0.3ATR内
                        wick_tests.append({'idx': i, 'type': 'near'})
                    if hi > d_high:  # 上引线穿越左峰
                        wick_tests.append({'idx': i, 'type': 'cross'})

            # 3分条件1: 前4天至少2天有上引线试探 + 收盘在峰上或近峰(1%内)
            tests_last4 = [w for w in wick_tests if w['idx'] >= -4]
            if len(tests_last4) >= 2 and dist_pct > -1:
                price_score = 3

            # 3分条件2: 前3天至少1天试探 + 近峰 + 之后回踩不破
            tests_last3 = [w for w in wick_tests if w['idx'] >= -3]
            if price_score < 3 and len(tests_last3) >= 1 and dist_pct > -1:
                post_lows = [self.lows[i] for i in range(-2, 0)]
                if post_lows and min(post_lows) > d_high * 0.98:
                    price_score = 3

            # 2分条件: 近峰+近3天至少有2天在峰上或近峰
            if price_score < 3 and dist_pct > -2:
                days_near = sum(1 for i in range(-3, 0) if self.closes[i] > d_high * 0.98)
                if days_near >= 2:
                    price_score = max(price_score, 2)

        # === 成交量维度 (0-6) ===
        # 子维度1: 突破日放量 vs 20日均量 (0-3)
        # 子维度2: 突破后缩量 vs 突破日量 (0-3)
        # 找最近一个突破日 (收盘站上左峰); bt_day=距当前天数(正数, 0=未找到)
        # (2026-08-16 修复: 原bt_day为负索引, "bt_day>0"恒假→avg_vol_20恒=bt_vol→ratio恒1.0,
        #  且"bt_day>=-20"恒真→未找到突破日也给v1=1, 量价子分完全无区分度)
        bt_day = 0
        for i in range(-20, 0):
            if self.closes[i] > d_high:
                bt_day = -i; break

        v1 = v2 = 0
        if bt_day > 0:
            bt_vol = self.volumes[-bt_day]
            # 突破日前20日均量 (不含突破日; 数据≥120根, 切片安全)
            avg_vol_20 = sum(self.volumes[-bt_day-20:-bt_day]) / 20
            vol_ratio_bt = bt_vol / avg_vol_20 if avg_vol_20 > 0 else 1

            # 子维度1: 突破放量倍数 (0-3)
            if vol_ratio_bt >= 2.0: v1 = 3
            elif vol_ratio_bt >= 1.5: v1 = 2
            elif vol_ratio_bt >= 1.0: v1 = 1
            else: v1 = 0

            # 子维度2: 突破后缩量程度 (0-3), 用最新交易日量 vs 突破日量
            latest_vol = self.volumes[-1]
            shrink_ratio = latest_vol / bt_vol if bt_vol > 0 else 1
            if shrink_ratio <= 0.5: v2 = 3       # 缩到一半以下, 强锁仓
            elif shrink_ratio <= 0.7: v2 = 2     # 明显缩量
            elif shrink_ratio <= 0.9: v2 = 1     # 略微缩量
            else: v2 = 0                         # 没缩

            vol_score = v1 + v2
        else:
            # 无近期突破日: 给基础分2分(量价维度没有明显信号)
            vol_score = 2

        return {'price': price_score, 'vol': vol_score,
                'v_breakout': v1 if bt_day > 0 else 0,
                'v_shrink': v2 if bt_day > 0 else 0,
                'total': price_score + vol_score}


class AShareVerifier:
    """A股适配验证 — 对任意买入信号做四道检查"""

    def __init__(self, kline_data: List[Dict], pool_info: Dict,
                 sector_returns: Optional[Dict] = None):
        self.data = kline_data
        self.pool = pool_info
        self.n = len(kline_data)
        self.closes = [d['close'] for d in kline_data]
        self.sector_returns = sector_returns or {}
        self.bias20 = Indicators.bias(kline_data, 20)

    def check_sector(self) -> Tuple[bool, float, str]:
        """板块共振: 个股所属板块是否偏强 (>0%)

        v2.216 (2026-10-24 用户 m06859): 第三个返回值 = 这个数**从哪来的**。
          'sector'  / 'concept' → 真板块/概念的当日等权平均涨幅 (同花顺官方指数口径)
          'self20'               → 三级都找不到时的兜底: 个股自己的 20 日涨幅
        为什么要标出来: 风语筑 16.7% 那次, 用户以为板块涨了 16.7%, 其实是该股自身
        20 日涨幅冒充了板块涨幅 (v2.68 起就存在, 一直没人发现)。前端按 src 显示
        「板块 4.1%」还是「自身20日 16.7%」, 不再鱼目混珠。
        """
        sector = self.pool.get('sector', '')
        concept = self.pool.get('concept', '')
        sec_data = self.sector_returns.get(sector, None)
        src = 'sector'
        # 模糊匹配
        if sec_data is None and sector:
            for tn, data in self.sector_returns.items():
                if sector in tn or tn in sector: sec_data = data; break
        if sec_data is None and concept:
            src = 'concept'
            for con in concept.split('|'):
                con = con.strip()
                for tn, data in self.sector_returns.items():
                    if con and (con in tn or tn in con): sec_data = data; break
                if sec_data is not None: break
        if isinstance(sec_data, dict):
            strength = sec_data.get('change_pct', 0)
        elif isinstance(sec_data, (int, float)):
            strength = sec_data
        else:
            # 兜底: 个股20日涨幅
            src = 'self20'
            if len(self.closes) >= 20:
                strength = (self.closes[-1] - self.closes[-20]) / self.closes[-20] * 100
            else: strength = 0
        return strength > 0, round(strength, 2), src

    def check_bias(self) -> Tuple[bool, float, str]:
        """BIAS乖离率: 偏离MA20是否过大 (>15%警告)"""
        bias = self.bias20[-1] if self.bias20[-1] is not None else 0
        if bias > 15: return False, round(bias, 2), '过度拉伸'
        if bias < -15: return True, round(bias, 2), '深度超跌'
        return True, round(bias, 2), '正常'

    def check_limit(self, change_pct: float, code: str = '') -> Tuple[bool, str]:
        """涨跌停检查
        (2026-08-16 修复: code 带 SH/SZ 前缀, 原 startswith('688') 恒False→双创按主板9.5%误判;
         双创=创业板300/301/302 + 科创板688/689, 阈值20%级别)
        (2026-09-11: 补 302 段 — 深交所新增创业板号段, 如 SZ302132 中航成飞)"""
        num = code[2:] if code.startswith(('SZ', 'SH')) else code
        is_star = num.startswith(('300', '301', '302', '688', '689'))
        limit = 19.5 if is_star else 9.5
        if change_pct >= limit: return False, '涨停'
        if change_pct <= -limit: return False, '跌停'
        return True, '正常'

    def verify(self, change_pct: float, code: str = '') -> Dict:
        """运行全部验证, 返回通过数 + 详情"""
        checks = []
        # 1. 板块
        sector_ok, sector_str, sector_src = self.check_sector()
        if sector_ok: checks.append('板块共振')
        # 2. BIAS
        bias_ok, bias_val, bias_label = self.check_bias()
        if not bias_ok: checks.append(f'BIAS警告({bias_label})')
        # 3. 涨跌停
        limit_ok, limit_label = self.check_limit(change_pct, code)
        if not limit_ok: checks.append(limit_label)

        passed = sum([sector_ok, bias_ok, limit_ok])
        return {
            'passed': passed,
            'total': 3,
            'sector_ok': sector_ok,
            'sector_strength': sector_str,
            'sector_src': sector_src,
            'bias_ok': bias_ok,
            'bias_val': bias_val,
            'bias_label': bias_label,
            'limit_ok': limit_ok,
            'limit_label': limit_label,
            'checks': checks,
        }


def _m4_for(kline, sector, concept, change_pct, code, sector_returns):
    """v2.216 (2026-10-24 用户 m06859): 给**非趋势三模式**的扫描行补 A股适配。

    为什么要有这个函数：A股适配原本只在 `QuantEngine.scan_single`（趋势主升/延续/反转）
    里算，涨停突破/涨停回调/均线粘合/龙头战法四个扫描器都没算 ⇒ 前端详情页那栏显示
    「--」。现在这四个扫描器在组装结果行时各自调一次，口径与 scan_single 完全一致
    （同一个 AShareVerifier、同一张 sector_returns 表）。

    返回 None 表示算不出来（K线太短、脏数据）—— 调用方**不要**把 None 写成 0，
    让前端继续显示「--」比显示一个假的 0 更诚实。
    """
    if not kline:
        return None
    try:
        return AShareVerifier(kline, {'sector': sector or '', 'concept': concept or ''},
                              sector_returns or {}).verify(change_pct, code)
    except Exception:
        return None


# ============================================================
# 横切执行层: 海龟仓位
# ============================================================

class TurtleExecutor:
    """海龟交易法则仓位执行"""

    def __init__(self, kline_data: List[Dict], account_size: float = 200000):
        self.data = kline_data
        self.account = account_size
        self.closes = [d['close'] for d in kline_data]
        self.highs = [d['high'] for d in kline_data]
        self.lows = [d['low'] for d in kline_data]
        self.ma250 = Indicators.sma(self.closes, 250)
        self.atr20 = Indicators.atr(kline_data, 20)
        self.atr22 = Indicators.atr(kline_data, 22)

    def calc_stop_loss(self, price: float, recent_low: Optional[float] = None,
                       hard_stop_pct: float = 0.12) -> Dict:
        """海龟止损: 2N 或近期低点, 取较紧 + 兜底硬止损

        v2.1 双轨制: 2N 对ATR巨大的突破票止损距离过大(实测-20%),
        硬止损兜底 = 止损线不低于 price×(1-hard_stop_pct), 默认-12%
        """
        n_val = self.atr20[-1] if self.atr20[-1] else price * 0.03
        stop_2n = price - 2 * n_val
        if recent_low and recent_low > 0:
            stop_loss = max(stop_2n, recent_low)
        else:
            stop_loss = stop_2n
        # 兜底硬止损: 与2N止损取较紧 (max=更靠近现价)
        hard = price * (1 - hard_stop_pct)
        stop_loss = max(stop_loss, hard)
        return {'stop_loss': round(stop_loss, 2), 'n_atr20': round(n_val, 2),
                'stop_distance_atr': round((price - stop_loss) / n_val, 2) if n_val else 0,
                'n_pct': round(n_val / price * 100, 2),
                'stop_pct': round((price - stop_loss) / price * 100, 1)}

    def calc_position(self, price: float) -> Dict:
        """海龟单位仓位: 1 Unit = 1%账户 / N"""
        n_val = self.atr20[-1] if self.atr20[-1] else price * 0.03
        if n_val <= 0: n_val = price * 0.03
        unit_shares = int(self.account * 0.01 / n_val)
        unit_shares = (unit_shares // 100) * 100
        unit_shares = max(unit_shares, 100)
        max_shares = min(unit_shares * 4, int(self.account * 0.24 / price))
        max_shares = (max_shares // 100) * 100
        return {'unit_shares': unit_shares, 'max_shares': max_shares,
                'unit_value': round(unit_shares * price, 2),
                'max_value': round(max_shares * price, 2),
                'unit_pct': round(unit_shares * price / self.account * 100, 1),
                'max_pct': round(max_shares * price / self.account * 100, 1)}

    def calc_addon(self, entry_price: float, units_held: int = 1) -> Dict:
        """海龟0.5N金字塔加仓: 每上涨0.5N加1单位, 止损上移到最后加仓价-2N

        units_held: 当前已持有单位数 (1=初始仓, 2=加过一次, ...)
        下一档加仓触发价 = entry_price + units_held × 0.5N
        加仓后新止损 = 加仓触发价 - 2N
        """
        n_val = self.atr20[-1] if self.atr20[-1] else entry_price * 0.03
        next_addon = entry_price + units_held * 0.5 * n_val
        new_stop = next_addon - 2 * n_val
        return {'next_addon_price': round(next_addon, 2), 'new_stop': round(new_stop, 2),
                'n_atr20': round(n_val, 2), 'n_half': round(0.5 * n_val, 2)}

    def calc_chandelier(self, period: int = 22, mult: float = 3.0) -> Dict:
        """吊灯止损: 最近period日最高 - mult×ATR(period), 跌破即离场

        像吊灯一样每天跟着价格往上走; 波动大的科技股可放宽到5×ATR
        """
        if len(self.closes) < period:
            return {'stop': 0, 'price_vs_stop': 99, 'valid': False}
        high_n = max(self.highs[-period:])
        atr_n = self.atr22[-1] if self.atr22[-1] else (self.atr20[-1] or self.closes[-1] * 0.03)
        stop = high_n - mult * atr_n
        close = self.closes[-1]
        return {'stop': round(stop, 2),
                'price_vs_stop': round((close - stop) / atr_n, 1) if atr_n else 99,
                'valid': True}

    def calc_time_stop(self, entry_idx: int, days: int = 10) -> Dict:
        """时间止损: 买入后days个交易日未突破买入前10日高点 → 假突破离场

        entry_idx: 买入日在K线数据中的索引
        """
        n = len(self.closes)
        days_held = n - 1 - entry_idx
        if entry_idx < 0 or days_held < days:
            return {'triggered': False, 'days_held': days_held, 'reason': ''}
        max_high_since = max(self.highs[entry_idx:])          # 买入后的最高价
        max_high_before = max(self.highs[max(0, entry_idx - 9):entry_idx + 1])  # 买入前10日最高
        if max_high_since <= max_high_before:
            return {'triggered': True, 'days_held': days_held,
                    'max_high_since': round(max_high_since, 2),
                    'max_high_before': round(max_high_before, 2),
                    'reason': f'持有{days_held}日未突破买入前高点({max_high_before:.2f})'}
        return {'triggered': False, 'days_held': days_held, 'reason': ''}

    def calc_take_profit(self, entry_price: float, gain_pct: float = 15.0,
                         ratio: float = 0.25) -> Dict:
        """Zanger分批止盈: 盈利gain_pct%先卖ratio比例头寸落袋, 剩余交给吊灯止损"""
        level = entry_price * (1 + gain_pct / 100)
        return {'level_price': round(level, 2), 'gain_pct': gain_pct, 'sell_ratio': ratio}

    def check_trend_filter(self) -> Dict:
        """海龟趋势过滤: close > MA250 (默认不强制, 仅参考 — 熊市低位股机会)"""
        close = self.closes[-1]
        above_ma250 = self.ma250[-1] is not None and close > self.ma250[-1]
        return {'filter_pass': above_ma250, 'above_ma250': above_ma250,
                'ma250': round(self.ma250[-1], 2) if self.ma250[-1] else None}

    def run(self, price: float, recent_low: Optional[float] = None) -> Dict:
        sl = self.calc_stop_loss(price, recent_low)
        ps = self.calc_position(price)
        tf = self.check_trend_filter()
        return {'stop_loss': sl, 'position': ps, 'trend_filter': tf}


# ============================================================
# 综合引擎
# ============================================================

class QuantEngine:
    """三周期左峰突破 + M4 验证 + 海龟执行"""

    def __init__(self, account_size: float = 200000):
        self.account = account_size
        self.pool = {}
        self.sector_returns = {}
        self.ind_map, self.con_map = load_industry_concept_maps()
        self.mh_ind, self.mh_con = load_multihead_sectors()  # 手动导出名单(fallback)
        self.mh_ind_rt, self.mh_con_rt = None, None  # 实时计算名单(优先, 扫描时刷新)
        # v2.96 (2026-09-16): 名单里每个板块的档位 {名: '强多头'|'偏多'} — 前端勾选框按档显示用
        self.mh_ind_tier_rt, self.mh_con_tier_rt = None, None
        self.sub_to_ind = load_sub_to_ind()  # 细分→一级 行业映射(板块铁律双匹配)
        self.mh_lines = '4'  # 均线档位 (3/4/5/6线, 2026-08-05 前端切换, 扫描过滤用)

    def load_data(self, mode: str = '20', progress_callback=None):
        # 多文件池合并 (2026-08-07): 每个模式 = 多个选股池文件并集去重, 先读优先
        # 100日突破(趋势主升): 百日新高 + 趋势选股 + 寻找趋势龙 (长期方向)
        # 55日突破(趋势延续):  55日新高 + 短期趋势股 (中期趋势)
        # 20日突破(趋势反转):  20日新高 + 短期趋势股 (短期入场, v2.78 补) — 与自建池 CONDS_BY_MODE 口径对齐
        # v2.17 (2026-08-29): 文件清单/路径由数据源设置面板配置 (pool_mode_100/55/20)
        mode_files = {
            '20': list(ds.get('local', 'pool_mode_20', ['选股池/20日新高.xlsx'])),
            '55': list(ds.get('local', 'pool_mode_55',
                              ['选股池/55日新高.xlsx', '选股池/短期趋势股.xlsx'])),
            '100': list(ds.get('local', 'pool_mode_100',
                               ['选股池/百日新高.xlsx', '选股池/趋势选股.xlsx',
                                '选股池/寻找趋势龙.xlsx'])),
        }
        filenames = mode_files.get(mode, ['基础股票池.xlsx'])
        self.pool = {}
        for fname in filenames:
            sub = load_pool(fname)
            for code, info in sub.items():
                if code not in self.pool:
                    self.pool[code] = info
        if not self.pool:
            self.pool = build_pool_auto(progress_callback)
        for code, info in self.pool.items():
            # v2.207b: 原写 `code.startswith(('SZ','SH'))` ⇒ 北交所 'BZ920285' 走 else 分支保留全码,
            #   查 ind_map(裸码)永远查不到。改成统一取末 6 位。
            num = code[-6:] if len(code) == 8 else code
            info['sector'] = self.ind_map.get(num, '')
            info['concept'] = self.con_map.get(num, '')
        return len(self.pool)

    def calc_sector_returns(self):
        """板块强度: 腾讯实时 + 个股采样补充 (v2.54: 整表按池子行业集合缓存, 盘中60s/盘后3600s)"""
        if not self.pool:
            return
        key = frozenset((info.get('sector', '') or '') for info in self.pool.values())
        with _SECTOR_RET_LOCK:
            ent = _SECTOR_RET_CACHE.get(key)
            if ent and time.time() - ent[0] < _sector_rt_ttl():
                self.sector_returns = ent[1]
                return
        self.sector_returns = fetch_sector_realtime()
        # 补充缺失板块
        missing = set()
        for info in self.pool.values():
            sec = info.get('sector', '')
            if sec and sec not in self.sector_returns:
                missing.add(sec)
        if missing:
            sector_codes = defaultdict(list)
            for code_num, sec_name in self.ind_map.items():
                if sec_name in missing: sector_codes[sec_name].append(code_num)
            pool_format_codes = []
            code_to_sector = {}
            for sec_name, codes in sector_codes.items():
                for c in codes[:8]:
                    pc = _px(c)
                    pool_format_codes.append(pc)
                    code_to_sector[pc] = sec_name
            if pool_format_codes:
                rt = self._fetch_realtime(pool_format_codes)
                sector_changes = defaultdict(list)
                sector_leads = {}  # 记录领涨股
                for pc, sec_name in code_to_sector.items():
                    if pc in rt:
                        chg = rt[pc]['change_pct']
                        sector_changes[sec_name].append(chg)
                        if sec_name not in sector_leads or chg > sector_leads[sec_name][1]:
                            # v2.133b (2026-10-01): 顺手记下代码 (pc = 'SH600519') —— 前端"领涨股点开K线"要用;
                            #   原先只留 (名, 涨幅), 采样出来的板块于是有名无码, 点不了。
                            sector_leads[sec_name] = (rt[pc].get('name', pc), chg, pc)
                for sec_name, changes in sector_changes.items():
                    if changes:
                        lead_name, lead_pct, lead_code = sector_leads.get(sec_name, ('', 0, ''))
                        self.sector_returns[sec_name] = {'change_pct': round(sum(changes) / len(changes), 2),
                            'up_count': sum(1 for c in changes if c > 0), 'total_count': len(changes),
                            'net_flow_yi': 0, 'lead_stock': lead_name, 'lead_pct': round(lead_pct, 2),
                            'lead_code': lead_code,   # v2.133b: 与 lead_stock 同源, 保证"显示谁就点谁"
                            'zdf5': 0, 'zdf20': 0}
        if self.sector_returns:  # 只缓存完整结果 (网络失败空表不缓存)
            with _SECTOR_RET_LOCK:
                _SECTOR_RET_CACHE[key] = (time.time(), dict(self.sector_returns))

    def calc_sector_returns_all(self):
        """v2.216 (2026-10-24): 不依赖自选池的板块涨幅表。

        `calc_sector_returns()` 开头有 `if not self.pool: return` —— 它按「池子里出现了哪些
        行业」做缓存键。而 涨停突破/涨停回调/均线粘合/龙头战法 这四个模式**不建自建池**，
        self.pool 是空的 ⇒ 那句直接 return，sector_returns 一直是 {}，A股适配的「板块」
        项于是永远走兜底（正是用户 m06754 看到的 16.7% 那种假数）。

        fetch_sector_realtime() 本身就是全量 710 个行业+概念的当日等权涨幅（同花顺官方
        指数口径），不需要再按池补缺，所以这里直接取整表即可。
        """
        try:
            self.sector_returns = fetch_sector_realtime()
        except Exception:
            self.sector_returns = {}

    def scan_single(self, code: str, info: Dict, price: float, change_pct: float,
                    mode: str = '20', kline_override: Optional[List[Dict]] = None) -> Dict:
        """
        单票扫描 — 按指定周期模式
        mode: '100'=长期突破, '55'=中期突破, '20'=短期突破
        """
        if kline_override is not None:
            kline = kline_override
        else:
            kline = load_csv_kline(code)
        if len(kline) < 120:
            return self._empty(code, info, price, change_pct, '数据不足')

        period = int(mode)
        label = f'{mode}日'
        de = LeftPeakEngine(kline, period, label)
        dm = de.run()
        quality = de.quality_score()  # 突破质量6分制

        # 均线共同要求: 收盘必须站上MA21 (三个模式统一底线)
        dm['above_ma21'] = dm['trend']['above_ma21']  # 从trend提取

        # 专属逻辑
        mode_detail = {}
        active_signals = ('BREAKOUT', 'PULLBACK', 'BREAKOUT_WEAK', 'NEAR_HIGH')
        if mode == '100':
            mode_detail['type'] = '长期突破'
            mode_detail['require'] = '100日高点附近 + 均线多头(MA21>MA55) + close>MA21'
            ma_ok = dm['trend']['direction'] == 'UP'  # 已经包含 aligned + above_ma21 + ma21_up
            is_valid = dm['signal'] in active_signals and ma_ok
        elif mode == '55':
            # P3-3 fix: 只取100日左峰价, 不重建整套LeftPeakEngine (口径=构造left_peak, 已由quick_left_peak复现)
            peak100 = LeftPeakEngine.quick_left_peak(kline, 100)
            mode_detail['type'] = '中期突破'
            mode_detail['require'] = '55日高点附近 + 低于100日高点 + close>MA21'
            below_100 = dm['channel']['close'] < peak100
            is_valid = dm['signal'] in active_signals and below_100 and dm['trend']['above_ma21']
        else:  # 20
            peak55 = LeftPeakEngine.quick_left_peak(kline, 55)
            mode_detail['type'] = '短期突破'
            mode_detail['require'] = '20日高点附近 + 低于55日高点 + close>MA21'
            below_55 = dm['channel']['close'] < peak55
            is_valid = dm['signal'] in active_signals and below_55 and dm['trend']['above_ma21']

        # v2.3 突破有效性: 近10日曾收盘站上左峰, 否则拒绝(滤掉从未站上过的NEAR_HIGH观察级干扰)
        # (云南旅游案例: 收盘从未站上4.45, 只是贴近 → 拒绝输出)
        if is_valid and dm['signal'] != 'BREAKOUT':
            peak_price = dm['channel'].get('left_peak', 0)
            if peak_price > 0:
                ever_stood = any(kline[i]['close'] >= peak_price for i in range(-11, -1))
                if not ever_stood:
                    is_valid = False

        # M4 横切验证
        m4 = AShareVerifier(kline, info, self.sector_returns).verify(change_pct, code)
        m4_pass = m4['passed']

        # 海龟执行
        tx_obj = TurtleExecutor(kline, self.account)
        tx = tx_obj.run(price, dm['channel']['recent_low'])

        # 评分: 迫近度(ATR距离, 0-8) + 量价(0-6) + 趋势(0-6) + M4(0-5) - 惩罚
        ch = dm['channel']
        gap_atr = abs(ch['dist_atr'])  # 距左峰的ATR距离(绝对值)
        # 距峰ATR越小越近: ≤0.3ATR=贴脸 8分, ≤0.7=很近 6分, ≤1.2=接近 4分, ≤2.0=可见 2分, >2=远 0分
        if gap_atr <= 0.3: pos_score = 8       # 贴脸(不到1/3个ATR)
        elif gap_atr <= 0.7: pos_score = 6     # 很近
        elif gap_atr <= 1.2: pos_score = 4     # 接近
        elif gap_atr <= 2.0: pos_score = 2     # 可见
        else: pos_score = 0

        # 量价维度: 由突破质量评分中的v_breakout+v_shrink取代 (见quality_score)
        trend = dm['trend']
        trend_score = 0
        if trend['above_ma21']: trend_score += 2
        if trend['aligned']: trend_score += 2
        if trend['ma21_rising']: trend_score += 1
        if trend['above_ma250']: trend_score += 1

        # v2.12 (2026-08-15): 取消涨跌停扣分 — 板块趋势支撑下涨停突破左峰=强确认(连板概率高)
        # M4 A股适配: 板块共振2分 + 乖离率2分 = 满分4 (原含"非涨跌停1分"已取消)
        m4_score = (2 if m4['sector_ok'] else 0) + (2 if m4['bias_ok'] else 0)

        score = round(pos_score + trend_score + m4_score + quality['total'])
        # 惩罚项
        penalty = 0
        if m4['bias_val'] > 12: penalty += 3  # BIAS过度拉伸
        # v2.12: 原"涨跌停-5"惩罚已取消 (涨停突破=强信号)
        score -= penalty
        score = min(30, max(0, score))

        score_detail = {
            'pos': pos_score, 'vol': quality['vol'], 'trend': trend_score,
            'm4': round(m4_score), 'quality': quality['total'],
            'q_price': quality['price'], 'q_vol': quality['vol'],
            'v_breakout': quality.get('v_breakout', 0),
            'v_shrink': quality.get('v_shrink', 0),
            'penalty': penalty, 'total': score
        }

        # 基础评级 (满分30: 迫近8+量价6+趋势6+M45+质量6)
        if score >= 24: rec = '强劲买入'
        elif score >= 18: rec = '推荐买入'
        elif score >= 12: rec = '试错买入'
        elif score >= 6: rec = '保持关注'
        else: rec = '回避'

        # v2.12 加分制: 加分只影响排序, 评级仍按基础分 score
        qv = quality.get('v_breakout', 0)
        vol_ratio_bt = 0.0
        if qv:
            # 突破放量倍数来自 quality_score 内部 (此处用当日量比近似表达)
            vol_ratio_bt = ch.get('volume_ratio', 0)
        shrink = quality.get('v_shrink', 0)
        shrink_ratio = 0.0
        if shrink and qv:
            shrink_ratio = 0.3 if shrink >= 3 else (0.5 if shrink >= 2 else 0.7)
        bn = self._calc_bonus(code, info, price,
                              sec_pct=0, sec_rank=0,
                              vol_ratio=vol_ratio_bt, rps=getattr(self, 'rps_cache', {}).get(code, 0),
                              limit_today=(change_pct >= 9.5 and not code.startswith(('SZ300','SZ301','SZ302','SH688','SH689'))) or (change_pct >= 19.5 and code.startswith(('SZ300','SZ301','SZ302','SH688','SH689'))),
                              breakout_vol=vol_ratio_bt,
                              shrink_ratio=shrink_ratio,
                              kline=kline)
        score_with_bonus = score + bn['bonus']

        num = code[2:] if code.startswith(('SZ', 'SH')) else code

        # 交易纪律 (v2.1: 0.5N加仓/吊灯止损/分批止盈 — 供模拟盘买入时初始化持仓参数)
        addon = tx_obj.calc_addon(price, 1)
        chandelier = tx_obj.calc_chandelier()
        tp = tx_obj.calc_take_profit(price)
        discipline = {
            'addon_price': addon['next_addon_price'],      # 首档加仓位 = 买价+0.5N
            'chandelier_stop': chandelier['stop'],         # 吊灯止损线
            'take_profit_at': tp['level_price'],           # 分批止盈价 = 买价×1.15
            'time_stop_days': 10,                          # 时间止损天数
            'n_half': addon['n_half'],                     # 0.5N
        }

        # v2.36 (2026-09-03): 量价同步率 — 纯展示列(不参与评分), 最近50日涨放量/跌缩量占比
        sync_pct = calc_vol_price_sync(kline)
        # v2.37 (2026-09-03): 实体量质 — 纯展示列, 量能配实体推进打分(0-100), 见 calc_entity_quality
        body_pct = calc_entity_quality(kline)

        return {
            'code': num, 'name': info.get('name', ''),
            'price': round(price, 2), 'change_pct': round(change_pct, 2),
            'sector': info.get('sector', ''), 'concept': info.get('concept', ''),
            'sync_pct': sync_pct, 'body_pct': body_pct,
            'market_cap': info.get('market_cap_str', ''),
            'mode': mode, 'mode_detail': mode_detail,
            'breakout': dm, 'm4_verify': m4, 'tx_execute': tx,
            'discipline': discipline,
            'is_valid': is_valid,
            'consensus': {
                'score': score_with_bonus, 'recommendation': rec,
                'base_score': score, 'bonus': bn['bonus'],
                'bonus_detail': bn['detail'], 'bonus_notes': bn['notes'],
                'm4_passed': m4_pass, 'm4_checks': m4['checks'],
                'signal': dm['signal'],
                'signal_strength': 'STRONG' if score >= 13 else ('MEDIUM' if score >= 8 else 'WEAK'),
                'score_detail': score_detail,
            }
        }

    def scan_watch_recent(self, code: str, info: Dict, price: float, change_pct: float,
                          mode: str = '100') -> Dict:
        """v2.54 自选分组口径 — 与 scan_single 同逻辑, 但标注左峰一律用"最近孤立峰" (图峰②)

        用户决策 (2026-09-05): 进入自选后无论模式档位, 左峰/距峰/仓位距离列 = 最近孤立峰
        (find_recent_two_peaks.peak2, 与K线图蓝色虚线峰②同口径, 操作锚点);
        评分/评级/信号逻辑仍按 mode 档位 (窗口最高峰) 不变 — 只改标注, 不改决策。
        无孤立峰 (单边极端行情) → 原样返回窗口口径, 不篡改。
        """
        kline = load_csv_kline(code)
        if len(kline) < 120:
            return self._empty(code, info, price, change_pct, '数据不足')
        r = self.scan_single(code, info, price, change_pct, mode=mode, kline_override=kline)
        dm = r.get('breakout')
        ch = (dm or {}).get('channel') if isinstance(dm, dict) else None
        if not isinstance(ch, dict):
            return r
        pk = find_recent_two_peaks([float(x.get('high', 0) or 0) for x in kline])['peak2']
        if not pk:
            return r
        pk_price = float(pk['price'])
        close = float(r.get('price') or ch.get('close') or price)
        atr = float(dm.get('atr') or 0.0)
        ch['left_peak'] = round(pk_price, 2)
        if atr > 0:
            ch['dist_atr'] = round((close - pk_price) / atr, 2)
        rl = ch.get('recent_low')
        if rl is not None and pk_price > rl:
            ch['position_pct'] = round((close - float(rl)) / (pk_price - float(rl)) * 100, 1)
        return r

    def _parallel_map(self, items, worker, total, progress_callback=None, workers=6):
        """v2.14 (2026-08-27): 逐票并行扫描

        worker(item) -> result | None; 线程池内只读 self.sector_returns/account/rps_cache,
        写操作仅限局部变量, 故并发安全。进度回调每20只报一次 (串行时每只都报)。
        ex.map 保序返回 → 结果顺序与串行版一致。
        """
        from concurrent.futures import ThreadPoolExecutor
        out = []
        done = 0
        with ThreadPoolExecutor(max_workers=workers) as ex:
            for r in ex.map(worker, items):
                if r is not None:
                    out.append(r)
                done += 1
                if progress_callback and done % 20 == 0:
                    progress_callback(done, total)
        if progress_callback:
            progress_callback(done, total)
        return out

    def scan(self, mode: str = '20', progress_callback=None) -> List[Dict]:
        """按指定模式扫描选股池"""
        if not self.pool: self.load_data()
        # v2.3: 刷新实时多头名单(行业趋势5/10/21/55判定), 替代手动导出快照
        # v2.96 (2026-09-16): 名单由「有强选强裁剪」改为 强多头∪偏多 并集 + 附档位表,
        #   显示哪几档交给前端 4 个勾选框 (用户 2026-09-16 定)。后端下限不变: 只有 强多头/偏多 进名单。
        self.mh_ind_rt, self.mh_con_rt, self.mh_ind_tier_rt, self.mh_con_tier_rt = \
            load_realtime_multihead_union(ma_periods=LINES_MAP.get(self.mh_lines, (5, 10, 21, 55)))
        self.calc_sector_returns()
        codes = list(self.pool.keys())
        rt_data = self._fetch_realtime(codes)
        # P2-8 fix: 实时价接入板块MA判定 (analyze_sector_ma 需 {6位代码: 价}, 原来 _rt_prices_for_ma 从未赋值→恒用昨日收盘口径)
        self._rt_prices_for_ma = {(c[2:] if c.startswith(('SZ', 'SH')) else c): v['price']
                                  for c, v in rt_data.items() if v.get('price', 0) > 0}
        def _worker(code):
            if code not in rt_data: return None
            rt = rt_data[code]
            price = rt.get('price', 0)
            change_pct = rt.get('change_pct', 0)
            if price <= 0: return None
            try:
                r = self.scan_single(code, self.pool[code], price, change_pct, mode=mode)
                if r.get('is_valid', True):
                    # v2.31: 自建池 name 为空 → 用实时行情名称补全 (文件池 name 非空则保留)
                    rt_name = rt.get('name', '')
                    if rt_name and not r.get('name'):
                        r['name'] = rt_name
                    return r
            except Exception:
                pass
            return None
        results = self._parallel_map(codes, _worker, len(codes), progress_callback)
        results = self._filter_multihead(results)
        results.sort(key=lambda r: (-r['consensus']['score']))
        return results

    def _filter_multihead(self, results, tag_only: bool = False):
        """选股输出限制: 只保留属于多头行业或概念名单的成分股

        v2.3: 优先用实时计算名单(mh_ind_rt, 行业趋势5/10/21/55多头判定),
        未计算时 fallback 手动导出的同花顺名单(mh_ind)

        v2.138 (2026-10-13): 加 tag_only —— True = **只标注档位不丢行**。
          给「均线粘合」用: 那个模式的池子只由均线决定 (用户 2026-10-13 定),
          档位筛选交给前端两个勾选框; 但它仍需要 ind_t/con_t 才能筛,
          于是复用这里同一套口径 (别名 / 细分→一级 / 取最强档) 把字段写上。
          默认 False ⇒ 趋势三模式的行为**逐字不变**。
        """
        # 同花顺↔通达信别名映射 (两套命名体系差异)
        ALIASES = {
            '饮料制造': ['白酒', '啤酒', '其他酒类'],
            '酿酒': ['白酒', '啤酒', '其他酒类'],  # 未改名股票兼容
            '全国性银行': ['银行', '国有大型银行', '股份制银行'],
            '地方性银行': ['银行', '农商行', '城商行'],
            '航运港口': ['港口航运'],
            '公路铁路': ['公路铁路运输'],
        }
        mh_ind = self.mh_ind_rt if self.mh_ind_rt is not None else self.mh_ind
        mh_con = self.mh_con_rt if self.mh_con_rt is not None else self.mh_con
        # v2.4 (2026-08-05): 实时名单已计算时, 空名单 = 严格过滤(无板块满足当前档位→0只)
        # 仅当名单从未计算且手动兜底也为空才不过滤 (旧行为保留)
        computed = self.mh_ind_rt is not None or self.mh_con_rt is not None
        if not computed and not mh_ind and not mh_con:
            return results
        ind_tier = self.mh_ind_tier_rt or {}
        con_tier = self.mh_con_tier_rt or {}
        filtered = []
        for r in results:
            sec = r.get('sector', '')
            con = r.get('concept', '')
            matched = False
            ind_t, con_t = '', ''   # v2.96: 该行在两维各自命中的档位 ('强多头'/'偏多'/''=没命中)
            # 行业匹配 (精确 + 别名 + 细分→一级双匹配: 细分'国有大型银行'→一级'银行')
            # ⚠ v2.95 (2026-09-16) 删掉了一句**方向反了**的子串匹配
            #   `any(mh_name in x or x in mh_name for x in sec_levels)` —— x 是个股的**一级行业**
            #   (如 '银行')、mh_name 是多头名单里的**细分**(如 '国有大型银行')，于是
            #       '银行' in '国有大型银行' → True
            #   ⇒ 城商行/农商行/股份制银行 全被这**一个**细分连坐放行。实测 9/15 落盘：
            #   三个趋势模式 151 行里 62 行(41%)是这么蒙进来的，档位全是偏空 —— 违反板块铁律。
            #   sec_levels 本身已是 {细分}∪{一级}，精确命中就完整覆盖了"双匹配"。
            aliases = ALIASES.get(sec, [])
            sec_levels = set([sec]) | self.sub_to_ind.get(sec, set())
            # v2.99 (2026-09-16): 不再 break 在**第一个**命中上 —— mh_ind 是 set, 迭代顺序随
            #   PYTHONHASHSEED 逐进程变; 而 sec_levels 同时含**细分和自己的一级**(印制电路板→元件),
            #   两个名字都在多头名单里时, 谁先被遍历到就取谁的档 —— 实测同一行 PYTHONHASHSEED=0..3
            #   得 '强多头', 4..5 得 '偏多'。档位现在要发到前端驱动勾选框, 必须确定: 改成「取命中的
            #   **最强**档」, 与下面概念那半段 (con_t 本来就取最强) 同一套语义。
            #   ⚠ 只改 ind_t 的**取值**: matched 仍是任一命中即 True, 入选集合一字不变。
            for mh_name in mh_ind:
                if mh_name in sec_levels or mh_name in aliases:
                    matched = True
                    t = ind_tier.get(mh_name, '')
                    if t == '强多头' or not ind_t:
                        ind_t = t
                    if ind_t == '强多头':
                        break        # 已是最强档, 不必再扫
            # 概念匹配 (精确, 已统一命名; 一行挂十几个概念, 取命中的**最强**档位)
            if con:
                for c in con.split('|'):
                    c = c.strip()
                    if c and c in mh_con:
                        matched = True
                        t = con_tier.get(c, '')
                        if t == '强多头' or not con_t:
                            con_t = t
            if matched:
                # v2.96 (2026-09-16): 把命中的档位写回行里 —— 前端勾选框按档显示, 后端不再硬裁剪。
                # 空串 = 该维度没命中 (只靠另一维进来的), 前端据此判断该行显不显示。
                r['ind_t'] = ind_t
                r['con_t'] = con_t
                filtered.append(r)
            elif tag_only:
                # v2.138: 没命中也要写空串 (前端靠 '' 判断"只从另一维进来"/"两维都没沾边"),
                #   ⚠ 而且必须照样 append —— tag_only 的语义是"一个都不丢", 只是把档位写上。
                r['ind_t'] = ind_t
                r['con_t'] = con_t
                filtered.append(r)
        return filtered

    def _prep_rps(self, codes: List[str]) -> Dict[str, int]:
        """预计算池内 RPS 相对强度 (120日涨幅全池百分位)

        Minervini: RS>70理想90+; 此处池内排名: 前10%=3, 前25%=2, 前50%=1, 其余0
        数据不足120日的记0 (次新股)
        """
        gains = {}
        for code in codes:
            fpath = os.path.join(KLINE_DIR, f'{code}.csv')
            if not os.path.exists(fpath): continue
            tail = _read_csv_tail(fpath, 300)
            if len(tail) < 120: continue
            g = (tail[-1]['close'] - tail[-120]['close']) / tail[-120]['close'] * 100
            gains[code] = g
        if not gains: return {}
        sorted_gains = sorted(gains.values(), reverse=True)
        n = len(sorted_gains)
        # 值→百分位映射 (O(n log n) 替代 O(n²) 排名)
        pct_map = {}
        for i, v in enumerate(sorted_gains):
            if v not in pct_map:
                pct_map[v] = i / n * 100
        return {code: (3 if pct_map[g] < 10 else (2 if pct_map[g] < 25 else (1 if pct_map[g] < 50 else 0)))
                for code, g in gains.items()}

    def _load_market_regime(self) -> str:
        """大盘状态: 沪/深指数站上MA21=进攻, 仅一方=结构, 均跌破=防御

        修复死代码 _check_index_ma21 (原缓存从未赋值), 用腾讯K线实时计算
        """
        try:
            ms = MarketSentiment()
            regimes = ms.classify_index_regime()
            sh_ok = regimes.get('sh000001', {}).get('close_vs_ma21', -99) > 0
            sz_ok = regimes.get('sz399001', {}).get('close_vs_ma21', -99) > 0
            if sh_ok and sz_ok: return '进攻'
            if sh_ok or sz_ok: return '结构'
            return '防御'
        except Exception:
            return '未知'

    def scan_limit_breakout(self, progress_callback=None,
                            realtime_prices: Optional[Dict] = None) -> List[Dict]:
        """涨停突破 v2.66 (2026-09-11): 涨停价突破左峰 → 博连板

        用户定案流程 (原「资金跟随/五灯」模式彻底改造):
          ① 建池 = 当日涨停池 (akshare 东财, 与 MCP get_limit_up_down 同源;
             失败自动降级同花顺快照识别, 降级状态记 self.limit_source)
          ② 筛行业 = 所属行业档位 ∈ {偏多, 强多头}; **概念不参与筛选**
          ③ 判峰 = 涨停价突破 最近峰/55日峰/百日峰 哪一档, 取最高级别 (见 peak_period)
          ④ 「峰期」列兼排序键 = 100/55/20, 显示为 百日峰/55日峰/最近峰

        旧五灯/执行灯/自建池/RPS 规则全部舍弃 (用户 2026-09-11 定案)。
        不限制主板 — 沪/深/科/创由前端股票列表上方勾选框过滤 (applyBoardFilter),
        引擎不砍 (用户: "我用股票列表上面的按钮来筛选即可")。

        止损口径: 被突破的那个峰价 — 跌回峰下 = 突破失败, 买入逻辑破坏即离场。
        """
        from quant_terminal import dragon as _dragon
        try:
            limit_rows = _dragon.fetch_limit_pool()
        except Exception as e:
            self.limit_source = {'provider': '', 'degraded': True,
                                 'reason': f'涨停池拉取失败: {str(e)[:60]}'}
            self.breakout_pool_size = 0
            return []
        self.limit_source = dict(_dragon.LIMIT_SOURCE)
        self.breakout_pool_size = len(limit_rows)
        if not limit_rows:
            return []

        # ② 行业档位 (与行业趋势面板同口径; 概念不参与)
        ma_periods = LINES_MAP.get(self.mh_lines, (5, 10, 21, 55))
        ind_status = load_industry_status(ma_periods, realtime_prices)

        cand = []
        for r in limit_rows:
            num = str(r.get('code') or '')
            if len(num) != 6 or not num.isdigit():
                continue
            sec = self.ind_map.get(num, '')
            st = ind_status.get(sec, '')
            if st not in ('偏多', '强多头'):
                continue                      # 行业非偏多/强多头 → 剔除 (含行业未收录)
            kpath = os.path.join(KLINE_DIR, f'{_px(num)}.csv')
            if not os.path.exists(kpath):
                continue                      # 无本地K线 (北交所等) → 无法判峰
            cand.append((num, sec, st, kpath, r))
        self.breakout_cand_size = len(cand)
        if not cand:
            return []

        def _worker(item):
            num, sec, st, kpath, lr = item
            try:
                kline = _read_csv_tail(kpath, 300)
                if len(kline) < 26:
                    return None               # 连最近峰窗口都不够 → 不判
                price = float(lr.get('price') or 0)
                if price <= 0:
                    return None
                pk = peak_period(kline, price)
                if not pk:
                    return None               # 三档都没突破 → 本模式不入选
                period, peak, dist = pk
                _atr = Indicators.atr(kline, 20)          # 返回整条 ATR 序列
                atr20 = (_atr[-1] or 0.0) if _atr else 0.0
                flow_mv = float(lr.get('flow_mv') or 0)
                return {
                    'code': num, 'name': lr.get('name') or num,
                    'price': round(price, 2),
                    'change_pct': round(float(lr.get('pct') or 0), 2),
                    'sector': sec, 'concept': self.con_map.get(num, ''),
                    'sector_status': st,
                    'market_cap': f'{flow_mv / 1e8:.0f}亿' if flow_mv > 0 else '',
                    'peak_period': period, 'peak_price': round(peak, 2),
                    'dist_pct': dist,
                    'gap_atr': round(abs(price - peak) / atr20, 2) if atr20 else 0.0,
                    'atr20': round(atr20, 3),      # v2.77 (2026-09-12): 供前端盘中按最新价重算 距峰/距峰ATR
                    'stop_loss': round(peak, 2),   # 跌回峰下 = 突破失败
                    'sync_pct': calc_vol_price_sync(kline),
                    'body_pct': calc_entity_quality(kline),
                    'limit': lr,                   # 涨停池原始行 (连板/封板资金/换手/炸板/首封时间)
                    # v2.216: A股适配 (与趋势三模式同口径) — 原先这个模式不算, 前端显示 --
                    'm4_verify': _m4_for(kline, sec, self.con_map.get(num, ''),
                                         float(lr.get('pct') or 0), num, self.sector_returns),
                    'mode': 'ai', 'mode_detail': '涨停突破',
                    'is_valid': True,
                    'consensus': {
                        # 评分即峰期 (100/55/20) — 兼作排序键, 前端「峰期」列映射成文字
                        'score': period, 'base_score': period, 'bonus': 0,
                        'bonus_detail': {}, 'bonus_notes': [],
                        'recommendation': PEAK_LABELS.get(period, '') + '突破',
                        'signal': '涨停突破',
                        'signal_strength': 'STRONG' if period == 100 else ('MEDIUM' if period == 55 else 'WEAK'),
                        'score_detail': {}, 'm4_passed': 0, 'm4_checks': [],
                    },
                }
            except Exception:
                return None

        results = self._parallel_map(cand, _worker, len(cand), progress_callback)
        # 峰期降序 (百日峰优先), 同档按距峰% 升序 (贴峰者优先, 追高者靠后)
        results.sort(key=lambda r: (-r['consensus']['score'], r['dist_pct']))
        return results

    def scan_ai(self, progress_callback=None) -> List[Dict]:
        """[v2.66 已废止] 原资金跟随/五灯选股 — 2026-09-11 用户定案改造为 scan_limit_breakout。
        保留函数体只是防止外部遗留引用报错, 终端「涨停突破」按钮走 scan_limit_breakout。"""
        return self.scan_limit_breakout(progress_callback=progress_callback)

    # ============ 龙头战法 v2.16 (2026-08-27): 连板妖龙筛选 + 龙回头低吸 ============
    def scan_dragon(self, progress_callback=None) -> List[Dict]:
        """龙头战法: 妖龙榜(今日涨停主板妖龙) + 龙回头池(妖龙回调低吸)
        数据源: akshare涨停池(与MCP同源) + 本地K线 + 新浪实时(名字/价)
        铁律: 龙头仅限主板; 创/科/北涨停=助攻因子; 回撤>30%一票否决;
              龙回头池必须板块多头(板块铁律)
        """
        import quant_terminal.dragon as dragon
        # 1. 涨停池 + 每日落盘
        rows = dragon.fetch_limit_pool()
        if not rows:
            raise RuntimeError('涨停池获取失败 (akshare stock_zt_pool_em)')
        dragon.save_limit_history(rows)
        heat = dragon.sector_heat(rows)

        # 2. 妖龙榜: 今日涨停主板票妖性评分
        main_rows = [r for r in rows if r['board'] == '主板']

        # v2.16.1: 预计算全部涨停池票的20日涨停史 (板块成员悬停显示: 连板梯队判断)
        zt_n20 = {}
        for r in rows:
            try:
                _p = os.path.join(KLINE_DIR, ('SZ' if r['code'][0] in '03' else 'SH') + r['code'] + '.csv')
                k = _read_csv_tail(_p, 120)
                zt_n20[r['code']] = dragon.count_limit20(k, r['code']) if len(k) >= 60 else 0
            except Exception:
                zt_n20[r['code']] = 0

        def _wd(r):
            try:
                kline = load_csv_kline(r['code'])
                if len(kline) < 60:
                    return None
                score, det = dragon.score_dragon(r, kline)
                h = heat.get(r['industry'], {})
                if h.get('zt_total', 0) >= 3:
                    score = min(100, score + 10)
                    det.append(f"板块{h['zt_total']}家")
                is_rot = r['zhaban'] > 5 or r['turnover'] > 35
                # v2.16.1 板块成员: 同行业其他涨停票 (含双创助攻) — 前端悬停显示连板梯队
                members = []
                for m in rows:
                    if m['industry'] != r['industry'] or m['code'] == r['code']:
                        continue
                    members.append({'code': m['code'], 'name': m['name'],
                                    'board': m['board'], 'lianban': m['lianban'],
                                    'n20': zt_n20.get(m['code'], 0)})
                members.sort(key=lambda x: -x['lianban'])
                return {
                    'group': 'dragon', 'code': r['code'], 'name': r['name'],
                    'pct': r['pct'], 'price': r['price'],
                    'sector': self.ind_map.get(r['code'], r['industry']),
                    'concept': self.con_map.get(r['code'], ''),
                    # v2.216: A股适配 (妖龙榜)
                    'm4_verify': _m4_for(kline, self.ind_map.get(r['code'], r['industry']),
                                         self.con_map.get(r['code'], ''),
                                         float(r.get('pct') or 0), r['code'], self.sector_returns),
                    'score': score, 'detail': ' '.join(det),
                    'lianban': r['lianban'], 'first_time': r['first_time'],
                    'zhaban': r['zhaban'],
                    # v2.189: 同花顺涨停池带来的三件 (东财/akshare 降级时为空/0, 前端自动回落)
                    'lianban_text': r.get('lianban_text', ''),
                    'reason': r.get('reason', ''),
                    'max_seal': r.get('max_seal', 0),
                    'seal_ratio': round(r['seal_money'] / r['flow_mv'] * 100, 1) if r['flow_mv'] > 0 else 0,
                    'seal_money': r.get('seal_money', 0),
                    'turnover': r['turnover'],
                    'heat': h.get('zt_total', 0), 'rotten': is_rot,
                    'limit20': zt_n20.get(r['code'], 0),
                    'sector_members': members[:10],
                }
            except Exception:
                return None

        dragons = self._parallel_map(main_rows, _wd, len(main_rows), progress_callback)
        dragons = [d for d in dragons if d]

        # 3. 龙回头池: 全市场K线扫描妖龙回调形态
        all_files = sorted(os.listdir(KLINE_DIR))

        def _wp(fname):
            if not fname.endswith('.csv'):
                return None
            code = fname[:-4]
            if not (code.startswith(('SZ', 'SH')) and dragon.board_of(code[2:]) == '主板'):
                return None
            try:
                fpath = os.path.join(KLINE_DIR, fname)
                tail = _read_csv_tail(fpath, 120)
                if len(tail) < 90:
                    return None
                for r in tail:
                    r['code'] = code  # 注入代码 (check_pullback按板块阈值判涨停)
                pb = dragon.check_pullback(tail)
                if not pb:
                    return None
                # v2.216: 把 tail 一起带出去 —— 外面算 A股适配要 K 线, 不能在 _wp 外面用 tail
                return (code, pb, tail)
            except Exception:
                return None

        hits = self._parallel_map(all_files, _wp, len(all_files), progress_callback)
        hits = [h for h in hits if h]
        # 4. 名字+实时价 (新浪批量, 龙回头池票数少一次搞定)
        rt = self._fetch_realtime([h[0] for h in hits]) if hits else {}
        # 5. 板块铁律过滤 + 评分
        sec_map = self._ma_status_map('sector')
        pullbacks = []
        for code, pb, tail in hits:
            num = code[2:]
            sec = self.ind_map.get(num, '')
            sector_ok = sec in sec_map and sec_map[sec].get('status', '')
            score, det = dragon.score_pullback(pb, sector_ok)
            info = rt.get(code, {})
            pullbacks.append({
                'group': 'pullback', 'code': num, 'name': info.get('name', num),
                'pct': info.get('change_pct', 0), 'price': info.get('price', pb['price']),
                'sector': sec, 'concept': self.con_map.get(num, ''),
                # v2.216: A股适配 (龙回头池) — 用 tail(120根) 算, 与妖龙榜同口径
                'm4_verify': _m4_for(tail, sec, self.con_map.get(num, ''),
                                     float(info.get('change_pct') or 0), num,
                                     self.sector_returns),
                'score': score, 'detail': ' '.join(det),
                'peak': pb['peak'], 'peak_date': pb['peak_date'],
                'pullback_pct': pb['pullback_pct'], 'days_since': pb['days_since'],
                'shrink': pb['shrink'], 'stable_shrink': pb['stable_shrink'],
                'ma10': pb['ma10'], 'ma21': pb['ma21'],
                'signal': pb['signal'], 'stop_loss': pb['stop_loss'],
                'limit20': pb['limit20'], 'sector_ok': sector_ok,
            })
        pullbacks.sort(key=lambda x: -x['score'])
        dragons.sort(key=lambda x: -x['score'])
        return dragons + pullbacks

    # ---- v2.12 加分制 (2026-08-15): 加分项只影响排序, 不影响基础分级评级 ----
    def _ma_status_map(self, kind: str) -> Dict[str, Dict]:
        """构建 行业/概念 名称 → {bullish_pct, status} 映射 (实例级缓存, 每次扫描只算一次)

        kind: 'sector' 或 'concept'
        红强多头=占比>=70, 黄强多头=40~70, 偏多=<40 (与前端 loadSectorMA 着色一致)
        """
        cache_key = 'ma_map_' + kind
        cached = getattr(self, cache_key, None)
        if cached is not None:
            return cached
        ma_periods = LINES_MAP.get(getattr(self, 'mh_lines', '4'), (5, 10, 21, 55))
        # v2.14 (2026-08-27): 盘后直接读 trend_data.json (bullish_pct/status全在落盘文件里),
        # 不碰板块MA计算 — 预热未完成/缓存被清也能秒级 (原: 冷缓存12.5s/档×2 + 6线程竞争 → 100秒)
        if not _is_trade_window():
            _d = _load_trend_data()
            _key = _trend_key(ma_periods)
            if _d and _key in _d.get('lines', {}):
                rows = _d['lines'][_key]['sector' if kind == 'sector' else 'concept']
                _out = {}
                _fi = defaultdict(list)
                for r in rows:
                    _out[r['name']] = {'bullish_pct': r.get('bullish_pct', 0),
                                       'status': r.get('status', '')}
                    if r['name']:
                        _fi[r['name'][0]].append(r['name'])
                setattr(self, cache_key, _out)
                setattr(self, cache_key.replace('ma_map', 'ma_first'), _fi)
                return _out
        rt = getattr(self, '_rt_prices_for_ma', None)
        if rt is not None and not _is_trade_window():
            rt = None
        if kind == 'sector':
            ind_map, _ = load_industry_concept_maps()
            rows = analyze_sector_ma(ind_map, ma_periods, realtime_prices=rt)
        else:
            _, con_map = load_industry_concept_maps()
            rows = analyze_concept_ma(con_map, ma_periods, realtime_prices=rt)
        out = {}
        first_idx = defaultdict(list)   # P3-2: 首字索引 — 模糊匹配兜底从O(cmap)降为O(候选)
        for r in rows:
            out[r['name']] = {'bullish_pct': r.get('bullish_pct', 0),
                              'status': r.get('status', '')}
            if r['name']:
                first_idx[r['name'][0]].append(r['name'])
        setattr(self, cache_key, out)
        setattr(self, cache_key.replace('ma_map', 'ma_first'), first_idx)
        return out

    def _count_limit30(self, code: str, kline: Optional[List[Dict]] = None) -> int:
        """30日内涨停次数 (分板块阈值: 主板>=9.8%, 双创>=19.8%)
        (2026-09-11: 补 SZ302 创业板新号段)"""
        # P3-1 fix: 调用方可传入已加载的kline复用 (原来每票每次重新全量解析CSV)
        if kline is None: kline = load_csv_kline(code)
        if len(kline) < 5: return 0
        is_chi = code.startswith(('SZ300', 'SZ301', 'SZ302', 'SH688', 'SH689'))
        th = 19.8 if is_chi else 9.8
        cnt = 0
        for i in range(max(1, len(kline) - 30), len(kline)):
            prev = kline[i - 1]['close']
            cur = kline[i]['close']
            if prev > 0 and (cur - prev) / prev * 100 >= th:
                cnt += 1
        return cnt

    def _calc_bonus(self, code: str, info: Dict, price: float,
                    sec_pct: float = 0.0, sec_rank: int = 0,
                    vol_ratio: float = 0.0, rps: int = 0,
                    limit_today: bool = False, breakout_vol: float = 0.0,
                    shrink_ratio: float = 0.0,
                    kline: Optional[List[Dict]] = None) -> Dict:
        """加分制计算 — 所有加分项汇总

        返回: {'bonus': 总加分, 'detail': {项名: 分值}, 'notes': [说明]}
        原则: 加分只用于排序, 不改变推荐评级
        """
        bonus = 0
        detail = {}
        notes = []

        # 1. 30日内涨停数: 1个+1, 2个+2, 3个及以上+3 (P3-1: 复用调用方已加载的kline)
        n30 = self._count_limit30(code, kline)
        if n30 >= 3: b, note = 3, f'30日涨停x{n30}'
        elif n30 == 2: b, note = 2, f'30日涨停x{n30}'
        elif n30 == 1: b, note = 1, f'30日涨停x1'
        else: b, note = 0, ''
        if b: bonus += b; detail['涨停数'] = b; notes.append(note)

        # 2. 行业红/黄强多头: 红(>=70)+3, 黄(40~70)+2, 偏多(<40)+1
        sec = (info.get('sector') or '')
        if sec:
            smap = self._ma_status_map('sector')
            # 精确匹配 + 首字索引包含匹配 (P3-2: 全量子串循环→首字候选, 行业/概念名同源首字一致)
            srow = None
            if sec in smap: srow = smap[sec]
            else:
                for k in getattr(self, 'ma_first_sector', {}).get(sec[0], []):
                    if sec in k or k in sec: srow = smap[k]; break
            if srow:
                pct = srow.get('bullish_pct', 0)
                if pct >= 70: b = 3; note = f'行业红强{pct:.0f}%'
                elif pct >= 40: b = 2; note = f'行业黄强{pct:.0f}%'
                else: b = 1; note = f'行业偏多{pct:.0f}%'
                bonus += b; detail['行业'] = b; notes.append(note)

        # 3. 概念红/黄: 每多一个红强概念+2, 黄强+1, 偏多+0
        con = (info.get('concept') or '')
        concepts = [c.strip() for c in con.split('|') if c.strip()]
        if concepts:
            cmap = self._ma_status_map('concept')
            red_c = yellow_c = 0
            for c in concepts:
                crow = None
                if c in cmap: crow = cmap[c]
                else:
                    # P3-2: 首字索引候选, 不再全量子串循环 (概念名与token同源, 首字不同不可能匹配)
                    for k in getattr(self, 'ma_first_concept', {}).get(c[0], []):
                        if c in k or k in c: crow = cmap[k]; break
                if crow:
                    pct = crow.get('bullish_pct', 0)
                    if pct >= 70: red_c += 1
                    elif pct >= 40: yellow_c += 1
            b = red_c * 2 + yellow_c * 1
            if b:
                bonus += b; detail['概念'] = b
                note = f'概念红x{red_c}黄x{yellow_c}'
                notes.append(note)

        # 4. 行业当日涨幅前3 (+2) — 板块热度排名
        # P2-9 fix: 调用方恒传 sec_pct=0,sec_rank=0 (scan_single) 或只传 sec_pct (_score_ai)
        # → 在 _calc_bonus 内部懒计算: 板块当日涨幅 + 涨幅排名, 该项原来在所有路径恒不触发
        if sec_pct <= 0 or sec_rank <= 0:
            sec_name = info.get('sector', '')
            if sec_name and isinstance(getattr(self, 'sector_returns', None), dict):
                if sec_pct <= 0:
                    sdata = self.sector_returns.get(sec_name)
                    if isinstance(sdata, dict):
                        sec_pct = sdata.get('change_pct', 0)
                    elif isinstance(sdata, (int, float)):
                        sec_pct = sdata
                    else:
                        for sname, sd in self.sector_returns.items():
                            if isinstance(sd, dict) and (sec_name in sname or sname in sec_name):
                                sec_pct = sd.get('change_pct', 0); break
                if sec_pct > 0 and sec_rank <= 0:
                    ranked = sorted(self.sector_returns.items(),
                                    key=lambda kv: kv[1].get('change_pct', 0) if isinstance(kv[1], dict) else (kv[1] or 0),
                                    reverse=True)
                    for i, (sname, _) in enumerate(ranked):
                        if sname == sec_name or sec_name in sname or sname in sec_name:
                            sec_rank = i + 1; break
        if sec_pct > 0 and sec_rank > 0 and sec_rank <= 3:
            bonus += 2; detail['行业领涨'] = 2; notes.append(f'行业涨幅前{sec_rank}')

        # 5. RPS 前10% (+2)
        if rps >= 3:
            bonus += 2; detail['RPS'] = 2; notes.append('RPS前10%')

        # 6. 突破放量>=3倍 (+2) — 仅突破模式有值
        if breakout_vol >= 3.0:
            bonus += 2; detail['放量'] = 2; notes.append(f'突破放量x{breakout_vol:.1f}')

        # 7. 突破日涨停/近涨停 (+2)
        if limit_today:
            bonus += 2; detail['涨停突破'] = 2; notes.append('突破日涨停')

        # 8. 缩量锁仓 (缩至30%以下) (+1)
        if 0 < shrink_ratio <= 0.3:
            bonus += 1; detail['锁仓'] = 1; notes.append(f'缩量{shrink_ratio:.0%}')

        return {'bonus': bonus, 'detail': detail, 'notes': notes}

    def _score_ai(self, code: str, info: Dict, price: float, change_pct: float,
                  rps: int = 0) -> Dict:
        """五灯模型评分 (每灯0-3, 满分15; 防御市总分-2)"""
        kline = load_csv_kline(code)
        if len(kline) < 60:
            return self._empty(code, info, price, change_pct, '数据不足')
        closes = [d['close'] for d in kline]
        volumes = [d['volume'] for d in kline]
        close = closes[-1]
        ma21 = sum(closes[-21:])/21 if len(closes)>=21 else close
        ma55 = sum(closes[-55:])/55 if len(closes)>=55 else ma21
        ma120 = sum(closes[-120:])/120 if len(closes)>=120 else ma55

        # 灯1: 行业灯 — 板块实时涨幅排名打分
        sector = info.get('sector', '')
        # 搜索腾讯板块数据(精确+模糊)
        sec_pct = 0
        for sname, sdata in self.sector_returns.items():
            if isinstance(sdata, dict) and (sector in sname or sname in sector):
                sec_pct = sdata.get('change_pct', 0); break
        if sec_pct == 0 and isinstance(self.sector_returns.get(sector, 0), (int, float)):
            sec_pct = self.sector_returns.get(sector, 0)
        s1 = 3 if sec_pct >= 3 else (2 if sec_pct >= 1 else (1 if sec_pct >= -1 else 0))

        # 灯2: 技术灯 — 站上MA21 + ADX强度 + MA21斜率 + RPS相对强度 (4项取3)
        adx_v, pdi, mdi = Indicators.adx(kline, 14)
        adx = adx_v[-1] if adx_v[-1] else 0
        ma21_prev = sum(closes[-22:-1])/21 if len(closes) >= 22 else ma21
        tech = 0
        if close > ma21: tech += 1        # 站上攻击线
        if adx > 20: tech += 1            # 趋势强度足够
        if ma21 > ma21_prev: tech += 1    # MA21 向上 (斜率替代)
        if rps >= 2: tech += 1            # RPS 前25%
        s2 = min(3, tech)

        # 灯3: 注意力灯 — 20日均成交额(亿元); CSV成交量单位=手, ×100换算为股
        # (2026-08-16 修复: 原来缺×100, 全市场成交额低估100倍, 注意力灯恒0分)
        raw_amt = sum(volumes[-20:]) * close * 100 / 20 / 1e8
        s3 = 3 if raw_amt > 10 else (2 if raw_amt > 3 else (1 if raw_amt > 1 else 0))

        # 灯4: 叙事灯 — 所属概念板块实时涨幅 (v2.1: 概念数量→真实板块热度)
        concept = info.get('concept', '')
        concepts = [c.strip() for c in concept.split('|') if c.strip()]
        best_pct = 0.0
        for c in concepts:
            for sname, sdata in self.sector_returns.items():
                if isinstance(sdata, dict) and (c in sname or sname in c):
                    best_pct = max(best_pct, float(sdata.get('change_pct', 0)))
        if concepts:
            s4 = 3 if best_pct >= 3 else (2 if best_pct >= 1 else (1 if best_pct >= 0 else 0))
        else:
            s4 = 0

        # 灯5: 执行灯 — 距20日左峰距离 + M4验证通过
        high20 = max([d['high'] for d in kline[-20:]])
        dist = (close - high20) / high20 * 100 if high20 > 0 else 0
        m4 = AShareVerifier(kline, info, self.sector_returns).verify(change_pct, code)
        exec_score = 0
        if dist >= -2: exec_score += 1  # 近高点
        if m4['passed'] >= 3: exec_score += 2  # M4全过
        s5 = min(3, exec_score)

        total = s1 + s2 + s3 + s4 + s5
        # 大盘防御模式: 总分-2 并标注 (不硬性拦截, 尊重手动勾选习惯)
        market_regime = getattr(self, 'market_regime', '未知')
        regime_note = ''
        if market_regime == '防御':
            total -= 2
            regime_note = f'防御市-2'
        if total >= 12: rec = '🔥强力推荐'
        elif total >= 9: rec = '推荐'
        elif total >= 6: rec = '关注'
        else: rec = '观望'

        # v2.12 加分制: 加分只影响排序, 评级仍按基础分 total
        bn = self._calc_bonus(code, info, price,
                              sec_pct=sec_pct,
                              rps=rps,
                              kline=kline)
        total_with_bonus = total + bn['bonus']

        num = code[2:] if code.startswith(('SZ','SH')) else code
        return {'code': num, 'name': info.get('name', ''), 'price': round(price,2),
                'change_pct': round(change_pct,2),
                'sector': info.get('sector','')[:10], 'concept': concept,
                'market_cap': info.get('market_cap_str',''),
                'ai_lights': {'s1': s1, 's2': s2, 's3': s3, 's4': s4, 's5': s5,
                              'rps': rps, 'concepts': len(concepts),
                              'concept_pct': round(best_pct, 1), 'total': total,
                              'bonus': bn['bonus'], 'bonus_detail': bn['detail'],
                              'bonus_notes': bn['notes']},
                'consensus': {'score': total_with_bonus, 'recommendation': rec,
                              'base_score': total, 'bonus': bn['bonus'],
                              'bonus_detail': bn['detail'], 'bonus_notes': bn['notes'],
                              'market_regime': market_regime, 'regime_note': regime_note,
                              'signal_strength': 'STRONG' if total>=12 else ('MEDIUM' if total>=9 else 'WEAK')}}

    def scan_limit_pullback(self, progress_callback=None,
                            realtime_prices: Optional[Dict] = None) -> List[Dict]:
        """涨停回调 v2.147 (2026-10-16): 前20日涨停1~3次 → MA21不走坏 → 缩量回调不破

        用户定案流程 (原「趋势跟踪/智能选股」模式彻底改造):
          ① 建池 = **前20个交易日涨停数 ∈ {1,2,3}** 的票 (本地K线全市场粗筛, 零网络)
          ② 筛行业 = 所属行业档位 ∈ {偏多, 强多头}; **概念不参与** (与涨停突破同口径)
          ③ MA21 走平或向上 (MA21今 ≥ MA21昨, 与建池同定义)
          ④ 今天不是涨停, 且 今量 ≤ 前20日最高量 × 60% (缩量回调)
          ⑤ 「类型」列 = 不破收/不破中/不破开 (回调不破最近那个涨停的 收/中/开)
             类型**前**那列 = 1板/2板/3板 (前20日涨停个数)
          ⑥ 距涨停价 ≤ +5% — 「不破收」档现价必然在涨停价上方, 不设限会混进
             +20% 的追高票 (2026-09-11 实测18只: 4只不破收全在 +4%~+14%, 且有一只
             +23% 混在不破中里)。三档统一卡这个上限 = "必须已经回到涨停价附近"。

        排序: 类型档 (收1 > 中2 > 开3) → 距峰% 升序 (回调最深的在前)
        止损: 涨停日**开盘价** — 收盘价跌穿 = 回调形态破坏, 买入逻辑破裂即离场
        左峰: 涨停日收盘价 (涨停价)。距峰% = 现价相对它 —— 用户: "左峰我可以点开K线自己看"

        排除: 北交所 (4/8/92开头) + B股 (200/900) + ST/*ST (显式读名单, 见 load_st_codes)
        不限制主板/双创 — 前端股票列表上方勾选框过滤 (applyBoardFilter), 引擎不砍。

        ⚠ 盘中语义: 不预先拉全市场实时价 —— 先用本地K线零网络粗筛出几百只, 只对活下来
          的拉快照拼 live bar。**盘中「今量」是截至此刻的累计量**, 与「前20日最高量」
          (整天量) 比 60% 天然偏松, 早盘几乎全过 —— 这是用户口径本身的性质, 不是bug;
          越接近收盘越准。

        v2.145 (2026-10-16) 三峰展示 (只做展示, **不改任何筛选条件**):
          用户指出「左峰 = 涨停日收盘价」是错的 —— 涨停阳线不能当左峰。正确口径:
            ① 豁免区间 = 首个涨停日及其之后的整个「涨停+回调」阶段, 一律不许当峰
            ② 峰定义不变 = 前后 5 个交易日最高价都没这天高 (既有孤立峰口径)
            ③ 再扣 5 日免疫期 (防把紧挨涨停的高点当峰) ⇒ 窗口右端 = 首个涨停idx-1-5
            ④ 三档: 最近峰 / 55日峰 / 百日峰 (右端往前 55 / 100 日窗口内, 从近到远
                第一个孤立峰)
          ⇒ 结果行新增 pk_recent/pk_d55/pk_d100 三价 + pk_*_d 三个距%, 前端在板数列
            悬停浮窗里分三行展示。**筛选口径一行未动** (距峰 A/B/C 三选一待用户表态)。
        """
        intraday = _is_trade_window()
        st_codes = load_st_codes()
        name_map = load_stock_name_map()
        files = sorted(os.listdir(KLINE_DIR))
        total = len(files)
        cands: List[Dict] = []

        # ── 第一层: 全市场本地粗筛 (零网络) ──────────────────────────
        for i, fname in enumerate(files):
            if not fname.endswith('.csv'):
                continue
            if progress_callback and i % 200 == 0:
                progress_callback(i, total)
            code = fname[:-4]
            if not (code.startswith('SZ') or code.startswith('SH')):
                continue
            num = code[2:]
            if len(num) != 6 or not num.isdigit():
                continue
            if num[0] in ('4', '8') or num.startswith(('92', '200', '900')):
                continue                    # 北交所 (4xx/8xx/920) + B股 (200/900)
            if num in st_codes:
                continue                    # ST/*ST 显式排除 (阈值法挡不住, 见 load_st_codes)
            # v2.145: tail 130 → 320 (百日峰窗口需要 100+免疫5+峰定义5 ≈ 110 根,
            #   盘中还要拼一根 live bar; 130 恰在边界上会把百日峰截断)
            rows = _read_csv_tail(os.path.join(KLINE_DIR, fname), 320)
            n = len(rows)
            if n < 32:
                continue                    # 上市不足 → 无20日窗口 / 无MA21
            # 涨停窗口右端(不含): 盘后 rows[-1] 就是今天 → 前20日 = rows[n-21:n-1]
            #                       盘中 rows[-1] 还是昨天 → 前20日 = rows[n-20:n]
            win_end = n if intraday else n - 1
            win = rows[win_end - 20:win_end]
            if len(win) < 20:
                continue
            th = limit_threshold(num)
            zts = []
            for j, r in enumerate(win):
                k = win_end - 20 + j
                pv = rows[k - 1]['close']
                if pv > 0 and (r['close'] - pv) / pv * 100 >= th:
                    zts.append(k)           # 与 _count_limit30 同口径 (百分比比较)
            if not (1 <= len(zts) <= 3):
                continue                    # 前20日涨停数必须 1~3 个
            cands.append({'code': code, 'num': num, 'rows': rows,
                          'zts': zts, 'th': th})

        self.pullback_pool_size = len(cands)       # 「池」= 前20日涨停1~3次
        if not cands:
            return []

        # ── 第二层: 盘中拼实时 bar (盘后零网络) ──────────────────────
        live = {}
        if intraday:
            if progress_callback:
                progress_callback(0, len(cands))
            live = fetch_live_bars([c['code'] for c in cands])
        use_live = intraday and bool(live)
        self.pullback_live = use_live
        self.pullback_degraded = intraday and not use_live   # 盘中拉不到实时价 → 退化为读盘

        # 行业档位 (与行业趋势面板同口径; 概念不参与, 与涨停突破一致)
        ma_periods = LINES_MAP.get(self.mh_lines, (5, 10, 21, 55))
        ind_status = load_industry_status(ma_periods, realtime_prices)
        today = datetime.now().strftime('%Y-%m-%d')

        # ── 第三层: 逐只细节判定 ────────────────────────────────────
        out: List[Dict] = []
        for c in cands:
            num, rows, th = c['num'], c['rows'], c['th']
            kline = rows
            if use_live:
                lb = live.get(c['code'])
                if not lb or lb.get('open', 0) <= 0:
                    continue                # 停牌 / 无报价 → 实时bar不可信, 跳过
                kline = rows + [{'date': today,
                                 'open': lb['open'], 'close': lb['close'],
                                 'high': max(lb.get('high', 0), lb['close']),
                                 'low': (min(lb['low'], lb['close'])
                                         if lb.get('low', 0) > 0 else lb['close']),
                                 'volume': lb['volume']}]
            n = len(kline)
            li = c['zts'][-1]               # 最近那个涨停 (索引对 rows / kline 通用)
            today_bar = kline[-1]
            if li >= n - 1:
                continue                    # 涨停就是最后一天 → 没回调可言
            pv = kline[n - 2]['close']
            if pv > 0 and (today_bar['close'] - pv) / pv * 100 >= th:
                continue                    # ④ 今天自己涨停 → 不是回调, 本模式不要

            # ④ 缩量: 今量 ≤ 前20日最高量 × 60%
            wv = [r['volume'] for r in kline[-21:-1]]
            mx = max(wv) if wv else 0.0
            if mx <= 0 or today_bar['volume'] > mx * 0.6:
                continue

            closes = [r['close'] for r in kline]
            if len(closes) < 22:
                continue
            ma21 = sum(closes[-21:]) / 21
            ma21_prev = sum(closes[-22:-1]) / 21
            if ma21 < ma21_prev:
                continue                    # ③ MA21 走平或向上

            # ⑤ 类型: 收盘价不破 涨停日 收/中/开 (固定 收→中→开 顺序, 见 pullback_kind)
            zc, zo = kline[li]['close'], kline[li]['open']
            mid = kline[li - 1]['close'] * (1 + limit_span(num) / 200.0)
            pkd = pullback_kind(kline, li, mid)
            if not pkd:
                continue                    # 收盘价已破涨停日开盘价 → 形态破坏
            tier, kind_label, _lv = pkd

            price = closes[-1]
            dist = (price - zc) / zc * 100
            if dist > 5.0:
                continue                    # ⑥ 距涨停价 >+5% = 追高, 不是回调低吸

            # ② 行业档位 —— v2.146 (2026-10-16): 引擎**不再硬裁剪**, 改成只标注。
            #   用户 m04061 定案: 涨停回调也吃表头上方那 4 个档位勾选框 (与趋势三模式/均线粘合同一套),
            #   「4 个框全不勾 = 全量展示」。所以这里只算 sec_st 写进行里 (下面的 sector_status),
            #   真正的显示裁剪交给前端 applyMhFilter —— 否则「全不勾」根本没有数据可放。
            sec = self.ind_map.get(num, '')
            sec_st = ind_status.get(sec, '')

            # v2.145: 三峰 (豁免「首个涨停及其之后的涨停+回调阶段」, 只展示不改筛选)
            #   首个涨停 = c['zts'][0]; 用 kline (盘中已拼 live bar) 保证索引一致
            pk3 = pullback_three_peaks(kline, c['zts'][0])

            # v2.235 (2026-10-27 用户 m10478): N字法 —— 现价贴近「涨停大阳线开盘价」的锚
            #   口径(用户逐字定案):
            #     ① 窗口 = 前 20 根日 K (与上面「前20日涨停数」同一个 win; 停牌照常算,
            #        次新不足 20 根在更早的 n < 32 处已经剔除)
            #     ② 从最近一根往远推, 找**第一根实体达标**的涨停日, 取它的**开盘价**
            #        实体 = (收 - 开) / 开 × 100; 主板 > 5%, 双创(300/301/302/688/689) > 10%
            #        ⇒ 一字板(实体≈0)与高开秒板天然被排除, 不合格就往前推一根
            #     ③ 窗口内一根达标的都没有 → nz_open = 0, 前端不染色(但仍留在列表里)
            #   前端只用 nz_open + 当前现价重算距离%, 盘中随价即时亮灭。
            _nz_th = 10.0 if is_chi_board(num) else 5.0
            _nz_open, _nz_date, _nz_ent = 0.0, '', 0.0
            for _zi in reversed(c['zts']):
                _zb = kline[_zi]
                _zop = _zb.get('open') or 0
                if _zop <= 0:
                    continue
                _ent = (_zb['close'] - _zop) / _zop * 100.0
                if _ent > _nz_th:
                    _nz_open = round(_zop, 2)
                    _nz_date = _zb['date']
                    _nz_ent = round(_ent, 2)
                    break
            _nz_d = ((price - _nz_open) / _nz_open * 100) if _nz_open > 0 else None

            _chg = round((price - pv) / pv * 100, 2) if pv > 0 else 0.0
            out.append({
                'code': num, 'name': name_map.get(num, num),
                'price': round(price, 2),
                'change_pct': _chg,
                'sector': sec, 'concept': self.con_map.get(num, ''),
                'sector_status': sec_st, 'market_cap': '',
                # v2.216: A股适配 (涨停回调)
                'm4_verify': _m4_for(kline, sec, self.con_map.get(num, ''),
                                     _chg, num, self.sector_returns),
                # 「类型」列 + 「类型」前那列 (1板/2板)
                'kind': kind_label, 'kind_tier': tier,
                'zt_cnt': len(c['zts']),
                'zt_date': kline[li]['date'],
                'zt_close': round(zc, 2), 'zt_mid': round(mid, 2),
                'zt_open': round(zo, 2),
                'left_peak': round(zc, 2),          # 左峰 = 涨停日收盘价(涨停价) [v2.145: 仅排序/展示沿用, 不再当"左峰"语义]
                'dist_pct': round(dist, 1),
                # v2.145: 三峰 (豁免涨停+回调阶段, 从近到远孤立峰) — 前端板数列悬停分三行展示
                #   注: left_peak/dist_pct 仍是「距涨停价」口径, 与三峰是两套东西
                'pk_recent': pk3['recent'][1] if pk3['recent'] else 0,
                'pk_recent_d': round((price - pk3['recent'][1]) / pk3['recent'][1] * 100, 1)
                               if pk3['recent'] else 0,
                'pk_d55': pk3['d55'][1] if pk3['d55'] else 0,
                'pk_d55_d': round((price - pk3['d55'][1]) / pk3['d55'][1] * 100, 1)
                            if pk3['d55'] else 0,
                'pk_d100': pk3['d100'][1] if pk3['d100'] else 0,
                'pk_d100_d': round((price - pk3['d100'][1]) / pk3['d100'][1] * 100, 1)
                             if pk3['d100'] else 0,
                'vol_ratio': round(today_bar['volume'] / mx * 100, 0),  # 占前20日最高量%
                'stop_loss': round(zo, 2),          # 跌穿涨停日开盘价 = 形态破坏
                # v2.235: N字法锚 (前20根K线内从近往远第一根实体达标的涨停日开盘价)
                'nz_open': _nz_open,                # 0 = 窗口内无达标涨停 → 不染色
                'nz_date': _nz_date,
                'nz_ent': _nz_ent,                  # 该涨停日实体%
                'nz_pct': (None if _nz_d is None else round(_nz_d, 2)),
                'nz_ok': (1 if (_nz_d is not None and 0.0 <= _nz_d <= 1.0) else 0),
                'sync_pct': calc_vol_price_sync(kline),
                'body_pct': calc_entity_quality(kline),
                'mode': 'smart', 'mode_detail': '涨停回调',
                'is_valid': True,
                'consensus': {
                    # 评分即类型档 (收1/中2/开3) — 兼作排序键, 前端「类型」列映射成文字
                    'score': tier, 'base_score': tier, 'bonus': 0,
                    'bonus_detail': {}, 'bonus_notes': [],
                    'recommendation': kind_label,
                    'signal': '涨停回调',
                    'signal_strength': ('STRONG' if tier == 1 else
                                        ('MEDIUM' if tier == 2 else 'WEAK')),
                    'score_detail': {}, 'm4_passed': 0, 'm4_checks': [],
                },
            })

        self.pullback_cand_size = len(out)
        # v2.146 (2026-10-16): 档位标注 —— 与 scan_ma_glue 完全同一套 (复用 _filter_multihead 的
        #   别名 / 细分→一级 / 取最强档 口径), tag_only=True ⇒ 只写 r['ind_t']/r['con_t'], 一行都不丢。
        #   前端那 4 个勾选框 (行业强多/偏多 + 概念强多/偏多) 靠这两个字段筛选;
        #   整段 try 兜住: 板块数据缺失时宁可不标注, 也绝不能让选股本身失败 (缺键=该维不参与, 不筛空)。
        try:
            self.mh_ind_rt, self.mh_con_rt, self.mh_ind_tier_rt, self.mh_con_tier_rt = \
                load_realtime_multihead_union(ma_periods=ma_periods)
            out = self._filter_multihead(out, tag_only=True)
        except Exception as e:
            print(f'[smart] 档位标注失败(不影响涨停回调选股): {e!r}', file=sys.stderr)
        # 类型档 (收1 > 中2 > 开3) → 距峰% 升序 (回调最深的在前)
        out.sort(key=lambda r: (r['consensus']['score'], r['dist_pct']))
        return out

    def scan_smart(self, progress_callback=None, realtime_prices=None):
        """[v2.67 已废止] 原趋势跟踪/智能选股 — 2026-09-11 用户定案改造为 scan_limit_pullback。
        保留函数体只是防止外部遗留引用报错, 终端「涨停回调」按钮走 scan_limit_pullback。"""
        return self.scan_limit_pullback(progress_callback=progress_callback,
                                        realtime_prices=realtime_prices)

    # ═══ 均线粘合 v2.138 — 用户定稿口径（v2.142/2.144 调过上下限）═══
    # 建池 现价 > MA5 > MA10 > MA21（三线多头排列；v2.142 起 MA55 完全退出选股因子）
    # 下限 现价−MA5 ≥1% / −MA10 ≥2% / −MA21 ≥3%（价距下限：价格已脱离均线簇）
    # 上限 现价−MA5 ≤5% / −MA10 ≤7% / −MA21 ≤10%；MA5−MA10 ≤4% / MA10−MA21 ≤4%（MA5 与 MA21 差距不设限）
    # 斜率 MA5/MA10/MA21 三线全部「走平或向上」(5 日变化率 ≥0%) —— 硬条件
    # 排序 粘合度 = max(MA5−MA10%, MA10−MA21%) 升序（越粘越靠前）
    # 思路 要的是「多头排列刚成型、还没大涨」那一刻，不是「已经涨过一段」。回测(39 个月/月末取样/持有 20 日)：
    #      基准 5123 只中位 −0.42% | 本规则 142 只中位 +0.39% | 加放量 ≥1.2 倍 30 只中位 +1.40%
    #      ⇒ 放量不是硬条件，是前端勾选框(默认不勾)。不做评分；行业/概念档位走通用 4 框筛选器。
    # 排除 北交所(4/8/92 开头) + B股(200/900) + ST/*ST —— 与 scan_limit_pullback 同口径
    GLUE_P5, GLUE_P10, GLUE_P21 = 5.0, 7.0, 10.0
    # v2.142: 价距下限 — 现价必须已经抬离均线簇 (用户口径「差距过小趋势没走出来」)
    GLUE_L5, GLUE_L10, GLUE_L21 = 1.0, 2.0, 3.0
    GLUE_G510, GLUE_G1021 = 4.0, 4.0   # v2.144 (2026-10-15): 由 3.0/3.0 放宽到 4.0/4.0 (用户定案)
    # v2.142 (2026-10-15) 用户定案: 「ma5,ma10,ma21 必须走平或向上」= 硬性条件 (MA55 退出)
    #   口径 = 每根均线 今天 vs GLUE_SLOPE_LOOK 个交易日前 的变化率 >= GLUE_SLOPE_MIN
    #   (0.0 = 走平也算; 不要求加速向上)。三线全部达标才入选 —— 没有前端开关。
    #   MA55 退出原因: 澳弘电子 9/07 那天 MA55 斜率 -3.94% 而 MA5/MA10/MA21 已 +1.52/+1.86/+2.18
    #   —— 慢线还没反应过来, 用它当硬门槛筛掉的正是"短线已启动"的票。
    GLUE_SLOPE_MIN = 0.0
    GLUE_SLOPE_LOOK = 5

    def scan_ma_glue(self, progress_callback=None,
                     realtime_prices: Optional[Dict] = None) -> List[Dict]:
        """均线粘合 — 三线多头 + 现价抬离均线簇 + 短线粘合 (阈值见类常量 GLUE_*, v2.142)。

        ⚠ 盘中语义: 不预先拉全市场实时价 —— 先用本地K线零网络粗筛(三线多头+价距区间+粘合),
          只对活下来的拉快照拼 live bar 复算一遍。盘后 / 拉不到实时价 → 直接读盘最后一根。
        ⚠ MA 口径 = **含当日的收盘价简单平均**, 与 /api/kline 的 mas 字段、板块合成指数一致。
        """
        intraday = _is_trade_window()
        st_codes = load_st_codes()
        name_map = load_stock_name_map()
        files = sorted(os.listdir(KLINE_DIR))
        total = len(files)
        cands: List[Dict] = []

        # ── 第一层: 全市场本地 K 线粗筛 (零网络) ────────────────────
        for i, fname in enumerate(files):
            if not fname.endswith('.csv'):
                continue
            if progress_callback and i % 200 == 0:
                progress_callback(i, total)
            code = fname[:-4]
            if not (code.startswith('SZ') or code.startswith('SH')):
                continue
            num = code[2:]
            if len(num) != 6 or not num.isdigit():
                continue
            if num[0] in ('4', '8') or num.startswith(('92', '200', '900')):
                continue                    # 北交所 (4xx/8xx/920) + B股 (200/900)
            if num in st_codes:
                continue                    # ST/*ST 显式排除
            rows = _read_csv_tail(os.path.join(KLINE_DIR, fname), 90)
            if len(rows) < 60:
                continue                    # 不够 MA21+斜率回看 (次新股 / 长期停牌); v2.142 由 MA55 口径改成 MA21 口径, 阈值仍是 60
            cands.append({'code': code, 'num': num, 'rows': rows})

        self.glue_pool_size = len(cands)
        if not cands:
            return []

        # ── 第二层: 盘中拼实时 bar (盘后零网络) ──────────────────────
        live = {}
        if intraday:
            if progress_callback:
                progress_callback(0, len(cands))
            live = fetch_live_bars([c['code'] for c in cands])
        use_live = intraday and bool(live)
        self.glue_live = use_live
        self.glue_degraded = intraday and not use_live   # 盘中拉不到实时价 → 退化为读盘
        today = datetime.now().strftime('%Y-%m-%d')

        # ── 第三层: 逐只判定 ────────────────────────────────────────
        out: List[Dict] = []
        for c in cands:
            num, rows = c['num'], c['rows']
            kline = rows
            if use_live:
                lb = live.get(c['code'])
                if not lb or lb.get('open', 0) <= 0:
                    continue                # 停牌 / 无报价 → 实时bar不可信, 跳过
                kline = rows + [{'date': today,
                                 'open': lb['open'], 'close': lb['close'],
                                 'high': max(lb.get('high', 0), lb['close']),
                                 'low': (min(lb['low'], lb['close'])
                                         if lb.get('low', 0) > 0 else lb['close']),
                                 'volume': lb['volume']}]
            closes = [r['close'] for r in kline]
            if len(closes) < 60:
                continue                    # v2.142: MA21(21) + 斜率回看(5) = 26 才够; 沿用 60 保守
            price = closes[-1]
            if price <= 0:
                continue
            ma5 = sum(closes[-5:]) / 5.0
            ma10 = sum(closes[-10:]) / 10.0
            ma21 = sum(closes[-21:]) / 21.0
            if not (price > ma5 > ma10 > ma21):
                continue                    # ① 三线多头排列 (v2.142: MA55 退出)
            p5 = (price - ma5) / ma5 * 100
            p10 = (price - ma10) / ma10 * 100
            p21 = (price - ma21) / ma21 * 100
            g510 = (ma5 - ma10) / ma10 * 100
            g1021 = (ma10 - ma21) / ma21 * 100
            if (p5 < self.GLUE_L5 or p10 < self.GLUE_L10
                    or p21 < self.GLUE_L21
                    or p5 > self.GLUE_P5 or p10 > self.GLUE_P10
                    or p21 > self.GLUE_P21
                    or g510 > self.GLUE_G510 or g1021 > self.GLUE_G1021):
                continue                    # ② 价距须落在 [下限, 上限] 区间 / 短线张口太大
            # 放量倍数: 今量 ÷ 前5日均量 (纯展示 + 前端可选筛, 不是硬条件)
            v5 = [r['volume'] for r in kline[-6:-1]]
            avg5 = sum(v5) / len(v5) if v5 else 0.0
            vol_ratio = round(kline[-1]['volume'] / avg5, 2) if avg5 > 0 else 0.0
            pv = closes[-2]
            glue = round(max(g510, g1021), 2)
            # v2.142 (2026-10-15) 用户定案: 「ma5/ma10/ma21 必须走平或向上」= **硬性条件**
            #   斜率 = 这根均线今天的值 vs GLUE_SLOPE_LOOK 个交易日前同一根的值, 单位 %。
            #   三线全部 >= GLUE_SLOPE_MIN (0.0 = 走平即可) 才入选; 有一条向下就淘汰。
            #   值仍然存进结果行 (s5/s10/s21/ma_up), 供前端悬停提示展示。
            _look = self.GLUE_SLOPE_LOOK
            _sl = []
            for _n in (5, 10, 21):
                _a = sum(closes[-_n:]) / _n
                _b = sum(closes[-_look - _n:-_look]) / _n
                _sl.append((_a / _b - 1) * 100 if _b > 0 else -999.0)
            s5, s10, s21 = _sl
            ma_up = 1 if min(_sl) >= self.GLUE_SLOPE_MIN else 0
            if not ma_up:
                continue                    # ③ 三线必须走平或向上
            sec = self.ind_map.get(num, '')
            _chg = round((price - pv) / pv * 100, 2) if pv > 0 else 0.0
            out.append({
                'code': num, 'name': name_map.get(num, num),
                'price': round(price, 2),
                'change_pct': _chg,
                'sector': sec, 'concept': self.con_map.get(num, ''),
                'market_cap': '',
                # v2.216: A股适配 (均线粘合)
                'm4_verify': _m4_for(kline, sec, self.con_map.get(num, ''),
                                     _chg, num, self.sector_returns),
                # 「左峰」那列改放 MA5 / 「距峰」那列改放 现价-MA5% (借 14 列既有表头)
                'left_peak': round(ma5, 2),
                'dist_pct': round(p5, 2),
                'glue': glue,
                'ma5': round(ma5, 2), 'ma10': round(ma10, 2),
                'ma21': round(ma21, 2),            # v2.142: MA55 字段已删
                'g510': round(g510, 2), 'g1021': round(g1021, 2),
                'gap5_21': round((ma5 - ma21) / ma21 * 100, 2),
                'p5': round(p5, 2), 'p10': round(p10, 2),
                'p21': round(p21, 2),               # v2.142: p55 字段已删
                'vol_ratio': vol_ratio,
                # v2.142: 三条均线的 5 日斜率(%) + 「三线走平或向上」总标记(前端勾选框用)
                's5': round(s5, 2), 's10': round(s10, 2),
                's21': round(s21, 2),
                'ma_up': ma_up,
                'stop_loss': round(ma21, 2),         # 跌破 MA21 = 三线排列破坏
                'sync_pct': None, 'body_pct': None,
                'mode': 'glue', 'mode_detail': '均线粘合',
                'is_valid': True,
                'consensus': {
                    # 评分即粘合度(%) — 兼作排序键, 前端第 1 列直接显示这个数
                    'score': glue, 'base_score': glue, 'bonus': 0,
                    'bonus_detail': {}, 'bonus_notes': [],
                    'recommendation': 'MA5-MA10 %.2f%% / MA10-MA21 %.2f%%' % (g510, g1021),
                    'signal': '均线粘合',
                    'signal_strength': ('STRONG' if glue <= 1.0 else
                                        ('MEDIUM' if glue <= 2.0 else 'WEAK')),
                    'score_detail': {}, 'm4_passed': 0, 'm4_checks': [],
                },
            })

        self.glue_cand_size = len(out)
        # v2.138 (2026-10-13): 档位标注 —— 复用 _filter_multihead 那套口径 (别名/细分→一级/取最强档),
        #   但 tag_only=True ⇒ **只写 r['ind_t']/r['con_t'], 一行都不丢**。
        #   粘合模式的池子只由均线决定 (用户定案), 档位怎么用交给前端两个勾选框; 这里必须先标注,
        #   否则前端拿不到档位就筛不出东西。整段用 try 兜住: 档位只是"显示层附加信息",
        #   板块数据缺失时宁可不标注也绝不能让选股本身失败。
        try:
            self.mh_ind_rt, self.mh_con_rt, self.mh_ind_tier_rt, self.mh_con_tier_rt = \
                load_realtime_multihead_union(ma_periods=(5, 10, 21, 55))
            out = self._filter_multihead(out, tag_only=True)
        except Exception as e:
            print(f'[glue] 档位标注失败(不影响均线选股): {e!r}', file=sys.stderr)
        out.sort(key=lambda r: r['glue'])       # 粘合度升序 (三条短线越粘越前)
        return out

    def _empty(self, code, info, price, change_pct, reason):
        num = code[2:] if code.startswith(('SZ', 'SH')) else code
        return {'code': num, 'name': info.get('name', ''), 'price': price,
                'change_pct': change_pct, 'sector': '', 'concept': '', 'market_cap': '',
                'consensus': {'score': 0, 'recommendation': reason, 'dimensions': [],
                              'dim_count': 0, 'm4_passed': 0, 'm4_checks': [], 'signal_strength': 'N/A'}}

    def _fetch_realtime(self, codes: List[str]) -> Dict[str, Dict]:
        results = {}
        sina_to_pool = {}
        sina_codes = []
        for c in codes:
            sc = _sc(c)
            sina_to_pool[sc] = c
            sina_codes.append(sc)
        # v2.14 (2026-08-27): 新浪50只/批串行 → 4并发, 639只 13批≈13秒 → ~4秒
        def fetch_batch(batch):
            # v1 数据源设置 (2026-08-29): 统一走 ds.fetch_realtime (主源失败自动降级备源)
            out = ds.fetch_realtime(list(batch))
            if '_err' in out:
                out = {}
            # 过滤: 只要属于选股池的代码 (sc 格式 sh600519, 与新浪/腾讯一致)
            for sc in [k for k in out if k not in sina_to_pool]:
                out.pop(sc, None)
            return out
        from concurrent.futures import ThreadPoolExecutor
        batches = [sina_codes[i:i + 50] for i in range(0, len(sina_codes), 50)]
        with ThreadPoolExecutor(max_workers=4) as ex:
            for out in ex.map(fetch_batch, batches):
                for sc, v in out.items():
                    pc = sina_to_pool.get(sc)
                    if pc:
                        results[pc] = v
        return results


# ============================================================
# 板块均线分析 (保留)
# ============================================================

# 板块均线缓存 (v2.4, 2026-08-05): 按(行业/概念, ma_periods, 实时/收盘)独立缓存
# 盘中实时价版60秒(保留盘中趋势刷新), 盘后收盘版1小时(用户定, 减少扫描等待)
# 2026-08-06 修复: key 必须含 kind(行业/概念) — 否则 analyze_sector_ma 与 analyze_concept_ma
# 共用同一 key 互相污染 (预热线程概念覆盖行业缓存, 行业面板显示概念名, 医药商业"消失")
_SECTOR_MA_CACHE: Dict = {}
_SECTOR_MA_TTL_RT = 60
_SECTOR_MA_TTL = 3600


def _rt_slot(realtime_prices):
    """v2.152: 缓存槽标记 —— 盘中实时价版 + **临时快照版本**
    原实现只有 'rt'/'close' 两槽。加了临时快照之后, 同一槽里可能装的是
    「11:35 拉的那份快照」算出来的板块, 快照一更新 (11:40 换了新数据)
    缓存还在 60 秒 TTL 内就会被直接命中 ⇒ 板块显示旧数据。
    把快照版本并进槽名, 快照一变自然换槽, 不做任何显式清理。"""
    base = 'rt' if realtime_prices else 'close'
    try:
        g = _intraday.gen()          # '' = 当前没有临时快照
    except Exception:
        g = ''
    if g:
        return f'{base}#{g}'
    return base


def _sector_ma_cache_get(kind, ma_periods, realtime_prices):
    import time as _t
    key = (kind, ma_periods, _rt_slot(realtime_prices))
    ttl = _SECTOR_MA_TTL_RT if realtime_prices else _SECTOR_MA_TTL
    ent = _SECTOR_MA_CACHE.get(key)
    if ent and _t.time() - ent[0] < ttl:
        return ent[1]
    return None


def _sector_ma_cache_set(kind, ma_periods, realtime_prices, result):
    import time as _t
    key = (kind, ma_periods, _rt_slot(realtime_prices))
    _SECTOR_MA_CACHE[key] = (_t.time(), result)
    # v2.89: 算完顺手落盘 (同一份结果, 下钻直接读, 别再全表重算一遍)
    _persist_slot(_trend_key(ma_periods), kind, result, bool(realtime_prices))


def _sector_ma_cache_clear():
    """清空全部档位缓存 (更新板块/切档后调用, 2026-08-05)"""
    _SECTOR_MA_CACHE.clear()
    global _MH_CACHE
    _MH_CACHE = {}


def sector_ma_cache_ts(kind, ma_periods, realtime_prices):
    """缓存条目的实际计算时刻 '%m-%d %H:%M:%S' (v2.60: 标题时间含日期)
    命中缓存=上次计算时刻; 未命中/已过期=None → 调用方以本次重算时刻兜底"""
    import time as _t
    from datetime import datetime as _dt
    key = (kind, ma_periods, _rt_slot(realtime_prices))
    ent = _SECTOR_MA_CACHE.get(key)
    if not ent:
        return None
    return _dt.fromtimestamp(ent[0]).strftime('%m-%d %H:%M:%S')


def composite_sector_kline(codes: List[str], name: str = '', kind: str = 'sector',
                           ma_periods=(5, 10, 21, 55, 120, 250), max_days: int = 500) -> Optional[Dict]:
    """板块/概念合成指数OHLCV — 涨幅等权 (与 analyze_sector_ma 同口径: 指数=成分股涨幅算术平均, 1000点起)
    返回与 /api/kline 同构: {dates, ohlc:[[o,c,l,h]...], volume, mas, count, first_date, last_date, last_close, is_composite}
    v2.13 (2026-08-16): 供前端渲染行业/概念指数日K
    """
    from collections import defaultdict as dd
    stock_kl = []
    for c in codes:
        fpath = os.path.join(KLINE_DIR, f'{_px(c)}.csv')
        if not os.path.exists(fpath):
            continue
        try:
            tail = _read_csv_tail(fpath, max_days + 10)
            if len(tail) < 21:
                continue
            # 停牌过滤 (同 analyze_sector_ma 口径: 最新K线滞后>3个**交易日**不参与)
            # v2.128: 原为 (datetime.now() - last_dt).days > 3 = 日历日, 节后必误杀
            if not kline_is_fresh(tail[-1]['date']):
                continue
            stock_kl.append(tail)
        except Exception:
            continue
    if not stock_kl:              # v5.0 (2026-09-12): 有几只算几只 (原 <3 只直接判不可用)
        return None
    # 每日分项回报 (相对前收) + 成交量累计
    d_open, d_close, d_high, d_low = dd(list), dd(list), dd(list), dd(list)
    d_vol = dd(lambda: 0.0)
    for kl in stock_kl:
        for j in range(1, len(kl)):
            prev = kl[j - 1]['close']
            if prev <= 0:
                continue
            d = kl[j]['date']
            d_open[d].append(kl[j]['open'] / prev - 1)
            d_close[d].append(kl[j]['close'] / prev - 1)
            d_high[d].append(kl[j]['high'] / prev - 1)
            d_low[d].append(kl[j]['low'] / prev - 1)
            d_vol[d] += kl[j]['volume']
    # 指数累积: close链为唯一基准, 每日开/高/低从上一收盘推导 (多链独立累积会发散成畸形OHLC)
    # v5.0 (2026-09-12): 每日 ≥1 只有数据即计点 (原 ≥5) — 细分行业成分少的按实际只数合成
    dates, ohlc, volume = [], [], []
    idx_prev = 1000.0
    for d in sorted(d_close.keys()):
        n = len(d_close[d])
        if n < 1:
            continue
        o = idx_prev * (1 + sum(d_open[d]) / n)
        c = idx_prev * (1 + sum(d_close[d]) / n)
        h = idx_prev * (1 + sum(d_high[d]) / n)
        l = idx_prev * (1 + sum(d_low[d]) / n)
        hi = max(o, c, h)
        lo = min(o, c, l)
        ohlc.append([round(o, 2), round(c, 2), round(lo, 2), round(hi, 2)])
        volume.append(round(d_vol[d], 0))
        dates.append(d)
        idx_prev = c
    if len(ohlc) < 30:
        return None
    closes = [r[1] for r in ohlc]
    mas = {}
    for p in ma_periods:
        mas[f'ma{p}'] = [round(v, 2) if v is not None else None for v in Indicators.sma(closes, p)]
    return {
        'name': name, 'kind': kind, 'is_composite': True, 'count': len(ohlc),
        'dates': dates, 'ohlc': ohlc, 'volume': volume, 'mas': mas,
        'first_date': dates[0], 'last_date': dates[-1], 'last_close': round(c, 2),
    }


def _recent_formed_date(composite: List[Dict], ma_periods) -> Optional[str]:
    """合成指数最近一次形成多头排列的日期 (v2.22, 按当前档位均线组合判定)

    多头排列 = close > MA[p1] > MA[p2] > ... > MA[pk] (ma_periods 逐级)
    从最新往回扫第一个不满足日 → 其下一交易日 = 最近一次"重新形成"日
    (天然涵盖 n日多头→n+1破坏→n+2修复 = n+2为形成日)
    返回: 形成日 (composite内日期); 窗口内从未破坏 → 最早可判定日 (调用方按长期多头处理);
         最新一天不满足 → None (状态不会是偏多/强多头, 前端不显示)
    前缀和 O(1) 均线查询 + early-exit, 全量板块毫秒级
    """
    n = len(composite)
    need = max(ma_periods)
    if n < need:
        return None
    closes = [r['close'] for r in composite]
    pref = [0.0] * (n + 1)
    for i, v in enumerate(closes):
        pref[i + 1] = pref[i] + v

    def _ma(p, i):
        return (pref[i + 1] - pref[i + 1 - p]) / p

    for i in range(n - 1, need - 2, -1):
        prev = closes[i]
        aligned = True
        for p in ma_periods:
            m = _ma(p, i)
            if prev <= m:
                aligned = False
                break
            prev = m
        if not aligned:
            return composite[i + 1]['date'] if i + 1 < n else None
    return composite[need - 1]['date']



def _official_series(kind, name):
    """板块/概念 **官方指数** 的收盘序列 [{'date','close'}] —— 给「形成日/新★」用

    v2.209 (2026-10-23 用户 m05300 定案): 既然档位已经换成官方指数口径, 形成日也必须同一套,
    否则会出现「档位说偏多(官方), 形成日却说上周三(成分股合成)」两张口径打架。

    ⚠ 只在 status 已经是 偏多/强多头 时才调 (全表只有个位数), 不参与占比/档位判定本身:
       517 个板块各读一次 CSV 会把面板拖到十几秒。取不到/异常 → 返回 None, 调用方回退合成。
    """
    try:
        from quant_terminal import sector_kline as _sk
        d = _sk.official_kline(kind, name)
        if not d or not d.get('dates'):
            return None
        return [{'date': dt, 'close': row[1]} for dt, row in zip(d['dates'], d['ohlc'])]
    except Exception:
        return None


def _formed_meta(composite: List[Dict], ma_periods, status: str) -> tuple:
    """封装形成日输出: (formed_date显示文本或None, is_new) — 仅偏多/强多头有意义"""
    if status not in ('偏多', '强多头'):
        return None, False
    need = max(ma_periods)
    fd = _recent_formed_date(composite, ma_periods)
    if fd is None:
        return None, False
    if fd == composite[-1]['date']:
        return ('今日盘中' if fd == 'rt' else fd), True
    if fd == composite[need - 1]['date']:
        return '长期多头', False  # 窗口内从未破坏
    return fd, False


_MA_TIER = {'强多头': 0, '偏多': 1, '中性': 2, '偏空': 3}


def _ma_sort_key(row: Dict) -> tuple:
    """v2.61 (2026-09-10): 行业/概念行确定性排序键 — 用户定案:
    (档位, -多头占比) 相同 → 成分股总数多者胜; 总数也相同 → 名称按 GBK 序
    (汉字按拼音序编码 ≈ 首字母序, 同首字母自动续比全名)。键值完全确定 →
    同数据跨日名次零漂移 (升降徽标不失真); 此前整数占比大量并列 + 无 tiebreak → 假跳"""
    return (_MA_TIER.get(row['status'], 9), -row['bullish_pct'], -row['total'],
            row['name'].encode('gbk', errors='ignore'))


def analyze_sector_ma(ind_map: Dict, ma_periods=(5, 10, 21, 55),
                      realtime_prices: Optional[Dict] = None,
                      as_of: Optional[str] = None,
                      use_pool: bool = True, use_cache: bool = True) -> List[Dict]:
    """realtime_prices: {6位代码: 实时价} — 盘中时最后一根K线用实时价, 反映盘中趋势变化"""
    """板块MA排列分析 (全成分股等权合成, 用户8/16定案: 删Top10误导注释)
    as_of: 'YYYY-MM-DD' 回填口径 (v2.61 补 n-1 日基线) — 每只股序列截到该日再算,
    新鲜度判定参照日同步改为 as_of; 走回填时绕开缓存(一次性计算不查不set, 防毒害)

    v2.100 (2026-09-16) 加 use_pool / use_cache —— 供「上级行业(90类)全量窗口」调用:
      use_pool=False: 跳过选股池过滤。面板只显示池内那几十个行业, 上级窗口要的是全 90 类。
      use_cache=False: **必须同时关掉「查缓存」和「写缓存」**。_SECTOR_MA_CACHE 的 key 是
        (kind, ma_periods, 'rt'|'close'), **不含 map 身份** —— 只关 setter 的话, 上级调用会
        命中面板预热过的**细分**缓存直接 `return cached`, 上级窗口一字不差地显示 257 行细分,
        且不报任何错(静默错数)。写侧同时关掉是因为 _sector_ma_cache_set 内部会
        _persist_slot() **写 trend_data.json**, 而那是选股数据源, 一个字都不能碰。
      两者默认 True → 与改动前**逐字一致**, 13 个现有调用点一行不改。"""
    if use_cache and as_of is None:  # v2.61: 缓存仅服务"最新口径"常规调用
        cached = _sector_ma_cache_get('sector', ma_periods, realtime_prices)
        if cached is not None:
            return cached
    from collections import defaultdict as dd
    sector_stocks = dd(list)
    for code_num, sec_name in ind_map.items():
        if sec_name: sector_stocks[sec_name].append(code_num)
    # v5.0 (2026-09-12): 成分门槛 5 → 1 — 细分行业(283→257类)里 30 类成分<5只,
    # 原门槛会让这些股票彻底失去板块归属 (板块铁律直接排除)。有几只算几只。
    valid_sectors = {sec: codes for sec, codes in sector_stocks.items() if len(codes) >= 1}
    pool = load_pool() if use_pool else {}   # v2.100: 上级窗口要全 90 类, 不按选股池裁
    if pool:
        pool_sectors = set()
        for info in pool.values():
            sec = info.get('sector', '')
            if sec and sec in valid_sectors: pool_sectors.add(sec)
        target = pool_sectors if pool_sectors else set(valid_sectors.keys())
    else:
        target = set(valid_sectors.keys())
    results = []
    # v2.173 (2026-10-22 用户定案): 板块档位改以**官方板块指数**均线排列为准,
    #   成分股多头占比只区分 强多头(≥40%)/偏多; 官方数据取不到时回退合成口径。
    OFFICIAL_ALIGNED = official_index_aligned(ma_periods)
    for sec_name in target:
        codes = valid_sectors.get(sec_name, [])
        stock_turnover = []
        for c in codes:
            fpath = os.path.join(KLINE_DIR, f'{_px(c)}.csv')
            if not os.path.exists(fpath): continue
            try:
                tail = _read_csv_tail(fpath, 250)
                if as_of:  # v2.61 回填: 截到截止日 (删尾行不删头, 序列仍满窗)
                    tail = [r for r in tail if r['date'] <= as_of]
                # v2.152: 先叠临时快照 (11:35 未写盘的那份), 再做新鲜度判定 ——
                #   新鲜度看的是**叠完之后的末行日期**, 否则「本地还停在前一天、
                #   但今天已经有快照」的股票会被当成不新鲜整只踢掉。
                tail = with_intraday(tail, c, as_of)
                if len(tail) < 21: continue
                # v2.128: 新鲜度改走共用判据 (交易日口径; 原为 (ref_dt - last_dt).days > 3 = 日历日)
                if not kline_is_fresh(tail[-1]['date'], as_of): continue
                # v2.3盘中: 实时价覆盖最后一根K线(反映盘中趋势)
                rt = (realtime_prices or {}).get(c)
                if rt and rt > 0:
                    import copy
                    tail = copy.deepcopy(tail)
                    tail[-1] = {'date': 'rt', 'open': tail[-1]['open'], 'close': rt,
                                'high': max(tail[-1]['high'], rt), 'low': min(tail[-1]['low'], rt),
                                'volume': tail[-1]['volume']}
                stock_turnover.append((c, tail))
            except Exception: continue
        if not stock_turnover: continue     # v5.0 (2026-09-12): 成分门槛 3 → 1 (有几只算几只)
        # v2.4 合成口径: 涨幅等权 (同花顺官方: 指数=成分股涨幅算术平均, 2026-08-04)
        # 价格等权 ΣP/N 高价股主导 vs 涨幅等权 ΣΔ%/N 低吸活跃股主导 — 数学不等价
        date_rets = dd(list)
        for c, kline in stock_turnover:
            for j in range(1, len(kline)):
                d = kline[j]['date']; prev = kline[j - 1]['close']
                if prev > 0: date_rets[d].append(kline[j]['close'] / prev - 1)
        composite = []
        idx = 1000.0
        # 'rt' 实时价日排最后 (避免字母序排前错位)
        for date in sorted(date_rets.keys(), key=lambda d: (d == 'rt', d)):
            vals = date_rets[date]
            if len(vals) >= 1:               # v5.0 (2026-09-12): 每日 ≥1 只即计点 (原 ≥5)
                idx *= (1 + sum(vals) / len(vals))
                composite.append({'date': date, 'close': idx})
        need = max(ma_periods)
        # v2.173: 官方指数多头排列优先（取不到才回退合成口径）
        _off_aligned = OFFICIAL_ALIGNED.get(sec_name)
        bullish_stocks = []  # v2.12: 多头排列成分股列表 (供前端点击板块下钻)
        if len(composite) >= need:
            closes_syn = [r['close'] for r in composite]
            mas_syn = {p: sum(closes_syn[-p:]) / p for p in ma_periods}
            seq = [closes_syn[-1]] + [mas_syn[p] for p in ma_periods]
            syn_aligned = all(seq[j] > seq[j + 1] for j in range(len(seq) - 1))
            bullish = 0
            for c, kline in stock_turnover:
                closes = [r['close'] for r in kline]
                if len(closes) < need: continue
                close = closes[-1]
                mas = {p: sum(closes[-p:]) / p for p in ma_periods}
                seq2 = [close] + [mas[p] for p in ma_periods]
                if all(seq2[j] > seq2[j + 1] for j in range(len(seq2) - 1)):
                    bullish += 1
                    bullish_stocks.append(c)
            # v5.0: max(1,…) — 成分 1~2 只时 //3=0 会把自身就多头的成分压成 0 多头 (合成未多头才打折)
            if not (syn_aligned if _off_aligned is None else _off_aligned):
                bullish = min(bullish, max(1, len(stock_turnover) // 3))
        else:
            bullish = 0
            syn_aligned = False  # 修复: 数据不足时未定义 (2026-08-05, 6线档位need=250触发)
            for c, kline in stock_turnover:
                closes = [r['close'] for r in kline]
                if len(closes) < need: continue
                close = closes[-1]
                mas = {p: sum(closes[-p:]) / p for p in ma_periods}
                seq2 = [close] + [mas[p] for p in ma_periods]
                if all(seq2[j] > seq2[j + 1] for j in range(len(seq2) - 1)):
                    bullish += 1
                    bullish_stocks.append(c)
        # v2.97 (2026-09-16): 下钻**名单**口径 —— 只要 现价 > MA5 (均线之间不要求有序)。
        #   单开一趟循环, 与上面的面板计数彻底解耦: 面板那趟在「合成指数不足 need 根」时要求
        #   len(closes) >= need(=55/250), 这里只要 5 根, 且**不参与** 占比/status 的判定。
        drill_stocks = []
        for c, kline in stock_turnover:
            closes = [r['close'] for r in kline]
            if len(closes) < DRILL_MA: continue
            if closes[-1] > sum(closes[-DRILL_MA:]) / DRILL_MA:
                drill_stocks.append(c)
        pct = bullish / len(stock_turnover) * 100
        # v2.173 判定（用户 2026-10-22 定案）：以**官方板块指数**均线是否多头排列定档，
        #   成分股多头占比只用来区分 强多头(≥40%) / 偏多(<40%)；指数没多头排列时才落到 中性/偏空。
        #   官方数据取不到 → 回退到原来的合成指数口径（syn_aligned）。
        _aligned = syn_aligned if _off_aligned is None else bool(_off_aligned)
        if _aligned:
            status = '强多头' if pct >= 40 else '偏多'
        else:
            status = '中性' if pct >= 30 else '偏空'
        # v2.209: 形成日也走官方指数口径 (与档位同一套); 取不到回退合成
        _fseq = _official_series('sector', sec_name) if status in ('偏多', '强多头') else None
        formed_date, is_new = _formed_meta(_fseq or composite, ma_periods, status)  # v2.22
        results.append({'name': sec_name, 'bullish_pct': round(pct, 0),
                        'bullish_count': bullish, 'total': len(stock_turnover), 'status': status,
                        'formed_date': formed_date, 'is_new': is_new,
                        'drill_count': len(drill_stocks),   # v2.97: 下钻名单真值(站上MA5)
                        'bullish_stocks': bullish_stocks, 'drill_stocks': drill_stocks})
    # v2.4 (2026-08-06): 多头优先排序 — 面板前N个能显示全部多头(占比低的多头不被截断, 医药商业案例)
    # v2.61 (2026-09-10): 确定性排序键 — 占比并列比成分股总数, 再并列比名称GBK序
    results.sort(key=_ma_sort_key)
    # v2.100: use_cache 同时守**取**(上面)和**写**(这里) —— setter 内部 _persist_slot() 会写
    #   trend_data.json(选股数据源), 上级窗口绝不能走到这
    if use_cache and as_of is None:  # v2.61: 回填口径不写缓存
        _sector_ma_cache_set('sector', ma_periods, realtime_prices, results)
    return results


def calc_rsrs(kline: List[Dict], N: int = 18, M: int = 600) -> Optional[float]:
    """RSRS 阻力支撑相对强度标准分 (光大证券原创)

    N日高低点OLS回归: high = a + b*low, β衡量支撑/阻力相对强度
    标准分: 最近β在M日窗口内的标准化值 (>0.7看多, <-0.7看空; N=18最优, N=20会劣化)
    返回标准分, 数据不足返回 None
    """
    if len(kline) < N + 5: return None
    highs = [d['high'] for d in kline]
    lows = [d['low'] for d in kline]
    betas = []
    for i in range(N - 1, len(kline)):
        h = highs[i - N + 1:i + 1]
        l = lows[i - N + 1:i + 1]
        n = N
        mx = sum(l) / n
        my = sum(h) / n
        sxy = sum((l[j] - mx) * (h[j] - my) for j in range(n))
        sxx = sum((l[j] - mx) ** 2 for j in range(n))
        b = sxy / sxx if sxx else 0.0
        betas.append(b)
    if len(betas) < M: return None
    window = betas[-M:]
    mu = sum(window) / M
    sd = (sum((x - mu) ** 2 for x in window) / M) ** 0.5
    if sd == 0: return None
    return round((betas[-1] - mu) / sd, 2)


def rsrs_to_mode(std: Optional[float], close: Optional[float] = None,
                 ma21: Optional[float] = None) -> str:
    """RSRS标准分 → 四模式 (进攻/结构/退潮/恐慌)

    v2.10 (2026-08-12): RSRS+MA21双确认 — 恐慌需同时跌破MA21,
    防趋势市假恐慌 (光大RSRS为震荡择时指标, 缩量慢牛收敛形态下β失真,
    2026-08实测 8/5-8/10 上证连涨4%却 std=-1.5 → 假恐慌)
    """
    if std is None: return '未知'
    if close is not None and ma21 is not None:
        if close < ma21:          # 趋势已破坏: RSRS弱=真恐慌
            return '恐慌' if std < -0.7 else '退潮'
        # 趋势完好(收盘≥MA21): RSRS弱只降级为退潮, 不再锁仓
        if std >= 0.7: return '进攻'
        if std >= 0: return '结构'
        return '退潮'
    # 旧逻辑 (未提供均线时退化)
    if std >= 0.7: return '进攻'
    if std >= 0: return '结构'
    if std >= -0.7: return '退潮'
    return '恐慌'


_MH_CACHE = {}  # ma_periods -> (ts, ind_tier, con_tier), 按档位独立缓存 (v2.4; v2.96 由名单改为档位表)


# ============================================================
# v2.14 (2026-08-27): 行业/概念趋势数据落盘 — 盘后读盘免重算
# 用户方案: 15:30后数据不再自动更新(收盘后K线不变), 选股直接读盘;
# 盘中照算并覆盖落盘; 手动"更新板块"全刷4档+覆盖落盘。单文件, 覆盖写, 不新增文件。
# ============================================================
TREND_DATA_FILE = os.path.join(BASE_DIR, 'quant_terminal', 'trend_data.json')


def _is_trade_window() -> bool:
    """交易时段判断 — 与左上角时钟模块 (server.get_trading_status) 同口径:
    周一至周五 9:30-11:30 / 13:00-15:00 为盘中; 周末/午休/盘前/盘后一律 False (读盘)
    v2.24 (2026-08-30): 用户定案「所有时间判断以时钟模块为准」— 原 9:15-15:30 连续窗口
    且不分周末, 周六被误判盘中 → load_realtime_multihead (~27s) + _ma_status_map 首只
    (~26s) 反复全量重算板块指数, 755只扫描卡好几分钟。对齐后周末扫描秒级
    v2.118 (2026-09-25): 补节假日闸 —— 周末只是这个病的特例, 节假日照样犯 (中秋节的
    星期五被判盘中, 同一个全量重算开销)。判定统一走 datasource.is_holiday(),
    与 server / turnover **同一处**, 不再各写一份拷贝"""
    now = datetime.now()
    if now.weekday() >= 5:  # 周六=5 周日=6
        return False
    if ds.is_holiday():    # v2.118: 工作日但休市
        return False
    t = now.hour * 100 + now.minute
    return (930 <= t <= 1130) or (1300 <= t <= 1500)


def _load_trend_data() -> Optional[Dict]:
    try:
        with open(TREND_DATA_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


def _save_trend_data(data: Dict):
    """原子覆盖写: 临时文件+rename, 防半写 (不增加文件, 写完即替换)"""
    try:
        tmp = TREND_DATA_FILE + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, TREND_DATA_FILE)
    except Exception:
        pass


def _trend_key(ma_periods) -> str:
    return ''.join(str(p) for p in ma_periods)


def _update_trend_data(key: str, sectors: List[Dict], cons: List[Dict]):
    """算完单档后更新落盘文件 (盘中自动更新/首次兜底时调用; 其他档位数据保留)
    v2.89: 加锁 — 下钻落盘(_persist_slot)与扫描落盘同抢一个文件, 读改写必须串行"""
    with _TREND_PERSIST_LOCK:
        d = _load_trend_data() or {'updated_at': '', 'lines': {}}
        d.setdefault('lines', {})[key] = {'sector': sectors, 'concept': cons}
        d['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        _save_trend_data(d)


# v2.89 (2026-09-12): 下钻免重算 —— 结果落盘
# 面板/扫描每算完一档一维, 就把整份结果(含 bullish_stocks 名单)写进 trend_data.json ——
# 算的是同一次结果, 下钻直接读, 别再为一个板块把整张表重算一遍 (行业 ~3.7s / 概念 ~7.4s)。
# ★ 命名空间铁律: d['lines'] 恒为收盘口径 —— 老读取方 (_ma_status_map 盘后读 /
#   load_realtime_multihead 盘后读 / breakout._trend_lookup / _trend_status_maps 状态染色)
#   全都不看口径只看键, 盘中口径绝不能写进去 (会默默污染涨停突破建池与选股)。
#   盘中口径 (面板传了实时价快照) 另存 d['rt_lines'], 只有板块下钻会读, 且只认当天的快照。
# ============================================================
_TREND_PERSIST_LOCK = threading.Lock()   # trend_data.json 读改写串行 (面板/扫描/下钻同抢一个文件)


def _persist_slot(key: str, kind: str, rows: List[Dict], rt_basis: bool):
    """单档单维度结果落盘 (v2.89) — 供板块下钻读盘免重算

    rt_basis=True 写 d['rt_lines'][key] (盘中口径, 只供下钻); False 写 d['lines'][key] (收盘口径, 老样子)。
    内容没变就不写盘: 面板盘中每60s重算一次, 白写 500KB 还会刷 mtime (状态染色那张表按 mtime 失效重读)。
    """
    with _TREND_PERSIST_LOCK:
        d = _load_trend_data() or {'updated_at': '', 'lines': {}}
        ns = d.setdefault('rt_lines' if rt_basis else 'lines', {}).setdefault(key, {})
        if ns.get(kind) == rows:
            return
        ns[kind] = rows
        if rt_basis:
            ns['at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')   # 隔夜作废依据
        d['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        _save_trend_data(d)


def trend_slot_rows(kind: str, ma_periods, rt_basis: bool) -> Optional[List[Dict]]:
    """读落盘某档某维度全表结果 (板块下钻用, v2.89)

    没有该档 / 没算过该口径 → None, 调用方老老实实重算。盘中口径只认当天的快照 ——
    昨天的盘中快照对今天的盘中询问没有意义。"""
    d = _load_trend_data() or {}
    key = _trend_key(ma_periods)
    if rt_basis:
        slot = (d.get('rt_lines') or {}).get(key) or {}
        if not str(slot.get('at') or '').startswith(datetime.now().strftime('%Y-%m-%d')):
            return None
    else:
        slot = (d.get('lines') or {}).get(key) or {}
    return slot.get(kind) or None


# v2.61: 行业/概念趋势 双槽基线 + 升降徽标（用户定案）
# 基线只被手动「更新板块」（force 且非交易窗口）写入 —— 启动预热/打开终端/盘中自动路径绝不写。
# 双槽滚动：基准永远 = cur(最近一次 force)；换数据日 force → cur 降为 prev、新算升为 cur；
#   同一数据日重复 force → 只覆盖 cur 不滚动。按 kind × 均线档位分槽，仅 4/5/6 线(3 线档不落盘)。
# ⚠ v2.220：**槽位日期用数据日，不用墙钟日**。原实现拿 datetime.now() 当槽位 —— 周六点「更新板块」算的其实
#   还是周五收盘数据却记成周六 → 与 prev 完全一致 → deltas 全 0 → 三个窗口徽标整片消失。
#   数据日由 baseline_data_date() 给出，墙钟只用于 ts 展示。
def baseline_data_date() -> str:
    """徽标槽位用的「数据日」= 本地板块指数日线末根日期 (拿不到退个股日线, 再退今天)

    为什么不用 datetime.now(): 周末/节假日点「更新板块」, 所有重算都基于上一个交易日
    的收盘数据, 墙钟日期却变了 —— 用墙钟当槽位日期会让新槽和旧槽内容相同、差值全 0。
    """
    try:
        from quant_terminal import sector_kline as _sk
        _d = _sk.index_last_date()
        if _d:
            return _d
    except Exception:
        pass
    try:
        for _c in ('SH600519', 'SZ000001', 'SH600036'):
            rows = load_csv_kline(_c)
            if rows:
                return rows[-1]['date']
    except Exception:
        pass
    return datetime.now().strftime('%Y-%m-%d')

MA_BASELINE_FILE = os.path.join(BASE_DIR, 'quant_terminal', 'ma_baseline.json')
PERSIST_LINES = ('4', '5', '6')
_MA_BASELINE_CACHE: Optional[Dict] = None  # 进程内缓存: 一次 force 多档只读一次盘


def _load_ma_baseline() -> Dict:
    """读基线文件(带进程缓存); 缺失/损坏自愈为空结构"""
    global _MA_BASELINE_CACHE
    if _MA_BASELINE_CACHE is None:
        d = {}
        try:
            with open(MA_BASELINE_FILE, 'r', encoding='utf-8') as f:
                d = json.load(f)
                if not isinstance(d, dict):
                    d = {}
                elif _fix_slot_dates(d):
                    try:   # 自愈结果立刻落盘, 下次启动不必再算
                        with open(MA_BASELINE_FILE, 'w', encoding='utf-8', newline='\n') as _f:
                            _f.write(json.dumps(d, ensure_ascii=False, separators=(',', ':')))
                    except Exception:
                        pass
        except Exception:
            d = {}
        _MA_BASELINE_CACHE = d
    return _MA_BASELINE_CACHE


def _fix_slot_dates(d: Dict):
    """v2.220 自愈: 清掉「周末/节假日误点 force」留下的幽灵槽。

    幽灵槽的签名(三条同时成立才动手):
      1. cur.date 是周末(或已知节假日/停市日) —— 绝不是交易日
      2. prev.date < cur.date 且 prev 有 deltas
      3. cur 的 deltas 是空的(那次算的和 prev 是同一份行情, 差值为 0)
    处理: 直接把 prev 提回 cur(徽标立即恢复), prev 置空; 记 ghost_fixed_from 留痕。
    只动这一种形态 —— 真的「今天没有变化」的槽 cur.date 是交易日, 不会命中。
    """
    try:
        try:
            from . import fuyao_limit as _fl
        except Exception:
            import fuyao_limit as _fl
        _days = set(_fl.trading_days() or [])
    except Exception:
        _days = set()

    def _is_off_day(ds):
        """非交易日判定: 在日历里→按日历; 超出日历→只有周末才算(不猜节假日)"""
        if not ds or len(ds) != 10:
            return False
        if _days:
            if ds in _days:
                return False
            if ds <= max(_days):
                return True          # 日历覆盖范围内却不在表里 ⇒ 停市
        try:
            return datetime.strptime(ds, '%Y-%m-%d').weekday() >= 5
        except Exception:
            return False

    _n = 0
    for _kind, _slots in d.items():
        if not isinstance(_slots, dict):
            continue
        for _ln, _slot in _slots.items():
            if not isinstance(_slot, dict):
                continue
            cur, prev = _slot.get('cur'), _slot.get('prev')
            if not isinstance(cur, dict) or not isinstance(prev, dict):
                continue
            _cd = str(cur.get('date') or '')
            _pd = str(prev.get('date') or '')
            if not (_cd and _pd and _pd < _cd):
                continue
            if not _is_off_day(_cd):
                continue
            if (cur.get('deltas') or {}) or not (prev.get('deltas') or {}):
                continue
            _slot['prev'] = None
            _slot['cur'] = prev
            prev['ghost_fixed_from'] = _cd
            _n += 1
    return _n


def _save_ma_baseline():
    """原子覆盖写 (同 trend_data.json 惯例); 失败不影响主流程"""
    global _MA_BASELINE_CACHE
    if _MA_BASELINE_CACHE is None:
        return
    try:
        tmp = MA_BASELINE_FILE + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(_MA_BASELINE_CACHE, f, ensure_ascii=False)
        os.replace(tmp, MA_BASELINE_FILE)
    except Exception:
        pass


def _sanitize_ma_rows(results: List[Dict]) -> List[Dict]:
    """落盘行: 剔除 bullish_stocks(大体积, 下钻另行取数), 补 count_real =
    截断前多头真值 (= 多头股列表长; 中性/偏空档 bullish_count 是截断展示值)"""
    out = []
    for r in results:
        # v2.97: drill_stocks 同为大体积名单, 一并剔除 (drill_count 是 int, 保留)
        row = {k: v for k, v in r.items() if k not in ('bullish_stocks', 'drill_stocks')}
        row['count_real'] = len(r.get('bullish_stocks') or [])
        out.append(row)
    return out


def _ma_deltas(base_slot: Optional[Dict], results: List[Dict]) -> Dict[str, List[int]]:
    """徽标差值: 每行 vs 基准槽 — 名次差 = 旧名次-新名次(正=升↑), 多头数差 = 真值差。
    基准无此行(新板块/改名)或差值 0 → 不标。返回 {板块名: [名次差, 多头数差]}"""
    if not base_slot:
        return {}
    rows = base_slot.get('rows') or []
    base = {r['name']: (i + 1, r.get('count_real', r.get('bullish_count', 0)))
            for i, r in enumerate(rows)}
    deltas = {}
    for i, r in enumerate(results):
        b = base.get(r['name'])
        if b is None:
            continue
        dr = b[0] - (i + 1)
        # v2.230 (2026-10-26): 原来只认 bullish_stocks，可它会被 _sanitize_ma_rows 剔掉，
        #   一旦传进来的是清洗过的行，len() 恒 0 ⇒ 差值算成「0 - 上次多头数」，
        #   徽标显示「多头 -219」这种荒唐数。先认 count_real / bullish_count 标量，
        #   都没有才退回数 bullish_stocks。
        _cv = r.get('count_real')
        if _cv is None:
            _cv = r.get('bullish_count')
        if _cv is None:
            _cv = len(r.get('bullish_stocks') or [])
        dc = int(_cv) - b[1]
        if dr or dc:
            deltas[r['name']] = [dr, dc]
    return deltas


def _ma_roll_slot(kind: str, lines: str, results: List[Dict],
                  today: str = None, now_ts: str = None) -> Dict:
    """双槽滚动 (force 落盘时调用): 返回新 cur 槽 {date, ts, rows, deltas}
    today = 数据日 (默认 baseline_data_date(), 不是墙钟日); now_ts = 该次刷新的时刻 (展示用)
    两者都可注入 (单测模拟跨日)"""
    d = _load_ma_baseline()
    slot = d.setdefault(kind, {}).setdefault(lines, {'prev': None, 'cur': None})
    cur = slot.get('cur')
    # v2.220: today 语义 = **数据日**(K线末根日), 不是墙钟日 —— 见 baseline_data_date 注释
    today = today or baseline_data_date()
    now_ts = now_ts or datetime.now().strftime('%m-%d %H:%M:%S')
    if cur is None or cur.get('date') != today:
        slot['prev'] = cur   # 跨日 force → 旧 cur 降为 prev (n+1→n 滚动)
        base = cur           # 基准 = 上一交易日的 cur
    else:
        # v5.0 (2026-09-12): 同日重复 force → 基准取 prev (上一交易日), 不再取 cur 自己。
        # 原实现 base=cur 时, 盘后同一天点第二次"更新板块"K线没变 → deltas 全 0 → 徽标整片消失
        # (且 baseline回填 的成果会被一次误点清空)。盘中重复 force 时该口径也更稳:
        # 差值恒为"盘中 vs 上一交易日收盘", 不随手动刷新时机漂移。
        base = slot.get('prev') or cur
    _d = _ma_deltas(base, results)
    # v2.220: 同一数据日重复 force 且没有 prev 可比时 (自愈后/首次落盘当天),
    # base 会退化成 cur 自己 → 差值恒 0, 徽标又整片消失。这种情形保留上一次算好的 deltas
    # (它本来就是"相对上一个数据日"的变化, 数据没变就不该变)。
    if (not _d) and cur and base is cur and cur.get('deltas'):
        _d = dict(cur.get('deltas') or {})
    new_cur = {'date': today, 'ts': now_ts, 'rows': _sanitize_ma_rows(results),
               'deltas': _d}
    slot['cur'] = new_cur
    return new_cur


def ma_baseline_snapshot(kind: str, lines: str):
    """server 读盘路径: 返回 (cur.date, cur.rows, cur.deltas, cur.ts); 无 cur → (None, None, {}, None)
    行 = 最近一次 force 的完整渲染行 (名次序即落盘序), 盘后打开终端直接返回不重算"""
    d = _load_ma_baseline()
    _slot = (d.get(kind) or {}).get(lines) or {}
    cur = _slot.get('cur')
    if not cur:
        return None, None, {}, None
    # v2.229 (2026-10-26): 落盘的 deltas 是「cur 对 prev」比出来的, 所以徽标基准日期
    #   必须报 prev.date, 不能报 cur.date —— 报 cur 会变成「名次较 10-26 上升 5 位」
    #   而实际比的是 10-23, 差一个数据日。上级行业窗口一直报的是 prev 日, 这里对齐。
    prev = _slot.get('prev') or {}
    _bd = prev.get('date') or cur.get('date')
    return (_bd, cur.get('rows') or [], cur.get('deltas') or {}, cur.get('ts'))


def ma_baseline_deltas(kind: str, lines: str, results: List[Dict]):
    """server 现算路径 (盘中/force/无当日快照): 返回 (cur.date, deltas) — 基准 = 最近 force"""
    d = _load_ma_baseline()
    cur = ((d.get(kind) or {}).get(lines) or {}).get('cur')
    if not cur:
        return None, {}
    return cur.get('date'), _ma_deltas(cur, results)


def refresh_trend_data_file(rt: Optional[Dict] = None, persist_ma: bool = False) -> Dict:
    """全量重算4档行业/概念趋势并落盘 (手动"更新板块"/服务器重启预热时调用)
    rt: 盘中实时价快照 (可选, 传了则盘中口径重算)
    persist_ma: v2.61 — 仅"更新板块" force 传 True: 循环内顺带滚动双槽基线
    (4/5/6线; 3线档用户定暂不涉及)并落盘; 启动预热传 False → 自动路径绝不写基线"""
    ind_map, con_map = load_industry_concept_maps()
    data = {'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'lines': {}}
    for lines, mp in LINES_MAP.items():
        try:
            sec = analyze_sector_ma(ind_map, mp, realtime_prices=rt)
            con = analyze_concept_ma(con_map, mp, realtime_prices=rt)
            data['lines'][_trend_key(mp)] = {'sector': sec, 'concept': con}
            if persist_ma and lines in PERSIST_LINES:
                _ma_roll_slot('sector', lines, sec)
                _ma_roll_slot('concept', lines, con)
        except Exception:
            continue
    _save_trend_data(data)
    if persist_ma:
        _save_ma_baseline()
    return data


def _mh_tier_maps(ma_periods=(5, 10, 21, 55), max_age: int = 3600) -> Tuple[dict, dict]:
    """v2.96: 两维的 {板块名: 档位} —— 只收 强多头/偏多 两类 (中性/偏空永不进名单)。

    名单计算的**唯一出处**: load_realtime_multihead 与 load_realtime_multihead_union 都由它派生。
    用行业趋势/概念趋势的合成指数多头判定(默认5/10/21/55, 可传更长周期)
    行情变化时调整均线周期即可, 无需再依赖导出文件
    v2.4缓存: 按均线档位独立缓存(4档), 默认3600秒(1小时)— 用户2026-08-05定(30分钟频繁)
    v2.14 (2026-08-27): 用户方案 — 盘后(15:30后)数据不再自动更新, 直接读落盘文件
    trend_data.json; 盘中照算并覆盖落盘; 手动"更新板块"全刷4档落盘 (不新增文件)
    """
    global _MH_CACHE
    now = time.time()
    hit = _MH_CACHE.get(ma_periods)
    if hit and now - hit[0] < max_age:
        return hit[1], hit[2]
    key = _trend_key(ma_periods)
    if _is_trade_window():
        # 盘中: 行情每分每秒在变 → 照算, 算完覆盖落盘
        ind_map, con_map = load_industry_concept_maps()
        sectors = analyze_sector_ma(ind_map, ma_periods)
        cons = analyze_concept_ma(con_map, ma_periods)
        _update_trend_data(key, sectors, cons)
    else:
        # 盘后: 收盘后数据不变 → 直接读盘不重算 (文件缺失才算一次并落盘)
        d = _load_trend_data()
        if d and key in d.get('lines', {}):
            sectors = d['lines'][key]['sector']
            cons = d['lines'][key]['concept']
        else:
            ind_map, con_map = load_industry_concept_maps()
            sectors = analyze_sector_ma(ind_map, ma_periods)
            cons = analyze_concept_ma(con_map, ma_periods)
            _update_trend_data(key, sectors, cons)
    TIERS = ('强多头', '偏多')
    ind_tier = {r['name']: r['status'] for r in sectors if r['status'] in TIERS}
    con_tier = {r['name']: r['status'] for r in cons if r['status'] in TIERS}
    _MH_CACHE[ma_periods] = (now, ind_tier, con_tier)
    return ind_tier, con_tier


def load_realtime_multihead(ma_periods=(5, 10, 21, 55), max_age: int = 3600) -> Tuple[set, set]:
    """实时多头名单 —— **有强选强版** (v2.11 2026-08-14 语义, 签名不动)。

    行业/概念各自独立判断: 维度内有强多头板块 → 该维度只留强多头(偏多禁选);
    无强多头 → 偏多+强多头都可选。

    ⚠ v2.96 (2026-09-16): 选股主链**已不用它** —— scan() 改走 load_realtime_multihead_union,
      因为硬裁剪会把整个概念维度锁成偏多、看不到"刚形成多头排列"的早期板块。
      本函数保留给旧的单测/诊断脚本 (语义一字未改, 随时可回退)。
    """
    ind_tier, con_tier = _mh_tier_maps(ma_periods, max_age)
    allow_ind = ('强多头',) if '强多头' in ind_tier.values() else ('偏多', '强多头')
    allow_con = ('强多头',) if '强多头' in con_tier.values() else ('偏多', '强多头')
    return ({k for k, v in ind_tier.items() if v in allow_ind},
            {k for k, v in con_tier.items() if v in allow_con})


def load_realtime_multihead_union(ma_periods=(5, 10, 21, 55),
                                  max_age: int = 3600) -> Tuple[set, set, dict, dict]:
    """v2.96 (2026-09-16): 并集版 —— 两维都取 强多头∪偏多, **不再**做「有强选强」的下游硬裁剪。

    改为把每行命中的档位写回行里 (_filter_multihead 写 r['ind_t']/r['con_t']),
    由前端 4 个勾选框决定显示哪几档 (用户 2026-09-16 定)。

    为什么改: 有强选强在"行业有强多头、概念没有"的日子里会把整个概念维度锁死, 偏多被禁选;
    而偏多恰恰常是"板块刚走向强多头排列"的初期 —— 用户要能看到这个早期机会。

    ⚠ 并集只是**放宽显示**, 不破板块铁律: 名单仍只含 强多头/偏多, 中性/偏空永不入选。
    返回 (mh_ind, mh_con, ind_tier, con_tier)。
    """
    ind_tier, con_tier = _mh_tier_maps(ma_periods, max_age)
    return set(ind_tier), set(con_tier), ind_tier, con_tier


def load_industry_status(ma_periods=(5, 10, 21, 55),
                         realtime_prices: Optional[Dict] = None) -> Dict[str, str]:
    """行业档位表 {行业名: 强多头/偏多/中性/偏空} — 涨停突破模式建池用 (v2.66)

    与 load_realtime_multihead 同源同缓存 (analyze_sector_ma): 盘后读 trend_data.json
    落盘件, 盘中/传了实时价则现算 (命中面板刚算完的 _SECTOR_MA_CACHE → 零重复计算)。

    与 multihead 的唯一区别: 不启用「有强选强」偏多禁选 — 本模式 偏多 和 强多头 都直接
    放行 (用户 2026-09-11 定案: 限制行业偏多/强多头只为增胜率, 不做维度内择强)。
    概念不参与本模式筛选, 故不调 analyze_concept_ma。
    """
    key = _trend_key(ma_periods)
    if realtime_prices is None and not _is_trade_window():
        d = _load_trend_data()
        if d and key in d.get('lines', {}):
            return {r['name']: r['status'] for r in d['lines'][key]['sector']}
    try:
        ind_map, _ = load_industry_concept_maps()
        rows = analyze_sector_ma(ind_map, ma_periods, realtime_prices=realtime_prices)
        return {r['name']: r['status'] for r in rows}
    except Exception:
        return {}


PEAK_LABELS = {100: '百日峰', 55: '55日峰', 20: '最近峰'}


def peak_period(kline: List[Dict], price: float):
    """涨停价突破哪一档左峰 — 取最高级别 (涨停突破模式的核心判定, v2.66)

    三档峰值窗口口径与 LeftPeakEngine.quick_left_peak 完全一致 (前 period 日窗口,
    排除当日+最近5日), 因此与趋势主升「左峰」列同源可比 — 同一只票在两个模式里
    报出的左峰价相同。

    窗口嵌套关系保证取第一个命中即最高级别:
    window20 ⊂ window55 ⊂ window100 → max100 >= max55 >= max20
    → price > max100 必同时 > max55/max20, 所以按 (100,55,20) 顺序判即可。

    返回 (周期, 峰价, 距峰%) 或 None (三档都没突破 → 本模式不入选)
      · 百日峰(100) — 破百日窗口最高, 级别最高
      · 55日峰(55)  — 破 55 日窗口最高
      · 最近峰(20)  — 破 20 日窗口最高
    """
    n = len(kline)
    for period in (100, 55, 20):
        if n < period + 6:
            continue      # 上市不足 → 该档窗口不成立 (quick_left_peak 会退化成截断窗口, 不能冒充百日峰)
        pk = LeftPeakEngine.quick_left_peak(kline, period)
        if pk and price > pk:
            return period, pk, round((price - pk) / pk * 100, 1)
    return None


# ============================================================
# 涨停回调 公用件 (v2.67, 2026-09-11)
# ============================================================
PULLBACK_KINDS = {1: '不破收', 2: '不破中', 3: '不破开'}

# v2.145 涨停回调三峰 — 用户定案口径
#   ① 豁免：首个涨停日及其之后整个「涨停+回调」阶段一律不许当峰（用户原话「左峰应该豁免涨停回调这个阶段」）
#   ② 峰定义：前后 5 个交易日最高价都没这天高（与 find_recent_two_peaks 同源）
#   ③ 免疫期：窗口右端再往前扣 5 日（防把紧挨涨停的高点当峰）
#   ④ 三档：最近峰(20日) / 55日峰 / 百日峰 —— 各窗口内**最高**的孤立峰。
#      🔴 v2.145b 修正：初版「从近到远第一个孤立峰」+ recent 窗口=全历史 ⇒ 三档必然同值
#         （窗口嵌套 20⊂55⊂100）。实测 638 只同值率 93.3%~100%；改成「窗口内最高」后降到 14.1%。
_PK_NOH = 5            # no_new_high_days: 前后 5 日无更高
_PK_MIN_AGO = 5        # 免疫期: 排除右端之前 5 日
_PK_SPANS = (('recent', 20), ('d55', 55), ('d100', 100))


def pullback_three_peaks(kline: List[Dict], first_zt_idx: int) -> Dict:
    """三峰 (v2.145, 2026-10-16) — 返回 {'recent':(idx,price)|None, 'd55':..., 'd100':...}

    first_zt_idx = **首个**涨停日在 kline 里的索引 (豁免起点, 含)。
    窗口右端 = first_zt_idx - 1 - _PK_MIN_AGO (首个涨停前一日再往前扣 5 日免疫期)。
    每档 = 该 span 日窗口内**最高**的孤立峰 (前后 _PK_NOH 日无更高)。

    窗口长度不够 span ⇒ 该档返回 None, 前端显示「--」, 不拿截断窗口冒充 55/百日峰
    (与 peak_period 的 `if n < period + 6: continue` 同一道防线)。
    """
    highs = [r['high'] for r in kline]
    n = len(highs)
    right = first_zt_idx - 1 - _PK_MIN_AGO
    out: Dict = {}
    for label, span in _PK_SPANS:
        if right < 0 or right + 1 < span:      # 窗口被豁免期吃掉 → 该档不成立
            out[label] = None
            continue
        lo = right - span + 1
        best = None
        for i in range(lo, right + 1):         # 窗口内最高的孤立峰
            h = highs[i]
            # 峰后 _PK_NOH 日内无更高
            if any(highs[j] > h for j in range(i + 1, min(n - 1, i + _PK_NOH) + 1)):
                continue
            # 峰前 _PK_NOH 日内无更高或等高 (>= 保证唯一, 与既有口径一致)
            if any(highs[j] >= h for j in range(max(0, i - _PK_NOH), i)):
                continue
            if best is None or h > best[1]:
                best = (i, round(float(h), 2))
        out[label] = best
    return out

_ST_CACHE = {'mtime': 0.0, 'codes': frozenset()}


def load_st_codes() -> frozenset:
    """ST/*ST 代码集 (6位) — 读用户导出的 选股池/ST股票.xlsx, mtime 失效缓存

    为什么不能靠涨跌幅反推 (2026-09-11 实证): ST 涨跌幅 5%, 9.8% 的涨停阈值
    看着能自动挡掉它们 —— 但 000610 在 9/10 仍走 10% 板 (+8.54%), 是当晚才被
    实施 *ST 的 (同花顺 18:06 快照与新浪均已提前显示新名 *ST西旅)。它因此一路
    通过涨停判定混进候选池。**名字是唯一可靠判据**, 必须显式过滤。
    """
    try:
        fp = ds.file_path('选股池/ST股票.xlsx', POOL_DIR)
        mt = os.path.getmtime(fp)
    except Exception:
        return _ST_CACHE['codes'] or frozenset()
    if mt == _ST_CACHE['mtime'] and _ST_CACHE['codes']:
        return _ST_CACHE['codes']
    codes = set()
    try:
        with open(fp, encoding='gbk', errors='replace') as fh:
            for line in fh.readlines()[1:]:
                p = line.split('\t')
                if not p:
                    continue
                c = p[0].strip()
                if c[:2] in ('SZ', 'SH'):
                    c = c[2:]
                c = c[:6]
                if len(c) == 6 and c.isdigit():
                    codes.add(c)
    except Exception:
        return _ST_CACHE['codes'] or frozenset()
    if not codes:
        return _ST_CACHE['codes'] or frozenset()      # 解析异常 → 保留上次结果, 绝不当"没有ST"
    _ST_CACHE.update(mtime=mt, codes=frozenset(codes))
    return _ST_CACHE['codes']


_NAMES_CACHE = {'mtime': 0.0, 'map': {}}


def load_stock_name_map() -> Dict[str, str]:
    """代码(6位) → 简称 —— 与 server._load_stock_names 同源 (同花顺标的表)

    v2.182 (2026-10-23): 🔴 修「涨停回调 / 均线粘合 简称列整列显示成代码」
      老实现读 local.snapshot_files 那 5 个导出快照; v2.177 已把导出快照体系整体下线,
      data_sources.json 里这个键被剪掉, 退回默认值时文件名也早改名了
      (实际是 创业板.xlsx / 沪市主板.xlsx …, 默认值写的是 创业板全部股票.xlsx …),
      5 个里只有 ST股票.xlsx 命中 ⇒ 全市场只剩 198 只 ST 有名字,
      其余全落到 name_map.get(num, num) = 直接拿代码当简称。
      server 侧 v2.177 当天就换成了 sector_kline.tickers(), engine 这份漏改
      (铁律 17: 同一假设别散成多份实现) —— 现在两边同源。
      tickers() = 同花顺标的表 5580 只, 7 天文件缓存 + 1 小时内存缓存, 离线可用。
    兜底保留老路径: tickers() 拿不到时才读导出快照 (新旧两套文件名都试),
    宁可少几个名字, 也不能让整个 name_map 变空。
    """
    now = time.time()
    if _NAMES_CACHE['map'] and (now - _NAMES_CACHE['mtime']) < 3600:
        return _NAMES_CACHE['map']

    # ── 主路径: 同花顺标的表 (与 server._load_stock_names 同源) ──────────────
    m: Dict[str, str] = {}
    try:
        from quant_terminal import sector_kline as _sk
        for k, v in (_sk.tickers() or {}).items():
            k = str(k)
            if len(k) == 6 and k.isdigit() and v and k not in m:
                m[k] = str(v)
    except Exception:
        m = {}

    # ── 兜底: 老导出快照 (v2.177 已下线, 仅标的表不可用时救急) ──────────────
    if not m:
        cands = list(ds.get('local', 'snapshot_files', []) or [])
        cands += [
            '选股池/创业板.xlsx', '选股池/科创板.xlsx', '选股池/深市主板.xlsx',
            '选股池/沪市主板.xlsx', '选股池/ST股票.xlsx',
            '选股池/创业板全部股票.xlsx', '选股池/科创板全部股票.xlsx',
            '选股池/深市主板股票.xlsx', '选股池/沪市主板股票.xlsx',
        ]
        seen_fp = set()
        for fn in cands:
            try:
                fp = ds.file_path(fn, POOL_DIR)
            except Exception:
                continue
            if fp in seen_fp or not os.path.exists(fp):
                continue
            seen_fp.add(fp)
            try:
                with open(fp, encoding='gbk', errors='replace') as fh:
                    for line in fh.readlines()[1:]:
                        p = line.split('\t')
                        if len(p) < 2:
                            continue
                        c = p[0].strip()
                        num = c[2:] if c[:2] in ('SH', 'SZ') else c
                        nm = p[1].strip()
                        if len(num) == 6 and num.isdigit() and nm and num not in m:
                            m[num] = nm
            except Exception:
                continue

    if m:
        _NAMES_CACHE.update(mtime=now, map=m)
    return m


def is_chi_board(num: str) -> bool:
    """双创 (创业板 300/301/302 + 科创板 688/689) — 涨停 20%
    (2026-09-11: 补 302 — 深交所新增创业板号段, 如 SZ302132 中航成飞;
     漏了会把它的涨停阈值按 9.8%/中价按+5% 算, 全错)"""
    return num.startswith(('300', '301', '302', '688', '689'))


def limit_threshold(num: str) -> float:
    """涨停判定阈值 (收盘涨幅≥此值算涨停): 主板 9.8 / 双创 19.8
    —— 与 _count_limit30 同口径, 全终端一致"""
    return 19.8 if is_chi_board(num) else 9.8


def limit_span(num: str) -> float:
    """名义涨跌幅上限: 主板 10 / 双创 20 (用于算中间价)"""
    return 20.0 if is_chi_board(num) else 10.0


def pullback_kind(kline: List[Dict], li: int, mid: float):
    """回调类型判定 (v2.67, 2026-09-11 晚间修正为「现价」口径)

    用户定案 (逐字, 修正版):
      · 不破收: **现价** ≥ 最近一个涨停板的收盘价
      · 不破中: 现价 < 涨停收盘价 且 现价 ≥ 涨停板涨幅一半的价
      · 不破开: 现价 < 中间价     且 现价 ≥ 涨停板的开盘价
      · 现价 < 涨停日开盘价 → 本模式不入选

    ⚠ 与首版的关键差异 (必须记住): 首版用「涨停次日起的**最低收盘价**」, 即"回调
      全程从未跌破"; 现改为**只看当下价格** —— 中途跌破过、现在又拉回来的, 按
      "现在站在哪一档"归档。两口径 2026-09-11 实测差 8/18 只 (且首版多淘汰 2 只),
      例: 内蒙一机现价 13.62 > 涨停收 13.41 → 现价口径=不破收, 首版=不破中。

    其余口径不变:
      · 判「破」用**收盘价** (不看盘中最低价, 容忍下影线) —— 盘后=今收, 盘中=实时价
      · 三档固定按 收 → 中 → 开 顺序判; 一字/高开/T字板一律不管 ——
        标签语义自洽(高开板跌到开与中之间报「不破中」= 字面成立), 用户明确说不管
      · 中间价 = 涨停日昨收 × (1 + 涨停幅度/2) —— 主板 +5%, 双创 +10%
        用户原话: "反正涨停那天假设涨幅减半是什么价就为中线"
      · +5% 距离上限由 scan_limit_pullback 兜底 (不破收 档现价可在涨停价上方)

    返回 (档位1/2/3, 标签, {收,中,开}) 或 None (跌破涨停日开盘价 → 本模式不入选)
    """
    C = kline[li]['close']
    O = kline[li]['open']
    if li >= len(kline) - 1:
        return None                                   # 涨停就是最后一根 → 无回调可言
    price = kline[-1]['close']                        # 盘后=今收, 盘中=实时价
    lv = {'close': C, 'mid': mid, 'open': O}
    if price >= C:
        return 1, PULLBACK_KINDS[1], lv
    if price >= mid:
        return 2, PULLBACK_KINDS[2], lv
    if price >= O:
        return 3, PULLBACK_KINDS[3], lv
    return None


def fetch_live_bars(codes: List[str]) -> Dict[str, Dict]:
    """盘中对候选股批量拉实时快照 → {SH600519: {...}}, 成交量已归一到「手」

    ⚠ 单位陷阱 (2026-09-11 查证): datasource 两个 provider 的成交量单位不同 ——
      新浪 fields[8] = **股**, 腾讯 fs[6] = **手**, 而统一入口 fetch_realtime 不做归一。
      盘后拼 live_bar 的旧代码无条件 /100, 只在主源(新浪)应答时正确。
      本模式「今量 ≤ 前20日最高量×60%」直接吃成交量, 若降级到腾讯就会小 100 倍
      → 缩量条件恒成立, 全部候选误通过 (静默, 不报错)。故此处按实际应答的
      provider 折算; source 读不到时按主源(新浪)处理, 与既有代码口径一致。
    """
    out: Dict[str, Dict] = {}
    if not codes:
        return out
    for i in range(0, len(codes), 50):
        batch = codes[i:i + 50]
        sc2code = {}
        for code in batch:
            num = code[2:] if code[:2] in ('SH', 'SZ') else code
            if not num:
                continue
            sc2code[('sh' if num[0] in ('6', '9') else 'sz') + num] = code
        if not sc2code:
            continue
        try:
            q = ds.fetch_realtime(list(sc2code.keys()))
        except Exception:
            continue
        if not q or '_err' in q:
            continue
        try:
            src = (ds.LAST_REALTIME_SOURCE or {}).get('source', '')
        except Exception:
            src = ''
        div = 1.0 if src == 'tencent' else 100.0      # 腾讯=手(原样) / 新浪=股→手
        for sc, v in q.items():
            code = sc2code.get(sc)
            if not code:
                continue
            px = v.get('price') or 0
            if px <= 0:
                continue
            out[code] = {
                'name': v.get('name') or '',
                'open': v.get('open') or 0,
                'close': px,
                'high': v.get('high') or px,
                'low': v.get('low') or px,
                'volume': (v.get('volume') or 0) / div,
            }
    return out


def analyze_concept_ma(con_map: Dict, ma_periods=(5, 10, 21, 55),
                        realtime_prices: Optional[Dict] = None,
                        as_of: Optional[str] = None) -> List[Dict]:
    """概念MA排列分析 (全成分股等权合成, 与板块均线同逻辑)

    概念数多(1000+), 预加载全市场K线tail缓存避免重复读盘
    返回按多头占比降序: {name, bullish_pct, bullish_count, total, status}
    as_of: 'YYYY-MM-DD' 回填口径 (v2.61 补 n-1 日基线) — 预载段统一截断,
    新鲜度判定参照日同步改 as_of; 回填绕缓存(一次性计算不查不set)"""
    if as_of is None:  # v2.61: 缓存仅服务"最新口径"常规调用
        cached = _sector_ma_cache_get('concept', ma_periods, realtime_prices)
        if cached is not None:
            return cached
    from collections import defaultdict as dd
    # 反查概念→成分股
    concept_stocks = dd(list)
    for code_num, cons in con_map.items():
        for c in cons.split('|'):
            c = c.strip()
            if c: concept_stocks[c].append(code_num)
    valid = {con: codes for con, codes in concept_stocks.items() if len(codes) >= 5}

    # 预加载K线缓存 (全部成分股只读一次)
    kline_cache = {}
    for codes in valid.values():
        for c in codes:
            if c in kline_cache: continue
            fpath = os.path.join(KLINE_DIR, f'{_px(c)}.csv')
            if os.path.exists(fpath):
                t = _read_csv_tail(fpath, 250)
                if as_of:  # v2.61 回填: 预载阶段统一截断 (355概念共享, 只截一次)
                    t = [r for r in t if r['date'] <= as_of]
                # v2.152: 预载阶段就叠上临时快照 —— 355 个概念共享同一份 tail,
                #   只叠一次, 不能放到下面按概念循环里 (那样同一只股要叠 355 遍)。
                t = with_intraday(t, c, as_of)
                kline_cache[c] = t

    results = []
    # v2.173: 同板块 —— 概念档位也以官方概念指数均线排列为准
    OFFICIAL_ALIGNED = official_index_aligned(ma_periods)
    for con_name, codes in valid.items():
        stock_turnover = []
        for c in codes:
            tail = kline_cache.get(c)
            if not tail or len(tail) < 21: continue
            # v2.128: 同 analyze_sector_ma —— 新鲜度改走共用判据 (交易日口径)
            if not kline_is_fresh(tail[-1]['date'], as_of): continue
            # v2.3盘中: 实时价覆盖最后一根K线(反映盘中趋势)
            rt = (realtime_prices or {}).get(c)
            if rt and rt > 0:
                tail = list(tail)
                tail[-1] = {'date': 'rt', 'open': tail[-1]['open'], 'close': rt,
                            'high': max(tail[-1]['high'], rt), 'low': min(tail[-1]['low'], rt),
                            'volume': tail[-1]['volume']}
            stock_turnover.append((c, tail))
        if not stock_turnover: continue     # v5.0 (2026-09-12): 成分门槛 3 → 1 (有几只算几只)
        # v2.4 合成口径: 涨幅等权 (同花顺官方: 指数=成分股涨幅算术平均, 2026-08-04)
        # 价格等权 ΣP/N 高价股主导 vs 涨幅等权 ΣΔ%/N 低吸活跃股主导 — 数学不等价
        date_rets = dd(list)
        for c, kline in stock_turnover:
            for j in range(1, len(kline)):
                d = kline[j]['date']; prev = kline[j - 1]['close']
                if prev > 0: date_rets[d].append(kline[j]['close'] / prev - 1)
        composite = []
        idx = 1000.0
        # 'rt' 实时价日排最后 (避免字母序排前错位)
        for date in sorted(date_rets.keys(), key=lambda d: (d == 'rt', d)):
            vals = date_rets[date]
            if len(vals) >= 1:               # v5.0 (2026-09-12): 每日 ≥1 只即计点 (原 ≥5)
                idx *= (1 + sum(vals) / len(vals))
                composite.append({'date': date, 'close': idx})
        need = max(ma_periods)
        _off_aligned = OFFICIAL_ALIGNED.get(con_name)   # v2.173: 官方概念指数
        bullish_stocks = []  # v2.12: 多头排列成分股列表 (供前端点击概念下钻)
        if len(composite) >= need:
            closes_syn = [r['close'] for r in composite]
            mas_syn = {p: sum(closes_syn[-p:]) / p for p in ma_periods}
            seq = [closes_syn[-1]] + [mas_syn[p] for p in ma_periods]
            syn_aligned = all(seq[j] > seq[j + 1] for j in range(len(seq) - 1))
            bullish = 0
            for c, kline in stock_turnover:
                closes = [r['close'] for r in kline]
                if len(closes) < need: continue
                close = closes[-1]
                mas = {p: sum(closes[-p:]) / p for p in ma_periods}
                seq2 = [close] + [mas[p] for p in ma_periods]
                if all(seq2[j] > seq2[j + 1] for j in range(len(seq2) - 1)):
                    bullish += 1
                    bullish_stocks.append(c)
            # v5.0: max(1,…) — 成分 1~2 只时 //3=0 会把自身就多头的成分压成 0 多头 (合成未多头才打折)
            if not (syn_aligned if _off_aligned is None else _off_aligned):
                bullish = min(bullish, max(1, len(stock_turnover) // 3))
        else:
            bullish = 0
            syn_aligned = False  # 修复: 数据不足时未定义 (2026-08-05, 6线档位need=250触发)
            for c, kline in stock_turnover:
                closes = [r['close'] for r in kline]
                if len(closes) < need: continue
                close = closes[-1]
                mas = {p: sum(closes[-p:]) / p for p in ma_periods}
                seq2 = [close] + [mas[p] for p in ma_periods]
                if all(seq2[j] > seq2[j + 1] for j in range(len(seq2) - 1)):
                    bullish += 1
                    bullish_stocks.append(c)
        # v2.97 (2026-09-16): 下钻**名单**口径 —— 只要 现价 > MA5 (均线之间不要求有序)。
        #   单开一趟循环, 与上面的面板计数彻底解耦: 面板那趟在「合成指数不足 need 根」时要求
        #   len(closes) >= need(=55/250), 这里只要 5 根, 且**不参与** 占比/status 的判定。
        drill_stocks = []
        for c, kline in stock_turnover:
            closes = [r['close'] for r in kline]
            if len(closes) < DRILL_MA: continue
            if closes[-1] > sum(closes[-DRILL_MA:]) / DRILL_MA:
                drill_stocks.append(c)
        pct = bullish / len(stock_turnover) * 100
        # v2.173 判定（同板块）: 官方概念指数均线排列定档, 成分股多头占比细分强弱
        _aligned = syn_aligned if _off_aligned is None else bool(_off_aligned)
        if _aligned:
            status = '强多头' if pct >= 40 else '偏多'
        else:
            status = '中性' if pct >= 30 else '偏空'
        # v2.209: 形成日也走官方指数口径 (与档位同一套); 取不到回退合成
        _fseq = _official_series('concept', con_name) if status in ('偏多', '强多头') else None
        formed_date, is_new = _formed_meta(_fseq or composite, ma_periods, status)  # v2.22
        results.append({'name': con_name, 'bullish_pct': round(pct, 0),
                        'bullish_count': bullish, 'total': len(stock_turnover), 'status': status,
                        'formed_date': formed_date, 'is_new': is_new,
                        'drill_count': len(drill_stocks),   # v2.97: 下钻名单真值(站上MA5)
                        'bullish_stocks': bullish_stocks, 'drill_stocks': drill_stocks})
    # v2.4 (2026-08-06): 多头优先排序 — 面板前N个能显示全部多头(占比低的多头不被截断, 医药商业案例)
    # v2.61 (2026-09-10): 确定性排序键 — 占比并列比成分股总数, 再并列比名称GBK序
    results.sort(key=_ma_sort_key)
    if as_of is None:  # v2.61: 回填口径不写缓存
        _sector_ma_cache_set('concept', ma_periods, realtime_prices, results)
    return results


# ============================================================
# 市场情绪 (保留)
# ============================================================

def _regime_from_rows(rows: List[Dict]) -> Optional[Dict]:
    """单序列趋势档位 (末根 = 判定日) — classify_index_regime 与 index_hist_recent 唯一共用实现
    MA21 + ADX(14) + 20日高低; 只看尾部窗口, 前面多长都不影响结果; 行数 <30 返回 None"""
    if not rows or len(rows) < 30:
        return None
    closes = [r['close'] for r in rows]
    highs = [r['high'] for r in rows]; lows = [r['low'] for r in rows]
    ma21 = sum(closes[-21:]) / 21; close = closes[-1]
    adx_vals, plus_di, minus_di = Indicators.adx(rows, 14)
    adx = adx_vals[-1] if adx_vals[-1] else 0
    pdi = plus_di[-1] if plus_di[-1] else 0; mdi = minus_di[-1] if minus_di[-1] else 0
    high_20 = max(highs[-20:]); low_20 = min(lows[-20:])
    if adx > 25:
        if pdi > mdi and close > ma21: regime, icon = '上升趋势', '\U0001f4c8'
        elif mdi > pdi and close < ma21: regime, icon = '下降趋势', '\U0001f4c9'
        else: regime, icon = '震荡', '\U0001f504'
    elif adx > 20:
        regime, icon = ('偏多震荡', '↗') if (pdi > mdi and close > ma21) else ('偏空震荡', '↘')
    else:
        if close > ma21: regime, icon = '底部蓄力', '\U0001f535'
        elif close >= low_20 * 1.05: regime, icon = '低位盘整', '⚪'
        else: regime, icon = '弱势探底', '\U0001f53b'
    return {'regime': regime, 'icon': icon, 'adx': round(adx, 1),
            'close_vs_ma21': round((close - ma21) / ma21 * 100, 2),
            'di_diff': round(pdi - mdi, 1)}


_index_hist_cache = {'ts': 0.0, 'data': None}


def index_hist_recent(n: int = 5) -> Dict[str, List[Dict]]:
    """七大指数「前 n 个交易日」逐日 收盘/涨跌幅/趋势档位 — 顶栏指数悬停浮窗 (v2.70, 2026-09-12)

    排除锚点 = 顶栏此刻显示的那一根: 工作日 9:30 后 = 今日 (盘中live / 盘后收盘),
    周末·盘前 = 本地末根 → 浮窗内容永不与顶栏重复 (用户口径「前五个交易日, 不包括最新的」)。
    趋势档位 = 该日收盘后口径 (截断序列重算, 同 RSRS 逐日回填法): 取该日往前 60 根喂 _regime_from_rows,
    与 classify_index_regime 的 need=60 同窗口 → 历史各日描述与当时顶栏显示的一致。
    纯本地指数日K, 零联网; 60秒类级缓存 (与 classify_index_regime 同节奏)"""
    import time as _t
    if _t.time() - _index_hist_cache['ts'] < 60 and _index_hist_cache['data']:
        return _index_hist_cache['data']
    out = {}
    try:
        _now = datetime.now()
        _open = _now.weekday() < 5 and (_now.hour * 100 + _now.minute) >= 930
        _today = _now.strftime('%Y-%m-%d')
        for code in MarketSentiment.INDEX_MAP:      # 前向引用: 调用时类已定义
            rows = get_index_bars(code, need=n + 60, with_live=True)
            if len(rows) < n + 40:
                continue
            anchor = _today if _open else rows[-1]['date']
            hist = []
            for i in [j for j in range(len(rows)) if rows[j]['date'] < anchor][-n:]:
                d = _regime_from_rows(rows[max(0, i - 59):i + 1])
                prev = rows[i - 1]['close'] if i >= 1 else None
                hist.append({
                    'date': rows[i]['date'],
                    'close': round(rows[i]['close'], 2),
                    'pct': round((rows[i]['close'] - prev) / prev * 100, 2) if prev else None,
                    'regime': d['regime'] if d else ''})
            if hist:
                out[code] = hist
    except Exception:
        pass
    _index_hist_cache.update({'ts': _t.time(), 'data': out})
    return out


class MarketSentiment:
    """市场情绪量化"""

    # v2.360 (用户 m38512): 登记表全集 —— 悬停浮窗的「前5日历史趋势」与趋势档位
    #   要覆盖所有候选指数, 不管它此刻是否显示在顶栏。
    INDEX_MAP = {c: _idxr.short_of(c) for c in _idxr.BAR_CODES}

    def __init__(self):
        self.index_data = {}

    # 指数趋势判别缓存 (60秒): sentiment 每次 fetch 调用 classify_index_regime 2次,
    # 6个腾讯K线串行请求走代理慢, 缓存避免重复拉取 (2026-08-04)
    _regime_cache = {'ts': 0.0, 'data': None}

    def classify_index_regime(self) -> Dict[str, Dict]:
        import time as _t
        if _t.time() - self._regime_cache['ts'] < 60 and self._regime_cache['data']:
            return self._regime_cache['data']
        regimes = {}
        try:
            for sina_code, name in self.INDEX_MAP.items():
                # v2.57 (2026-09-05): 读本地指数日K (缺失自动回补; 盘中当日live合并, 不再每60s
                # 串行拉 7×60 根腾讯K线 — 本地化后零联网, 60s类级缓存语义不变)
                # v2.70 (2026-09-12): 判定式抽到 _regime_from_rows, 与 index_hist_recent 共用同一实现
                rows = get_index_bars(sina_code, need=60, with_live=True)
                d = _regime_from_rows(rows)
                if not d: continue
                regimes[sina_code] = dict(d, name=name)
        except Exception: pass
        self._regime_cache.update({'ts': _t.time(), 'data': regimes})
        return regimes

    def fetch(self) -> Dict:
        # v1 数据源设置 (2026-08-29): 统一走 ds.fetch_realtime (主备切换, 指数/个股同源)
        # v2.360 (用户 m38512): 登记表全集。tencent 通路走 ds.fetch_realtime;
        #   ths 通路(同花顺全A) ds 没有 provider, 单独补 —— 否则顶栏那格永远空。
        codes = list(_idxr.BAR_CODES)
        try:
            q = ds.fetch_realtime(codes)
            for code, v in q.items():
                if code.startswith('_') or code in self.index_data:
                    continue
                self.index_data[code] = {'name': v.get('name', code),
                    'price': float(v.get('price', 0) or 0),
                    'change_pct': round(float(v.get('change_pct', 0) or 0), 2),
                    'amount_yi': round(float(v.get('amount_yi', 0) or 0), 0)}
        except Exception:
            pass
        # v2.360: ths 通路补实时价。ds.fetch_realtime 只有 sina/tencent 两个 provider,
        #   同花顺全A 两边都没有 ⇒ 不补这一手, 顶栏那格就是空白。
        #   现价/涨跌取同花顺分时末点, 昨收取本地日K末根收盘(与顶栏「涨跌幅」口径一致)。
        for _c in codes:
            if _idxr.src_of(_c) != 'ths' or _c in self.index_data:
                continue
            try:
                from quant_terminal import ths_index as _ths_q
                _m = _ths_q.minute(_c)
                if not _m.get('ok'):
                    continue
                self.index_data[_c] = {
                    'name': _m.get('name') or _idxr.name_of(_c),
                    'price': round(float(_m.get('last') or 0), 3),
                    'change_pct': round(float(_m.get('pct') or 0), 2),
                    'amount_yi': 0.0,
                }
            except Exception:
                pass
        total_amount = self.index_data.get('sh000001', {}).get('amount_yi', 0) + \
                       self.index_data.get('sz399001', {}).get('amount_yi', 0)
        # v2.62 (2026-09-10): 量能口径改「成交预测 = 预计全天 − 昨日全天」, 分级改较昨日绝对差值。
        # 旧实现是 total/18000 的绝对水位比 (18000亿硬编码, 0.8~1.2 档宽达 7200亿),
        # 且盘中按「已过时间比例」线性外推 — 早盘严重高估: 9/10 10:00 外推 46000亿,
        # 实际 16471亿 (+179%), 评级由「无量」翻成「爆量」, 情绪分虚高 12 分。
        # 现改用 turnover 的自校准外推 (今日累计 ÷ (昨日同期÷昨日全天)), 同点位误差 -0.6%。
        tv = turnover.context()
        vol_status, vol_score = turnover.grade(tv.get('fcst_delta_yi'))
        vol_note = tv.get('note', '')
        # vol_ratio 语义已变: 预计全天/昨日全天 倍数 (原为 成交额/18000, 无消费方)
        vol_ratio = round((tv.get('est_full_yi') or 0) / tv['prev_full_yi'], 3) \
                    if tv.get('prev_full_yi') else 0
        sectors = fetch_sector_realtime()
        up_count = sum(1 for s in sectors.values() if isinstance(s, dict) and s.get('change_pct', 0) > 0)
        breadth = up_count / len(sectors) * 100 if sectors else 50
        score = 0
        score += vol_score   # 平量=12 锚定旧「正常」档; 缩量侧衰减更快 (无量最差, 见 turnover.grade)
        sh_pct = self.index_data.get('sh000001', {}).get('change_pct', 0)
        if sh_pct > 1.5: score += 25
        elif sh_pct > 0.5: score += 18
        elif sh_pct > 0: score += 12
        elif sh_pct > -0.5: score += 8
        elif sh_pct > -1.5: score += 4
        else: score += 0
        if breadth > 70: score += 20
        elif breadth > 55: score += 14
        elif breadth > 45: score += 8
        elif breadth > 30: score += 5
        else: score += 2
        net_in = sum(s.get('net_flow_yi', 0) for s in sectors.values() if isinstance(s, dict))
        if net_in > 200: score += 12
        elif net_in > 50: score += 8
        elif net_in > -50: score += 5
        else: score += 2
        score = max(0, min(100, score))
        level = '火爆' if score >= 70 else ('活跃' if score >= 50 else ('偏弱' if score >= 35 else ('低迷' if score >= 20 else '冰点')))
        # 大盘MA21状态 (用于买入分级硬门槛)
        index_ma21_status = {}
        for sina_code in ['sh000001', 'sz399001']:
            reg = self.classify_index_regime().get(sina_code, {})
            index_ma21_status[sina_code] = {
                'above_ma21': reg.get('close_vs_ma21', -999) > -0.5,  # 允许0.5%容错
                'regime': reg.get('regime', ''),
            }
        # v2.31 (2026-09-01): 大盘沪深勾选下线 — 前端不再有勾选框,
        # 降级判定改由后端按指数趋势计算 (上证/深证成指=上升趋势→对应市场牌放行)
        regimes = self.classify_index_regime()
        tier_flags = {
            'sh': regimes.get('sh000001', {}).get('regime') == '上升趋势',
            'sz': regimes.get('sz399001', {}).get('regime') == '上升趋势',
        }

        return {'score': score, 'level': level, 'total_amount_yi': total_amount,
                'vol_ratio': round(vol_ratio, 2), 'vol_status': vol_status,
                'vol_note': vol_note,
                'turnover': tv,   # v2.62: 成交预测全套 (顶栏「成交预测:±n亿」+ 5行悬停浮窗)
                'breadth': round(breadth, 1), 'indices': self.index_data,
                'index_regimes': regimes,
                'index_ma21': index_ma21_status,
                'tier': tier_flags}
