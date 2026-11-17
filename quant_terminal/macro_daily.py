# -*- coding: utf-8 -*-
"""顶栏宏观指标日度落盘 (v2.63 2026-09-10)

需求 (用户 2026-09-10 定案):
  · RSRS / 情绪 / 量能 滚动保留最近 5 个交易日, 当天每次刷新覆盖当日行
    v2.94c (2026-09-13): 窗口按「交易日」数, 不是按日历日 —— 周末/节假日不占名额,
    也不许把前面的交易日挤掉。旧版 record() 周末照样写行 + _trim() 按日历日裁,
    一个周末就顶掉一天 (用户看到的「量能历史只剩 3 个交易日」就是这个)。
    今天该不该记一行由 _session_today() 判, 不是交易日直接不写。
  · 悬停「RSRS」或「情绪」→ 同一个浮窗, 两列多行 (RSRS 一列, 情绪一列), 一天一行
  · 悬停「成交预测」或「量能」→ 同一个浮窗, 左列现有「成交额明细」, 右列新增「量能历史」

三个指标各自的落盘方式 (不搞一刀切):
  · 量能历史 — 不落盘。由 turnover.daily() 从成交额日线派生 (min['1500'] + grade())。
    盘中记录会失真 (14:59 的预测值 vs 收盘结算值), 派生永远准, 且冷启动就有 5 天。
  · RSRS 历史 — 落盘 + 可回填。calc_rsrs 只吃 high/low 且窗口 M=600, 把本地指数日K
    截断到历史某日即得「当日收盘后」的 std, 与当天实盘算出的值一致 → 冷启动即有 5 天。
  · 情绪分 — 只能从今天起积累。4 个分项里「市场宽度」「资金流」没有历史数据源,
    拿现存数据硬凑出来的是假分数, 不做。跑满一周浮窗才满 5 行。

当日行 upsert 时机 (server 侧):
  · /api/sentiment — 盘中每 5 秒一刷 → 覆盖当日行
  · /api/status    — RSRS (5分钟缓存) + 盘后定稿
  · 盘后交易日: 成交额结算(complete)后情绪分不再变 → 补算一次并置 final=True, 之后不再重写。
    为什么必须补这一次: 终端整晚开着时最后一次 /api/sentiment 停在 14:59,
    尾盘资金流与收盘值有差, 不补就是一条错的历史。

落盘: quant_terminal/macro_daily.json (原子写 tmp + os.replace, WINDOW=5 天滚动)
冻结便携版: 打包时 MACRO_FILE 需同 MA_BASELINE_FILE / TURNOVER_FILE 一并改锚 ds._app_root()
"""
import os, json, threading
from datetime import datetime

from quant_terminal import turnover

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MACRO_FILE = os.path.join(BASE_DIR, 'quant_terminal', 'macro_daily.json')

SCHEMA = 1
WINDOW = 5                            # 保留最近 5 个交易日
MIN_BARS = 617                        # calc_rsrs 出值下限: M(600) + N(18) - 1
_LOCK = threading.RLock()
_CACHE = {'mtime': -1.0, 'data': None}


# ---------------------------------------------------------------- 落盘
def _today():
    return datetime.now().strftime('%Y-%m-%d')


def _is_weekend(iso):
    """'2026-09-12' → 是不是周末 (纯时钟, 不依赖任何数据源; 解析失败当 False)"""
    try:
        return datetime.strptime(iso, '%Y-%m-%d').weekday() >= 5
    except Exception:
        return False


def _session_today():
    """今天是不是真交易日 (该不该记一行) —— v2.94c

    ① 周末 → 否 (纯时钟, 与 turnover.ensure() 同一条门: 周五之后不许再写行)
    ② turnover 已有今天的分钟数据 → 是 (盘中开盘 1 分钟内就出现; 工作日节假日没有)
       _merge_into 只收非空分钟, 节假日不会留下空壳日期, 所以能放心当判据。
    都不是 (工作日节假日 / 9:30 前 / 行情源断) → 否: 这次不记, 下个轮询再判。
    宁可晚一点有行, 也不能让非交易日占掉窗口里的名额。
    """
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    try:
        rec = (turnover.status().get('days') or {}).get(now.strftime('%Y-%m-%d')) or {}
        return int(rec.get('n') or 0) > 0
    except Exception:
        return False


def _load():
    """读盘 (mtime 缓存, 免每次请求解析 JSON)。返回的 dict 是共享对象, 只读用。"""
    try:
        mt = os.path.getmtime(MACRO_FILE)
    except OSError:
        return {'schema': SCHEMA, 'updated': '', 'days': {}}
    if _CACHE['data'] is not None and mt == _CACHE['mtime']:
        return _CACHE['data']
    try:
        with open(MACRO_FILE, encoding='utf-8') as fh:
            obj = json.load(fh)
        if not isinstance(obj, dict) or not isinstance(obj.get('days'), dict):
            raise ValueError('bad schema')
    except Exception:
        obj = {'schema': SCHEMA, 'updated': '', 'days': {}}
    _CACHE.update(mtime=mt, data=obj)
    return obj


def _save(store):
    store['schema'] = SCHEMA
    store['updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    tmp = MACRO_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as fh:
        json.dump(store, fh, ensure_ascii=False, separators=(',', ':'))
    os.replace(tmp, MACRO_FILE)              # Windows 下 os.replace 原子覆盖
    try:
        _CACHE.update(mtime=os.path.getmtime(MACRO_FILE), data=store)
    except OSError:
        _CACHE.update(mtime=-1.0, data=None)


def _trim(days):
    """滚动覆盖: 只留最新 WINDOW 个交易日 (v2.94c)

    先清周末行 (v2.94c 之前 record() 不设门留下的历史垃圾), 再按条数裁 —— 清掉周末
    之后条数就等于交易日数, 所以「一个周末顶掉一天」不会再发生。
    """
    for k in [k for k in days if _is_weekend(k)]:
        days.pop(k, None)
    for k in sorted(days.keys())[:-WINDOW]:
        days.pop(k, None)


# ---------------------------------------------------------------- 写入
def record(rsrs=None, senti=None, final=False):
    """upsert 当日行 (有值才覆盖, 已 final 的行直接跳过)。返回是否真的落盘。

    rsrs : {'std': float|None, 'mode': str}  — server.get_rsrs_cached()
    senti: {'score': int, 'level': str}      — MarketSentiment().fetch()
    final: 盘后成交额已结算 → 置 final, 之后不再重写
    """
    try:
        if not _session_today():
            return False                     # v2.94c: 非交易日 (周末/节假日) 不记行
        with _LOCK:
            st = _load()
            days = st.setdefault('days', {})
            today = _today()
            row = days.get(today)
            if not isinstance(row, dict):
                row = {}
            if row.get('final'):
                return False                     # 已定稿, 盘中残留的旧值不许再盖回来
            ch = False
            if rsrs and rsrs.get('std') is not None:
                for k, v in (('rsrs_std', rsrs['std']),
                             ('rsrs_mode', rsrs.get('mode') or '未知')):
                    if row.get(k) != v:
                        row[k], ch = v, True
            if senti:
                for k in ('score', 'level'):
                    v = senti.get(k)
                    if v is not None and row.get(k) != v:
                        row[k], ch = v, True
            if final and not row.get('final'):
                row['final'], ch = True, True
            if not ch:
                return False
            row['ts'] = datetime.now().strftime('%H:%M:%S')
            row.pop('backfill', None)            # 有实盘值了, 回填标记去掉
            days[today] = row
            _trim(days)
            _save(st)
            return True
    except Exception:
        return False


def needs_final():
    """当日行是否还没定稿 (盘后补算情绪的触发条件)

    v2.94c: 非交易日直接 False —— 那天根本没有当日行, 谈不上定稿。
    少了这一条, 工作日节假日 (国庆这类) 的晚上会每 5 分钟白补算一次情绪。
    """
    if not _session_today():
        return False
    row = (_load().get('days') or {}).get(_today()) or {}
    return not row.get('final')


def backfill_rsrs(n=WINDOW):
    """回填最近 n 个交易日的 RSRS (截断本地指数日K 逐日重算), 只补缺的日期, 返回补了几行。

    与 server.get_rsrs_cached() 同一套算法 + 同一套 RSRS/MA21 双确认,
    区别只是 with_live=False (不能把今日 live bar 算进历史某天)。
    """
    try:
        from quant_terminal.engine import get_index_bars, calc_rsrs, rsrs_to_mode
        rows = get_index_bars('sh000001', need=MIN_BARS + n, with_live=False)
        if len(rows) < MIN_BARS:
            return 0
        with _LOCK:
            st = _load()
            days = st.setdefault('days', {})
            added = 0
            for i in range(max(MIN_BARS - 1, len(rows) - n), len(rows)):
                sub = rows[:i + 1]
                iso = str(sub[-1].get('date') or '')
                if len(iso) != 10:
                    continue
                row = days.get(iso)
                if isinstance(row, dict) and row.get('rsrs_std') is not None:
                    continue                     # 已有实盘值, 不覆盖
                std = calc_rsrs([{'high': r['high'], 'low': r['low']} for r in sub])
                if std is None:
                    continue
                closes = [r['close'] for r in sub]
                if not isinstance(row, dict):
                    row = {}
                row['rsrs_std'] = std
                row['rsrs_mode'] = rsrs_to_mode(std, closes[-1], sum(closes[-21:]) / 21)
                row['backfill'] = True
                days[iso] = row
                added += 1
            if added:
                _trim(days)
                _save(st)
            return added
    except Exception:
        return 0


def purge_nonsession():
    """清掉落盘里历史遗留的周末行 (v2.94c 之前留下的), 返回清掉几行 —— 自愈入口。

    用途: 换机/重装/拿旧 macro_daily.json 回来时, 把占着窗口名额的周末行一次扫掉。
    正常运行不需要手动跑 —— _trim() 每次落盘顺手清。
    """
    try:
        with _LOCK:
            st = _load()
            days = st.get('days') or {}
            bad = [k for k in days if _is_weekend(k)]
            if not bad:
                return 0
            for k in bad:
                days.pop(k, None)
            _save(st)
            return len(bad)
    except Exception:
        return 0


# ---------------------------------------------------------------- 读取
def view(n=WINDOW):
    """浮窗数据 (前端两个浮窗共用一次调用, 体积 ~0.5KB)。

    rsrs_senti: [{date, std, mode, score, level}] 由新到旧 — 浮窗A 两列
    vol       : [{date, status, delta_yi, full_yi}] 由新到旧 — 浮窗B 右列
    两者按日期对齐: 同一行的 RSRS 列与情绪列必是同一天, 缺的那格前端显示 '--'。
    """
    days = _load().get('days') or {}
    vol = {v['date']: v for v in turnover.daily(n)}
    # v2.94c: 只从「非周末」的键里取窗口 —— 周末行不许占名额 (历史遗留的行也不许)
    keys = [k for k in sorted(set(days) | set(vol)) if not _is_weekend(k)][-n:]
    keys.reverse()                               # 由新到旧
    a, b = [], []
    for d in keys:
        r = days.get(d) or {}
        md = d[5:]                               # '2026-09-10' → '09-10'
        a.append({'date': md, 'std': r.get('rsrs_std'), 'mode': r.get('rsrs_mode'),
                  'score': r.get('score'), 'level': r.get('level')})
        v = vol.get(d)
        if v:
            b.append({'date': md, 'status': v['status'],
                      'delta_yi': v['delta_yi'], 'full_yi': v['full_yi']})
    return {'rsrs_senti': a, 'vol': b}


def status():
    """落盘概览 (自检 / 排查用)"""
    st = _load()
    return {'file': MACRO_FILE, 'updated': st.get('updated', ''),
            'days': {d: dict(r or {}) for d, r in sorted((st.get('days') or {}).items())}}


if __name__ == '__main__':
    import sys
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    if len(sys.argv) > 1 and sys.argv[1] == 'backfill':
        print('回填行数:', backfill_rsrs())
    if len(sys.argv) > 1 and sys.argv[1] == 'purge':
        print('清掉周末行:', purge_nonsession())
    print(json.dumps(status(), ensure_ascii=False, indent=2))
    print(json.dumps(view(), ensure_ascii=False, indent=2))
