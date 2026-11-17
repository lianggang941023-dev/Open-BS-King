"""
龙头战法 v2.16 (2026-08-27)
================================
连板妖龙筛选 + 龙回头低吸 (用户确认方向: 妖龙低吸, 非趋势龙)

- 妖龙榜: 当日涨停池主板票, 妖性评分 (连板/涨停史/封板质量/换手/板块热度)
- 龙回头池: 全市场K线扫描妖龙回调形态
  (妖性>=3涨停 + 见顶2-10交易日 + 回撤10-30% + 横盘段缩量 + 企稳信号)
- 助攻因子: 创业板/科创板/北交所涨停 = 板块热度验证, 不进龙头候选
- 每日落盘: dragon_data/limit_up_history.csv (涨停池快照积累, 未来自算晋级率)

数据源: 东财涨停池 push2ex (与MCP get_limit_up_down同源;
        2026-09-28起自建请求 ds.zt_pool_em: 直连优先+超时, 绕开 akshare 的裸 requests 走系统代理)
        2026-08-27已验证与同花顺"今日涨停"78家口径一致, 唯一差异=ST被自动排除)

一票否决(妖龙低吸铁律):
- 回撤>30% 或 跌破20日线 = 资金出逃不是洗盘
- 烂板天量: 炸板>5次 或 换手>35% (妖龙榜标记, 龙回头池当日涨停票不重复入选)
- 龙头仅限主板: 600/601/603/605 + 000/001/002/003 (参与门槛决定群众基础)
"""

import os, sys
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)
from quant_terminal import datasource as ds
from quant_terminal.engine import load_concept_blocklist, _strip_blocked_concepts  # v2.58: 概念屏蔽名单
DRAGON_DATA_DIR = os.path.dirname(ds.resolve(ds.get('local', 'limit_history', 'dragon_data/limit_up_history.csv')))
LIMIT_HISTORY_FILE = os.path.join(DRAGON_DATA_DIR, 'limit_up_history.csv')

MAIN_BOARD_PREFIXES = ('600', '601', '603', '605', '000', '001', '002', '003')

_LIMIT_CACHE: Dict = {}  # {'date': 'YYYYMMDD', 'rows': [...]}

# 涨停池实际使用源 (v1 数据源设置 2026-08-29):
# provider=akshare_em 正常 / degraded=True = 已降级快照识别 (无封板质量/连板数据, 前端须提示)
LIMIT_SOURCE: Dict = {'provider': 'akshare_em', 'degraded': False, 'reason': '', 'ts': 0.0}


# ============================================================
# 字段清洗
# ============================================================
def _f(x, default: float = 0.0) -> float:
    try:
        v = float(x)
        return v if v == v else default  # NaN → default
    except Exception:
        return default


def _i(x, default: int = 0) -> int:
    try:
        v = int(_f(x, 0))
        return v
    except Exception:
        return default


def _s(x, default: str = '') -> str:
    if x is None:
        return default
    s = str(x).strip()
    return s if s and s.lower() != 'nan' else default


# ============================================================
# 板块判定
# ============================================================
def board_of(code: str) -> str:
    """6位代码 → 板块. 龙头仅限主板 (参与门槛: 创业/科创/北交散户参与不了难成龙)"""
    if code.startswith(MAIN_BOARD_PREFIXES):
        return '主板'
    if code.startswith(('300', '301', '302')):   # 302 = 深交所新增创业板号段 (2026-09-11 补)
        return '创业板'
    if code.startswith(('688', '689')):
        return '科创板'
    return '北交所'  # 920/43/83/87/88等


# ============================================================
# 涨停池获取 (东财接口直连, 与MCP get_limit_up_down同源)
# ============================================================
def _in_intraday(now: Optional[datetime] = None) -> bool:
    """交易日盘中判定 (9:30-11:30 / 13:00-15:00, 周一~五) — 涨停数 TTL 只在盘中生效"""
    now = now or datetime.now()
    if now.isoweekday() > 5:
        return False
    t = now.hour * 60 + now.minute
    return (9 * 60 + 30) <= t < (11 * 60 + 30) or (13 * 60) <= t < (15 * 60)


def _fetch_limit_rows() -> List[Dict]:
    """涨停池拉取本体 (无缓存): 东财直连 → 失败自动降级快照识别 (v1 数据源设置);
    两者都失败 raise (LIMIT_SOURCE 已记录原因与降级状态)"""
    today = datetime.now().strftime('%Y%m%d')
    # v2.190 (2026-10-23 用户定案): **东财为主、同花顺只做补充**。
    #   东财池有换手率/首次封板时间(到秒)/炸板次数/封板资金/所属行业；
    #   同花顺池独有「涨停原因 / 连板文本 / 峰值封单额」⇒ 东财成功后按代码补这三样。
    #   东财整池失败时才用同花顺池顶替，再不行才快照识别。
    try:
        df = ds.zt_pool_em(today)   # v2.129: 自建请求 (直连优先+超时), 不再走 akshare
        rows = []
        for r in df:
            code = _s(r.get('代码'))
            if len(code) != 6 or not code.isdigit():
                continue
            rows.append({
                'code': code,
                'name': _s(r.get('名称'), code),
                'pct': _f(r.get('涨跌幅')),
                'price': _f(r.get('最新价')),
                'turnover': _f(r.get('换手率')),
                'seal_money': _f(r.get('封板资金')),
                'flow_mv': _f(r.get('流通市值')),
                'first_time': _s(r.get('首次封板时间')),
                'last_time': _s(r.get('最后封板时间')),
                'zhaban': _i(r.get('炸板次数')),
                'lianban': _i(r.get('连板数')),
                'industry': _s(r.get('所属行业')),
                'board': board_of(code),
            })
        LIMIT_SOURCE.update({'provider': 'akshare_em', 'degraded': False,
                              'reason': '', 'ts': datetime.now().timestamp()})
        # v2.190: 东财没有「涨停原因」，向同花顺要一份按代码补进来（失败不影响主数据）
        try:
            try:
                from . import fuyao_limit as _fl2
            except Exception:
                import fuyao_limit as _fl2
            if _fl2.configured():
                _fr, _ = _fl2.limit_up_pool()
                _sup = {x.get('code'): x for x in (_fr or [])}
                _n_fill = 0
                for r in rows:
                    s2 = _sup.get(r['code'])
                    if not s2:
                        continue
                    if s2.get('reason'):
                        r['reason'] = s2['reason']; _n_fill += 1
                    r['lianban_text'] = s2.get('lianban_text', '') or ''
                    r['max_seal'] = s2.get('max_seal', 0) or 0
                LIMIT_SOURCE['reason_src'] = 'fuyao(%d只)' % _n_fill
        except Exception as _e2:
            LIMIT_SOURCE['reason_src'] = 'fuyao失败(%s)' % str(_e2)[:30]
        return rows
    except Exception as e:
        # v2.190: 东财失败 → 先试同花顺涨停池顶替（带原因/连板文本，但缺换手率与炸板次数）
        try:
            try:
                from . import fuyao_limit as _fl3
            except Exception:
                import fuyao_limit as _fl3
            if _fl3.configured():
                _rows3, _ = _fl3.limit_up_pool()
                if _rows3:
                    LIMIT_SOURCE.update({'provider': 'fuyao', 'degraded': True,
                                         'reason': '东财涨停池失败(%s), 改用同花顺' % str(e)[:40],
                                         'ts': datetime.now().timestamp()})
                    return _rows3
        except Exception:
            pass
        # 再降级: 从同花顺快照识别当日涨停 (现价/昨收≥阈值; 北交所不在快照5文件内)
        snaps = ds.snapshot_zt_codes()
        if not snaps:
            # v2.68 (2026-09-12): 不再只说"无涨停数据" — 把快照表头异常透传出去。
            # 旧版按列位置读, 用户精简表头后整行被长度检查挡掉, 表现与"今天真没涨停"完全一样。
            _sk = getattr(ds, '_ZT_DIAG', {}).get('skipped') or []
            why = ('快照表头异常: ' + '; '.join(f'{f} {r}' for f, r in _sk)) if _sk else '快照无涨停数据'
            LIMIT_SOURCE.update({'provider': '', 'degraded': True,
                                 'reason': f'涨停池失败({str(e)[:40]})且{why}', 'ts': datetime.now().timestamp()})
            raise
        rows = [{'code': s['code'], 'name': s['name'], 'pct': s['pct'], 'price': s['price'],
                 'turnover': 0, 'seal_money': 0, 'flow_mv': 0,
                 'first_time': '', 'last_time': '', 'zhaban': 0, 'lianban': 0,
                 'industry': '', 'board': board_of(s['code'])} for s in snaps]
        LIMIT_SOURCE.update({'provider': 'snapshot', 'degraded': True,
                             'reason': f'涨停池失败({str(e)[:40]}), 已用快照识别{len(rows)}只涨停 (无封板质量/连板)',
                             'ts': datetime.now().timestamp()})
        return rows


def fetch_limit_pool(force: bool = False, max_age: int = 0) -> List[Dict]:
    """全市场当日涨停池. 当日缓存; 东财直连失败 → 自动降级快照识别 (v1 数据源设置)
    返回 [{code,name,pct,price,turnover,seal_money,flow_mv,
           first_time,last_time,zhaban,lianban,industry,board}]
    降级时: LIMIT_SOURCE['degraded']=True, 无封板质量/连板字段 (调用方/前端须提示)

    v2.48 (2026-09-04): 加 force/max_age 刷新控制 (行业涨停数列用):
    - 默认 (force=False, max_age=0) 行为与旧版完全一致 = 当日只拉一次
    - 盘中 max_age>0: 缓存超过 max_age 秒自动重拉; 盘后/周末恒当日缓存 (收盘数据当日不变, 重拉浪费)
    - force=True: 无视缓存强制重拉 (「更新板块」按钮联动)
    - 重拉失败但有旧缓存 → 返回旧缓存不打断; 无旧缓存 → raise"""
    global _LIMIT_CACHE
    now = datetime.now()
    today = now.strftime('%Y%m%d')
    cached = _LIMIT_CACHE.get('date') == today and _LIMIT_CACHE.get('rows')
    if cached and not force:
        age = now.timestamp() - _LIMIT_CACHE.get('ts', 0)
        if not _in_intraday(now) or max_age <= 0 or age < max_age:
            return _LIMIT_CACHE['rows']
    try:
        rows = _fetch_limit_rows()
    except Exception:
        if cached:
            return _LIMIT_CACHE['rows']   # 网络失败 → 旧缓存兜底 (不打断)
        raise
    _LIMIT_CACHE = {'date': today, 'rows': rows, 'ts': now.timestamp()}
    return rows


# ============================================================
# 每日落盘 (涨停池快照积累 → 未来自算晋级率/回测)
# ============================================================
def save_limit_history(rows: List[Dict]) -> str:
    """涨停池快照落盘 dragon_data/limit_up_history.csv (幂等: 当日已写跳过)
    返回文件路径"""
    os.makedirs(DRAGON_DATA_DIR, exist_ok=True)
    today = datetime.now().strftime('%Y-%m-%d')
    if os.path.exists(LIMIT_HISTORY_FILE):
        try:
            with open(LIMIT_HISTORY_FILE, 'r', encoding='utf-8-sig') as f:
                for line in f:
                    if line.startswith(today + ','):
                        return LIMIT_HISTORY_FILE  # 当日已写
        except Exception:
            pass
    header = ('date,code,name,board,pct,price,turnover,seal_money,flow_mv,'
              'first_time,last_time,zhaban,lianban,industry')
    is_new = not os.path.exists(LIMIT_HISTORY_FILE)
    with open(LIMIT_HISTORY_FILE, 'a', encoding='utf-8-sig', newline='') as f:
        if is_new:
            f.write(header + '\n')
        for r in rows:
            f.write('{},{},{},{},{:.2f},{:.2f},{:.2f},{:.0f},{:.0f},{},{},{},{},{}'.format(
                today, r['code'], r['name'].replace(',', '·'), r['board'],
                r['pct'], r['price'], r['turnover'], r['seal_money'], r['flow_mv'],
                r['first_time'], r['last_time'], r['zhaban'], r['lianban'],
                r['industry'].replace(',', '·')) + '\n')
    return LIMIT_HISTORY_FILE


# ============================================================
# 板块热度 (助攻因子: 创/科/北涨停 = 板块强度证据, 非龙头候选)
# ============================================================
def sector_heat(rows: List[Dict]) -> Dict:
    """行业 → {zt_total 涨停总数, main_cnt 主板数, assist_cnt 助攻数,
              max_lianban, first_cnt 首板数}"""
    heat = defaultdict(lambda: {'zt_total': 0, 'main_cnt': 0, 'assist_cnt': 0,
                                'max_lianban': 0, 'first_cnt': 0})
    for r in rows:
        h = heat[r['industry']]
        h['zt_total'] += 1
        if r['board'] == '主板':
            h['main_cnt'] += 1
        else:
            h['assist_cnt'] += 1
        if r['lianban'] > h['max_lianban']:
            h['max_lianban'] = r['lianban']
        if r['lianban'] <= 1:
            h['first_cnt'] += 1
    return dict(heat)


# ============================================================
# 涨停统计 v2.48/v2.56f (2026-09-04: 主表「涨停」列 + 行业全量弹窗; 09-05: + 概念全量弹窗)
# 口径: 当日涨停池 × 本地同花顺行业/概念 map 归属 → 与板块面板同名同源;
#       ST 涨停东财池不含 / 北交所无 map 归属 → 均与板块面板口径一致;
#       概念侧 concept_map.csv 值 pipe'|'分隔 → 一只涨停股可计入其所属多个概念
# ============================================================
_IND_MAP_CACHE: Dict = {'mtime': -1.0, 'map': {}}


def _load_industry_map() -> Dict:
    """code → 行业 (industry_map.csv 同花顺唯一权威, 按 mtime 失效缓存)"""
    fpath = ds.resolve(ds.get('local', 'industry_map', 'kline_data/industry_map.csv'))
    try:
        mt = os.path.getmtime(fpath)
        if _IND_MAP_CACHE['mtime'] == mt and _IND_MAP_CACHE['map']:
            return _IND_MAP_CACHE['map']
    except Exception:
        return _IND_MAP_CACHE['map'] or {}
    out = {}
    try:
        import csv
        with open(fpath, 'r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                code = (row.get('代码') or '').strip()
                val = (row.get('行业') or '').strip()
                if code and code.isdigit():
                    out[code] = val
        _IND_MAP_CACHE.update({'mtime': mt, 'map': out})
    except Exception:
        pass
    return out


_CON_MAP_CACHE: Dict = {'mtime': -1.0, 'map': {}}


def _load_concept_map() -> Dict:
    """code → 概念串 (concept_map.csv 同花顺唯一权威, pipe'|'分隔多概念, 按 mtime 失效缓存)"""
    fpath = ds.resolve(ds.get('local', 'concept_map', 'kline_data/concept_map.csv'))
    try:
        mt = os.path.getmtime(fpath)
        if _CON_MAP_CACHE['mtime'] == mt and _CON_MAP_CACHE['map']:
            return _CON_MAP_CACHE['map']
    except Exception:
        return _CON_MAP_CACHE['map'] or {}
    out = {}
    try:
        import csv
        bl = load_concept_blocklist()  # v2.58: 与 engine 同名单同口径 — 涨停概念统计不显示屏蔽概念
        with open(fpath, 'r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                code = (row.get('代码') or '').strip()
                val = (row.get('概念') or '').strip()
                if code and code.isdigit() and val:
                    out[code] = _strip_blocked_concepts(val, bl)
        _CON_MAP_CACHE.update({'mtime': mt, 'map': out})
    except Exception:
        pass
    return out


def fetch_sector_zt(force: bool = False, max_age: int = 120) -> Dict:
    """行业/概念 → 当日涨停统计 (主表「涨停」列 + 行业/概念全量弹窗).
    涨停池刷新策略见 fetch_limit_pool: 盘中≤max_age秒自动重拉, 盘后当日缓存, force 强刷.
    v2.56f: 概念侧与行业同源同构 — 涨停池 × concept_map → 每概念独立计数 (一票可计多概念).
    返回 {'zt': {行业名:...}, 'zt_concept': {概念名:...}, 结构同为
           {名: {'n': 涨停家数, 'members': [{code,name,lianban,board}]}},
           'degraded'/'provider'/'reason': 涨停池数据源状态, 'ts': 完成时间戳}"""
    rows = fetch_limit_pool(force=force, max_age=0 if force else max_age)
    ind = _load_industry_map()
    con = _load_concept_map()
    out: Dict = {}
    c_out: Dict = {}
    for r in rows:
        code = r['code']
        mb = {'code': code, 'name': r['name'], 'lianban': r['lianban'], 'board': r['board']}
        sec = ind.get(code, '')
        if sec:
            o = out.setdefault(sec, {'n': 0, 'members': []})
            o['n'] += 1
            o['members'].append(mb)
        for cname in (x for x in (con.get(code) or '').split('|') if x):
            o2 = c_out.setdefault(cname, {'n': 0, 'members': []})
            o2['n'] += 1
            o2['members'].append(mb)
    for o in out.values():
        o['members'].sort(key=lambda m: -m['lianban'])  # 梯队: 高连板在前
    for o in c_out.values():
        o['members'].sort(key=lambda m: -m['lianban'])
    return {'zt': out, 'zt_concept': c_out,
            'degraded': bool(LIMIT_SOURCE.get('degraded')),
            'provider': _s(LIMIT_SOURCE.get('provider'), ''),
            'reason': _s(LIMIT_SOURCE.get('reason'), ''),
            'ts': datetime.now().timestamp()}


# ============================================================
# 妖性统计 (本地K线)
# ============================================================
def count_limit20(kline: List[Dict], code: str = '') -> int:
    """前 20 个交易日的涨停次数 —— **不含今天**(分板块阈值: 主板>=9.8%, 双创>=19.8%)

    v2.170 (2026-10-21, 用户 m00524 定口径): 「今天之前的 20 个交易日」，当天不算。
      旧实现取「K线最后 20 根」——
        · 盘中最后一根=昨天  ⇒ 天然不含今天，读数正确
        · 15:35 写库后最后一根=今天 ⇒ 把今天也数进去，同一只票盘中/盘后读数不一致
      现在统一：若最后一根的 date == 今天，先剔除再往前取 20 根；
      盘中(最后一根≠今天)不受影响，两种情况读数完全一致。
    """
    if not kline:
        return 0
    rows = kline
    try:
        _last = str(rows[-1].get('date') or '')[:10]
        if _last and _last == datetime.now().strftime('%Y-%m-%d'):
            rows = rows[:-1]          # 今天那根不进「前20日」统计
    except Exception:
        pass
    if len(rows) < 20:
        return 0
    th = 19.8 if board_of(code[-6:]) in ('创业板', '科创板') else 9.8
    cnt = 0
    for i in range(max(1, len(rows) - 20), len(rows)):
        prev = rows[i - 1]['close']
        cur = rows[i]['close']
        if prev > 0 and (cur - prev) / prev * 100 >= th:
            cnt += 1
    return cnt


# ============================================================
# 妖龙榜评分 (当日涨停主板票, 0-100)
# ============================================================
def score_dragon(row: Dict, kline: List[Dict]) -> Tuple[int, List[str]]:
    """妖性评分: 连板30 + 涨停史20 + 封板质量30 + 换手10 + 板块热度10"""
    s = 0
    det = []
    # 1. 连板 (30)
    lb = row['lianban']
    if lb >= 3:
        s += 30; det.append(f'{lb}连板')
    elif lb == 2:
        s += 25; det.append('2连板')
    elif lb == 1:
        s += 15; det.append('首板')
    # 2. 近20日涨停史 (20)
    n20 = count_limit20(kline, row['code'])
    if n20 >= 4:
        s += 20; det.append(f'20日涨停x{n20}')
    elif n20 == 3:
        s += 15; det.append(f'20日涨停x{n20}')
    elif n20 == 2:
        s += 10; det.append(f'20日涨停x{n20}')
    # 3. 封板质量 (30)
    ft = _fmt_time(row['first_time'])
    if ft and ft <= 930:
        s += 15
    elif ft and ft <= 1000:
        s += 10
    if row['zhaban'] == 0:
        s += 10
    elif row['zhaban'] == 1:
        s += 5
    if row['flow_mv'] > 0 and row['seal_money'] / row['flow_mv'] >= 0.05:
        s += 5
    # 4. 换手率 8-25% (10)
    if 8 <= row['turnover'] <= 25:
        s += 10
    return min(100, s), det


def _fmt_time(t: str) -> Optional[int]:
    """'092500' → 925; 空/异常 → None"""
    if len(t) >= 4 and t.isdigit():
        try:
            return int(t[:4])
        except Exception:
            return None
    return None


# ============================================================
# 龙回头形态检测 (妖龙回调低吸)
# ============================================================
def check_pullback(tail: List[Dict]) -> Optional[Dict]:
    """妖龙回调检测. tail = 至少90行K线 (升序, 每行带code字段)
    通过条件:
      - 妖性: 近20日涨停>=3
      - 见顶: 20日高点在2-10个交易日前
      - 回撤: 高点回撤 10-30% (>30% = 资金出逃, 一票否决)
      - 缩量: 横盘段(最近3日含今日)均量 < 一波均量*0.9
      - 企稳: 缩量小阳/十字星/横盘 (信号分级)
    返回 None 或 {price,peak,peak_date,pullback_pct,days_since,
                  shrink,stable_shrink,ma10,ma21,signal,stop_loss,
                  limit20,today_vol_ratio}
    """
    n = len(tail)
    if n < 90:
        return None
    code = tail[-1].get('code', '')
    closes = [r['close'] for r in tail]
    th = 19.8 if board_of(code[-6:]) in ('创业板', '科创板') else 9.8

    # 1. 妖性: 近20日涨停>=3
    cnt20 = 0
    for i in range(max(1, n - 20), n):
        if closes[i - 1] > 0 and (closes[i] / closes[i - 1] - 1) * 100 >= th:
            cnt20 += 1
    if cnt20 < 3:
        return None

    # 2. 见顶: 20日高点 (不含今天)
    win = tail[-21:-1]
    pi = max(range(len(win)), key=lambda k: win[k]['high'])
    hi_idx = n - 1 - 20 + pi
    peak = win[pi]['high']
    days_since = n - 1 - hi_idx
    if not (2 <= days_since <= 10):
        return None

    # 3. 回撤 10-30%
    px = closes[-1]
    pullback = (peak - px) / peak * 100
    if not (10 <= pullback <= 30):
        return None

    # 4. 量能: 一波起点 = 见顶前40日窗口最低点
    lo_idx = min(range(max(0, hi_idx - 40), hi_idx + 1), key=lambda k: tail[k]['low'])
    up_len = hi_idx - lo_idx + 1
    up_vol = sum(r['volume'] for r in tail[lo_idx:hi_idx + 1]) / up_len
    # 回调期均量 (见顶次日~昨天) + 横盘段均量 (最近3日含今日)
    back = tail[hi_idx + 1:-1]
    if not back:
        return None
    back_vol = sum(r['volume'] for r in back) / len(back)
    stable = tail[max(0, n - 3):]
    stable_vol = sum(r['volume'] for r in stable) / len(stable)
    shrink = back_vol / up_vol if up_vol > 0 else 9.9
    stable_shrink = stable_vol / up_vol if up_vol > 0 else 9.9
    if stable_shrink > 0.9:
        return None  # 横盘段必须缩量 (跌停急洗后横盘也算)

    # 5. 企稳信号分级
    m10 = sum(closes[-10:]) / 10
    m21 = sum(closes[-21:]) / 21
    today = tail[-1]
    prev = tail[-2]
    t_chg = (today['close'] / prev['close'] - 1) * 100 if prev['close'] > 0 else 0
    t_ratio = today['volume'] / up_vol if up_vol > 0 else 9.9
    body = abs(today['close'] - today['open']) / today['close'] * 100

    if t_ratio >= 1.5 and t_chg > 0:
        signal = '放量启动'          # 已启动, 低吸窗口已过, 只观察
    elif t_ratio >= 1.0 and t_chg >= 5:
        signal = '放量启动'
    elif stable_shrink <= 0.5 and days_since >= 3:
        signal = '缩量企稳'          # 强信号
    elif stable_shrink <= 0.9 and days_since >= 3:
        signal = '横盘企稳'          # 次信号 (跌停急洗型)
    elif body <= 1.5 and t_ratio <= 1.0:
        signal = '十字星企稳'
    else:
        signal = '回调中'

    # 6. 止损: 回调低点下方3%
    recent_low = min(r['low'] for r in tail[hi_idx + 1:])
    stop_loss = round(recent_low * 0.97, 2)

    return {
        'price': round(px, 2),
        'peak': round(peak, 2),
        'peak_date': tail[hi_idx]['date'],
        'pullback_pct': round(pullback, 1),
        'days_since': days_since,
        'shrink': round(shrink, 2),
        'stable_shrink': round(stable_shrink, 2),
        'ma10': round(m10, 2),
        'ma21': round(m21, 2),
        'signal': signal,
        'stop_loss': stop_loss,
        'limit20': cnt20,
        'today_vol_ratio': round(t_ratio, 2),
    }


# ============================================================
# 龙回头评分 (0-100)
# ============================================================
def score_pullback(pb: Dict, sector_ok: bool) -> Tuple[int, List[str]]:
    """妖性25 + 回撤位置20 + 缩量20 + 企稳20 + 均线15 + 板块多头10"""
    s = 0
    det = []
    # 1. 妖性 (25)
    n20 = pb['limit20']
    if n20 >= 5:
        s += 25; det.append(f'20日涨停x{n20}')
    elif n20 == 4:
        s += 20; det.append(f'20日涨停x{n20}')
    else:
        s += 15; det.append(f'20日涨停x{n20}')
    # 2. 回撤位置 (20): 越浅越好 (10-20%最佳)
    p = pb['pullback_pct']
    if 10 <= p <= 20:
        s += 20
    else:
        s += 15
    det.append(f'回撤{p}%')
    # 3. 缩量 (20): 横盘段
    ss = pb['stable_shrink']
    if ss <= 0.5:
        s += 20
    elif ss <= 0.7:
        s += 15
    else:
        s += 10
    det.append(f'缩量{ss}')
    # 4. 企稳 (20)
    sig = pb['signal']
    if sig == '缩量企稳':
        s += 20
    elif sig == '横盘企稳':
        s += 12
    elif sig == '十字星企稳':
        s += 10
    else:
        s += 5  # 回调中/放量启动
    det.append(sig)
    # 5. 均线 (15)
    if pb['price'] >= pb['ma10']:
        s += 15
    elif pb['price'] >= pb['ma21']:
        s += 8
    # 6. 板块多头 (10) — 板块铁律: 必须在多头行业
    if sector_ok:
        s += 10
        det.append('板块多头')
    return min(100, s), det
