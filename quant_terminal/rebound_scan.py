# -*- coding: utf-8 -*-
"""超跌反弹 · 主逻辑  (2026-11-07 新增模块, v2.257)

⚠ 本文件是「超跌反弹」模式的**全部业务逻辑**, 与终端现有功能零耦合:
   - 不 import engine (只借用它的 _read_csv_tail 读本地 CSV 的小函数, 缺失时自读)
   - 不写任何全局状态; 结果由 server.py 存进既有的 SCAN_STATE / scan_cache 槽
   - 摘除方式见 _uninstall_rebound.py; 压舱开关见 rebound_off.flag

===============================================================================
策略口径 —— 全部由用户 2026-11-07 逐条拍板, 阈值一个不改
===============================================================================
【共用条件】
  ① 跌幅: **近 5 日最低价** 距 **百日最高价(极值, 非收盘)** ≤ −30%
  ② 现价 > MA5                        (站上 5 日线)
  ③ 三变体**并集**(满足任一即入池):
       ①  百低锚点之后的**最低价**从未跌破百低 (均为极值)
       ②  百低锚点之后的**收盘价**从未跌破百低
       ③  收盘价跌破百低但**不超过 3%**, 且两日内收回 (现价 > 百低)
      ⚠ ①∩②∩③ 恒为空集: ③ 要求"破过"、①②要求"没破", 天然互斥。
  ④ 百高距今 ≥ 20 个交易日          (防呆, 实测只砍 5 只, 见会话状态)
  ⑤ near  = (近5日最低 − 百低) / 百低 ≤ **+2%**      ← 摸底贴不贴
  ⑥ bounce= (现价 − 近5日最低) / 近5日最低 ≤ **+13%** ← 反弹太多=追高
  ⑦ days5 = 近5日最低距今交易日数 ≤ **3**            ← 探底必须新鲜 (v2.267: 2→3)
  ⑧ 排除 ST ⑨ 排除北交所(BZ) ⑩ 当日涨幅 < 涨停线−0.3 (排除已涨停, 买不进)

【百低锚点为什么是"前推 20"】
  用户原话"这个百日最低价必须豁免近 20 个交易日"= 锚点不许落在近 20 日内。
  实现: 取 **T-119 … T-20 共 100 根**(跨度 120 根) 的 low 最小值 ——
  不是"把 100 根砍成 80 根"(那样两种口径差 11 只, 已实测: 918 vs 907)。
  百高**不豁免**(用户未要求; 实测豁免后会砍掉刚跌下来的票, 正是本模式要的)。

【为什么必须"排除已涨停"】
  侨银股份 002973 在 2026-11-06 收 +10.00% 涨停, 按条件会被选出**但买不进**。
  用户要的是"涨停前一天选出来"⇒ 11-05 收 9.80 (+3.59%) 才是唯一入口。
  这条就是为它加的。

===============================================================================
数据源 (全部本地, 零网络)
===============================================================================
  历史 K 线         本地 CSV kline_data/daily/*.csv   (前复权, 9 列)
  个股简称/行业/概念  选股池/*.xlsx (GBK 制表符) + kline_data/*_map.csv
  ST 名单            选股池/ST股票.xlsx  (名字是唯一可靠判据, 见 engine.load_st_codes)
  盘中实时价         server.py 传进来的 rt_prices (新浪主源, 非交易时段不传)

===============================================================================
实测结果 (写模块前已用 final2.py 跑过两天, 口径逐字段核对)
===============================================================================
  2026-11-05  可分析 5462 只 → 选出 **105 只**, 含侨银股份 ✅  (这就是要的入口日)
  2026-11-06  可分析 5463 只 → 选出 **64 只**, 不含侨银    (11-06 已涨停, 正确剔除)
"""
import datetime
import os
import sys
import time

# ---- 可选依赖: 都做 try 兜住, 缺了也不许崩 -------------------------------
try:
    from quant_terminal import engine as _engine
except Exception:
    try:
        import engine as _engine
    except Exception:
        _engine = None

# ============================================================
# 阈值 —— 全部来自用户 2026-11-07 拍板, 改动前先问用户
# ============================================================
DROP_MIN = -30.0         # ① 近5日最低距百高 (≤, 百分点)
MA5_REQ = True           # ② 现价 > MA5
HIGH_AGE_MIN = 20        # ④ 百高距今交易日 ≥
NEAR_MAX = 2.0           # ⑤ 距百低上限 (%)
BOUNCE_MAX = 13.0        # ⑥ 自5日低反弹上限 (%)
DAYS5_MAX = 3            # ⑦ 探底新鲜度上限 (交易日)
#   v2.267 (2026-11-09 用户 m06927「那就放宽到3天吧」): 2 → 3
#   起因: 11-09 全市场 130 只过了前六道、却在这一条全灭 (days5=3 的 111 只 + days5=4 的 19 只),
#         因为 11-05/06/09 连涨三天没人创新低, 而 5 日最低停在 11-04 = 距今 3 个交易日。
#   实测 180 个交易日: 0 只的日期 12/180 = 6.7%, 都是「反弹走了两三天才回看」这种形态。
#   回测 (180 天 / 11896 事件 / T+1 收盘买): ≤2 → 44.4%/-1.17% (+30d), ≤3 → 45.1%/-0.81%,
#         随机买基准 38.2%/-1.98% ⇒ 放宽不降质, 反而略好 (days5=1 那档最差 -1.18%, 被摊薄)。
#   11-09 具体: ≤2 出 0 只, ≤3 出 124 只, ≤4 出 147 只。
BREAK_MAX = 3.0          # ③ 变体③ 允许的最大破位深度 (%)
RECOVER_DAYS = 2         # ③ 变体③ 收回用几天
LIMIT_MARGIN = 0.3       # ⑩ 涨停线要留的余量 (百分点)

WIN_HIGH = 100           # 百日最高: T-99 … T (含当日)
WIN_LOW_SKIP = 20        # 百低豁免近 20 个交易日
WIN_LOW = 100            # 百低取 T-119 … T-20 共 100 根
WIN_5 = 5                # 近 5 日
MIN_BARS = 150           # 少于这么多根 (次新/长期停牌) 不参与

# 变体显示名 (2026-11-07 用户 m01093 定案: 圈码看不快, 要中文)
#   用户原话: 「用文字我才能一眼抗冻 → 就叫: 不破（极）, 不破（收）, 跌破（假）」("抗冻"=看懂)
VAR_TX = {'①': '不破（极）', '②': '不破（收）', '③': '跌破（假）'}

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')


# ============================================================
# 小工具
# ============================================================
def _now():
    return datetime.datetime.now()


def _today():
    return _now().strftime('%Y-%m-%d')


def _f(x, d=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return d


def kline_dir():
    if _engine is not None:
        try:
            return _engine.KLINE_DIR
        except Exception:
            pass
    base = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(os.path.dirname(base), 'kline_data', 'daily')


def _pool_dir():
    base = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(os.path.dirname(base), '选股池')


def limit_pct(code):
    """涨停线 (%) —— 双创 20% / 主板 10% / 北交所 30%

    ⚠ 北交所虽已在池外剔除, 这里仍给正确值, 免得以后放开时静默按 10% 判。
    ST 的 5% 也一并处理 (同样已剔除, 只是防呆)。
    """
    n = str(code)[2:] if str(code)[:2].isalpha() else str(code)
    if str(code).startswith('BZ'):
        return 30.0
    if n.startswith(('688', '689')):
        return 20.0
    if n.startswith(('300', '301', '302')):
        return 20.0
    return 10.0


def read_csv_tail(fpath, n_lines=200):
    """读 K 线 CSV 末尾 N 行 → [{date, open, close, high, low, vol}]

    9 列格式: date,open,close,high,low,volume,总市值,流通市值,自由流通股本
    优先借 engine._read_csv_tail (它已处理"文件 < 20000 字节时要回退"那个坑),
    拿不到才自读 —— 自读版把那个坑也照抄了一遍。

    ⚠ 返回值统一成 vol 键: engine 那份回来的是 {'volume': ...},
      本模块内部一律用 x['vol'] —— 归一放在这里做, 别让调用方去猜。
    """
    if _engine is not None:
        try:
            rows = _engine._read_csv_tail(fpath, n_lines)
            if rows:
                for r in rows:
                    if 'vol' not in r:
                        r['vol'] = r.get('volume') or 0.0
                return rows
        except Exception:
            pass
    rows = []
    try:
        with open(fpath, 'r', encoding='utf-8') as f:
            f.readline()
            f.seek(0, 2)
            fsize = f.tell()
            if fsize > 20000:
                f.seek(max(0, fsize - 20000))
                f.readline()
            else:
                f.seek(0)
                f.readline()
            for line in f:
                line = line.strip().replace('\r', '')
                if not line:
                    continue
                p = line.split(',')
                if len(p) < 5:
                    continue
                try:
                    rows.append({'date': p[0], 'open': float(p[1]), 'close': float(p[2]),
                                 'high': float(p[3]), 'low': float(p[4]),
                                 'vol': float(p[5]) if len(p) > 5 and p[5] else 0.0})
                except (ValueError, IndexError):
                    continue
    except Exception:
        return rows
    return rows[-n_lines:] if len(rows) > n_lines else rows


# ---- 名称 / 行业 / 概念 / ST -------------------------------------------
_CACHE = {'name': None, 'ind': None, 'con': None, 'st': None, 'at': 0.0}
_TTL = 600.0


def _read_pool_table(fname):
    """选股池 xlsx (其实是 GBK 制表符文本) → [(code8, name, ind_sub), ...]"""
    out = []
    p = os.path.join(_pool_dir(), fname)
    if not os.path.exists(p):
        return out
    try:
        with open(p, 'r', encoding='gbk', errors='ignore') as f:
            for line in f:
                parts = line.rstrip('\n').split('\t')
                if not parts or not parts[0].startswith(('SH', 'SZ', 'BZ')):
                    continue
                code = parts[0].strip()[:8]
                name = parts[1].strip() if len(parts) > 1 else ''
                ind = parts[5].strip() if len(parts) > 5 else ''
                out.append((code, name, ind))
    except Exception:
        pass
    return out


def load_meta(force=False):
    """→ (name_map, ind_map, con_map, st_set)  key 一律 **8 位带前缀**(SZ002973)

    ⚠ 为什么 key 用 8 位: 本模块的 code 全程是 "SZ002973" 这种带前缀形态
      (与 CSV 文件名一致), 不做裸码归一 —— 一归一就会和北交所 8 位码纠缠。
    """
    now = time.time()
    c = _CACHE
    if not force and c['name'] is not None and (now - c['at']) < _TTL:
        return c['name'], c['ind'], c['con'], c['st']

    name, ind_pool = {}, {}
    for fn in ('沪市主板.xlsx', '深市主板.xlsx', '创业板.xlsx', '科创板.xlsx', '北交所.xlsx'):
        for code, nm, sub in _read_pool_table(fn):
            if nm and code not in name:
                name[code] = nm
            if sub and code not in ind_pool:
                ind_pool[code] = sub

    # 名称兜底: 同花顺标的表 (engine 已实现, 缺它时简称列会整列显示代码)
    if _engine is not None and len(name) < 5000:
        try:
            for bare, nm in (_engine.load_stock_name_map() or {}).items():
                if not nm:
                    continue
                b = str(bare)
                if len(b) != 6 or not b.isdigit():
                    continue
                pfx = 'SH' if b[0] in '69' else ('BZ' if b[:3] == '920' or b[0] in '48' else 'SZ')
                name.setdefault(pfx + b, nm)
        except Exception:
            pass

    st = set()
    for code, _nm, _sub in _read_pool_table('ST股票.xlsx'):
        st.add(code)

    # 行业/概念: **engine 优先** (它处理了 BOM / 北交所归一 / 概念屏蔽名单)
    #   v2.257a (2026-11-07 用户 m01093) 用户发现「水发燃气」显示成「燃气Ⅲ」:
    #   选股池 xlsx 第 6 列是导出方混进来的申万三级后缀(燃气Ⅲ), 不是同花顺口径;
    #   kline_data/industry_map.csv(257 类) 里就是干净的「燃气」。
    #   原代码是 ind 先填选股池、再 ind.setdefault(engine) —— setdefault 不覆盖,
    #   结果脏数据赢了。改成 engine 优先、选股池兜底。
    con = {}
    ind = {}
    if _engine is not None:
        try:
            im, cm = _engine.load_industry_concept_maps()
            for bare, v in (im or {}).items():
                b = str(bare)
                if len(b) != 6 or not b.isdigit():
                    continue
                pfx = 'SH' if b[0] in '69' else ('BZ' if b[:3] == '920' or b[0] in '48' else 'SZ')
                ind.setdefault(pfx + b, v)
            for bare, v in (cm or {}).items():
                b = str(bare)
                if len(b) != 6 or not b.isdigit():
                    continue
                pfx = 'SH' if b[0] in '69' else ('BZ' if b[:3] == '920' or b[0] in '48' else 'SZ')
                con.setdefault(pfx + b, v)
        except Exception:
            pass

    # 选股池第 6 列只作兜底: engine 没有这个码才用 (个别票会被打上申万三级后缀)
    for code, sub in ind_pool.items():
        if sub and code not in ind:
            ind[code] = sub

    c['name'], c['ind'], c['con'], c['st'], c['at'] = name, ind, con, st, now
    return name, ind, con, st


# ============================================================
# 单只判定
# ============================================================
def evaluate_one(code, rows, T):
    """在 rows[T] = 观察日 上算一只票的全部指标 → dict (数据不足返回 None)

    ⚠ rows 必须已经包含观察日那一根; rt 覆盖由调用方在传进来之前做完。
    """
    if T < MIN_BARS - 1 or T >= len(rows):
        return None
    try:
        c = rows[T]['close']
        if c <= 0:
            return None
        pv = rows[T - 1]['close']
        if pv <= 0:
            return None

        # ---- 百日最高 (含当日, 极值口径) ----
        hi_lo = T - WIN_HIGH + 1
        H = max(x['high'] for x in rows[hi_lo:T + 1])
        iH = max(i for i in range(hi_lo, T + 1) if rows[i]['high'] == H)

        # ---- 百低锚点 (豁免近 20 个交易日, 即 T-119 … T-20) ----
        la_lo, la_hi = T - WIN_LOW_SKIP - WIN_LOW + 1, T - WIN_LOW_SKIP   # T-119 … T-20
        if la_lo < 0:
            return None
        L = min(x['low'] for x in rows[la_lo:la_hi + 1])
        if L <= 0:
            return None
        iLa = max(i for i in range(la_lo, la_hi + 1) if rows[i]['low'] == L)

        # ---- 近 5 日最低 ----
        low5 = min(x['low'] for x in rows[T - WIN_5 + 1:T + 1])
        i5 = max(i for i in range(T - WIN_5 + 1, T + 1) if rows[i]['low'] == low5)

        # ---- MA5 ----
        ma5 = sum(x['close'] for x in rows[T - 4:T + 1]) / 5.0
        ma5p = sum(x['close'] for x in rows[T - 5:T]) / 5.0

        # ---- 三变体 (锚点之后的形态) ----
        post = rows[iLa + 1:T + 1]
        if not post:
            return None
        mnl = min(x['low'] for x in post)          # 锚点后最低价
        mnc = min(x['close'] for x in post)        # 锚点后最低收盘
        v1 = mnl >= L
        v2 = mnc >= L
        depth = (L - mnc) / L * 100.0 if mnc < L else 0.0   # 破位深度 (%)
        v3 = False
        if 0 < depth <= BREAK_MAX:
            k = next((i for i in range(iLa + 1, T + 1) if rows[i]['close'] < L), None)
            if k is not None:
                v3 = (any(rows[j]['close'] > L for j in range(k, min(k + RECOVER_DAYS + 1, T + 1)))
                      and c > L)

        # ---- 派生 ----
        drop5 = (low5 - H) / H * 100.0             # 近5日最低距百高 (%)
        dropc = (c - H) / H * 100.0                # 现价距百高 (%)  仅展示
        near = (low5 - L) / L * 100.0              # 距百低 (%)    越低越贴
        bounce = (c - low5) / low5 * 100.0         # 自5日低反弹 (%)
        chg = (c - pv) / pv * 100.0                # 当日涨幅 (%)
        vol5 = sum(x['vol'] for x in rows[T - 5:T]) / 5.0
        vr = (rows[T]['vol'] / vol5) if vol5 > 0 else 0.0

        return {
            'code': code, 'c': c, 'pv': pv,
            'H': H, 'iH': iH, 'dsh': T - iH,
            'L': L, 'iLa': iLa, 'dsl': T - iLa,
            'low5': low5, 'i5': i5, 'days5': T - i5,
            'ma5': ma5, 'ma5up': ma5 >= ma5p,
            'drop5': drop5, 'dropc': dropc, 'near': near, 'bounce': bounce,
            'chg': chg, 'vr': vr,
            'v1': v1, 'v2': v2, 'v3': v3, 'depth': depth,
            'variant': '①' if v1 else ('②' if v2 else ('③' if v3 else '')),
            # v2.257a (2026-11-07 用户 m01093) 显示用中文: 用户要一眼看懂, 不要圈码
            #   ①不破（极）= 百低之后的最低价从未跌破百低
            #   ②不破（收）= 收盘价从未跌破百低 (盘中破过)
            #   ③跌破（假）= 收盘跌破百低不超 3% 且两日内收回 → 假破位
            'variant_tx': VAR_TX['①'] if v1 else (VAR_TX['②'] if v2 else (VAR_TX['③'] if v3 else '')),
        }
    except (KeyError, ValueError, ZeroDivisionError, IndexError):
        return None


def hit(a):
    """套 final 口径 —— 返回 (是否入选, 未过的第一条原因)"""
    if a['drop5'] > DROP_MIN:
        return False, '跌幅不够'
    if MA5_REQ and a['c'] <= a['ma5']:
        return False, '未站MA5'
    if not a['variant']:
        return False, '未摸底'
    if a['dsh'] < HIGH_AGE_MIN:
        return False, '百高太近'
    if a['near'] > NEAR_MAX:
        return False, '离百低太远'
    if a['bounce'] > BOUNCE_MAX:
        return False, '反弹过多'
    if a['days5'] > DAYS5_MAX:
        return False, '探底太旧'
    if a['chg'] >= a['lp'] - LIMIT_MARGIN:
        return False, '已涨停'
    return True, ''


# ============================================================
# 全市场扫描
# ============================================================
def scan(progress_callback=None, rt_prices=None, log=None, asof=None):
    """超跌反弹 完整流程 (全本地, 零网络 —— 除非传了 rt_prices)

    asof: 指定观察日 'YYYY-MM-DD'; None = 自动取最新 (末日出现次数最多的那天)。
          给回看历史用, 不影响日常扫描。

    返回 dict:
      ok / date / n_pool / n_total / n_hit / rows / stages / elapsed / note
      funnel: 每层剩余只数 (给前端「怎么筛出来的」用)
    """
    t0 = time.time()
    stages = {}

    def _p(i, n, msg=''):
        if progress_callback:
            try:
                progress_callback(i, n, msg)
            except Exception:
                pass

    def _log(s):
        if log:
            try:
                log(s)
            except Exception:
                pass

    d = kline_dir()
    _p(0, 3, '读本地K线...')
    try:
        files = [fn for fn in os.listdir(d) if fn.lower().endswith('.csv')]
    except OSError:
        return {'ok': False, 'note': 'K线目录不可用: %s' % d, 'rows': [],
                'n_pool': 0, 'n_total': 0, 'n_hit': 0, 'elapsed': 0}
    # 北交所一律不要 (BZ 前缀); 只做沪深
    files = [fn for fn in files if fn[:2].upper() in ('SH', 'SZ')]
    files.sort()
    _log('[启动] 超跌反弹 · 本地CSV %d 个 · %s' % (len(files), _now().strftime('%H:%M:%S')))
    stages['list'] = round(time.time() - t0, 2)

    # ---- 第一趟: 读尾部, 同时统计"末日分布"定观察日 ----
    t_read = time.time()
    cache = {}
    date_cnt = {}
    n = len(files)
    for i, fn in enumerate(files):
        if i % 400 == 0:
            _p(1, 3, '读取K线 %d/%d ...' % (i, n))
        code = fn[:-4].upper()
        rows = read_csv_tail(os.path.join(d, fn), 220)
        if len(rows) < MIN_BARS:
            continue
        cache[code] = rows
        d_last = rows[-1]['date']
        date_cnt[d_last] = date_cnt.get(d_last, 0) + 1
    stages['read'] = round(time.time() - t_read, 2)

    # 观察日 = 末日出现次数最多的那天 (个别文件滞后不影响)
    data_date = max(date_cnt.items(), key=lambda kv: (kv[1], kv[0]))[0] if date_cnt else _today()
    _log('[数据] 最新交易日 = %s (末日为这天的文件 %d 个 / 共 %d 个)'
         % (data_date, date_cnt.get(data_date, 0), len(cache)))
    if asof:
        # 回看历史: 观察日前移; K线尾部读了 220 根, 够往前翻 ~200 个交易日
        asof = str(asof)[:10]
        if asof < data_date:
            _log('[数据] 按指定观察日回看: %s (原最新 %s)' % (asof, data_date))
            data_date = asof
        else:
            _log('[数据] 指定观察日 %s 不早于最新交易日, 忽略, 仍用 %s' % (asof, data_date))

    # ---- 第二趟: 逐只算 ----
    t_calc = time.time()
    _p(2, 3, '计算指标...')
    name_map, ind_map, con_map, st_set = load_meta()
    out = []
    n_eval = 0
    for code, rows in cache.items():
        # 停牌/滞后: 日常扫描时观察日必须落在最后 6 根之内 (容忍 1~5 天停牌);
        # 指定了 asof 回看历史时就允许出现在任意位置 (K线尾部只读了 220 根)。
        T = None
        lo = max(len(rows) - 7, -1) if not asof else -1
        for j in range(len(rows) - 1, lo, -1):
            if rows[j]['date'] == data_date:
                T = j
                break
        if T is None:
            continue
        # 盘中实时价覆盖最后一根 (只覆盖观察日那根; 与 engine.scan_* 同口径)
        # ⚠ 回看历史 (asof) 时绝不覆盖 —— 否则会拿今天的价去算历史那天
        if rt_prices and not asof:
            bare = code[2:]
            px = rt_prices.get(bare) or rt_prices.get(code)
            if px and px > 0:
                rows = list(rows)
                bar = dict(rows[T])
                bar['close'] = px
                bar['high'] = max(bar['high'], px)
                bar['low'] = min(bar['low'], px)
                rows[T] = bar
        a = evaluate_one(code, rows, T)
        if a is None:
            continue
        n_eval += 1
        a['lp'] = limit_pct(code)
        a['is_st'] = code in st_set
        a['is_bz'] = code.startswith('BZ')
        a['name'] = name_map.get(code, code[2:])
        a['ind'] = ind_map.get(code, '')
        a['con'] = con_map.get(code, '')
        a['ok'], a['why'] = hit(a) if not (a['is_st'] or a['is_bz']) else (False, 'ST/北交所')
        out.append(a)
    stages['calc'] = round(time.time() - t_calc, 2)

    rows_hit = [a for a in out if a['ok']]
    # 排序: 反弹小的在前 (= 越接近刚启动, 用户看名单的直觉顺序)
    rows_hit.sort(key=lambda a: (a['bounce'], a['near']))

    # ---- 漏斗 (给前端「怎么筛出来的」) ----
    live = [a for a in out if not a['is_st'] and not a['is_bz']]
    funnel = {
        'total': len(out),
        'no_st_bz': len(live),
        'drop': sum(1 for a in live if a['drop5'] <= DROP_MIN),
        'ma5': sum(1 for a in live if a['drop5'] <= DROP_MIN and a['c'] > a['ma5']),
        'variant': sum(1 for a in live
                       if a['drop5'] <= DROP_MIN and a['c'] > a['ma5'] and a['variant']),
        'near': sum(1 for a in live
                    if a['drop5'] <= DROP_MIN and a['c'] > a['ma5'] and a['variant']
                    and a['near'] <= NEAR_MAX),
        'bounce': sum(1 for a in live
                      if a['drop5'] <= DROP_MIN and a['c'] > a['ma5'] and a['variant']
                      and a['near'] <= NEAR_MAX and a['bounce'] <= BOUNCE_MAX),
        'hit': len(rows_hit),
    }

    res = {
        'ok': True,
        'date': data_date,
        'n_pool': len(cache),
        'n_total': n_eval,
        'n_hit': len(rows_hit),
        'rows': rows_hit,
        'funnel': funnel,
        'stages': stages,
        'elapsed': round(time.time() - t0, 2),
        'note': ('观察日 %s · 可分析 %d 只 → 入选 %d 只 (跌幅≤%d%% / 站MA5 / 三变体 / '
                 '距百低≤%.1f%% / 反弹≤%.1f%% / 探底≤%d日 / 排除ST·北交所·涨停)'
                 % (data_date, n_eval, len(rows_hit), int(DROP_MIN),
                    NEAR_MAX, BOUNCE_MAX, DAYS5_MAX)),
    }
    _log('[结果] ' + res['note'])
    # v2.267: 这行原来把 2%/13% 写死, 改阈值后日志会说谎 —— 改成跟着常量走
    _log('[漏斗] 有数据 %d → 非ST非北交所 %d → 跌超%d%% %d → 站MA5 %d → 三变体 %d '
         '→ 距百低≤%.1f%% %d → 反弹≤%.1f%% %d → 探底≤%d日 → **入选 %d**  总耗时 %.1fs'
         % (funnel['total'], funnel['no_st_bz'], int(-DROP_MIN), funnel['drop'],
            funnel['ma5'], funnel['variant'], NEAR_MAX, funnel['near'],
            BOUNCE_MAX, funnel['bounce'], DAYS5_MAX, funnel['hit'], res['elapsed']))
    return res


def to_scan_rows(res):
    """把 scan() 结果转成终端表格行 (字段名对齐 resetTableHead('rebound') 的 data-col)

    列序 (14 列):
      rebound 反弹% | code | name | change_pct 涨幅 | price 现价 |
      near 距百低% | low5 5日低 | days5 探底 | drop5 距百高% |
      sector 行业 | zt_cnt 梯队 | concept 概念 | variant 变体 | 操作
    """
    out = []
    for a in res.get('rows') or []:
        # v2.261 (2026-11-08 用户 m03399): 下发给前端的 code 一律**裸 6 位**
        #   ★ 为什么: 本模块内部一律用 8 位带前缀 (SZ002973) 作 load_meta 查表键, 但前端
        #     selectStock() 会把 r.code 直接塞进 K 线搜索框、renderStockDetail() 直接上屏,
        #     于是详情页与搜索框都多出「SZ」前缀 —— 而其它九个模式全是裸 6 位。
        #   ★ 顺带修掉一个静默 bug: /api/scan/cache/prices 回写落盘时用的是
        #     `prices.get(str(r['code']))`, 而前端 prices 的键是去掉前缀的 6 位 →
        #     8 位码永远匹配不上, 本模式的实时价回写一直在静默失败。
        _c6 = a['code'][2:] if a['code'][:2].upper() in ('SH', 'SZ', 'BJ', 'BZ') else a['code']
        out.append({
            'code': _c6,
            'code8': a['code'],      # 保留 8 位, 万一以后要反查 (当前前端不用)
            'name': a['name'],
            'price': a['c'],
            'change_pct': a['chg'],
            # 默认排序键 (升序: 反弹越小越靠前)
            'rebound': a['bounce'],
            # 第 1 列同时挂 cons_score, 前端排序复位时统一用它
            'cons_score': a['bounce'],
            'near': a['near'],
            'low5': a['low5'],
            'days5': a['days5'],
            'drop5': a['drop5'],
            'high100': a['H'],
            'low100': a['L'],
            'ma5': a['ma5'],
            'ma5up': 1 if a['ma5up'] else 0,
            'vol_ratio': a['vr'],
            'depth': a['depth'],
            'variant': a['variant'],
            'variant_tx': a.get('variant_tx') or VAR_TX.get(a['variant'], ''),
            'sector': a['ind'],
            'concept': a['con'],
            'zt_cnt': 0,          # 前端会用 /api/sector_zt 的行业家数覆盖
            'is_valid': True,
            'mode': 'rebound',
            'mode_detail': '超跌反弹',
        })
    return out


# ============================================================
# 自测
# ============================================================
if __name__ == '__main__':
    import io as _io
    _stdout = sys.stdout
    try:
        sys.stdout = _io.TextIOWrapper(_stdout.buffer, encoding='utf-8', errors='replace')
    except Exception:
        pass
    # 可用命令行指定回看日: python rebound_scan.py 2026-11-05
    _asof = sys.argv[1] if len(sys.argv) > 1 else None
    print('=' * 96)
    print('超跌反弹 自测   %s' % _now().strftime('%Y-%m-%d %H:%M:%S'))
    print('K线目录: %s' % kline_dir())
    print('口径: 跌≤%d%% / 站MA5 / 三变体 / 距百低≤%.1f%% / 反弹≤%.1f%% / 探底≤%d日 / 百高距今≥%d / 排除ST·BZ·涨停'
          % (int(DROP_MIN), NEAR_MAX, BOUNCE_MAX, DAYS5_MAX, HIGH_AGE_MIN))
    if _asof:
        print('回看观察日: %s' % _asof)
    print('=' * 96)
    r = scan(asof=_asof)
    print('观察日:', r['date'], '| 可分析:', r['n_total'], '| 入选:', r['n_hit'],
          '| 耗时:', r['elapsed'], '秒')
    f = r.get('funnel') or {}
    print('漏斗: 有数据 %s → 非ST非BZ %s → 跌够 %s → 站MA5 %s → 三变体 %s → 距百低 %s → 反弹 %s → 入选 %s'
          % (f.get('total'), f.get('no_st_bz'), f.get('drop'), f.get('ma5'),
             f.get('variant'), f.get('near'), f.get('bounce'), f.get('hit')))
    print()
    print('%-9s %-8s %-16s %6s %6s %6s %6s %7s %6s %5s %6s %-5s'
          % ('代码', '名称', '行业', '百高', '百低', '5日低', '现价',
             '距百高', '距百低', '反弹', '涨幅', '变体'))
    for a in (r.get('rows') or [])[:30]:
        print('%-9s %-8s %-16s %6.2f %6.2f %6.2f %6.2f %+6.1f%% %+6.2f%% %+6.1f%% %+6.1f%% %-5s'
              % (a['code'], a['name'][:8], (a['ind'] or '')[:16], a['H'], a['L'],
                 a['low5'], a['c'], a['drop5'], a['near'], a['bounce'], a['chg'],
                 VAR_TX.get(a['variant'], a['variant'])))
    qy = next((a for a in (r.get('rows') or []) if a['code'] == 'SZ002973'), None)
    print()
    if qy:
        print('★ 侨银股份在名单里: 百高%.2f 百低%.2f 5日低%.2f 现价%.2f 距百低%+.2f%% 反弹%+.2f%% 探底%d日 涨幅%+.2f%%'
              % (qy['H'], qy['L'], qy['low5'], qy['c'], qy['near'], qy['bounce'],
                 qy['days5'], qy['chg']))
    else:
        print('· 侨银股份(SZ002973) 不在本次名单 — 正常 (涨停/反弹过头时会被剔)')
