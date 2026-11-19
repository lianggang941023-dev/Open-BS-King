# -*- coding: utf-8 -*-
"""波段低吸 · 主逻辑  (2026-11-07 新增模块, v2.260 口径重写)

⚠ 本文件是「波段低吸」模式的**全部业务逻辑**, 与终端现有功能零耦合:
    - 不 import engine (只借用它的 _read_csv_tail 读本地 CSV, 缺失时自读)
    - 不 import rebound_scan (两个模式必须能各自摘除, 所以小工具函数是有意重复的)
    - 不写任何全局状态; 结果由 server.py 存进既有的 SCAN_STATE / scan_cache 槽
    - 摘除方式见 _uninstall_wave.py; 压舱开关见 wave_off.flag

===============================================================================
v2.260 口径 —— 由用户 2026-11-08 分三条消息最终定稿, 一个字不改
===============================================================================
【m02414 原话(主体规则)】
  "首先找到百日最低点，以此为画线的起点A，然后这一天后面出现一个新低点B
   （在8-15天内形成）比A点高，然后AB连线作为上涨趋势线，后续只要回踩到现价
   高于趋势线0到1.5%区间，就可以选出来，不再接受假跌破（不能有收盘价跌破
   趋势线但可以盘中跌破后收回）...每一个买点前5日收盘价都比这一天高，且与
   上一个趋势线触及点之间有过一个前后4日收盘价都比这一天高的清晰峰...
   简单来说就是从百日最低点开始反弹，经过几天上涨之后又回调了几天形成一个
   小山峰，回调之后再次上涨，又形成一个更高的高点再次回调到趋势线附近即是
   第一个买点"

【m02577 原话(首次回踩)】"我希望C点选出来...所以我现在的想法是，趋势线画出
   来之后的首次回踩是最好的买点"  ← 第二次以后的回踩不选(相对 A 已涨太多)

【m02603 原话(取价规则)】"画线的规则也要改一下，阳线要看最低价，阴线要看收盘价。"
   ⚠ 这条是决定性的: 改之前 新疆天业 的 C 点算不出来、只会在 9/15 那根大阴线
     上"贴线"(而那天是踩破); 改之后 C 点正确定位在 2026-08-13.
   ⚠ v2.270 起这条**只管辖"今日离线%"的取价**(pv), A/B 两个锚点改按收盘价(ab_px)。

【m02539 原话(不许加均线)】"低于ma5没有问题啊，这个模式我从来没说过要大于ma5，
   我想的是贴着趋势线买，选出来的时候是阴线没关系，第二天红盘了在盘中买入即可"
   ⇒ 绝不加 MA5 条件; T 日选出的常常是阴线, 实盘是 T+1 红盘再买.

===============================================================================
【v2.270 (2026-11-11 用户 m07460) A/B 两点改收盘价】
  原话: "波段低吸模式，把AB点位取点统一为按收盘价取点"
  为什么: v2.260 沿用 m02603 的 pv 取点, A 点常常落在**阳线的插针最低价**上
          (A 偏低) 而 B 落在阴线收盘 (B 偏高), 两个点口径不一致 ⇒ 斜率被人为放大,
          画出来的线比真实趋势陡。现在 A/B 都用 ab_px() = close, 口径统一。
  影响: 新疆天业 SH600075 A 从 2026-06-26@3.90 变成 2026-06-30@4.04, 斜率 0.396→0.083 %/日;
        两个旧正样本的"原买点"不再入选(线变平后收盘价离得太远), 这是口径修正的必然结果。

【取价 —— 两套, 各管一件事, 不要混用】
  ① ab_px(bar) = close     —— **只用于 A 点 / B 点**(画线的两个锚点)
  ② pv(bar)  = (close >= open) ? low : close
                           —— **只用于"今日离线%"**(m02603 原意: 阳线的插针最低价
                              也算触及趋势线, 所以回踩要用它才抓得到阴线/插针)
  破线判定一直用 close (收盘破线即判死), 盘中插针不算。

【画线】
    A = 最近 100 根内 **收盘价** 最低的那根, 且距今 >= 8 根
    B = A+8 ~ A+15 之间、**收盘价**比 A 高、且是 ±2 窗口内收盘价最低的那几根 (可多个候选)
    线 = A -> B 连线, 斜率 = (closeB - closeA) / (iB - iA)
    一条票可能有多个合格 B, 选哪条? => 选"**最早那个还活着**的 B"
        活 = 从 B 之后到观察日, **没有任何一天收盘价 < 线值**
        (收盘破线即判死, 无容差 —— 用户原话"不再接受假跌破"; 盘中插针不算)
        选最早的: 实盘中趋势线在第一个合格低点出现时就画下去了, 事后重新画线会把买点往后推

【买点 = 首次回踩】
    峰   = 某日 close 比**前后各 4 日** close 都高  (小山峰)
    首次回踩 C = 第一个峰之后, 第一次出现 0 <= 离线 <= 1.5%
        离线 = (pv(T) - 线值(T)) / 线值(T) * 100
    ⚠ C 是"一段"不是"一天" (v2.260b 用户 m03201): 峰后第一段连续贴线的日子里
      每天都算同一次回踩, 出带或收盘破线才算这段结束
    ⚠ 必须是"首次": 峰之前、B 之后若已贴过线, 这条线不算首次回踩 (次数 > 1 不选)
    ⚠ C 到 B 之间不许有收盘破线

【买点的附加条件】
    T 日往前 5 日的 close **全部** > T 日 close
        (= 今天创了近 6 日新低, 是回调的最深处, 不是追高)
        ⚠ 这条不是筛子在捣乱: 它保证"只在最近一周最弱那天发现信号",
          次日转红才是买点。实测 T+1 红盘率 59%, 只买次日红盘的
          +3日 均收益 3.78% / 胜率 73% (全买是 1.94% / 58%)。

【其他硬条件】
    · 排除 ST / 排除北交所(BZ)
    · 当日涨幅 < 涨停线 - 0.3 (排除已涨停, 买不进)
    · **不用缩量、不用回踩深度、不用回撤比例、不用均线** ——
      这四条是 v2.258 旧口径的, 新口径(m02414)没提, 一律不加。
      量比 vr 仍然算出来显示(给用户参考), 但不参与筛选。

===============================================================================
数据源 (全部本地, 零网络)
===============================================================================
  历史 K 线         本地 CSV kline_data/daily/*.csv   (前复权, 9 列, 纯 LF)
  个股简称/行业/概念  选股池/*.xlsx (GBK 制表符) + kline_data/*_map.csv
  ST 名单            选股池/ST股票.xlsx
  盘中实时价         server.py 传进来的 rt_prices (新浪主源, 非交易时段不传)

===============================================================================
正样本 (v2.260 回归基准, 改算法后必须仍然复现)
===============================================================================
  新疆天业 SH600075  A=2026-06-26 pv=3.90(阳线取low)
                     B=2026-07-13 pv=4.07(阴线取close)  跨度11 斜率0.396%/日
                     峰=2026-08-03
                     C=**2026-08-13** 离线 +1.01%  ← 用户图上标的那个买点
                     之后 08-14 收盘 4.42 < 线值 4.441 破线(−0.47%), 此线到此为止
                     9/15 那根大阴线 = 离线 −4.20% (用户说的"踩破了")
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
# 阈值 —— 全部来自用户 2026-11-08 拍板 (m02414 / m02577 / m02603), 改动前先问用户
# ============================================================
WIN = 100                # "百日"窗口: A 点(最低点)只在这 100 根里找
PIV = 4                  # 峰确认: close 比前后各 N 根都高 (= 小山峰)

SPAN_MIN = 8             # AB 两低点跨度下限 (根) —— m02414 "在8-15天内形成"
SPAN_MAX = 15            # AB 两低点跨度上限 (根)
B_LOCAL = 2              # B 必须是 ±N 窗口内的局部最低 (pv 最低)
A_AGE_MIN = 8            # A 点距今至少这么多根 (否则 B 无处可放)

GAP_MAX = 1.5            # 买点: 收盘价 pv 在趋势线上方 0~1.5%
PREV_HIGH_N = 5          # 买点前 N 日收盘价必须全部高于当日收盘价

LIMIT_MARGIN = 0.3       # 涨停线要留的余量 (百分点)

MIN_BARS = 150           # 少于这么多根 (次新/长期停牌) 不参与
TAIL_NORMAL = 260        # 日常扫描读尾部根数 (够覆盖 100 窗口 + 停牌容忍)
TAIL_ASOF = 700          # 回看历史时读尾部根数 (要能翻到指定的老日期)

# ---- 以下常量保留给前端/统计用, v2.260 不再参与筛选 ----------------------
SHRINK_MAX = 1.0         # 量比上限 (只显示, 不筛选)
SLOPE_MIN = 0.05         # 斜率下限 %/日 (只显示)
SLOPE_MAX = 3.00         # 斜率上限 %/日 (只显示)

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')


# ============================================================
# 小工具
# ============================================================
def _now():
    return datetime.datetime.now()


def _today():
    return _now().strftime('%Y-%m-%d')


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
    """涨停线 (%) —— 双创 20% / 主板 10% / 北交所 30%"""
    n = str(code)[2:] if str(code)[:2].isalpha() else str(code)
    if str(code).startswith('BZ'):
        return 30.0
    if n.startswith(('688', '689', '300', '301', '302')):
        return 20.0
    return 10.0


def read_csv_tail(fpath, n_lines=200):
    """读 K 线 CSV 末尾 N 行 -> [{date, open, close, high, low, vol}]

    9 列格式: date,open,close,high,low,volume,总市值,流通市值,自由流通股本
    优先借 engine._read_csv_tail (它已处理"文件 < 20000 字节时要回退"那个坑),
    拿不到才自读 —— 自读版把那个坑也照抄了一遍。

    ⚠ 返回值统一成 vol 键: engine 那份回来的是 {'volume': ...}, 归一放在这里做。

    ⚠⚠ 为什么 n_lines 大时**不走 engine**: engine._read_csv_tail 的字节预算写死
       20000 字节 (与 n_lines 无关), 折合约 430~480 根。日常扫描 (要 ~260 根)
       够用; 但回看历史 (asof) 时需要 700 根, 走 engine 就拿不到那么长的尾巴,
       观察日会落到窗口之外 → 静默 0 只。所以这里按 n_lines 自己算字节预算。
    """
    if _engine is not None and n_lines <= 300:
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
            # 每行最长约 78 字节 (9 列全填, 实测 5,590 只最大 78), 这里按 130 留余量,
            # 按行数折算字节预算
            budget = max(20000, n_lines * 130)
            if fsize > budget:
                f.seek(max(0, fsize - budget))
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
    """选股池 xlsx (其实是 GBK 制表符文本) -> [(code8, name, ind_sub), ...]"""
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
                nm = parts[1].strip() if len(parts) > 1 else ''
                ind = parts[5].strip() if len(parts) > 5 else ''
                out.append((code, nm, ind))
    except Exception:
        pass
    return out


def load_meta(force=False):
    """-> (name_map, ind_map, con_map, st_set)  key 一律 **8 位带前缀**(SZ002973)

    ⚠ 行业口径: **engine 优先**, 选股池第 6 列只兜底。
      超跌反弹那边踩过这个坑 (v2.257a): 选股池导出方混进了申万三级后缀
      (水发燃气 显示成 "燃气Ⅲ"), 而 kline_data/industry_map.csv 里是干净的
      "燃气"。原写法是 ind.setdefault(engine) —— setdefault 不覆盖已存在 key,
      结果脏数据赢了。这里从一开始就按正确优先级写。
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

    con, ind = {}, {}
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

    for code, sub in ind_pool.items():
        if sub and code not in ind:
            ind[code] = sub

    c['name'], c['ind'], c['con'], c['st'], c['at'] = name, ind, con, st, now
    return name, ind, con, st


# ============================================================
# 趋势线 (v2.260 口径)
# ============================================================
def pv(r):
    """今日贴线取价 (用户 m02603 原话) —— **只用于"贴线距离"这一件事**

    阳线(close >= open) 取最低价 low; 阴线(close < open) 取收盘价 close。
    理由: 阳线的最低点只是插针, 收回来就说明有人在那接;
          阴线收在低位才是真的"价格站不住"。

    ⚠ v2.270 (2026-11-11 用户 m07460): A/B **两点取点**已经改成收盘价,
      不再用这条 pv。见 ab_px()。pv 现在只负责「今日离线%」的分子。
    """
    return r['low'] if r['close'] >= r['open'] else r['close']


def ab_px(r):
    """A/B 两点的取价 (v2.270 默认口径) —— 收盘价

    v2.270 用户 m07460: "把AB点位取点统一为按收盘价取点" ⇒ 默认就是收盘价。
    v2.271 用户 m07829/m07846: 改成**六口径可切** (见下面的 PX / COMBOS),
      这个函数保留为默认口径 (close) 的别名, 旧的调用点不动也还是收盘价行为。
    """
    return r['close']


# ============================================================
# 六口径取价表 (v2.271 2026-11-11 用户 m07829 + m07846)
# ============================================================
# 用户原话 (m07829): "A点有'低''收'两种口径, B点有'低''收''开'三种口径,
#   B点的开只取阳线, 收只取阴线, 低阴阳线皆可。"
# 用户原话 (m07846): "A点[不限阴阳]你默认的是对的" —— 所以 A 的低/收都不看阴阳。
# ============================================================
def px_low(r):
    """最低价 —— 阴阳线皆可 (最低价永远在最底下, 哪种线都算低点)"""
    return r['low']


def px_close(r):
    """收盘价 —— **不限阴阳** (A 点用这个; 用户 m07846 第2问确认 A 点默认不限阴阳)"""
    return r['close']


def px_open(r):
    """开盘价 —— **只取阳线** (B 点用这个)

    用户 m07829 原话: "B点的开只取阳线"。
    阴线 (close < open) 返回 None ⇒ 这一根不能当 B 候选, 也不参与"局部最低"的比较。
    """
    return r['open'] if r['close'] >= r['open'] else None


def pxb_close(r):
    """B 点用的收盘价 —— **只取阴线**

    用户 m07829 原话: "收只取阴线"。
    道理同 px_open: 阳线的收盘价在最上面, 当"低点"没有意义;
      阴线收在下面, 收盘价才是一个真实的低位。
    """
    return r['close'] if r['close'] < r['open'] else None


# A 点取价表 —— 低/收 都不限阴阳 (m07846 第2问: "你默认的是对的")
PX_A = {'low': px_low, 'close': px_close}
# B 点取价表 —— 低皆可 / 收只取阴线 / 开只取阳线 (m07829 原话)
PX_B = {'low': px_low, 'close': pxb_close, 'open': px_open}
# 兼容旧名 (早期代码/探针里用过 PX)
PX = PX_A

A_SRCS = ('low', 'close')            # A 点两种口径
B_SRCS = ('low', 'close', 'open')    # B 点三种口径

# 扫描与按钮切换顺序 —— 用户 m07846 原话:
#   "默认初始口径A低B收，没有就跳其他组合，按A低收B收低开的顺序跳。"
DEFAULT_COMBO = 'low|close'
COMBOS = [('low', 'close'),      # 低/收  ← 默认
          ('low', 'low'),        # 低/低
          ('low', 'open'),       # 低/开
          ('close', 'close'),    # 收/收
          ('close', 'low'),      # 收/低
          ('close', 'open')]     # 收/开
COMBO_LBL = {'low': '低', 'close': '收', 'open': '开'}


def ck(a_src, b_src):
    """组合键 (英文, 传给服务器/前端) —— 例 'low|close'"""
    return '%s|%s' % (a_src, b_src)


def cn(a_src, b_src):
    """组合中文名 —— 例 '低/收'"""
    return COMBO_LBL.get(a_src, '?') + '/' + COMBO_LBL.get(b_src, '?')


def combo_list():
    """给前端的组合清单 (顺序 = 切换顺序)"""
    return [{'key': ck(a, b), 'a': a, 'b': b, 'cn': cn(a, b)} for a, b in COMBOS]


def find_A(rows, T, a_src='low'):
    """A = 最近 WIN(100) 根内**取价最低**的那根 -> idx

    用户 m02414: "首先找到百日最低点，以此为画线的起点A"
    用户 m07460 (v2.270): "把AB点位取点统一为按收盘价取点" -> 默认 a_src='close'
    用户 m07829 (v2.271): A 点可切「低」(low) / 「收」(close) —— 由 a_src 决定
    ⚠ 不看未来: 窗口右端就是 T。
    """
    lo = max(0, T - WIN + 1)
    if lo > T:
        return None
    pxf = PX_A.get(a_src) or px_close
    iA = min(range(lo, T + 1), key=lambda k: pxf(rows[k]))
    if T - iA < A_AGE_MIN:
        return None          # A 太新, B 放不下
    return iA


def cand_bs(rows, T, iA, a_src='low', b_src='close'):
    """A 之后 SPAN_MIN~SPAN_MAX 根内、取价比 A 高、且是取价局部最低的 B 候选 -> [idx, ...]

    v2.270 (用户 m07460): A/B 两点统一用收盘价。
    v2.271 (用户 m07829): 按 a_src/b_src 取价。
      ⚠ b_src='open' 时 px_open() 对**阴线返回 None** —— 阴线那一根没有"开价语义",
        直接淘汰 (用户原话: "B点的开只取阳线"), 也不参与局部最低的比较。
    """
    out = []
    paxf = PX_A.get(a_src) or px_close
    pbxf = PX_B.get(b_src) or pxb_close
    pa = paxf(rows[iA])
    if pa is None or pa <= 0:
        return out
    for i in range(iA + SPAN_MIN, min(iA + SPAN_MAX, T - 1) + 1):
        if i >= T:
            break
        pb = pbxf(rows[i])
        if pb is None:
            continue                      # 开口径 + 阴线 ⇒ 不能当 B
        if pb <= pa:
            continue                      # B 必须比 A 高
        lo2, hi2 = max(iA + 1, i - B_LOCAL), min(T, i + B_LOCAL)
        bad = False
        for j in range(lo2, hi2 + 1):
            if j == i:
                continue
            vj = pbxf(rows[j])
            if vj is not None and vj < pb:
                bad = True
                break
        if bad:
            continue                      # 必须是 ±2 窗口内的局部最低
        out.append(i)
    return out


def eval_line(rows, T, iA, iB, a_src='low', b_src='close'):
    """评估一条 A->B 线在观察日 T 的状态 -> dict

    返回的字段:
      alive       到 T 为止有没有收盘破线 (破过就 False, 这条线判死)
      i_break     首次收盘破线的 idx (None = 没破过)
      peak        第一个"小山峰"的 idx (close 比前后各 PIV 根都高), 必须在 B 之后
      i_c         首次回踩 idx —— 第一个峰之后**第一段**贴线的第一天
                  (v2.260b: 原来只取"那一天", 现在取"那一段"的第一天)
      c_seg_end   第一段的最后一天 (段内每天都算同一次回踩)
      in_c_seg    观察日 T 是否落在第一段里 -> 这才是买点判据
      gap         观察日 T 的离线% (只有 in_c_seg 时才有意义)

    v2.260b 为什么改成"段"而不是"天"(用户 m03201 原话):
      "我把短时间内的继续接近趋势线当成二次回踩了。第一次接近 1.5% 你当成第一次回踩,
       如果次日更加接近变成 1% 你当成第二次, 第三天接近到 0.5% 又当成第三次,
       但实际上只是因为这几天在逐渐接近趋势线。"
      实测: 4965 只票的 2798 个贴线段里, 788 段(28%)是 2 天以上(最长 8 天),
      例如白云机场 2026-08-12 gap+1.02% → 08-13 gap+0.65%, 第二天会被误判成"第二次回踩"而丢掉。
      回测对比(2022-01 起 4974 只, 见 _probe_wave\\_seg_bt.py):
        旧口径(只认第一天) 5157 事件 → +30d 47.8%/+1.63%
        新口径(段内每天都算) 6677 事件 → +30d 48.2%/+1.64%
      **机会多 29% 而质量不变**, 所以改成"段"。
    v2.270 (用户 m07460): A/B 两点改用**收盘价** ab_px(); 斜率随之重算。
      贴线距离 g 与今日离线 gap 仍用 pv(m02603 原意: 阳线的插针最低点也算触及趋势线),
      破线本来就用 close —— 所以整条线现在是"收盘价画线 + 插针允许触及"。
    v2.271 (用户 m07846): "今日离线根据口径不同变化呀。用的哪个口径就要按哪个口径算离线距离"
      ⇒ 贴线距离 g 与今日离线 gap 的**分子**也跟口径走:
        A 口径='low' → 今日取价用最低价;  A 口径='close' → 用收盘价。
      (今天这一根没有"开"语义, 三种 B 口径里 open 只用于 B 点, 不用于今日取价;
       用 A 口径保证今日取价永远有值, 不会因为今天收阴就变成 None。)
      破线判据始终是 close (用户: 收盘价绝不允许跌破趋势线), 不随口径变。
    """
    paxf = PX_A.get(a_src) or px_close
    pbxf = PX_B.get(b_src) or pxb_close
    # 今日取价 = A 点口径 (v2.271: 离线距离跟口径走)
    txf = paxf
    pa = paxf(rows[iA])
    sl = (pbxf(rows[iB]) - pa) / float(iB - iA)
    spct = sl / pa * 100.0 if pa > 0 else 0.0

    def lv(k):
        return pa + sl * (k - iA)

    # ---- 首次收盘破线 (无容差: 用户原话"不再接受假跌破") ----
    i_break = None
    for j in range(iB + 1, T + 1):
        v = lv(j)
        if v > 0 and rows[j]['close'] < v:
            i_break = j
            break

    # ---- 峰: close 比前后各 PIV 根都高 (小山峰), 且必须在 B 之后、破线之前 ----
    # v2.260a 修正: 起点从 iB+PIV+1 改成 iB+PIV
    #   峰只需要"前后各 PIV 根 close 都更低", 不必离 B 点再额外隔 PIV 根;
    #   原来多减的那一根会把"紧贴 B 之后 4 根就冒出来的峰"整片跳过
    #   (浙江震元 SZ000705: B=2026-07-10, 峰=2026-07-16 只隔 4 根 → 旧代码找不到峰,
    #    2026-07-22 那个首回踩买点被漏掉。修正后 7-22 正常选出,
    #    7-24 是第二次贴线, 按"只要首次回踩"的规则仍不选, 符合用户口径)
    peak = None
    hi = T if i_break is None else i_break - 1
    for j in range(iB + PIV, hi - PIV + 1):
        cj = rows[j]['close']
        if all(cj > rows[k]['close'] for k in range(j - PIV, j)) and \
           all(cj > rows[k]['close'] for k in range(j + 1, j + PIV + 1)):
            peak = j
            break

    # ---- 首次回踩段: 峰之后第一段连续"0 <= 离线 <= GAP_MAX" ----
    # v2.260b: 原来是"第一次进带那一天"(break 掉), 现在把整段都算作同一次回踩。
    #   段内只要收盘不破线、离线没跑到 GAP_MAX 之外, 就还是这一段;
    #   出了带(离线 > GAP_MAX)或收盘破线, 这一段结束, 后面再进带就是第二次回踩。
    i_c = None
    c_seg_end = None
    if peak is not None:
        for j in range(peak + 1, hi + 1):
            v = lv(j)
            if v <= 0:
                continue
            tj = txf(rows[j])
            if tj is None:
                tj = rows[j]['close']
            g = (tj - v) / v * 100.0
            if 0.0 <= g <= GAP_MAX:
                if i_c is None:
                    i_c = j          # 第一段的第一天
                c_seg_end = j        # 一直往后推, 直到断开
            else:
                if i_c is not None:
                    break            # 第一段结束

    vt = lv(T)
    _tvt = txf(rows[T])
    if _tvt is None:
        _tvt = rows[T]['close']
    gap = ((_tvt - vt) / vt * 100.0) if vt > 0 else -999.0
    in_c_seg = (i_c is not None and c_seg_end is not None and i_c <= T <= c_seg_end)
    return {
        'iA': iA, 'iB': iB, 'span': iB - iA, 'slope': spct,
        'gap': gap, 'lvT': vt,
        'i_break': i_break, 'i_peak': peak, 'i_c': i_c,
        'c_seg_end': c_seg_end, 'in_c_seg': in_c_seg,
        'alive': (i_break is None),
        'adate': rows[iA]['date'], 'bdate': rows[iB]['date'],
        'alow': pa, 'blow': pbxf(rows[iB]),
        'a_src': a_src, 'b_src': b_src,
        'peak_date': rows[peak]['date'] if peak is not None else '',
        'c_date': rows[i_c]['date'] if i_c is not None else '',
        'c_end_date': rows[c_seg_end]['date'] if c_seg_end is not None else '',
    }


def pick_line(rows, T, iA, bs, a_src='low', b_src='close'):
    """多个 B 候选里选一条 -> 最早那个"活到今天"的 B 画出来的线

    为什么是"最早的 B": 实盘中趋势线是在**第一个**合格的低点出现时就画下去的,
      后面再出现的 B 属于"事后重新画线", 会把买点往后推。
      用户要的是"趋势线画出之后的首次回踩", 那就必须是当时那条线。
    为什么只留"活着"的: 收盘破过线的线在破线那一刻就作废了,
      拿它继续算"首次回踩"会把买点放到破位之后 —— 新疆天业 9/15 那根大阴线
      就是这么被误当成买点的 (旧口径)。
    """
    for iB in sorted(bs):                 # 从小到大 = 最早的 B 优先
        r = eval_line(rows, T, iA, iB, a_src, b_src)
        if r['alive']:
            return r
    return None


# ============================================================
# 单只判定
# ============================================================
def evaluate_one(code, rows, T, intraday=False, a_src='low', b_src='close', _cache=None):
    """在 rows[T] = 观察日 上算一只票 -> dict (数据不足返回 None)

    intraday 只影响量比 vr 的算法(盘中用昨日量), v2.260 里 vr 不参与筛选。
    v2.271 (用户 m07829): a_src/b_src 决定 A/B 两点取价口径, 全链路透传。
    _cache: 同一只票跑多个口径时复用的 memo (A 点只跟 a_src 有关, 别重复找)
    ⚠ rows 必须已经包含观察日那一根; rt 覆盖由调用方在传进来之前做完。
    """
    if T < MIN_BARS - 1 or T >= len(rows):
        return None
    try:
        bar = rows[T]
        c = bar['close']
        if c <= 0 or T < 1:
            return None
        pc = rows[T - 1]['close']
        if pc <= 0:
            return None

        # ---- 缩量(只显示) ----
        if intraday:
            vt = rows[T - 1]['vol'] if T >= 6 else bar['vol']
            vbase = sum(x['vol'] for x in rows[T - 6:T - 1]) / 5.0 if T >= 6 else vt
        else:
            vt = bar['vol']
            vbase = sum(x['vol'] for x in rows[T - 5:T]) / 5.0 if T >= 5 else vt
        vr = (vt / vbase) if vbase > 0 else 9.99

        chg = (c - pc) / pc * 100.0

        # ---- 趋势线 (A 点只跟 a_src 有关, 六口径复用时走 memo) ----
        _kA = 'iA:' + a_src
        if _cache is not None and _kA in _cache:
            iA = _cache[_kA]
        else:
            iA = find_A(rows, T, a_src)
            if _cache is not None:
                _cache[_kA] = iA
        _kB = 'bs:%s:%s' % (a_src, b_src)
        if _cache is not None and _kB in _cache:
            bs = _cache[_kB]
        else:
            bs = cand_bs(rows, T, iA, a_src, b_src) if iA is not None else []
            if _cache is not None:
                _cache[_kB] = bs
        best = pick_line(rows, T, iA, bs, a_src, b_src) if bs else None

        # ---- 买点: 今天落在"第一段"里 + 前5日收盘全高于今日 ----
        # v2.260b: c_today 从"i_c == T"改成"T 落在第一段区间内"
        c_today = bool(best is not None and best.get('in_c_seg') and best['i_c'] <= T <= best['c_seg_end'])
        prev5_ok = False
        if c_today:
            prev5_ok = all(rows[k]['close'] > c for k in range(T - PREV_HIGH_N, T))
        is_buy = bool(c_today and prev5_ok)

        ma5 = sum(x['close'] for x in rows[T - 4:T + 1]) / 5.0
        sigs = []
        if c > bar['open']:
            sigs.append('收阳')
        if c > ma5:
            sigs.append('站5')

        # 今日取价 —— 随 A 点口径走 (v2.271 用户 m07846: 离线距离按所选口径算)
        _pxf = PX_A.get(a_src) or px_close
        _pv_today = _pxf(bar)
        if _pv_today is None:
            _pv_today = bar['close']

        return {
            'code': code, 'c': c, 'pc': pc, 'o': bar['open'],
            'low': bar['low'], 'high': bar['high'], 'pv': _pv_today,
            'chg': chg, 'vr': vr, 'shrink': vr < SHRINK_MAX,
            'ma5': ma5, 'sig': '+'.join(sigs) or '无',
            'best': best, 'is_buy': is_buy, 'prev5_ok': prev5_ok,
            'c_today': c_today,
            'n_b': len(bs), 'has_a': iA is not None,
            'n_line': 1 if best else 0,
            'a_src': a_src, 'b_src': b_src,
        }
    except (KeyError, ValueError, ZeroDivisionError, IndexError):
        return None


def hit(a):
    """套口径 -> (是否入选, 未过的第一条原因)"""
    if a.get('is_st') or a.get('is_bz'):
        return False, 'ST/北交所'
    if a['chg'] >= a['lp'] - LIMIT_MARGIN:
        return False, '已涨停'
    if not a['has_a']:
        return False, 'A点太新'
    if a['n_b'] == 0:
        return False, '无合格B点'
    if not a['best']:
        return False, '趋势线已破'
    if a['best']['i_c'] is None:
        return False, '还没走出首次回踩'
    if not a['c_today']:
        return False, '今日不在首次回踩段内'
    if not a['prev5_ok']:
        return False, '前5日收盘非全高'
    return True, ''


# ============================================================
# 全市场扫描
# ============================================================
def scan(progress_callback=None, rt_prices=None, log=None, asof=None, intraday=False,
         all_combos=True):
    """波段低吸 完整流程 (全本地, 零网络 —— 除非传了 rt_prices)

    asof:      指定观察日 'YYYY-MM-DD'; None = 自动取最新 (末日出现次数最多的那天)
    intraday:  盘中/午休=True (量能用昨日); 盘后=False
    all_combos: v2.271 (用户 m07829) True = 六种 A/B 口径全跑;
                False = 只跑默认口径 (低/收), 给自测/回测用

    返回 dict:
      ok / date / n_pool / n_total / n_hit / rows / funnel / stages / elapsed / note
      combos: [{key,a,b,cn,n_total,n_hit,funnel,rows}, ...]   ← 顺序 = 按钮切换顺序
      combo_default: 默认组合 key
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
        return {'ok': False, 'note': 'K线目录不可用: %s' % d, 'rows': [], 'combos': [],
                'n_pool': 0, 'n_total': 0, 'n_hit': 0, 'elapsed': 0}
    # 北交所一律不要 (BZ 前缀); 只做沪深
    files = [fn for fn in files if fn[:2].upper() in ('SH', 'SZ')]
    files.sort()
    _log('[启动] 波段低吸 · 本地CSV %d 个 · %s' % (len(files), _now().strftime('%H:%M:%S')))
    stages['list'] = round(time.time() - t0, 2)

    tail = TAIL_ASOF if asof else TAIL_NORMAL

    # ---- 第一趟: 读尾部, 同时统计"末日分布"定观察日 ----
    t_read = time.time()
    cache = {}
    date_cnt = {}
    n = len(files)
    for i, fn in enumerate(files):
        if i % 400 == 0:
            _p(1, 3, '读取K线 %d/%d ...' % (i, n))
        code = fn[:-4].upper()
        rows = read_csv_tail(os.path.join(d, fn), tail)
        if len(rows) < MIN_BARS:
            continue
        cache[code] = rows
        d_last = rows[-1]['date']
        date_cnt[d_last] = date_cnt.get(d_last, 0) + 1
    stages['read'] = round(time.time() - t_read, 2)

    data_date = max(date_cnt.items(), key=lambda kv: (kv[1], kv[0]))[0] if date_cnt else _today()
    _log('[数据] 最新交易日 = %s (末日为这天的文件 %d 个 / 共 %d 个)'
         % (data_date, date_cnt.get(data_date, 0), len(cache)))
    if asof:
        asof = str(asof)[:10]
        if asof < data_date:
            _log('[数据] 按指定观察日回看: %s (原最新 %s)' % (asof, data_date))
            data_date = asof
        else:
            _log('[数据] 指定观察日 %s 不早于最新交易日, 忽略, 仍用 %s' % (asof, data_date))

    # ---- 第一趟半: 定每只票的观察日下标 + 盘中实时价覆盖 (六个口径共用) ----
    # v2.271: 原来这段在逐只循环里, 六口径各跑一遍会重复 6 次; 提到外面只做一次。
    prepared = []
    for code, rows in cache.items():
        # 停牌/滞后: 日常扫描时观察日必须落在最后 6 根之内; asof 回看时允许任意位置
        T = None
        lo = max(len(rows) - 7, -1) if not asof else -1
        for j in range(len(rows) - 1, lo, -1):
            if rows[j]['date'] == data_date:
                T = j
                break
        if T is None:
            continue
        # 盘中实时价覆盖观察日那根; 回看历史时绝不覆盖
        if rt_prices and not asof:
            bare = code[2:]
            px = rt_prices.get(bare) or rt_prices.get(code)
            if px and px > 0:
                rows = list(rows)
                b = dict(rows[T])
                b['close'] = px
                b['high'] = max(b['high'], px)
                b['low'] = min(b['low'], px)
                rows[T] = b
        prepared.append((code, rows, T))

    # ---- 第二趟: 逐口径逐只算 ----
    t_calc = time.time()
    name_map, ind_map, con_map, st_set = load_meta()
    combos_run = COMBOS if all_combos else [(DEFAULT_COMBO.split('|')[0],
                                             DEFAULT_COMBO.split('|')[1])]
    combos_out = []
    by_key = {}
    n_eval = 0
    for ci, (a_src, b_src) in enumerate(combos_run):
        _p(2, 3, '计算趋势线 %s (%d/%d)...' % (cn(a_src, b_src), ci + 1, len(combos_run)))
        out = []
        for code, rows, T in prepared:
            memo = {}
            a = evaluate_one(code, rows, T, intraday=bool(intraday and not asof),
                             a_src=a_src, b_src=b_src, _cache=memo)
            if a is None:
                continue
            if ci == 0:
                n_eval += 1        # 可分析只数各口径相同, 只数一遍
            a['lp'] = limit_pct(code)
            a['is_st'] = code in st_set
            a['is_bz'] = code.startswith('BZ')
            a['name'] = name_map.get(code, code[2:])
            a['ind'] = ind_map.get(code, '')
            a['con'] = con_map.get(code, '')
            a['ok'], a['why'] = hit(a)
            out.append(a)

        rows_hit = [a for a in out if a['ok']]
        # 排序: 离线小的在前 (= 越贴近趋势线, 越接近买点)
        rows_hit.sort(key=lambda a: (a['best']['gap'] if a['best'] else 99, a['vr']))

        live = [a for a in out if not a['is_st'] and not a['is_bz']]

        def _s1(x):
            return x['chg'] < x['lp'] - LIMIT_MARGIN

        funnel = {
            'total': len(out),
            'no_st_bz': len(live),
            'limit': sum(1 for a in live if _s1(a)),
            'has_a': sum(1 for a in live if _s1(a) and a['has_a']),
            'has_b': sum(1 for a in live if _s1(a) and a['has_a'] and a['n_b'] > 0),
            'alive': sum(1 for a in live if _s1(a) and a['has_a'] and a['n_b'] > 0
                         and a['best']),
            'first_c': sum(1 for a in live if _s1(a) and a['has_a'] and a['n_b'] > 0
                           and a['best'] and a['c_today']),
            'hit': len(rows_hit),
        }
        one = {'key': ck(a_src, b_src), 'a': a_src, 'b': b_src,
               'cn': cn(a_src, b_src), 'a_cn': COMBO_LBL.get(a_src, '?'),
               'b_cn': COMBO_LBL.get(b_src, '?'),
               'n_total': n_eval, 'n_hit': len(rows_hit),
               'funnel': funnel, 'rows': rows_hit}
        combos_out.append(one)
        by_key[one['key']] = one
        _log('[口径 %s] 入选 %d 只  (漏斗 first_c=%d / alive=%d / has_b=%d)'
             % (one['cn'], one['n_hit'], funnel['first_c'], funnel['alive'], funnel['has_b']))
    stages['calc'] = round(time.time() - t_calc, 2)

    base = by_key.get(DEFAULT_COMBO) or (combos_out[0] if combos_out else None)
    if base is None:
        base = {'key': DEFAULT_COMBO, 'cn': cn(*DEFAULT_COMBO.split('|')),
                'rows': [], 'funnel': {}, 'n_hit': 0}
    funnel = base['funnel']
    rows_hit = base['rows']

    res = {
        'ok': True,
        'date': data_date,
        'n_pool': len(cache),
        'n_total': n_eval,
        'n_hit': len(rows_hit),
        'rows': rows_hit,
        'funnel': funnel,
        'combos': combos_out,
        'combo_default': DEFAULT_COMBO,
        'stages': stages,
        'elapsed': round(time.time() - t0, 2),
        'note': ('观察日 %s · 可分析 %d 只 → 默认口径[%s] 入选 %d 只 '
                 '(A点%s / B点%s / 收盘不破线 / '
                 '首次回踩段内离线0~%.1f%% / 前%d日收盘全高于今日 / 排除ST·北交所·涨停)'
                 % (data_date, n_eval, base.get('cn', '低/收'), len(rows_hit),
                    COMBO_LBL.get(base.get('a'), '低'), COMBO_LBL.get(base.get('b'), '收'),
                    GAP_MAX, PREV_HIGH_N)),
    }
    _log('[结果] ' + res['note'])
    _log('[漏斗] 有数据 %d → 非ST非北交所 %d → 非涨停 %d → 有A点 %d → 有合格B点 %d '
         '→ 线未破 %d → 今日在首次回踩段内 %d → **入选 %d**  总耗时 %.1fs'
         % (funnel.get('total', 0), funnel.get('no_st_bz', 0), funnel.get('limit', 0),
            funnel.get('has_a', 0), funnel.get('has_b', 0), funnel.get('alive', 0),
            funnel.get('first_c', 0), funnel.get('hit', 0), res['elapsed']))
    # 六口径一览 (自测/排障用)
    if all_combos:
        _log('[六口径] ' + ' | '.join('%s:%d' % (c['cn'], c['n_hit']) for c in combos_out))
    return res


def to_scan_rows(res, rows=None, combo=None):
    """把 scan() 结果 (或某一口径的 rows) 转成终端表格行

    用法:
      to_scan_rows(res)                      → 默认口径 (低/收) 的行
      to_scan_rows(res, rows, combo=dict)    → 指定口径的行 (combo = combos 里那一项)

    列序 (14 列):
      wave 离线% | code | name | change_pct 涨幅 | price 现价 |
      pull 自A涨% | vr 量比 | slope 斜率% |
      span 跨度 | sector 行业 | zt_cnt 梯队 | concept 概念 | peak_date 峰值日 | 操作

    ⚠ v2.260: pull/retrace 已不参与筛选, 这里改为给"从 A 点涨了多少 / 从峰回撤多少"
      这两个用户看得懂的度量; touch 改为"此线是第几次回踩"(恒为 1, 保留字段位)。
    """
    _src = rows if rows is not None else (res.get('rows') or [])
    out = []
    for a in _src:
        b = a['best']
        # 从 A 点(百日最低)涨到今天多少 —— 用户关心的"相对百日低点已涨多少"
        rise = (a['c'] - b['alow']) / b['alow'] * 100.0 if b['alow'] > 0 else 0.0
        # v2.261 (2026-11-08 用户 m03399): 下发给前端的 code 一律**裸 6 位**
        #   ★ 为什么: 内部用 8 位带前缀查 load_meta, 但前端 selectStock() 会把 r.code 塞进
        #     K 线搜索框、renderStockDetail() 直接上屏 → 详情页/搜索框多出「SZ」前缀,
        #     而其它九个模式都是裸 6 位。
        #   ★ 顺带修掉一个静默 bug: /api/scan/cache/prices 回写用 str(r['code']) 匹配前端
        #     的 6 位键 → 8 位永远匹配不上, 本模式实时价回写一直静默失败。
        _c6 = a['code'][2:] if a['code'][:2].upper() in ('SH', 'SZ', 'BJ', 'BZ') else a['code']
        out.append({
            'code': _c6,
            'code8': a['code'],      # 保留 8 位, 万一以后要反查 (当前前端不用)
            'name': a['name'],
            'price': a['c'],
            'change_pct': a['chg'],
            # 默认排序键 (升序: 离线越小越贴近趋势线)
            'wave': b['gap'],
            'cons_score': b['gap'],
            'pull': rise,
            'retrace': 0.0,
            'vr': a['vr'],
            'slope': b['slope'],
            'span': b['span'],
            'gap': b['gap'],
            'touch': 1,
            'ratio': 0.0,
            'adate': b['adate'],
            'bdate': b['bdate'],
            # v2.270 (用户 m07460): A/B 两点取点已改成**收盘价** (ab_px), 所以
            #   alow/blow 是 A/B 那两天的收盘价, 不是插针最低价。前端提示里写的
            #   "收盘"就是这个意思; 别再把它们当 low 用。
            'alow': b['alow'],
            'blow': b['blow'],
            'line_t': b['lvT'],
            'peak_date': b.get('peak_date', ''),
            'c_date': b.get('c_date', ''),
            # v2.260b: 首次回踩"段"的最后一天 —— 段内可能连好几天都算同一次回踩
            'c_end_date': b.get('c_end_date', ''),
            'c_days': ((b['c_seg_end'] - b['i_c'] + 1) if (b.get('i_c') is not None and b.get('c_seg_end') is not None) else 0),
            'hi': b['blow'],
            'hi_date': b['bdate'],
            'low': a['low'],
            # v2.260b: 今日"取价"(阳线=最低价/阴线=收盘价) —— 前端提示里要显示它,
            #   不单独下发的话 r.pv 是 undefined, 悬停会显示 NaN
            'pv': a.get('pv'),
            'ma5': a['ma5'],
            'sig': a['sig'],
            'sector': a['ind'],
            'concept': a['con'],
            'zt_cnt': 0,          # 前端会用 /api/sector_zt 的行业家数覆盖
            'is_valid': True,
            'mode': 'wave',
            'mode_detail': '波段低吸',
            # v2.271 (用户 m07829): 这一行属于哪个口径 —— 前端切口径时用来校验/显示
            'a_src': b.get('a_src') or a.get('a_src') or 'low',
            'b_src': b.get('b_src') or a.get('b_src') or 'close',
            'combo_key': ck(b.get('a_src') or a.get('a_src') or 'low',
                            b.get('b_src') or a.get('b_src') or 'close'),
            'combo_cn': cn(b.get('a_src') or a.get('a_src') or 'low',
                           b.get('b_src') or a.get('b_src') or 'close'),
        })
    return out


def to_scan_rows_all(res):
    """六口径一次全转 -> {'low|close': [rows...], 'low|low': [...], ...}

    v2.271 (用户 m07829): 扫描时把六种口径全跑出来, 服务器把它们一起存进缓存槽,
      前端切口径时不再重新扫描, 直接从这份里换表 (秒切)。
    """
    out = {}
    for one in res.get('combos') or []:
        out[one['key']] = to_scan_rows(res, one.get('rows') or [], combo=one)
    if not out:
        out[DEFAULT_COMBO] = to_scan_rows(res)
    return out


def combo_summary(res):
    """六口径一览 (给 /api/status 和前端状态条) —— 不含行数据"""
    out = []
    for one in res.get('combos') or []:
        out.append({
            'key': one['key'], 'a': one['a'], 'b': one['b'],
            'cn': one['cn'], 'a_cn': one.get('a_cn'), 'b_cn': one.get('b_cn'),
            'n_total': one.get('n_total'), 'n_hit': one.get('n_hit'),
            'funnel': one.get('funnel'),
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
    # 可用命令行指定回看日: python wave_scan.py 2026-08-13
    _asof = sys.argv[1] if len(sys.argv) > 1 else None
    print('=' * 118)
    print('波段低吸 自测   %s' % _now().strftime('%Y-%m-%d %H:%M:%S'))
    print('K线目录: %s' % kline_dir())
    print('口径 v2.270: A=百日(%d根)收盘最低 / B=A后%d~%d日更高低点(收盘) / 收盘绝不破线 / '
          '首次回踩离线0~%.1f%% / 前%d日收盘全高 / 排除ST·BZ·涨停'
          % (WIN, SPAN_MIN, SPAN_MAX, GAP_MAX, PREV_HIGH_N))
    print('取价: A/B 两点=收盘价 close; 今日离线% 仍用 阳线取最低价 low / 阴线取收盘价 close')
    if _asof:
        print('回看观察日: %s' % _asof)
    print('=' * 118)
    r = scan(asof=_asof)
    print('观察日:', r['date'], '| 可分析:', r['n_total'], '| 入选:', r['n_hit'],
          '| 耗时:', r['elapsed'], '秒')
    f = r.get('funnel') or {}
    print('漏斗: 有数据 %s -> 非ST非BZ %s -> 非涨停 %s -> 有A点 %s -> 有B点 %s '
          '-> 线未破 %s -> 今日在首次回踩段内 %s -> 入选 %s'
          % (f.get('total'), f.get('no_st_bz'), f.get('limit'), f.get('has_a'),
             f.get('has_b'), f.get('alive'), f.get('first_c'), f.get('hit')))
    print()
    print('%-9s %-8s %-14s %7s %7s %6s %6s %6s %5s %-11s %-11s'
          % ('代码', '名称', '行业', '现价', '离线%', '涨幅%', '自A涨%', '量比',
             '跨度', 'A->B', '峰'))
    for a in (r.get('rows') or [])[:40]:
        b = a['best']
        rise = (a['c'] - b['alow']) / b['alow'] * 100.0 if b['alow'] > 0 else 0.0
        print('%-9s %-8s %-14s %7.2f %+7.2f %+6.1f %+6.1f %6.2f %5d %s->%s %s'
              % (a['code'], a['name'][:8], (a['ind'] or '')[:14], a['c'], b['gap'],
                 a['chg'], rise, a['vr'], b['span'],
                 b['adate'][2:], b['bdate'][2:], (b.get('peak_date') or '-')[2:]))
    print()
    # ---- 正样本回归: 新疆天业 2026-08-13 必须入选 ----
    print('* 正样本回归 (新疆天业 SH600075 在 2026-08-13 应为买点):')
    try:
        rows = read_csv_tail(os.path.join(kline_dir(), 'SH600075.csv'), TAIL_ASOF)
        iT = next(i for i, x in enumerate(rows) if x['date'] == '2026-08-13')
        a = evaluate_one('SH600075', rows, iT)
        b = a['best'] if a else None
        if b:
            rise = (a['c'] - b['alow']) / b['alow'] * 100.0
            print('   A=%s@%.2f  B=%s@%.2f  跨度%d  斜率%.3f%%/日  峰=%s'
                  % (b['adate'], b['alow'], b['bdate'], b['blow'], b['span'],
                     b['slope'], b.get('peak_date') or '-'))
            print('   离线%+.2f%%  自A涨%+.1f%%  首次回踩段=%s~%s(%d天)  前5日全高=%s  => %s'
                  % (b['gap'], rise, b['c_date'] or '-', b.get('c_end_date') or '-',
                     (b['c_seg_end'] - b['i_c'] + 1) if b.get('i_c') is not None else 0,
                     a['prev5_ok'],
                     'PASS 是买点' if a['is_buy'] else 'FAIL 未入选'))
        else:
            print('   FAIL 算不出趋势线')
    except Exception as _e:
        print('   (跳过: %s)' % _e)
