# -*- coding: utf-8 -*-
"""竞价打板 · 主逻辑  (v2.249 新增模块, 2026-11-06)

⚠ 本文件是「竞价打板」模式的**全部业务逻辑**, 与终端现有功能零耦合:
   - 不 import engine (只借用它的 _read_csv_tail 读本地 CSV 的小函数)
   - 不写任何全局状态; 结果由 server.py 存进既有的 SCAN_STATE / scan_cache 槽
   - 摘除方式见 _uninstall_auction.py; 压舱开关见 auction_off.flag

===============================================================================
策略来源
===============================================================================
F:\\竞价选股 那一套 (作者称 v2.14)。条件原样搬过来, 阈值一个不改:

候选池 11 条硬条件 (全过才进池):
  ① 沪深主板 (600/601/603/605/000/001/002/003; 排除创业板300/科创板688/北交所)
  ② 非 ST (名称含 ST / *ST / 退)
  ③ 流通市值 < 160 亿   (= 流通股本 × **昨日收盘价**, 不是今日价)
  ④ 现价 < 50 元        (用今开价; v2.254 由 20 放宽到 50)
                        注: 现价 >20 元的票不淘汰, 只在界面上标红提醒
  ⑤ 昨日 K 线阳线       (收 ≥ 开, **含平盘**)
  ⑥ 20 个交易日内现过涨停 (收盘涨幅 ≥9.8%, **不含当日**, 防未来函数)
  ⑦ 量比 > 1            (= 竞价量 / (前5日均量 / 240))
  ⑧ 竞价涨幅 > 0%
  ⑨ 竞价额占昨全日 > 1% 且 ≤ 15%
  ⑩ 昨日成交额 > 1 亿
  ⑪ 今开价 ≠ 涨停价      (否则买不进)

主推/备选:
  竞价涨幅 ∈ [3%, 7%] 且 占比 ≥2%(v2.296 起无上限) 且 市值 <160 亿
  v2.295 (2026-11-13 用户 m14470) 起:
    区间 [4.5%, 6%] (含两端) 内按**竞总比**降序, 第1=主推, 第2=备选, 第3=备选*
  ★ 区间外的一律不进主推: ≥7% 直接淘汰; 1%~4.5% 只进「观察」名单
  (旧口径是 [3%,7%] 按竞价涨幅降序取前二 —— 已废弃)
  ★ 无票满足 → 今日空仓, 绝不硬塞 (2026-11-06 修 F 盘原代码的 bug)

大盘开关:
  上证 9:25 竞价 < −0.3% → 降半仓 (只买主推 50%, 备选空仓)

===============================================================================
数据源 (2026-11-06 凌晨全部实测, 一个都没猜)
===============================================================================
  实时快照(今开/昨收/竞价量/竞价额)  腾讯 qt.gtimg.cn    50只/批, 3488只≈3.6s
  昨日全天成交额 + 前5日均量          腾讯 newfqkline     第9位=成交额(万), 第6位=量(手)
  历史K线(阳线/20日涨停)             本地 CSV kline_data/daily/*.csv  零网络
  流通市值                           本地 CSV 第8列 (亿元)
  上证竞价涨跌                       腾讯 sh000001 快照

★ 为什么踩点用腾讯而不是东财:
  腾讯分时第1根 `0930 14.32 36795 52690440.00` (时间/价/量手/额元) 与 F 盘用东财
  trends2 算出的竞价量 36795 手、额 5269 万**完全一致** —— 两个源同口径, 可互换。
  而东财 push2 是 AGENTS.md:104 明令禁用的, 腾讯本来就在生产用 (server.py:246)。

★ 时间闸 (整个功能成立的前提):
  只在 09:25:00 ~ 09:29:59 认为「腾讯快照的成交量 = 竞价量」。
  9:30 以后快照的 vol/amt 是全天累计, 拿它算量比会错 28 倍
  (F 盘实测: 000506 盘后算量比 510.85, 真值 18.05)。所以过点仍可跑, 但只给观察名单,
  不给主推, 并在界面显著标注「⚠️ 已过 9:30, 数据非竞价口径」。
"""
import datetime
import json
import os
import math
import re
import sys
import threading
import time
import urllib.error
import urllib.request

# ---- 可选依赖: 都做 try 兜住, 缺了也不许崩 -------------------------------
try:
    from quant_terminal import net as _net
except Exception:
    try:
        import net as _net
    except Exception:
        _net = None

try:
    from quant_terminal import mcap as _mcap
except Exception:
    try:
        import mcap as _mcap
    except Exception:
        _mcap = None

try:
    from quant_terminal import engine as _engine
except Exception:
    try:
        import engine as _engine
    except Exception:
        _engine = None

# 独立实现的 CSV 读取 —— 不硬依赖 engine (摘除 engine 也能跑)
# v2.250 (2026-11-06) 用户决定: 取消公告核查
#   理由(用户原话): "他这个模式当天竞价买第二天就要卖, 公告如果真有利空,
#   盘前的利空如果有用, 那竞价就不应该还能高开。"
#   → 9:25 的竞价价格已经把所有盘前公开信息定价进去了, 再拿标题猜一遍
#     不但不增加信息量, 还会因为关键词误判错杀 (2026-11-06 实测:
#     600261 阳光照明被「问询函的回复」误判成"收到问询函"而淘汰, 当日涨停)。
#   模块 auction_ann.py 保留但不再被本文件引用, 想恢复只需import回来。

_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'

# ============================================================
# 阈值 —— 与 F:\竞价选股 v2.14 一致, 除了下面标注的几处改动:
#         v2.254: PRICE_MAX 20→50 (用户要求放宽), PRICE_SOFT=20 只做界面标红
#         v2.262: 用户 m03902 新增四道「硬排除」, 详见下方 HARD_* 注释
# ============================================================
MKT_CAP_MAX = 160.0      # 流通市值上限 (亿元)
PRICE_MAX = 50.0         # 现价上限 (元, 用今开价) —— v2.254 由 20 放宽到 50
PRICE_SOFT = 20.0        # 现价 >20 元不再淘汰, 但界面标红提醒 (v2.254)
YDAY_AMT_MIN = 5e7       # 昨日成交额下限 (元) = 5000 万 (v2.262: 由 1 亿放宽)
AUCT_RATIO_MIN = 2.0     # 竞价额占昨全日下限 (%) (v2.262: 由 1.0 提高)
AUCT_RATIO_MAX = 999.0   # v2.296: 上限已取消(原 15.0), 只保留 2% 下限。
                         #   用户 m14608 原话: "竞价额高也代表资金关注度高, 我认为不是坏事情,
                         #   就今天的结果来看, 竞总比小的, 涨停后也炸板了, 说明资金关注度低,
                         #   市场情绪转弱就被砸开了。所以只设置一个下限，不设上限"
                         #   15% 的老理由是「占比过高=诱多出货(黑芝麻24.71%)」, 与 v2.295 的
                         #   「按竞总比降序取前三」直接冲突 ⇒ 按用户裁决拆掉。
                         #   这个常量现在**只用于界面染色阈值**, 不再参与任何淘汰判断。
AUCT_PCT_MIN = 1.0       # 竞价涨幅下限 (%) (v2.262: 由 0.0 提高到 1.0)

# ---- v2.262 (2026-11-08 用户 m03902) 四道「硬排除」 ---------------------
# 用户原话: "筛除竞价涨幅＜1%的不进入股票列表, 竞价占比＜2%, 量比＜5也排除掉,
#           昨日成交额低于5千万的也排除"
#
# 注意「不进入股票列表」≠ 现行 classify 的 verdict='no' (那是"进列表但标淘汰")。
# 这四道要在**拼 sample 之前**就把票剔掉, 所以走 HARD_GATE, 不走 classify。
# 阈值边界取**闭区间**: 用户说"＜1%排除" ⇒ =1% 保留。
#
# 为什么不能全塞进第二步粗筛: 占比和量比要等 09:30 竞价量额锁定,
# 昨额要等腾讯日K回来, 都发生在粗筛之后 —— 所以单独设一道闸。
HARD_PCT_MIN = AUCT_PCT_MIN      # 竞价涨幅 >= 1%
HARD_RATIO_MIN = AUCT_RATIO_MIN  # 竞价占比 >= 2%
HARD_VOLRATIO_MIN = 5.0          # 量比 >= 5
HARD_PREV_AMT_MIN = YDAY_AMT_MIN  # 昨日成交额 >= 5000 万
# v2.295 (2026-11-13 用户 m14470): 主推区间由 [3%, 7%] 收窄到 [4.5%, 6%],
#   并且排序键由「竞价涨幅」改成「竞总比」(见 screen())。
#   用户原话:「"主推"改成竞价涨幅在4.5%-6%区间，包含4.5%和6%，按竞总比排序」
AUCT_PCT_MAIN_LO = 4.5   # 主推区间下限 (%) —— 闭区间, =4.5 算进
AUCT_PCT_MAIN_HI = 6.0   # 主推区间上限 (%) —— 闭区间, =6.0 算进
AUCT_PCT_HI = 7.0        # v2.295: ≥7% 判淘汰(原来是 MAIN_HI 兼任, 现在两者分开了)
VOLRATIO_MIN = 5.0       # 量比下限 (v2.262: 由 1.0 提高到 5.0, 与 HARD_VOLRATIO_MIN 同步)
LIMITUP_DAYS = 20        # 「20日内现涨停」窗口
LIMITUP_PCT = 9.8        # 涨停判定阈值 (收盘涨幅 %)
INDEX_DROP_HALF = -0.3   # 大盘开关: 上证竞价低于此值 → 降半仓 (%)
# v2.253 (2026-11-06): 竞价量/额「锁定」策略
#   09:25~09:30  → 腾讯快照的 vol/amt 本身就是竞价量, 直接用 (mode=live)
#   09:30 之后   → 快照变全天累计, 改读 腾讯 day/query 的 09:30 bar (mode=locked)
#                 那根是竞价定格结果, 收盘后依然可读
#   取不到       → 退回全天空口径, 标记 mode=replay 且不给主推
AUCT_MINUTES = 240.0     # 量比分母固定除 240 分钟 (F 盘实测口径, 不是 241/245)

# ---- v2.274 (2026-11-11 用户 m09088) 实际换手率 / 封板率 ----------------------
# 用户原话: 「把"竞价打板"模式的表头"昨额"替换成"换手"... 实际换手率 = 成交量 ÷ 自由流通股本」
#   ⇒ 用 v2.273 刚落的 CSV 第 9 列「自由流通股本」(万股) 当分母。
#   ★ 单位链: 本地/腾讯/新浪/同花顺的 vol 全是**手**(100股), free 是**万股**。
#     换手率% = (vol手 × 100股) / (free万股 × 10000股) × 100 = **vol / free**
#     （推导见 turnover_pct() 的 docstring；我第一次按 vol/free/100 写，结果是 0.0727，差 100 倍）
#     实测核对 002973 侨银: 昨日 vol=123511 手, free=16985.85 万股 → 7.27% ✔ (与 v2.273 交付表一致)
TURN_SWITCH_MIN = 10 * 60   # 10:00 —— 之前显示昨日换手(方便选股), 之后显示当日实时换手(看有没有出货)
# 用户 m09088: 「"流通市值"表头换成"封板率"… 最近500个交易日或者说最近500根K线
#   （上市后总K线数不满则最多有多少根就用多少根）涨停封板总数占最高涨幅＞9.5%的比」
#   ★ 分母用**最高价**涨幅(盘中摸过就算), 分子用**收盘**涨停(真封住了) —— 用户明确说了
#     "注意这里是最高涨幅不是收盘涨幅, 盘中触及9.5%收盘回落到1%也算一次分母"。
SEAL_WINDOW = 500            # 回看 K 线根数
SEAL_TOUCH_PCT = 9.5         # 分母: 最高价涨幅 > 9.5% (2026-11-11 用户原话)
SEAL_LOCK_PCT = 9.8          # 分子: 收盘涨幅 >= 9.8% (与 LIMITUP_PCT 同口径)

# 时间闸
T_OPEN = (9, 25)         # 竞价结束
T_CUT = (9, 30)          # 连续竞价开始 —— 过此点数据不再是竞价口径

MAIN_BOARD_PREFIX = ('600', '601', '603', '605', '000', '001', '002', '003')
# ★ v2.376 (2026-11-23 用户 m00833): 北交所号段 —— 920 是 2024 年后统一新段
#   (本地 357 只全在这一段), 43/83/87/88 是老三板/新三板转板遗留段。
#   必须与前端 terminal.html 的 _tierLim() 用同一套正则, 两端判板别不能分叉。
BSE_PREFIX = ('92', '43', '83', '87', '88')
# 一字板参考名单最多返回多少只 (前端默认只展开 10 只, 其余手动展开)
REF_MAX = 60

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')

# ============================================================
# 小工具
# ============================================================


def _now():
    return datetime.datetime.now()


def _today():
    return _now().strftime('%Y-%m-%d')


def _hm(dt=None):
    dt = dt or _now()
    return dt.hour * 60 + dt.minute


def in_auction_window(now=None):
    """当前是否处在「快照成交量 = 竞价量」的窗口内 (09:25 ~ 09:30)"""
    n = now or _now()
    if n.weekday() >= 5:
        return False
    m = n.hour * 60 + n.minute
    return (T_OPEN[0] * 60 + T_OPEN[1]) <= m < (T_CUT[0] * 60 + T_CUT[1])


def past_auction(now=None):
    """已经过了 9:30 —— 数据不再具备竞价口径"""
    n = now or _now()
    return (n.hour * 60 + n.minute) >= (T_CUT[0] * 60 + T_CUT[1])


def is_main_board(code):
    c = re.sub(r'\D', '', str(code or ''))
    return len(c) == 6 and c.startswith(MAIN_BOARD_PREFIX)


def is_st(name):
    n = (name or '').upper().replace(' ', '')
    return ('ST' in n) or ('退' in n)


def limit_up_pct(code):
    """该股涨停幅度(%) —— 主板 10, 其余 20 (本模式只选主板, 兜底留着)

    ★ 2026-11-06 修: 原来写 9.9/19.9, 导致涨停价算低。实测 605339 南侨食品
      昨收 14.36, 真涨停价 15.80, 按 9.9% 算成 15.78, 差 0.02 > 容差 → 一字板没识别出来。
    """
    c = re.sub(r'\D', '', str(code or ''))
    # v2.376: 北交所 30% —— 原来非主板一律按 20% 算, 920 段的涨停价会低 8.3%,
    #   一字板全部漏识别 (20% 档算出来的价远低于真涨停价)。
    if c.startswith(BSE_PREFIX):
        return 30.0
    return 10.0 if c.startswith(MAIN_BOARD_PREFIX) else 20.0


def zt_price(code, prev):
    """交易所真实涨停价 = 昨收 ×(1+幅度), 四舍五入到分(round-half-up)。

    不能直接用 Python 的 round(): 它是银行家舍入, 4.55×1.1=5.005 会给出 5.00,
    而交易所给的是 5.01。
    """
    if not prev or prev <= 0:
        return None
    pct = limit_up_pct(code)
    return math.floor(prev * (1 + pct / 100.0) * 100.0 + 0.5) / 100.0


def _f(x, d=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return d


def _data_date_from_ts(ts):
    """腾讯快照 f[30] (形如 20261105161454) → 'YYYY-MM-DD'

    ★ 决策日必须来自快照自己声明的行情时间, 不能取系统 today:
      凌晨/周末跑时快照仍是上一个交易日的收盘数据, 用 today 会让
      「昨日」和 quote['prev'] 错位一天 (2026-11-06 实测踩到)。
    """
    t = re.sub(r'\D', '', str(ts or ''))
    if len(t) < 8:
        return None
    y, m, d = t[0:4], t[4:6], t[6:8]
    if not (y.isdigit() and m.isdigit() and d.isdigit()):
        return None
    if not (1 <= int(m) <= 12 and 1 <= int(d) <= 31):
        return None
    return '%s-%s-%s' % (y, m, d)


# ============================================================
# 网络: 腾讯实时快照
# ============================================================
def _tx_sym(num):
    # ★ v2.376 (2026-11-23 用户 m00833): 补北交所分支。
    #   920xxx 以 '9' 开头, 按下面那行会被错发成 sh920001 —— 腾讯回
    #   v_pv_none_match="1"; (空串, 拿不到任何字段)。
    #   实测 bj920001 / bj430047 / bj830799 全部正常返回 87 个字段。
    n = str(num)
    if n.startswith(BSE_PREFIX):
        return 'bj' + n
    return ('sh' + n) if n.startswith(('6', '9')) else ('sz' + n)


def _get(url, timeout=10, encoding='utf-8', referer=None):
    h = {'User-Agent': _UA}
    if referer:
        h['Referer'] = referer
    req = urllib.request.Request(url, headers=h)
    if _net is not None:
        r = _net.open_url(req, timeout=timeout)
    else:
        r = urllib.request.urlopen(req, timeout=timeout)
    with r:
        return r.read().decode(encoding, 'ignore')


def fetch_quotes(nums, workers=20):
    """腾讯批量快照 → {裸码: {...}}

    字段: f[1]=名称 f[3]=现价 f[4]=昨收 f[5]=今开 f[6]=成交量(手) f[37]=成交额(万)
    键: 腾讯回 `v_sz000506="..."` → 取 sz000506 的 6 位裸码
    """
    nums = [re.sub(r'\D', '', str(n)) for n in nums]
    nums = [n for n in nums if len(n) == 6]
    out = {}
    if not nums:
        return out

    lock = threading.Lock()

    def _one_batch(part):
        got = {}
        try:
            txt = _get('http://qt.gtimg.cn/q=' + ','.join(_tx_sym(n) for n in part),
                       timeout=10, encoding='gbk')
        except Exception:
            return got
        for line in txt.split(';'):
            line = line.strip()
            if not line.startswith('v_') or '="' not in line:
                continue
            key = line[2:line.index('=')]
            f = line.split('="', 1)[1].rstrip('"').split('~')
            if len(f) < 38 or not f[3]:
                continue
            num = key[-6:]
            got[num] = {
                'name': f[1],
                'price': _f(f[3]),
                'prev': _f(f[4]),
                'open': _f(f[5]),
                'vol': _f(f[6]),                    # 手; 9:25~9:30 = 竞价量
                'amt': _f(f[37]) * 10000.0,         # 元; 9:25~9:30 = 竞价额
                'high': _f(f[33]),
                'low': _f(f[34]),
                'ts': f[30] if len(f) > 30 else '',
            }
        return got

    batches = [nums[i:i + 50] for i in range(0, len(nums), 50)]
    try:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max(1, min(workers, len(batches)))) as ex:
            for got in ex.map(_one_batch, batches):
                with lock:
                    out.update(got)
    except Exception:
        for b in batches:
            out.update(_one_batch(b))
    return out


def fetch_index_auction(secid='sh000001', date=None):
    """上证指数 9:25 竞价涨跌幅 (%) —— 大盘开关用

    ★ 必须用「今开 vs 昨收」, 不能用现价 vs 昨收:
      9:25 那一刻现价≈今开, 但 9:30 之后现价就变成盘中价了, 回放/延迟都会错。

    ★ 传 date 时必须走日K (2026-11-06 发现的问题):
      实时快照 `qt.gtimg.cn/q=sh000001` **永远只给最新交易日**。
      回放 11-03 时它也返回 11-05 的 -1.0743%, 让 11-02/11-03/11-04 三天
      全部误触发降半仓 (11-02 真实值是 +0.4248%, 本该正常满仓)。
      正确做法: 按 date 从指数日K算 (open vs 前一根 close)。
    """
    if date:
        try:
            kl = _fetch_index_kline(secid, count=12)
            for i, r in enumerate(kl):
                if r['date'] != date or i == 0:
                    continue
                pc = kl[i - 1]['close']
                if pc > 0 and r['open'] > 0:
                    return round((r['open'] - pc) / pc * 100, 4)
        except Exception:
            pass
        return None
    try:
        txt = _get('http://qt.gtimg.cn/q=' + secid, timeout=8, encoding='gbk')
        f = txt.split('="', 1)[1].rstrip('";\n').split('~')
        o, pv = _f(f[5]), _f(f[4])
        if o > 0 and pv > 0:
            return round((o - pv) / pv * 100, 4)
    except Exception:
        pass
    return None


def _fetch_index_kline(secid='sh000001', count=12, timeout=8):
    """指数日K → [{date, open, close}]  (fetch_day_kline 只认 6 位数字码, 指数走这个)"""
    url = ('https://web.ifzq.gtimg.cn/appstock/app/newfqkline/get?param=%s,day,,,%d,qfq'
           % (secid, int(count)))
    try:
        j = json.loads(_get(url, timeout=timeout))
    except Exception:
        return []
    d = (j.get('data') or {}).get(secid) or {}
    out = []
    for k in (d.get('qfqday') or d.get('day') or []):
        if len(k) < 3:
            continue
        out.append({'date': str(k[0])[:10], 'open': _f(k[1]), 'close': _f(k[2])})
    return out


# ============================================================
# 网络: 腾讯日K (拿昨日成交额 + 前5日均量)
# ============================================================
def fetch_auction_bar(code, date, timeout=10):
    """腾讯 day/query 取指定交易日的 09:30 竞价 bar → {open, vol, amt, src} | None

    返回的 bar 格式: "0930 5.16 154380 79660080.00" = 时间 / 价 / 量(手) / 额(元)

    ★ 为什么需要它 (2026-11-06 用户提出「为什么不能锁定竞价数据」):
      腾讯**快照**的 vol/amt 只有 9:25~9:30 那 5 分钟才是竞价数据;
      一旦过了 9:30 就变成「当天累计」, 拿它算量比/占比会放大几十倍
      (实测 600261 阳光照明: 09:27 占比 5.19% → 14:21 变成 82.42%)。
      而 day/query 的 09:30 bar 是**收盘后依然可读**的, 那根就是竞价定格结果:
      实测 600261 竞价额 79,660,080 元 → 占比 5.21%, 与早盘 09:27 的 5.19% 吻合。

    ★ 为什么用 day/query 而不是 minute/query:
      minute/query 严格限流, 连续 400+ 次直接 HTTP 501; day/query 压测 30/30 成功,
      且一次回**最近 5 个交易日**, 顺带把历史回放也解决了。
    """
    n = re.sub(r'\D', '', str(code or ''))
    if len(n) != 6:
        return None
    d = (date or '').replace('-', '')          # 20261106
    if len(d) != 8:
        return None
    url = 'https://web.ifzq.gtimg.cn/appstock/app/day/query?code=' + _tx_sym(n)
    try:
        raw = _get(url, timeout=timeout)
        obj = json.loads(raw)
        days = obj['data'][_tx_sym(n)]['data']
    except Exception:
        return None
    for item in days:
        if str(item.get('date') or '') != d:
            continue
        bars = item.get('data') or []
        if not bars:
            return None
        f = str(bars[0]).split()
        if len(f) < 4 or f[0] != '0930':
            return None
        try:
            return {'open': float(f[1]), 'vol': float(f[2]),
                    'amt': float(f[3]), 'src': 'day0930'}
        except (TypeError, ValueError):
            return None
    return None


def fetch_day_kline(num, count=8, timeout=8):
    """腾讯 newfqkline → [{date, open, close, high, low, vol(手), amt(元)}]

    ★ 为什么用 newfqkline 而不是 fqkline:
      fqkline 只回 6 个字段 (date/open/close/high/low/vol), **没有成交额**;
      newfqkline 回 10+ 个, 第 9 位就是成交额(万元)。
      实测 000506 11-04: 腾讯 103167.11 万 = 东财 1031671075.88 元 —— 完全一致。
    """
    n = re.sub(r'\D', '', str(num))
    if len(n) != 6:
        return []
    url = ('https://web.ifzq.gtimg.cn/appstock/app/newfqkline/get?param=%s,day,,,%d,qfq'
           % (_tx_sym(n), count))
    try:
        j = json.loads(_get(url, timeout=timeout))
    except Exception:
        return []
    d = (j.get('data') or {}).get(_tx_sym(n)) or {}
    kl = d.get('qfqday') or d.get('day') or []
    out = []
    for k in kl:
        if len(k) < 6:
            continue
        out.append({
            'date': k[0],
            'open': _f(k[1]),
            'close': _f(k[2]),
            'high': _f(k[3]),
            'low': _f(k[4]),
            'vol': _f(k[5]),                                  # 手
            'amt': _f(k[8]) * 10000.0 if len(k) > 8 else 0.0,  # 万元 → 元
        })
    return out


def _prev5_vol_before(kline_rows, n=5):
    """yday **之前**那 n 个交易日的均量 (手) —— 弱转强量比分母用

    ★ 与 _prev5_vol 的区别: _prev5_vol 取的是「含 yday 在内」的最近 n 根
      (那是「竞价量比」的分母, 竞价量 / 日均量); 这里要的是 yday 之前那 n 根,
      用来判断 **yday 自己那根 K 线放没放量**。
    """
    if not kline_rows or len(kline_rows) < n + 1:
        return 0.0
    seg = kline_rows[-(n + 1):-1]
    vols = [r['vol'] for r in seg if r.get('vol', 0) > 0]
    if len(vols) < n:
        return 0.0
    return sum(vols) / float(n)


def _prev5_vol(kline_rows):
    """前 5 个交易日的均量 (手) —— 量比分母用

    口径已用 11-05 招金黄金验证: 36795 / (489163/240) = 18.05, 与报告完全一致。
    """
    vols = [r['vol'] for r in kline_rows if r.get('vol', 0) > 0]
    if len(vols) < 5:
        return 0.0
    return sum(vols[-5:]) / 5.0


# ============================================================
# 本地 CSV (零网络) —— 自己实现, 不硬依赖 engine
# ============================================================
def kline_dir():
    if _engine is not None:
        try:
            return _engine.KLINE_DIR
        except Exception:
            pass
    if _mcap is not None:
        try:
            return _mcap.daily_dir()
        except Exception:
            pass
    base = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(os.path.dirname(base), 'kline_data', 'daily')


def read_csv_tail(fpath, n_lines=90):
    """读 K 线 CSV 末尾 N 行 → [{date, open, close, high, low, vol(手), cap_t, cap_f}]

    9 列格式: date,open,close,high,low,volume,总市值,流通市值,自由流通股本
    市值两列是**常量列, 只有最后一行有值**, 老行留空 —— 必须从后往前找有值的那个。
    ⚠ 文件可能带 '\\r' (2026-10-14 事故遗留) → 统一 strip 掉, 它黏在最后一列上会让
      市值解析失配。这是 mcap.py 已有的自愈口径, 这里对齐。

    ★ v2.263 性能改造 (2026-11-08): 原来是「f.readline() 跳过表头后读整个文件,
      再 rows[-n_lines:] 切片」。每个 CSV 约 1650 行, 3202 只主板票 = 528 万行
      全部解析完再扔掉 99% —— 实测这一步独占粗筛 6.9 秒里的 6.8 秒。
      改成从文件尾倒着读 8KB 块, 收够 n_lines 行就停:
        全读版 n_lines=90 → 7.47 秒   seek版 → 0.70 秒 (10.6 倍)
      200 只抽样逐行比对, 字段零误差。
      正确性要点: pos>0 时第一行可能是被截断的半行, 必须丢掉。
    """
    rows = []
    try:
        with open(fpath, 'rb') as f:
            f.seek(0, os.SEEK_END)
            pos = f.tell()
            buf = b''
            # 多收 1 行余量: 倒读可能切在半个 UTF-8 字符 / 半行上
            while pos > 0 and buf.count(b'\n') <= n_lines + 1:
                step = min(8192, pos)
                pos -= step
                f.seek(pos)
                buf = f.read(step) + buf
            lines = [ln for ln in buf.split(b'\n') if ln.strip()]
            if pos > 0 and lines:
                lines = lines[1:]          # 丢掉被截断的行首
    except Exception:
        return rows
    for raw in lines[-n_lines:]:
        line = raw.decode('utf-8', 'ignore').strip().replace('\r', '')
        if not line:
            continue
        p = line.split(',')
        if len(p) < 5:
            continue
        try:
            rows.append({
                'date': p[0],
                'open': float(p[1]),
                'close': float(p[2]),
                'high': float(p[3]),
                'low': float(p[4]),
                'vol': float(p[5]) if len(p) > 5 and p[5] else 0.0,
                'cap_t': _f(p[6]) if len(p) > 6 and p[6] else 0.0,
                'cap_f': _f(p[7]) if len(p) > 7 and p[7] else 0.0,
                # v2.274 (2026-11-11 用户 m09088): 第 9 列 自由流通股本 (万股) —— 实际换手率的分母。
                #   与市值两列同一套规矩: 常量列, 只有最后一行有值, 老行留空 ⇒ 必须从后往前找。
                'free': _f(p[8]) if len(p) > 8 and p[8] else 0.0,
            })
        except (ValueError, IndexError):
            continue
    return rows


def _cap_of(rows):
    """从尾部找最近一次有值的 (总市值, 流通市值) —— 亿元"""
    for r in reversed(rows):
        if r['cap_t'] or r['cap_f']:
            return r['cap_t'], r['cap_f']
    return 0.0, 0.0


def _free_of(rows):
    """从尾部找最近一次有值的 自由流通股本 (万股, v2.274)

    与 _cap_of 同一套规矩: 它是**股本常量列**, 只有最后一行有值、老行留空。
    返回 0.0 表示这只票还没有这一列(9 只停牌票/新建 CSV) —— 调用方要按"算不出来"处理,
    绝不能把 0 当换手率分母(会除零, 或者算出一个天文数字)。
    """
    for r in reversed(rows):
        v = r.get('free') or 0.0
        if v > 0:
            return v
    return 0.0


def turnover_pct(vol_shou, free_wan):
    """实际换手率(%) = 成交量 ÷ 自由流通股本

    vol_shou  成交量(手)
    free_wan  自由流通股本(万股)
    口径见文件顶部 TURN_SWITCH_MIN 注释。free<=0 或 vol<=0 → None(绝不返回 0 冒充结果)

    ★ 单位推导(别想当然, 这里差 100 倍就是天上地下):
        vol 手 × 100            = 成交量(股)
        free 万股 × 10000       = 自由流通股本(股)
        比例 = vol×100 / (free×10000) = vol / (free×100)
        百分数 = 比例 × 100     = vol / free          ← 就这么简单, 不要再除 100
    实测: 002973 侨银 vol=123511 手, free=16985.85 万股 → 123511/16985.85 = 7.27% ✔
    """
    if not free_wan or free_wan <= 0 or vol_shou is None or vol_shou <= 0:
        return None
    return vol_shou / free_wan


def seal_rate(rows, win=SEAL_WINDOW):
    """封板率(%) = 窗口内「收盘涨停」根数 ÷ 「最高价涨幅>9.5%」根数 × 100

    v2.274 (2026-11-11 用户 m09088)。用户原话:
      「最近500个交易日或者说最近500根K线(上市后总K线数不满则最多有多少根就用多少根)
        涨停封板总数占最高涨幅＞9.5%的比…注意这里是最高涨幅不是收盘涨幅,
        盘中触及9.5%收盘回落到1%也算一次分母。」
    ⇒ 分母: (high - 前收)/前收*100 > 9.5   (摸过就算, 含竞价一字/炸板)
       分子: (close - 前收)/前收*100 >= 9.8 (真封住了; 与 LIMITUP_PCT 同口径)

    返回 (rate, touched, locked): 分母为 0 时 rate=None(这只票窗口内从没摸过 9.5%,
    显示 "--" 比显示 0% 诚实)。
    """
    if not rows or len(rows) < 2:
        return None, 0, 0
    seg = rows[-win:] if len(rows) > win else rows
    touched = locked = 0
    for i in range(1, len(seg)):
        pc = seg[i - 1]['close']
        if pc <= 0:
            continue
        hi_pct = (seg[i]['high'] - pc) / pc * 100.0
        cl_pct = (seg[i]['close'] - pc) / pc * 100.0
        if hi_pct > SEAL_TOUCH_PCT:
            touched += 1
            if cl_pct >= SEAL_LOCK_PCT:
                locked += 1
    if touched <= 0:
        return None, 0, 0
    return locked * 100.0 / touched, touched, locked


# ---- 行业/概念映射 (表格「行业」「概念」「梯队」三列要用) -------------------
# 复用 engine 的 load_industry_concept_maps: 它已经处理了
#   ① 配置项覆盖 (local.industry_map / local.concept_map)
#   ② BOM
#   ③ 北交所 8 字符码 → 6 位裸码归一 (v2.207b)
#   ④ 概念屏蔽名单 (v2.58)
# 自己重写一遍只会漏掉这四条, 所以宁可依赖 engine —— 它缺失时才退回自读。
_IC_CACHE = {'at': 0.0, 'ind': None, 'con': None}
_IC_TTL = 300.0


def load_ind_con(force=False):
    """→ (ind_map, con_map)   {"000506": "贵金属", ...}  带 300 秒缓存"""
    now = time.time()
    c = _IC_CACHE
    if not force and c['ind'] is not None and (now - c['at']) < _IC_TTL:
        return c['ind'], c['con']

    ind, con = {}, {}
    if _engine is not None:
        try:
            ind, con = _engine.load_industry_concept_maps()
        except Exception:
            ind, con = {}, {}

    if not ind and not con:            # engine 不可用 → 自己读一遍 (简化版)
        import csv as _csv
        base = os.path.join(os.path.dirname(kline_dir()), '..', 'kline_data')
        for fname, target, col in (('industry_map.csv', ind, '行业'),
                                   ('concept_map.csv', con, '概念')):
            fpath = os.path.join(base, fname)
            try:
                with open(fpath, 'r', encoding='utf-8-sig') as f:
                    for row in _csv.DictReader(f):
                        code = (row.get('代码') or '').strip().upper()
                        val = (row.get(col) or '').strip()
                        bare = code[-6:] if (len(code) == 8 and code[:2]
                                             in ('SZ', 'SH', 'BZ', 'BJ')) else code
                        if bare and bare.isdigit():
                            target[bare] = val
            except Exception:
                pass

    c['ind'], c['con'], c['at'] = ind, con, now
    return ind, con


# ============================================================
# 策略判定
# ============================================================
def auct_volratio(auct_vol, prev5_vols):
    """量比 = 竞价量 / (前5日均量 / 240)"""
    if not prev5_vols or prev5_vols <= 0 or auct_vol <= 0:
        return None
    return auct_vol / (prev5_vols / AUCT_MINUTES)


def had_limit_up(rows, back=LIMITUP_DAYS):
    """20 个交易日内现过涨停 (收盘涨幅 ≥9.8%)。

    ★ rows 必须**已经去掉当日** —— 9:25 决策时今天没收盘, 用当日 K 线判涨停
      等于偷看答案 (F 盘原代码就犯了这个错, 2026-11-06 已修)。
    """
    if len(rows) < 2:
        return False
    for i in range(max(1, len(rows) - back), len(rows)):
        pc = rows[i - 1]['close']
        if pc <= 0:
            continue
        if (rows[i]['close'] - pc) / pc * 100 >= LIMITUP_PCT:
            return True
    return False


def evaluate_one(code, name, rows, quote, prev_amt=None, prev5_vol=None,
                 data_date=None):
    """对单只票套 v2.14 全部条件 → dict | None (None = 数据不足, 丢弃)

    rows      本地历史 K 线 (可含 data_date 当日, 内部会切掉)
    quote     腾讯快照 {open, prev, vol, amt, price, ts, ...}
    prev_amt  昨日全天成交额(元); None → 退回用本地 CSV 估算
    prev5_vol 前5日均量(手);  None → 退回用本地 CSV 算
    data_date 快照数据所属交易日 (YYYY-MM-DD), 默认 today

    ★ data_date 为什么不能直接用 today (2026-11-06 实测踩到的坑):
      凌晨 03:00 跑, 腾讯快照 f[30]=20261105161454 —— 数据是 **11-05 收盘**的。
      若按 today=11-06 切 hist, 「昨日」会取到 11-05 那根(收14.33),
      但 quote['prev'] 是 13.41(11-04 收盘) —— 两者不是同一天,
      昨日阳线判定、昨日成交额、市值折算全部错位。
      正确做法: 决策日 = 快照自己声明的日期。
    """
    if not rows or not quote:
        return None
    q_open = quote.get('open') or 0.0
    q_prev = quote.get('prev') or 0.0
    if q_open <= 0 or q_prev <= 0:
        return None

    day = data_date or _today()
    # 严格切分: hist = 数据日之前; today_bar = 数据日当天
    hist = [r for r in rows if r['date'] < day]
    today_bar = next((r for r in rows if r['date'] == day), None)
    if len(hist) < 21:
        return None

    yday = hist[-1]                       # 昨日那根
    # 交叉校验: 本地昨收 必须 ≈ 快照昨收, 否则说明数据不同步 → 丢弃更安全
    if yday['close'] > 0 and q_prev > 0:
        if abs(yday['close'] - q_prev) / q_prev > 0.02:
            return None

    # 竞价涨幅 = (今开 - 昨收) / 昨收
    auct_pct = (q_open - q_prev) / q_prev * 100

    # 量比
    if prev5_vol is None:
        prev5_vol = _prev5_vol(hist)
    vr = auct_volratio(quote.get('vol') or 0.0, prev5_vol)
    if vr is None:
        return None

    # 竞价额占比 = 竞价额 / 昨日全天成交额
    if prev_amt is None:
        prev_amt = (yday['vol'] * 100.0
                    * ((yday['high'] + yday['low'] + yday['close']) / 3.0))
    ratio = ((quote.get('amt') or 0.0) / prev_amt * 100.0) if prev_amt > 0 else None
    if ratio is None:
        return None

    # 流通市值 = 流通股本 × 昨日收盘价 (报告口径, 不是今日价)
    #
    # ★ 为什么用昨收: 核对原数据时发现原作者两处不一致 —— 同一只票同一天，
    #   daily.json 写 133 亿 (用**今开价**) / 复盘报告写 125.0 亿 (用**昨收价**)，差 7%。
    #   对"高开又刚好卡在 160 亿"的票会一个过、一个毙。本模块选昨收: 9:25 时确定已知，
    #   且不会因一票高开就让市值虚增 —— 更保守。同时给 cap_near 标记，155~165 亿区间的票
    #   在界面上会被拎出来让人复核，避免「口径不同 → 静默错杀」。
    cap_t_yi, cap_f_yi = _cap_of(rows)
    if cap_f_yi > 0:
        last_close = next((r['close'] for r in reversed(rows) if r['close'] > 0), 0.0)
        if last_close > 0:
            shares = cap_f_yi * 1e8 / last_close
            cap_f_yday = shares * q_prev / 1e8
        else:
            cap_f_yday = cap_f_yi
    else:
        cap_f_yday = 0.0

    # v2.274 (2026-11-11 用户 m09088): 实际换手率两口径
    #   昨日: 昨天那根的量 ÷ 自由流通股本 —— 9:27 选股时全天量早已定, 口径最干净
    #   今日: 快照里 vol(全天累计, 手) ÷ 自由流通股本 —— 盘后就是当日真实换手
    #   ⚠ 9:25~9:30 那段 quote['vol'] 只是竞价量, 算出来的"今日换手"极小 ——
    #     这正是用户要「10 点前显示昨日」的原因, 不是 bug。
    #   ⚠ v2.274b: 9:30 之后 scan() 会走「锁定竞价量」那一步, 把 quote['vol'] 覆盖成
    #     09:30 那根定格量(几十手), 所以那里先存了一份 quote['vol_day'] = 覆盖前的
    #     全天累计量。这里**优先用 vol_day**, 没有才退回 vol —— 不这么做的话,
    #     下午跑出来的"今日换手"会小到 0.1%, 完全失去"看有没有出货"的意义。
    _free = _free_of(rows)
    _vol_now = quote.get('vol_day')
    if _vol_now is None:
        _vol_now = quote.get('vol') or 0.0
    turnover_y = turnover_pct(yday['vol'], _free)
    turnover_t = turnover_pct(_vol_now, _free)

    # v2.392 (2026-11-26 用户 m06467): 弱转强口径 —— 前一日放量阴线
    #   量比 = 昨日成交量 / 昨日之前 5 个交易日均量 (与「竞价量比」口径分开)
    _pv5 = _prev5_vol_before(hist)

    return {
        'code': re.sub(r'\D', '', code),
        'name': name or quote.get('name') or '',
        'price': q_open,                        # 竞价定格价 = 今开
        # 现价 >20 元: v2.254 起不再淘汰, 界面标红提醒 (用户: ">20 的进行标红")
        'price_over20': bool(q_open and q_open > PRICE_SOFT),
        'open': q_open,
        'prev': q_prev,
        'auct_pct': round(auct_pct, 2),
        'vol_ratio': round(vr, 2),
        'auct_ratio': round(ratio, 2),
        'mkt_cap': round(cap_f_yday, 1),
        # 155~165 亿: 口径差异可能改变结论 → 界面标出来让人复核
        'cap_near': bool(cap_f_yday and 155.0 <= cap_f_yday < 165.0),
        'auct_vol': quote.get('vol') or 0.0,
        'auct_amt': quote.get('amt') or 0.0,
        'prev_amt': prev_amt,
        # v2.274 (2026-11-11 用户 m09088): 实际换手率两口径 + 封板率
        #   turnover_y = 昨日全天实际换手率(%), turnover_t = 今日实时(全天累计)实际换手率(%)
        #   前端按钟点选一个显示(10:00 前显示昨日的方便选股, 之后显示当日的看有没有出货),
        #   悬停浮窗两个都显示 —— 所以这里两个都算好带下去, 不在前端重复推导。
        #   封板率移到 scan() 末尾给进列表的票单独算(要 500 根 K 线, 不能给 3202 只都读)。
        'turnover_y': turnover_y,
        'turnover_t': turnover_t,
        'free_shares': _free,                   # 自由流通股本(万股), 便于自查
        'prev_bullish': yday['close'] >= yday['open'],      # 阳线含平盘
        # v2.392 (2026-11-26 用户 m06467): 弱转强 —— 前一日放量阴线 + 今日竞价红开
        #   prev_yin  = 昨日收 < 昨开 (阴线, 与 prev_bullish 互补, 平盘算阳)
        #   prev_vr5  = 昨日量 / 昨日之前 5 日均量 (倍数); 分母算不出给 None
        #   prev_date = 昨日那根 K 线的日期 (浮窗里给人看, 防止错位到别的日子)
        'prev_yin': yday['close'] < yday['open'],
        'prev_vr5': (round(yday['vol'] / _pv5, 2) if _pv5 > 0 else None),
        'prev_date': yday['date'],
        # v2.292 (2026-11-12 用户 m13718): 昨日那根 K 线的日期 —— scan() 要用它去
        #   腾讯 day/query 查「昨日 09:30 竞价额」，算「竞昨比」= 今竞价额 / 昨竞价额。
        #   不能自己拿 today-1 天算：周末/节假日会错位，而 hist[-1] 就是真正的上一根。
        'yday_date': yday['date'],
        'limitup_20': had_limit_up(hist),                   # 不含当日
        'yday_close': yday['close'],
        'yday_open': yday['open'],
    }


def classify(r):
    """v2.14 条件判定 → ('exec' | 'watch' | 'no', 原因)

    exec  = 满足全部硬条件 **且** 竞价涨幅 ∈ [4.5%, 6%] → 可作主推/备选/备选*
            (v2.295 用户 m14470: 原来 [3%,7%], 收窄到 [4.5%,6%])
    watch = 满足全部硬条件但竞价涨幅 1%~4.5% → 只观察, 不买
    no    = 硬条件不过 / 竞价涨幅 ≥7% / ≤0%

    ★ 三态语义是 2026-11-06 修 F 盘原代码 bug 时定的:
      原实现把 <3% 的票也返回 pass, 再取前两只当主推 → 空仓日会误推一只 +2.9% 的弱票。
    """
    miss = []
    # ① 主板 ② 非ST —— 已在粗筛排除
    # ③ 流通市值 < 160 亿
    if not r['mkt_cap'] or r['mkt_cap'] >= MKT_CAP_MAX:
        miss.append('流通市值%g亿≥%g' % (r['mkt_cap'], MKT_CAP_MAX))
    # ④ 现价 < 50 元  (v2.254 由 20 放宽; 20~50 元之间界面上标红提醒)
    if r['price'] >= PRICE_MAX:
        miss.append('现价%.2f≥%g' % (r['price'], PRICE_MAX))
    # ⑤ 昨日阳线 (收≥开, 含平盘)
    if not r['prev_bullish']:
        miss.append('昨收阴线')
    # ⑥ 20日内现涨停 (不含当日)
    if not r['limitup_20']:
        miss.append('20日内无涨停')
    # ⑦ 量比 >= 5 (v2.262: 边界与 hard_gate() 严格一致 —— 用户口径是"<5 排除",
    #    所以 =5 必须保留。原来写的是 <= 会把刚好 5.00 的票误标成淘汰)
    if r['vol_ratio'] < VOLRATIO_MIN:
        miss.append('量比%.2f<%g' % (r['vol_ratio'], VOLRATIO_MIN))
    # ⑧ 竞价涨幅 >= 1% (v2.262: 同上, "<1% 排除" ⇒ =1% 保留)
    if r['auct_pct'] < AUCT_PCT_MIN:
        miss.append('竞价%.2f%%<%g%%' % (r['auct_pct'], AUCT_PCT_MIN))
    # ⑨ 竞价额占比 >= 2% (v2.262: 同上, "<2% 排除" ⇒ =2% 保留)
    #    v2.296 (用户 m14608): **上限取消**。原来是 2%~15%, 15% 以上的判淘汰;
    #    现在只判下限。理由见常量 AUCT_RATIO_MAX 处的注释 —— 放量不是坏事,
    #    而且主推已改成「按竞总比降序」, 上限会把最该排前面的票直接删掉。
    if r['auct_ratio'] < AUCT_RATIO_MIN:
        miss.append('占比%.2f%%<%g%%' % (r['auct_ratio'], AUCT_RATIO_MIN))
    # ⑩ 昨日成交额 >= 5000万 (v2.262: 由 1 亿放宽)
    if r['prev_amt'] < YDAY_AMT_MIN:
        miss.append('昨成交%.2f亿<%g万' % (r['prev_amt'] / 1e8, YDAY_AMT_MIN / 1e4))
    # ⑪ 今开 ≠ 涨停价 (一字板买不进)
    _zt = zt_price(r['code'], r['prev'])
    if _zt and abs(r['price'] - _zt) < 0.005:
        miss.append('一字涨停买不进')
    # ⑫ 竞价占比 —— 由 ⑨ 覆盖, 这里不再重复

    if miss:
        return 'no', ' / '.join(miss)

    # 硬条件全过 → 看竞价涨幅落在哪一档
    if r['auct_pct'] < AUCT_PCT_MIN:
        # v2.262: 这道闸已由 hard_gate() 前置拦掉, 留作防御 (理论上到不了这里)
        return 'no', '竞价%.2f%%<%g%%' % (r['auct_pct'], AUCT_PCT_MIN)
    # v2.295 (用户 m14470): 淘汰线仍是 7%, 但主推上限已改成 6% ——
    #   两者不再是同一个常量, 所以这里必须用 AUCT_PCT_HI, 否则 6~7% 的票会被误判成 exec。
    if r['auct_pct'] >= AUCT_PCT_HI:
        return 'no', '竞价%.2f%%≥%g%%淘汰' % (r['auct_pct'], AUCT_PCT_HI)
    # v2.295c: **闭区间 [4.5%, 6%]** —— 两端都含。
    #   ★ 原来这里只判下界 `>= MAIN_LO` 就 return exec, 导致 6%~7% 的票
    #     (实测 600715 6.69% / 000678 6.61% / 001282 6.26%) 被误标成 exec,
    #     其中 600715 还因竞总比最高被排成「主推」。用户口径是 4.5%~6% 区间。
    #   6%~7% 属于「除这几类之外的」⇒ 观察。
    if AUCT_PCT_MAIN_LO <= r['auct_pct'] <= AUCT_PCT_MAIN_HI:
        return 'exec', '全条件达标'
    if r['auct_pct'] > AUCT_PCT_MAIN_HI:
        return 'watch', '竞价%.2f%%>%g%%(超区间, 只观察)' % (
            r['auct_pct'], AUCT_PCT_MAIN_HI)
    # v2.295: 走到这里 = 1% <= 涨幅 < 4.5% ⇒ 观察
    return 'watch', '竞价%.2f%%未达%g%%(只观察)' % (r['auct_pct'], AUCT_PCT_MAIN_LO)


def hard_gate(r):
    """v2.262 四道硬门槛 → (pass: bool, 原因: str)

    用户 m03902: "筛除竞价涨幅＜1%的不进入股票列表, 竞价占比＜2%, 量比＜5也排除掉,
                 昨日成交额低于5千万的也排除"
    ——「不进入股票列表」意味着**根本不出现在列表里**, 不是出现在列表里标「淘汰」。
    所以这道闸放在 screen() 之前, 被拦下的票连 sample 都进不去。

    与 classify() 的关系: classify 里 ⑦⑧⑨⑩ 四条判的是同一批指标, 阈值也已同步。
    这里再判一次不是重复: classify 只负责给「进列表的票」贴主推/观察/淘汰标签,
    而 hard_gate 决定「能不能进列表」。边界都取闭区间(>=)。
    """
    miss = []
    if (r.get('auct_pct') or 0) < HARD_PCT_MIN:
        miss.append('竞价%.2f%%<%g%%' % (r.get('auct_pct') or 0, HARD_PCT_MIN))
    if (r.get('auct_ratio') or 0) < HARD_RATIO_MIN:
        miss.append('占比%.2f%%<%g%%' % (r.get('auct_ratio') or 0, HARD_RATIO_MIN))
    if (r.get('vol_ratio') or 0) < HARD_VOLRATIO_MIN:
        miss.append('量比%.2f<%g' % (r.get('vol_ratio') or 0, HARD_VOLRATIO_MIN))
    if (r.get('prev_amt') or 0) < HARD_PREV_AMT_MIN:
        miss.append('昨额%.2f亿<%g亿' % ((r.get('prev_amt') or 0) / 1e8,
                                        HARD_PREV_AMT_MIN / 1e8))
    if miss:
        return False, ' / '.join(miss)
    return True, ''


def screen(rows, index_pct=None, ann_map=None, log=None):
    """把候选行组装成最终结果

    rows 每个元素 = evaluate_one 的返回 + 'verdict' + 'reason'
    ann_map 自 2026-11-06 取消公告核查后不再读取, 形参保留仅为不破坏调用方签名。
    """
    log = log or (lambda *a: None)
    sample = [r for r in rows if r.get('verdict') in ('exec', 'watch', 'no')]
    execs = [r for r in sample if r['verdict'] == 'exec']
    # v2.295 (2026-11-13 用户 m14470): 排序键由「竞价涨幅」改成「竞总比」——
    #   用户原话:「按竞总比排序，竞总比最大的为"主推"，竞总比第二"备选"，竞总比第三的"备选*"」
    #   取不到竞总比的票(理论上不会发生, hard_gate 已拦 ≥2%)排到最后, 不参与抢前三。
    execs.sort(key=lambda r: -(r.get('auct_ratio') or 0.0))

    half = (index_pct is not None) and (index_pct < INDEX_DROP_HALF)
    main_s = execs[0] if execs else None
    alt_s = execs[1] if len(execs) > 1 else None
    alt2_s = execs[2] if len(execs) > 2 else None   # v2.295: 「备选*」第三档

    buy = bool(main_s)
    note = ''
    if not execs:
        note = '候选池无票满足全部主推条件 · 今日空仓, 绝不硬塞'
    elif half:
        note = '上证竞价 %+.3f%% < %g%% → 降半仓: 只买主推 50%%, 备选空仓' % (
            index_pct, INDEX_DROP_HALF)
    elif not alt_s:
        # v2.250: 只有一只票达标时, 不能还说「主推50% + 备选50%」
        note = '上证竞价 %+.3f%% · 仅一只达标: 主推 50%%, 备选空仓' % (index_pct or 0.0)
    elif not alt2_s:
        # v2.295: 两只达标(主推+备选), 没有备选*
        note = '上证竞价 %+.3f%% · 正常仓位 (主推50%% + 备选50%% · 无备选*)' % (index_pct or 0.0)
    else:
        note = '上证竞价 %+.3f%% · 正常仓位 (主推50%% + 备选50%% · 备选* 仅记录)' % (index_pct or 0.0)

    return {
        'buy': buy,
        'main': main_s,
        'alt': None if half else alt_s,
        # v2.295: 「备选*」第三档 —— 降半仓时和 alt 一样不给(只买主推 50%)
        'alt2': None if half else alt2_s,
        'half': half,
        'index_pct': index_pct,
        'note': note,
        'n_exec': len(execs),
        'n_watch': sum(1 for r in sample if r['verdict'] == 'watch'),
        'n_no': sum(1 for r in sample if r['verdict'] == 'no'),
        'sample': sample,
    }


# ============================================================
# 主流程
# ============================================================
def log_writer(date_str=None):
    """返回一个 logger —— 同时写 logs/auction_YYYYMMDD.log 和 stderr

    支持运行中换日期: `_w.set_date('2026-11-05')` 会把后续行写到新文件。
    为什么需要 —— 决策日由快照时间戳决定, 而它要等第一批快照回来才知道
    (凌晨跑时快照是上一交易日的), 所以启动日志先写到临时日期, 拿到真日期后再切。
    """
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
    except Exception:
        pass

    def _path_for(ds):
        return os.path.join(LOG_DIR, 'auction_%s.log'
                            % ((ds or _today()).replace('-', '')))

    state = {'path': _path_for(date_str), 'lines': []}

    def _w(msg):
        line = '[%s] %s' % (_now().strftime('%H:%M:%S'), msg)
        try:
            with open(state['path'], 'a', encoding='utf-8') as f:
                f.write(line + '\n')
            state['lines'].append(line)
        except Exception:
            pass
        try:
            print(line, file=sys.stderr, flush=True)
        except Exception:
            pass

    def _set_date(ds):
        # 换日志文件时把本 writer 已经写进旧文件的那几行搬过去,
        # 否则一次运行会被劈成两个文件 (旧文件里只剩 [启动]/[警告])。
        if not ds:
            return
        new_path = _path_for(ds)
        if new_path == state['path']:
            return
        moved = list(state['lines'])
        if moved:
            try:
                old = ''
                if os.path.exists(state['path']):
                    with open(state['path'], 'r', encoding='utf-8') as f:
                        old = f.read()
                tail = '\n'.join(moved) + '\n'
                if old.endswith(tail):
                    rest = old[:-len(tail)]
                    if rest.strip():
                        with open(state['path'], 'w', encoding='utf-8') as f:
                            f.write(rest)
                    else:
                        try:
                            os.remove(state['path'])
                        except OSError:
                            with open(state['path'], 'w', encoding='utf-8') as f:
                                f.write('')
                with open(new_path, 'a', encoding='utf-8') as f:
                    f.write(tail)
            except Exception:
                pass
        state['path'] = new_path
        _w.path = state['path']

    _w.path = state['path']
    _w.set_date = _set_date
    return _w


def scan(progress_callback=None, log=None, with_ann=False, limit=0):
    """竞价打板 完整流程

    返回 dict:
      ok / mode(live|replay) / date / index_pct / half / buy
      main / alt / note / sample / n_sample / n_exec / n_watch / n_no
      elapsed / stages / log_path / ann
    """
    t0 = time.time()
    date_str = _today()
    log = log or log_writer(date_str)
    stages = {}

    def _p(i, n, msg=''):
        if progress_callback:
            try:
                progress_callback(i, n, msg)
            except Exception:
                pass

    live = in_auction_window()
    mode = 'live' if live else 'replay'      # 后面取到 09:30 bar 会升级成 'locked'
    log('=' * 66)
    log('[启动] 竞价打板 · 日期 %s · 数据模式=%s (%s)' % (
        date_str,
        '实盘竞价' if live else '历史回放/非竞价时点',
        _now().strftime('%H:%M:%S')))
    if not live:
        if past_auction():
            log('[警告] 已过 9:30 —— 快照成交量已是全天累计, 下面会改用 day/query '
                '的 09:30 bar 还原真实竞价量额 (v2.253)。')
        else:
            log('[警告] 不在 09:25~09:30 窗口内 (如盘中/盘后过早或节假日), '
                '尽量从 day/query 还原 09:30 竞价数据。')

    # ---- 第一步: 全主板快照 ---------------------------------------
    _p(0, 5, '拉取主板快照...')
    d = kline_dir()
    files = []
    try:
        files = [fn for fn in os.listdir(d) if fn.lower().endswith('.csv')]
    except OSError:
        log('[错误] K线目录不可用: %s' % d)

    nums = []
    for fn in files:
        c = fn[:-4].upper()
        if not (c.startswith('SH') or c.startswith('SZ')):
            continue                    # 北交所/其它一律不要 (v2.14 只做沪深主板)
        n = c[2:]
        if is_main_board(n):
            nums.append(n)
    t_snap = time.time()
    log('[快照] 本地K线目录 %s → 主板候选 %d 只' % (d, len(nums)))
    quotes = fetch_quotes(nums, workers=20)
    stages['snapshot'] = round(time.time() - t_snap, 2)
    log('[快照] 腾讯快照成功 %d / %d 只, 耗时 %.2f 秒' % (
        len(quotes), len(nums), stages['snapshot']))

    # 决策日 = 快照自己声明的行情日期 (不能取 today, 见 _data_date_from_ts)
    snap_date = _data_date_from_ts(
        next(iter(quotes.values())).get('ts')) if quotes else None
    if not snap_date:
        snap_date = _today()
        log('[快照] 快照无时间戳, 退回用系统日期 %s (有跨日风险)' % snap_date)
    else:
        log('[快照] 快照行情日期 = %s (系统日期 %s, %s)' % (
            snap_date, _today(),
            '一致' if snap_date == _today() else '⚠️ 不一致 — 按快照日期决策'))
    # 日志文件跟着决策日走
    if snap_date != date_str:
        date_str = snap_date
        if hasattr(log, 'set_date'):
            log.set_date(date_str)

    # 上证竞价 —— 必须用决策日, 不能只用实时值 (见 fetch_index_auction 注释)
    idx = fetch_index_auction(date=None if live else date_str)
    log('[快照] 上证 9:25 竞价 = %s' % (
        ('%+.4f%%' % idx) if idx is not None else '未取到'))
    _p(1, 5, '粗筛中...')

    # ---- 第二步: 本地 K 线粗筛 (零网络) ----------------------------
    t_coarse = time.time()
    cands = []
    drop = {'st': 0, 'no_quote': 0, 'open0': 0, 'short_k': 0, 'cap': 0, 'price': 0,
            'auct_le0': 0}
    for n in nums:
        q = quotes.get(n)
        if not q:
            drop['no_quote'] += 1
            continue
        if is_st(q.get('name')):
            drop['st'] += 1
            continue
        if (q.get('open') or 0) <= 0 or (q.get('prev') or 0) <= 0:
            drop['open0'] += 1          # 停牌 / 无报价
            continue
        # ★ v2.263 顺序优化: 只靠快照就能判的两道(价格 / 竞价涨幅)提到读 CSV 之前。
        #   原来 3202 只全都要先读 CSV 才算这两条, 现在 2265 只竞价≤0 和 47 只超价的票
        #   直接跳过磁盘 IO —— 粗筛从 6.9 秒降到 0.69 秒的主因就是这个(配合 seek 读)。
        #   判定结果与原来一字不差: 这两道本来就与本地 CSV 无关。
        if q['open'] >= PRICE_MAX:
            drop['price'] += 1
            continue
        q_prev = q['prev']
        if (q['open'] - q_prev) / q_prev * 100 < AUCT_PCT_MIN:
            # v2.262: 边界改成 < (原 <=) —— 用户口径是"<1% 排除", =1% 要保留,
            # 与 hard_gate() 的 HARD_PCT_MIN 判据严格一致。
            drop['auct_le0'] += 1
            continue
        pref = ('SH' if n.startswith(('6', '9')) else 'SZ') + n
        rows = read_csv_tail(os.path.join(d, pref + '.csv'), 90)
        if len(rows) < 21:
            drop['short_k'] += 1        # 次新 / 长期停牌
            continue
        cap_t, cap_f = _cap_of(rows)
        last_close = next((r['close'] for r in reversed(rows) if r['close'] > 0), 0.0)
        if cap_f > 0 and last_close > 0:
            cap_yday = (cap_f * 1e8 / last_close) * q_prev / 1e8
        else:
            cap_yday = cap_f
        if cap_yday <= 0 or cap_yday >= MKT_CAP_MAX:
            drop['cap'] += 1
            continue
        cands.append((n, q, rows, cap_yday))

    stages['coarse'] = round(time.time() - t_coarse, 2)
    log('[粗筛] %d → %d 只  (耗时 %.2f 秒)' % (len(nums), len(cands), stages['coarse']))
    # v2.263: 明细口径变了 —— 现在是「按新顺序」计数(价格/竞价涨幅在读CSV之前),
    # 所以「K线不足」「市值」的数字会比旧版小很多, 这是正常现象不是丢票:
    #   旧顺序 3202 只全读CSV → K线不足1 / 市值714 / 价格47 / 竞价≤02265
    #   新顺序 先砍快照两道   → 竞价≤02889 / 价格133 / K线不足0 / 市值5
    # 两者最终都是 3202 → 22 只, 结果一字不差。
    log('[粗筛] 剔除明细(按新顺序计, 先快照后CSV): 无快照%d / ST%d / 停牌%d / '
        '竞价<%g%%%d / 价格≥%g%d / K线不足%d / 市值≥%g亿%d'
        % (drop['no_quote'], drop['st'], drop['open0'],
           AUCT_PCT_MIN, drop['auct_le0'], PRICE_MAX, drop['price'],
           drop['short_k'], MKT_CAP_MAX, drop['cap']))
    # ---- 第二步半: 锁定竞价量/额 (v2.253 2026-11-06) --------------
    # 9:25~9:30: 快照的 vol/amt 就是竞价数据, 直接用。
    # 9:30 之后: 快照变成全天累计, 必须改用 day/query 的 09:30 bar —— 那根是定格值。
    # 这一步是用户提出「为什么不能锁定竞价阶段的数据」后的实现:
    #   锁定量/额(决定量比/占比), 现价/涨幅仍随快照实时刷新。
    auct_src = 'snapshot'
    if not live and cands:
        t_lock = time.time()
        _p(2, 5, '锁定 09:30 竞价数据...')

        def _one_auct(item):
            return item[0], fetch_auction_bar(item[0], date_str)

        got = 0
        try:
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=16) as ex:
                qmap = {it[0]: it[1] for it in cands}
                for n, ab in ex.map(_one_auct, cands):
                    if ab:
                        got += 1
                        q = qmap.get(n)
                        if q is not None:
                            # v2.274 (2026-11-11 用户 m09088): 先把手里的**全天累计量**存下来,
                            #   再覆盖成 09:30 竞价量。为什么必须存 —— 「当日实时换手」要的是
                            #   全天累计量 ÷ 自由流通股本(看有没有出货), 用竞价量算出来会小 100 倍。
                            #   实测 600228 返利科技: 竞价量 <100 手 ⇒ 换手 0.10%; 全天 224198 手
                            #   ⇒ 换手 0.77%, 与 CSV 对得上。不存这一手就全是错的。
                            q['vol_day'] = q.get('vol')
                            q['vol'] = ab['vol']
                            q['amt'] = ab['amt']
        except Exception:
            for it in cands:
                ab = fetch_auction_bar(it[0], date_str)
                if ab:
                    got += 1
                    it[1]['vol_day'] = it[1].get('vol')
                    it[1]['vol'] = ab['vol']
                    it[1]['amt'] = ab['amt']
        stages['lock'] = round(time.time() - t_lock, 2)
        if got:
            auct_src = 'day0930'
            log('[锁定] 从 day/query 取回 %d/%d 只的 09:30 竞价量额, 耗时 %.2f 秒 '
                '→ 量比/占比已还原为真实竞价值' % (got, len(cands), stages['lock']))
        else:
            log('[锁定] ⚠️ 一只都没取到 09:30 竞价数据 (该日可能不在腾讯最近5个交易日内), '
                '量比/占比仍是全天口径, 只作观察')

    _p(2, 5, '算昨日成交额...')

    # ---- 第三步：昨日成交额 + 前5日均量（v2.263 改本地算 + 边界复核）----
    # 原来逐只打腾讯日K：200 只 40.62 秒；改成从已读进内存的 rows 里算 —— 200 只 0.50 秒。
    # 【精度】本地估算 = vol(手) × 100 × (H+L+C)/3，与腾讯日K 交叉验证 120 只：相对误差中位 0.20% /
    #   90分位 0.83% / 最大 2.27%。另三种均价误差更大（ohlc 0.46/1.59/2.85、hl2 0.37/1.32/2.63、
    #   close 0.47/1.57/6.43）⇒ 选 (H+L+C)/3。前5日均量与腾讯完全一致（分母同源）。
    # 【翻转风险】5000 万门槛上 120 只抽样只有 1 只因估算偏差翻转 ⇒ 加边界复核：估算值落在门槛
    #   ±BOUNDARY_PCT 内才去腾讯拿准数（实测 0~3 只，<0.1 秒）。evaluate_one 本来就有 2% 昨收交叉校验。
    BOUNDARY_PCT = 5.0            # 估算值距门槛 ±5% 内 → 网络复核
    # ★ 占比门槛也要复核: auct_ratio = 竞价额 / 昨日成交额，分子准、分母是估算的，
    #   昨额那点误差会原样传导到占比上(实测 600261 5.19% → 5.05%)。v2.296 起上限已取消，
    #   只剩下限这一侧需要复核。
    RATIO_EDGE_PCT = 5.0
    t_amt = time.time()
    amt_map = {}
    n_local = 0
    near_edge = []
    for n, q, kl_rows, cap_yday in cands:
        hist = [r for r in kl_rows if r['date'] < date_str]
        if len(hist) < 2:
            amt_map[n] = (None, None)
            continue
        y = hist[-1]
        amt = y['vol'] * 100.0 * ((y['high'] + y['low'] + y['close']) / 3.0)
        p5 = _prev5_vol(hist) or None
        amt_map[n] = (amt or None, p5)
        if not amt:
            continue
        n_local += 1
        # ① 昨额自己卡在门槛上?
        edge = abs(amt - HARD_PREV_AMT_MIN) <= HARD_PREV_AMT_MIN * BOUNDARY_PCT / 100.0
        # ② 由它算出的竞价占比卡在**下限**上? (v2.296: 上限已取消)
        if not edge:
            a_amt = q.get('amt') or 0.0
            if a_amt > 0:
                est_ratio = a_amt / amt * 100.0
                # v2.296: 上限已取消(999%), 它不可能再是"卡在门槛上"的边界,
                #   所以这里只复核下限, 省掉一半无用分支。
                if abs(est_ratio - HARD_RATIO_MIN) <= HARD_RATIO_MIN * RATIO_EDGE_PCT / 100.0:
                    edge = True
        if edge:
            near_edge.append(n)
    # 边界票复核 (通常 0~3 只)
    n_fixed = 0
    if near_edge:
        def _refetch(code):
            kl = fetch_day_kline(code, count=8)
            if not kl:
                return code, None
            hh = [k for k in kl if k['date'] < date_str]
            if not hh:
                return code, None
            return code, (hh[-1].get('amt') or None)

        try:
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=8) as ex:
                for code, real_amt in ex.map(_refetch, near_edge):
                    if real_amt:
                        old = amt_map[code][0]
                        amt_map[code] = (real_amt, amt_map[code][1])
                        n_fixed += 1
                        if (old >= HARD_PREV_AMT_MIN) != (real_amt >= HARD_PREV_AMT_MIN):
                            log('[成交额] 边界复核纠正 %s: 估算 %.0f万 → 实际 %.0f万 (门槛 %g万)'
                                % (code, old / 1e4, real_amt / 1e4,
                                   HARD_PREV_AMT_MIN / 1e4))
        except Exception as _e:
            log('[成交额] 边界复核失败(忽略, 仍用估算): %s' % _e)
    stages['amount'] = round(time.time() - t_amt, 2)
    log('[成交额] v2.263 本地算出 %d/%d 只昨日成交额 (零网络); 边界复核 %d 只→联网校 %d 只; '
        '总耗时 %.2f 秒' % (n_local, len(cands), len(near_edge), n_fixed, stages['amount']))
    _p(3, 5, '逐只判定...')

    # ---- 第四步: 逐只判定 -----------------------------------------
    t_eval = time.time()
    rows = []
    # v2.262: 四道硬门槛 —— 被拦下的票「不进入股票列表」, 连 sample 都不进
    hdrop = {'pct': 0, 'ratio': 0, 'vr': 0, 'amt': 0}
    n_hard = 0
    for n, q, kl_rows, cap_yday in cands:
        amt, p5 = amt_map.get(n, (None, None))
        r = evaluate_one(n, q.get('name'), kl_rows, q, prev_amt=amt, prev5_vol=p5,
                         data_date=date_str)
        if not r:
            continue
        passed, why_hard = hard_gate(r)
        if not passed:
            n_hard += 1
            if (r.get('auct_pct') or 0) < HARD_PCT_MIN:
                hdrop['pct'] += 1
            if (r.get('auct_ratio') or 0) < HARD_RATIO_MIN:
                hdrop['ratio'] += 1
            if (r.get('vol_ratio') or 0) < HARD_VOLRATIO_MIN:
                hdrop['vr'] += 1
            if (r.get('prev_amt') or 0) < HARD_PREV_AMT_MIN:
                hdrop['amt'] += 1
            continue
        v, why = classify(r)
        r['verdict'] = v
        r['reason'] = why
        rows.append(r)
    stages['evaluate'] = round(time.time() - t_eval, 2)
    if n_hard:
        log('[硬门槛] v2.262 淘汰 %d 只不进入列表 (竞价<%.1f%% %d / 占比<%.1f%% %d / '
            '量比<%.1f %d / 昨额<%g万 %d; 可重复计数)'
            % (n_hard, HARD_PCT_MIN, hdrop['pct'], HARD_RATIO_MIN, hdrop['ratio'],
               HARD_VOLRATIO_MIN, hdrop['vr'], HARD_PREV_AMT_MIN / 1e4, hdrop['amt']))
    log('[判定] 有效样本 %d 只 (exec %d / watch %d / no %d)'
        % (len(rows),
           sum(1 for r in rows if r['verdict'] == 'exec'),
           sum(1 for r in rows if r['verdict'] == 'watch'),
           sum(1 for r in rows if r['verdict'] == 'no')))

    # ---- 第五步: 出结论 -------------------------------------------
    # v2.250 (2026-11-06): 原来的「公告核查」整段已删除 (用户决定取消)。
    res = screen(rows, index_pct=idx, log=log)
    # v2.274 (2026-11-11 用户 m09088): 封板率(最近500根K线) —— 只给**进列表的**票算。
    #   为什么不在粗筛里算: 粗筛要遍历 3202 只、每只只读 90 行(0.70 秒); 封板率要 500 行,
    #   若对全市场都读, 单这一步就要 3.5 秒以上 —— 而最终进列表的通常只有 20~40 只,
    #   对它们逐个补读一次几乎免费。实测 18 只 ≈ 0.05 秒。
    #   ★ 读的是同一个 CSV 文件, 但 read_csv_tail 的 seek 倒读是按行数来的, 所以直接再读一次
    #     500 行即可, 不需要把整份文件缓存起来。
    t_seal = time.time()
    n_seal = 0
    for r in (res.get('sample') or []):
        try:
            _code = r['code']
            _pref = ('SH' if _code.startswith(('6', '9')) else 'SZ') + _code
            _big = read_csv_tail(os.path.join(d, _pref + '.csv'), SEAL_WINDOW + 20)
            rate, touched, locked = seal_rate(_big, SEAL_WINDOW)
            r['seal_rate'] = round(rate, 2) if rate is not None else None
            r['seal_touched'] = touched
            r['seal_locked'] = locked
            if rate is not None:
                n_seal += 1
        except Exception:
            r['seal_rate'] = None
            r['seal_touched'] = 0
            r['seal_locked'] = 0
    stages['seal'] = round(time.time() - t_seal, 2)

    # ---- v2.292 (2026-11-12 用户 m13718): 竞昨比 = 今日竞价额 ÷ 昨日竞价额 ----
    #   用户原话：「今日竞价额与昨日竞价额的比，内容为 xN，今日竞价额如果是昨日的 150%，
    #             就写作 x1.5」
    #   为什么放在这里（和封板率同一处）：都是**只给进列表的票**算的补充指标。
    #   粗筛那 3202 只不能算 —— 每只多打一次腾讯接口，30 多秒就没了。
    #   进列表通常 20~40 只，16 线程并发 ≈ 0.5 秒。
    #   ★ 昨日竞价额只能从我方 day/query 拿，而它**只回最近 5 个交易日**
    #     （这就是为什么 11-05 及更早查不到、显示 "--"）。要更久得自己每天落盘存档。
    t_yy = time.time()
    n_yy = 0
    _rows_yy = [r for r in (res.get('sample') or []) if r.get('yday_date')]
    if _rows_yy:
        def _one_yy(item):
            return item['code'], fetch_auction_bar(item['code'], item['yday_date'])
        try:
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=16) as ex:
                _pairs = list(ex.map(_one_yy, _rows_yy))
        except Exception:
            _pairs = [(r['code'], fetch_auction_bar(r['code'], r.get('yday_date')))
                      for r in _rows_yy]
        _ymap = dict(_pairs)
        for r in _rows_yy:
            ab = _ymap.get(r['code'])
            _amt_now = r.get('auct_amt') or 0.0
            _amt_y = (ab or {}).get('amt') or 0.0
            if _amt_y > 0 and _amt_now > 0:
                r['auct_ratio_yy'] = round(_amt_now / _amt_y, 2)
                r['auct_amt_yday'] = round(_amt_y, 0)
                n_yy += 1
            else:
                # 拿不到就 None(前端显示 --)，绝不写 0 —— 0 会被误读成「今日竞价额是 0」
                r['auct_ratio_yy'] = None
                r['auct_amt_yday'] = None
    stages['auct_yy'] = round(time.time() - t_yy, 2)
    log('[竞昨比] v2.292 给 %d/%d 只算了「今竞价额 ÷ 昨竞价额」 (耗时 %.2f 秒;'
        ' 昨日竞价额来自腾讯 day/query, 该接口只保留最近5个交易日)'
        % (n_yy, len(res.get('sample') or []), stages['auct_yy']))
    log('[封板率] v2.274 给 %d/%d 只算了封板率 (最近 %d 根K线, 分母=最高涨幅>%.1f%%, '
        '分子=收盘涨停≥%.1f%%), 耗时 %.2f 秒'
        % (n_seal, len(res.get('sample') or []), SEAL_WINDOW,
           SEAL_TOUCH_PCT, SEAL_LOCK_PCT, stages['seal']))
    # 模式三态: live(9:25~9:30 快照) / locked(9:30 后用 09:30 bar 还原) / replay(取不到)
    if mode == 'replay' and auct_src == 'day0930':
        mode = 'locked'
    # >>> AUCTION REF BEGIN (v2.376) 一字板参考通道 —— 旁路, 不影响上面的选股口径
    _t_ref = time.time()
    try:
        _refs = scan_refs(quotes=quotes, date_str=date_str, log=log)
    except Exception as _re:
        _refs = []
        log('[参考] 一字板参考通道失败 (不影响主结果): %s' % _re)
    _ref_el = round(time.time() - _t_ref, 1)
    # <<< AUCTION REF END
    res.update({
        'ok': True, 'mode': mode, 'date': date_str, 'auct_src': auct_src,
        'elapsed': round(time.time() - t0, 1),
        'stages': stages, 'log_path': getattr(log, 'path', ''),
        'n_snapshot': len(quotes), 'n_main_board': len(nums),
        'n_sample': len(rows),
        'warn': ('' if mode != 'replay' else '数据非竞价口径'),
        'auction_window': live,
        # >>> AUCTION REF BEGIN (v2.376) 参考名单只跟着 res 走, 不进 sample
        'refs': _refs, 'n_refs': len(_refs), 'ref_elapsed': _ref_el,
        # <<< AUCTION REF END
    })
    log('[结果] 候选池 %d 只 · 主推 %s · 备选 %s' % (
        len(rows),
        ('%s %s %.2f%%' % (res['main']['code'], res['main']['name'],
                           res['main']['auct_pct'])) if res['main'] else '空仓',
        ('%s %s %.2f%%' % (res['alt']['code'], res['alt']['name'],
                           res['alt']['auct_pct'])) if res['alt'] else '空仓'))
    log('[结果] %s' % res['note'])
    log('[完成] 总耗时 %.1f 秒' % res['elapsed'])
    return res



# ============================================================
# 一字板参考通道 (v2.376 · 2026-11-23 用户 m00662 / m00721 / m00833)
#
# 用户原话 (m00662):「若某个板块内出现一字板开盘, 那么同板块内的其他票如果也出现在
#   竞价打板模式, 买入他的胜率可能更高 … 这个列表不能对现在的竞价打板模式的数据
#   造成前后条件不同导致的割裂的影响 … 单独分开显示并且不能写入每天的记录里面」
# 用户原话 (m00721):「在9:27分启动竞价打板模式选股的时候, 把除了ST股票的所有
#   开盘即涨停的列出来作为参考」
# 用户原话 (m00833):「这一条算你多虑了 … 因为A股规则是25分锁定开盘价格」
#
# ★ 为什么必须走旁路, 不能改主流程:
#   evaluate_one() 有 4 道闸会挡掉一字板 ——
#     · len(hist) < 21            → K线太短直接丢
#     · 量比 / 竞价占比 为 None    → 指标算不出来直接丢 (一字板竞价量极小)
#     · 流通市值 >= 160 亿         → 粗筛阶段就丢掉
#   hard_gate() 4 道硬门槛 (竞价涨幅≥1% / 竞占比≥2% / 量比≥5 / 昨额≥5000万) 再挡一遍
#   classify() 第 ⑪ 条还会把「一字涨停买不进」判成淘汰
#   改这些阈值 = 改变历史选股口径 → 回测前后不可比。所以这里一行都不碰主流程,
#   只额外算一份名单, 由 server 单独下发、前端单独显示, 既不进 results 也不进归档。
# ============================================================

def _board_of(code):
    """板别标签 (只用于界面提示, 不参与任何判定)"""
    c = re.sub(r'\D', '', str(code or ''))
    if c.startswith(MAIN_BOARD_PREFIX):
        return '主板'
    if c.startswith(BSE_PREFIX):
        return '北交所'
    if c.startswith(('688', '689')):
        return '科创板'
    if c.startswith(('300', '301', '302')):
        return '创业板'
    return ''


def _csv_name(code):
    """6 位裸码 → 本地 K 线文件名 (不含 .csv)

    北交所是 BZ 前缀 (实测 kline_data/daily 里 357 只全是 BZ920xxx.csv)。
    ★ scan() 里算封板率那段写死了 SH/SZ 两种前缀, 北交所会读不到文件 ——
      那条路径只服务主列表, 这里旁路自己按板别拼, 不动主流程。
    """
    c = re.sub(r'\D', '', str(code or ''))
    if c.startswith(BSE_PREFIX):
        return 'BZ' + c
    return ('SH' if c.startswith(('6', '9')) else 'SZ') + c


def scan_refs(quotes=None, date_str=None, log=None):
    """一字板参考名单 —— 开盘即涨停(非ST), 覆盖沪深主板+创业板+科创板+北交所

    判据 (用户 m00721 ② 明确要求): 用涨停价 zt_price() 判「今开 == 涨停价」,
    不用「竞价涨幅 > 9.9%」—— 双创是 20%、北证是 30%, 9.9% 那套在它们身上不成立。

    用户 m00833 ⑥ 已确认不必再判「是否炸板」:
      9:25 集合竞价已锁定开盘价, 25 分涨停的 27 分必然还是涨停 —— 本模式就是
      9:27 定时跑, 所以「今开 == 涨停价」等价于「开盘即涨停且当时仍封住」。

    返回 list[dict], 字段与 to_scan_rows() 同构 (verdict='ref', verdict_tx='参考'),
    按竞价额降序 (一字板里竞价额越大 = 封单越硬), 最多 REF_MAX 只。

    ★ 绝不返回进结果表的东西: 调用方(scan)只会把它塞进 res['refs'],
      server 只下发 / 不写 results / 不写归档。
    """
    t0 = time.time()
    date_str = date_str or _today()
    d = kline_dir()
    try:
        files = [fn for fn in os.listdir(d) if fn.lower().endswith('.csv')]
    except Exception:
        files = []

    codes = []
    for fn in files:
        c = fn[:-4].upper()
        if len(c) != 8 or c[:2] not in ('SH', 'SZ', 'BZ', 'BJ'):
            continue
        n = c[2:]
        if len(n) == 6 and n.isdigit():
            codes.append(n)
    if not codes:
        return []

    # 主流程已经拉过沪深主板那 3200 只的快照 → 只补差集 (双创 + 北证), 省 2/3 的等待
    q = dict(quotes or {})
    miss = [n for n in codes if n not in q]
    if miss:
        try:
            q.update(fetch_quotes(miss, workers=20))
        except Exception as _e:
            if log:
                log('[参考] 补拉快照失败(%s), 用已有 %d 只继续' % (_e, len(q)))

    hit = []
    for n in codes:
        qq = q.get(n)
        if not qq:
            continue
        o = qq.get('open') or 0.0
        prev = qq.get('prev') or 0.0
        if o <= 0 or prev <= 0:          # 停牌 / 无量 / 数据缺失
            continue
        nm = qq.get('name') or ''
        if is_st(nm):                    # 用户 m00721 ①: 特例股不考虑 ST
            continue
        zt = zt_price(n, prev)
        if zt is None or abs(o - zt) >= 0.005:
            continue
        hit.append((n, nm, qq, o, prev))

    if log:
        log('[参考] 一字板候选 %d 只 (全市场 %d 只扫过, 快照 %d 只, 耗时 %.1f 秒)'
            % (len(hit), len(codes), len(q), time.time() - t0))

    ind_map, con_map = load_ind_con()
    t_sw = _hm()
    out = []
    for n, nm, qq, o, prev in hit:
        try:
            rows = read_csv_tail(os.path.join(d, _csv_name(n) + '.csv'), 90)
        except Exception:
            rows = []
        hist = [r for r in rows if r['date'] < date_str]

        cap_t_yi, cap_f_yi = _cap_of(rows)
        cap_f_yday = 0.0
        if cap_f_yi > 0:
            last_close = next((r['close'] for r in reversed(rows) if r['close'] > 0), 0.0)
            if last_close > 0:
                cap_f_yday = (cap_f_yi * 1e8 / last_close) * prev / 1e8
            else:
                cap_f_yday = cap_f_yi

        prev5 = _prev5_vol(hist) if hist else 0.0
        vr = auct_volratio(qq.get('vol') or 0.0, prev5)

        prev_amt = 0.0
        if hist:
            yd = hist[-1]
            prev_amt = (yd['vol'] * 100.0
                        * ((yd['high'] + yd['low'] + yd['close']) / 3.0))
        amt = qq.get('amt') or 0.0
        ratio = (amt / prev_amt * 100.0) if prev_amt > 0 else None

        _free = _free_of(rows)
        turnover_y = turnover_pct(hist[-1]['vol'], _free) if hist else None
        turnover_t = turnover_pct(qq.get('vol') or 0.0, _free)

        srate, stouch, slock = seal_rate(rows, SEAL_WINDOW) if rows else (None, 0, 0)
        use_today = (t_sw >= TURN_SWITCH_MIN)

        px = qq.get('price') or o
        out.append({
            'code': n, 'name': nm,
            'price': o, 'open': o, 'prev': prev,
            'change_pct': round(px / prev * 100.0 - 100.0, 2),
            'auct_pct': round((o - prev) / prev * 100.0, 2),
            'auct_ratio': (round(ratio, 2) if ratio is not None else None),
            'auct_ratio_yy': None, 'auct_amt_yday': None,
            'auct_amt': amt, 'prev_amt': prev_amt,
            'prev_amt_yi': round(prev_amt / 1e8, 2),
            'vol_ratio': (round(vr, 2) if vr is not None else None),
            'mkt_cap': round(cap_f_yday, 1),
            'cap_near': bool(cap_f_yday and 155.0 <= cap_f_yday < 165.0),
            'seal_rate': (round(srate, 1) if srate is not None else None),
            'seal_touched': stouch, 'seal_locked': slock,
            'turnover': (turnover_t if use_today else turnover_y),
            'turnover_y': turnover_y, 'turnover_t': turnover_t,
            'turnover_src': ('today' if use_today else 'yday'),
            'free_shares': _free,
            'sector': ind_map.get(n, ''),
            'concept': con_map.get(n, ''),
            'zt_cnt': 0,
            'verdict': 'ref', 'verdict_tx': '参考',
            'reason': '开盘即涨停(一字板)',
            'is_valid': True,
            'mode': 'auction', 'mode_detail': '竞价打板 · 参考',
            'price_over20': bool(o and o > PRICE_SOFT),
            'board': _board_of(n),
            'limit_pct': limit_up_pct(n),
        })

    out.sort(key=lambda x: (-(x.get('auct_amt') or 0.0), x.get('code') or ''))
    if len(out) > REF_MAX:
        out = out[:REF_MAX]
    if log:
        log('[参考] 一字板参考名单 %d 只 (总耗时 %.1f 秒) —— 只做参考展示, 不进选股/不进归档'
            % (len(out), time.time() - t0))
    return out


def to_scan_rows(res):
    """把 scan() 结果转成终端表格行 (字段名对齐 resetTableHead 的 data-col)

    列: auct_pct 竞价涨幅 | code | name | change_pct 涨幅 | price 现价 |
        auct_ratio 竞价占比 | vol_ratio 量比 | seal_rate 封板率 |
        sector 行业 | zt_cnt 梯队 | concept 概念 | turnover 换手 | verdict 结论 | 操作

    ★ v2.274 (2026-11-11 用户 m09088) 三处列改动:
      ① 「流通市值」→「封板率」(mkt_cap 仍下发, 只是表格不再显示它)
      ② 「昨额」→「换手」(turnover 字段; 昨额 prev_amt_yi 仍下发备用)
      ③ 「结论」挪到换手之后 = recTh 的最后一列(操作列永远最后)
    """
    ind_map, con_map = load_ind_con()
    out = []
    # v2.274: 10:00 前显示昨日换手, 之后显示当日实时换手 (用户 m09088 原话)
    _tm = _hm()
    _use_today = (_tm >= TURN_SWITCH_MIN)
    for r in res.get('sample') or []:
        v = r.get('verdict') or 'no'
        code = r['code']
        _ty = r.get('turnover_y')
        _tt = r.get('turnover_t')
        _cur = _tt if _use_today else _ty
        out.append({
            'code': code,
            'name': r['name'],
            'price': r['price'],
            'change_pct': r['auct_pct'],
            'auct_pct': r['auct_pct'],
            'auct_ratio': r['auct_ratio'],
            # v2.292 (用户 m13718): 竞昨比 = 今日竞价额 ÷ 昨日竞价额, 显示成 xN
            'auct_ratio_yy': r.get('auct_ratio_yy'),
            'auct_amt_yday': r.get('auct_amt_yday'),
            # v2.292b: 今日竞价额也要下发 —— 前端浮窗要显示「今日竞价额 X 万 / 昨日 Y 万」,
            #   只给比率的话看不出绝对值 (x12 到底是 1044万/5.9万 还是 1.2亿/1000万, 差别很大)
            'auct_amt': r.get('auct_amt'),
            # v2.292d (2026-11-13 用户 m14012): 竞总比的浮窗也要写清公式和绝对值,
            #   所以昨日**全天**成交额(元)必须下发 —— 它是竞总比的分母。
            #   这个值是本地 CSV 算的 (走量*100*均价), 不受 5 交易日限制。
            'prev_amt': r.get('prev_amt'),
            'vol_ratio': r['vol_ratio'],
            'mkt_cap': r['mkt_cap'],
            'mkt_cap_f': r['mkt_cap'],
            # v2.274: 封板率(最近500根K线) —— 分母=最高涨幅>9.5%的根数, 分子=收盘涨停根数
            'seal_rate': r.get('seal_rate'),
            'seal_touched': r.get('seal_touched'),
            'seal_locked': r.get('seal_locked'),
            # v2.274: 换手 = 实际换手率(成交量 ÷ 自由流通股本)
            #   turnover   = 当前该显示哪个(后端已按 10:00 切换好, 前端直接用)
            #   turnover_y = 昨日全天 / turnover_t = 当日实时(全天累计)
            #   ★ 两个都下发, 因为悬停浮窗要同时显示它们(用户 m09088 明确要求)
            'turnover': (round(_cur, 2) if _cur is not None else None),
            'turnover_y': (round(_ty, 2) if _ty is not None else None),
            'turnover_t': (round(_tt, 2) if _tt is not None else None),
            'turnover_src': ('today' if _use_today else 'yday'),
            'free_shares': r.get('free_shares'),
            'sector': ind_map.get(code, ''),
            'concept': con_map.get(code, ''),
            'zt_cnt': 0,        # 前端 renderTable 会用 /api/sector_zt 的行业家数覆盖它
            'verdict': v,
            # v2.295 (2026-11-13 用户 m14470): 三档顺序 = 主推 / 备选 / 备选*
            #   ★ 判断必须按「先 main、再 alt、最后才是备选*」的次序,
            #     因为 res['main'] / res['alt'] / res['alt2'] 是三个不同对象。
            # ★ v2.297 BUGFIX: 补第四档。用户原话「除这几类之外的为观察」——
            #   exec 池里**第 4 名及以后**必须降级成「观察」。
            #   v2.295 用字典映射只有三个分支, else 落到 '备选*',
            #   实测 4.5~6% 区间有 5 只时, 第 4/5 名也被挂上了「备选*」(错)。
            'verdict_tx': (('主推' if r is res.get('main')
                            else '备选' if r is res.get('alt')
                            else '备选*' if r is res.get('alt2')
                            else '观察')
                           if v == 'exec' else
                           ('观察' if v == 'watch' else '淘汰')),
            'reason': r.get('reason', ''),
            # 昨日全天成交额(亿元) —— v2.274 表格不再显示它, 但硬条件⑩的依据留着
            'prev_amt_yi': (round((r.get('prev_amt') or 0) / 1e8, 2)
                            if r.get('prev_amt') else None),
            'is_valid': (v != 'no'),
            'mode': 'auction',
            'mode_detail': '竞价打板',
            'cap_near': r.get('cap_near'),
            'price_over20': r.get('price_over20'),
        })
    return out


# ============================================================
# 自测
# ============================================================
if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser(description='竞价打板 自测')
    ap.add_argument('--no-ann', action='store_true', help='(已废弃, 公告核查已取消)')
    ap.add_argument('--top', type=int, default=12, help='打印前 N 只')
    a = ap.parse_args()

    print('=' * 74)
    print('竞价打板 自测   %s' % _now().strftime('%Y-%m-%d %H:%M:%S'))
    print('时间闸: %s' % ('✅ 在 09:25~09:30 窗口内' if in_auction_window()
                          else '⚠️ 不在竞价窗口 — 量比/占比会偏大'))
    print('K线目录: %s' % kline_dir())
    print('=' * 74)

    r = scan()
    print()
    print('数据模式:', r['mode'], '| 耗时:', r['elapsed'], '秒')
    print('上证竞价:', r['index_pct'], '| 降半仓:', r['half'])
    print('候选池:', r['n_sample'], '(exec', r['n_exec'], '/ watch', r['n_watch'],
          '/ no', r['n_no'], ')')
    print('结论:', r['note'])
    if r['main']:
        m = r['main']
        print('\n★ 主推 %s %s  竞价%+.2f%%  量比%.2f  占比%.2f%%  市值%.1f亿'
              % (m['code'], m['name'], m['auct_pct'], m['vol_ratio'],
                 m['auct_ratio'], m['mkt_cap']))
    if r['alt']:
        m = r['alt']
        print('☆ 备选 %s %s  竞价%+.2f%%  量比%.2f  占比%.2f%%  市值%.1f亿'
              % (m['code'], m['name'], m['auct_pct'], m['vol_ratio'],
                 m['auct_ratio'], m['mkt_cap']))

    print('\n--- 前 %d 只 (按 exec/watch/no 排序) ---' % a.top)
    for x in (r.get('sample') or [])[:a.top]:
        print('  %-6s %-6s %-4s 竞价%+7.2f%% 量比%7.2f 占比%6.2f%% 市值%6.1f亿  %s'
              % (x['code'], (x['name'] or '')[:6], x['verdict'],
                 x['auct_pct'], x['vol_ratio'], x['auct_ratio'],
                 x['mkt_cap'], (x.get('reason') or '')[:34]))
    print('\n日志:', r['log_path'])
