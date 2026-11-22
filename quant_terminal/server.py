"""
BS King Quant Terminal v2.0 — HTTP服务器
=========================================
零外部依赖: 纯Python http.server + SSE实时推送
启动: python quant_terminal/server.py → http://127.0.0.1:8799
"""

import http.server, json, os, sys, io, time, threading, queue, socketserver, csv, re
import urllib.parse
import urllib.request
try:                                    # v2.234: 境内源统一请求层 (先直连, 失败回退代理)
    from quant_terminal import net as _net
except Exception:
    import net as _net
# v2.248 (2026-11-04): 只走 IPv4。net.py 里已装了一次(幂等), 这里再显式装一次 ——
#   若哪天 net.py 被裁掉/改坏, 这一行仍能挡住「每请求白等 8 秒」的故障。
try:
    from quant_terminal import netv4 as _netv4
    _netv4.install()
except Exception:
    pass
from typing import Dict
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR))

from quant_terminal.engine import (QuantEngine, TurtleExecutor, load_csv_kline,
                                   Indicators, MarketSentiment, analyze_sector_ma,
                                   analyze_concept_ma, load_industry_concept_maps,
                                   load_industry_parent,  # v5.0: 细分行业→上级 (表头悬停)
                                   index_hist_recent,     # v2.70: 顶栏指数悬停浮窗 (前5交易日)
                                   _req_get,
                                   LINES_MAP, _sector_ma_cache_clear, composite_sector_kline,
                                   _sector_ma_cache_get, trend_slot_rows,  # v2.89: 板块下钻三级取数
                                   sector_ma_cache_ts,  # v2.60: 缓存计算时刻(标题时间含日期)
                                   refresh_trend_data_file, _is_trade_window, refresh_dirs,
                                   PEAK_LABELS,  # v2.66: 涨停突破「峰期」列文字 (100→百日峰/55→55日峰/20→最近峰)
                                   ma_baseline_snapshot, ma_baseline_deltas,  # v2.61: 双槽基线读盘/升降徽标

                                   build_pool_conds, TREND_DATA_FILE, _load_trend_data,
                                   clear_sector_rt_cache,
                                   INDEX_DAILY_CODES, read_index_daily, ensure_index_daily,
                                   trade_days_behind,  # v2.128: K线滞后与四个消费者共用交易日判据
                                   sync_index_daily,
                                   _read_csv_tail, KLINE_DIR, DRILL_MA_COLS,  # v2.97: 下钻「均线」列
                                   # >>> M4 BEGIN
                                   _m4_for,               # v2.261: 竞价/超跌反弹/波段低吸补 A股适配
                                   # <<< M4 END
                                   _strip_blocked_concepts)  # v2.58b: 落盘行概念串清洗
from quant_terminal import turnover  # v2.62: 成交额分钟落盘 (节流触发拉取, 前端零改动)
from quant_terminal import macro_daily  # v2.63: RSRS/情绪 日度落盘 (悬停浮窗历史)
from quant_terminal.simulator import (SimAccount, simulate_history, simulate_sector_trend,
                                      ACCOUNT_LOCK, get_sim_history, clear_sim_history_cache)
from quant_terminal import datasource as ds
# v2.131 (2026-10-01): 市值两列读写层 (总市值/流通市值, 单位亿元) —— 快照导入/详情页写回 + /api/mcap 下发
from quant_terminal import mcap
# v2.279 (用户 m10425): 统一 'sh600519' / 'sz000001' / 'bj920157' 拼法。
#   全仓只有 mcap.prefixed 能正确判北交所 (920xxx / 4 / 8 开头), 它返回 '大写前缀+6位'。
#   老判据 `9 开头 = 沪市` 会把 920xxx 拼成 sh920157, 新浪/腾讯一律返回空串。
def _sc_of(code):
    """6位码 / 8位带前缀 → 新浪腾讯风格代码 (sh600519 / sz000001 / bj920157)"""
    try:
        _p = mcap.prefixed(mcap.bare(str(code or '').strip()))
    except Exception:
        return ''
    return ('bj' if _p[:2] == 'BZ' else _p[:2].lower()) + _p[2:]


# v2.100 (2026-09-16): 上级行业(90类)趋势全量窗口 — 长按「行业趋势」标题2秒打开, 只读参考。
#   ★ 自有落盘 parent_trend.json, 与选股数据源 trend_data.json 完全无关。
from quant_terminal import parent_trend as pt
# v2.112 (2026-09-25): 新闻 AI 影响分析 (纯 LLM 层, 只依赖 datasource, 不反向 import server)
from quant_terminal import ai_news
# v2.116 (2026-09-25): AI 对话窗口 (纯 LLM 层 + 工具循环)。工具执行由 _ai_tool_exec 提供 ——
#   反向注入, 所以 ai_chat 不需要认识行情, 也就不会与 server 循环导入。
from quant_terminal import ai_chat
# v2.150 (2026-10-18): 标题栏系统右键菜单拦截 (Chrome --app 窗口的标题栏是原生非客户区,
#   页面 contextmenu 拦不到) + 顶部「最小化」按钮的服务端实现。纯 ctypes, 无第三方依赖。
from quant_terminal import win_caption
# v2.152 (2026-10-18): 临时快照覆盖层 (通达信 11:31/15:31 自动任务) + 四时点调度器。
#   快照不写 CSV 时, 板块/选股靠 intraday 覆盖层拿到当日 OHLCV;
#   15:31 那轮才真正写 daily/*.csv (盘中写盘会毒化历史文件)。
from quant_terminal import intraday as _intraday
from quant_terminal import auto_task

# v2.277 (2026-11-11 用户 m09653): 行业/概念「板块」大窗口 —— 从 TSP(3011) 移植过来。
#   与 /api/sector_ma 的「均线广度」口径**完全不同** (那是"多少票站上N线", 这是"今天谁最强"),
#   所以是独立一层计算, 不碰 sector_ma 那条链路。数据全本地 + 现有快照, 不新增网络源。
from quant_terminal import board_rank as _brd
# v2.281 (2026-11-12 用户 m10425/m10906): 板块涨幅轮动矩阵 (RPS) + 它的 AI 分析。
#   矩阵全从本地 K 线现算 (不需攒历史); AI 分析复用 ai_news 的降级链。
from quant_terminal import board_rotation as _brt
from quant_terminal import board_rps_ai as _bra
from quant_terminal import ai_news as _an2
# v2.285 (2026-11-12 用户 m11544): 「市场总览」大窗口 —— 从 TSP 的「看板」移植。
#   board_overview = 大盘指标 / 涨跌分布 / 情绪雷达 / 趋势强度 / 涨停梯队 / 概念行业热度
#   board_monitor  = 监控中心 (设目标价, 盘中触及即报警)
#   两者数据全本地 + 现有快照, 不新增网络源。
from quant_terminal import board_overview as _bov
from quant_terminal import board_monitor as _bmo
# v2.302 (2026-11-13 用户 m16307): AI 大盘复盘 + 历史复盘存档 —— 从 TSP 的「复盘」模块移植。
from quant_terminal import market_recap as _mrc
from quant_terminal import regime_scan as _rg   # v2.311 市场环境
# v2.323 (2026-11-15 用户 m22464): 结果归档 —— 竞价打板全票全字段 + 五个窗口落盘
from quant_terminal import archive as _arc
# v2.340 (2026-11-18 用户 m25750): 版本机制 —— VERSION 文件是唯一真源, 本模块只读
from quant_terminal import version as _ver
# v2.353 (2026-11-20 用户 m36569): 「大盘云图」—— 全市场板块/个股的方块树图。
#   数据全复用 board_rank._facts()/_maps() 的现成缓存, 不新增网络源。
from quant_terminal import yuntu as _yt
from quant_terminal import idx_store as _ids
from quant_terminal import equity_bond as _eb   # v2.369 股债利差
from quant_terminal import etf_src as _etf      # v2.367: 5 只沪深300ETF + 头部占比指标
from quant_terminal import pe_ratio as _pr      # v2.370 市盈率走势图（复刻乐咕 hs300-ttm-lyr）
# v2.360 (2026-11-21 用户 m38439): 指数登记表 —— 全仓唯一一份。
#   原先 INDEX_CODES / INDEX_NAMES 各写死七码, 用户要求「顶栏两格可右键换指数」后
#   候选指数(同花顺全A/中证1000/中证全指/上证50)必须也能走指数通路, 否则点进去
#   会被当个股 ⇒ 拿 BZ/个股 前缀去查, 结果是别人的数据。
from quant_terminal import idx_registry as _idxr

PORT = 8799  # 新端口, 不冲突旧的8788

# P3-12: INDEX_CODES 模块级单一定义 (原两处handler内重复定义)
# v2.34c 补 sh000680 科创综指 — 前端指数栏7个指数, 缺它则点击走个股路径显示错误数据(1616根/收盘10.69)
# v2.360 (用户 m38512): 登记表全集 —— 含候选指数(同花顺全A/中证1000/中证全指/上证50)
#   与特色指标分母(中证红利)。它们都要走指数通路, 不能落到个股分支。
INDEX_CODES = set(_idxr.BAR_CODES)

# v2.117 (2026-09-25): 指数**名字**的唯一定义 —— get_market_indices() 与 AI 工具共用。
# 原先这份 dict 长在 get_market_indices() 函数体里, AI 侧拿不到 ⇒ 用户问「上证指数」
# 时工具只能回"没找到"。原则同 INDEX_CODES: 一处定义, 不许长出第二份。
INDEX_NAMES = {c: _idxr.name_of(c) for c in _idxr.BAR_CODES}
# 俗称/简写 → 标准指数代码 (模型说"上证"/"创业板"也要能命中)
INDEX_ALIAS = {
    '上证': 'sh000001', '上证指数': 'sh000001', '上证综指': 'sh000001', '沪指': 'sh000001',
    '深证': 'sz399001', '深证成指': 'sz399001', '深成指': 'sz399001',
    '创业板': 'sz399006', '创业板指': 'sz399006',
    '科创50': 'sh000688', '科创': 'sh000688',
    '沪深300': 'sh000300', '沪深': 'sh000300',
    # v2.360 (用户 m38439): 顶栏可换进来的候选 + 特色指标分母的俗称
    '同花顺全A': '883957', '全A': '883957', '同花顺全A(沪深京)': '883957',
    '中证1000': 'sh000852', '中证全指': 'sh000985', '上证50': 'sh000016',
    '中证红利': 'sh000922', '红利': 'sh000922',
    '中证500': 'sz399905', '中证': 'sz399905',
    '科创综指': 'sh000680',
}

# ============================================================
# 全局扫描状态
# ============================================================
SCAN_STATE = {
    'running': False,
    'progress': 0,
    'total': 0,
    'done': False,
    'error': None,
    'results': None,
    'scan_time': '',
    'market_status': '',
    'pool_size': 0,
    'scan_id': 0,   # v2.23: 扫描代数 — 新扫描替换旧扫描, 旧线程检测到换代后自我退出
}

# ============================================================
# v2.53：选股结果落盘 — 按(池×模式)分槽，单击看上次/双击重扫
# v2.107：键加**第三维「均线档位」** —— 每个 (池 × 模式 × 档位) 各有独立存档点。
#   切档位 = 换一份已落盘的结果来看，**绝不触发扫描**；数据保留到下次手动双击扫描覆盖它。
# scan_cache.json: {fmt:2, updated_at, px_updated_at, last:{pool,mode,lines}, slots:{pool:{mode:{lines:槽}}}}
#   槽 = {mode,pool,lines,scan_time,ts,total_results,results[]}；旧结构 slots{pool:{mode:槽}} 由 _migrate_scan_cache 无损升级
#   ⚠ SCAN_SCHEMA 是**行形状版本**，与本次「槽键结构」无关，一个字不动
# 板块热度 sector_hot.json: {ts, source, sectors[]} — 平时读盘，force=1 才现拉腾讯(防限流)
# ============================================================
SCAN_CACHE_FILE = os.path.join(BASE_DIR, 'quant_terminal', 'scan_cache.json')
SECTOR_HOT_FILE = os.path.join(BASE_DIR, 'quant_terminal', 'sector_hot.json')
# v2.147: /api/limit20 进程内缓存 {裸码: (ts, n20)}, TTL 5 分钟 (与 mcap 同)
_LIMIT20_CACHE: Dict[str, tuple] = {}


# ============================================================
# v2.170 三源轮换实时行情 —— 用户定案「停哪页刷哪页」
# ------------------------------------------------------------
# 同花顺 → 腾讯 → 新浪 轮流上，每 30 秒换一家(v2.232 起；原来 5 秒一刷 15 秒一换，同花顺一天被限流
#   100 多次，连累 15:35 的板块更新被 429 打挂)。
# 换源节奏放**服务端进程内**的 30 秒时间片，不靠前端计数 —— 刷页面/开多标签页/热榜补价共享同一条时间轴，
#   不会因为重开页面就从同花顺重新连打。选中的源失败 → 立刻顺延下一家(一轮最多 3 家)，游标照样前进。
# 持仓恒定带上: 模拟盘 positions 并入本次请求，单独放 holds 返回，前端只刷持仓面板数字、**不插进当前列表**。
#   失败/限流写 quote_cycle.log，事后能查是哪家、哪一刻限的。
# 口径: 三源统一输出 {前缀码: {price,pct,prev,open,high,low,vol(手)}}；新浪量=股→÷100，同花顺源内已 ÷100。
# ============================================================
_QC_SOURCES = ('fuyao', 'tencent', 'sina')
_QC_SLOT = 30.0                           # v2.232: 每一个源连续用 30 秒, 满 30 秒才换下一家
_QC_LOCK = threading.Lock()
_QC_STATE = {'idx': 0, 'fails': {'fuyao': 0, 'tencent': 0, 'sina': 0}, 'n': 0,
             'slot': -1, 'bad': {}}       # v2.232: slot=当前时间片, bad=本片内已失败过的源
QC_LOG = os.path.join(BASE_DIR, 'quant_terminal', 'quote_cycle.log')
QC_MAX_CODES = 1500                       # 单次请求上限, 再多就该分页轮转了


def _qc_log(level: str, src: str, detail: str):
    """轮换行情日志 —— 只在失败/限流时写, 免得每 3 秒一行把盘撑爆"""
    try:
        try:
            if os.path.getsize(QC_LOG) > 2 * 1024 * 1024:
                with open(QC_LOG, 'r', encoding='utf-8', errors='replace') as fh:
                    _tail = fh.readlines()[-2000:]
                with open(QC_LOG, 'w', encoding='utf-8') as fh:
                    fh.write(''.join(_tail))
        except OSError:
            pass
        with open(QC_LOG, 'a', encoding='utf-8') as fh:
            fh.write('%s | %-7s | %-7s | %s\n' % (
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'), level, src, detail))
    except Exception:
        pass


SWALLOW_LOG = os.path.join(BASE_DIR, 'quant_terminal', 'swallow.log')


def _swlog(where, e=None):
    """v2.212 (2026-10-24 DEBUG #6): 确实要吞的异常, 至少留一行痕迹

    原来这些地方是 `except: pass` —— 已经出错但日志里一个字都没有,
    将来「数据莫名不动了」根本没法查。规则:
      · 能降级继续跑的, 记一行带堆栈首行的日志, 然后照旧继续;
      · 绝不因为写日志失败再抛一次(写日志本身也包了 try)。
    """
    try:
        if not os.path.isdir(os.path.dirname(SWALLOW_LOG)):
            return
        if os.path.exists(SWALLOW_LOG) and os.path.getsize(SWALLOW_LOG) > 2 * 1024 * 1024:
            with open(SWALLOW_LOG, 'rb') as fh:
                fh.seek(-1 * 1024 * 1024, os.SEEK_END)
                tail = fh.read()
            with open(SWALLOW_LOG, 'wb') as fh:
                fh.write(tail)
        with open(SWALLOW_LOG, 'a', encoding='utf-8') as fh:
            fh.write('%s | %-22s | %s: %s\n' % (
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'), str(where)[:22],
                type(e).__name__ if e is not None else 'Error',
                str(e)[:200] if e is not None else ''))
    except Exception:
        pass


def _qc_pre(code) -> str:
    """各种写法的代码 → 本地前缀码 'SH600519'/'SZ000001'/'BZ920010' (非法给 '')"""
    c = str(code or '').strip().upper()
    if not c:
        return ''
    if c[:2] in ('SH', 'SZ', 'BJ', 'BZ') and len(c) == 8 and c[2:].isdigit():
        return ('SH' if c[:2] == 'SH' else ('SZ' if c[:2] == 'SZ' else 'BZ')) + c[2:]
    if len(c) == 6 and c.isdigit():
        # ⚠ 9 开头归沪、920 与 4/8 开头归北交所(920xxx 是深交所新号段的教训同源: 302 属创业板)
        if c[:3] == '920' or c[0] in ('4', '8'):
            return 'BZ' + c
        if c[0] in ('6', '9'):
            return 'SH' + c
        return 'SZ' + c
    return ''


def _qc_sym(pre: str) -> str:
    """前缀码 → 腾讯/新浪 symbol (小写; 北交所必须 bj, 发 bz 会回 pv_none_match 空值)"""
    p = str(pre)
    return ('bj' + p[2:]) if p[:2] == 'BZ' else p.lower()


def _qc_fetch_fuyao(pres):
    """同花顺: thscodes 批量, 一次 200 只 —— 免费、无累计调用限制"""
    from quant_terminal import fuyao as _fq
    if not _fq.configured():
        raise RuntimeError('未配置同花顺 Key')
    out = {}
    for i in range(0, len(pres), 200):
        part = pres[i:i + 200]
        got = _fq.snapshot(thscodes=[_fq.to_thscode(p) for p in part])
        for p in part:
            r = got.get(p[2:])
            if r and r.get('close', 0) > 0:
                out[p] = {'price': r['close'], 'prev': r['prev_close'], 'open': r['open'],
                          'high': r['high'], 'low': r['low'], 'vol': float(r['volume'])}
    return out


def _qc_fetch_tencent(pres):
    """腾讯: 50 只/批, 量=手; 顺手带市值(本接口不用, 但别扔)"""
    out = {}
    for i in range(0, len(pres), 50):
        part = pres[i:i + 50]
        url = 'http://qt.gtimg.cn/q=' + ','.join(_qc_sym(p) for p in part)
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        txt = _net.open_url(req, timeout=10).read().decode('gbk', 'ignore')
        for line in txt.split(';'):
            line = line.strip()
            if not line.startswith('v_') or '="' not in line:
                continue
            key = line[2:line.index('=')].upper()
            if key[:2] == 'BJ':
                key = 'BZ' + key[2:]
            try:
                f = line.split('="')[1].rstrip('"').split('~')
                if len(f) < 36 or not f[3]:
                    continue
                out[key] = {'price': float(f[3]), 'prev': float(f[4]), 'open': float(f[5]),
                            'vol': float(f[6]), 'high': float(f[33]), 'low': float(f[34])}
            except (IndexError, TypeError, ValueError):
                continue
    return out


def _qc_fetch_sina(pres):
    """新浪: 50 只/批, 量=**股** ⇒ ÷100 成本地口径"""
    out = {}
    for i in range(0, len(pres), 50):
        part = pres[i:i + 50]
        url = 'http://hq.sinajs.cn/list=' + ','.join(_qc_sym(p) for p in part)
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0', 'Referer': 'https://finance.sina.com.cn/'})
        # v2.239 (2026-10-29 用户 m13487): 新浪是境内源, 走本机 SSR 代理只会变慢/被掐
        #   (net.py 实测: 腾讯/同花顺走代理慢 4~11 倍且更易断) ⇒ 统一走 _net: 先直连, 失败才回退代理。
        with _net.open_url(req, timeout=10) as _r:
            txt = _r.read().decode('gbk', 'ignore')
        for line in txt.split(';'):
            line = line.strip()
            if not line.startswith('var hq_str_') or '="' not in line:
                continue
            key = line[len('var hq_str_'):line.index('=')].upper()
            if key[:2] == 'BJ':
                key = 'BZ' + key[2:]
            try:
                f = line.split('="')[1].rstrip('"').split(',')
                if len(f) < 10 or not f[3]:
                    continue
                out[key] = {'price': float(f[3]), 'prev': float(f[2]), 'open': float(f[1]),
                            'vol': float(f[8]) / 100.0,
                            'high': float(f[4]), 'low': float(f[5])}
            except (IndexError, TypeError, ValueError):
                continue
    return out


_QC_FETCHERS = {'fuyao': _qc_fetch_fuyao, 'tencent': _qc_fetch_tencent, 'sina': _qc_fetch_sina}


def _qc_hold_codes():
    """模拟盘持仓的前缀代码表 —— 让持仓恒被刷到(手机率再低也不漏)"""
    try:
        with ACCOUNT_LOCK:
            acc = SimAccount()
            poss = list(getattr(acc, 'positions', []) or [])
    except Exception:
        return []
    out = []
    for p in poss:
        if not isinstance(p, dict):
            continue
        c = _qc_pre(p.get('code') or '')
        if c and c not in out:
            out.append(c)
    return out


def quote_cycle(codes):
    """三源轮换取实时价 —— 返回 {ok, source, ts, quotes, holds, errors, fails}

    codes: 迭代器/逗号串, 前端当前列表的全部代码(前缀码或裸码都行)
    """
    if isinstance(codes, str):
        codes = [x for x in codes.split(',') if x.strip()]
    pres = []
    for c in (codes or []):
        p = _qc_pre(c)
        if p and p not in pres:
            pres.append(p)
    pres = pres[:QC_MAX_CODES]
    holds = [c for c in _qc_hold_codes() if c not in pres]
    ask = pres + holds

    # v2.232（用户 m09422）：从「来一个请求就换下一家」改成**按 30 秒时间片换源**。原来清单每 5 秒
    #   刷一次、同花顺每 15 秒被打一次，盘中经常被限流（quote_cycle.log 一天 105 次失败），还连累
    #   15:35 的自动任务被 429 打挂。现在时间片 = int(epoch/30)，30 秒内所有请求共用同一家。
    #   ★ 用时间片而非「请求计数」：多标签页、热榜补价这些旁路请求也走同一条时间轴，不会有人偷偷
    #     把游标推进，任何页面看到的「现在哪一家」都一致。
    #   ★ 同一片内某家失败过就记进 bad、后续跳过（免得 30 秒里连撞 6 次超时把整页刷价拖死），下一片清空重试。
    with _QC_LOCK:
        _slot = int(time.time() // _QC_SLOT)
        if _QC_STATE.get('slot') != _slot:
            _QC_STATE['slot'] = _slot
            _QC_STATE['bad'] = {}
        start = _slot % len(_QC_SOURCES)
        _QC_STATE['idx'] = start
        _QC_STATE['n'] += 1
        _bad = dict(_QC_STATE.get('bad') or {})

    order = [_QC_SOURCES[(start + i) % len(_QC_SOURCES)] for i in range(len(_QC_SOURCES))]
    order = [x for x in order if x not in _bad] or order
    res, used, errors = {}, '', {}
    if ask:
        for src in order:
            try:
                got = _QC_FETCHERS[src](ask)
            except Exception as e:
                errors[src] = str(e)[:80]
                with _QC_LOCK:
                    _QC_STATE['fails'][src] = _QC_STATE['fails'].get(src, 0) + 1
                    _QC_STATE['bad'][src] = 1        # v2.232: 本时间片内不再重试它
                _qc_log('失败', src, '%s (要 %d 只)' % (str(e)[:70], len(ask)))
                continue
            if got:
                res, used = got, src
                if len(got) < len(ask):
                    _qc_log('缺票', src, '%d/%d 只有价' % (len(got), len(ask)))
                break
            errors[src] = '空结果'
            with _QC_LOCK:
                _QC_STATE['bad'][src] = 1            # v2.232: 本时间片内不再重试它
            _qc_log('空结果', src, '要 %d 只, 一只没回' % len(ask))

    quotes, hq = {}, {}
    for p, v in res.items():
        prev = v.get('prev') or 0
        v['pct'] = round((v['price'] - prev) / prev * 100.0, 2) if prev > 0 else 0.0
        (hq if p in holds else quotes)[p] = v
    _left = int(_QC_SLOT - (time.time() % _QC_SLOT))
    return {'ok': bool(quotes or hq), 'source': used, 'ts': int(time.time() * 1000),
            'quotes': quotes, 'holds': hq, 'errors': errors,
            'fails': dict(_QC_STATE['fails']), 'n': _QC_STATE['n'],
            'slot_left': _left,          # v2.232: 还有几秒换下一家(前端只用于悬停提示)
            'slot_span': int(_QC_SLOT)}


SCAN_MODES = ('100', '55', '20', 'ai', 'smart', 'dragon', 'glue')
# >>> AUCTION BEGIN
# 竞价打板 (2026-11-06): 独立模块 auction_scan.py, 与本文件其余部分零耦合。
# 摘除: 运行 quant_terminal/_uninstall_auction.py  (按 >>> AUCTION 标记块精确切除)
_AUCT_OFF = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'auction_off.flag')
if not os.path.exists(_AUCT_OFF):
    SCAN_MODES = SCAN_MODES + ('auction',)
del _AUCT_OFF
# <<< AUCTION END
# >>> REBOUND BEGIN
# 超跌反弹 (2026-11-07): 独立模块 rebound_scan.py, 与 auction / 其余模式零耦合。
#   口径全部由用户逐条拍板, 阈值写在 rebound_scan.py 顶部常量里, 改前先问用户。
#   摘除: 运行 quant_terminal/_uninstall_rebound.py  (按 >>> REBOUND 标记块精确切除)
#   压舱: 建空文件 rebound_off.flag 即可让本模式从 SCAN_MODES 消失(前端按钮随之变灰)
_RBD_OFF = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'rebound_off.flag')
if not os.path.exists(_RBD_OFF):
    SCAN_MODES = SCAN_MODES + ('rebound',)
del _RBD_OFF
# <<< REBOUND END
# >>> WAVE BEGIN
# 波段低吸 (2026-11-07): 独立模块 wave_scan.py, 与 rebound / auction / 其余模式零耦合。
#   口径全部由用户逐条拍板 (2025 兆易创新 08-08 为正样本标定), 阈值写在 wave_scan.py
#   顶部常量里, 改前先问用户。
#   摘除: 运行 quant_terminal/_uninstall_wave.py  (按 >>> WAVE 标记块精确切除)
#   压舱: 建空文件 wave_off.flag 即可让本模式从 SCAN_MODES 消失(前端按钮随之变灰)
_WAV_OFF = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'wave_off.flag')
if not os.path.exists(_WAV_OFF):
    SCAN_MODES = SCAN_MODES + ('wave',)
del _WAV_OFF
# <<< WAVE END
# v2.107: 均线档位 = 落盘槽第三维。取值与 /api/scan/start 的校验同一套, 与 engine.LINES_MAP
#   的四档一一对应 (3=(5,10,21) / 4=(5,10,21,55) / 5=+120 / 6=+250)
SCAN_LINES = ('3', '4', '5', '6')
# v2.66 (2026-09-11): 落盘槽结构版本 — ai 由「资金跟随(五灯)」改造为「涨停突破」,
# 列结构从 ai_lights 换成 peak_period/left_peak。旧槽行套新表头会整列 '--' → 版本不匹配
# 一律当作"无历史结果"(前端提示双击首扫), 绝不静默展示错列。其余模式恒 1。
SCAN_SCHEMA = {'ai': 2, 'smart': 2}
# v2.106 (2026-09-21): 落盘槽有两个写方 —— 扫描完成 (_cache_scan_slot) 与
#   前端刷实时价后回写 (_patch_scan_slot)。两边都是「整份读-改-写」, 必须互斥。
SCAN_CACHE_LOCK = threading.RLock()


def _read_json_file(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


def _write_json_file(path, data):
    """原子覆盖写: 临时文件+rename (与 trend_data.json 落盘同款), 防半写"""
    try:
        tmp = path + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, path)
        return True
    except Exception:
        return False


def _norm_lines(lines):
    """档位归一化 — 非 3/4/5/6 一律折成 '4' (与 /api/scan/start 校验同一套判据)。
    '4' 是历史默认: v2.107 之前落盘的槽全部是 4 线档。"""
    return lines if lines in SCAN_LINES else '4'


def _migrate_scan_cache(d):
    """v2.107 (2026-09-23): 槽键 slots[池][模式] → slots[池][模式][档位], 就地升级 + 打 fmt 标记。
    老槽**自带 lines 字段** ⇒ 直接搬进它自己那一档, 无损、不丢一行结果。
    幂等: 判据是「该层键里有没有档位键」—— 已升级层 (含空层) 不会被重复包壳。"""
    if not isinstance(d, dict):
        return d
    slots = d.get('slots')
    if not isinstance(slots, dict):
        slots = {}
        d['slots'] = slots
    if d.get('fmt') == 2:
        return d
    for lv1 in slots.values():
        if not isinstance(lv1, dict):
            continue
        for mode, lv2 in list(lv1.items()):
            if not isinstance(lv2, dict) or not lv2:
                continue                                  # 空层/坏值: 保持原样 (= "无槽")
            if any(k in SCAN_LINES for k in lv2):
                continue                                  # 已是「档位→槽」层
            lv1[mode] = {_norm_lines(lv2.get('lines')): lv2}
    lt = d.get('last')
    if isinstance(lt, dict) and lt.get('pool') and lt.get('mode') and not lt.get('lines'):
        # 老 last 没记档位 → 取该 (池×模式) 下唯一那一档 (上面刚搬完, 至多一个)
        lv2 = (slots.get(lt['pool']) or {}).get(lt['mode'])
        ks = [k for k in lv2 if k in SCAN_LINES] if isinstance(lv2, dict) else []
        lt['lines'] = ks[0] if ks else '4'
    d['fmt'] = 2
    return d


def _read_scan_cache():
    """读落盘槽 — 就地升级旧结构 (返回的 dict 恒为 fmt2 形态; 之后任何写回都自然落成新结构)"""
    return _migrate_scan_cache(_read_json_file(SCAN_CACHE_FILE))


def _cache_scan_slot(mode, pool, lines, results):
    """扫描完成落盘: 覆盖同(池,模式,档位)槽, 记 last.
    行 = QuantHandler._summarize_result(None, r) — self 未用, 直接未绑定调用,
    与 /api/scan/result 返回完全同构 (attach 此时读 SCAN_STATE['lines']=本次档位, 无错位)"""
    try:
        ln = _norm_lines(lines)
        rows = [QuantHandler._summarize_result(None, r) for r in results[:500]]
        with SCAN_CACHE_LOCK:   # v2.106: 与 _patch_scan_slot 互斥 (两边都是整份读-改-写)
            d = _read_scan_cache() or {'fmt': 2, 'updated_at': '', 'last': None, 'slots': {}}
            d['fmt'] = 2
            d.setdefault('slots', {}).setdefault(pool, {}).setdefault(mode, {})[ln] = {
                'mode': mode, 'pool': pool, 'lines': ln,
                'schema': SCAN_SCHEMA.get(mode, 1),
                'scan_time': SCAN_STATE.get('scan_time') or datetime.now().strftime('%m-%d %H:%M:%S'),
                'ts': time.time(),
                'total_results': len(results),
                'results': rows,
                # >>> AUCTION BEGIN (v2.249 2026-11-06) 竞价打板结论也落盘
                'auction_info': (SCAN_STATE.get('auction_info') if mode == 'auction' else None),
                # <<< AUCTION END
                # >>> REBOUND BEGIN (v2.257 2026-11-07) 超跌反弹结论也落盘
                'rebound_info': (SCAN_STATE.get('rebound_info') if mode == 'rebound' else None),
                # <<< REBOUND END
                # >>> WAVE BEGIN (v2.258 2026-11-07) 波段低吸结论也落盘
                'wave_info': (SCAN_STATE.get('wave_info') if mode == 'wave' else None),
                # v2.271: 六口径各自的完整行数据 (切按钮时前端直接换数组, 不必重扫)
                'wave_combos': (SCAN_STATE.get('wave_combos') if mode == 'wave' else None),
                # <<< WAVE END
            }
            d['last'] = {'pool': pool, 'mode': mode, 'lines': ln}
            d['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            _write_json_file(SCAN_CACHE_FILE, d)
    except Exception as _e:
        _swlog('scan_cache_save', _e)  # 落盘失败不影响扫描主流程, 但留痕


def _patch_scan_slot(pool, mode, lines, prices):
    """v2.106 (2026-09-21): 前端刷到的实时价回写落盘槽。
    ★ 为什么需要: 落盘槽原先**只在扫描完成时写一次**, 而前端 refreshPrices() 只改内存数组
      → 「点一行刷实时价 → 切模式/切池/刷新页面 → 切回来」会把刚刷到的价丢回扫描那一刻的旧值,
      且不报任何错。用户 2026-09-21 报的就是这个。
    ★ 只改**价格类 8 字段**(price/change_pct/dist_pct/gap_atr + v2.227 加的 p5/p10/p21/pk_recent_d)的行, 结果本身一个字节不动。
    ★ 槽不存在 / 结构版本不符 → 返回 0 且**绝不新建槽** (免得把别处的价写进不该有的槽)。
    ★ scan_time / ts 原样不动 —— 槽里必须看得出"结果是几点扫的",
      另记 px_time 标明"价是几点刷的"。
    ★ v2.107: 定位键加第三维**档位** —— 前端只回写它**正在看的那一档**槽 (没切过去的一律不管);
      档位缺省 '4' (v2.107 前端之前的兼容口径, 也正好是当时的唯一档位)。"""
    if not isinstance(prices, dict) or not prices:
        return 0
    fields = ('price', 'change_pct', 'dist_pct', 'gap_atr', 'p5', 'p10', 'p21', 'pk_recent_d',
              'nz_pct', 'nz_ok',
              # v2.272 (2026-11-11 用户 m07846): 前端 pxDistRecalc 现在会跟着实时价重算这几列,
              #   回写时必须一并落盘, 否则「切模式再切回来」会退回扫描那一刻的旧值。
              #   超跌反弹: rebound=反弹% (随现价)  波段低吸: wave=离线% / pull=自A涨%
              'rebound', 'wave', 'pull',
              # v2.274 (2026-11-11 用户 m09088): 竞价打板的「换手」也随实时量走
              #   turnover_t=当日实时换手%; turnover=当前主列(10:00 后与 turnover_t 同值);
              #   seal_rate 是最近 500 根K线的历史比例, 盘中不会变 —— 不在这里, 故意不刷。
              'turnover', 'turnover_t', 'turnover_src')
    try:
        with SCAN_CACHE_LOCK:
            d = _read_scan_cache()
            if not d:
                return 0
            lv = ((d.get('slots') or {}).get(pool) or {}).get(mode)
            slot = lv.get(_norm_lines(lines)) if isinstance(lv, dict) else None
            if not slot:
                return 0
            if slot.get('schema', 1) != SCAN_SCHEMA.get(mode, 1):
                return 0
            rows = slot.get('results') or []
            hit = 0
            # v2.272: 波段低吸的六口径是**分开存的**(slot.wave_combos[键].rows),
            #   slot.results 只是默认口径那一组。前端切口径后会刷新**当前口径**的行,
            #   所以这里要把六组一起刷, 否则切一次口径回来价又变回扫描时的旧值。
            _buckets = [rows]
            _wc = slot.get('wave_combos')
            if mode == 'wave' and isinstance(_wc, dict):
                for _v in _wc.values():
                    if isinstance(_v, dict) and isinstance(_v.get('rows'), list):
                        _buckets.append(_v['rows'])
            for _rows in _buckets:
                for r in _rows:
                    p = prices.get(str(r.get('code', '')))
                    if not isinstance(p, dict):
                        continue
                    for f in fields:
                        if f in p:
                            r[f] = p[f]
                    hit += 1
            if not hit:
                return 0
            now = datetime.now().strftime('%m-%d %H:%M:%S')
            slot['px_time'] = now
            d['px_updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            _write_json_file(SCAN_CACHE_FILE, d)
            return hit
    except Exception:
        return 0


def _sanitize_rows_concept(rows):
    """v2.58b (2026-09-05): 落盘行可能是概念屏蔽名单生效前生成的 → 行概念串按名单重滤
    (只动 'concept' 字段; 名单外一字不差之外的概念原样保留; 无名单/字段缺失零开销)"""
    if not rows:
        return rows
    for r in rows:
        if isinstance(r, dict) and r.get('concept'):
            c = _strip_blocked_concepts(str(r['concept']))
            if c != r['concept']:
                r['concept'] = c
    return rows


def _slot_of(d, pool, mode, lines):
    """从**已读入**的 scan_cache 取槽 (含行形状版本校验 + 概念屏蔽名单重滤)。
    无槽 / 槽为空 / 版本不符 → None"""
    if not d:
        return None
    lv = ((d.get('slots') or {}).get(pool) or {}).get(mode)
    if not isinstance(lv, dict):
        return None
    slot = lv.get(_norm_lines(lines))
    if not isinstance(slot, dict) or not slot:
        return None
    # v2.66: 结构版本不符 = 改造前的旧列槽 (如 ai 的五灯行) → 视为无历史结果
    if slot.get('schema', 1) != SCAN_SCHEMA.get(mode, 1):
        return None
    slot['results'] = _sanitize_rows_concept(slot.get('results') or [])
    return slot


def _slot_key(pool, mode, lines, have_lines=''):
    """落盘槽定位键 (同时也是回给前端的 key)。
    have_lines = 该 (池×模式) 下**实际存有结果的**档位, '' = 从没扫过 —— 前端靠它区分
      "这一档没扫过" 和 "整个组合都没扫过", 空串时不做任何提示。**绝不猜成请求的档位**"""
    return {'pool': pool, 'mode': mode, 'lines': _norm_lines(lines),
            'have_lines': have_lines if have_lines in SCAN_LINES else ''}


def _have_lines(d, pool, mode):
    """该 (池×模式) 下实际有结果的档位; 多档都有 → 取 ts 最新那个 (即"上次扫的那档"); 无 → ''"""
    lv = ((d or {}).get('slots') or {}).get(pool) or {}
    lv = lv.get(mode) if isinstance(lv, dict) else None
    if not isinstance(lv, dict):
        return ''
    best, best_ts = '', -1.0
    for ln in SCAN_LINES:
        s = lv.get(ln)
        if isinstance(s, dict) and s:
            ts = s.get('ts') or 0
            if ts >= best_ts:
                best, best_ts = ln, ts
    return best


def _get_scan_slot(pool, mode, lines='4'):
    """取指定 (池 × 模式 × 档位) 槽; 找不到返回 None (前端提示双击首扫, **不静默回退别档**)"""
    return _slot_of(_read_scan_cache(), pool, mode, lines)


def _get_scan_slot_keyed(pool, mode, lines='4'):
    """取槽 + 定位键。key.have_lines 查的是该组合**真实**有结果的档位 (供前端提示, 不参与取槽)"""
    d = _read_scan_cache()
    return _slot_of(d, pool, mode, lines), _slot_key(pool, mode, lines, _have_lines(d, pool, mode))


def _get_last_scan_slot(lines='4'):
    """v2.53 (2026-09-05): 开终端/服务端重启后恢复上次结果。
    v2.107: 「池 + 模式」取自 last (前端无从得知, 只能服务端告诉它),
      **档位取自前端当前档位** (localStorage qt_lines) —— 这样下拉框 / 表头徽章 / 数据三者恒同档。
      该档位没扫过 → 返回 (None, key): key 里带着「上次实际扫的是哪一档」, 前端只能提示, 不自动切。"""
    d = _read_scan_cache()
    if not d:
        return None, None
    lt = d.get('last') or {}
    lp, lm = lt.get('pool'), lt.get('mode')
    if not lp or not lm:
        return None, None
    key = _slot_key(lp, lm, lines, _have_lines(d, lp, lm) or lt.get('lines'))
    return _slot_of(d, lp, lm, lines), key


class _ScanCancelled(Exception):
    """扫描被新请求替换 (scan_id 换代), 旧线程自我退出"""


def _check_scan_id(my_id: int):
    """代际检查: 当前扫描已被新请求替换 → 抛 _ScanCancelled 让旧线程退出"""
    if SCAN_STATE.get('scan_id') != my_id:
        raise _ScanCancelled()


# SSE 订阅者 (2026-08-16: 加锁防并发修改 — 原notify_sse迭代中删除+handler finally删除
#  会 RuntimeError: list changed size during iteration, 扫描中刷新页面即杀死整个扫描)
SSE_CLIENTS = []
SSE_LOCK = threading.Lock()

# 自选股文件锁 (2026-08-16: load→save 包锁防双开页面竞态)
WATCH_LOCK = threading.Lock()

# K线更新状态
UPDATE_STATE = {
    'running': False,
    'progress': '',
    'error': '',  # 健康检查拦截/失败信息 (2026-08-21: 透传 ❌ 拒绝写入类提示)
    'log_file': None,
    'started_at': '',
    'kind': '',      # v2.30 (2026-09-01): 'snapshot'(快照导入) | 'qt_snapshot'(腾讯批量快照) — 前端进度条按类型显示
    'percent': 0,    # v2.30: 批量快照百分比 0-100 (快照导入保持文本进度)
}
UPDATE_LOCK = threading.Lock()  # v2.30: 快照导入/批量快照 check-then-swap 竞态防护

# 模拟交易回测状态
SIM_BT_STATE = {
    'running': False,
    'progress': 0,
    'total': 0,
    'msg': '',
    'result': None,
    'error': None,
}

# 自选股 (持久化 watchlist.json)
WATCH_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'watchlist.json')

# ============================================================
# v2.54 (2026-09-05): 自选明细缓存 — watchlist_cache.json 按mode分槽, wl_mtime 自动失效
# 原每次点击全量重算 (~3s: 腾讯板块+549股采样+28票串行scan) → 单击读盘秒回
# 槽 = {'mode','wl_mtime','scan_time','ts','total_results','results':[summarize后行]}
# 自选增删 → watchlist.json mtime 变 → 槽失效自动重算; 手动刷新 → 前端 force=1
# ============================================================
WATCH_CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'watchlist_cache.json')


def _cache_watch_slot(mode, wl_mtime, rows):
    """自选明细落盘: 覆盖同mode槽, 记 wl_mtime (add/remove后自然失效)"""
    try:
        d = _read_json_file(WATCH_CACHE_FILE) or {'updated_at': '', 'slots': {}}
        d.setdefault('slots', {})[mode] = {
            'mode': mode,
            'wl_mtime': wl_mtime,
            'scan_time': datetime.now().strftime('%m-%d %H:%M:%S'),
            'ts': time.time(),
            'total_results': len(rows),
            'results': rows,
        }
        d['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        _write_json_file(WATCH_CACHE_FILE, d)
    except Exception as _e:
        _swlog('watch_cache_save', _e)  # 缓存落盘失败不影响主流程, 但留下痕迹


def _get_watch_slot(mode):
    d = _read_json_file(WATCH_CACHE_FILE)
    if not d:
        return None
    slot = (d.get('slots') or {}).get(mode)
    if slot:
        slot['results'] = _sanitize_rows_concept(slot.get('results') or [])
    return slot


def _safe_int(v, default: int = 0) -> int:
    """POST参数安全转int (2026-08-16: 原裸int()遇非数字抛ValueError→do_POST断连)"""
    try:
        return int(v) if v not in (None, '') else default
    except (TypeError, ValueError):
        return default


def _safe_float(v, default: float = 0.0) -> float:
    """POST参数安全转float"""
    try:
        return float(v) if v not in (None, '') else default
    except (TypeError, ValueError):
        return default


def load_watchlist() -> list:
    try:
        if os.path.exists(WATCH_FILE):
            with open(WATCH_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as _e:
        _swlog('watchlist_read', _e)     # 读坏了当空表, 至少让人知道
    return []


def save_watchlist(wl: list):
    try:
        tmp = WATCH_FILE + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(wl, f, ensure_ascii=False, indent=1)
        os.replace(tmp, WATCH_FILE)
    except Exception as _e:
        _swlog('watchlist_save', _e)


# 实时价缓存 (行业/概念趋势盘中刷新用, 60秒)
RT_CACHE = {'ts': 0, 'prices': {}}

# RSRS 缓存 (大盘四模式, 5分钟)
RSRS_CACHE = {'ts': 0, 'std': None, 'mode': '未知'}


def get_rsrs_cached(max_age: int = 300) -> Dict:
    """上证RSRS标准分+四模式 (本地指数日K, 5分钟缓存)
    2026-08-06: 非交易时段不重算 (收盘后RSRS不变), 缓存1小时
    v2.57 (2026-09-05): 改读本地 kline_data/index_daily (缺失自动回补); 盘中当日live合并,
    不再每5分钟联网拉800根腾讯K线"""
    now = time.time()
    if not is_trading_time() and max_age == 300:
        max_age = 3600  # 盘后: 1小时才可能重算
    if now - RSRS_CACHE['ts'] < max_age:
        return {'std': RSRS_CACHE['std'], 'mode': RSRS_CACHE['mode']}
    try:
        from quant_terminal.engine import get_index_bars, calc_rsrs, rsrs_to_mode
        rows = get_index_bars('sh000001', need=650, with_live=True)
        if len(rows) < 620:
            return {'std': None, 'mode': '未知'}
        kline = [{'high': r['high'], 'low': r['low']} for r in rows]
        closes = [r['close'] for r in rows]
        std = calc_rsrs(kline)
        # v2.10 (2026-08-12): RSRS+MA21双确认 — 恐慌需同时跌破MA21 (防趋势市假恐慌)
        mode = rsrs_to_mode(std, closes[-1], sum(closes[-21:]) / 21)
        RSRS_CACHE['ts'] = now
        RSRS_CACHE['std'] = std
        RSRS_CACHE['mode'] = mode
        return {'std': std, 'mode': mode}
    except Exception:
        return {'std': None, 'mode': '未知'}


# v2.63 (2026-09-10): 盘后把当日宏观行定稿
_FIN_TS = [0.0]


def _macro_finalize(rsrs=None):
    """盘后补算一次情绪并把当日行定稿 (成交额已结算 → 情绪分不再变)

    终端整晚开着时, 最后一次 /api/sentiment 停在 14:59, 而尾盘资金流与收盘值有差 —
    不补就是一条错的历史。挂 /api/status (盘后仍每 10 分钟轮询), 成功一次即置 final 永不再跑。
    300 秒节流: 万一行情源抽风, 也不会每次轮询都去重试。
    """
    try:
        if time.time() - _FIN_TS[0] < 300:
            return
        _FIN_TS[0] = time.time()
        s = MarketSentiment().fetch()
        macro_daily.record(rsrs=rsrs, senti=s, final=True)
    except Exception as _e:
        _swlog('macro_final', _e)


# 基本面缓存 (东财datacenter, 逐票拉取后缓存)
FUND_CACHE = {}


def fetch_fundamental(code: str) -> Dict:
    """个股基本面 (东财datacenter业绩报表): EPS/营收增速/净利增速/ROE"""
    num = code[2:] if code.startswith(('SH', 'SZ', 'sh', 'sz')) else code
    if num in FUND_CACHE: return FUND_CACHE[num]
    try:
        import requests
        url = ds.get('online', 'fundamental', {}).get('url', '').format(code=num)
        d = _req_get(url, timeout=8).json()
        rows = (d.get('result') or {}).get('data') or []
        # 防御: 接口默认排序可能非最新 (2026-08-11实测默认首条=最早公告), 按报告期倒序取最新
        rows = sorted(rows, key=lambda x: str(x.get('REPORTDATE') or ''), reverse=True)
        if not rows:
            FUND_CACHE[num] = {'error': '无财务数据'}
            return FUND_CACHE[num]
        r = rows[0]
        fund = {
            'report_date': (r.get('REPORT_DATE') or r.get('REPORTDATE') or r.get('END_DATE') or '')[:10],
            'eps': r.get('BASIC_EPS'),
            'revenue_yoy': round(r.get('YSTZ'), 1) if r.get('YSTZ') is not None else None,
            'profit_yoy': round(r.get('SJLTZ'), 1) if r.get('SJLTZ') is not None else None,
            'roe': round(r.get('WEIGHTAVG_ROE'), 1) if r.get('WEIGHTAVG_ROE') is not None else None,
        }
        # v2.13 总市值/流通市值 (腾讯qt: [44]=流通市值亿 [45]=总市值亿; 新浪精简格式已无市值 2026-08-16实测)
        try:
            # v2.131: 北交所 (920xxx/8xxxxx) 必须走 bj 前缀 —— 原来一律 sh/sz, 920xxx 会被当沪市查空
            num_lc = {'SH': 'sh', 'SZ': 'sz', 'BZ': 'bj'}[mcap.prefixed(num)[:2]] + num
            resp = _req_get(ds.get('online', 'realtime', {}).get('tencent_url', 'http://qt.gtimg.cn/q={codes}').format(codes=num_lc), timeout=8)
            resp.encoding = 'gbk'
            fs = resp.text.split('"')[1].split('~') if '"' in resp.text else []
            def _mc(v):
                try: return round(float(v), 1) if v else None
                except Exception: return None
            fund['market_cap_total'] = _mc(fs[45]) if len(fs) > 45 else None
            fund['market_cap_float'] = _mc(fs[44]) if len(fs) > 44 else None
            # v2.276 (2026-11-11 用户 m09562): 详情页要显示「流通股本」。
            #   流通股本(亿股) = 流通市值(亿元) ÷ 现价(元) —— 市值已经有了, 现价同一条腾讯行里就有 (fs[3])。
            #   取不到就走下面的本地 K 线兜底, 绝不写 0。
            fund['float_shares'] = None
            try:
                _px = float(fs[3]) if len(fs) > 3 and fs[3] else 0.0
                _fc = fund.get('market_cap_float')
                if _px > 0 and _fc:
                    fund['float_shares'] = round(_fc / _px, 4)
            except (TypeError, ValueError, IndexError):
                pass
        except Exception:
            fund['market_cap_total'] = fund['market_cap_float'] = None
            fund['float_shares'] = None
        # v2.276 兜底: 腾讯取不到市值/现价时, 用本地 K 线末行算 (流通市值第8列 ÷ 第3列收盘价)
        if fund.get('float_shares') is None:
            try:
                _fp = mcap.path_of(num)
                _t2, _f2 = mcap.read_two(_fp)
                if _f2:
                    with open(_fp, encoding='utf-8') as _fh:
                        _ls = _fh.read().splitlines()
                    _cl = float(_ls[-1].split(',')[2]) if len(_ls) > 1 else 0.0
                    if _cl > 0:
                        fund['float_shares'] = round(_f2 / _cl, 4)
            except (OSError, ValueError, IndexError, TypeError):
                pass
        # v2.131 (2026-10-01) 用户定案: 详情页拉到的市值写回本地K线, 每票每天只写一次 (日期没变就不写)
        #   强刷走「📥 快照导入」—— 那边 force=True 全市场覆盖, 不受这里的日限。
        try:
            mcap.write_daily_once(num, fund.get('market_cap_total'), fund.get('market_cap_float'))
        except Exception as _e:
            _swlog('mcap_write_daily', _e)
        FUND_CACHE[num] = fund
        return fund
    except Exception as e:
        FUND_CACHE[num] = {'error': str(e)[:50]}
        return FUND_CACHE[num]


# ---- 股东户数 (emweb F10, 本地 holder_num.csv 全市场存档, 每票最近10期) ----
HOLDER_DATA = None  # {code: [rows...]} 惰性加载, 季度低频数据无需过期处理
HOLDER_LOAD_AT = 0.0
# v2.25: 增量更新 — 财报密集期每天有新数据, 前端"更新"按钮 → 子进程 --incremental
HOLDER_UPDATE_STATE = {'running': False, 'msg': '', 'log_file': ''}
HOLDER_UPDATE_LOCK = threading.Lock()


def _fnum(v):
    try:
        return int(float(v))
    except Exception:
        return None


def load_holder_data(force: bool = False) -> Dict:
    """全量读入 holder_num.csv (约5万行, 内存分组), 10分钟缓存"""
    global HOLDER_DATA, HOLDER_LOAD_AT
    now = time.time()
    if HOLDER_DATA is None or force or now - HOLDER_LOAD_AT > 600:
        data = {}
        p = os.path.join(str(BASE_DIR), 'kline_data', 'holder_num.csv')
        try:
            with open(p, encoding='utf-8') as f:
                rd = csv.reader(f)
                next(rd, None)
                for r in rd:
                    if len(r) < 7 or not r[0].strip():
                        continue
                    row = {'report_date': r[1].strip(),
                           'holder_num': _fnum(r[2]),
                           'total_ratio': r[3].strip(),
                           'avg_free_shares': _fnum(r[4]),
                           'hold_focus': r[5].strip(),
                           'price': _fnum(r[6])}
                    data.setdefault(r[0].strip(), []).append(row)
            for v in data.values():
                v.sort(key=lambda x: x['report_date'])
        except FileNotFoundError:
            data = {}
        HOLDER_DATA, HOLDER_LOAD_AT = data, now
    return HOLDER_DATA


def _holder_update_progress() -> Dict:
    """实时解析子进程日志: fetch_holder_num 输出 '进度 {i}/{total} 成功{ok} 失败{fail} 新增行{added}'"""
    st = HOLDER_UPDATE_STATE
    out = {'running': st['running'], 'progress': 0, 'total': 0, 'ok': 0, 'fail': 0,
           'added': 0, 'msg': st.get('msg', '')}
    lp = st.get('log_file', '')
    if lp and os.path.exists(lp):
        try:
            with open(lp, 'r', encoding='utf-8', errors='replace') as f:
                lines = f.readlines()
            for l in reversed(lines):
                m = re.search(r'进度 (\d+)/(\d+).*成功(\d+).*失败(\d+).*新增行(\d+)', l)
                if m:
                    out.update({'progress': int(m.group(1)), 'total': int(m.group(2)),
                                'ok': int(m.group(3)), 'fail': int(m.group(4)),
                                'added': int(m.group(5))})
                    break
            for l in reversed(lines):
                if l.strip().startswith('完成'):
                    out['msg'] = l.strip()
                    break
        except Exception as _e:
            _swlog('holder_progress_parse', _e)
    return out


def fetch_holder_num(code: str) -> Dict:
    """股东户数详情: 全部期数 + 连续集中/分散期数 (季度为单位)"""
    num = code[2:] if code.startswith(('SH', 'SZ', 'sh', 'sz')) else code
    data = load_holder_data()
    rows = data.get(num)
    if rows is None:  # CSV 代码带 SH/SZ 前缀, 兜底后6位
        for k, v in data.items():
            if k[-6:] == num:
                rows = v
                break
    if not rows:
        return {'error': '暂无股东户数数据 (待全市场存档)'}
    out = [dict(r) for r in rows]
    # 连续趋势: 从最新一期向前数连续同向 (ratio正=户数增/分散, 负=户数减/集中)
    def sgn(v):
        try:
            v = float(v)
            return -1 if v < -0.5 else (1 if v > 0.5 else 0)
        except Exception:
            return 0
    streak, dirn = 0, 0
    for i in range(len(out) - 1, 0, -1):
        s = sgn(out[i]['total_ratio'])
        if i == len(out) - 1:
            dirn, streak = s, 1 if s else 0
        elif s == dirn and s:
            streak += 1
        else:
            break
    latest = out[-1]
    return {'code': num, 'rows': out, 'trend': {
        'dir': {1: 'up', -1: 'down', 0: 'flat'}[dirn],
        'label': {1: '连续分散', -1: '连续集中', 0: '持平'}[dirn],
        'streak': streak,
        'latest_ratio': latest['total_ratio'],
        'latest_focus': latest['hold_focus'],
        'report_date': latest['report_date'],
    }}


def get_realtime_prices_cached(max_age: int = 60) -> Dict:
    """全市场成分股实时价 (新浪批量), 60秒缓存 — 盘中趋势模块用
    2026-08-16: 并发去重 — 原ts在全部爬完后才写入, 并发请求(板块面板同时loadSectorMA+ConceptMA)各自起整轮爬取
    → 线程无上限膨胀+页面数分钟不出数据. 现先占位ts标记"爬取中", 后来请求共享同一轮结果"""
    now = time.time()
    if now - RT_CACHE['ts'] < max_age and RT_CACHE['prices']:
        return RT_CACHE['prices']
    if not is_trading_time():
        return None  # 非交易时段用收盘数据
    if RT_CACHE['ts'] > now:
        # 已有其他线程在爬 (占位ts在未来): 等待共享结果, 最多15秒
        deadline = time.time() + 15
        while time.time() < deadline:
            if RT_CACHE['prices']:
                return RT_CACHE['prices']
            time.sleep(0.3)
        return None  # 当前轮超时, 前端走收盘口径兜底
    RT_CACHE['ts'] = now + max_age  # 占位: 标记爬取中
    prices = {}
    try:
        ind_map, con_map = load_industry_concept_maps()
        codes = set(ind_map.keys()) | set(con_map.keys())
        sina_codes = [_sc_of(c) for c in codes if c.isdigit()]
        for i in range(0, len(sina_codes), 50):
            batch = sina_codes[i:i + 50]
            q = fetch_sina(batch)
            for sc, d in q.items():
                px = d.get('price', 0)
                if px > 0:
                    prices[sc[2:]] = px
        RT_CACHE['ts'] = now
        RT_CACHE['prices'] = prices
    except Exception:
        RT_CACHE['ts'] = 0  # 失败复位, 下次请求重新爬
        return None
    return prices



def notify_sse(event_type: str, data: dict):
    """向所有SSE客户端推送事件 (2026-08-16: 迭代副本+加锁, 防list changed size during iteration)"""
    msg = f"event: {event_type}\ndata: {json.dumps(data, ensure_ascii=False, default=str)}\n\n"
    dead = []
    with SSE_LOCK:
        for q in list(SSE_CLIENTS):
            try:
                q.put(msg)
            except Exception:
                dead.append(q)
        for q in dead:
            try:
                SSE_CLIENTS.remove(q)
            except ValueError:
                pass


# 股票名称索引 (2026-08-06, 简称搜索用; 从快照文件读, 文件变化时刷新)
_STOCK_NAMES_CACHE = {'ts': 0, 'mtime': 0, 'data': []}


def _load_stock_names() -> list:
    """加载 代码→名称 索引 (同花顺标的表; 缓存1小时)"""
    now = time.time()
    if now - _STOCK_NAMES_CACHE['ts'] < 3600 and _STOCK_NAMES_CACHE['data']:
        return _STOCK_NAMES_CACHE['data']
    data = []
    seen = set()
    try:
        # v2.177 (2026-10-22): 名称表改由同花顺标的表提供 (用户 m02201 ③ 定案)。
        #   原来读 5 个导出快照 xlsx —— 那批文件本月下线, 表会直接变空。
        #   同花顺标的表由 sector_kline.tickers() 落盘缓存 7 天, 每天刷一次 ⇒ 新股/改名最多延后 1 天。
        from quant_terminal import sector_kline as _sk
        tk = _sk.tickers() or {}
        for num, name in tk.items():
            num = str(num)
            if len(num) == 6 and num.isdigit() and num not in seen and name:
                seen.add(num)
                data.append((num, str(name)))
    except Exception as _e:
        print('[names] 同花顺标的表不可用: %s' % _e, file=sys.stderr, flush=True)
    data.sort()
    _STOCK_NAMES_CACHE.update({'ts': now, 'mtime': now, 'data': data})

    return data


_PREWARM_FLAG = False  # v2.61: "更新板块"双请求(sector+concept force)并发去重 — 全档只跑一遍


_SKL_DATE = {'ts': 0.0, 'v': ''}


def _skl_index_last():
    """本地板块指数日线最新日期（60 秒缓存）—— 前端「行业/趋势」标题显示「指数至 MM-DD」用

    2026-10-22 用户提问踩出的坑：标题上的时刻是**这次分析跑的时间**，不是数据日期。
    当天指数那根因为「权威交易日」取自滞后的指数历史而永远写不进去，判定悄悄停在昨天。
    现在标题同时显示指数日线截止哪天，一眼能看出有没有对齐。
    """
    try:
        now = time.time()
        if now - _SKL_DATE['ts'] < 60 and _SKL_DATE['v']:
            return _SKL_DATE['v']
        from quant_terminal import sector_kline as _sk
        _SKL_DATE['v'] = _sk.index_last_date()
        _SKL_DATE['ts'] = now
        return _SKL_DATE['v']
    except Exception:
        return ''


def _today_str() -> str:
    return datetime.now().strftime('%Y-%m-%d')


def _yest_str() -> str:
    return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')


def _data_date_str() -> str:
    """v2.220: 数据日 (= 本地板块指数日线末根日), 周末/节假日与墙钟日不同

    徽标基线里的 date 现在是数据日(engine.baseline_data_date), 读路径若还拿墙钟日比,
    周六周日就永远命中不了「近两日」→ 每次都白重算 50 秒, 而且拿到的是重算结果不是基线那份。
    """
    try:
        return engine.baseline_data_date() or _today_str()
    except Exception:
        return _today_str()


def _recent_data_date(bd: str, days: int = 7) -> bool:
    """bd 是否是最近的某个数据日 (容忍长假, 故给 7 天窗)"""
    bd = str(bd or '')
    if len(bd) != 10:
        return False
    try:
        dd = _data_date_str()
        if bd > dd:
            return False
        return (datetime.strptime(dd, '%Y-%m-%d') - datetime.strptime(bd, '%Y-%m-%d')).days <= days
    except Exception:
        return False


def _prewarm_all_lines(persist_ma: bool = False):
    """后台预热四档均线缓存 (2026-08-05): 打开终端/更新板块后全档位秒切换
    v2.14 (2026-08-27): 改为 refresh_trend_data_file — 全算4档并落盘 trend_data.json,
    "更新板块" force 与启动预热共用, 盘后扫描直接读盘不再重算
    v2.61 (2026-09-10): persist_ma=True 仅手动"更新板块"(force)传 — 线程内滚动双槽
    基线(4/5/6线)并落盘; 启动预热/自动路径默认 False → 绝不写基线"""
    global _PREWARM_FLAG
    if _PREWARM_FLAG:  # 并发 force: 第二个跳过 (同值重复计算/写盘无意义)
        return
    _PREWARM_FLAG = True
    try:
        refresh_trend_data_file(persist_ma=persist_ma)
        # v2.213 (用户 m05977): 上级行业(90类)趋势跟着一起刷 —— 用户要求
        #   「行业趋势和概念趋势通过任何途径启动了更新，都顺带更新上级行业趋势」。
        #   这里就是那个"任何途径"：启动预热 / 「板块更新」force / 快照后重算 全走这条。
        #   只刷**前端当前选的那一档**（auto_task.ui_lines 由心跳带上来），其余档位等切档再现算。
        #   ★ 失败不影响上面：上级行业是只读参考、不参与任何选股。
        #   ★ run_sector_refresh_auto 里另有一处同样的刷新 —— 两处并存不会重复算，
        #     parent_trend 内部按 (lines, kline_last) 判新鲜度，同一根 K 线不重算。
        try:
            _pln = auto_task.ui_lines() or '4'
            pt.parent_trend(_pln, force=True)
        except Exception as _e:
            _swlog('prewarm_parent_trend', _e)
    except Exception as _e:
        _swlog('prewarm_trend', _e)
    finally:
        _PREWARM_FLAG = False



# ============================================================
# v2.152 (2026-10-18): 自动任务的两个执行体
#   与前端「⚡快照更新」「🔄更新板块」按钮**同一条路**，只是入口换成定时器。
# ============================================================
SNAP_CSV_LOCK = threading.Lock()


def run_snapshot_from_intraday(log=print):
    """用临时快照写 daily/*.csv（15:31 那轮）

    为什么不复用 _run_kline_update_snapshot:
      那条路走腾讯 qt.gtimg.cn，逐批 50 只联网、还要处理除权兜底/新股建CSV，
      全市场 15~40 秒。15:31 时临时快照**已经是全市场当天的正确数据了**
      （含通达信已调整好的除权因子），本地写盘只要零点几秒，且零联网。
      ⇒ 这里走纯本地写盘，腾讯那条路保持原样给手动按钮用。

    写盘口径与 _run_kline_update_snapshot 完全一致:
      行 = 日期,开,收,高,低,量(手) [,总市值,流通市值,自由流通股本]
      纯 LF、tmp + os.replace 原子替换、表头有市值两列才写市值。
    """
    import os as _os
    from quant_terminal import mcap as _mcap
    out = {'ok': False, 'written': 0, 'skipped': 0, 'fixed': 0,
           'failed': 0, 'errors': []}
    st = _intraday.status()
    if not st.get('ok'):
        out['error'] = '没有临时快照'
        return out
    snap_date = st.get('trade_date') or ''
    if len(snap_date) != 10:
        out['error'] = f'快照日期异常: {snap_date!r}'
        return out
    bars = _intraday.bars()
    if not bars:
        out['error'] = '快照里没有行情'
        return out
    caps = _intraday.caps() or {}
    daily_dir = ds.resolve(ds.get('local', 'kline_dir', _os.path.join('kline_data', 'daily')))
    if not _os.path.isdir(daily_dir):
        out['error'] = f'daily 目录不存在: {daily_dir}'
        return out
    with SNAP_CSV_LOCK:
        for code, b in bars.items():
            if b['close'] <= 0 or float(b.get('open') or 0) <= 0:
                # v2.168: 写盘侧再兜一道 —— 快照里若已混进「收盘=昨收、开盘/高/低=0」的停牌行,
                #   绝不能落成 K 线 (否则 MA/ATR/振幅 全被这根 0 价柱污染)。判据与 tdx_fetch 保持一致。
                out['skipped'] += 1
                continue
            # v2.152 fix: 本地 CSV 文件名带 SH/SZ/BZ 前缀（SH600000.csv），
            #   裸码直接拼 '600000.csv' 永远不存在 ⇒ 全部静默 skipped。
            #   mcap.path_of 已处理 920xxx 北交所等全部边界，直接用它。
            fp = _mcap.path_of(code)
            if not _os.path.exists(fp):
                out['skipped'] += 1      # 新股不在这里建文件（建CSV要补历史，交给手动那条路）
                continue
            try:
                with open(fp, encoding='utf-8') as fh:
                    txt = fh.read()
                lines = txt.splitlines()
                if not lines:
                    out['skipped'] += 1
                    continue
                while lines and not lines[-1].strip():
                    lines.pop()
                last_date = lines[-1].split(',')[0] if len(lines) > 1 else ''
                # v2.275: 半天那根被作废时, 把它的第 9 列(自由流通股本)接在手上带给新行。
                #   不 pop 的分支这里保持空串 —— 后面写第 9 列时会走「往前找最近一次非空」。
                _carry_free = ''
                if last_date == snap_date:
                    # v2.250 (2026-11-07 用户 m21973): 同一日期也必须**比值**，不能只比日期。
                    #   盘中手动点「⚡ 快照更新」会用半天数据先写一根；15:31 收盘那轮
                    #   若只判「今天已有行」就整天跳过 ⇒ 当天 K 线永远停在半天的价/量上，
                    #   连带 MA / 距峰 / 涨跌幅 / 模拟盘历史收益 / 板块档位全跟着错。
                    #   （与 v2.208 板块指数「自锁」是同一类 bug：存在性 ≠ 正确性）
                    #   收盘=收盘 且 量=量 才算真写过；否则作废那根、下面重写。
                    _o = lines[-1].split(',')
                    try:
                        _same = (abs(float(_o[2]) - float(b['close'])) < 5e-4
                                 and abs(float(_o[5]) - float(b['volume'])) < 1.0)
                    except (IndexError, TypeError, ValueError):
                        _same = False
                    if _same:
                        out['skipped'] += 1
                        continue
                    # 🔴 v2.275 (2026-11-11 用户 m09426 金辰股份无自由流通股本): 作废半天那根**之前**,
                    #   必须先把它第 9 列的值接住。为什么 —— 市值两列在**每一根**上都有值, 丢掉一根
                    #   下一根还能找回来; 但第 9 列(自由流通股本)按规矩只活在**最新那一行**, 而这根
                    #   恰恰就是最新那根。pop 掉它 = 全文件再无第 9 列, 后面那句"往前找最近一次非空"
                    #   就只能找到空 → 写出来的新行第 9 列永远是空的, 数据就此丢失。
                    #   实测受害 7 只: 603396 金辰股份 / 600255 / 600830 / 603341 / 000920 / 002942
                    #   (回填写在 11-11 半天那根上 → 15:31 收盘轮 pop 掉 → 空)。
                    _popped = lines.pop()        # 半天那根作废（历史行/市值列都保留）
                    _pf = _popped.split(',')
                    if len(_pf) > 8 and _pf[8].strip():
                        _carry_free = _pf[8].strip()
                    out['fixed'] += 1
                if last_date and last_date > snap_date:
                    out['skipped'] += 1      # 本地比快照还新（不可能，但绝不回退）
                    continue
                # v2.159 (2026-10-20): 今天有除权 ⇒ 先把历史行价格换到今日尺度
                #   （前复权）。成交量不乘 —— 与通达信官方前复权导出口径一致。
                #   系数由 tdx_fetch 给出（= 通达信 LastClose / 本地末行收盘），
                #   且只在「本地末行 == 上一个交易日」时才非 1，停牌股拿不到系数。
                fac = 0.0
                try:
                    fac = float(b.get('factor') or 0)
                except (TypeError, ValueError):
                    fac = 0.0
                if fac > 0 and abs(fac - 1.0) > 1e-9:
                    fixed = 0
                    for _i in range(1, len(lines)):
                        _s = lines[_i].split(',')
                        if len(_s) < 5:
                            continue
                        try:
                            for _j in (1, 2, 3, 4):     # 开,收,高,低
                                _s[_j] = f'{float(_s[_j]) * fac:.3f}'
                        except (TypeError, ValueError):
                            continue
                        lines[_i] = ','.join(_s)
                        fixed += 1
                    out['exrights'] = out.get('exrights', 0) + 1
                row = (f"{snap_date},{b['open']:.3f},{b['close']:.3f},"
                       f"{b['high']:.3f},{b['low']:.3f},{b['volume']:.0f}")
                hdr = lines[0].split(',')
                _fld = row.split(',')
                if _mcap.CAP_T in hdr and _mcap.CAP_F in hdr:
                    z, lz = caps.get(code, (0, 0))
                    # v2.177 (2026-10-22 用户 m02201 ①): 本轮拿不到市值 (通达信+腾讯都失败) →
                    #   沿用文件里最近一次非空的两列, 绝不写成空/0。
                    if not z or not lz:
                        for _k in range(len(lines) - 1, 0, -1):
                            _s2 = lines[_k].split(',')
                            if len(_s2) >= 8 and (_s2[6].strip() or _s2[7].strip()):
                                try:
                                    if not z and _s2[6].strip():
                                        z = float(_s2[6])
                                except ValueError:
                                    pass
                                try:
                                    if not lz and _s2[7].strip():
                                        lz = float(_s2[7])
                                except ValueError:
                                    pass
                                break
                    _fld += [_mcap.fmt(z if z else None), _mcap.fmt(lz if lz else None)]
                # v2.273 (用户 m08653): 第 9 列「自由流通股本」—— 它是**股本常量**, 不随价格变,
                #   直接从文件里最近一次非空的那一行搬过来; 拿不到就留空 (绝不写 0)。
                if _mcap.FREE in hdr:
                    while len(_fld) < len(hdr) - 1:
                        _fld.append('')
                    # v2.275: 优先用被作废那根带过来的值（_carry_free），没有再往前找最近一次非空。
                    _free_v = _carry_free
                    if not _free_v:
                        for _k in range(len(lines) - 1, 0, -1):
                            _s3 = lines[_k].split(',')
                            if len(_s3) > 8 and _s3[8].strip():
                                _free_v = _s3[8].strip()
                                break
                    _fld.append(_free_v)
                row = ','.join(_fld)
                lines.append(row)
                tmp = fp + '.tmp'
                with open(tmp, 'w', encoding='utf-8', newline='') as fh:
                    fh.write('\n'.join(lines) + '\n')
                _os.replace(tmp, fp)
                out['written'] += 1
            except Exception as e:
                out['failed'] += 1
                if len(out['errors']) < 8:
                    out['errors'].append(f'{code}: {type(e).__name__}: {str(e)[:80]}')
    # ---- v2.249 (2026-11-06 用户 m21803): 顺手给新股建档 ----
    # 为什么必须在这里做: 上面这个循环只遍历「本地已有 CSV」里有的代码
    #   （快照在 tdx_fetch 阶段就被本地清单过滤过），新股永远进不来。
    # 手动按钮那条路一直有建档逻辑，自动任务从来没调过 ⇒ 用户 m21716 发现的死循环。
    # 放在写盘之后、清缓存之前: 新建的文件也要让 /api/mcap 立即看到。
    try:
        from quant_terminal import newstock as _ns
        _nr = _ns.sync(log=lambda _m: print(f'[auto] {_m}', flush=True))
        out['new_stock'] = {'found': _nr.get('found', 0),
                            'created': [x.get('code') for x in (_nr.get('created') or [])],
                            'skipped': _nr.get('skipped', 0),
                            'failed': len(_nr.get('failed') or []),
                            'note': _nr.get('note') or ''}
    except Exception as _e:
        _swlog('newstock_sync', _e)
        out['new_stock'] = {'found': 0, 'created': [], 'note': str(_e)[:120]}
    try:
        _mcap.invalidate()
    except Exception as _e:
        _swlog('mcap_invalidate', _e)
    out['ok'] = out['failed'] == 0 and out['written'] > 0
    if not out['ok'] and not out.get('error'):
        out['error'] = f"写入 {out['written']} 只 / 失败 {out['failed']} 只"
    try:
        log(f"[auto] 快照写CSV: 写{out['written']} 改{out['fixed']} "
            f"跳{out['skipped']} 败{out['failed']}")
    except Exception as _e:
        _swlog('snapshot_log', _e)
    return out


_SIDX_MA_CACHE: Dict[str, tuple] = {}
_SIDX_MA_TTL = 300


# v2.221 (2026-10-25): 板块指数当日涨跌幅 —— 三个趋势窗口/悬停浮窗共用一张 name→pct 表
#   数据来自 sector_kline.ma_scan.json（build_ma_cache 落的 pct/z5/z20/z60），<50ms 秒读，
#   不重算。名称有 3 个概念与同花顺写法不同（WiFi 6/AI PC/中国AI 50 我们存成带空格），
#   所以再做一份「去空格大写」索引兜底。
_IDX_PCT_CACHE = {'ts': 0.0, 'exact': {}, 'norm': {}}
_IDX_PCT_TTL = 300


def _idx_pct_map():
    import time as _t
    _now = _t.time()
    if _IDX_PCT_CACHE['exact'] and (_now - _IDX_PCT_CACHE['ts']) < _IDX_PCT_TTL:
        return _IDX_PCT_CACHE['exact'], _IDX_PCT_CACHE['norm']
    ex, nm = {}, {}
    try:
        from quant_terminal import sector_kline as _sk
        rows = _sk.ma_cache_rows() or {}
        for _tc, _v in rows.items():
            _n = (_v or {}).get('name') or ''
            if not _n:
                continue
            _rec = {'pct': _v.get('pct', 0.0), 'z5': _v.get('z5', 0.0),
                    'z20': _v.get('z20', 0.0), 'z60': _v.get('z60', 0.0)}
            ex[_n] = _rec
            nm[''.join(str(_n).split()).upper()] = _rec
    except Exception as _e:
        _swlog('idx_pct_map', _e)
        return {}, {}
    _IDX_PCT_CACHE.update({'ts': _now, 'exact': ex, 'norm': nm})
    return ex, nm


def _idx_pct(name):
    """→ {'pct','z5','z20','z60'} 或 {} (查不到就是空, 前端显 --)"""
    _n = str(name or '')
    if not _n:
        return {}
    ex, nm = _idx_pct_map()
    r = ex.get(_n)
    if r is None:
        r = nm.get(''.join(_n.split()).upper())
    return r or {}


def _idx_pct_attach(rows):
    """就地给每行补 pct/z5/z20/z60（查不到的留空, 前端显 --）"""
    for _r in (rows or []):
        try:
            _p = _idx_pct(_r.get('name'))
        except Exception:
            _p = {}
        if _p:
            _r['pct'] = _p.get('pct', 0.0)
            _r['z5'] = _p.get('z5', 0.0)
            _r['z20'] = _p.get('z20', 0.0)
            _r['z60'] = _p.get('z60', 0.0)
    return rows


def _top5_attach(rows, lines, rt):
    """v2.242 (2026-10-30 用户 m20347/m20454): 三个趋势窗口的两列新数据。

    给每行补 5 个字段：
      mcap_max_name  成分股里**总市值最大**那只的简称（6 字以内，最长见过 '奥比中光-W'）
      mcap_max_code  它的 6 位代码（v2.244：前端点它 / 悬停它要拉个股 K 线，见用户 m20802）
      mcap_max_st    它的三态：'多' / '中' / '空'
      mcap_top_n     topN 的 N（min(5, 能算出状态的只数)；不足 5 家真实存在 ⇒ 可能 1~4）
      mcap_top_score topN 得分（多=1 中=0.5 空=0）

    ⚠ 只写字段、不改顺序、失败一律不抛（这两列是锦上添花，不能拖垮整个窗口）。
    """
    try:
        import quant_terminal.top5 as t5
        t5.attach(rows, lines, rt)
    except Exception as _e:
        _swlog('top5_attach', _e)


def _sector_index_ma(periods, tag='', only=False, force=False):
    """板块指数自身均线排列（走 sector_kline 本地库，710 个 CSV 约 1~2 秒 → 缓存 5 分钟）"""
    import time as _t
    # v2.180: 盘中/午休把当天指数快照当临时一根（与 engine.official_index_aligned 同口径）
    _ov, _ovd = {}, ''
    try:
        from quant_terminal.engine import _index_live_window as _ilw
        if _ilw():
            from quant_terminal import sector_kline as _sk0
            _ov = _sk0.live_index_overlay() or {}
            _ovd = _t.strftime('%Y-%m-%d')
    except Exception:
        _ov, _ovd = {}, ''
    key = (tuple(int(p) for p in periods), tag or '', bool(only), bool(_ov))
    if not force:
        ent = _SIDX_MA_CACHE.get(key)
        if ent and (_t.time() - ent[0]) < _SIDX_MA_TTL:
            return ent[1]
    try:
        from quant_terminal import sector_kline as _sk
        tags = ((tag,) if tag else ('industry', 'cn_concept'))
        sc = _sk.scan_all(periods=periods, tags=tags,
                          overlay=(_ov or None), overlay_date=_ovd)
        rows = sc['rows']
        # v2.173 (2026-10-22 用户定案): 概念只呈现终端认可的那一批 —— 与 /api/concept_ma
        #   完全同口径（concept_blocklist.txt 精确名屏蔽，390 → 350），
        #   上级 90 + 细分 230 + 概念 350 = 670 个, 不再把 710 个全塞给用户。
        try:
            from quant_terminal.engine import load_concept_blocklist as _lbl
            _bl = _lbl()
            if _bl:
                rows = [r for r in rows
                        if not (r.get('tag') == 'cn_concept' and r.get('name') in _bl)]
        except Exception:
            _bl = ()
        if only:
            rows = [r for r in rows if r['aligned']]
        res = {'ok': True, 'rows': rows, 'total': len(rows),
               'aligned': sum(1 for r in rows if r['aligned']),
               'filtered_concepts': len(_bl),
               'periods': sc['periods'],
               'tag': tag, 'ts': _t.strftime('%m-%d %H:%M:%S')}
    except Exception as e:
        return {'ok': False, 'rows': [], 'error': f'{type(e).__name__}: {str(e)[:160]}'}
    _SIDX_MA_CACHE[key] = (_t.time(), res)
    return res


_FIDX_SECTOR_CACHE_CLEAR = None


def run_sector_refresh_auto(eod=False, log=print):
    """自动更新板块 —— 与前端「🔄更新板块」按钮同一条路（四档全刷 + 落盘）

    前端按钮做的事: loadSectorMA(force) → loadConceptMA(force) → loadSectors(force)
    → fetchSectorZt(force)。后端等价物就是 refresh_trend_data_file(persist_ma=True)
    + 重拉板块热度 + 重拉涨停数。
    """
    out = {'ok': False, 'sector': 0, 'concept': 0, 'parts': {}}

    # v2.172 (2026-10-22) 成分股增量修正 —— 用户 m01560②：「每天更新板块之前先修正一下成分股，
    #   拉取一下成分股数据到缓存，然后和本地文件的成分股进行对比，将差异部分改写，没有变化的不动」。
    #   · 每天一次（fuyao_index.sync_maps 自带日期闸 map_sync_state.json），11:35/15:35 只有第一次真拉
    #   · 只改写差异行，无变化的行一个字节都不动 ⇒ 文件 mtime 不变时 engine 的 mtime 缓存也不会失效
    #   · 失败**绝不影响**板块刷新：整段包 try，结果单独放 out['map_sync'] 供气泡显示
    try:
        from quant_terminal import fuyao_index as _fidx
        _ms = _fidx.sync_maps(verbose=False)
        out['map_sync'] = {'ok': bool(_ms.get('ok')), 'skipped': bool(_ms.get('skipped')),
                           'changed': _ms.get('total_changed', 0),
                           'added': _ms.get('total_added', 0),
                           'took': _ms.get('elapsed'),
                           'error': (_ms.get('error') or '')[:150]}
    except Exception as e:
        out['map_sync'] = {'ok': False, 'error': f'{type(e).__name__}: {str(e)[:150]}'}

    # v2.173 (2026-10-22) 板块指数日线本地库维护
    #   · 收盘后（eod=True）：用指数快照把**当天那一根**追加进本地日线（710 个约 2 秒）
    #     必须在 15:35 之后 —— 盘中跑会把半天数据当成一根日线写进去
    #   · 任何时段：补齐落后的指数（漏跑 / 新上市 / 停更），走逐只拉历史补洞
    #   · 成分股有变动（map_sync.changed>0）⇒ 怀疑官方重算过历史 ⇒ 后台全量重拉覆盖
    try:
        from quant_terminal import sector_kline as _skl
        _sk_res = {}
        if eod:
            _sk_res = _skl.append_today(verbose=False)
        _gaps = _skl.gap_report()
        if _gaps:
            for _tc in _gaps[:400]:
                try:
                    _skl.rebuild(_tc, years=5)
                except Exception as _e:
                    _swlog('sector_index_rebuild', _e)
            _skl.write_zdf()
            _skl.invalidate_zdf()
            _sk_res['gaps'] = len(_gaps)
        out['sector_kline'] = _sk_res
    except Exception as e:
        out['sector_kline'] = {'ok': False, 'error': f'{type(e).__name__}: {str(e)[:150]}'}

    # 成分股有变动 ⇒ 官方很可能重算过板块指数历史 ⇒ 后台全量重拉覆盖（不阻塞本次刷新）
    try:
        _chg = int((out.get('map_sync') or {}).get('changed') or 0)
        if _chg > 0:
            import threading as _th

            def _rebuild_bg():
                try:
                    from quant_terminal import sector_kline as _skb
                    _skb.build_all(force=True, verbose=False)
                except Exception as _e:
                    _swlog('sector_index_build_all', _e)

            _th.Thread(target=_rebuild_bg, daemon=True).start()
            out.setdefault('sector_kline', {})['rebuild_bg'] = _chg
    except Exception as _e:
        _swlog('sector_index_block', _e)

    try:
        _sector_ma_cache_clear()
        t0 = time.time()
        # 临时快照已由 engine.with_intraday 在 analyze_*_ma 内部自动叠加 ——
        # 这里不传 rt, 让「快照覆盖层」自己生效 (与前端按钮完全同一条路)。
        data = refresh_trend_data_file(persist_ma=True)
        ls = (data or {}).get('lines') or {}
        # 4 线档的 key 是 _trend_key((5,10,21,55)) = '5102155'
        slot = ls.get('5102155') or {}
        out['sector'] = len(slot.get('sector') or [])
        out['concept'] = len(slot.get('concept') or [])
        out['parts']['trend'] = {'ok': True, 'sec': out['sector'],
                                 'con': out['concept'], 'sec_took': round(time.time() - t0, 1)}
    except Exception as e:
        out['parts']['trend'] = {'ok': False, 'error': f'{type(e).__name__}: {str(e)[:150]}'}

    # 板块热度（腾讯实时 Top20）
    try:
        clear_sector_rt_cache()
        eng = QuantEngine()
        eng.load_data()
        eng.calc_sector_returns()
        sorted_sectors = sorted(
            eng.sector_returns.items(),
            key=lambda x: -(x[1].get('change_pct', 0) if isinstance(x[1], dict) else (x[1] or 0)))
        rows = []
        for name, d in sorted_sectors[:20]:
            if isinstance(d, dict):
                rows.append({'name': name, 'change_pct': d.get('change_pct', 0),
                             'zdf5': d.get('zdf5', 0), 'zdf20': d.get('zdf20', 0),
                             'up_count': d.get('up_count', 0),
                             'total_count': d.get('total_count', 0),
                             'net_flow_yi': d.get('net_flow_yi', 0),
                             'lead_stock': d.get('lead_stock', ''),
                             'lead_pct': d.get('lead_pct', 0),
                             'lead_code': d.get('lead_code', '')})
            else:
                rows.append({'name': name, 'change_pct': round(d or 0, 2)})
        now_str = datetime.now().strftime('%m-%d %H:%M:%S')
        _write_json_file(SECTOR_HOT_FILE, {'ts': now_str, 'source': '同花顺 fuyao + 腾讯资金面', 'sectors': rows})
        out['parts']['hot'] = {'ok': True, 'n': len(rows)}
    except Exception as e:
        out['parts']['hot'] = {'ok': False, 'error': f'{type(e).__name__}: {str(e)[:150]}'}

    # 行业涨停数
    try:
        import quant_terminal.dragon as dragon
        zt = dragon.fetch_sector_zt(force=True)
        n_zt = len(zt.get('zt') or {})
        out['parts']['zt'] = {'ok': not zt.get('degraded'), 'n': n_zt,
                              'provider': zt.get('provider') or ''}
    except Exception as e:
        out['parts']['zt'] = {'ok': False, 'error': f'{type(e).__name__}: {str(e)[:150]}'}

    # v2.160 (2026-10-20) 上级行业(90类)趋势 —— 用户 m20042: 更新板块时一起刷。
    #   刷哪一档 = **前端当前选的均线档位**（心跳带上来，见 auto_task.ui_lines）。
    #   ★ 不传 rt: 与上面 refresh_trend_data_file 同一口径 —— 让「临时快照覆盖层」
    #     (engine.with_intraday) 自己生效。11:31/11:35/15:35 都非交易窗口，
    #     _is_trade_window() 本来就返回 False ⇒ 与 /api/parent_sector_ma 那条路一致。
    #   ★ 失败**不影响** out['ok']: 上级行业是只读参考，不参与任何选股；
    #     它挂了不该让「板块更新」整体报失败。单独放进 out['parent'] 供气泡显示。
    try:
        _pln = auto_task.ui_lines() or '4'
        _pr = pt.parent_trend(_pln, force=True)
        out['parent'] = {'ok': bool(_pr.get('ok')), 'lines': _pln,
                         'n': len(_pr.get('rows') or []),
                         'took': _pr.get('elapsed'),
                         'error': ('' if _pr.get('ok') else (_pr.get('error') or '')[:150])}
    except Exception as e:
        out['parent'] = {'ok': False, 'lines': auto_task.ui_lines() or '4',
                         'error': f'{type(e).__name__}: {str(e)[:150]}'}

    out['ok'] = out['sector'] > 0 and out['concept'] > 0
    if not out['ok']:
        out['error'] = '趋势重算没出结果'
    # v2.231 (2026-10-26): 原来这里用 ✓/✗，GBK 控制台打印时抛 UnicodeEncodeError，
    #   被下面 except 送进 swallow.log —— 数据没影响，但那行汇总日志永远看不到。
    #   改成纯 ASCII 的 OK/失败。
    try:
        _pc = out.get('parent') or {}
        log(f"[auto] 板块更新: 行业{out['sector']} 概念{out['concept']} "
            f"热度{out['parts'].get('hot',{}).get('n','-')} "
            f"上级{_pc.get('lines','')}线"
            + (f"OK {_pc.get('n',0)}个" if _pc.get('ok') else f"失败 {_pc.get('error','')}"))
    except Exception as _e:
        _swlog('sector_refresh_summary', _e)
    return out



def _restart_server():
    """v2.14: 重启自身进程 (前端状态栏"重连服务器"按钮触发)
    Popen 无窗口拉起新进程后 os._exit, 原端口绑定在 2-4 秒内被新进程接管"""
    import os
    import subprocess
    try:
        subprocess.Popen([sys.executable, '-m', 'quant_terminal.server'],
                         cwd=str(BASE_DIR),
                         creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    except Exception:
        pass
    os._exit(0)


# ============================================================
# 指数日K每日自动落盘 guard (v2.57 2026-09-05)
# 宿主: server __main__ 启动的 daemon 线程 (300s tick)。触发公式:
#   a) 非交易且已过15:05 → 当日首次 sync (盘后settle落当日bar)
#   b) 本地末日 < 最近工作日 → sync (漏开终端/周末/长假后启动自愈)
#   文件缺失 → ensure (任何时刻, 盘中建库自动滤当日forming bar)
#   连续失败≥5 → 暂停扫尾30分钟 (防腾讯501风暴), 缺失ensure不受暂停影响
# 日历无关设计: 节假日无新bar时远端比对判 noop, 天然空跑。
# 环境变量 QT_NO_IDX_GUARD=1 单点停用 (不影响本地读取路径)
# ============================================================
_IDX_GUARD = {'synced_on': {}, 'fails': 0, 'pause_until': 0.0}
GUARD_TICK = 300
IDX_GUARD_ON = os.environ.get('QT_NO_IDX_GUARD') != '1'


def _index_local_status() -> Dict:
    """7文件状态: exists/count/first/last (供 /api/index/status)"""
    out = {}
    for code in INDEX_DAILY_CODES:
        rows = read_index_daily(code)
        out[code] = {'exists': rows is not None,
                     'count': len(rows) if rows else 0,
                     'first': rows[0]['date'] if rows else None,
                     'last': rows[-1]['date'] if rows else None}
    return out


def sync_all_index_daily(force: bool = False) -> Dict:
    """7码逐码 sync (guard 与 /api/index/sync 共用). force=1 允许盘中执行(仍不写forming bar)"""
    results = {}
    for code in INDEX_DAILY_CODES:
        try:
            results[code] = sync_index_daily(code, force=force)
        except Exception as e:
            results[code] = f'err:{e!r}'
    return results


def _run_index_sync(force: bool = False):
    try:
        sync_all_index_daily(force=force)
    except Exception as e:
        print(f'[index_sync] {e!r}', file=sys.stderr)


def _index_guard_loop():
    """daemon 300s tick — 见模块注释触发公式; 单码/整轮异常不杀线程"""
    while IDX_GUARD_ON:
        try:
            now = datetime.now()
            today = now.strftime('%Y-%m-%d')
            t = now.hour * 100 + now.minute
            d = now
            while d.weekday() >= 5:  # 周末回溯到周五
                d = d - timedelta(days=1)
            recent_wd = d.strftime('%Y-%m-%d')
            # v2.118: 休市日基准日要退回**上一个交易日**。
            #   ⚠ 这是 v2.118 自己带出来的: 原先节假日 _is_trade_window() 返 True → 上面
            #   867 行直接 continue 跳过同步; 现在返 False, 一路走到 back-sync, 若基准日
            #   仍按"今天"算, 则 back 恒为真 ⇒ **每 5 分钟白拉一轮 7 个指数的 K线**。
            #   is_holiday() 拿不准时返 False (退回纯时钟), 正是这里要的安全方向。
            if ds.is_holiday():
                _ls = ds.last_session(None)
                if _ls:
                    recent_wd = _ls
                else:
                    e = d - timedelta(days=1)
                    while e.weekday() >= 5:
                        e -= timedelta(days=1)
                    recent_wd = e.strftime('%Y-%m-%d')
            paused = time.time() < _IDX_GUARD.get('pause_until', 0)
            for code in INDEX_DAILY_CODES:
                try:
                    rows = read_index_daily(code)
                    if rows is None:
                        ensure_index_daily(code)  # 缺失自愈 (暂停期也做, 盘中建库滤forming)
                        continue
                    if _is_trade_window():
                        continue  # 盘中绝不写盘 (实时性由读路径 with_live 承担)
                    evn = t >= 1505 and _IDX_GUARD['synced_on'].get(code) != today
                    back = (not evn) and (not paused) and rows[-1]['date'] < recent_wd
                    if not (evn or back):
                        continue
                    st = sync_index_daily(code)
                    if st == 'fail':
                        _IDX_GUARD['fails'] += 1
                        if _IDX_GUARD['fails'] >= 5:
                            _IDX_GUARD['pause_until'] = time.time() + 1800
                            print('[index_guard] 连续失败≥5, 暂停扫尾30分钟', file=sys.stderr)
                    else:
                        _IDX_GUARD['fails'] = 0
                        # 盘后例行(evening)noop也记账(防节假日每5分钟空转);
                        # 补缺(back)noop不记账 — 15:05盘后例行仍需机会落当日bar
                        if evn or st != 'noop':
                            _IDX_GUARD['synced_on'][code] = today
                except Exception as e:
                    print(f'[index_guard] {code}: {e!r}', file=sys.stderr)
        except Exception as e:
            print(f'[index_guard] tick异常: {e!r}', file=sys.stderr)
        time.sleep(GUARD_TICK)


def _m4_flat(r: dict) -> dict:
    """v2.216 (2026-10-24 用户 m06859): 把 engine 行里的 m4_verify 摊平成前端读的字段。

    前端 renderStockDetail 里「A股适配」那一段读的是**扁平**键
    (m4_passed / sector_strength / bias_val / limit_exclude …)，而 engine 四个扫描器
    现在写的是嵌套的 r['m4_verify']。这里统一摊一次，四个模式 + 龙头两条链共用。

    ★ 没有 m4_verify 就返回 {}（一个键都不加），前端继续显示「--」——
      绝不写 0 冒充「算过了」。缺键 = 不确定，比假 0 诚实。
    """
    m4 = r.get('m4_verify')
    if not isinstance(m4, dict) or not m4:
        return {}
    return {
        'm4_passed': m4.get('passed', 0),
        'm4_total': m4.get('total', 3),
        'sector_strength': m4.get('sector_strength', 0),
        'sector_ok': m4.get('sector_ok', False),
        'sector_src': m4.get('sector_src', ''),
        'bias_val': m4.get('bias_val', 0),
        'bias_ok': m4.get('bias_ok', True),
        'bias_label': m4.get('bias_label', ''),
        'limit_exclude': not m4.get('limit_ok', True),
        'limit_label': m4.get('limit_label', ''),
    }


# >>> M4 BEGIN
def _m4_bulk(rows):
    """v2.261 (2026-11-08 用户 m03399): 给竞价打板/超跌反弹/波段低吸三个新模式补 A股适配。

    ★ 为什么需要: 这三个模式的扫描模块 (auction_scan / rebound_scan / wave_scan) 只产出
      表格行, 从不调 engine 的 _m4_for —— 所以前端详情页「A股适配」那一栏一直显示
      「-- (本行未算)」。而趋势三模式 + 涨停突破/回调/粘合/龙头五个扫描器都算了。
    ★ 口径与 engine 那四处完全一致: 同一个 AShareVerifier + 同一张 sector_returns 表
      (fetch_sector_realtime 走同花顺 fuyao 全量 710 板块, 带 TTL 缓存)。
    ★ 算不出来就**不写** m4_verify (绝不写 0) —— _m4_flat 见不到这个键, 前端继续显「--」,
      「缺键 = 不确定, 比假 0 诚实」。
    ★ 任何异常都吞掉: 补不上 A股适配绝不能让整轮扫描失败 (尤其竞价打板卡在 09:25~09:30 窗口)。
    """
    if not rows:
        return rows
    try:
        from quant_terminal.engine import fetch_sector_realtime as _fsr
        _sr = _fsr()
    except Exception:
        _sr = {}
    for r in rows:
        try:
            code = str(r.get('code') or '')
            if not code:
                continue
            num = code[2:] if code[:2].upper() in ('SH', 'SZ', 'BJ', 'BZ') else code
            if not num:
                continue
            full = mcap.prefixed(num)
            kl = load_csv_kline(full)
            if not kl:
                continue
            m4 = _m4_for(kl, r.get('sector', ''), r.get('concept', ''),
                         r.get('change_pct'), code, _sr)
            if isinstance(m4, dict) and m4:
                r['m4_verify'] = m4
        except Exception:
            continue
    return rows
# <<< M4 END


def run_scan_async(mode='20', lines='4', pool='auto'):
    """后台线程: 运行扫描并推送进度. mode: 100/55/20; lines: 均线档位 3/4/5/6
    pool: 'auto'=自建池(8801同花顺条件全市场建池) | 'file'=导出池(读选股池文件)
    v2.23 (2026-08-30): scan_id 换代 — 新扫描启动时 scan_id+1, 旧线程在进度回调/写结果前
    发现代际不匹配即自我退出 (不写状态不推送), 彻底解决"扫描卡住后永远'已在扫描中'"。
    v2.31 (2026-09-01): 股池来源切换 — 100/55/20 三趋势模式支持自建池/导出池两选
    """
    global SCAN_STATE
    my_id = SCAN_STATE.get('scan_id', 0) + 1
    SCAN_STATE = {'running': True, 'progress': 0, 'total': 0, 'done': False,
                  'error': None, 'results': None, 'scan_time': '', 'market_status': '',
                  'pool_size': 0, 'mode': mode, 'lines': lines, 'pool': pool, 'scan_id': my_id}

    try:
        if mode == 'dragon':
            # v2.16 (2026-08-27): 龙头战法 — 妖龙榜 + 龙回头池 (自建池, 不预加载)
            engine = QuantEngine()
            engine.ind_map, engine.con_map = load_industry_concept_maps()
            # v2.216: 不建池的模式也要拿全量板块涨幅表, 否则 A股适配的「板块」项永远走兜底
            engine.calc_sector_returns_all()
            SCAN_STATE['pool_size'] = 0
            SCAN_STATE['total'] = 0
            notify_sse('progress', {'progress': 0, 'total': 0, 'msg': '拉取涨停池+扫描妖龙回调中...'})
        # >>> AUCTION BEGIN
        elif mode == 'auction':
            # 竞价打板 (2026-11-06): 不需要 QuantEngine/K线池预加载 —— 自己的
            # auction_scan.py 直接打腾讯快照 + 本地 CSV, 与 engine 完全无关
            engine = None
            SCAN_STATE['pool_size'] = 0
            SCAN_STATE['total'] = 0
            notify_sse('progress', {'progress': 0, 'total': 0,
                                    'msg': '拉取全市场竞价快照中...(约 5~20 秒)'})
        # <<< AUCTION END
        else:
            engine = QuantEngine()
            engine.mh_lines = lines  # 板块铁律按档位过滤 (2026-08-05)
        if mode in ('ai', 'smart', 'glue'):
            # AI/Smart模式自己建池, 不预加载
            engine.ind_map, engine.con_map = load_industry_concept_maps()
            # v2.216: 同上 —— 这四个模式 self.pool 为空, calc_sector_returns() 会直接 return
            engine.calc_sector_returns_all()
            SCAN_STATE['pool_size'] = 0  # 扫描时更新
            SCAN_STATE['total'] = 0
            # v2.66: ai = 涨停突破 (当日涨停池建池) / v2.67: smart = 涨停回调 (全市场K线建池)
            # v2.138: glue = 均线粘合 (全市场K线建池, 同样零网络粗筛)
            notify_sse('progress', {'progress': 0, 'total': 0,
                                    'msg': '拉取当日涨停池中...' if mode == 'ai'
                                           else ('全市场K线筛「前20日涨停1~3次」中...' if mode == 'smart'
                                                 else '全市场K线筛「四线多头+贴线粘合」中...')})
        # >>> AUCTION BEGIN
        elif mode == 'auction':
            # 竞价打板: engine=None, 无股池预加载 —— 直接进下方扫描分支
            SCAN_STATE['pool_size'] = 0
            SCAN_STATE['total'] = 0
        # <<< AUCTION END
        # >>> REBOUND BEGIN
        elif mode == 'rebound':
            # 超跌反弹: 全本地 K 线建池 (零网络), engine=None —— 直接进下方扫描分支
            SCAN_STATE['pool_size'] = 0
            SCAN_STATE['total'] = 0
        # <<< REBOUND END
        # >>> WAVE BEGIN
        elif mode == 'wave':
            # 波段低吸: 全本地 K 线建池 (零网络), engine=None —— 直接进下方扫描分支
            SCAN_STATE['pool_size'] = 0
            SCAN_STATE['total'] = 0
        # <<< WAVE END
        elif mode not in ('100', '55', '20', 'glue') or pool != 'auto':
            n = engine.load_data(mode=mode)
            _check_scan_id(my_id)
            SCAN_STATE['pool_size'] = n
            SCAN_STATE['total'] = n
            notify_sse('progress', {'progress': 0, 'total': n, 'msg': f'加载{n}只股票...'})
        # v2.31: 自建池模式 (100/55/20) — 建池在 on_progress 定义后执行 (build_pool_conds 需要回调)

        def on_progress(current, total, extra=None):
            # extra: build_pool_auto 传第3参 (池子已筛出数), 兼容 (2026-08-05 修复: 传2参曾致AI扫描TypeError卡死)
            _check_scan_id(my_id)  # v2.23: 被新扫描替换 → 抛异常自我退出, 不写状态不推送
            SCAN_STATE['progress'] = current
            SCAN_STATE['total'] = total
            # >>> AUCTION BEGIN
            # 竞价打板: auction_scan 的 _p(i, n, msg) 第3参是阶段说明字符串 (不是池子数),
            # 直接显示「拉取主板快照…/粗筛中…」比「扫描中 1/5…」有用得多
            if mode == 'auction' and isinstance(extra, str) and extra:
                notify_sse('progress', {'progress': current, 'total': total, 'msg': extra})
                return
            # <<< AUCTION END
            # >>> REBOUND BEGIN
            # 超跌反弹: 同 auction, 第3参是阶段说明字符串
            if mode == 'rebound' and isinstance(extra, str) and extra:
                notify_sse('progress', {'progress': current, 'total': total, 'msg': extra})
                return
            # <<< REBOUND END
            # >>> WAVE BEGIN
            # 波段低吸: 同 rebound, 第3参是阶段说明字符串
            if mode == 'wave' and isinstance(extra, str) and extra:
                notify_sse('progress', {'progress': current, 'total': total, 'msg': extra})
                return
            # <<< WAVE END
            notify_sse('progress', {'progress': current, 'total': total,
                                     'msg': f'扫描中 {current}/{total}...'})

        if mode == 'ai':
            # v2.66 (2026-09-11): 「涨停突破」— 涨停池建池 + 行业偏多/强多头筛 + 涨停价破峰
            # 行业档位与行业趋势面板同口径: 盘中传实时价快照, 盘后传 None 走 trend_data.json
            _rt = get_realtime_prices_cached()
            if not _is_trade_window():
                _rt = None
            results = engine.scan_limit_breakout(progress_callback=on_progress,
                                                 realtime_prices=_rt)
            SCAN_STATE['pool_size'] = getattr(engine, 'breakout_cand_size', len(results))
            SCAN_STATE['total'] = SCAN_STATE['pool_size']
        elif mode == 'smart':
            # v2.147 (2026-10-16): 「涨停回调」— 前20日涨停1~3次建池 (v2.67 是 10 日/1~2) +
            # MA21不走坏 + 缩量(今量≤前20日最高量×60%)回调不破 收/中/开。行业档位口径与涨停突破完全一致
            # (盘中传实时价快照, 盘后传 None 走 trend_data.json)
            _rt2 = get_realtime_prices_cached()
            if not _is_trade_window():
                _rt2 = None
            results = engine.scan_limit_pullback(progress_callback=on_progress,
                                                 realtime_prices=_rt2)
            # 「池」= 前20日涨停1~3次的只数 (本模式第一层漏斗), 不是最终结果数
            SCAN_STATE['pool_size'] = getattr(engine, 'pullback_pool_size', len(results))
            SCAN_STATE['total'] = SCAN_STATE['pool_size']
        elif mode == 'glue':
            # v2.138 (2026-10-13): 「均线粘合」— 全市场K线零网络建池
            #   v2.142: MA55 退出选股因子, 价距区间 1~5%/2~7%/3~10%, 三线斜率≥0 硬条件; v2.144: 短线粘合(≤4%/4%)
            #   不做评分: 行业/概念档位 + 放量倍数由前端显示层筛选 (复用 v2.96 applyMhFilter)
            _rt3 = get_realtime_prices_cached()
            if not _is_trade_window():
                _rt3 = None
            results = engine.scan_ma_glue(progress_callback=on_progress,
                                          realtime_prices=_rt3)
            SCAN_STATE['pool_size'] = getattr(engine, 'glue_pool_size', len(results))
            SCAN_STATE['total'] = SCAN_STATE['pool_size']
        # >>> AUCTION BEGIN
        elif mode == 'auction':
            # 竞价打板 (2026-11-06) —— 逻辑全在 auction_scan.py, 这里只做转接
            #   ★ 数据有效性由 auction_scan.in_auction_window() 自检:
            #     只有 09:25~09:30 快照成交量才是竞价量; 窗口外仍可跑但会在
            #     摘要里标「非竞价口径」且不给主推 (screen 内 buy=False)
            import auction_scan as _AUCT
            _auc = _AUCT.scan(progress_callback=on_progress)
            results = _AUCT.to_scan_rows(_auc)
            _m4_bulk(results)   # v2.261: A股适配 (本模式自己不算)
            SCAN_STATE['pool_size'] = _auc.get('n_sample') or len(results)
            SCAN_STATE['total'] = SCAN_STATE['pool_size']
            SCAN_STATE['auction_info'] = {
                'mode': _auc.get('mode'), 'date': _auc.get('date'),
                'index_pct': _auc.get('index_pct'), 'half': _auc.get('half'),
                'buy': _auc.get('buy'), 'note': _auc.get('note'),
                'n_exec': _auc.get('n_exec'), 'n_watch': _auc.get('n_watch'),
                'elapsed': _auc.get('elapsed'), 'log_path': _auc.get('log_path'),
                # v2.253: locked=9:30 后读 day/query 还原的真实竞价值, 前端据此显示「已锁定」
                'auct_src': _auc.get('auct_src'),
            }
        # <<< AUCTION END
        # >>> REBOUND BEGIN
        elif mode == 'rebound':
            # 超跌反弹 (2026-11-07) —— 逻辑全在 rebound_scan.py, 这里只做转接
            #   ★ 全本地 CSV, 零网络; 盘中用实时价覆盖当日那根 K 线 (与其它模式同口径)
            import rebound_scan as _RBD
            _rtb = get_realtime_prices_cached()
            if not _is_trade_window():
                _rtb = None
            _rbd = _RBD.scan(progress_callback=on_progress, rt_prices=_rtb)
            results = _RBD.to_scan_rows(_rbd)
            _m4_bulk(results)   # v2.261: A股适配 (本模式自己不算)
            SCAN_STATE['pool_size'] = _rbd.get('n_total') or len(results)
            SCAN_STATE['total'] = SCAN_STATE['pool_size']
            SCAN_STATE['rebound_info'] = {
                # ★ 'mode' 不能省: 前端 renderReboundBar() 靠 info.mode 判断「有没有内容」,
                #   少了这个键状态条永远 display:none (且不报错) —— 与 auction_info 同款约定
                'mode': 'rebound',
                'date': _rbd.get('date'), 'note': _rbd.get('note'),
                'n_total': _rbd.get('n_total'), 'n_hit': _rbd.get('n_hit'),
                'funnel': _rbd.get('funnel'), 'elapsed': _rbd.get('elapsed'),
            }
        # <<< REBOUND END
        # >>> WAVE BEGIN
        elif mode == 'wave':
            # 波段低吸 (2026-11-07) —— 逻辑全在 wave_scan.py, 这里只做转接
            #   ★ 全本地 CSV, 零网络; 盘中用实时价覆盖当日那根 K 线 (与其它模式同口径)
            #   ★ 盘中/午休时段用**昨日量**判缩量 (当日量才半天, 拿去比整天均量必然"缩量")
            import wave_scan as _WAV
            _rtw = get_realtime_prices_cached()
            if not _is_trade_window():
                _rtw = None
            # v2.271 (2026-11-11 用户 m07829/m07846): 一次把六种口径全跑出来
            #   A 点 = 低/收   B 点 = 低/收/开  ⇒ 2x3 = 6 组
            #   默认『低/收』(低=最低价, 收=收盘价); 前端只显示当前选中那一组,
            #   点状态条上的两颗按钮切换 —— 空组合会被跳过, 切换不到
            _wav = _WAV.scan(progress_callback=on_progress, rt_prices=_rtw,
                             intraday=bool(_rtw), all_combos=True)
            results = _WAV.to_scan_rows(_wav)          # 默认口径(低/收)那组, 老路径不动
            _m4_bulk(results)   # v2.261: A股适配 (本模式自己不算)
            SCAN_STATE['pool_size'] = _wav.get('n_total') or len(results)
            SCAN_STATE['total'] = SCAN_STATE['pool_size']
            # 六口径全量行数据 (每行都过一遍 _summarize_result, 与 /api/scan/result 同构)
            _wc = {}
            for _k, _rows in (_WAV.to_scan_rows_all(_wav) or {}).items():
                _wc[_k] = {'rows': [QuantHandler._summarize_result(None, r) for r in _rows]}
            SCAN_STATE['wave_combos'] = _wc
            SCAN_STATE['wave_info'] = {
                # ★ 'mode' 不能省: 前端 renderWaveBar() 靠 info.mode 判断「有没有内容」,
                #   少了这个键状态条永远 display:none (且不报错) —— 与 auction_info 同款约定
                'mode': 'wave',
                'date': _wav.get('date'), 'note': _wav.get('note'),
                'n_total': _wav.get('n_total'), 'n_hit': _wav.get('n_hit'),
                'funnel': _wav.get('funnel'), 'elapsed': _wav.get('elapsed'),
                # v2.271: 六口径一览 (不含行数据, 只给状态条按钮用)
                'combo_default': _wav.get('combo_default'),
                'combos': _WAV.combo_summary(_wav),
            }
        # <<< WAVE END
        elif mode == 'dragon':
            results = engine.scan_dragon(progress_callback=on_progress)
        elif pool == 'auto' and mode in ('100', '55', '20'):
            # v2.31: 自建池 — 同花顺条件全市场建池 (与8801一致, 不读导出文件)
            engine.ind_map, engine.con_map = load_industry_concept_maps()
            engine.pool = build_pool_conds(mode, progress_callback=on_progress)
            _check_scan_id(my_id)
            for code, info in engine.pool.items():
                num = code[2:] if code.startswith(('SZ', 'SH')) else code
                info['sector'] = engine.ind_map.get(num, '')
                info['concept'] = engine.con_map.get(num, '')
            n = len(engine.pool)
            SCAN_STATE['pool_size'] = n
            SCAN_STATE['total'] = n
            notify_sse('progress', {'progress': 0, 'total': n, 'msg': f'自建池 {n} 只 (同花顺条件全市场)'})
            results = engine.scan(mode=mode, progress_callback=on_progress)
        else:
            results = engine.scan(mode=mode, progress_callback=on_progress)
        _check_scan_id(my_id)  # 写结果前终检 (旧线程结果不得覆盖新扫描)
        SCAN_STATE['results'] = results
        SCAN_STATE['scan_time'] = datetime.now().strftime('%m-%d %H:%M:%S')
        SCAN_STATE['done'] = True
        SCAN_STATE['running'] = False
        notify_sse('done', {'count': len(results), 'scan_time': SCAN_STATE['scan_time']})
        _cache_scan_slot(mode, pool, lines, results)  # v2.53: 结果落盘 (单击/双击读槽免重扫)
        # v2.323 (2026-11-15 用户 m22464): 「把最新的这次扫出来的全票全部信息都记录归档…
        #   累积一定数量的记录后帮我看看什么指标最有用」。写的是**列表里的全部票、全部字段**,
        #   不是只记主推/备选 —— 竞价数据只有 09:25~09:30 那 5 分钟是真的, 过了永久丢失。
        #   只归档竞价打板: 其它模式随时可重扫, 没有这个不可重放的特性。
        try:
            if mode == 'auction':
                _arc.save_scan(mode, pool, lines,
                               [QuantHandler._summarize_result(None, r) for r in results[:500]],
                               info=SCAN_STATE.get('auction_info'),
                               extra={'pool_size': SCAN_STATE.get('pool_size'),
                                      'total': SCAN_STATE.get('total')})
        except Exception as _ae:
            _swlog('scan_archive', _ae)

    except _ScanCancelled:
        return  # v2.23: 被替换的旧扫描 — 静默退出, 不污染新扫描状态/推送
    except Exception as e:
        import traceback
        if SCAN_STATE.get('scan_id') != my_id:
            return  # 过期线程的异常不得写入新扫描
        SCAN_STATE['error'] = str(e)
        SCAN_STATE['running'] = False
        SCAN_STATE['done'] = True
        notify_sse('error', {'msg': str(e)})


# ============================================================
# 行情工具
# ============================================================
def _in_session_window():
    """纯钟点: 周一至周五 9:30-11:30 / 13:00-15:00。**不含节假日**。

    v2.300 (2026-11-13 用户 m15464): 模拟交易已提前到 9:25(见 `_in_auction_window`),
      但**本函数仍从 9:30 起** —— 它是全局"盘中"口径, 还被 `get_market_indices()` 的 TTL、
      `get_realtime_prices_cached()`(非盘中返 None 走收盘价)、RSRS 缓存共用。
      提前到 9:25 会让这些模块在竞价阶段就开始拿撮合价, 副作用面太大, 而用户只要求改模拟交易。

    🔴 必须是**无依赖的叶子函数** —— `get_market_indices()` 的 TTL 要问它。那条路一旦
      改问 `is_trading_time()`, 就会走到 `is_holiday()` → 读指数缓存/取数 → **成环**
      (节假日判定反过来又依赖指数缓存), 页面一开就递归爆栈。
    """
    now = datetime.now()
    if now.weekday() >= 5: return False
    t = now.hour * 100 + now.minute
    return (930 <= t <= 1130) or (1300 <= t <= 1500)


def _in_auction_window():
    """纯钟点: 周一至周五 9:25-9:30 —— 集合竞价已撮出开盘价、可下单的那 5 分钟。

    v2.300 (2026-11-13 用户 m15464): "把模拟交易的交易时间改到9:25开始, 模拟集合竞价
      结束后以开盘价买入"。只给模拟交易的闸门(`_sim_trade_gate`)用, **不进全局 is_trading**。
    同样是**无依赖叶子函数**, 不碰 is_holiday。
    """
    now = datetime.now()
    if now.weekday() >= 5: return False
    t = now.hour * 100 + now.minute
    return 925 <= t < 930


def is_trading_time():
    """正在开市 = 钟点窗口内 **且** 今天确实开了市
    v2.118 (2026-09-25): 补节假日闸 —— 原先只看星期几, 中秋节的星期五被判成盘中
    v2.300: **不含 9:25~9:30 竞价** —— 本函数是全局"盘中"口径(指数 TTL / 实时价缓存 /
      RSRS 缓存都用它), 竞价阶段提前返回 True 会让这些模块拿撮合价。模拟交易单独放宽,
      见 `sim_tradable_now()`。"""
    return _in_session_window() and not ds.is_holiday()


def sim_tradable_now():
    """模拟交易能不能下单 —— 在 is_trading_time() 基础上**多放 9:25~9:30 竞价**。

    v2.300 (2026-11-13 用户 m15464): "交易时间改到9:25开始, 集合竞价结束后以开盘价买入"。
    只给模拟盘用: `_sim_trade_gate()` 与 `/api/status` 的 `sim_tradable` 字段。
    """
    if _in_session_window():          # 9:30~11:30 / 13:00~15:00
        return not ds.is_holiday()
    if _in_auction_window():          # 9:25~9:30 竞价
        return not ds.is_holiday()
    return False


def in_auction_window():
    """当前是否处在 9:25~9:30 竞价下单窗口(且今天开市) —— 买入按开盘价成交。"""
    return _in_auction_window() and not ds.is_holiday()

# ═══ v2.351 (2026-11-20 用户 m35797/m35885): 一键更新的两个新零件 ═══════════════
#   为什么不在文件顶部 import zipfile/tempfile/shutil:
#   那两个只给"没有 git 时的 zip 兜底"用, 绝大多数更新走不到, 顶层的 import 成本与
#   与其他模块的耦合都没必要。函数内 import 是 Python 官方认可的写法。

def _up_zip_guard():
    """zip 兜底前的版本护栏 —— 返回 {'ok':True} 表示"远端确实更新，可以覆盖"。

    为什么必须比版本：zip 覆盖是按文件内容逐字节替换，它不知道谁新谁旧。
    自用版（没有 .git）通常比 GitHub 上那份还新（GitHub 是自己推上去的快照），
    不比版本就覆盖 = 把本机较新的代码**降级**成远端旧版，而且没有任何提示。
    """
    try:
        local = _ver.current()
    except Exception:
        local = ''
    remote = ''
    try:
        _ri, _ie = _ver.remote_info_http()
        remote = (_ri or {}).get('latest') or ''
    except Exception:
        remote = ''
    cmp = ''
    try:
        cmp = _ver.compare(local, remote) if (local and remote) else ''
    except Exception:
        cmp = ''
    if not remote:
        return {'ok': False, 'changed': False, 'how': 'zip', 'before': '', 'after': '',
                'version': local,
                'msg': '查不到远端版本（网络/代理问题）—— 为避免把本机较新的代码覆盖成旧版，'
                       '本次不执行 zip 覆盖。请检查网络后重试。'}
    # ⚠ compare(local, remote) 的返回值是**远端视角**：
    #   'newer' = 远端比我新（可以覆盖） / 'older' = 我比远端新（绝不能覆盖） / 'same'
    if cmp != 'newer':
        if cmp == 'older':
            _why = ('本机 %s 比远端 %s 还新 —— 不做 zip 覆盖'
                    '（按文件逐字节覆盖会把本机代码降级成远端旧版）' % (local, remote))
        elif cmp == 'same':
            _why = '已经是最新的（本机 %s / 远端 %s）—— 无需 zip 覆盖' % (local, remote)
        else:
            _why = '版本无法比较（本机 %s / 远端 %s）—— 不做 zip 覆盖' % (local, remote)
        return {'ok': True, 'changed': False, 'how': 'zip', 'before': '', 'after': '',
                'version': local, 'msg': _why}
    return {'ok': True}


def _up_rt_specs(paths):
    """把 dirty 列表收成 git pathspec —— 用目录前缀覆盖同一目录下的整批文件。

    `git status --porcelain` 会把 5,590 个 kline_data/daily/*.csv 逐行列出来,
    直接把 5,590 个路径塞进 stash 命令行会超 Windows 32K 参数上限。
    收到两级目录即可: kline_data/daily/SH600000.csv → kline_data/daily
    """
    out, seen = [], set()
    for p in (paths or []):
        pp = str(p or '').replace('\\', '/').strip()
        if not pp:
            continue
        parts = [x for x in pp.split('/') if x]
        if not parts:
            continue
        spec = '/'.join(parts[:2]) if len(parts) > 2 else '/'.join(parts)
        if spec not in seen:
            seen.add(spec)
            out.append(spec)
    return out[:80]          # 再兜一层, 免得极端情况下命令行过长


def _up_zip_update(base, why=''):
    """没有可用 git 时的兜底更新: 下载 GitHub 的 main.zip, **只覆盖代码文件**。

    只动这些后缀: .py .html .js .vbs .ico .css (+ 根目录 VERSION)
    绝不动:
      quant_terminal/data_sources.json  —— 里面是真 Key(被 git 跟踪, 覆盖就把 Key 冲了)
      quant_terminal/concept_blocklist.txt / watchlist.json —— 用户自己维护的配置
      kline_data/ data_archive/ dragon_data/ 选股池/ —— 全部行情与归档数据
      会话状态.md / *.bat —— 用户侧文档与启动脚本

    被覆盖的原文件先整套备份到 <base>/_bak_zip_<时间戳>/ 下(保留相对路径),
    万一新代码有问题, 用户能自己拷回去。
    """
    import zipfile
    import tempfile

    t0 = time.time()
    url = 'https://codeload.github.com/%s/zip/refs/heads/main' % _ver.GH_REPO
    try:
        raw, err = _ver._http_get(url, timeout=180.0, tries=1)
    except Exception as _e:
        return {'ok': False, 'changed': False, 'how': 'zip', 'before': '', 'after': '',
                'msg': '下载更新包失败（%s）：%s' % (why or '无 git', _e)}
    if not raw:
        return {'ok': False, 'changed': False, 'how': 'zip', 'before': '', 'after': '',
                'msg': '下载更新包失败（%s）：%s' % (why or '无 git', err or '空响应')}

    keep = ('.py', '.html', '.js', '.vbs', '.ico', '.css')
    stamp = time.strftime('%Y%m%d_%H%M%S')
    bak = os.path.join(base, '_bak_zip_%s' % stamp)
    n_same = n_write = n_skip = 0
    ver_new = ''
    wrote = []
    tmpd = tempfile.mkdtemp(prefix='bsking_up_')
    old = b''
    try:
        zf = zipfile.ZipFile(io.BytesIO(raw))
    except Exception as _e:
        return {'ok': False, 'changed': False, 'how': 'zip', 'before': '', 'after': '',
                'msg': '更新包解压失败：%s' % _e}
    try:
        names = zf.namelist()
        for n in names:
            nm = str(n or '').replace('\\', '/')
            if nm.endswith('/'):
                continue
            low = nm.lower()
            take = low.endswith(keep) or nm.upper().endswith('/VERSION') or nm.upper() == 'VERSION'
            if not take:
                continue
            parts = [x for x in nm.split('/') if x]
            if len(parts) < 2:
                continue
            rel = '/'.join(parts[1:])            # 去掉 "Open-BS-King-main/" 这一层
            if '/' in rel:
                if not rel.startswith('quant_terminal/'):
                    n_skip += 1
                    continue
            elif rel.upper() != 'VERSION':
                n_skip += 1
                continue
            if rel in ('quant_terminal/data_sources.json',):
                n_skip += 1
                continue
            try:
                data = zf.read(n)
            except Exception:
                n_skip += 1
                continue
            dst = os.path.join(base, rel.replace('/', os.sep))
            try:
                old = io.open(dst, 'rb').read()
            except Exception:
                old = b''
            if old == data:
                n_same += 1
                continue
            if old:
                bdst = os.path.join(bak, rel.replace('/', os.sep))
                bdir = os.path.dirname(bdst)
                if bdir and not os.path.isdir(bdir):
                    os.makedirs(bdir, exist_ok=True)
                io.open(bdst, 'wb').write(old)
            ddir = os.path.dirname(dst)
            if ddir and not os.path.isdir(ddir):
                os.makedirs(ddir, exist_ok=True)
            tmp = dst + '.uptmp'
            io.open(tmp, 'wb').write(data)
            os.replace(tmp, dst)
            n_write += 1
            wrote.append(rel)
            if rel.upper() == 'VERSION':
                ver_new = data.decode('utf-8', 'replace').strip()
    finally:
        try:
            zf.close()
        except Exception:
            pass
        try:
            shutil.rmtree(tmpd, ignore_errors=True)
        except Exception:
            pass

    secs = round(time.time() - t0, 1)
    if not n_write:
        return {'ok': True, 'changed': False, 'how': 'zip', 'before': '', 'after': '',
                'version': _ver.current(), 'n_write': 0, 'n_same': n_same,
                'msg': '已经是最新的（zip 兜底：%d 个代码文件内容一致，用时 %s 秒）'
                       % (n_same, secs)}
    head = ', '.join(wrote[:6]) + (' …' if len(wrote) > 6 else '')
    return {'ok': True, 'changed': True, 'how': 'zip', 'before': '', 'after': '',
            'version': ver_new or _ver.current(), 'n_write': n_write, 'n_same': n_same,
            'backup': bak, 'files': wrote[:40],
            'msg': '已通过 zip 覆盖更新 %d 个代码文件（%s，用时 %s 秒）：%s —— 需要重启终端生效；'
                   '原文件备份在 %s'
                   % (n_write, why or '无 git', secs, head, bak)}


def _do_git_pull():
    """v2.351 (2026-11-20 用户 m35797/m35885): 一键更新 —— 三分类护栏 + merge-tree 预检 + zip 兜底。

    为什么不再"有任何未提交改动就整体拒绝"（v2.340 的老逻辑）：
      程序每天跑完都会改 kline_data/*.csv 与几个缓存 json，只要用过一次终端，
      `git status` 就永远不干净 ⇒ 更新功能永久失效（测试机实测 18 个脏文件全是这类）。

    现在的分类（判据见 version.classify_dirty）：
      runtime —— 程序自己生成的行情/缓存/日志 ⇒ **自动 stash 后继续 pull**，
                 pull 完再 pop 回来；pop 冲突就以远端为准并明确告知
                 （那些数据下次「数据更新」会重新生成，不会真的丢）
      config  —— 用户填过 Key 的配置（data_sources.json）⇒ 拒绝，并列出文件
      source  —— 手改过的 .py/.html/.js ⇒ 拒绝，并附 merge-tree 三方预演结果，
                 提前说清"会不会冲突"，而不是等 git pull 报一行天书
      other   —— 其余保守拒绝

    没有可用 git（没装 Git for Windows / 拿的是 zip 解压版）⇒ 走 zip 兜底：
      只覆盖 quant_terminal/ 下的代码与根 VERSION，
      data_sources.json 与全部数据目录一个字节都不动。

    返回 dict:
      ok / changed / before / after / version / msg / how(git|zip) / 可选 stash / merge
    """
    import subprocess

    base = _ver.BASE_DIR
    gexe, ghow = '', ''
    try:
        gexe, ghow = _ver.find_git()
    except Exception:
        gexe, ghow = '', ''
    isrepo = os.path.isdir(os.path.join(base, '.git'))

    if (not gexe) or (not isrepo):
        why = ('本机找不到 git 命令' if not gexe else '这不是 git 副本（没有 .git 目录）')
        # 🔴 v2.351e: zip 覆盖**必须先比版本**。
        #   自用版就没有 .git（不是 git 副本），而它通常比远端还新（远端是自己推上去的快照）
        #   ⇒ 不比版本就覆盖 = 把本机较新的代码降级成远端旧版，且没有任何提示。
        guard = _up_zip_guard()
        # 🔴 v2.351f: 判据必须是"明确允许覆盖" —— 护栏在"本机 >= 远端"时返回的
        #   正是 ok=True/changed=False，只判 ok 会把这条当成放行，照样覆盖本机。
        if (not guard.get('ok')) or (guard.get('changed') is False):
            return guard
        r = _up_zip_update(base, why)
        r.setdefault('before', '')
        r.setdefault('after', '')
        return r

    def _git(*args, timeout=180):
        # 🔴 v2.342 (2026-11-18 用户 m26567): 必须带 CREATE_NO_WINDOW ——
        #   服务是 pythonw.exe 起的、自身无控制台，subprocess 起 git.exe 时 Windows
        #   会给新进程配一个终端窗口，屏幕上就是「黑窗一闪」。与 version.py 同一处根因。
        # v2.351: 可执行文件不再写死 'git', 改走 find_git() 的结果（PATH→安装目录→便携版）。
        return subprocess.run([gexe, '-C', base] + list(args),
                              capture_output=True, text=True,
                              encoding='utf-8', errors='replace',
                              timeout=timeout,
                              creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))

    def _refuse(msg, **kw):
        d = {'ok': False, 'changed': False, 'how': 'git', 'git': ghow,
             'before': kw.pop('before', ''), 'after': kw.pop('before', ''), 'msg': msg}
        d.update(kw)
        return d

    try:
        before = (_git('rev-parse', '--short', 'HEAD').stdout or '').strip()

        bk, bkerr = ({'runtime': [], 'config': [], 'source': [], 'other': [], 'n': 0}, '')
        try:
            bk, bkerr = _ver.dirty_breakdown()
        except Exception as _e:
            bkerr = str(_e)
        if bkerr:
            return _refuse('读取 git 状态失败：%s' % bkerr, before=before)

        # ── ① config / source / other 一律拒绝（这些文件的内容是"人的决定"）──
        if bk.get('config'):
            return _refuse(
                '本地有 %d 个配置文件被改过（%s）—— 里面可能填了 API Key，'
                '自动更新会覆盖掉，已拒绝。请先手动备份/还原这些文件再更新。'
                % (len(bk['config']), '、'.join(bk['config'][:5])),
                before=before, dirty=bk)

        if bk.get('source') or bk.get('other'):
            mc = {}
            try:
                mc = _ver.merge_check()
            except Exception as _e:
                mc = {'ok': False, 'err': str(_e)}
            names = (bk.get('source') or []) + (bk.get('other') or [])
            tip = ''
            if mc.get('ok'):
                ov = mc.get('overlap') or []
                cf = mc.get('conflicts') or []
                if cf:
                    tip = ('；三方预演：远端也改了 %d 个你改过的文件，其中 %d 个会**文本冲突**（%s）。'
                           '直接 pull 必炸，得手工合并。'
                           % (len(ov), len(cf), '、'.join(cf[:4])))
                elif ov:
                    tip = '；三方预演：远端也改了 %d 个你改过的文件（%s），但不同区域，可以自动合并。' % (
                        len(ov), '、'.join(ov[:4]))
                else:
                    tip = '；三方预演：远端没有碰过你改的文件，冲突风险很低。'
            elif mc.get('err'):
                tip = '；三方预演没跑成（%s）' % str(mc.get('err'))[:120]
            return _refuse(
                '本地有 %d 个代码/其他文件被改过（%s）—— 强拉会静默冲掉它们，已拒绝%s。'
                '请先 `推送数据.bat` 把你认可改动提交，或把这些文件还原后再更新。'
                % (len(names), '、'.join(names[:6]), tip),
                before=before, dirty=bk, merge=mc)

        # ── ② runtime 类：自动 stash → pull → pop ──
        specs = _up_rt_specs(bk.get('runtime') or [])
        stashed = False
        stash_msg = ''
        if specs:
            try:
                n0 = len([x for x in (_git('stash', 'list').stdout or '').splitlines() if x.strip()])
            except Exception:
                n0 = 0
            sr = _git('stash', 'push', '--include-untracked', '-m',
                      'bsking-auto-update %s' % time.strftime('%m-%d %H:%M:%S'), '--', *specs)
            try:
                n1 = len([x for x in (_git('stash', 'list').stdout or '').splitlines() if x.strip()])
            except Exception:
                n1 = n0
            # ⚠ 必须判"stash 条数真的变多了" —— `git stash push` 在没东西可存时
            #   也返回 0, 这时候去 pop 会弹出**别人的旧 stash**, 是个很脏的 bug。
            stashed = (n1 > n0)
            if not stashed:
                stash_msg = '（运行时文件没有实际改动，无需暂存）'

        r = _git('pull', '--ff-only', 'origin', 'main')
        pulled_ok = (r.returncode == 0)

        restored = ''
        if stashed:
            pr = _git('stash', 'pop', timeout=120)
            if pr.returncode == 0:
                restored = '；本地运行时数据已还原'
            else:
                # pop 冲突（同一批 csv 两边都追加了行）⇒ 以远端为准
                try:
                    _git('checkout', 'HEAD', '--', *specs, timeout=120)
                except Exception:
                    pass
                try:
                    _git('stash', 'drop', timeout=60)
                except Exception:
                    pass
                restored = '；本地运行时数据与远端冲突，已按远端为准（下次「数据更新」会重新生成）'

        if not pulled_ok:
            if stashed:
                # pull 失败也要把用户的数据还回去
                try:
                    _git('stash', 'pop', timeout=120)
                except Exception:
                    pass
            return _refuse('git pull 失败：' + ((r.stderr or r.stdout or '').strip()[:600]),
                           before=before)

        after = (_git('rev-parse', '--short', 'HEAD').stdout or '').strip()
        try:
            _ver.code_state(force=True)
        except Exception:
            pass
        after_ver = _ver.current()
        changed = (before != after)

        if changed:
            msg = '已从 %s 更新到 %s（当前版本 %s）—— 需要重启终端生效' % (before, after, after_ver)
        else:
            msg = '已经是最新的（%s / %s）' % (after, after_ver)
        if bk.get('runtime'):
            msg += '；自动暂存并处理了 %d 个运行时文件%s%s' % (
                len(bk['runtime']), stash_msg, restored)
        return {'ok': True, 'changed': changed, 'how': 'git', 'git': ghow,
                'before': before, 'after': after, 'version': after_ver, 'msg': msg,
                'dirty': bk}
    except subprocess.TimeoutExpired:
        return {'ok': False, 'changed': False, 'how': 'git',
                'msg': 'git pull 超时（180 秒），请检查网络或梯子', 'before': ''}
    except FileNotFoundError:
        r = _up_zip_update(base, 'git 可执行文件消失了')
        r.setdefault('before', '')
        r.setdefault('after', '')
        return r
    except Exception as e:
        return {'ok': False, 'changed': False, 'how': 'git', 'before': '',
                'msg': '更新时出错：%s' % e}


def get_trading_status():
    now = datetime.now()
    if now.weekday() >= 5: return 'WEEKEND'
    if ds.is_holiday(): return 'HOLIDAY'      # v2.118: 第 7 态 (工作日但休市)
    t = now.hour * 100 + now.minute
    if t < 925: return 'PRE_MARKET'
    if t < 930: return 'AUCTION'              # v2.300: 9:25~9:30 集合竞价已出开盘价, 可下单
    if 930 <= t <= 1130: return 'MORNING'
    if t < 1300: return 'NOON_BREAK'
    if 1300 <= t <= 1500: return 'AFTERNOON'
    return 'POST_MARKET'


def fetch_sina(codes):
    """实时行情统一入口 (v1 数据源设置 2026-08-29)
    原新浪直连 → datasource.fetch_realtime: 主源失败自动降级备源,
    解析器按源切换 (sina/tencent), 返回结构不变, 调用点零改动"""
    if not codes: return {}
    batch = ds.get('online', 'realtime', {}).get('batch_size', 50)
    return ds.fetch_realtime(list(codes[:batch]), batch_size=batch)


# ---- v2.236 (2026-10-29 用户 m11003): 银行避险情绪浮窗 ----
# 为什么要新建取数: 同花顺的指数接口**没有分时** (/prices/minute|intraday|timeline|tick 全 404,
#   /prices/historical 的 interval 只支持 1d), 腾讯分钟接口也只认个股 —— 板块指数喂进去直接报参数错。
#   唯一可用的是东财板块分时 (trends2)。口径已验: 90.BK1283「银行」对本地 TI881155、
#   90.BK1611「国有大型银行Ⅲ」对本地 TI884249, 最近 19 个交易日涨跌幅**平均差 0.000pp、最大 0.000pp**,
#   两条线的时间轴也逐格一致 (09:30~11:30 + 13:01~15:00, 241 点)。
_BANK_MIN = {'ts': 0.0, 'd': None}
_EM_TRENDS = ('https://push2his.eastmoney.com/api/qt/stock/trends2/get?secid=%s'
              '&fields1=f1,f2,f3,f4,f5,f6,f7,f8'
              '&fields2=f51,f52,f53,f54,f55,f56,f57,f58&iscr=0&ndays=1')

# v2.238 (2026-10-29 晚): 东财 push2his 被风控 —— 连 curl 都是 http=000 / RemoteDisconnected,
#   换 host、换 klt、换完整浏览器头、10 次重试全 0 成功, 只有根路径 404 (域名通、路径被 RST)。
#   同花顺指数接口也没有分时 (minute/intraday/timeline/tick 全 404, historical 的 interval 只认 1d)。
# ⇒ 改成**用成分股分钟线等权合成板块指数分时**。这不是权宜之计, 而是本来就同源:
#   同花顺板块指数 = 成分股涨跌幅等权平均 (8/8 精确吻合, 见 sector_kline.py 文件头)。
#   实测 2026-10-29 收盘: 881155 银行 42 只合成 1.9815% vs 官方 1.981511%;
#   884249 国有大型银行 6 只合成 2.3055% vs 官方 2.305484%;
#   开/高/低/收 四项差 ≤ 0.035pp。数据源是腾讯个股分钟线 (web.ifzq.gtimg.cn), 6 只 0.25s / 42 只 0.88s。
_BANK_IDX = (('bank', '881155.TI', '银行'), ('big', '884249.TI', '国有大型银行'))


def _tx_pfx(code):
    """6 位裸码 → 腾讯前缀。与 fuyao.to_thscode / 日线 CSV 前缀规则完全一致。"""
    c = str(code)
    if c[:3] == '920' or c[0] in '48':
        return 'bj'
    if c[0] in '69':
        return 'sh'
    return 'sz'


def _tx_minute(code, timeout=8):
    """腾讯个股当日分钟线 → ([[HHMM, 价], ...], 昨收)。只保留 15:00 及以前 (15:06+ 是盘后固定价交易)。"""
    sc = _tx_pfx(code) + str(code)
    # ⚠️ 必须用 ifzq.gtimg.cn, **不能**用 web.ifzq.gtimg.cn —— 后者在高频请求下直接返
    #   `HTTP 501 Not Implemented` (实测 42 只并发时 31 只 501, 换成不带 web 的 42/42 全通,
    #   0.9 秒)。两个域名同一套接口、同一份数据, 只有限流策略不同。
    url = 'https://ifzq.gtimg.cn/appstock/app/minute/query?code=' + sc
    req = urllib.request.Request(url, headers={'Referer': 'https://gu.qq.com/',
                                               'User-Agent': 'Mozilla/5.0'})
    with _net.open_url(req, timeout=timeout) as _r:
        j = json.loads(_r.read().decode('utf-8'))
    dd = (j.get('data') or {}).get(sc) or {}
    arr = ((dd.get('data') or {}).get('data') or [])
    qt = (dd.get('qt') or {}).get(sc) or []
    prev = float(qt[4]) if len(qt) > 4 and qt[4] else 0.0
    rows = []
    for ln in arr:
        c = ln.split(' ')
        if len(c) < 2:
            continue
        if c[0] > '1500':
            continue
        try:
            rows.append([c[0], float(c[1])])
        except Exception:
            continue
    # 腾讯给的交易日是 'YYYYMMDD' —— 用它而不是墙钟日: 周末/节假日悬停时墙钟日是错的,
    #   会让用户以为看的是当天数据 (用户对日期口径非常敏感, 10-22/10-23 那次就是日期骗人)。
    _d = str((dd.get('data') or {}).get('date') or '')
    if len(_d) == 8 and _d.isdigit():
        _d = _d[:4] + '-' + _d[4:6] + '-' + _d[6:]
    else:
        _d = ''
    return rows, prev, _d


def _tx_composite(thscode, timeout=8, workers=12):
    """板块指数分时 = 成分股分钟涨跌幅的等权平均。

    → (times, pct_list, open_pct, high_pct, low_pct, last_pct, n_ok, n_all)
      pct_* 均为「相对各股昨收的等权平均涨跌幅」(%), 与同花顺官方指数同口径。
    任何一只拉不到就把它剔出当分钟的平均 (分母跟着变), 不拿 0 顶 —— 顶 0 会把曲线拽平。
    """
    from concurrent.futures import ThreadPoolExecutor
    from quant_terminal import sector_kline as _sk
    codes = (_sk.members(('industry', 'cn_concept')).get(thscode) or [])
    if not codes:
        raise Exception('成分股表里没有 ' + thscode)

    def _one(c):
        try:
            rows, prev, dt = _tx_minute(c, timeout=timeout)
            if not rows or prev <= 0:
                return None
            return (rows, prev, dt)
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=min(workers, len(codes))) as ex:
        got = [x for x in ex.map(_one, codes) if x]
    if not got:
        raise Exception('成分股分钟线全部失败: ' + thscode)
    # 时间轴取「有效只数最多」的那条做基准 (个别股票停牌会缺几分钟)
    base = got[0][0]
    best = 0
    for rows, _pc, _dt in got:
        cnt = 0
        idx = dict((t, p) for t, p in rows)
        for t, _ in base:
            if t in idx:
                cnt += 1
        if cnt > best:
            best, base = cnt, rows
    times = [t for t, _ in base]
    pct = []
    for j, t in enumerate(times):
        s = 0.0
        n = 0
        for rows, pc, _dt in got:
            if j < len(rows) and rows[j][0] == t:
                s += (rows[j][1] / pc - 1) * 100.0
                n += 1
        pct.append(round(s / n, 4) if n else None)
    vals = [x for x in pct if x is not None]
    if not vals:
        raise Exception('合成序列为空: ' + thscode)
    _dt = ''
    for _r, _p, _d in got:
        if _d:
            _dt = _d
            break
    return (times, pct, vals[0], max(vals), min(vals), vals[-1], len(got), len(codes), _dt)


def _em_trends(secid, timeout=8):
    """东财板块/指数当日分时 → (name, preClose, [[HH:MM, open, close, high, low], ...])

    v2.237e: 必须走 `_net.open_url` (先直连, 失败才回退系统代理)。
      ⚠️ 本机常挂全局 SSR 代理, 东财是**境内源** —— 实测走代理会
      `ProxyError: Unable to connect to proxy, RemoteDisconnected`,
      直连 0.4~0.9 秒就拿到。这跟 fuyao / 腾讯那几处是同一个坑。
    """
    req = urllib.request.Request(_EM_TRENDS % secid,
                                 headers={'Referer': 'https://quote.eastmoney.com/',
                                          'User-Agent': 'Mozilla/5.0'})
    with _net.open_url(req, timeout=timeout) as _r:
        j = json.loads(_r.read().decode('utf-8'))
    d = j.get('data') or {}
    rows = []
    for x in (d.get('trends') or []):
        c = x.split(',')
        if len(c) < 5:
            continue
        rows.append([c[0][11:16], float(c[1]), float(c[2]), float(c[3]), float(c[4])])
    return (d.get('name') or ''), float(d.get('preClose') or 0), rows


_BANK_FILE = str(BASE_DIR / 'kline_data' / 'bank_minute_cache.json')


def _bank_cache_read():
    """磁盘缓存 → dict|None。只在内存为空时读一次 (重启后第一次悬停不用等)。"""
    try:
        if not os.path.isfile(_BANK_FILE):
            return None
        d = json.load(io.open(_BANK_FILE, 'r', encoding='utf-8'))
        if not isinstance(d, dict) or not d.get('series'):
            return None
        return d
    except Exception as _e:
        _swlog('bank_cache_read', _e)
        return None


def _bank_cache_write(d):
    """原子写: 先写 .tmp 再 os.replace, 中断不会留下半个文件。"""
    try:
        os.makedirs(os.path.dirname(_BANK_FILE), exist_ok=True)
        tmp = _BANK_FILE + '.tmp'
        with io.open(tmp, 'w', encoding='utf-8', newline='\n') as f:
            json.dump(d, f, ensure_ascii=False)
        os.replace(tmp, _BANK_FILE)
    except Exception as _e:
        _swlog('bank_cache_write', _e)


# ============================================================
# v2.239（用户 m13487）：浮窗分时 —— 同花顺官方分时
# ============================================================
# 🔴 为什么是 d.10jqka.com.cn，不是同花顺 API 网关(fuyao.aicubes.cn)：
#   网关的 /prices/minute /intraday /timeline /tick 全部 404，/prices/historical 只认 interval='1d'
#   —— **它不提供分时**。而这个域名给的是同花顺网页图表用的官方原始分时：不需要 key/Referer，
#   一次 241 点约 0.45 秒，耗时与成分股数量**完全无关**（融资融券 3889 只也是 0.45 秒，因为不是合成的），
#   与官方快照开/高/低/收差 0.0000。⇒ 板块分时**不再需要**成分股等权合成，v2.238 的 _tx_composite 只当降级兜底。
# 代码格式：板块/概念/创科指数 '48_884176'，个股 'hs_600519'。顶部 7 个大盘指数走不了这条，仍用腾讯。
_THS_TIME_URL = 'https://d.10jqka.com.cn/v6/time/{code}/{kind}.js'
_THS_DAY_URL = 'https://d.10jqka.com.cn/v6/line/{code}/01/today.js'
_THS_UA = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
           'Referer': 'https://www.10jqka.com.cn/'}

# 内存 + 磁盘两级缓存。悬停是个高频动作, 但用户要的是「立刻有图」——
#   前端还有一层 localStorage (见 terminal.html 的 BANK_LS), 这里负责重启后也能秒回。
_MINI_MEM = {}                       # 'kind:code' -> {'ts': epoch, 'd': {...}}
_MINI_TTL = 20                       # 秒: 同一只票 20 秒内重复悬停不再打网络
_MINI_MAX = 1200                     # 内存条数上限, 防止长跑无限涨
# v2.349 (用户 m29038): 300 → 1200。悬停改成批量预取后, 一次整页就是 30 条,
#   300 条不到十分钟就被挤光, 被挤掉的又得重拉 —— 上限太小会让预取白干。
#   1200 条约 8.4 MB(memory), 对这个进程可以忽略。
_MINI_FILE = str(BASE_DIR / 'kline_data' / 'mini_minute_cache.json')
_MINI_KEEP = 86400                   # 磁盘留一天: 隔夜的旧分时没意义
# v2.349: 批量拉取计数 —— >0 时 mini_minute **不逐只写盘**(30 只 = 30 次全量 JSON 落盘),
#   由 mini_minute_batch 在整批结束时统一写一次。用列表而不是普通 int, 免得拆包赋值。
_MINI_DEFER = [0]
# v2.359c: 指数存档后台增量补的节流状态 {'day': 'YYYY-MM-DD', 'running': 0/1}
_IDX_SYNC = {}


def _ymd8(s):
    """'20261029' → '2026-10-29'; 认不出就原样返回。"""
    s = str(s or '').strip()
    if len(s) == 8 and s.isdigit():
        return s[:4] + '-' + s[4:6] + '-' + s[6:]
    return s


def _ths_jsonp(url, timeout=8, tries=2):
    """取 10jqka 的 JSONP 并剥壳 → 内层 dict (已取唯一 value)。失败抛异常。

    重试 2 次: 实测这条链路偶发 HTTP 502 (Bad Gateway, 2026-10-29 21:00 抓到一次)。
    父级对照线那次若赶上 502, 行业浮窗就只剩一条线 —— 多花 0.25 秒换稳定, 值。
    """
    last = None
    for _k in range(max(1, tries)):
        try:
            req = urllib.request.Request(url, headers=_THS_UA)
            with _net.open_url(req, timeout=timeout) as _r:
                txt = _r.read().decode('utf-8', 'ignore')
            i, j = txt.find('('), txt.rfind(')')
            if i < 0 or j <= i:
                raise ValueError('10jqka 返回不是 JSONP')
            d = json.loads(txt[i + 1:j])
            if not isinstance(d, dict) or not d:
                raise ValueError('10jqka 返回空')
            return list(d.values())[0]
        except Exception as _e:
            last = _e
            if _k + 1 < max(1, tries):
                time.sleep(0.25)
    raise last if last else ValueError('10jqka 取数失败')


def _ths_code(code):
    """'884176.TI' / '884176' → '48_884176'; '600519' → 'hs_600519'。"""
    c = str(code or '').strip().split('.')[0]
    if c[:3].lower() == 'hs_':
        return 'hs_' + c[3:]
    if len(c) == 6 and c.isdigit():
        # 88x = 同花顺行业/概念/统计/地域指数, 在 10jqka 里统一是 48_ 前缀
        if c[:3] in ('881', '882', '883', '884', '885', '886'):
            return '48_' + c
        return 'hs_' + c
    return 'hs_' + c[-6:]


def _ths_minute(code, kind='last', timeout=8):
    """同花顺官方分时 → dict(name, pre, date, times[], prices[])。

    code: '48_884176'(板块) / 'hs_600519'(个股)
    kind: 'last' —— 非交易时段/收盘后也返回**最近一个交易日**的完整分时(实测盘中同样 200)。
    ⚠ 只取时间与价格两个字段; 后面三段是累计成交额/均价/累计成交量, 这里用不上。
    ⚠ 必须剔除 >1500 的点: 个股会给到 1505~1530 的**盘后固定价交易**, 留着会把末价拖偏。
    """
    d = _ths_jsonp(_THS_TIME_URL.format(code=code, kind=kind), timeout=timeout)
    times, prices = [], []
    for seg in str(d.get('data') or '').split(';'):
        c = seg.split(',')
        if len(c) < 2:
            continue
        t = c[0].strip()
        if len(t) != 4 or not t.isdigit():
            continue
        if t > '1500':          # 盘后固定价交易不是连续竞价
            continue
        try:
            p = float(c[1])
        except (TypeError, ValueError):
            continue
        if p <= 0:
            continue
        times.append(t)
        prices.append(p)
    if not times:
        raise ValueError('10jqka 分时无有效点')
    return {'name': d.get('name') or '', 'pre': float(d.get('pre') or 0),
            'date': _ymd8(d.get('date') or ''), 'times': times, 'prices': prices}


def _line_of(key, m, code, name=None):
    """把一条分时序列包装成前端要的 line。

    开/高/低/现一律**从分时序列自己算** —— 实测与官方 today.js 的 7/8/9/11 逐项相等,
    这样一条线只要一个请求(1.8 秒变 0.9 秒), 不必再单独打一次 fast 快照。
    """
    pre = m['pre'] or 0.0
    pr = m['prices']
    pcts = [round((v / pre - 1) * 100, 4) if pre else 0.0 for v in pr]

    def _pc(v):
        return round((v / pre - 1) * 100, 2) if pre else 0.0

    return {'key': key, 'name': name or m['name'], 'code': code,
            'date': m.get('date') or '',
            'times': list(m['times']), 'prices': [round(v, 4) for v in pr],
            'pcts': pcts, 'pre': pre,
            'open_pct': _pc(pr[0]), 'high_pct': _pc(max(pr)),
            'low_pct': _pc(min(pr)), 'last_pct': _pc(pr[-1]),
            'n_ok': 1, 'n_all': 1, 'source': 'ths_official'}


def _sector_parent(tc):
    """884xxx 细分行业 → 它的上级 881xxx。返回 (上级名, 上级6位码); 取不到 ('', '')。

    ⚠ 别用 engine.load_industry_parent_map() —— 那是「**股票6位码** → 上级行业名」(5227 条),
      拿行业码去查永远查不到。行业→上级要走 industry_parent.csv, 即 load_industry_parent()
      → {细分行业名: 上级行业名}(257 条)。两步串起来: 行业码 →(sector_kline.names)→ 行业名
      →(industry_parent)→ 上级名 →(sector_kline.resolve)→ 上级行业码。
    """
    try:
        from quant_terminal import sector_kline as _sk
        from quant_terminal.engine import load_industry_parent as _lip
        _n = _sk.names()               # 键是 '884176.TI' 这种带后缀的, 裸码查不到
        _ent = _n.get(tc) or _n.get(tc + '.TI') or {}
        _nm = str(_ent.get('name') or '')
        if not _nm:
            return '', ''
        pn = str((_lip() or {}).get(_nm) or '')
        if not pn or pn == _nm:
            return '', ''
        _t = _sk.resolve(pn)
        if not _t:
            return pn, ''
        _p = _t.split('.')[0]
        return pn, ('' if _p == tc else _p)
    except Exception as _e:
        _swlog('mini_sector_parent', _e)
        return '', ''


def sector_minute(thscode, with_parent=True, timeout=8):
    """板块/概念分时 → (lines, date)。第二条线是它的上级行业(淡色对照)。

    上级行业只对 884xxx 细分行业有意义; 881xxx 本身就是上级、885/886 概念没有上级,
    这两种情况只返回一条线。取上级失败不影响主图 —— 用户要的主线永远先画。
    """
    _raw = str(thscode or '').strip()
    tc = _raw.split('.')[0]
    if not (len(tc) == 6 and tc.isdigit()):
        # v2.239: 也接受板块**名字** —— 前端 .sec-cell 上的 data-par 就是名字,
        #   让它自己先查一次代码纯属多一步, 而且前端没有 resolve 能力。
        from quant_terminal import sector_kline as _sk
        _r = _sk.resolve(_raw)
        if not _r:
            raise ValueError('板块未找到: %s' % _raw)
        tc = _r.split('.')[0]
    m = _ths_minute(_ths_code(tc), timeout=timeout)
    lines = [_line_of('main', m, tc)]
    date = m.get('date') or ''
    if with_parent:
        pn, ptc = _sector_parent(tc)
        if ptc and ptc != tc:
            try:
                pm = _ths_minute(_ths_code(ptc), timeout=timeout)
                lines.append(_line_of('parent', pm, ptc, name=pn))
            except Exception as _e:
                _swlog('mini_parent_minute', _e)
    return lines, date


def _ths_minute_points(code):
    """同花顺自编指数(48_ 前缀)分时 → /api/minute 要的 {points, prev_close, name, date}。

    v2.360 (用户 m38439): 腾讯 minute 不认 883957(同花顺全A), 那条路会抛
      'could not convert string to float: \'\''。这里把它转成和腾讯完全一样的形状,
      前端 fetchMinute 一行都不用改。
    points = [{'t':'HHMM','p':价,'v':增量,'cv':累计}] —— 同花顺分时第 5 段是累计成交量,
      增量要自己逐点相减(和 fetch_minute 口径一致)。
    """
    try:
        from quant_terminal import ths_index as _ths_i
        _m = _ths_i.minute(code)
        if not _m.get('ok'):
            return None
        _ts = _m.get('times') or []
        _ps = _m.get('prices') or []
        _points = []
        _prev = 0.0
        for _i in range(min(len(_ts), len(_ps))):
            _points.append({'t': str(_ts[_i]), 'p': float(_ps[_i]),
                            'v': 0.0, 'cv': 0.0})
        _d = str(_m.get('date') or '')
        _d = ('%s-%s-%s' % (_d[:4], _d[4:6], _d[6:])) if len(_d) == 8 else _d
        return {'points': _points, 'prev_close': float(_m.get('pre') or 0),
                'name': _m.get('name') or _idxr.name_of(code),
                'date': _d, 'source': 'ths_official'}
    except Exception as _e:
        _swlog('ths_minute_points', _e)
        return None


def _tx_line(sc, code, kind, timeout=8):
    """腾讯分时(个股 / 大盘指数) → (line, date)。复用 fetch_minute, 与分时图同一份实现。

    ⚠ 个股会给到 1505~1530 的**盘后固定价交易**(实测茅台 267 点), 必须截到 1500 ——
      同花顺那条路已经在 _ths_minute 里截过, 两边口径要一致, 否则「现价」会差一笔。
    """
    points, prev_close, name, mdate = fetch_minute(sc)
    points = [p for p in points if str(p.get('t') or '') <= '1500']
    if not points:
        raise ValueError('腾讯分时无有效点')
    times = [p['t'] for p in points]
    prices = [p['p'] for p in points]
    return _line_of('main', {'name': name, 'pre': prev_close,
                             'date': _ymd8(mdate), 'times': times, 'prices': prices},
                    code, name=name), _ymd8(mdate)


def _mini_key(kind, code):
    return '%s:%s' % (kind, code)


def _mini_disk_load():
    """重启后第一次悬停不该空等 —— 内存为空时从磁盘捞一遍(只捞一天内的)。"""
    try:
        if not os.path.isfile(_MINI_FILE):
            return
        d = json.load(io.open(_MINI_FILE, 'r', encoding='utf-8'))
        items = (d or {}).get('items') or {}
        now = time.time()
        for k, v in items.items():
            if not isinstance(v, dict):
                continue
            if now - float(v.get('ts') or 0) > _MINI_KEEP:
                continue
            _MINI_MEM.setdefault(k, v)
    except Exception as _e:
        _swlog('mini_disk_load', _e)


def _mini_disk_save():
    """原子写: 先 .tmp 再 os.replace, 进程被杀也不会留下半个 JSON。"""
    try:
        os.makedirs(os.path.dirname(_MINI_FILE), exist_ok=True)
        now = time.time()
        items = {}
        for k, v in _MINI_MEM.items():
            if not isinstance(v, dict):
                continue
            if now - float(v.get('ts') or 0) > _MINI_KEEP:
                continue
            items[k] = v
        if len(items) > _MINI_MAX:
            _keep = sorted(items.items(), key=lambda kv: -float(kv[1].get('ts') or 0))
            items = dict(_keep[:_MINI_MAX])
        tmp = _MINI_FILE + '.tmp'
        with io.open(tmp, 'w', encoding='utf-8', newline='\n') as f:
            json.dump({'updated': int(now), 'items': items}, f, ensure_ascii=False)
        os.replace(tmp, _MINI_FILE)
    except Exception as _e:
        _swlog('mini_disk_save', _e)


def mini_minute(kind, code, ttl=_MINI_TTL):
    """浮窗分时的**唯一入口** (v2.239)。

    kind: 'sector' 板块/概念指数(自动带上级对照线) | 'stock' 个股 | 'index' 大盘指数
    返回 {ok, kind, code, date, ts, source, url, stale, age, lines:[{...}]}

    缓存四层 (与 bank_minute 同款思路, 用户 m11176 明确要求「不许先空白」):
      ① ttl 内回内存 → ② 内存空就捞磁盘 → ③ 拉取失败**沿用旧数据**标 stale,
      绝不拿空结果覆盖好的 → ④ 成功才写内存 + 磁盘。
    """
    kind = str(kind or 'sector').strip().lower()
    code = str(code or '').strip()
    if not code:
        return {'ok': False, 'kind': kind, 'code': '', 'error': '缺少 code'}
    if kind not in ('sector', 'stock', 'index'):
        return {'ok': False, 'kind': kind, 'code': code, 'error': 'kind 只认 sector/stock/index'}

    k = _mini_key(kind, code)
    if kind == 'sector' and not (len(code.split('.')[0]) == 6 and code.split('.')[0].isdigit()):
        try:                                     # 名字 → 代码, 让缓存键与代码写法统一
            from quant_terminal import sector_kline as _sk2
            _r2 = _sk2.resolve(code)
            if _r2:
                k = _mini_key(kind, _r2.split('.')[0])
        except Exception:
            pass
    now = time.time()
    it = _MINI_MEM.get(k)
    if not _MINI_MEM:
        _mini_disk_load()
        it = _MINI_MEM.get(k)
    if it and now - float(it.get('ts') or 0) < ttl:
        d = dict(it['d'])
        d['age'] = int(now - float(it.get('ts') or 0))
        d['stale'] = False
        return d

    lines, date, src, url = [], '', '', ''
    err = ''
    try:
        if kind == 'sector':
            lines, date = sector_minute(code)
            src = '同花顺官方分时'
            url = _THS_TIME_URL.format(code=_ths_code(code), kind='last')
        elif kind == 'index':
            # v2.360 (用户 m38439): src='ths' 的指数(同花顺全A 883957)走同花顺分时 ——
            #   腾讯 minute 不认 883957, 走那条会抛 'could not convert string to float'。
            if _idxr.src_of(code) == 'ths':
                from quant_terminal import ths_index as _ths_i
                _mm = _ths_i.minute(code)
                if not _mm.get('ok'):
                    raise ValueError(_mm.get('error') or '同花顺分时取不到')
                _md = str(_mm.get('date') or '')
                _md = ('%s-%s-%s' % (_md[:4], _md[4:6], _md[6:])) if len(_md) == 8 else _md
                _l = _line_of('main', {'name': _mm.get('name') or _idxr.name_of(code),
                                       'pre': _mm.get('pre') or 0,
                                       'date': _md,
                                       'times': _mm.get('times') or [],
                                       'prices': _mm.get('prices') or []},
                              code, name=_mm.get('name') or _idxr.name_of(code))
                _l['source'] = 'ths_official'
                lines = [_l]
                date = _md
                src = '同花顺分时'
                url = 'https://d.10jqka.com.cn/v6/time/48_%s/last.js' % code
            else:
                # 其余大盘指数: 10jqka 不认 sh000001 这类代码, 走腾讯(与分时图同源同口径)
                _l, date = _tx_line(code if code.lower().startswith(('sh', 'sz', 'bj'))
                                    else ('sh' if code[:1] in '69' else 'sz') + code,
                                    code, kind)
                lines = [_l]
                src = '腾讯分时'
            _mt = ds.get('online', 'minute', {})
            url = str(_mt.get('url_primary')
                      or 'https://ifzq.gtimg.cn/appstock/app/minute/query?code={code}')
        else:  # stock
            _num = code[2:] if code[:2].lower() in ('sh', 'sz', 'bj') else code
            _num = _num.split('.')[0][-6:]
            if code[:3].lower() == '920' or _num[:1] in '48':
                _sc = 'bj' + _num
            elif _num[:1] in '69':
                _sc = 'sh' + _num
            else:
                _sc = 'sz' + _num
            try:
                _l, date = _tx_line(_sc, _num, kind)
                lines = [_l]
                src = '腾讯分时'
                _mt = ds.get('online', 'minute', {})
                url = str(_mt.get('url_primary')
                          or 'https://ifzq.gtimg.cn/appstock/app/minute/query?code={code}')
            except Exception as _e1:
                # 腾讯挂了退到同花顺官方分时(个股 hs_ 前缀)
                _swlog('mini_stock_tx', _e1)
                _m = _ths_minute(_ths_code(_num))
                lines = [_line_of('main', _m, _num)]
                date = _m.get('date') or ''
                src = '同花顺官方分时'
                url = _THS_TIME_URL.format(code=_ths_code(_num), kind='last')
    except Exception as _e:
        _swlog('mini_minute_' + kind, _e)
        err = str(_e)[:160]

    if not lines:
        if it and (it.get('d') or {}).get('lines'):
            old = dict(it['d'])
            old['ok'] = True
            old['stale'] = True
            old['age'] = int(max(0, now - float(it.get('ts') or 0)))
            old['err'] = err or '本次拉取失败, 显示的是上次的数据'
            return old
        return {'ok': False, 'kind': kind, 'code': code, 'stale': True,
                'error': err or '没取到分时数据', 'lines': []}

    out = {'ok': True, 'kind': kind, 'code': code,
           'date': date or time.strftime('%Y-%m-%d'),
           'ts': time.strftime('%H:%M:%S'), 'source': src, 'url': url,
           'stale': False, 'age': 0, 'lines': lines}
    _MINI_MEM[k] = {'ts': now, 'd': out}
    if len(_MINI_MEM) > _MINI_MAX:
        _drop = sorted(_MINI_MEM.items(), key=lambda kv: float(kv[1].get('ts') or 0))
        for _dk, _dv in _drop[:len(_MINI_MEM) - _MINI_MAX]:
            _MINI_MEM.pop(_dk, None)
    if not _MINI_DEFER[0]:              # v2.349: 批量期间攒到整批结束再写
        _mini_disk_save()
    return out


def mini_minute_batch(items, ttl=_MINI_TTL, workers=8):
    """v2.349 (2026-11-20 用户 m29038): 一次拿多只浮窗分时。

    items: ['stock:600000', 'sector:半导体', 'index:sh000001'] —— **就是前端的缓存键**
    返回  {ok, items:{key: <mini_minute 的原样返回>}, n_ok, n_all, elapsed}

    内部**直接调 mini_minute** —— 四层缓存、失败沿用旧数据、脏数据不覆盖好的, 全走原路,
    不重写取数逻辑。单只失败只影响它自己(那一条塞 {'ok':False,...})。

    并发为什么是 8: 实测 16/24 并发并不比 8 快(上游腾讯限速), 8 既快又不把上游打疼。
    单批上限 60 —— 前端自己按 40 一批切, 这里再兜一道防止有人手拼超大请求。
    """
    t0 = time.time()
    out = {}
    keys = []
    for it in (items or []):
        s = str(it or '').strip()
        if not s or ':' not in s:
            continue
        _k, _c = s.split(':', 1)
        _k = _k.strip().lower()
        _c = _c.strip()
        if _k not in ('sector', 'stock', 'index') or not _c:
            out[s] = {'ok': False, 'error': 'kind 只认 sector/stock/index 或 code 为空',
                      'lines': []}
            continue
        if s not in keys:
            keys.append(s)
    keys = keys[:60]

    def _one(k):
        kind, code = k.split(':', 1)
        try:
            return k, mini_minute(kind, code, ttl=ttl)
        except Exception as _e:                      # 单只炸了不许拖垮整批
            _swlog('mini_batch_' + kind, _e)
            return k, {'ok': False, 'error': str(_e)[:160], 'lines': []}

    if keys:
        _MINI_DEFER[0] += 1
        try:
            # v2.349b: 本模块顶层没有 import ThreadPoolExecutor(只在 _tx_composite 里局部 import 过),
            #   不补这一行 ⇒ 批量接口恒返 {ok:false, error:'name ThreadPoolExecutor is not defined'},
            #   而 HTTP 还是 200、界面表现是「取数失败」—— 又一个静默坑。
            from concurrent.futures import ThreadPoolExecutor as _TPE2
            with _TPE2(max_workers=min(workers, len(keys))) as ex:
                for _k2, _d2 in ex.map(_one, keys):
                    out[_k2] = _d2
        finally:
            _MINI_DEFER[0] = max(0, _MINI_DEFER[0] - 1)
            if not _MINI_DEFER[0]:
                _mini_disk_save()
            # v2.349c: 失败项串行重试一次。
            #   实测 20 个板块一批 = 8 并发 × 2(主图+上级) = 16 个并发打同花顺, 偶发
            #   `HTTP Error 502: Bad Gateway`, 一次能坏 5/20 条 ⇒ 前端全显示「取数失败」。
            #   串行 + 间隔基本不再撞; 只有真失败才走这条路, 不拖慢正常批。
            _bad = [k for k in keys
                    if not (isinstance(out.get(k), dict) and out.get(k, {}).get('ok'))]
            for _k3 in _bad:
                time.sleep(0.12)
                try:
                    _k4, _d4 = _one(_k3)
                    if isinstance(_d4, dict) and _d4.get('ok'):
                        out[_k4] = _d4
                except Exception:
                    pass
    n_ok = sum(1 for _d in out.values() if isinstance(_d, dict) and _d.get('ok'))
    return {'ok': True, 'items': out, 'n_ok': n_ok,
            'n_all': len(out), 'elapsed': round(time.time() - t0, 3)}


def _mini_test():
    """v2.239 (2026-10-29): 「数据源设置 → 浮窗分时」那行的「测试」按钮打这里。
    真拉一次板块分时(884176 出版, 自带上级对照线), 报耗时与点数。
    datasource.test() 不认 mini_minute 这个 key —— 这条链路横跨两个源(同花顺 + 腾讯),
    不是 data_sources.json 里的一行, 所以只能单独测。"""
    _t0 = time.time()
    try:
        _r = mini_minute('sector', '884176', ttl=0)
        _dt = int((time.time() - _t0) * 1000)
        if not _r or not _r.get('ok'):
            return {'ok': False,
                    'detail': '取数失败 %dms %s' % (_dt, str((_r or {}).get('err') or ''))[:60]}
        _ls = _r.get('lines') or [{}]
        _n = len(_ls)
        _pts = len(_ls[0].get('pcts') or [])
        return {'ok': True, 'detail': '%d 条线 / %d 点 / %dms' % (_n, _pts, _dt)}
    except Exception as _e:
        return {'ok': False, 'detail': '%s: %s' % (type(_e).__name__, str(_e)[:80])}


def mini_minute_status():
    """给「数据源设置」窗口: 浮窗分时到底打哪个地址。🔒 不含任何 key。"""
    _mt = ds.get('online', 'minute', {})
    _tx = str(_mt.get('url_primary')
              or 'https://ifzq.gtimg.cn/appstock/app/minute/query?code={code}')
    return {'source': '同花顺官方分时 (板块/概念/个股) · 腾讯 (大盘指数)',
            'url_sector': _THS_TIME_URL.format(code='48_884176', kind='last'),
            'url_stock': _THS_TIME_URL.format(code='hs_600519', kind='last'),
            'url_index': _tx,
            'ttl': _MINI_TTL,
            'cached': len(_MINI_MEM),
            'cache_file': _MINI_FILE}


def bank_minute(ttl=20):
    """银行避险情绪浮窗数据 (v2.236, v2.237 加磁盘缓存)。

    两条线: 全银行指数 (BK1283, 42 只) / 国有大型银行 (BK1611, 6 只)。
    返回的是**相对各自昨收的涨跌幅**序列, 不是点位 —— 两条线点位差 800 点, 只有换成涨跌幅
    才画在同一根 Y 轴上可比 (用户要的就是「银行 vs 六大行谁更强」)。

    v2.237 缓存策略 (用户 m11176: 隔一段时间悬停不能先空白):
      ① 内存 → 磁盘 → 都没有才真去拉;
      ② ttl 内直接返回内存;
      ③ 拉取失败时**沿用旧数据**并标 stale, 绝不把好数据覆盖成空;
      ④ 返回 data_ts (数据本身的时间) 与 age 秒数, 前端据此显示「几秒前」。
    """
    now = time.time()

    # ① 内存缓存: ttl 内直接回
    if _BANK_MIN['d'] and now - _BANK_MIN['ts'] < ttl:
        _out = dict(_BANK_MIN['d'])
        _out['age'] = int(now - _BANK_MIN['ts'])
        _out['stale'] = False
        return _out

    # ② 内存空了 (刚重启) → 先捞磁盘的, 让前端马上有东西画
    if not _BANK_MIN['d']:
        _disk = _bank_cache_read()
        if _disk:
            _BANK_MIN['d'] = _disk
            _BANK_MIN['ts'] = now - 3600.0     # 标记为「旧」, 下面照常尝试刷新
            _BANK_MIN['disk'] = True

    out = {'ok': True, 'series': [], 'ts': time.strftime('%H:%M:%S'),
           'date': time.strftime('%Y-%m-%d')}
    _got = 0
    for key, thscode, label in _BANK_IDX:
        # v2.239: 改用**同花顺官方分时**。合成版(42 只等权平均)与官方差 0.01~0.03pp,
        #   官方版逐点全等, 而且 1 个请求 0.45 秒 vs 并发拉 42 个成分股 —— 没有理由不用官方。
        #   _tx_composite 保留作降级兜底 (同花顺整体挂了才走它)。
        _ln = None
        try:
            _m = _ths_minute(_ths_code(thscode))
            _ln = _line_of(key, _m, thscode.split('.')[0], name=label)
        except Exception as _e:
            _swlog('bank_minute_ths_' + key, _e)
        if not _ln:
            try:
                _ts, _pct, _op, _hi, _lo, _last, _nok, _nall, _td = _tx_composite(thscode)
                if _ts:
                    _ln = {'key': key, 'name': label, 'code': thscode.split('.')[0],
                           'date': _td or '', 'n_ok': _nok, 'n_all': _nall,
                           'open_pct': round(_op, 2), 'high_pct': round(_hi, 2),
                           'low_pct': round(_lo, 2), 'last_pct': round(_last, 2),
                           'times': _ts, 'pcts': _pct, 'source': 'tx_composite'}
            except Exception as _e:
                _swlog('bank_minute_tx_' + key, _e)
        if not _ln:
            continue
        _ln['thscode'] = thscode
        if _ln.get('date'):
            out['date'] = _ln['date']
        out['series'].append(_ln)
        _got += 1

    # ③ 一条都没拉到 → 沿用旧数据, 标 stale
    if not out['series']:
        if _BANK_MIN['d'] and _BANK_MIN['d'].get('series'):
            _old = dict(_BANK_MIN['d'])
            _old['ok'] = True
            _old['stale'] = True
            _old['age'] = int(max(0, now - _BANK_MIN['ts']))
            _old['err'] = '本次拉取失败, 显示的是上次的数据'
            return _old
        out['ok'] = False
        out['stale'] = True
        return out

    # ④ 成功: 写内存 + 写磁盘
    out['data_ts'] = out['ts']
    out['age'] = 0
    # v2.237d: 只拿到一条 (另一条接口挂了) 时, 若手上有完整的旧数据就别用残缺的覆盖它 ——
    #   宁可显示「上次的两条」并标 stale, 也不要让图里突然少一条线。
    if _got < 2 and _BANK_MIN['d'] and len(_BANK_MIN['d'].get('series') or []) >= 2:
        _old = dict(_BANK_MIN['d'])
        _old['ok'] = True
        _old['stale'] = True
        _old['age'] = int(max(0, now - _BANK_MIN['ts']))
        _old['err'] = '只拿到一条线, 显示的是上次完整的数据'
        return _old
    out['stale'] = False
    _BANK_MIN['d'] = out
    _BANK_MIN['ts'] = now
    _bank_cache_write(out)
    return out


def fetch_minute(sc):
    """当日分时 (腾讯分钟接口, 只覆盖**当天**)。

    → (points, prev_close, name, date); points = [{'t':'HHMM','p':价,'v':增量,'cv':累计}]
    全部候选源都失败 → 抛最后一个异常。

    🔴 **/api/minute 的 handler 与 AI 工具 get_intraday 共用这一个实现**。
       两处各写一份的话, 界面上看到的分时和 AI 说的分时早晚会对不上
       (取数铁律: 宁可复用现成的, 不另起炉灶)。
    """
    mt = ds.get('online', 'minute', {})
    candidates = [
        mt.get('url_primary', f'https://ifzq.gtimg.cn/appstock/app/minute/query?code={sc}').format(code=sc),
        mt.get('url_backup', f'https://ifzq.gtimg.cn/appstock/app/minute/query?code={sc}').format(code=sc),
    ]
    last_err = None
    for i, url in enumerate(candidates):
        try:
            resp = _req_get(url, timeout=6, headers={'Referer': 'https://gu.qq.com/'})
            d = resp.json()
            dd = d.get('data', {}).get(sc, {})
            arr = (dd.get('data') or {}).get('data', []) if isinstance(dd.get('data'), dict) else []
            if not arr and i < len(candidates) - 1:
                time.sleep(0.5); continue      # 空数据换下一个源
            qt = (dd.get('qt') or {}).get(sc, [])
            prev_close = float(qt[4]) if len(qt) > 4 and qt[4] else 0
            # v2.120: 兜底名从"裸 6 位 num"改成 sc —— 指数走这条时原先 num 根本没定义
            #   (NameError 被吞成"分时数据获取失败"); sc 两处都成立且无歧义。
            name = qt[1] if len(qt) > 1 else sc
            points = []
            prev_v = 0
            for line in arr:
                parts = line.split(' ')
                # v2.279 (用户 m10425): 北交所分时行只有 3 段 (`0930 43.85 599`, 无成交额段),
                #   沪市/深市是 4 段 (`0930 38.38 6044 23196871.94`)。原来卡 >=4 ⇒ 北交所
                #   整条分时被静默丢光 (接口有 242 根, 前端画不出来)。
                if len(parts) >= 3:
                    cv = int(float(parts[2]))
                    points.append({'t': parts[0], 'p': float(parts[1]),
                                   'v': cv - prev_v, 'cv': cv})
                    prev_v = cv
            return points, prev_close, name, (dd.get('data') or {}).get('date', '')
        except Exception as e:
            last_err = e
            time.sleep(0.5)
    raise last_err or Exception('all sources failed')


# ============================================================
# v2.116：AI 对话窗口的**工具执行器**
# ============================================================
# 模型点哪个工具这里就跑哪个，返回**给模型读的纯文本**（不是 JSON —— 它要能直接引用的数字，包一层结构反而读岔）。
# 🔒 取数一律复用终端现成函数（fetch_sina / load_csv_kline / _load_stock_names / analyze_sector_ma /
#   analyze_concept_ma）。另起炉灶的话「AI 说的」会和「终端显示的」对不上，那比查不到更糟。
# v2.119 起 11 个工具：行情/扫描/情绪/模拟盘/自选/基本面/分时/新闻都够得着。
# 🔴 **按「用户会怎么问」打包，不是一个端点一个工具** —— 每份 schema 都进每次请求上下文，工具太多模型反而挑错。
#   get_index 合并成 get_market（指数+情绪本是同一个问题）；get_sector 用 kind 参数分摊(仍是一个工具)。
_AI_TOOL_NAMES = ('search_stock', 'get_realtime', 'get_kline', 'get_sector',
                  'get_market', 'get_news', 'get_scan', 'get_sim',
                  'get_watchlist', 'get_stock_info', 'get_intraday')


def _ai_norm_code(code):
    """'600206' / 'sh600206' / 'SH600206' / '600206.SH' → ('SH600206', 'sh600206')
    非法 → (None, None)。⚠️ 9 开头归沪 (B股/老股) —— 与 _sim_std_code 同口径,
    两处不一致的话同一个代码在两个功能里会指向不同的票。"""
    s = str(code or '').strip().upper().split('.')[0]
    if s[:2] in ('SH', 'SZ', 'BJ'):
        s = s[2:]
    s = ''.join(ch for ch in s if ch.isdigit())
    if len(s) != 6:
        return None, None
    p = 'SH' if s[0] in ('6', '9') else 'SZ'
    return p + s, (p + s).lower()


# 句子兜底允许的"余文": 中文里跟在指数名后面的疑问/描述性字词。
# 🔴 **宁可漏认, 不可错认** —— 余文里只要有一个字不在这张表里, 就当"可能是另一个
#    指数"处理 (沪深300**成长** / 深证**红利** / 中证**全指** / 创业板**综** 都属此类),
#    落到"不确定, 终端只有这 7 个"的诚实路径。
# ⚠ **刻意不收**: 数字 0-9 与 成/长/价/值/红/利/全/综/等/基/本/面/低/波 —— 它们是
#   竞争指数名的构件。收错一个就等于把闸门拆了。
_AI_IDX_FILL = set(
    '是多少呢吗么了吧啊呀哦嘛怎如何样'
    '现在目前今天今日最近这两走势行情点位报价收盘开盘涨跌幅情况数据'
    '请问帮我查看一下个的了的把丶'
    '？?！!，,。.、；;：: 　+-~～'
)


def _ai_idx_fill(t):
    """余文是不是"纯疑问/描述填充词"。空串算通过。"""
    return all(ch in _AI_IDX_FILL for ch in t)


def _ai_norm_index(s):
    """'上证指数' / '上证' / 'sh000001' / '000001.SH' → ('上证指数', 'sh000001')
    不是指数 → (None, None)。

    🔴 **必须先于 _ai_norm_code 判定**。_ai_norm_code 是给个股写的: 它剥掉 sh/sz 前缀后
    **按首位数字重新判所** ('0' → SZ) ⇒ 'sh000001' 被解析成 **sz000001 = 平安银行**,
    上证指数被静默答成 11.30 元 —— 而且不报错。个股那条路早有同款防御
    (见本文件指数无个股分析那段), AI 工具这条路 v2.116 漏了。
    ⚠️ 裸 '000001' **仍归个股**(平安银行) —— 指数必须带 sh/sz 前缀或写名字。这是终端
       既有口径, 别去"修"它: 000001 在深市确实是平安银行。
    """
    t = str(s or '').strip()
    if not t:
        return None, None
    low = t.lower().replace(' ', '')
    pre = ''
    if low[:2] in ('sh', 'sz'):
        pre, low = low[:2], low[2:]
    elif '.' in low:
        low, _, suf = low.partition('.')
        pre = suf if suf in ('sh', 'sz') else ''
    if pre:
        cand = pre + ''.join(ch for ch in low if ch.isdigit())
        return (INDEX_NAMES[cand], cand) if cand in INDEX_NAMES else (None, None)
    if low in INDEX_ALIAS:
        sc = INDEX_ALIAS[low]
        return INDEX_NAMES[sc], sc
    # 长名优先 ('科创综指' 要赢过 '科创'), 整句也兜住 ("上证指数是多少")
    # 🔴 闸门在**余文**上: 别名后面剩下的那截必须整段是疑问填充词。
    #    v2.117 初版没这道闸, get_index("中证1000") 静默答成了**中证500**;
    #    补丁二只卡了数字, 又漏掉 沪深300成长 / 深证红利 / 中证全指 / 创业板综。
    #    全是同一个病: 不报错的错答案。
    #    ⚠ 前缀不管 —— 前缀改不了这个别名指的是哪个指数。
    for k in sorted(INDEX_ALIAS, key=len, reverse=True):
        i = low.find(k)
        if i < 0:
            continue
        tail = low[i + len(k):]
        if tail.endswith('指数'):
            tail = tail[:-2]        # '沪深300指数' 是合法写法, 剥掉再判
        if not _ai_idx_fill(tail):
            continue
        sc = INDEX_ALIAS[k]
        return INDEX_NAMES[sc], sc
    # 裸 6 位代码 (399001 / 399006 / 399905): 只有指数, 没有同名个股 —— 认。
    # ⚠ 靠 INDEX_NAMES 兜底: sz000001(平安银行) / sh600206 都不在表里, 不会误吞个股。
    _f, _c = _ai_norm_code(t)
    if _c in INDEX_NAMES:
        return INDEX_NAMES[_c], _c
    return None, None


def _ai_ma(closes, periods=(5, 10, 21, 55, 120, 250)):
    """as-of 最后一根的均线。数据不足的周期**不出键** —— 拿 0 或拿短窗口冒充,
    会让模型读出"跌破MA250"这种反向结论。"""
    out = {}
    for n in periods:
        if len(closes) >= n:
            out[n] = round(sum(closes[-n:]) / n, 2)
    return out


def _ai_fmt_hand(v):
    v = int(v or 0)
    if v >= 100000000:
        return '%.2f亿' % (v / 100000000.0)
    if v >= 10000:
        return '%.1f万' % (v / 10000.0)
    return str(v)


def _ai_vol_hand(rt):
    """实时成交量 → **手**。⚠️ 两个源的 volume 单位不同 (新浪=股, 腾讯=手),
    照抄会让模型算出差 100 倍的量比 —— 用 datasource 记的**实际源**判再换算。"""
    v = rt.get('volume') or 0
    if not v:
        return 0
    return int(v / 100) if ds.LAST_REALTIME_SOURCE.get('source') == 'sina' else int(v)


def _ai_code_names(codes, nm2code, cap):
    s = ', '.join('%s %s' % (c, nm2code.get(c, '')) for c in codes[:cap])
    if len(codes) > cap:
        s += ' ...(共 %d 只)' % len(codes)
    return s


def _ai_t_search(kw):
    kw = str(kw or '').strip()
    if not kw:
        return '关键词是空的。'
    low = kw.lower()
    names = _load_stock_names()
    # v2.117: 指数也要在这里露面 —— 否则模型压根不知道终端有指数,
    # 用户问「上证指数」时它只能回"没找到" (这正是用户报的那个问题)。
    idx = [(sc, nm) for sc, nm in INDEX_NAMES.items() if low in nm.lower()]
    _, isc = _ai_norm_index(kw)
    if isc and (isc, INDEX_NAMES[isc]) not in idx:
        idx.insert(0, (isc, INDEX_NAMES[isc]))
    if low.isdigit():
        hits = [(c, n) for c, n in names if c.startswith(low)]
    else:
        hits = [(c, n) for c, n in names if low in n.lower()]
        hits.sort(key=lambda t: (0 if t[1].lower().startswith(low) else 1, t[0]))
    blocks = []
    if idx:
        blocks.append('指数匹配「%s」:\n%s\n'
                      '(这些是**指数**不是个股, 取行情要用 get_index)'
                      % (kw, '\n'.join('%s %s' % (sc, nm) for sc, nm in idx[:6])))
    if hits:
        body = '\n'.join('%s %s' % (c, n) for c, n in hits[:12])
        tail = '' if len(hits) <= 12 else '\n(共 %d 条, 只列前 12)' % len(hits)
        blocks.append('个股匹配「%s」:\n%s%s\n取用其中一只时, 把 6 位代码原样传下去。'
                      % (kw, body, tail))
    if not blocks:
        return ('没找到「%s」(个股和指数都没有)。换个简称再试, '
                '或者让用户直接给 6 位代码。' % kw)
    return '\n\n'.join(blocks)


def _ai_t_realtime(code):
    # v2.117: **先判指数** —— 见 _ai_norm_index 的注释
    # (不判的话 sh000001 会被当个股, 答成 sz000001 平安银行)
    inm, isc = _ai_norm_index(code)   # 裸代码 399001 也在里面兜了, 见该函数末尾
    if isc:
        return _ai_index_quote(isc, inm)
    full, sina = _ai_norm_code(code)
    if not full:
        return '「%s」不是合法的 6 位 A 股代码。' % code
    rt = fetch_sina([sina]).get(sina) or {}
    price = rt.get('price') or 0
    if price <= 0:
        return ('%s 没取到行情 (停牌 / 代码不存在 / 数据源不通, 三者之一)。'
                '如实告诉用户没查到, 不要猜价格。' % full)
    out = ['%s %s' % (full, rt.get('name') or full[2:]),
           '现价 %.2f   涨跌 %+.2f%%   昨收 %.2f'
           % (price, rt.get('change_pct') or 0, rt.get('prev_close') or 0),
           '今开 %.2f   最高 %.2f   最低 %.2f'
           % (rt.get('open') or 0, rt.get('high') or 0, rt.get('low') or 0)]
    vline = '成交量 %s 手' % _ai_fmt_hand(_ai_vol_hand(rt))
    if rt.get('amount_yi'):
        vline += '   成交额 %s 亿' % rt['amount_yi']
    out.append(vline)
    out.append('(实时快照, 源=%s, %s)'
               % (ds.LAST_REALTIME_SOURCE.get('source') or '?',
                  datetime.now().strftime('%m-%d %H:%M:%S')))
    return '\n'.join(out)


_AI_INDEX_ALL = ('', '全部', '所有', '指数', '大盘', '市场', 'a股', '概况', '总览')


def _ai_index_broad(kw):
    """笼统一问 (留空 / "大盘怎么样" / "今天市场") —— 该给全部指数, 而不是某一个。

    🔴 **同源, 不分叉**: get_index 与 get_market 必须走**这一个**判据。
       两处各写一份同义谓词, 改了一处忘另一处, 就会出现"问 A 答 B"且不报错
       (v2.117 中证1000 被静默答成中证500, 就是这么来的)。
    """
    lk = str(kw or '').strip().lower()
    return (not lk) or lk in _AI_INDEX_ALL or any(k in lk for k in _AI_INDEX_ALL if k)


def _ai_index_quote(sc, nm):
    """指数快照。⚠️ 指数**没有**个股那套"现价/成交量(手)"口径 —— 点位是"点"不是"元",
    所以只给点位/涨跌/成交额, 不搬 _ai_t_realtime 的成交量那两行。"""
    row = (get_market_indices() or {}).get(sc) or {}
    price = row.get('price') or 0
    if price <= 0:
        return '%s (%s) 没取到行情 —— 如实告诉用户没查到, 不要猜点位。' % (nm, sc)
    pc = row.get('prev_close') or 0
    out = ['%s (%s) —— 这是**指数**, 不是个股' % (nm, sc),
           '点位 %.2f   涨跌 %+.2f%%   涨跌 %+.2f 点   昨收 %.2f'
           % (price, row.get('change_pct') or 0, (price - pc) if pc else 0, pc),
           '今开 %.2f   最高 %.2f   最低 %.2f'
           % (row.get('open') or 0, row.get('high') or 0, row.get('low') or 0)]
    if row.get('amount_yi'):
        out.append('成交额 %s 亿' % row['amount_yi'])
    # ★ 标出这个点位**是哪天的**。非交易时段(收盘后/周末/节假日)拿到的就是最近一个
    #   交易日的收盘 —— 不标的话模型会说成"今天"。同「模拟盘结算取价必须带日期」的教训。
    try:
        from quant_terminal.engine import get_index_bars
        _b = get_index_bars(sc, need=None, with_live=False)
        d = _b[-1]['date'] if _b else ''
    except Exception:
        d = ''
    if d:
        out.append('★ 该指数日K最后一根 = %s%s'
                   % (d, '' if d == datetime.now().strftime('%Y-%m-%d')
                      else ' (**不是今天** —— 非交易时段或指数日K尚未落盘, '
                           '别说成"今天"或"此刻")'))
    out.append('(实时快照, 源=%s, %s)'
               % (ds.LAST_REALTIME_SOURCE.get('source') or '?',
                  datetime.now().strftime('%m-%d %H:%M:%S')))
    return '\n'.join(out)


def _ai_t_index(name):
    kw = str(name or '').strip()
    nm, sc = _ai_norm_index(kw)
    if sc:
        return _ai_index_quote(sc, nm)
    # 笼统一问 (留空 / "大盘怎么样" / "今天市场") → 一次给全 7 个, 别逼模型先猜名字。
    # 🔴 **必须排在具体匹配之后**: '上证指数' 里也含 '指数', 顺序反了就把
    #    具体问题答成概况。判据见 _ai_index_broad (与 get_market 共用同一个)。
    if _ai_index_broad(kw):
        rows = get_market_indices() or {}
        lines = ['终端跟踪的 %d 个 A 股指数 (实时):' % len(INDEX_NAMES) if not kw else
                 '终端跟踪的 %d 个 A 股指数 (实时) —— 「%s」是笼统问法, 下面是全部:'
                 % (len(INDEX_NAMES), kw)]
        for sc2, nm2 in INDEX_NAMES.items():
            r = rows.get(sc2) or {}
            if r.get('price'):
                lines.append('  %-9s %-6s %10.2f  %+6.2f%%'
                             % (sc2, nm2, r['price'], r.get('change_pct') or 0))
        lines.append('(要某一个的详细数据, 再调 get_market 并传它的名字; '
                     '要看走势就调 get_kline)')
        return '\n'.join(lines)
    return ('终端**没有**「%s」这个指数。终端只有这 %d 个: %s。\n'
            '如果用户问的是个股, 请改用 get_realtime / search_stock —— '
            '**不要拿某个指数冒充它**。'
            % (kw, len(INDEX_NAMES), ', '.join(INDEX_NAMES.values())))


def _ai_index_kline(sc, nm, bars):
    """指数日K —— 形状照个股那套(收盘/均线/多头排列/最近N根), 但数据来自
    kline_data/index_daily, **不走个股 CSV**。不输出成交量: 指数成交量与个股不是一个口径。"""
    try:
        from quant_terminal.engine import get_index_bars
        k = get_index_bars(sc, need=None, with_live=False)
    except Exception as e:
        return '指数 %s 日K读取失败 (%s)。' % (sc, type(e).__name__)
    if not k:
        return '指数 %s 没有本地日K (kline_data/index_daily 缺失)。' % sc
    try:
        nb = int(bars)
    except (TypeError, ValueError):
        nb = 60
    nb = max(20, min(250, nb))
    last = k[-1]
    mas = _ai_ma([r['close'] for r in k])
    head = ['%s (%s) 指数日K  共 %d 根, 最后一根 = %s'
            % (nm, sc, len(k), last['date']),
            '收盘 %.2f' % last['close']]
    if len(k) >= 2 and k[-2]['close']:
        head[1] += '   较前一日 %+.2f%%' % ((last['close'] / k[-2]['close'] - 1) * 100)
    if mas:
        head.append('均线(截至 %s): %s'
                    % (last['date'],
                       '  '.join('MA%d=%.2f' % (n, mas[n]) for n in sorted(mas))))
        seq = [last['close']] + [mas[n] for n in (5, 10, 21, 55) if n in mas]
        if len(seq) == 5:
            head.append('多头排列(收>MA5>MA10>MA21>MA55): %s'
                        % ('是' if all(seq[i] > seq[i + 1] for i in range(4)) else '否'))
    rows = ['', '最近 %d 根 (日期 / 收盘 / 涨跌%%):' % nb,
            '日期          收盘      涨跌%']
    base = k[-(nb + 1):]
    for i in range(1, len(base)):
        r, pv = base[i], base[i - 1]['close']
        rows.append('%s  %8.2f  %+7.2f%%'
                    % (r['date'], r['close'], (r['close'] / pv - 1) * 100 if pv else 0))
    return '\n'.join(head + rows)


def _ai_t_kline(code, bars):
    # v2.117: 指数走本地指数日K (kline_data/index_daily)
    inm, isc = _ai_norm_index(code)   # 裸代码 399001 也在里面兜了, 见该函数末尾
    if isc:
        return _ai_index_kline(isc, inm, bars)
    full, sina = _ai_norm_code(code)
    if not full:
        return '「%s」不是合法的 6 位 A 股代码。' % code
    k = load_csv_kline(full)
    if not k:
        return '%s 没有本地日K数据 (CSV 缺失)。' % full
    try:
        nb = int(bars)
    except (TypeError, ValueError):
        nb = 60
    nb = max(20, min(250, nb))
    last = k[-1]
    mas = _ai_ma([r['close'] for r in k])
    head = ['%s 日K  共 %d 根, 最后一根 = %s (本地CSV口径)'
            % (full, len(k), last['date']),
            '收盘 %.2f' % last['close']]
    if len(k) >= 2 and k[-2]['close']:
        head[1] += '   较前一日 %+.2f%%' % ((last['close'] / k[-2]['close'] - 1) * 100)
    if mas:
        head.append('均线(截至 %s): %s'
                    % (last['date'],
                       '  '.join('MA%d=%.2f' % (n, mas[n]) for n in sorted(mas))))
        seq = [last['close']] + [mas[n] for n in (5, 10, 21, 55) if n in mas]
        if len(seq) == 5:
            head.append('多头排列(收>MA5>MA10>MA21>MA55): %s'
                        % ('是' if all(seq[i] > seq[i + 1] for i in range(4)) else '否'))
    # 盘中补一行实时价 —— 但**明说它没进均线**, 否则模型会拿"到昨收为止的均线"
    # 去和"今天的价"比, 得出假的突破/跌破结论。
    if _is_trade_window():
        px = (fetch_sina([sina]).get(sina) or {}).get('price') or 0
        if px:
            head.append('★ 实时价 %.2f —— 上面均线**不含它**(口径到 %s 收盘), '
                        '盘中判断以实时价为准。' % (px, last['date']))
    rows = ['', '最近 %d 根 (日期 / 收盘 / 涨跌%% / 成交量):' % nb,
            '日期          收盘      涨跌%      成交量']
    base = k[-(nb + 1):]
    for i in range(1, len(base)):
        r, pv = base[i], base[i - 1]['close']
        rows.append('%s  %8.2f  %+7.2f%%  %s'
                    % (r['date'], r['close'],
                       (r['close'] / pv - 1) * 100 if pv else 0, _ai_fmt_hand(r['volume'])))
    return '\n'.join(head + rows)


def _ai_name_key(t):
    """板块名匹配的排序键: (名字长度, 名字)。

    短的排前面 —— 问「半导体」时「半导体材料」比「半导体设备产业链」更接近原词。
    同名同长不可能出现, 所以这个键是全序 ⇒ 结果跨进程、跨机器完全一致。
    """
    return (len(t[1]), t[1])


def _ai_pick_sector(kw, ind_map, con_map):
    """名字 → (种类, 标准名, 候选)。精确 > 前缀 > 包含。"""
    # 🔴 **必须先排序**: 原先是 `set(...)` 直接建池, 而 set 的迭代顺序随
    #    PYTHONHASHSEED 变, 匹配又是"第一个命中的赢" ⇒ 同一个「半导体」在两次
    #    进程里会命中不同板块 (实测 10 次: 4 次半导体材料 / 6 次半导体设备)。
    #    同一句话得到两个不同的板块, 且两个数都"看着对" —— 最难查的一类错。
    #    排序键 (名字长度, 名字): 短名更接近原词; 同长按字典序 ⇒ 跨进程稳定。
    #    ⚠️ 行业块整体仍排在概念块前面 (保持原有的"行业优先"语义)。
    pool = sorted((('行业', v) for v in set(ind_map.values()) if v),
                  key=_ai_name_key) + \
           sorted((('概念', v) for v in set(con_map.values()) if v),
                  key=_ai_name_key)
    for test in (lambda n: n == kw, lambda n: n.startswith(kw), lambda n: kw in n):
        hit = [t for t in pool if test(t[1])]
        if hit:
            return hit[0][0], hit[0][1], hit[:8]
    return None, None, []


def _ai_sector_rows(kind, mp):
    """跑板块均线全表。档位/实时价口径与 /api/sector_ma **逐字一致** ——
    不一致就命不中面板预热好的缓存, 每次调用白算几秒。"""
    rt = get_realtime_prices_cached()
    if not _is_trade_window():
        rt = None                      # v2.14 盘后: None 才命中 'close' 预热缓存
    lines = LINES_MAP.get('4', (5, 10, 21, 55))
    fn = analyze_sector_ma if kind == '行业' else analyze_concept_ma
    return fn(mp, lines, realtime_prices=rt)


def _ai_sector_trend(kw):
    """视角一 (默认): 均线多头状态 + 成分股。与终端「行业/概念趋势」面板同源。"""
    kw = str(kw or '').strip()
    if not kw:
        return '板块名是空的。'
    ind_map, con_map = load_industry_concept_maps()
    kind, sec, cands = _ai_pick_sector(kw, ind_map, con_map)
    if not sec:
        # 没匹配上 → 顺手递几个当前最强的行业名, 下一轮它就能报对 (比让它瞎试强)
        top = ', '.join('%s(%s %d%%)' % (r.get('name'), r.get('status'),
                                         int(r.get('bullish_pct') or 0))
                        for r in _ai_sector_rows('行业', ind_map)[:8])
        return ('没有叫「%s」的行业或概念。当前最强的几个行业是: %s\n'
                '请从中挑一个准确名字重新调用, 或者直接问用户。' % (kw, top))
    rows = _ai_sector_rows(kind, ind_map if kind == '行业' else con_map)
    hit = next((r for r in rows if r.get('name') == sec), None)
    if hit is None:
        return '%s「%s」这次没算出结果 (成分股K线不足或未更新)。' % (kind, sec)
    nm2code = dict(_load_stock_names())
    out = ['%s「%s」  %s   多头占比 %d%% (%d/%d)'
           % (kind, sec, hit.get('status') or '',
              int(hit.get('bullish_pct') or 0),
              int(hit.get('bullish_count') or 0), int(hit.get('total') or 0))]
    if hit.get('formed_date'):
        out[0] += '   多头成立日 %s%s' % (hit['formed_date'],
                                       ' (新)' if hit.get('is_new') else '')
    if len(cands) > 1:
        out.append('(同名近似: %s)'
                   % ', '.join('%s %s' % (k, v) for k, v in cands if v != sec))
    bull = hit.get('bullish_stocks') or []
    drill = hit.get('drill_stocks') or []
    if bull:
        out.append('多头排列成分股 %d 只: %s' % (len(bull), _ai_code_names(bull, nm2code, 20)))
    if drill:
        out.append('站上MA5的成分股 %d 只: %s' % (len(drill), _ai_code_names(drill, nm2code, 25)))
    out.append('(口径: 板块指数=成分股涨幅等权合成, 4线档 5/10/21/55, 与终端「行业趋势」面板同源)')
    return '\n'.join(out)


def _ai_t_news(kw='', limit=12):
    """搜终端**已经拉下来的**新闻池 (财联社 + 同花顺, ~200 条)。

    ⚠️ 这**不是联网搜索** —— 只能看到终端新闻窗那份池子 (滚动覆盖约一两天)。
       答不上来时**如实说池子里没有**, 别让模型以为"世界上没有这条消息"。
    ⚠️ 池子空就顺手拉一次 (用户可能从没开过新闻窗) —— 两个源都是匿名公开接口, 无 key。
    """
    kw = str(kw or '').strip()
    try:
        limit = max(1, min(30, int(limit)))
    except Exception:
        limit = 12
    r = get_news()                      # 缓存空/过期时它自己会拉
    items = r.get('items') or []
    if not items:
        return ('新闻源这次没取到数据 (%s)。**如实告诉用户你没查到**, 别编。'
                % (r.get('warn') or '两个源都没返回'))

    # ── 跨源去重 ────────────────────────────────────────────────────────
    # 财联社和同花顺常把同一条报两遍。不去重的话模型会把同一条消息当两件事念两遍
    # (实测: 5 条里 3 条是乌克兰, 实为 1 条)。
    # ⚠️ 只在**本工具内**去重, 不动 news 窗口 —— 那边用户可能就想看两个源各怎么说。
    def _key(t):
        return re.sub(r'[^0-9a-z\u4e00-\u9fff]+', '', (t or '').lower())

    kept, keys = [], []
    for it in items:
        k = _key(it.get('title'))
        dup = False
        if k:
            for k2 in keys:
                # 完全相同, 或"短的被长的包含"(泽连斯基：… ⊂ 乌克兰总统泽连斯基：…)
                # ⚠️ 卡 10 字下限: 短标题互相包含太容易误判成重复
                if k == k2 or (len(k) >= 10 and len(k2) >= 10 and (k in k2 or k2 in k)):
                    dup = True
                    break
        if not dup:
            if k:
                keys.append(k)
            kept.append(it)

    low = [t.lower() for t in re.split(r'[\s\u3000,，、;；/|]+', kw) if t]

    def _hay(it, with_body=True):
        s = it.get('title') or ''
        s += ' ' + ' '.join('%s %s' % (k.get('n') or '', k.get('c') or '')
                            for k in (it.get('ks') or []))
        if with_body:
            s += ' ' + (ai_news.body_of(it.get('id')) or '')
        return s.lower()

    hits = []
    for it in kept:
        n = sum(1 for t in low if t in _hay(it))
        if n:
            hits.append((n, it))
    hits.sort(key=lambda x: (-x[0], -x[1]['t']))

    loose = False
    if low and not hits:
        # 整词 0 命中 ⇒ 拆成 2 字窗口再搜一遍。
        #   为什么需要: 用户问"科技板块有什么消息", 模型会把**整句话**当关键词传下来,
        #   而新闻标题里没人写"科技板块"四个字 —— 实测就是这么 0 命中的。
        #   ⚠️ 这一轮**只搜标题 + 个股, 不翻正文** —— 正文长, 2 字词几乎必然命中, 那是噪声。
        #   ⚠️ 2 字以内不发散 (grams 只剩它自己), 否则单字词会把池子整个捞回来。
        grams = set(t[i:i + 2] for t in low for i in range(len(t) - 1))
        if len(grams) > 1:
            for it in kept:
                n = sum(1 for g in grams if g in _hay(it, with_body=False))
                if n:
                    hits.append((n, it))
            hits.sort(key=lambda x: (-x[0], -x[1]['t']))
            loose = bool(hits)

    picked = [it for _, it in (hits or [(0, x) for x in kept])][:limit]

    head = '新闻池 %d 条' % len(items)
    if len(kept) != len(items):
        head += ' (跨源去重后 %d)' % len(kept)
    head += ' (财联社+同花顺, 缓存于 %s)' % time.strftime(
        '%m-%d %H:%M', time.localtime(r.get('ts') or 0))
    if kw:
        head += ' · 关键词「%s」%s命中 %d 条' % (kw, '宽松' if loose else '', len(hits))
    out = [head]
    for i, it in enumerate(picked, 1):
        lv = {2: ' ★重要', 1: ' ·置顶'}.get(it.get('lvl'), '')
        out.append('%d. [%s %s%s] %s'
                   % (i, time.strftime('%m-%d %H:%M', time.localtime(it.get('t') or 0)),
                      '财联社' if it.get('src') == 'cls' else '同花顺', lv,
                      it.get('title') or ''))
        ks = it.get('ks') or []
        if ks:
            out.append('   个股: ' + ', '.join('%s(%s)' % (k.get('n'), k.get('c'))
                                              for k in ks[:8]))
        b = ai_news.body_of(it.get('id')) or ''
        if b:
            out.append('   正文: ' + re.sub(r'\s+', ' ', b)[:200])
    if kw and not hits:
        out.append('⚠️ 没搜到「%s」, 上面给的是池子里最新的几条。**如实告诉用户没搜到**, '
                   '并说明这里只有终端已拉取的最新约 200 条 (约一两天), **不是全网搜索**。'
                   '可以换个**更短的词**(2~4 字, 如「半导体」「算力」「降准」)再调一次。' % kw)
    elif loose:
        out.append('⚠️ 「%s」整词没搜到, 上面是**拆成 2 字词宽松匹配**的结果, 可能只是部分相关。'
                   '**如实说明这是宽松匹配**, 别当成精确命中。' % kw)
    else:
        out.append('(口径: 只搜终端已拉取的约 200 条, **不是联网搜索**; 更早的题目这里没有。)')
    return '\n'.join(out)


# ════════════════════════════════════════════════════════════════════════
# v2.120 (2026-09-25): 新增的 6 个工具 + 板块三视角
#
# 🔒 一条铁律贯穿下面每一个函数: **面板上显示什么, 这里就说什么**。
#    取数一律复用终端现成函数与现成口径, **不在这层算新指标** ——
#    AI 说的和顶栏/面板显示的对不上, 比查不到更糟。
# ════════════════════════════════════════════════════════════════════════

# ⚠️ 这张表是 terminal.html 里那份模式标签的**服务端副本** (那边模式按钮/批量跑/
#    结果表头各写了一遍)。改前端标签时这里要一起改, 否则 AI 说的模式名和界面对不上。
_AI_SCAN_LABELS = {'100': '趋势主升', '55': '趋势延续', '20': '趋势反转',
                   'ai': '涨停突破', 'smart': '涨停回调', 'dragon': '龙头战法', 'glue': '均线粘合'}
# >>> REBOUND BEGIN
# 超跌反弹: 服务端标签副本 (前端 terminal.html 的 labels 表要同步改名)
_AI_SCAN_LABELS['rebound'] = '超跌反弹'
# <<< REBOUND END
# >>> WAVE BEGIN
# 波段低吸: 服务端标签副本 (前端 terminal.html 的 labels 表要同步改名)
_AI_SCAN_LABELS['wave'] = '波段低吸'
# <<< WAVE END


def _ai_market_sentiment():
    """市场情绪 + 量能 —— 与终端顶栏「情绪 / 成交预测 / 量能」**同一个数据源**。

    🔴 **只搬原样存在的字段, 不在这层算新指标**。
    ⚠️ 口径照抄, 别望文生义:
       breadth  = **上涨板块占全部板块的百分比** (不是涨跌家数, 面板上不显示它)
       vol_ratio / vol_status = 今日成交额对 5 日均量的分档 (显著放量…无量)
       turnover.fcst_delta_yi = 预计全天成交额**较昨日**的增量 (不是绝对水位)
       tier     = 沪/深 大盘是否「上升趋势」—— 用户框架里决定该市场能否开新仓
    """
    try:
        s = MarketSentiment().fetch() or {}
    except Exception as e:
        return '市场情绪这次没取到 (%s)。**如实说没查到**, 别猜。' % type(e).__name__
    if not s:
        return '市场情绪这次没取到。**如实说没查到**, 别猜。'
    out = ['【市场情绪】(与终端顶栏同源, 取数于 %s)'
           % datetime.now().strftime('%m-%d %H:%M:%S')]
    if s.get('score') is not None:
        out.append('情绪分 %s (%s)' % (s.get('score'), s.get('level') or '?'))
    if s.get('breadth') is not None:
        out.append('上涨板块占比 %.1f%%' % s['breadth'])
    if s.get('vol_status'):
        out.append('量能 %s%s' % (s['vol_status'],
                                (' (%.2f 倍)' % s['vol_ratio']) if s.get('vol_ratio') else ''))
    if s.get('vol_note'):
        out.append('  量能说明: %s' % s['vol_note'])
    tv = s.get('turnover') or {}
    if tv.get('mode') and tv.get('mode') != 'none':
        seg = []
        if tv.get('cur_yi') is not None:
            seg.append('当前 %.0f 亿' % tv['cur_yi'])
        if tv.get('fcst_delta_yi') is not None:
            seg.append('预计全天 %+d 亿 (较昨日)' % round(tv['fcst_delta_yi']))
        if tv.get('ma5_yi') is not None:
            seg.append('5 日均 %.0f 亿' % tv['ma5_yi'])
        if seg:
            out.append('成交额 (%s): %s' % (tv.get('caliber') or '?', '   '.join(seg)))
        if tv.get('asof'):
            _today = datetime.now().strftime('%Y-%m-%d')
            out.append('  ★ 数据日期 %s%s'
                       % (tv['asof'], '' if tv['asof'] == _today else
                          ' (**不是今天** —— 非交易时段看到的是最近一个交易日的, '
                          '别说成"今天")'))
    t = s.get('tier') or {}
    out.append('大盘趋势档: 上证 %s / 深证 %s   (上升趋势才允许开新仓)'
               % ('上升趋势' if t.get('sh') else '非上升趋势',
                  '上升趋势' if t.get('sz') else '非上升趋势'))
    reg = s.get('index_regimes') or {}
    # ⚠️ 只取 regime/adx/name —— icon 是 emoji (📈/📉), 零信息量还占注意力
    for _sc in ('sh000001', 'sz399001'):
        r = reg.get(_sc) or {}
        if r.get('regime'):
            out.append('  %s: %s  (ADX %.1f)'
                       % (r.get('name') or _sc, r['regime'], r.get('adx') or 0))
    out.append('(口径: 来自终端 MarketSentiment, 与顶栏显示的是同一次计算)')
    return '\n'.join(out)


def _rps_market_block():
    """v2.281: 给「板块轮动 AI 分析」组装大盘背景。

    只要四样: 指数涨跌 / 情绪分 / 涨停炸板跌停 / 两市成交额 —— 与 TSP 的
    `_build_market_block` 对齐。**每一项失败都降级, 不阻断分析**:
    AI 报告的第五节「结合大盘」数据缺了会自己说信号不足, 比整篇不给强。
    """
    out = {'index': [], 'emotion': {}, 'limit': {}, 'amount_yi': None}
    try:
        idx = get_market_indices() or {}
        for _sc in ('sh000001', 'sz399001', 'sz399006', 'sh000688'):
            r = idx.get(_sc) or {}
            if r.get('name') and r.get('change_pct') is not None:
                out['index'].append((r['name'], float(r['change_pct'])))
    except Exception as _e:
        _swlog('rps_mkt_index', _e)
    try:
        s = MarketSentiment().fetch() or {}
        out['emotion'] = {'score': s.get('score'), 'label': s.get('level') or ''}
        tv = s.get('turnover') or {}
        if tv.get('cur_yi'):
            out['amount_yi'] = float(tv['cur_yi'])
        elif s.get('total_amount_yi'):
            out['amount_yi'] = float(s['total_amount_yi'])
    except Exception as _e:
        _swlog('rps_mkt_senti', _e)
    try:
        # 涨停/炸板/跌停 —— 走同花顺涨停池 (与 /api/limit_pools 同源)。
        #   返回 (list, meta) 元组 —— 家数就是 len(list)。
        from quant_terminal import fuyao_limit as _fl2
        _pd = _fl2.default_pool_date()
        _up = _fl2.limit_up_pool(_pd)
        _up_n = len(_up[0]) if isinstance(_up, tuple) else len(_up or [])
        _bk = _fl2.limit_break_pool(_pd)
        _bk_n = len(_bk[0]) if isinstance(_bk, tuple) else len(_bk or [])
        _dn = _fl2.limit_down_pool(_pd)
        _dn_n = len(_dn[0]) if isinstance(_dn, tuple) else len(_dn or [])
        out['limit'] = {'up': _up_n, 'broken': _bk_n, 'down': _dn_n, 'max_boards': None}
        # 最高连板：涨停池里 boards 字段最大
        try:
            _rows = _up[0] if isinstance(_up, tuple) else (_up or [])
            _mb = 0
            for _r in _rows:
                _b = _r.get('boards') or _r.get('board_num') or 0
                try:
                    _b = int(_b)
                except (TypeError, ValueError):
                    _b = 0
                if _b > _mb:
                    _mb = _b
            if _mb:
                out['limit']['max_boards'] = _mb
        except Exception:
            pass
    except Exception as _e:
        _swlog('rps_mkt_limit', _e)
    return out


def _ai_t_market(name=''):
    """大盘 —— 指数快照 + 市场情绪/量能/成交额。

    两件事合成一个工具, 因为它们**回答的是同一个问题**。只说指数不说量能,
    那句"今天不错"经常是缩量假涨 (用户框架里量能是独立一档)。
    传具体指数名 → 只答那个指数 (单指数与全市场情绪无关, 不硬凑);
    留空 / "大盘" / "今天市场" → 全部指数 + 情绪面板。
    """
    kw = str(name or '').strip()
    _nm, sc = _ai_norm_index(kw)
    quote = _ai_t_index(kw)          # 只算一次, 别为拼接重跑一遍
    if sc or not _ai_index_broad(kw):
        return quote
    return quote + '\n\n' + _ai_market_sentiment()


def _ai_scan_find(d, mode):
    """在落盘缓存里找该模式**最新**的一个有结果的槽。
    → (pool, mode, lines, slot); 找不到 → (None, None, None, None)

    ⚠️ 不写死池名: 趋势三档两种池都可能扫过, 涨停突破/涨停回调/龙头战法恒自建股池。
       哪个池有结果就用哪个, 池名照实报给模型。
    """
    best = None
    for pool, modes in (d.get('slots') or {}).items():
        if not isinstance(modes, dict):
            continue
        lv = modes.get(mode)
        if not isinstance(lv, dict):
            continue
        for ln in SCAN_LINES:
            s = _slot_of(d, pool, mode, ln)
            if s and s.get('results'):
                ts = s.get('ts') or 0
                if best is None or ts > best[3]:
                    best = (pool, mode, ln, ts, s)
    return (best[0], best[1], best[2], best[4]) if best else (None, None, None, None)


def _ai_scan_line(mode, r, i):
    """一行扫描结果 → 一行文字。

    🔴 **按模式分派**: 各模式的列**不是一套** (见 QuantHandler._summarize_result) ——
       趋势三档是评分/左峰/距峰, 涨停突破是破峰档位/连板/炸板, 涨停回调是
       类型/板数/缩量比, 龙头战法只有评分。硬套同一张表就会把"距峰%"念成"止损"。
    """
    s = ['%2d. %-9s %-6s %8.2f %+6.2f%%'
         % (i, r.get('code') or '', str(r.get('name') or '')[:6],
            r.get('price') or 0, r.get('change_pct') or 0)]
    if mode == 'dragon' or r.get('group') in ('dragon', 'pullback'):
        s.append('评分 %s' % (r.get('score', r.get('cons_score'))))
        if r.get('lianban'):
            s.append('%d 连板' % r['lianban'])
        return '  '.join(s)
    if mode == 'ai':                          # 涨停突破
        s.append('%s破峰 %.2f 距峰 %+.1f%% 止损 %.2f'
                 % (r.get('peak_label') or '', r.get('left_peak') or 0,
                    r.get('dist_pct') or 0, r.get('stop_loss') or 0))
        if r.get('lianban'):
            s.append('%d 连板' % r['lianban'])
        if r.get('turnover_rate'):
            s.append('换手 %.1f%%' % r['turnover_rate'])
        if r.get('zhaban'):
            s.append('炸板 %d 次' % r['zhaban'])
    elif mode == 'smart':                     # 涨停回调
        # ⚠️ vol_ratio = 今日成交量占**前20日最高量**的百分比 (engine.py:2859),
        #    前端也是这么标的 ('今量=前20日最高量的 N%')。按字面念成"缩量比"数与义全错。
        s.append('%s %d板(%s) 距峰 %+.1f%% 今量为前20日最高量的 %.0f%% 止损 %.2f'
                 % (r.get('kind') or '', r.get('zt_cnt') or 0, r.get('zt_date') or '',
                    r.get('dist_pct') or 0, r.get('vol_ratio') or 0,
                    r.get('stop_loss') or 0))
    elif mode == 'glue':                      # 均线粘合
        # ⚠️ vol_ratio 在 glue 里 = 今量 ÷ **前5日均量** (倍数), 不是涨停回调那个"占前20日最高量%"
        s.append('粘合 %.2f%% (MA5-MA10 %.2f%% / MA10-MA21 %.2f%%) 距MA5 %+.2f%% 距MA21 %+.2f%% 量比 %.2f'
                 % (r.get('glue') or 0, r.get('g510') or 0, r.get('g1021') or 0,
                    r.get('dist_pct') or 0, r.get('p21') or 0,
                    r.get('vol_ratio') or 0))
    # >>> AUCTION BEGIN
    elif mode == 'auction':                   # 竞价打板
        # ⚠️ auct = 竞价涨幅 (今开 vs 昨收), 不是现价涨幅; ratio = 竞价额占昨日全天成交额%
        s.append('竞价 %+.2f%% 量比 %.2f 占比 %.2f%% 流通 %.1f亿 %s'
                 % (r.get('auct_pct') or 0, r.get('vol_ratio') or 0,
                    r.get('auct_ratio') or 0, r.get('mkt_cap') or 0,
                    r.get('verdict_tx') or ''))
    # <<< AUCTION END
    # >>> REBOUND BEGIN
    elif mode == 'rebound':                   # 超跌反弹
        # ⚠️ 这里 rebound 是"现价自近5日最低价反弹的百分比", 不是"反弹天数";
        #    near 是"近5日最低价距百日最低价", 负数=已破百低(变体③)
        s.append('反弹 %+.2f%% 距百低 %+.2f%% 探底%d日 距百高 %+.1f%% 5日低 %.2f 百低 %.2f %s'
                 % (r.get('rebound') or 0, r.get('near') or 0, r.get('days5') or 0,
                    r.get('drop5') or 0, r.get('low5') or 0, r.get('low100') or 0,
                    r.get('variant') or ''))
    # <<< REBOUND END
    # >>> WAVE BEGIN
    elif mode == 'wave':                      # 波段低吸 (v2.260 新口径)
        # wave 是"今日取价(pv: 阳线取最低价/阴线取收盘价)距趋势线的百分比", 正=在线上方, 越小越贴线
        #   span=AB跨度 / slope=斜率%/日 / pull=从A点(百日最低)涨了多少% / 峰值日=首次回踩前那个小山峰
        s.append('离线 %+.2f%% 自A涨 %+.1f%% 量比 %.2f 斜率 %.2f%%/日 跨度%d 峰%s %s->%s 首回踩%s'
                 % (r.get('wave') or 0, r.get('pull') or 0, r.get('vr') or 0,
                    r.get('slope') or 0, r.get('span') or 0,
                    r.get('peak_date') or '-', r.get('adate') or '', r.get('bdate') or '',
                    ((r.get('c_date') or '-') + ('~' + r.get('c_end_date') if r.get('c_end_date') and r.get('c_end_date') != r.get('c_date') else ''))))
    # <<< WAVE END
    else:                                     # 趋势主升 / 延续 / 反转
        s.append('评分 %s 左峰 %.2f 距峰 %+.1f%% 止损 %.2f'
                 % (r.get('cons_score'), r.get('left_peak') or 0,
                    r.get('dist_pct') or 0, r.get('stop_loss') or 0))
        if r.get('ind_t'):
            s.append('行业档%s' % r['ind_t'])
        if r.get('con_t'):
            s.append('概念档%s' % r['con_t'])
    if r.get('sector'):
        s.append('行业=%s%s' % (r['sector'], r.get('sector_status') or ''))
    return '  '.join(s)


def _ai_t_scan(mode='', top=20):
    """**上次扫描**的结果 (scan_cache.json 的落盘槽) —— 五框架那几张表。

    ⚠️ 这**不会触发扫描**: 扫一次几十秒, 还会给 SCAN_STATE 换代, 把用户正看着的
       结果顶掉。没有结果就如实说"还没扫过, 请在终端上双击一个模式按钮"。
    ⚠️ 扫描用的"今天" = 本地K线CSV的**最后一根** —— 盘前跑出来的其实是上一交易日。
    """
    try:
        top = max(5, min(50, int(top)))
    except Exception:
        top = 20
    kw = str(mode or '').strip().lower()
    if kw and kw not in SCAN_MODES:
        return ('没有「%s」这个扫描模式。可用: %s'
                % (mode, ' / '.join('%s=%s' % (m, _AI_SCAN_LABELS.get(m, m))
                                    for m in SCAN_MODES)))
    d = _read_scan_cache()
    pool = md = ln = slot = None
    if kw:
        pool, md, ln, slot = _ai_scan_find(d, kw)
    else:
        lt = (d or {}).get('last') or {}
        p0, m0 = lt.get('pool'), lt.get('mode')
        if p0 and m0:
            l0 = _have_lines(d, p0, m0) or lt.get('lines') or '4'
            s0 = _slot_of(d, p0, m0, l0)
            if s0 and s0.get('results'):
                pool, md, ln, slot = p0, m0, l0, s0
        if not slot and m0:
            # last 指的档位正好空着 → 退回该模式实际有结果的那一档 (模式不换)
            pool, md, ln, slot = _ai_scan_find(d, m0)
    if not slot or not slot.get('results'):
        did = ('「%s」还没有扫描结果' % _AI_SCAN_LABELS.get(kw, kw)) if kw else \
              '还没有任何扫描结果'
        return ('%s。\n**这次不会替你触发扫描** —— 扫一次几十秒, 还会把用户正在看的'
                '结果换代。请如实告诉用户: 在终端上**双击**对应的模式按钮扫一次, 再回来问。\n'
                '终端上的六个模式: %s'
                % (did, ' / '.join('%s(%s)' % (_AI_SCAN_LABELS[m], m) for m in SCAN_MODES)))
    rows = slot['results']
    out = ['【扫描结果】%s  (%s, 档位 %s线)  共 %d 只'
           % (_AI_SCAN_LABELS.get(md, md),
              '启动选股', ln,
              slot.get('total_results') or len(rows)),
           '扫描时刻 %s%s'
           % (slot.get('scan_time') or '?',
              ('   价回写于 %s' % slot['px_time']) if slot.get('px_time') else '')]
    for i, r in enumerate(rows[:top], 1):
        out.append(_ai_scan_line(md, r, i))
    if len(rows) > top:
        out.append('...(共 %d 只, 只列前 %d; 要更多把 top 调大)' % (len(rows), top))
    out.append('(口径: **落盘的上次扫描结果**, 不是实时重算; 价可能是"扫描那一刻"或'
               '"最近一次刷价"的 —— 上面标了两个时刻。要现价请用 get_realtime。)')
    out.append('⚠️ 扫描用的"今天" = 本地K线CSV的最后一根: 盘前跑的话那是上一交易日。')
    return '\n'.join(out)


def _ai_t_sim():
    """模拟盘当前状态 —— 与终端「模拟盘」面板同源 (SimAccount)。

    🔴 **只读**。这层不给 AI 任何买卖/重置的口子 —— 仓位是用户的, 不是 AI 的。
    """
    acc = SimAccount()
    pos = acc.positions or []
    recs = acc.records or []
    need = sorted({p['code'] for p in pos if p.get('code')})
    px = {}
    if need:
        rtd = fetch_sina([c.lower() for c in need])
        for c in need:
            px[c] = (rtd.get(c.lower()) or {}).get('price') or 0
    hv = acc.holdings_value(px) if pos else 0.0
    nav = acc.nav(px) if pos else acc.cash
    out = ['【模拟盘】初始 %.0f   现金 %.0f   持仓市值 %.0f   总资产 %.0f   收益 %+.2f%%'
           % (acc.initial_cash, acc.cash, hv, nav,
              (nav / acc.initial_cash - 1) * 100 if acc.initial_cash else 0)]
    if not pos:
        out.append('当前**空仓**。')
    else:
        out.append('持仓 %d 只 (铁律上限 5 只, 同方向≤2 只):' % len(pos))
        for p in pos:
            c = p.get('code') or ''
            sh = p.get('shares') or 0
            cost = p.get('avg_cost') or p.get('entry_price') or 0
            cur = px.get(c) or 0
            line = ('  %-9s %-6s %6d股  成本 %.3f  现价 %s  浮动 %+.0f 元 (%+.2f%%)'
                    % (c, str(p.get('name') or '')[:6], sh, cost,
                       ('%.2f' % cur) if cur else '没取到',
                       (cur - cost) * sh if (cur and cost) else 0,
                       (cur / cost - 1) * 100 if (cur and cost) else 0))
            if p.get('stop_loss'):
                line += '  止损 %.2f' % p['stop_loss']
            if p.get('take_profit_at'):
                line += '  止盈 %.2f' % p['take_profit_at']
            if p.get('entry_date'):
                line += '  建仓 %s' % p['entry_date']
            if p.get('sector'):
                line += '  行业 %s' % p['sector']
            out.append(line)
    today = datetime.now().strftime('%Y-%m-%d')
    tr = [r for r in recs if str(r.get('time') or '')[:10] == today]
    if tr:
        out.append('今日操作 %d 笔 (最近 10 笔):' % len(tr))
        for r in tr[-10:]:
            s = ('  %s %-9s %-6s %s %s股 @%.2f'
                 % (str(r.get('time'))[11:16], r.get('code') or '',
                    str(r.get('name') or '')[:6], r.get('action') or '',
                    r.get('shares') or 0, r.get('price') or 0))
            if r.get('pnl') is not None:
                s += '  盈亏 %+.0f 元' % r['pnl']
            if r.get('reason'):
                s += '  (%s)' % r['reason']
            out.append(s)
    else:
        out.append('今日无操作。')
    out.append('(口径: 与终端「模拟盘」面板同源; 现价是实时快照, 非交易时段即最近一个'
               '交易日收盘价。**AI 只能看, 不能买卖** —— 用户要下单请让他在终端上操作。)')
    return '\n'.join(out)


def _ai_t_watchlist():
    """自选股 (含实时价) —— 与终端左侧「自选股」列表同源。"""
    wl = load_watchlist()
    if not wl:
        return '自选股是空表 (用户在终端上还没加过)。'
    codes = [w.get('code') for w in wl if w.get('code')]
    rtd = {}
    # P3-5 同款: fetch_sina 内部截断 50 只 → 分批拉
    for i in range(0, len(codes), 50):
        rtd.update(fetch_sina([c.lower() for c in codes[i:i + 50]]))
    out = ['【自选股】共 %d 只 (与终端左侧列表同源):' % len(wl)]
    for w in wl:
        c = w.get('code') or ''
        q = rtd.get(c.lower()) or {}
        out.append('  %-9s %-6s %8.2f  %+6.2f%%   加入于 %s'
                   % (c, str(w.get('name') or q.get('name') or '')[:6],
                      q.get('price') or 0, q.get('change_pct') or 0,
                      w.get('added') or '?'))
    out.append('(要其中某一只的走势/基本面, 用它的 6 位代码调 get_kline / '
               'get_realtime / get_stock_info。)')
    return '\n'.join(out)


def _ai_t_stock_info(code):
    """个股基本面 + 股东户数 (筹码集中度)。

    ⚠️ 两个源各自的**报告期**必须打出来 —— 不标的话模型会把一季报说成"最新"。
    """
    inm, isc = _ai_norm_index(code)
    if isc:
        return '%s 是**指数**, 没有基本面/股东户数。指数请用 get_market。' % inm
    full, sina = _ai_norm_code(code)
    if not full:
        return '「%s」不是合法的 6 位 A 股代码。' % code
    out = ['%s 基本面 / 筹码' % full]
    try:
        f = fetch_fundamental(full) or {}
    except Exception as e:
        f = {'error': '%s: %s' % (type(e).__name__, str(e)[:60])}
    if f.get('error'):
        out.append('基本面: 取不到 (%s)' % f['error'])
    else:
        seg = []
        if f.get('report_date'):
            seg.append('报告期 %s' % f['report_date'])
        if f.get('eps') is not None:
            seg.append('EPS %s' % f['eps'])
        if f.get('roe') is not None:
            seg.append('ROE %s%%' % f['roe'])
        if f.get('revenue_yoy') is not None:
            seg.append('营收同比 %+.1f%%' % f['revenue_yoy'])
        if f.get('profit_yoy') is not None:
            seg.append('净利同比 %+.1f%%' % f['profit_yoy'])
        out.append('基本面: %s' % ('   '.join(seg) if seg else '无数据'))
        mc = []
        if f.get('market_cap_total') is not None:
            mc.append('总市值 %s 亿' % f['market_cap_total'])
        if f.get('market_cap_float') is not None:
            mc.append('流通市值 %s 亿' % f['market_cap_float'])
        if mc:
            out.append('       %s' % '   '.join(mc))
    try:
        h = fetch_holder_num(full) or {}
    except Exception as e:
        h = {'error': '%s: %s' % (type(e).__name__, str(e)[:60])}
    if h.get('error'):
        out.append('股东户数: 取不到 (%s)' % h['error'])
    else:
        t = h.get('trend') or {}
        out.append('股东户数: 最新一期 %s  环比 %s%%  筹码 %s'
                   % (t.get('report_date') or '?', t.get('latest_ratio'),
                      t.get('latest_focus') or '?'))
        if t.get('label'):
            out.append('  趋势: %s%s'
                       % (t['label'],
                          (' 连续 %d 期' % t['streak']) if t.get('streak') else ''))
        rows = h.get('rows') or []
        if rows:
            tail = rows[-5:]
            out.append('  近 %d 期 (报告期 / 户数 / 环比%% / 筹码):' % len(tail))
            for r in tail:
                out.append('    %s  %8s  %+6s%%  %s'
                           % (r.get('report_date') or '?', r.get('holder_num'),
                              r.get('total_ratio'), r.get('hold_focus') or ''))
        out.append('  (口径: 户数减=筹码集中, 户数增=分散。'
                   '★ 披露**不按季度** —— 实测间隔 10 天到一季都有, '
                   '所以按**报告期**判断新旧, 别默认它是季报。)')
    return '\n'.join(out)


def _ai_t_intraday(code):
    """当日分时 —— 与终端「分时图」同源 (腾讯分钟接口, 只覆盖**当天**)。

    给的是**采样后的走势**, 不是 240 个点全灌 (全灌会把模型的注意力淹掉)。
    ⚠️ 与 get_realtime 的分工: 那个是"此刻一枪", 这个是"今天怎么走过来的" (做T用)。
    """
    raw = str(code or '').strip()
    inm, isc = _ai_norm_index(raw)
    if isc:
        sc, nm = isc, inm
    elif raw.lower() in INDEX_CODES:
        sc, nm = raw.lower(), INDEX_NAMES.get(raw.lower(), raw.lower())
    else:
        full, sina = _ai_norm_code(raw)
        if not full:
            return '「%s」不是合法的 6 位 A 股代码 (指数代码也可以)。' % code
        sc, nm = sina, full
    try:
        points, prev_close, _name, mdate = fetch_minute(sc)
    except Exception as e:
        return ('分时取不到 (%s)。可能原因: 非交易时段源不返回 / 该票当天没成交 / '
                '网络不通。**如实告诉用户没查到**。' % type(e).__name__)
    if not points:
        return ('%s 今天**没有分时数据** (没开盘 / 停牌 / 源没返回)。'
                '如实说没查到, 别猜。' % nm)
    last = points[-1]
    p0 = prev_close or points[0]['p']
    hi = max(x['p'] for x in points)
    lo = min(x['p'] for x in points)
    out = ['%s (%s) 当日分时   数据日期 %s   共 %d 个点'
           % (nm, sc, mdate or '?', len(points)),
           '最新 %.2f (%s)   较昨收 %+.2f%%   昨收 %.2f'
           % (last['p'], last['t'], (last['p'] / p0 - 1) * 100 if p0 else 0, p0),
           '序列内最高 %.2f   最低 %.2f   (仅分时序列内的高低, 不是官方最高/最低价)'
           % (hi, lo)]
    step = max(1, len(points) // 12)
    samp = points[::step]
    if samp[-1] is not last:
        samp.append(last)
    out.append('走势采样 (时刻 价):')
    out.append('  ' + '   '.join('%s %.2f' % (x['t'], x['p']) for x in samp))
    out.append('(口径: 腾讯分钟接口, **只有当天**; 与终端分时图同源。'
               '非交易时段/节假日拿到的是最近一个交易日的全天分时。)')
    return '\n'.join(out)


def _ai_sector_kline(kw):
    """视角二: 板块**等权合成指数**日K (与终端「板块K线」同源)。"""
    ind_map, con_map = load_industry_concept_maps()
    kind, sec, _cands = _ai_pick_sector(kw, ind_map, con_map)
    if not sec:
        return '没有叫「%s」的行业或概念。先用准确名字再试, 或者问用户。' % kw
    if kind == '概念':
        codes = sorted({c for c, s in con_map.items() if sec in (s or '').split('|')})
    else:
        codes = sorted({c for c, s in ind_map.items() if s == sec})
    if len(codes) < 3:
        return '%s「%s」成分股不足 3 只, 合成不了指数。' % (kind, sec)
    _kind = 'concept' if kind == '概念' else 'sector'
    # v2.177 (2026-10-22): 优先同花顺官方指数点数 —— 否则会出现"图上 1997 点、
    #   AI 却说 1000 点起"的自相矛盾。
    _off = None
    try:
        from quant_terminal import sector_kline as _sk
        _off = _sk.official_kline(_kind, sec)
    except Exception:
        _off = None
    data = _off or composite_sector_kline(codes, name=sec, kind=_kind)
    if not data:
        return '%s「%s」成分股K线不足, 这次没合成出来。' % (kind, sec)
    dates = data.get('dates') or []
    ohlc = data.get('ohlc') or []
    if not dates or not ohlc:
        return '%s「%s」合成指数没有数据。' % (kind, sec)
    _src_txt = '同花顺官方指数' if _off else '等权合成指数'
    out = ['%s「%s」%s  共 %d 根, 最后一根 %s  收 %.2f'
           % (kind, sec, _src_txt, len(dates), data.get('last_date') or dates[-1],
              data.get('last_close') or 0)]
    if _off and (data.get('synth_prefix') or 0):
        out.append('(注意: 该板块官方历史只有 %d 根, 前面 %d 根是按现有成分股倒推估算的'
                   ' —— 用来看形态可以, 别当真实点位引用)'
                   % (len(dates) - (data.get('synth_prefix') or 0),
                      data.get('synth_prefix') or 0))
    if sec != kw and _cands:
        out.append('(名字是按关键词匹配到这%s的, 不是精确同名)' % sec)
    # ⚠️ 成分股数必须数 `codes` —— 返回体里的 `count` 是 **len(ohlc) = 交易日根数**
    #    (实测: 白酒 19 只成分股, 但 count=454)。写错的话数字像真的, 模型会直接引用。
    if _off:
        out.append('(口径: **同花顺官方指数点位** —— 与终端「板块K线」同一份数据; '
                   '成分股 %d 只, 上面"共 N 根"是**交易日根数**, 不是成分股数)' % len(codes))
    else:
        out.append('(口径: **成分股 %d 只**按涨幅等权合成, 1000 点起 —— 与终端「板块K线」同源; '
                   '上面"共 N 根"是**交易日根数**, 不是成分股数)' % len(codes))
    # ⚠️ 合成指数的 mas 是 {ma5: [整条序列]} —— **要取最后一根**, 不是标量。
    #    日K那两个工具用的 _ai_ma() 返回的是 {5: 标量}, 形状不同, 别照抄。
    mas = data.get('mas') or {}
    _ma = []
    for _k in sorted(mas, key=lambda x: int(str(x).lstrip('ma') or 0)):
        _v = mas[_k]
        _v = _v[-1] if isinstance(_v, list) and _v else _v
        if isinstance(_v, (int, float)):
            _ma.append('MA%s=%.2f' % (str(_k).lstrip('ma'), _v))
    if _ma:
        out.append('均线(截至 %s): %s'
                   % (data.get('last_date') or dates[-1], '  '.join(_ma)))
    nb = min(20, len(dates) - 1)
    out.append('最近 %d 根 (日期 / 收盘 / 涨跌%%):' % nb)
    # ⚠️ ohlc 行的顺序是 [open, close, low, high] —— **不是** OHLC, 别按名字取
    for i in range(len(dates) - nb, len(dates)):
        c, pv = ohlc[i][1], ohlc[i - 1][1] if i else 0
        out.append('  %s  %9.2f  %+7.2f%%' % (dates[i], c, (c / pv - 1) * 100 if pv else 0))
    return '\n'.join(out)


def _ai_sector_breakout(kw):
    """视角三: 板块**三档左峰突破** (与终端「行业突破 / 概念突破」弹窗同源)。

    三档 = 主升(破100日窗峰) / 延续(破55日窗峰) / 反转(破最近孤立峰)。
    """
    ind_map, con_map = load_industry_concept_maps()
    kind, sec, _cands = _ai_pick_sector(kw, ind_map, con_map)
    if not sec:
        return '没有叫「%s」的行业或概念。先用准确名字再试, 或者问用户。' % kw
    try:
        import quant_terminal.breakout as bo
        d = bo.board_breakout('industry' if kind == '行业' else 'concept', force=False)
    except Exception as e:
        return '板块突破判定这次没跑出来 (%s)。如实说没查到。' % type(e).__name__
    cols = (d or {}).get('cols') or {}
    label = {'major': '主升(破100日窗峰)', 'mid': '延续(破55日窗峰)',
             'minor': '反转(破最近孤立峰)'}
    hit, tier = None, None
    for key in ('major', 'mid', 'minor'):
        for r in (cols.get(key) or []):
            if r.get('name') == sec:
                hit, tier = r, key
                break
        if hit:
            break
    head = ['%s「%s」 三档左峰突破  数据日期 %s'
            % (kind, sec, (d or {}).get('kline_date') or '?')]
    if sec != kw and _cands:
        head.append('(名字是按关键词匹配到这%s的, 不是精确同名)' % sec)
    if not hit:
        head.append('**没有在突破名单里** —— 这一档它现在没突破。')
        head.append('当前三档各有多少个板块在突破: %s'
                    % ' / '.join('%s %d 个' % (label[k], len(cols.get(k) or []))
                                 for k in ('major', 'mid', 'minor')))
        head.append('(口径: 等权合成板块指数做左峰突破判定; 与终端「行业/概念突破」弹窗同源)')
        return '\n'.join(head)
    head.append('档位: **%s**   合成指数 %.2f   被破的左峰 %.2f (%s)   距峰 %+.2f%%'
                % (label.get(tier, tier), hit.get('close') or 0, hit.get('pk') or 0,
                   hit.get('pk_date') or '?', hit.get('dist_pct') or 0))
    head.append('突破日 %s   连续 %s 天%s   板块多头占比 %.0f%%   成分 %s 只'
                % (hit.get('bdate') or '?', hit.get('streak') or 0,
                   ' (今日新突破)' if hit.get('is_new') else '',
                   hit.get('bullish_pct') or 0, hit.get('stocks') or '?'))
    if hit.get('status'):
        head.append('板块状态: %s' % hit['status'])
    head.append('(口径: 等权合成板块指数做左峰突破判定 —— 三档 = 主升/延续/反转; '
                '与终端「行业/概念突破」弹窗同源)')
    return '\n'.join(head)


def _ai_t_sector(name, kind='trend'):
    """板块 —— 三个视角, 按 kind 分派。

    trend (默认) = 均线多头状态 + 成分股 (终端「行业/概念趋势」面板)
    kline        = 等权合成指数日K (终端「板块K线」)
    breakout     = 三档左峰突破 (终端「行业/概念突破」弹窗)
    """
    kw = str(name or '').strip()
    if not kw:
        return '板块名是空的。'
    kd = str(kind or 'trend').strip().lower()
    if kd in ('', 'trend', 'ma', '均线', '趋势'):
        return _ai_sector_trend(kw)
    if kd in ('kline', 'k', '日k', '指数', '走势'):
        return _ai_sector_kline(kw)
    if kd in ('breakout', '突破', 'bk'):
        return _ai_sector_breakout(kw)
    return ('没有「%s」这种板块视角。可用: trend (默认, 均线多头状态) / '
            'kline (合成指数日K) / breakout (三档左峰突破)' % kind)


def _ai_tool_exec(name, args):
    """执行一次工具调用 → 返回给模型看的字符串。

    ★ 永不抛异常 (出错也返回说明文字): ai_chat 那边已经会包一层 "工具执行出错",
      这里再抛就成两层错误糊在模型脸上, 它反而不知道该怎么办。查不到 = 正常结果。
    """
    a = args if isinstance(args, dict) else {}
    try:
        if name == 'search_stock':
            return _ai_t_search(a.get('keyword', ''))
        if name == 'get_realtime':
            return _ai_t_realtime(a.get('code', ''))
        if name == 'get_kline':
            return _ai_t_kline(a.get('code', ''), a.get('bars', 60))
        if name == 'get_sector':
            return _ai_t_sector(a.get('name', ''), a.get('kind', 'trend'))
        if name == 'get_market':
            return _ai_t_market(a.get('name', ''))
        if name == 'get_news':
            return _ai_t_news(a.get('keyword', ''), a.get('limit', 12))
        if name == 'get_scan':
            return _ai_t_scan(a.get('mode', ''), a.get('top', 20))
        if name == 'get_sim':
            return _ai_t_sim()
        if name == 'get_watchlist':
            return _ai_t_watchlist()
        if name == 'get_stock_info':
            return _ai_t_stock_info(a.get('code', ''))
        if name == 'get_intraday':
            return _ai_t_intraday(a.get('code', ''))
        return '没有名为 %s 的工具。可用: %s' % (name, ', '.join(_AI_TOOL_NAMES))
    except Exception as e:
        return ('取数失败 (%s): %s。如实告诉用户这次没查到。'
                % (type(e).__name__, str(e)[:120]))


def get_kline_status():
    """查询本地K线数据的最新日期"""
    import random
    kline_dir = Path(ds.resolve(ds.get('local', 'kline_dir', os.path.join('kline_data', 'daily'))))
    if not kline_dir.exists():
        return {'latest_date': '无数据', 'file_count': 0}

    csv_files = sorted([f for f in os.listdir(kline_dir) if f.endswith('.csv')])
    if not csv_files:
        return {'latest_date': '无数据', 'file_count': 0}

    # 抽样检查: 随机选几只 + 必须包含平安银行(000001)
    samples = set()
    for code in ['SZ000001', 'SH600000', 'SH600519', 'SZ000333', 'SH601939']:
        f = f'{code}.csv'
        if f in csv_files or os.path.exists(kline_dir / f):
            samples.add(f)

    # 再随机补几只
    others = [f for f in csv_files if f not in samples]
    if others:
        samples.update(random.sample(others, min(5, len(others))))

    dates = set()
    for fname in samples:
        fpath = kline_dir / fname
        try:
            with open(fpath, 'r') as f:
                # seek到末尾读最后一行
                f.seek(0, 2)
                size = f.tell()
                if size < 100: continue
                f.seek(max(0, size - 200))
                lines = f.readlines()
                if len(lines) >= 2:
                    last = lines[-1].strip()
                    if ',' in last:
                        dates.add(last.split(',')[0])
        except Exception as _e:
            _swlog('kline_date_scan', _e)

    if dates:
        latest = max(dates)
    else:
        latest = '未知'

    # K线滞后检测 — 忘了导出快照时，前端据此显示 ⚠️ 警告。口径: K线最后一天 vs 最近"已收盘"交易日。
    # v2.118: 基准日改用**交易所自己报的**成交日期(ds.last_session)。🔴 原先纯时钟推算，中秋节的星期五被当成
    #   "今天已收盘"，而 CSV 最后一根是 09-24 ⇒ 休市日弹「⚠️K线滞后1天」，用户只会以为数据没更新。
    # v2.127: 🔴 v2.118 **没治根** —— 它换了"基准日怎么定"，但最后那步退一格仍**只跳周末、不认节假日** ⇒
    #   节后第一个交易日照样误报(09-28 周一 10:21 源已翻成今天，退一格落到 09-25 中秋休市)。
    #   **工作日 ≠ 交易日** 就是这个假设的错。⇒ 主路径改成数本地交易日历(见下)。
    stale_days = 0
    try:
        from datetime import date as _date, datetime as _dt, timedelta as _td
        def _workday_dist(d: _date) -> int:
            """距基准日的工作日数 (只用于差值)"""
            n, x = 0, _date(2020, 1, 1)
            while x < d:
                if x.weekday() < 5:
                    n += 1
                x += _td(days=1)
            return n

        def _prev_wd(d: _date) -> _date:
            """上一个工作日"""
            d -= _td(days=1)
            while d.weekday() >= 5:
                d -= _td(days=1)
            return d

        ld = _date.fromisoformat(latest)
        now = _dt.now()
        today = now.date()
        t = now.hour * 100 + now.minute
        # ── v2.127 主路径: 本地交易日历 ─────────────────────────────────────────
        # 数「日历里比 CSV 最后一根**晚**的有几个交易日」—— 节假日不进账，节后第一天不再误报"滞后1天"。
        #   日历由 _index_guard_loop 写(300s 一跳，盘中不写盘，15:05 后才落当日 bar) ⇒ 只含**已收盘确认**的交易日；
        #   且独立于用户手动导出的 kline_data/daily ⇒ "忘了导出"时照样准，不会跟着一起变旧让判定成环。
        # 🔴 v2.128: 调 engine 的共用判据，别再内联一份。v2.127 的教训就是「报告者改对了、消费者没改」
        #   ⇒ 出现「状态说 0 天滞后、面板却全空」。**一个判据只能有一份实现。**
        _n = trade_days_behind(latest)
        if _n is not None and _n >= 0:
            stale_days = _n
        else:
            # ── 兜底: 日历缺失/损坏 → 退回 v2.118 的源日期推算 ────────────────────
            #   ⚠️ 这条路**节假日盲** (退一格只跳周末, 不认节假日), 节后首个交易日会误报。
            #      留着只是因为"连日历文件都没了"比偶尔误报更糟, 不是因为它对。
            _ls = ds.last_session(None)
            if not _ls:
                # v2.118: 自愈 —— 本进程还没取过任何实时数据时 LAST_SESSION 是空的, 若就这么
                #   退回纯时钟, 冷启动撞上节假日会**照样弹「K线滞后1天」**, 而前端只在页面加载时
                #   调一次这个接口 (loadKlineStatus), 弹了就挂一整场, 不会自己好。
                #   顺手补取一次: get_market_indices() 有 TTL 缓存且**不碰 is_holiday()**(无环),
                #   失败也只是回到上面的兜底路径。
                try:
                    get_market_indices()
                    _ls = ds.last_session(None)
                except Exception as _e:
                    _swlog('market_indices_probe', _e)
            if _ls:
                # 源说的"最近交易日"就是最近**已收盘**那天 —— 休市日它天生说 09-24,
                # 不会像纯时钟那样把 09-25 当成已收盘。
                # ⚠ 但它 09:25 集合竞价后就翻成今天, 而今天的 bar 要 15:06 才落定
                #   ⇒ 15:06 前一律退回上一个工作日 (今天可能压根没开市)。
                last_closed = _date.fromisoformat(_ls)
                if t <= 1506:
                    last_closed = _prev_wd(last_closed)
            elif today.weekday() >= 5:  # 拿不到源日期 → 退回原来的纯时钟推算, 一个字不改
                last_closed = today - _td(days=(today.weekday() - 4))
            elif t < 1500:
                last_closed = _prev_wd(today)
            else:
                last_closed = today
            stale_days = max(0, _workday_dist(last_closed) - _workday_dist(ld))
    except Exception:
        stale_days = -1  # 日期无法解析 → 前端显示"未知"

    return {
        'latest_date': latest,
        'file_count': len(csv_files),
        'samples_checked': len(samples),
        'stale_days': stale_days,
    }


# 指数缓存 (2026-08-06): 盘中10秒(实时) / 盘后1小时(收盘价不变不重复拉)
INDEX_CACHE = {'ts': 0, 'data': {}}


def get_market_indices():
    """市场指数 (盘中10秒缓存, 盘后1小时缓存—收盘后不重复请求新浪)

    v2.118: TTL 改用 `_in_session_window()` (纯钟点) —— 见该函数的重入说明。
    节假日源报的成交日期不是今天 ⇒ 退到 5 分钟, 免得开市窗口里 10 秒一次白打一整天。
    ⚠ 拿不到日期('')时**按旧行为**走 10 秒: 上游字段一变就降频, 会静默拖慢顶栏。
    """
    now = time.time()
    ttl = 10 if _in_session_window() else 3600
    _ls = ds.last_session(None)
    if ttl == 10 and _ls and _ls != datetime.now().strftime('%Y-%m-%d'):
        ttl = 300
    if now - INDEX_CACHE['ts'] < ttl and INDEX_CACHE['data']:
        return INDEX_CACHE['data']
    # v2.117: 名字表已提到模块级 INDEX_NAMES (AI 工具要共用同一份, 见文件头)
    data = fetch_sina(list(INDEX_NAMES.keys()))
    INDEX_CACHE['ts'] = now
    INDEX_CACHE['data'] = data
    return data


# ============================================================
# v2.111：财经新闻聚合（财联社 + 同花顺）
# ============================================================
# 两个都是匿名公开接口，**只在用户开窗/点「获取新闻」时拉，无后台轮询**。
# 🔴 财联社 rn **硬上限 = 50** —— 传 51 会**静默返 0 条**(errno 仍 0)，要 100 条必须用 last_time 翻页
#   (游标 = 上一页最早的 ctime；实测零重叠)。财联社的 sign 是前端逆向方案 md5(sha1(urlencode(sorted(params))))，
#   **不是公开 API**、随时可能变 ⇒ 两源各拉各的，一边挂了另一边照常出。
# ⚠ 去重分两层：① 同源内按 id ② 跨源**同题**(归一化后逐字相同) → 留 lvl 高的。
#   **措辞不同**的跨源条目仍各留一条 —— 那种比对不可靠（「WTI原油期货价格涨3%」vs「WTI原油涨超3%」必漏）。
NEWS_UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
           '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
NEWS_TTL = 60        # 秒: 开窗吃这个窗口内的缓存 (连点连开不重复拉)
NEWS_RET_MAX = 600   # 返回上限 (财联社 5x50 + 同花顺 3x100 = 550, 留余量)
# v2.217 (2026-10-24 用户: "点击要闻能不能拉满整整一屏"):
#   原 2x50 + 1x100 = 152 条池子里"要闻"只有 9 条, 一屏能放 37 行却只占 1/4 屏,
#   用户看到的就是下面一大片空白。
#   财联社要闻比例随时段上升 (2页 7 条 → 4页 24 条 → 5页 50+ 条), 加深即可稳住两整屏。
#   5x50 + 3x100 后池子约 480 条、要闻 70+ 条; 各筛选组合 (财联社+要闻等) 也够填满。
#   ⚠ 代价: 拉取请求数 3 → 8 个 (25s 兜底超时不变), /api/news 体积 118KB → 约 210KB。
NEWS_CACHE = {'ts': 0.0, 'items': [], 'pool': 0, 'warn': ''}
NEWS_LOCK = threading.Lock()
# v2.218 (2026-10-24 用户: "两个源的新闻很多重复, 能不能做合并去重"):
#   跨源去重拆到 news_dedup.py —— 三层(机器直合并 / 股票名闸 / AI 判定),
#   留信息量多的那条; AI 不可用时**原样返回**, 永不拖垮新闻本身。
from quant_terminal import news_dedup as _ndedup


def _news_cls(pages=2):
    """财联社电报. 每页 50 条 (rn 上限), 用 last_time 翻页。"""
    import hashlib
    out, last = [], ''
    for _ in range(pages):
        p = {'app': 'CailianpressWeb', 'category': '', 'last_time': last, 'os': 'web',
             'refresh_type': '1', 'rn': '50', 'sv': '8.4.6'}
        qs = urllib.parse.urlencode(sorted(p.items()))
        sign = hashlib.md5(hashlib.sha1(qs.encode()).hexdigest().encode()).hexdigest()
        r = _req_get('https://www.cls.cn/v1/roll/get_roll_list?' + qs + '&sign=' + sign,
                     timeout=8, headers={'User-Agent': NEWS_UA,
                                         'Referer': 'https://www.cls.cn/telegraph'})
        rd = (r.json().get('data') or {}).get('roll_data') or []
        if not rd:
            break
        out.extend(rd)
        cur = [int(x.get('ctime') or 0) for x in rd]
        if not any(cur):
            break
        last = str(min(cur))
    return out


def _news_ths(pages=1, size=100):
    """同花顺快讯. page 从 1 起 (实测翻页零重叠)。"""
    out = []
    for pg in range(1, pages + 1):
        r = _req_get('https://news.10jqka.com.cn/tapp/news/push/stock/'
                     '?page=%d&tag=&track=website&pagesize=%d' % (pg, size),
                     timeout=8, headers={'User-Agent': NEWS_UA})
        lst = (r.json().get('data') or {}).get('list') or []
        if not lst:
            break
        out.extend(lst)
    return out


def _news_norm_cls(raw):
    """归一化: lvl 2=A / 1=B或置顶 / 0=普通。个股只认 sh/sz 前缀 (bj 北交所不要)。"""
    for x in raw:
        lv = 2 if x.get('level') == 'A' else (1 if (x.get('level') == 'B' or x.get('is_top')) else 0)
        ks = []
        for st in (x.get('stock_list') or []):
            sid = str(st.get('StockID') or '')           # 'sh688498' / 'sz000001'
            if len(sid) == 8 and sid[:2] in ('sh', 'sz') and sid[2:].isdigit():
                ks.append({'n': str(st.get('name') or ''), 'c': sid[2:]})
        yield {'id': 'C' + str(x.get('id')), 'src': 'cls', 't': int(x.get('ctime') or 0),
               'lvl': lv, 'title': str(x.get('title') or x.get('brief') or '').strip(),
               'url': str(x.get('shareurl') or ''), 'ks': ks}


def _news_norm_ths(raw):
    """归一化. ⚠ 个股判据 = stockCode 是 **6 位纯数字** —— 港股带 HK 前缀(HK3308)、
    美股是字母(PFE), 一条就滤掉了, 不用去猜 stockMarket 的 17/22/33 是什么意思。"""
    for x in raw:
        lv = 1 if str(x.get('import') or '0') != '0' else 0
        ks = []
        for st in (x.get('stock') or []):
            c = str(st.get('stockCode') or '')
            if len(c) == 6 and c.isdigit():
                ks.append({'n': str(st.get('name') or ''), 'c': c})
        yield {'id': 'T' + str(x.get('id')), 'src': 'ths', 't': int(x.get('ctime') or 0),
               'lvl': lv, 'title': str(x.get('title') or x.get('short') or '').strip(),
               'url': str(x.get('url') or ''), 'ks': ks}


def _news_fetch():
    """并行拉两个源 → 归一化 → 同源去重 → 时间倒序。返回 (items, warn)。

    v2.217: 深度由 2x50 + 1x100 提到 5x50 + 3x100 (见 NEWS_RET_MAX 处的账)。
    两源并发: 财联社按 last_time 顺序翻页 (5 页约 3.3s), 同花顺按 page 翻页 (3 页约 1.9s)。
    """
    got, warn = {}, []

    def _one(key, fetch, norm):
        try:
            raw = fetch()
            got[key] = list(norm(raw))
            # v2.112: 在**归一化之前**把正文抄进旁路 (ai_news 自带字段映射)。
            #   ⚠️ 刻意不动 _news_norm_*: 它们保证 items 恰好 7 字段, 是条硬契约
            #      (NEWS_CACHE / /api/news 响应 / 前端三处都消费它)。正文一律走旁路。
            ai_news.stash_raw(key, raw)
        except Exception as e:
            warn.append('%s 拉取失败: %s' % (key, e))

    ths = [threading.Thread(target=_one, args=('cls', lambda: _news_cls(5), _news_norm_cls)),
           threading.Thread(target=_one, args=('ths', lambda: _news_ths(3), _news_norm_ths))]
    for t in ths:
        t.start()
    for t in ths:
        t.join(25)          # 两源并发, 最坏 ~8s 超时, 25s 兜底
    items, seen, seen_t = [], set(), {}
    for k in ('cls', 'ths'):
        for it in (got.get(k) or []):
            if not it['title'] or it['id'] in seen:
                continue
            seen.add(it['id'])
            tk = re.sub(r'[\s\u3000]+', '', it['title'])      # 归一: 只去空白, 其余逐字
            old = seen_t.get(tk)
            if old is not None:                                # 同题 → 留 lvl 高的 (平手留 cls)
                if it['lvl'] > old['lvl']:
                    items[items.index(old)] = it
                    seen_t[tk] = it
                continue
            seen_t[tk] = it
            items.append(it)
    items.sort(key=lambda x: -x['t'])
    return items[:NEWS_RET_MAX], warn


def _news_attach_imp(resp):
    """给响应里的每条新闻挂上 AI 影响判定 (没判过就是 None)。**就地改**, 返回同一个 resp。

    ⚠️ 收的是**整个响应 dict**, 不是 items list —— 传错的话 `for it in resp` 会在
       dict 的**键名**上迭代, 报 "str object does not support item assignment",
       而且只在跑到这一行时才炸 (静态检查看不出来)。调用点务必传 resp。
    ⚠️ 只搬运前端要用的字段, 不整个 rec 透传 —— 内部字段(ts)没必要上网。
    ⚠️ ai_news 任何异常都不许影响新闻本身: 判定是**锦上添花**, 新闻拉不到才是大事。

    v2.124: **正文也在这层挂** (`body`) —— 名字里的 imp 是历史, 这层实际是"上网前的
    最后一站"。⚠️ 正文只在**这一层**加, `_news_norm_*` 的 7 字段契约照旧冻结;
    `_BODY` 是 LRU, 掉出去的老新闻 body 就是空串, 前端据此不渲染正文块。
    """
    try:
        sc = ai_news.scores_map()
    except Exception:
        sc = {}          # v2.124: 判定挂了**不该把正文一起拖下水** —— 降级成"没判定"
    for it in ((resp or {}).get('items') or []):
        try:
            r = sc.get(str(it.get('id') or ''))
        except Exception:
            r = None
        it['imp'] = ({'i': r.get('i'), 'd': r.get('d'), 's': r.get('s'),
                      'l': r.get('l'), 'w': r.get('w') or ''} if r else None)
        # v2.124: 正文与 imp **同一层**挂 (内部 7 字段契约不动, 这是 API 边界)
        try:
            it['body'] = ai_news.display_body(it.get('id'), it.get('title'))
        except Exception:
            it['body'] = ''
    return resp


NEWS_RAW = {'items': []}      # 去重前的完整池子 (供 /api/news/dedup 重算用)


def _ndedup_resp(resp, raw_items):
    """v2.218: 给响应套一层跨源去重。**只改 items 与追加 dedup 字段**, 其余键不动。

    ⚠️ 去重任何异常都不许影响新闻本身: news_dedup.apply 内部已 try 全包,
       这里再包一层是防它连 import 都炸。
    """
    try:
        new, info = _ndedup.apply(raw_items or [])
        resp['items'] = new
        resp['dedup'] = info
        if info.get('warn'):
            resp['warn'] = (resp.get('warn') or '') + ('; ' if resp.get('warn') else '') + info['warn']
    except Exception as e:
        resp['dedup'] = {'merged': 0, 'warn': '去重不可用: %s' % e}
    return resp


def get_news(force=False):
    """带缓存的取数入口。单源挂 → 另一源照常出; 双源挂 → 退回上次缓存并标高。

    v2.122: 在**这一层**统一挂 AI 影响判定 —— 内部有 5 个 return 出口, 逐个去改
    漏一个就是"某条路径没有 imp" ⇒ 前端偶发筛不出东西, 极难查。包一层只改一处。
    """
    return _news_attach_imp(_get_news_raw(force))


def _get_news_raw(force=False):
    now = time.time()
    with NEWS_LOCK:
        if not force and NEWS_CACHE['items'] and (now - NEWS_CACHE['ts']) < NEWS_TTL:
            return _ndedup_resp({'ok': True, 'cached': True, 'ts': NEWS_CACHE['ts'],
                                 'pool': NEWS_CACHE['pool'], 'warn': NEWS_CACHE['warn'],
                                 'items': NEWS_CACHE['items']}, NEWS_CACHE['items'])
    items, warn = _news_fetch()
    if not items:
        with NEWS_LOCK:
            if NEWS_CACHE['items']:
                return _ndedup_resp({'ok': True, 'cached': True, 'ts': NEWS_CACHE['ts'],
                                     'pool': NEWS_CACHE['pool'], 'items': NEWS_CACHE['items'],
                                     'warn': '两个源都没返回, 显示上次缓存'
                                             + (' · ' + '; '.join(warn) if warn else '')},
                                    NEWS_CACHE['items'])
        return {'ok': False, 'items': [], 'pool': 0, 'ts': now, 'cached': False,
                'warn': '; '.join(warn) or '两个源都没返回数据'}
    with NEWS_LOCK:
        # v2.218: 缓存存**去重前**的完整池子 —— 新判定结果回来时不用重新拉源,
        #   直接对同一份池子再 apply 一遍即可(记在 NEWS_RAW 里)。
        NEWS_RAW['items'] = items
        NEWS_CACHE.update({'ts': now, 'items': items, 'pool': len(items), 'warn': '; '.join(warn)})
    return _ndedup_resp({'ok': True, 'cached': False, 'ts': now, 'pool': len(items),
                         'warn': '; '.join(warn), 'items': items}, items)


# ============================================================
# HTTP Handler
# ============================================================
class ThreadingHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    """多线程HTTP服务器, 避免长时间请求阻塞其他连接"""
    daemon_threads = True
    # v2.212 (2026-10-24 DEBUG #4): 监听队列深度。socket.listen(backlog) 默认只有 5,
    #   页面刷新瞬间几十个请求(html + 30 多个接口 + 静态)一起挤进来时, 多出来的会被
    #   内核直接回 RST ⇒ 浏览器侧表现为「连接被拒 / 接口偶发失败」。
    #   daemon_threads 只决定"每个连接能开几个线程", 不决定"能排多少个等 accept"。
    request_queue_size = 128

class QuantHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        qs = dict(urllib.parse.parse_qsl(parsed.query))

        # SSE 流
        if path == '/api/stream':
            self._handle_sse()
            return

        # v2.112: 新闻 AI 影响分析 —— **每请求一条私有流** (不是 /api/stream 那种进程级广播)。
        # ⚠️ 跟上面那条同理: **必须在 send_response(200) 之前早返回**,
        #    否则下面主链的 send_response 会撞成双状态行。
        if path == '/api/news/ai':
            self._handle_ai_news(qs.get('id', ''), qs.get('force', '') == '1')
            return

        # v2.281 (2026-11-12 用户 m10425): 板块轮动 AI 分析 —— 也是流式 NDJSON。
        #   与上面两条同理: **必须在主链 send_response 之前早返回**, 否则双状态行。
        # v2.340 (2026-11-18 用户 m25750): 版本信息与更新检查
        if path == '/api/version':
            # 🔴 注意: _json() 只发 Content-Type + body, **不发状态行**。
            #   这两条在主链 self.send_response(200) 之前早返回, 必须自己先发一次,
            #   否则客户端收到的是没有状态行的残缺响应(urllib 直接抛 HTTP 0)。
            self.send_response(200)
            _v = _ver.info()
            _v['update'] = _ver.check_update()
            self._json(_v)
            return
        if path == '/api/version/check':
            self.send_response(200)
            self._json(_ver.check_update(force=('force' in qs)))
            return
        if path == '/api/board_rps_ai':
            self._handle_board_rps_ai(qs)
            return

        # v2.302 (2026-11-13 用户 m16307): AI 大盘复盘 —— 同样是流式 NDJSON，
        #   照抄 TSP /api/market-recap/analyze。必须在主链 send_response 之前早返回。
        if path == '/api/recap/ai':
            self._handle_recap_ai(qs)
            return

        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')

        try:
            # v2.150: 顶部「—」按钮 → 最小化终端窗口。页面发不了 Win32 消息, 由服务端代做。
            #   走 WM_SYSCOMMAND/SC_MINIMIZE (与点标题栏最小化同路径), 不受已删空的系统菜单影响。
            if path == '/api/win/minimize':
                n = win_caption.minimize_windows()
                self._json({'ok': n > 0, 'count': n,
                            'msg': '' if n else '未找到终端窗口 (标题需为 %s)' % win_caption.TARGET_TITLE})

            elif path in ('/', '/index.html'):
                self._serve_html('terminal.html')

            elif path == '/echarts.min.js':
                # 本地 echarts (2026-08-04): 摆脱 jsdelivr CDN 依赖 (国内不稳定/代理断即空白)
                self._serve_html('echarts.min.js')

            elif path in ('/favicon.ico', '/favicon.png', '/apple-touch-icon.png'):
                # v2.265 (2026-11-09 用户 m06336): 任务栏图标与桌面快捷方式统一。
                #   桌面 .lnk 用的是 _开发产物\文档\图标\BSKing8799.ico（红色 K 线柱，7 档尺寸）；
                #   而 --app=http://127.0.0.1:8799 这种无边框窗口，任务栏图标取的是**页面 favicon**，
                #   原来没有 favicon ⇒ 回落成 Chrome 默认的灰球。这里把同一份 .ico 发出去。
                #   ⚠ 只认文件名不认路径，防目录穿越（与 /fonts/ 那条同款）。
                self._serve_html('favicon.ico')

            elif path.startswith('/fonts/'):
                # v2.80 (2026-09-12): 启动动画签名体自托管 (fonts/GreatVibes.woff2, 42KB)
                # basename 防目录穿越; font-display:block 保证开屏不等字体跳变
                self._serve_html('fonts/' + os.path.basename(path))

            elif path == '/api/status':
                # v2.152: 「终端开着」的唯一判据 —— 自动任务只在这一行被
                #   走过 PAGEC_ALIVE 秒内才认为页面还开着 (用户定案: 没开就不触发)。
                # v2.160: 顺便收下前端当前均线档位 —— 自动任务刷上级行业时跟它走
                auto_task.touch_heartbeat(qs.get('lines', ''))
                # v2.62：成交额拉取挂这里 —— /api/status 盘后仍被轮询（10 分钟），盘后当日未结算时靠它重试补齐。
                # v2.119：**先取指数，再判状态** —— is_holiday() 读的是「行情源报的成交日期」，而那个日期正是
                #   get_market_indices() 这次取数顺手记下的（ds._note_qdate）。反过来则冷启动（开机自启/刚重启）的
                #   第一个 /api/status 会因 LAST_SESSION 还空着而判成 POST_MARKET —— 节假日开机顶栏先闪一下「盘后」。
                #   ⚠ 不成环：get_market_indices() 的 TTL 只问 _in_session_window()（纯钟点），不碰 is_holiday()。
                indices = get_market_indices()
                _ph = get_trading_status()
                turnover.ensure(_ph)
                turnover.capture_bj(_ph)     # v2.64 方案A: 盘后悄悄攒北交所成交额 (界面不动)
                _rsrs = get_rsrs_cached()
                # v2.63: RSRS 日度落盘; 盘后成交额结算后把当日行定稿 (情绪分此时不再变)
                macro_daily.record(rsrs=_rsrs)
                if _ph == 'POST_MARKET' and macro_daily.needs_final():
                    _macro_finalize(_rsrs)
                status = {
                    'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'trading_status': _ph,
                    'is_trading': is_trading_time(),
                    # v2.300 (2026-11-13 用户 m15464): 模拟交易专用口径 —— 比 is_trading 多放
                    #   9:25~9:30 竞价。前端 simTradable() 优先读它; 实时刷新等其它模块仍读 is_trading。
                    'sim_tradable': sim_tradable_now(),
                    'in_auction': in_auction_window(),
                    # v2.118: 源报的最近交易日 —— 顶栏「最近交易日 09-24」直接用它。
                    # 节假日 K线最后一根必然停在这一天, 常显出来用户就不会以为"没更新"。
                    'last_session': ds.last_session(None),
                    'is_holiday': _ph == 'HOLIDAY',
                    'indices': indices,
                    'rsrs': _rsrs,
                    'macro_hist': macro_daily.view(),
                    'caliber': turnover.bj_info(),   # v2.64 方案A 自检: 北交所攒了几/6天
                    'scan': {
                        'running': SCAN_STATE['running'],
                        'done': SCAN_STATE['done'],
                        'scan_time': SCAN_STATE['scan_time'],
                        'pool_size': SCAN_STATE['pool_size'],
                        'result_count': len(SCAN_STATE['results']) if SCAN_STATE['results'] else 0,
                        'progress': SCAN_STATE.get('progress', 0),   # v2.4 轮询进度 (2026-08-05)
                        'total': SCAN_STATE.get('total', 0),
                        'error': SCAN_STATE.get('error'),   # P2-3 fix: 暴露扫描错误 (前端轮询才能终止并显示)
                        # v2.331 (用户 m24382): 下发当前扫的是哪个模式 —— 前端要靠它判断
                        #   「正在进行的是不是竞价打板」, 好给按钮点灯。SCAN_STATE 里本来就有, 只是没带出去。
                        'mode': SCAN_STATE.get('mode', ''),
                        # >>> AUCTION BEGIN
                        # 竞价打板进度条: scan() 的 _p(i,n) 只有粗 5 档, 需要把阶段文本带出去
                        'auction_info': SCAN_STATE.get('auction_info'),
                        # <<< AUCTION END
                        # >>> REBOUND BEGIN (v2.257 2026-11-07) 超跌反弹的观察日/漏斗也带出去
                        #   与 auction_info 同款: 只读 SCAN_STATE, 前端按 _scanMode 决定要不要画
                        'rebound_info': SCAN_STATE.get('rebound_info'),
                        # <<< REBOUND END
                        # >>> WAVE BEGIN (v2.258 2026-11-07) 波段低吸的观察日/漏斗也带出去
                        #   与 auction_info/rebound_info 同款: 只读 SCAN_STATE
                        'wave_info': SCAN_STATE.get('wave_info'),
                        # <<< WAVE END
                    }
                }
                self._json(status)

            elif path == '/api/auto_task':
                # v2.152: 四时点自动任务状态 (前端可选显示; 不显示也不影响功能)
                self._json(auto_task.status())

            elif path == '/api/auto_task/reset':
                # v2.152: 清今日已完成标记 (调试用; 正常不需要)
                self._json(auto_task.reset_today())

            elif path == '/api/auto_task/run':
                # v2.152: 手动立即执行某个时点 (验收/补跑用; 不占今日名额)
                # v2.157c (2026-10-18): 加 &dry=1 —— 验收脚本必须走它，否则每跑一次
                #   验收就真拉一次全市场。17:06 的 20 分账单就是这么来的 (m17996)。
                qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                key = (qs.get('key') or [''])[0]
                dry = (qs.get('dry') or [''])[0].lower() in ('1', 'true', 'yes')
                self._json(auto_task.run_now(key, dry=dry))

            elif path == '/api/auto_task/events':
                # v2.152 气泡: 前端带 after=<上次序号> 轮询, 只拿新增的
                qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                after = (qs.get('after') or ['0'])[0]
                self._json(auto_task.ev_since(after))

            elif path == '/api/intraday_snapshot':
                # v2.152: 临时快照摘要 (不含 5571 行明细)
                self._json(_intraday.status())

            elif path == '/api/intraday_snapshot/clear':
                _intraday.clear()
                self._json({'ok': True, 'msg': '临时快照已清除'})

            elif path == '/api/restart':
                # v2.14 (2026-08-27): 手动重启服务器 (状态栏"重连服务器"按钮)
                # 延时2秒让响应先回到浏览器, 新进程无窗口接管 8799 端口
                # 注意: Timer 无 daemon 构造参数, 需先建再赋值 (曾致500)
                _t = threading.Timer(2.0, _restart_server)
                _t.daemon = True
                _t.start()
                self._json({'ok': True, 'msg': '服务器重启中, 新进程约3秒后接管'})

            elif path == '/api/tdx/calls':
                # v2.164 (2026-10-20 用户 m21624 批准): 通达信本地调用审计账
                # 为什么需要: tdxaidata 的 DLL 不写任何本地日志（lib\log 与 lib\data 两个
                #   目录都是空的），包内也没有余额/积分查询 API ⇒ 「每次运行到底扣几分」
                #   只能靠数自己的调用次数推断。这个端点把「谁在什么时候调了几个接口」
                #   逐笔钉死，下笔账单来时缺的只剩「每次几分」这一个系数。
                # 只读、不花积分。
                try:
                    from quant_terminal import tdx_fetch as _tf2
                    try:
                        _lim = int(str(qs.get('limit', '50') or '50').strip())
                    except (ValueError, TypeError):
                        _lim = 50          # 非法 limit 用默认值，不当成错误
                    _lim = max(1, min(_lim, 400))          # 与 CALLS_KEEP 对齐
                    self._json(_tf2.calls_log(limit=_lim))
                except Exception as e:
                    self._json({'ok': False, 'error': str(e)[:200], 'runs': [], 'total': 0})

            elif path == '/api/datasource':
                # v1 (2026-08-29): 数据源设置 — 配置+本地存在性+降级状态
                # v2.112: 改用 masked() —— AI key 的真值**绝不发给浏览器**;
                #   前端看到的是 \'******\', 回传时按它判定"未修改"(见 datasource._preserve_keys)
                cfg = ds.masked()
                local_status = {}
                for key in ('kline_dir', 'industry_map', 'concept_map',
                            'sub_to_ind', 'tdx_export_dir', 'limit_history'):
                    r = ds.test('local.' + key)
                    local_status[key] = {'path': ds.resolve(str(cfg['local'].get(key, ''))),
                                         'exists': r['ok'], 'detail': r['detail']}
                try:
                    import quant_terminal.dragon as dragon
                    lim = dragon.LIMIT_SOURCE
                except Exception:
                    lim = {'provider': 'akshare_em', 'degraded': False, 'reason': ''}
                # v2.153: 通达信 —— 只是"是否需要填 key"的状态, 绝不含 key 真值
                try:
                    from quant_terminal import tdx_fetch as _tf
                    tdx_status = _tf.token_status()
                except Exception as e:
                    tdx_status = {'configured': False, 'length': 0, 'files': [],
                                  'ini_dir': '', 'error': str(e)[:80]}
                # v2.170 (2026-10-21) 同花顺 fuyao —— 只报"有没有 key / 启用没", 不回传真值
                try:
                    from quant_terminal import fuyao as _fqf
                    _fc2 = _fqf.load_cfg()
                    fuyao_status = {'configured': bool(str(_fc2.get('key') or '').strip()),
                                    'length': len(str(_fc2.get('key') or '')),
                                    'enabled': bool(_fc2.get('enabled')),
                                    'base': _fc2.get('base')}
                except Exception as e:
                    fuyao_status = {'configured': False, 'length': 0,
                                    'enabled': False, 'base': '', 'error': str(e)[:80]}
                # v2.239 (用户 m13487): 浮窗分时的取数地址要能在设置窗口里看到
                try:
                    mini_status = mini_minute_status()
                except Exception as e:
                    mini_status = {'source': '', 'error': str(e)[:80]}
                self._json({'config': cfg, 'is_default': not os.path.exists(ds.CFG_FILE),
                            'broken': ds.is_broken(), 'local_status': local_status,
                            'realtime_last_used': ds.LAST_REALTIME_SOURCE,
                            'limit_source': lim,
                            'tdx_status': tdx_status,
                            'fuyao_status': fuyao_status,
                            'mini_status': mini_status,
                            'disabled_sources': ds.DISABLED_SOURCES})

            elif path == '/api/fundamental':
                code = qs.get('code', '600519')
                self._json(fetch_fundamental(code))

            elif path == '/api/holder_num':
                code = qs.get('code', '600519')
                self._json(fetch_holder_num(code))

            elif path == '/api/holder/update':
                self._json(self._run_holder_update())

            elif path == '/api/holder/update/progress':
                self._json(_holder_update_progress())

            elif path == '/api/refresh_prices':
                # 批量刷新指定股票实时价 (当前列表+模拟持仓, 比全市场快)
                codes = [c for c in qs.get('codes', '').split(',') if c.strip()]
                prices = {}
                if codes:
                    sina = []
                    for c in codes[:200]:
                        sina.append(_sc_of(c))
                    for i in range(0, len(sina), 50):
                        q = fetch_sina(sina[i:i + 50])
                        for sc, d in q.items():
                            px = d.get('price', 0)
                            if px > 0:
                                prices[sc[2:]] = {'price': px, 'change_pct': d.get('change_pct', 0)}
                self._json({'prices': prices, 'count': len(prices)})

            elif path == '/api/watchlist':
                # 自选股列表 (附实时价)
                wl = load_watchlist()
                codes = [w['code'] for w in wl]
                # P3-5 fix: fetch_sina内部截断50只 — 分批拉取, >50只自选时后部不再缺实时价
                rt = {}
                for i in range(0, len(codes), 50):
                    rt.update(fetch_sina([c.lower() for c in codes[i:i + 50]]))
                for w in wl:
                    q = rt.get(w['code'].lower(), {})
                    w['price'] = q.get('price', 0)
                    w['change_pct'] = q.get('change_pct', 0)
                self._json({'watchlist': wl})

            elif path == '/api/watchlist/detail':
                # v2.54 (2026-09-05): 自选股完整分析 — 结果按mode落盘watchlist_cache.json,
                # 单击读缓存(秒回), 自选增删(wl_mtime变)自动失效, force=1 强制重算
                # 左峰口径: 一律标注"最近孤立峰"(K线图蓝虚峰②), 评分/评级仍按mode档位
                #   (scan_watch_recent — 只改标注不改决策, 用户2026-09-05定案)
                mode = qs.get('mode', '100')
                # P2-6 fix: mode 白名单 — 前端在AI/智能模式下点自选会传'ai'/'smart', int('ai')抛ValueError→自选面板白屏
                if mode not in ('100', '55', '20'): mode = '100'
                force = qs.get('force', '0') == '1'
                wl = load_watchlist()
                if not wl:
                    self._json({'watchlist': [], 'total': 0}); return
                wl_mtime = os.path.getmtime(WATCH_FILE)
                if not force:
                    slot = _get_watch_slot(mode)
                    if slot and slot.get('wl_mtime') == wl_mtime and slot.get('results'):
                        self._json({'watchlist': slot['results'],
                                    'total': slot.get('total_results', len(slot['results'])),
                                    'cached': True,
                                    'calc_time': slot.get('scan_time', '')})
                        return
                t0 = time.time()
                engine = QuantEngine()
                engine.load_data(mode=mode)
                engine.calc_sector_returns()
                codes = [w['code'] for w in wl]
                # P3-5 fix: fetch_sina内部截断50只 — 分批拉取
                rt = {}
                for i in range(0, len(codes), 50):
                    rt.update(fetch_sina([c.lower() for c in codes[i:i + 50]]))
                results = []
                for w in wl:
                    full = w['code']
                    q = rt.get(full.lower(), {})
                    price = q.get('price', 0)
                    change_pct = q.get('change_pct', 0)
                    if price <= 0: continue
                    info = engine.pool.get(full, {'code': full, 'name': w['name'],
                                                  'sector': '', 'concept': ''})
                    num = full[2:]
                    info['sector'] = engine.ind_map.get(num, '')
                    info['concept'] = engine.con_map.get(num, '')
                    try:
                        r = engine.scan_watch_recent(full, info, price, change_pct, mode=mode)
                        results.append(self._summarize_result(r))
                    except Exception:
                        continue
                _cache_watch_slot(mode, wl_mtime, results)
                calc_time = datetime.now().strftime('%m-%d %H:%M:%S')
                self._json({'watchlist': results, 'total': len(results),
                            'cached': False, 'calc_time': calc_time,
                            'elapsed': round(time.time() - t0, 2)})

            elif path == '/api/stock/search':
                # 股票简称搜索 (从快照文件建名称索引; 2026-08-06)
                kw = qs.get('kw', '').strip().lower()
                if not kw:
                    self._json({'results': []}); return
                names = _load_stock_names()
                results = []
                if kw.isdigit():
                    # 代码前缀匹配
                    for code, name in names:
                        if code.startswith(kw):
                            results.append({'code': code, 'name': name})
                            if len(results) >= 10: break
                else:
                    # 简称包含匹配 (优先前缀, 再包含)
                    for code, name in names:
                        nm = name.lower()
                        if nm.startswith(kw):
                            results.append({'code': code, 'name': name})
                            if len(results) >= 8: break
                    if len(results) < 8:
                        for code, name in names:
                            nm = name.lower()
                            if kw in nm and nm not in [r['name'].lower() for r in results]:
                                results.append({'code': code, 'name': name})
                                if len(results) >= 10: break
                self._json({'results': results, 'total': len(results)})

            elif path == '/api/index/search':
                """v2.213 (2026-10-24 用户 m05977): 板块指数搜索 —— K线代码框支持文字检索板块。

                K线框原来只认「个股代码 / 个股简称」(/api/stock/search)。板块指数没有个股
                那种 6 位代码，用户只能输中文板块名 ⇒ 单开这条接口，返回：
                    {thscode, name, tag, tag_cn, detail}
                前端把它和个股结果拼在同一张候选栏里（板块项显示「代码 名称 指数」三列）。

                ★ 只返回**真板块**：883 特色统计指数 / 882 地域 一律不出现
                  （与 official_kline 的判据一致 —— 它们不是板块，点进去毫无意义）。
                ★ 概念照样吃屏蔽名单：终端里被屏蔽的概念不该在搜索里冒出来。
                ★ 排序：名称前缀命中 > 包含命中；同级内部短名优先（"文化传媒" 排在
                  "文化传媒概念" 前面），保证回车默认选中的就是最贴切的那个。
                """
                kw = (qs.get('kw', '') or '').strip()
                if not kw:
                    self._json({'results': []}); return
                try:
                    from quant_terminal import sector_kline as _sk
                    from quant_terminal.engine import load_concept_blocklist as _lbl
                    _nm = _sk.names()
                    _bl = _lbl() or frozenset()
                except Exception as _e:
                    _swlog('index_search', _e)
                    self._json({'results': [], 'error': str(_e)[:120]}); return
                _low = kw.lower()
                _hit = []
                for _tc, _info in _nm.items():
                    _tag = (_info or {}).get('tag') or ''
                    if _tag in ('tszs', 'region'):
                        continue                      # 883 统计指数 / 882 地域: 不是板块
                    _n = (_info or {}).get('name') or ''
                    if not _n:
                        continue
                    if _tag == 'cn_concept' and _n in _bl:
                        continue                      # 终端屏蔽概念: 搜索里也不出现
                    _nl = _n.lower()
                    if _low == _nl:
                        _rank = 0                     # 完全同名
                    elif _nl.startswith(_low):
                        _rank = 1                     # 前缀
                    elif _low in _nl:
                        _rank = 3                     # 包含
                    else:
                        continue
                    _hit.append((_rank, len(_n), _n, _tc, _tag))
                _hit.sort()
                # v2.213b: 同花顺的行业有两级，下钻通道**不同** ——
                #   881xxx = 上级行业(90 类)，档位/成分股在 parent_trend.json ⇒ kind='parent'
                #   884xxx = 细分行业(230 类)，在 trend_data.json        ⇒ kind='sector'
                #   概念(885/886)                                        ⇒ kind='concept'
                # 一开始这里一律给 'sector'，于是搜「文化传媒」(881164) 下钻报「未找到板块」。
                #   现在把 kind 一并返回，前端照抄，不再自己从 tag 猜。
                _TAGC = {'industry': '细分行业', 'cn_concept': '概念'}
                _out = []
                for _rank, _ln, _n, _tc, _tag in _hit[:20]:
                    _cnt = ''
                    try:
                        _rows = _sk.read_kline(_tc, limit=1)
                        if _rows:
                            _cnt = _rows[-1]['date']
                    except Exception:
                        _cnt = ''
                    _pre3 = (_tc or '')[:3]
                    if _tag == 'cn_concept':
                        _kd, _tcn = 'concept', '概念'
                    elif _pre3 == '881':
                        _kd, _tcn = 'parent', '上级行业'
                    else:
                        _kd, _tcn = 'sector', _TAGC.get(_tag, '行业')
                    _out.append({'thscode': _tc, 'name': _n, 'tag': _tag,
                                 'kind': _kd, 'tag_cn': _tcn,
                                 'detail': _cnt})
                self._json({'results': _out, 'total': len(_out)})

            elif path == '/api/bank_minute':
                # v2.236 (2026-10-29 m11003): 悬停 RSRS/情绪 → 左侧银行分时浮窗 (两条线)
                try:
                    self._json(bank_minute())
                except Exception as _e:
                    _swlog('bank_minute_route', _e)
                    self._json({'ok': False, 'error': str(_e)[:200]})

            elif path == '/api/mini_minute':
                # v2.239 (2026-10-29 m13487): 浮窗分时
                #   kind=sector 板块/概念(自动带上级对照线) | stock 个股 | index 大盘指数
                #   ttl=0 可强制刷新 (调试用), 缺省 20 秒
                try:
                    _t = float(qs.get('ttl') or _MINI_TTL)
                except (TypeError, ValueError):
                    _t = _MINI_TTL
                try:
                    self._json(mini_minute(qs.get('kind') or 'sector',
                                           qs.get('code') or '', ttl=_t))
                except Exception as _e:
                    _swlog('mini_minute_route', _e)
                    self._json({'ok': False, 'error': str(_e)[:200], 'lines': []})

            elif path == '/api/minute':
                # 分时图数据 (腾讯分钟接口, 仅当日; 2026-08-06)
                code = qs.get('code', '000938')
                if _idxr.norm(code) in INDEX_CODES and _idxr.src_of(code) == 'ths':
                    # v2.360: 同花顺全A 腾讯 minute 不认, 走同花顺分时转成同样的 points 形状
                    # v2.360c: **写完必须 return** —— 这是个终态分支。原版漏了 return,
                    #   控制流会落到下面的 fetch_minute(sc), 而 sc 在这条路没赋值 ⇒
                    #   NameError 被 except 吞掉后**又写第二个 JSON** 进同一个响应,
                    #   客户端 json.loads 直接报 Extra data。
                    _hm = _ths_minute_points(code)
                    if _hm:
                        self._json(_hm)
                    else:
                        self._json({'error': '分时数据获取失败'})
                    return
                elif code.lower() in INDEX_CODES:
                    sc = code.lower()  # 指数代码直接使用 (2026-08-07)
                else:
                    # v2.279 (用户 m10425): 北交所要拼 bj, 老判据把 920xxx 当沪市 ⇒ 分时永远空
                    _n = mcap.bare(code)
                    _p = mcap.prefixed(_n)[:2]
                    sc = ('bj' if _p == 'BZ' else _p.lower()) + _n
                # v2.120: 取数抽成模块级 fetch_minute() —— 与 AI 工具 get_intraday 共用
                #   同一个实现 (原先 handler 里 name 兜底用 `num`, 指数走这条时 num 还没定义)。
                try:
                    points, prev_close, name, mdate = fetch_minute(sc)
                    self._json({'points': points, 'prev_close': prev_close, 'name': name,
                                'date': mdate})
                except Exception:
                    self._json({'error': '分时数据获取失败'})

            elif path == '/api/kline5':
                # 5分钟K线 (v2.94r 2026-09-14): 新浪 scale=5, 固定最新100根 (≈2个交易日, 48根/日)
                # 为什么用新浪不用腾讯: 新浪 datalen 上限按"根数"(1023)卡而非跨度, 且腾讯 n>700
                #   会被砍回 320; 实测同 scale 新浪深度全面胜出 (5分: 22天 vs 15天)。这里只取100根。
                # 量纲: 新浪返回"股", 终端统一"手" → /100 (9/14 实测 1,657,146股 vs CSV 16,571手)
                code = qs.get('code', '000938')
                if _idxr.norm(code) in INDEX_CODES and _idxr.src_of(code) == 'ths':
                    # v2.360: 新浪 5 分 K 认不了 883957。明确回空 + 原因, 不静默成功。
                    self._json({'points': [], 'error': '同花顺全A 无 5 分钟K线源'})
                    return
                if code.lower() in INDEX_CODES:
                    sc = code.lower()  # 指数代码直接用 (同 /api/minute)
                else:
                    # v2.279 (用户 m10425): 北交所要拼 bj, 老判据把 920xxx 当沪市 ⇒ 5分图永远空
                    _n5 = mcap.bare(code)
                    _p5 = mcap.prefixed(_n5)[:2]
                    sc = ('bj' if _p5 == 'BZ' else _p5.lower()) + _n5
                try:
                    u = ('https://quotes.sina.cn/cn/api/json_v2.php/'
                         'CN_MarketDataService.getKLineData'
                         f'?symbol={sc}&scale=5&ma=no&datalen=100')
                    resp = _req_get(u, timeout=8,
                                    headers={'Referer': 'https://finance.sina.com.cn/'})
                    arr = resp.json()
                    if not isinstance(arr, list) or not arr:
                        raise Exception('empty payload')
                    bars = []
                    for x in arr:
                        day = x['day']            # '2026-09-14 15:00:00'
                        bars.append({'d': day[:10], 't': day[11:16],
                                     'o': float(x['open']), 'h': float(x['high']),
                                     'l': float(x['low']),  'c': float(x['close']),
                                     'v': round(float(x['volume']) / 100.0, 2)})
                    # 名称/昨收走实时接口 (失败不影响主数据: 图照样画, 只是标题缺名)
                    name, pc = '', 0
                    try:
                        q = fetch_sina([sc]).get(sc, {})
                        name = q.get('name', '') or name
                        pc = q.get('prev_close', 0) or 0
                    except Exception as _e:
                        _swlog('kline5_name_fetch', _e)
                    self._json({'bars': bars, 'name': name, 'prev_close': pc,
                                'code': sc, 'n': len(bars)})
                except Exception:
                    self._json({'error': '5分钟数据获取失败'})

            elif path == '/api/kline':
                code = qs.get('code', '000938')
                # v2.244 (用户 m20802): limit=N ⇒ 只要最后 N 根。
                #   三个趋势窗口悬停「最大市值成分股」画 50 日日K浮窗就走这里 ——
                #   不带 limit 的老调用方一字不变（整份 1600+ 根 + MA + 画线）。
                # >>> WAVE BEGIN (v2.259 2026-11-07) K线画线口径跟列表模式走
                #   用户 m01825: 列表在「波段低吸」模式时, K线图要屏蔽左峰/止损, 改画趋势线。
                #   前端 loadKline() 把 window._scanMode 当 mode 传过来, 只有 wave 会走趋势线分支;
                #   不带 mode 的老调用方 (悬停浮窗/其他页面) 行为一字不变。
                # <<< WAVE END
                self._serve_kline(code, limit=qs.get('limit', ''), mode=qs.get('mode', ''),
                                  asrc=qs.get('asrc', ''), bsrc=qs.get('bsrc', ''))

            elif path == '/api/stock':
                code = qs.get('code', '000938')
                self._serve_stock_detail(code)

            elif path == '/api/scan/start':
                mode = qs.get('mode', '20')
                lines = qs.get('lines', '4') if qs.get('lines', '4') in ('3', '4', '5', '6') else '4'
                # v2.31 (2026-09-01): 股池来源 — auto=自建池(同花顺条件全市场) file=导出池(读文件)
                pool = qs.get('pool', 'auto')
                if pool not in ('auto', 'file'):
                    pool = 'auto'
                # v2.23 (2026-08-30): 不再拒绝运行中的请求 — 新扫描直接替换旧扫描
                # (run_scan_async 内部 scan_id+1 换代, 旧线程自我退出, 旧结果不覆盖)
                was_running = SCAN_STATE['running']
                t = threading.Thread(target=run_scan_async, args=(mode, lines, pool), daemon=True)
                t.start()
                self._json({'started': True,
                            'msg': '已取消旧扫描, 开始新扫描' if was_running else '扫描启动',
                            'mode': mode, 'lines': lines, 'pool': pool})

            elif path == '/api/scan/result':
                if SCAN_STATE['done'] and SCAN_STATE['results']:
                    # v2.12 (2026-08-15): 滚动加载 — 前端分页, 后端一次返回全部
                    # (数据量可控: 池子一般几十到几百只; 若超500只再考虑后端分页)
                    all_results = SCAN_STATE['results'][:500]
                    summary = {
                        'scan_time': SCAN_STATE['scan_time'],
                        'pool_size': SCAN_STATE['pool_size'],
                        'total_results': len(SCAN_STATE['results']),
                        'lines': SCAN_STATE.get('lines', '4'),  # v2.4 档位标注 (2026-08-05)
                        'top30': [self._summarize_result(r) for r in all_results[:30]],
                        'all_results': [self._summarize_result(r) for r in all_results],
                    }
                    # 模块计数
                    # 当前模式统计
                    mode = SCAN_STATE.get('mode', '20')
                    if mode == 'dragon':
                        # v2.16 龙头战法: 龙回头企稳=买入信号, 妖龙高分=观察
                        summary['buy_signals'] = sum(1 for r in SCAN_STATE['results']
                                                     if r.get('group') == 'pullback'
                                                     and r.get('signal') in ('缩量企稳', '横盘企稳'))
                        summary['watch_signals'] = sum(1 for r in SCAN_STATE['results']
                                                       if r.get('group') == 'dragon'
                                                       and r.get('score', 0) >= 70)
                    # >>> AUCTION BEGIN
                    elif mode == 'auction':
                        # 竞价打板 (2026-11-06): 无 consensus/score 语义, 按 verdict 三态计数
                        _rs = SCAN_STATE['results']
                        summary['buy_signals'] = sum(1 for r in _rs if r.get('verdict') == 'exec')
                        summary['watch_signals'] = sum(1 for r in _rs if r.get('verdict') == 'watch')
                        summary['auction_info'] = SCAN_STATE.get('auction_info') or {}
                        summary['module_stats'] = {
                            'auction_total': len(_rs),
                            'auction_exec': summary['buy_signals'],
                            'auction_watch': summary['watch_signals'],
                            'pool_size': SCAN_STATE.get('pool_size', 0),
                            'mode': mode}
                    # <<< AUCTION END
                    # >>> REBOUND BEGIN
                    elif mode == 'rebound':
                        # 超跌反弹 (2026-11-07): 无 consensus/score 语义, 按反弹幅度分档计数
                        #   "买点" = 反弹 ≤8% (刚起来, 还有空间) / "观察" = 8~13% (已走一段)
                        _rs = SCAN_STATE['results']
                        summary['buy_signals'] = sum(1 for r in _rs
                                                     if (r.get('rebound') or 99) <= 8.0)
                        summary['watch_signals'] = sum(1 for r in _rs
                                                       if 8.0 < (r.get('rebound') or 99) <= 13.0)
                        summary['rebound_info'] = SCAN_STATE.get('rebound_info') or {}
                        _ri = summary['rebound_info']
                        summary['module_stats'] = {
                            'rebound_total': len(_rs),
                            'rebound_buy': summary['buy_signals'],
                            'rebound_watch': summary['watch_signals'],
                            'pool_size': SCAN_STATE.get('pool_size', 0),
                            'data_date': _ri.get('date'),
                            'mode': mode}
                    # <<< REBOUND END
                    # >>> WAVE BEGIN
                    elif mode == 'wave':
                        # 波段低吸 v2.260: 入选的每一条**都是买点**(首次回踩当天), 不再分档。
                        #   旧口径(v2.258)按离线距离分"买点≤0.5%/观察0.5~1.5%",
                        #   新口径里离线 0~1.5% 是硬门槛, 分档失去意义。
                        _rs = SCAN_STATE['results']
                        summary['buy_signals'] = len(_rs)
                        summary['watch_signals'] = 0
                        summary['wave_info'] = SCAN_STATE.get('wave_info') or {}
                        # v2.271 (用户 m07829): 六口径全量行数据 — 扫完这一下发,
                        #   前端点按钮就能原地换表(不必重扫)。每行已过 _summarize_result。
                        summary['wave_combos'] = SCAN_STATE.get('wave_combos') or {}
                        _wi = summary['wave_info']
                        summary['module_stats'] = {
                            'wave_total': len(_rs),
                            'wave_buy': len(_rs),
                            'wave_watch': 0,
                            'pool_size': SCAN_STATE.get('pool_size', 0),
                            'data_date': _wi.get('date'),
                            'mode': mode}
                    # <<< WAVE END
                    elif mode == 'glue':
                        # v2.138 (2026-10-13) 均线粘合: score = 粘合度(%), 不是 13 分制 → 换判据
                        #   买点 = 粘合度 ≤1% (三线几乎重合, 最紧的试错位) / 观察 = 1~2%
                        summary['buy_signals'] = sum(
                            1 for r in SCAN_STATE['results']
                            if (r.get('glue') or 99) <= 1.0)
                        summary['watch_signals'] = sum(
                            1 for r in SCAN_STATE['results']
                            if 1.0 < (r.get('glue') or 99) <= 2.0)
                    else:
                        summary['buy_signals'] = sum(1 for r in SCAN_STATE['results']
                                                     if r['consensus']['score'] >= 13)
                        summary['watch_signals'] = sum(1 for r in SCAN_STATE['results']
                                                       if 8 <= r['consensus']['score'] < 13)
                    if mode == 'dragon':
                        summary['module_stats'] = {
                            'dragon_total': sum(1 for r in SCAN_STATE['results']
                                                if r.get('group') == 'dragon'),
                            'pullback_total': sum(1 for r in SCAN_STATE['results']
                                                  if r.get('group') == 'pullback'),
                            'mode': mode}
                    elif mode == 'ai':
                        # v2.66 涨停突破: score = 峰期 (100/55/20), 不存在"≥9分"语义 → 改数百日峰
                        top5 = sum(1 for r in SCAN_STATE['results'] if r['consensus']['score'] == 100)
                        summary['module_stats'] = {'ai_top': top5, 'mode': mode}
                    elif mode == 'smart':
                        # v2.67 涨停回调: score = 类型档 (收1/中2/开3), 不存在"≥9分"语义
                        _rs = SCAN_STATE['results']
                        summary['module_stats'] = {
                            'smart_total': len(_rs),
                            'kind_cnt': {k: sum(1 for r in _rs if r.get('kind') == k)
                                         for k in ('不破收', '不破中', '不破开')},
                            'pool_size': SCAN_STATE.get('pool_size', 0),
                            'mode': mode}
                    # >>> AUCTION BEGIN
                    elif mode == 'auction':
                        # 竞价打板: 第一段已按 verdict 填好 buy/watch/module_stats
                        # 这里落 else 分支会被 breakout 统计覆盖, 必须显式拦截
                        pass
                    # <<< AUCTION END
                    # >>> REBOUND BEGIN
                    elif mode == 'rebound':
                        # 超跌反弹: 第一段已填好 buy/watch/module_stats
                        # 这里落 else 分支会被 breakout 统计覆盖, 必须显式拦截
                        pass
                    # <<< REBOUND END
                    # >>> WAVE BEGIN
                    elif mode == 'wave':
                        # 波段低吸: 第一段已填好 buy/watch/module_stats
                        # 这里落 else 分支会被 breakout 统计覆盖, 必须显式拦截
                        pass
                    # <<< WAVE END
                    elif mode == 'glue':
                        # v2.138 均线粘合: score = 粘合度%, 无「≥9分」语义
                        _rs = SCAN_STATE['results']
                        summary['module_stats'] = {
                            'glue_total': len(_rs),
                            'glue_tight': sum(1 for r in _rs if (r.get('glue') or 99) <= 1.0),
                            'glue_vol': sum(1 for r in _rs if (r.get('vol_ratio') or 0) >= 1.2),
                            'pool_size': SCAN_STATE.get('pool_size', 0),
                            'mode': mode}
                    else:
                        bt = sum(1 for r in SCAN_STATE['results'] if r.get('breakout',{}).get('signal','') in ('BREAKOUT', 'PULLBACK'))
                        near = sum(1 for r in SCAN_STATE['results'] if r.get('breakout',{}).get('signal','') == 'NEAR_HIGH')
                        m4_ok = sum(1 for r in SCAN_STATE['results'] if r.get('m4_verify',{}).get('passed',0) >= 3)
                        summary['module_stats'] = {'breakout': bt, 'near_high': near, 'm4_all_pass': m4_ok, 'mode': mode}
                    self._json(summary)
                else:
                    self._json({'error': '无扫描结果', 'done': SCAN_STATE['done'],
                               'scan_error': SCAN_STATE.get('error'), 'running': SCAN_STATE['running']})

            elif path == '/api/scan/cache':
                """v2.53 (2026-09-05): 读落盘槽 — 单击看上次结果不重扫
                v2.107 (2026-09-23): 定位键加第三维档位 (lines)。
                last=1 → 「last 的池与模式 + 前端当前档位」那一槽; 否则 mode+pool+lines 取指定槽。
                找不到 → found:false + key (key.have_lines = 该(池×模式)上次实际扫过的档位,
                前端据此提示"X线档有结果", 但**绝不自动切档**)"""
                if qs.get('last', '0') == '1':
                    slot, key = _get_last_scan_slot(qs.get('lines', '4'))
                else:
                    mode = qs.get('mode', '')
                    pool = qs.get('pool', 'auto') if qs.get('pool', 'auto') in ('auto', 'file') else 'auto'
                    if mode not in SCAN_MODES:
                        self._json({'found': False, 'error': 'bad mode'})
                        return
                    slot, key = _get_scan_slot_keyed(pool, mode, qs.get('lines', '4'))
                if not slot:
                    # have_lines: 该 (池×模式) **上次实际扫过的**档位; 从没扫过任何槽时给空串
                    # (前端靠它区分"这一档没扫过"和"整个组合都没扫过", 空串=后者, 不提示)
                    self._json({'found': False,
                                'key': (key['pool'] + '_' + key['mode'] + '_' + key['lines']) if key else '',
                                'lines': key['lines'] if key else _norm_lines(qs.get('lines', '4')),
                                'have_lines': key['have_lines'] if key else ''})
                    return
                self._json({'found': True, 'key': key['pool'] + '_' + key['mode'] + '_' + key['lines'],
                            'slot': {'mode': slot.get('mode'), 'pool': slot.get('pool'),
                                     'lines': slot.get('lines'), 'scan_time': slot.get('scan_time'),
                                     # v2.106: 价最近一次回写时刻 (空=从没刷过, 价=扫描那一刻的)
                                     'px_time': slot.get('px_time', ''),
                                     'total_results': slot.get('total_results', len(slot.get('results') or [])),
                                     # >>> AUCTION BEGIN (v2.249) 落盘里的结论下发给前端
                                     'auction_info': slot.get('auction_info'),
                                     # <<< AUCTION END
                                     # >>> REBOUND BEGIN (v2.257 2026-11-07) 超跌反弹的观察日/漏斗
                                     #   ★ 上面这个 'slot' 字典是**白名单**, 不在这里列出的键
                                     #     一律不下发 —— 落盘里有、API 里没有就是这个原因
                                     'rebound_info': slot.get('rebound_info'),
                                     # <<< REBOUND END
                                     # >>> WAVE BEGIN (v2.258 2026-11-07) 波段低吸的观察日/漏斗
                                     #   ★ 上面这个 'slot' 字典是**白名单**, 不在这里列出的键
                                     #     一律不下发 —— 落盘里有、API 里没有就是这个原因
                                     'wave_info': slot.get('wave_info'),
                                     # v2.271 (2026-11-11 用户 m07829): 六口径行数据
                                     #   {combo_key: {'rows': [...]}} —— 前端点按钮切口径时
                                     #   直接换这个数组, 不必重新扫一遍
                                     'wave_combos': slot.get('wave_combos'),
                                     # <<< WAVE END
                                     'results': slot.get('results') or []}})

            elif path == '/api/index/status':  # v2.57: 指数日K本地文件状态 (exists/count/first/last)
                self._json(_index_local_status())

            elif path == '/api/index/sync':  # v2.57: 手动触发7码落盘 (force=1 盘中也可跑, 仍不写forming bar)
                force = qs.get('force', '0') == '1'
                threading.Thread(target=_run_index_sync, args=(force,), daemon=True).start()
                self._json({'ok': True, 'msg': '指数日K同步已启动'})

            elif path == '/api/sentiment':
                # v2.62: 盘中每分钟拉一次两市分钟成交额并落盘 (内部节流, 首启整窗刷新, 异步不阻塞响应)
                # v2.63: 情绪分日度落盘; 盘后且成交额已结算 → 直接定稿
                # (成交额 complete 后情绪分不再变, 不必等 /api/status 的补算)
                # v2.119: 同 /api/status —— 冷启动时 is_holiday() 手里没日期, 先补一次取数。
                #   这里**不重排** ensure()/fetch() 的先后: fetch() 里的 turnover.context()
                #   依赖 ensure() 已经跑过, 换序会改 vol_status 的取值。
                #   只在 LAST_SESSION 还空着时补 —— 正常路径下它是热的, 一行都不执行。
                if not ds.last_session(None):
                    get_market_indices()
                _ph = get_trading_status()
                turnover.ensure(_ph)
                turnover.capture_bj(_ph)     # v2.64 方案A: 同上, 两条路径任一被轮询都能采到
                _s = MarketSentiment().fetch()
                macro_daily.record(senti=_s, final=(
                    _ph == 'POST_MARKET' and bool((_s.get('turnover') or {}).get('settled'))))
                _s['macro_hist'] = macro_daily.view()
                # v2.70 (2026-09-12): 顶栏七大指数悬停浮窗 — 前5交易日逐日 收盘/涨跌幅/趋势档位
                # (本地指数日K现算, 60秒类级缓存; 与 index_regimes 同一套判定式)
                _s['index_hist'] = index_hist_recent()
                self._json(_s)

            elif path == '/api/kline/status':
                self._json(get_kline_status())

            elif path == '/api/mcap':
                # v2.131 (2026-10-01): 全市场市值 {裸码: [总市值, 流通市值]} (亿元, 进程内缓存5分钟)
                #   前端只在开机/快照导入后拉一次 —— 悬停浮窗与「简称」列排序都吃这一份, 不再逐票请求。
                self._json(mcap.payload())

            elif path == '/api/zt_reason':
                # v2.190 (2026-10-23): 各选股模式列表的「涨幅」格悬停显示涨停原因 {裸码: 原因}
                #   数据来自 dragon.fetch_limit_pool()（东财主池 + 同花顺补原因，当日缓存）
                try:
                    import quant_terminal.dragon as _dgr
                    _rp = _dgr.fetch_limit_pool(max_age=300)
                    _mp = {}
                    for _r in (_rp or []):
                        _why = (_r.get('reason') or '').strip()
                        if _why and _r.get('code'):
                            _mp[str(_r['code'])] = _why
                    self._json({'ok': True, 'n': len(_mp), 'reasons': _mp})
                except Exception as _e:
                    self._json({'ok': False, 'msg': str(_e)[:80], 'reasons': {}})

            elif path == '/api/limit20':
                # v2.147 (2026-10-16): 批量 20 日涨停数 {裸码: n} — 9 个股票列表「代码」列染色
                #   规则 (用户 m05012/m05047 定案): 0=保持现色(cyan) / 1=黄 / 2=橙 / >=3=红,
                #   v2.170 (2026-10-21 m00524): 口径改为「今天之前的 20 个交易日」，**不含当天**
                #   (dragon.count_limit20 内部已剔除"date==今天"那根)；双创仍按 20% 阈值。
                #   走批量端点而不是给每个 scan 函数加字段: ① 旧落盘缓存也能立即染色, 不必重扫
                #   ② 全终端只有这一处 20 日口径实现 (铁律 17) ③ 行业/概念下钻天然覆盖。
                codes = qs.get('codes', '')
                if not codes:
                    codes = ','.join(qs.getlist('code')) if hasattr(qs, 'getlist') else ''
                if not codes:
                    self._json({})
                    return
                import quant_terminal.dragon as _dragon
                out = {}
                now = time.time()
                for raw_code in codes.split(','):
                    code = raw_code.strip().upper()
                    if len(code) != 6 or not code.isdigit():
                        continue
                    hit = _LIMIT20_CACHE.get(code)
                    if hit and now - hit[0] < 300:
                        out[code] = hit[1]
                        continue
                    try:
                        # KLINE_DIR 下文件名带 SH/SZ 前缀 (BJ 票不参与 9 列表, 不查)
                        fpath = os.path.join(KLINE_DIR, 'SH%s.csv' % code)
                        if not os.path.exists(fpath):
                            fpath = os.path.join(KLINE_DIR, 'SZ%s.csv' % code)
                        tail = _read_csv_tail(fpath, 22)
                        n20 = _dragon.count_limit20(tail, code) if tail else -1
                    except Exception:
                        n20 = -1
                    _LIMIT20_CACHE[code] = (now, n20)
                    out[code] = n20
                self._json(out)

            elif path == '/api/lhb':
                # v2.194 龙虎榜（右键「龙头战法」打开的窗口）
                _date = (qs.get('date', '') or '').strip()
                _board = (qs.get('board', 'all') or 'all').strip()
                if _board not in ('all', 'org', 'hot_money'):
                    _board = 'all'
                try:
                    from quant_terminal import fuyao_lhb as _lhb
                    _d = _lhb.lhb_list(_date or None, _board)
                    # v2.304 (2026-11-14 用户 m16852): 给每行打「债」标 —— 转债正股。
                    #   数据源见 quant_terminal/kzz.py（东财 RPT_BOND_CB_LIST，存续 316 只正股）。
                    #   失败绝不影响榜单: 整段 try/except 吞掉, is_kzz 保持缺省（前端按"没有"处理）。
                    try:
                        from quant_terminal import kzz as _kzz
                        _ks = _kzz.stock_set()
                        for _r in (_d.get('rows') or []):
                            _c = str(_r.get('ticker') or '')
                            _r['is_kzz'] = bool(_c and _c in _ks)
                    except Exception:
                        pass
                    # v2.323: 三种榜单各存一份（all/org/hot_money），打开窗口先读它秒显
                    _arc.save_window('lhb_' + ('hm' if _board == 'hot_money'
                                               else 'org' if _board == 'org' else 'all'), _d)
                    self._json(_d)
                except Exception as _e:
                    self._json({'ok': False, 'error': str(_e)[:200]})

            elif path == '/api/lhb_inst':
                # v2.194 个股席位明细（机构合计 + 买卖营业部）
                _code = (qs.get('code', '') or '').strip()
                _date = (qs.get('date', '') or '').strip()
                try:
                    from quant_terminal import fuyao_lhb as _lhb
                    self._json(_lhb.lhb_inst(_code, _date or None))
                except Exception as _e:
                    self._json({'ok': False, 'error': str(_e)[:200]})

            elif path == '/api/lhb_dept':
                # v2.194 营业部画像（点击营业部名）
                _did = (qs.get('id', '') or '').strip()
                try:
                    from quant_terminal import fuyao_lhb as _lhb
                    self._json(_lhb.lhb_dept(_did, int(qs.get('page', '1') or 1), int(qs.get('size', '20') or 20)))
                except Exception as _e:
                    self._json({'ok': False, 'error': str(_e)[:200]})

            elif path == '/api/recap/reports':
                # v2.302 (2026-11-13 用户 m16307): AI 复盘历史存档列表（TSP /api/market-recap/reports）
                try:
                    from quant_terminal import market_recap as _mrc
                    self._json({'ok': True, 'reports': _mrc.list_reports()})
                except Exception as _e:
                    self._json({'ok': False, 'error': str(_e)[:200]})

            elif path == '/api/hot_list':
                # v2.194 同花顺热榜（板块热度面板 Top10 / 全量窗口 100 名）
                _per = (qs.get('period', 'day') or 'day').strip()
                try:
                    _lim = int(qs.get('limit', '100') or 100)
                except Exception:
                    _lim = 100
                try:
                    from quant_terminal import fuyao_lhb as _lhb
                    _hr = _lhb.hot_list(_per, _lim)
                    # v2.207 (2026-10-23 用户 m04644): 全量热榜窗口要显示 行业/概念/市值
                    #   同花顺热榜接口本身只给 名次/代码/简称/热度/升降 —— 行业与概念从本地 map 补,
                    #   板块状态走股票列表同一套 (_attach_trend_status), 前端才能用同一套配色。
                    #   市值不在这里塞: 前端已有 MCAP 全市场表 (/api/mcap 开机拉一次), 直接查表更省。
                    try:
                        if isinstance(_hr, dict) and _hr.get('ok'):
                            _im, _cm = load_industry_concept_maps()
                            _rows2 = []
                            for _row in (_hr.get('rows') or []):
                                _r2 = dict(_row)
                                _tk = str(_r2.get('ticker') or '')
                                _num = _tk[-6:] if len(_tk) >= 6 else _tk
                                _r2['ticker'] = _num
                                _r2['sector'] = _im.get(_num, '')
                                _r2['concept'] = _cm.get(_num, '')
                                try:
                                    self._attach_trend_status(_r2)
                                except Exception:
                                    _r2['sector_status'] = None
                                    _r2['sector_pct'] = None
                                    _r2['concept_status'] = {}
                                _rows2.append(_r2)
                            _hr = dict(_hr, rows=_rows2)
                    except Exception as _e2:
                        print('[hot_list] 行业/概念补充失败: %s' % _e2)
                    self._json(_hr)
                except Exception as _e:
                    self._json({'ok': False, 'error': str(_e)[:200]})

            elif path == '/api/limit_pools':
                # v2.194 涨停池五件套（龙虎榜窗口右侧再展开一层）
                #   kind one of: up / down / break / ladder / stat
                _kind = (qs.get('kind', 'up') or 'up').strip()
                _date = (qs.get('date', '') or '').strip()
                try:
                    from quant_terminal import fuyao_limit as _fl
                    # 空日期一律落到「上一个交易日」—— 传 '' 进去会被 _norm_date 当成今天，
                    #   今天盘中池子还没生成 ⇒ 前端看到空表。
                    _dd = _date or _fl.default_pool_date()
                    if _kind == 'down':
                        _r = _fl.limit_down_pool(_dd)
                    elif _kind == 'break':
                        _r = _fl.limit_break_pool(_dd)
                    elif _kind == 'ladder':
                        _raw = _fl.limit_ladder() or {}
                        _it = (_raw.get('item') or [])
                        _boards = (_it[0].get('boards') or {}) if _it else {}
                        _label = {'one_board': '一板', 'two_board': '二板', 'three_board': '三板',
                                  'four_board': '四板', 'five_board': '五板', 'six_board': '六板',
                                  'seven_over': '七板以上'}
                        _lads = []
                        for _k, _lab in _label.items():
                            _st = _boards.get(_k) or []
                            if not _k.endswith('board'):
                                continue
                            if _st:
                                _lads.append({'key': _k, 'name': _lab, 'stocks': _st})
                        _r = {'ok': True, 'date': (_it[0].get('date') if _it else ''),
                              'ladders': _lads,
                              'window': (_raw.get('window') or {})}
                    elif _kind == 'stat':
                        _st2 = _fl.limit_statistics(_dd) or {}
                        if not _st2.get('trade_date'):
                            _st2 = _fl.limit_statistics(_fl.default_pool_date()) or {}
                        _r = {'ok': True,
                              'date': _st2.get('trade_date') or _dd,
                              'overview': {
                                  'up_count': _st2.get('limit_up_stock_count'),
                                  'touched_count': _st2.get('touched_limit_up_stock_count'),
                                  'open_count': _st2.get('open_stock_count'),
                                  'seal_rate': _st2.get('seal_rate'),
                              },
                              'page_boards': _st2.get('page_boards') or [],
                              'exact_boards': _st2.get('exact_boards') or []}
                    else:
                        _r = _fl.limit_up_pool(_dd)
                    if isinstance(_r, tuple):
                        _r = _r[0]
                    if isinstance(_r, list):
                        _r = {'ok': True, 'date': _dd, 'rows': _r}
                    self._json(_r if isinstance(_r, dict) else {'ok': True, 'rows': []})
                except Exception as _e:
                    self._json({'ok': False, 'error': str(_e)[:200]})

            elif path == '/api/ladder':
                # v2.299 (2026-11-13 用户 m15118): 连板梯队（TSP limit-ladder 移植）
                #   ?direction=up|down & date=YYYY-MM-DD & force=1
                #   不复用 /api/limit_pools?kind=ladder —— 那份只有 {ticker,name,board_num},
                #   没有涨幅/封单/炸板断板状态，做不出卡片；这里用涨停池+炸板池+昨日涨停池重算。
                try:
                    from quant_terminal import ladder_scan as _lad
                    _ld = (qs.get('direction', 'up') or 'up').strip()
                    _lf = (qs.get('force', '0') or '0').strip() in ('1', 'true', 'yes')
                    _ldd = _lad.ladder((qs.get('date', '') or '').strip(), _ld, force=_lf)
                    # v2.323: 涨停/跌停两张梯队各存一份, 打开窗口先读它秒显
                    _arc.save_window('ladder_' + ('down' if _ld == 'down' else 'up'), _ldd)
                    self._json(_ldd)
                except Exception as _e:
                    self._json({'ok': False, 'error': str(_e)[:200]})

            elif path == '/api/quote/cycle':
                # v2.170 (2026-10-21) 用户 m00787: 停在哪页刷哪页 —— 三源轮换实时行情
                #   同花顺 → 腾讯 → 新浪, 每 3 秒一次, 一圈 9 秒 (轮换游标在服务端, 见 quote_cycle)
                #   ?codes=SH600519,SZ000001,...  返回 {ok,source,ts,quotes,holds,errors,fails}
                #   持仓恒带上但单独放 holds, 前端不插进当前列表。
                _codes = qs.get('codes', '')
                if not _codes:
                    _codes = ','.join(qs.getlist('code')) if hasattr(qs, 'getlist') else ''
                try:
                    self._json(quote_cycle(_codes))
                except Exception as _e:
                    _qc_log('异常', 'cycle', str(_e)[:100])
                    self._json({'ok': False, 'source': '', 'quotes': {}, 'holds': {},
                                'errors': {'cycle': str(_e)[:100]}})

            elif path == '/api/kline/update':
                # v2.177 (2026-10-22): 「📥 快照导入」随导出快照文件一起下线 (用户 m02099)。
                #   路由保留只为给出明确提示 —— 功能已被「⚡ 快照更新」(同花顺/腾讯/新浪三级) 取代。
                self._json({'ok': False, 'msg': '「快照导入」已下线 — 请用「⚡ 快照更新」'})

            elif path == '/api/kline/update_snapshot':  # v2.30: ⚡ 批量快照更新 (腾讯qt)
                limit = int(qs.get('limit', '0') or 0)
                self._json(self._run_kline_update_snapshot(limit))

            elif path == '/api/kline/update/progress':
                self._json({
                    'running': UPDATE_STATE['running'],
                    'progress': UPDATE_STATE['progress'],
                    'error': UPDATE_STATE.get('error', ''),
                    'started_at': UPDATE_STATE['started_at'],
                    'kind': UPDATE_STATE.get('kind', ''),
                    'percent': UPDATE_STATE.get('percent', 0),
                })

            elif path == '/api/sector_ma':
                ind_map, _ = load_industry_concept_maps()
                lines = qs.get('lines', '4') if qs.get('lines', '4') in ('3', '4', '5', '6') else '4'
                force = qs.get('force', '0') == '1'
                if force:
                    # 更新板块: 清空四档缓存 + 后台重算全部档位 (2026-08-05)
                    # v2.61: persist_ma=True → 全档(4/5/6线)滚动双槽基线落盘
                    _sector_ma_cache_clear()
                    threading.Thread(target=_prewarm_all_lines,
                                     kwargs={'persist_ma': True}, daemon=True).start()
                mp = LINES_MAP.get(lines, (5, 10, 21, 55))
                # v2.3盘中: 实时价覆盖最后一根K线(趋势随盘中变化)
                # v2.14盘后: 传None命中'close'预热缓存 (盘后价=收盘价, 覆盖无意义)
                rt = get_realtime_prices_cached()
                if not _is_trade_window():
                    rt = None
                # v2.61: 升降徽标 — 非交易窗口且近2日 force 过 → 直接返回基线快照(只读不自动重算);
                # 否则现算(与快照同源同值), 附徽标(基准=最近 force)。force 主线程响应不附(后台滚动后弹窗重拉即对)
                base_date, deltas, snap_rows, snap_ts = None, {}, None, None
                if not force and rt is None:
                    bd, rows, dls, ts0 = ma_baseline_snapshot('sector', lines)
                    if rows and _recent_data_date(bd):   # v2.220: 数据日口径(原为墙钟今日/昨日)
                        snap_rows, snap_ts, base_date, deltas = rows, ts0, bd, dls
                if snap_rows is not None:
                    results = snap_rows
                else:
                    results = analyze_sector_ma(ind_map, mp, realtime_prices=rt)
                    if not force:
                        base_date, deltas = ma_baseline_deltas('sector', lines, results)
                # v2.20 (2026-08-29): 返回全部 (前端面板取前10, 点击标题弹窗看全量)
                # v2.60 (2026-09-05): ts=缓存条目的实际计算时刻 '%m-%d %H:%M:%S' (重算后已set缓存, 必命中)
                _idx_pct_attach(results)   # v2.221: 每行补板块指数当日涨跌幅
                _top5_attach(results, lines, rt)   # v2.242: 成分股最大市值多空 + topN 多空比
                # v2.324 (用户 m22862): 「有更新就覆盖掉旧数据」—— 整份响应原样落档,
                #   下次启动/刷新点开先用存档秒显, 不必等 6 秒现算。
                try:
                    _arc.save_window('trend_sector_' + lines, {
                        'sectors': results, 'realtime': rt is not None,
                        'ts': snap_ts or sector_ma_cache_ts('sector', mp, rt),
                        'lines': lines, 'force': force,
                        'kline_last': _skl_index_last(),
                        'parents': load_industry_parent(),
                        'base_date': base_date, 'deltas': deltas})
                except Exception as _ae:
                    _swlog('trend_arc_sector', _ae)
                self._json({'sectors': results, 'realtime': rt is not None,
                            'ts': snap_ts or sector_ma_cache_ts('sector', mp, rt),
                            'lines': lines, 'force': force,
                            'kline_last': _skl_index_last(),
                            # v5.0 (2026-09-12): 细分行业→上级映射, 供前端表头悬停"上级所属-XX"
                            'parents': load_industry_parent(),
                            'base_date': base_date, 'deltas': deltas})  # v2.61: 徽标基准日期与差值

            elif path == '/api/sector_index_ma':
                """v2.173 (2026-10-22): 板块**指数自身**的均线排列 —— 同花顺官方指数日线本地库

                与 /api/sector_ma 的区别（两个都要留着）：
                  /api/sector_ma      = 有多少成分股站上该均线（广度，成分股合成）
                  /api/sector_index_ma = 板块指数**自己的**均线排列（MA5>MA10>...）
                口径统一到终端 LINES_MAP（3/4/5/6 档 = 5-10-21(,55)(,120)(,250)）。
                ?lines=6&tag=industry&aligned=1&force=1
                """
                lines = qs.get('lines', '6')
                if lines not in ('3', '4', '5', '6'):
                    lines = '6'
                mp = LINES_MAP.get(lines, (5, 10, 21, 55, 120, 250))
                tag = qs.get('tag', '') or ''
                only = qs.get('aligned', '0') == '1'
                force = qs.get('force', '0') == '1'
                d = _sector_index_ma(mp, tag, only, force=force)
                d['lines'] = lines
                self._json(d)

            elif path == '/api/sector_zt':
                """v2.48 (2026-09-04): 行业涨停数 — 涨停池×本地行业map归属
                主表「涨停」列/行业全量弹窗数据源. 盘中TTL刷新/盘后当日缓存/降级
                全部在 dragon.fetch_sector_zt 内; force=1 = 「更新板块」按钮强刷"""
                import quant_terminal.dragon as dragon
                force = qs.get('force', '0') == '1'
                self._json(dragon.fetch_sector_zt(force=force))

            elif path == '/api/concept_ma':
                _, con_map = load_industry_concept_maps()
                lines = qs.get('lines', '4') if qs.get('lines', '4') in ('3', '4', '5', '6') else '4'
                force = qs.get('force', '0') == '1'
                if force:
                    _sector_ma_cache_clear()
                    threading.Thread(target=_prewarm_all_lines,
                                     kwargs={'persist_ma': True}, daemon=True).start()
                mp = LINES_MAP.get(lines, (5, 10, 21, 55))
                rt = get_realtime_prices_cached()
                if not _is_trade_window():
                    rt = None  # v2.14 盘后命中'close'预热缓存
                # v2.61: 同 /api/sector_ma — 非交易窗口近2日 force → 读基线快照; 否则现算附徽标
                base_date, deltas, snap_rows, snap_ts = None, {}, None, None
                if not force and rt is None:
                    bd, rows, dls, ts0 = ma_baseline_snapshot('concept', lines)
                    if rows and _recent_data_date(bd):   # v2.220: 数据日口径(原为墙钟今日/昨日)
                        snap_rows, snap_ts, base_date, deltas = rows, ts0, bd, dls
                if snap_rows is not None:
                    results = snap_rows
                else:
                    results = analyze_concept_ma(con_map, mp, realtime_prices=rt)
                    if not force:
                        base_date, deltas = ma_baseline_deltas('concept', lines, results)
                # v2.20 (2026-08-29): 返回全部 (前端面板取前10, 点击标题弹窗看全量)
                # v2.60 (2026-09-05): ts=缓存条目的实际计算时刻 '%m-%d %H:%M:%S' (同 /api/sector_ma)
                _idx_pct_attach(results)   # v2.221: 每行补板块指数当日涨跌幅
                _top5_attach(results, lines, rt)   # v2.242: 同 /api/sector_ma
                # v2.324 (用户 m22862): 同上, 概念趋势整份落档
                try:
                    _arc.save_window('trend_concept_' + lines, {
                        'concepts': results, 'total': len(results),
                        'realtime': rt is not None,
                        'ts': snap_ts or sector_ma_cache_ts('concept', mp, rt),
                        'lines': lines, 'force': force,
                        'kline_last': _skl_index_last(),
                        'base_date': base_date, 'deltas': deltas})
                except Exception as _ae:
                    _swlog('trend_arc_concept', _ae)
                self._json({'concepts': results, 'total': len(results),
                            'realtime': rt is not None, 'ts': snap_ts or sector_ma_cache_ts('concept', mp, rt),
                            'lines': lines, 'force': force,
                            'kline_last': _skl_index_last(),
                            'base_date': base_date, 'deltas': deltas})  # v2.61: 徽标基准日期与差值

            elif path == '/api/parent_sector_ma':
                """v2.100 (2026-09-16): 上级行业(90类)趋势全量窗口 — 长按「行业趋势」标题2秒打开。

                force=0: 只读 parent_trend.json 秒回 (**绝不算**); force=1: ▶ 启动刷新 (只算**当前档**)。
                ★ 完全独立于 trend_data.json / 细分口径, 仅供查看, 不参与任何选股。
                rt 口径与 /api/sector_ma 逐字一致 (盘中传实时价, 非交易窗口 None) — 两个窗口可比。"""
                lines = qs.get('lines', '4')
                if lines not in ('3', '4', '5', '6'):
                    lines = '4'
                # v2.161 (2026-10-20): **只读路径绝不拉实时价**
                #   原来这里无条件 get_realtime_prices_cached(), 而 parent_trend() 的
                #   force=0 分支 (parent_trend.py:141-166) 根本不读 rt —— 白拉。
                #   代价: 交易时段冷缓存 105 批新浪串行 = 实测 107.56 秒; 若别的线程正在爬,
                #   会进 get_realtime_prices_cached 的「等共享结果最多 15 秒」分支
                #   ⇒ 实测 15.26s / 15.26s / 11.94s, 再排一个直接超 30s 超时。
                #   写路径 parent_trend.py:187 才用 rt, 所以只在 force=1 时取。
                _force = (qs.get('force', '0') == '1')
                rt = None
                if _force:
                    rt = get_realtime_prices_cached()
                    if not _is_trade_window():
                        rt = None
                _pd = pt.parent_trend(lines, _force, rt=rt)
                _idx_pct_attach(_pd.get('rows'))   # v2.221: 上级行业也带板块指数涨跌幅
                _top5_attach(_pd.get('rows'), lines, rt)   # v2.242: 上级行业同样两列
                # v2.324 (用户 m22862): 上级行业趋势整份落档 (四档各自一份)
                try:
                    _arc.save_window('trend_parent_' + lines, _pd)
                except Exception as _ae:
                    _swlog('trend_arc_parent', _ae)
                self._json(_pd)

            elif path == '/api/board_breakout':
                """v2.56 (2026-09-05): 行业/概念 等权合成指数 左峰突破三档 (弹窗数据源)
                kind=industry|concept; force=1 = 启动刷新(重合成+重判定+落盘);
                默认读 sector_breakout_cache.json 落盘结果秒回 (kline_date 一致才算新)"""
                import quant_terminal.breakout as bo
                kind = qs.get('kind', 'industry')
                if kind not in ('industry', 'concept'):
                    kind = 'industry'
                force = qs.get('force', '0') == '1'
                self._json(bo.board_breakout(kind, force=force))

            elif path == '/api/snapshot':
                """v2.323 (2026-11-15 用户 m22464): 读窗口落盘存档 —— **不重算、不联网**, 只读盘。

                用户原话：「点开读存档避免点开的时候没有缓存什么都不显示，点开之后刷新
                数据之后界面再更新成新数据即可。没有缓存的时候点开一片空白等几秒看着很不爽。」
                name: market_overview | ladder_up | ladder_down | lhb_all | lhb_org | lhb_hm
                      board_industry_1 | board_industry_2 | board_concept_1 | board_concept_2
                """
                _nm = (qs.get('name', '') or '').strip()
                if not _nm:
                    self._json({'ok': False, 'error': '缺少 name'})
                else:
                    self._json(_arc.load_window(_nm))

            elif path == '/api/archive/scans':
                """v2.323: 竞价打板归档清单（按日汇总 or 逐次明细）"""
                _md = (qs.get('mode', 'auction') or 'auction').strip()
                if (qs.get('by', '') or '') == 'day':
                    self._json({'ok': True, 'days': _arc.scan_days(_md)})
                else:
                    self._json({'ok': True,
                                'items': _arc.list_scans(_md,
                                                         int(qs.get('limit', '100') or 100))})

            elif path == '/api/archive/scan':
                """v2.323: 读一份竞价打板归档（file=裸文件名）"""
                self._json(_arc.load_scan(qs.get('file', '')) or
                           {'ok': False, 'error': '没有这份归档'})

            elif path == '/api/board_overview':
                """v2.285 (2026-11-12 用户 m11544): 「市场总览」大盘快照 —— 从 TSP 看板移植。

                照着 TSP backend/app/services/market_overview_builder.py 的字段布局，
                但**去掉了四大指数那一格**（用户：「舍弃界面中的四大指数因为我们顶栏
                有七大指数的数据了」）。指数只喂情绪雷达的「指数」维度，不上界面。

                返回:
                  breadth{total,up,down,flat,up_pct,down_pct,avg_pct,median_pct,strong_up,strong_down}
                  amount{total(亿),avg(亿)}
                  limit{limit_up,broken,limit_down,max_boards,seal_rate,tiers[{boards,count,stocks}]}
                  distribution[{label,count,pct}]  8 档
                  trend{above_ma5/20/60(+_pct),new_high,new_low}
                  activity{avg_turnover,high_turnover,high_vol_ratio,vol_ratio}
                  radar[{key,label,value}] / emotion{score,label,dimensions,thresholds}
                  top_gainers / top_losers / turnover_leaders / active_leaders
                  concept_rank{leading,lagging} / industry_rank{leading,lagging}
                参数: force=1 清缓存（60 秒 TTL）
                """
                if qs.get('force', '0') == '1':
                    _bov.invalidate()
                _snap = _bov.snapshot(force=False)
                _arc.save_window('market_overview', _snap)   # v2.323 落盘存档
                self._json(_snap)

            elif path == '/api/monitor':
                """v2.285 (2026-11-12 用户 m11544): 监控中心。

                用户原话：「做两个按钮，一个是"监控结果"，另一个是"正在监控"，
                正在监控里面可以添加监控，可以输入股票代码或简称找到具体某个股票然后
                设定一个价格，后续只要触及过那个价格就把结果展示在"监控结果"里面，
                盘中刷新价格的时候触发了就点亮"市场总览"按钮进入红光呼吸闪烁并且设计
                一个警报音效，被监控的股票在盘中每次刷新行情的时候都必定同步拉取这些
                股票的实时行情」

                参数 action:
                  search   kw=<代码或简称>            → {results:[{code,name}]}
                  quote    code=<6位>                 → {price}  添加时取基准价
                  add      code=&target=&note=        → {ok,watch}
                  remove   id=<监控 id>               → {ok}
                  check    (无)                       → 拉价并判触发, 返回全量状态 + new[]
                  state    (无)                       → 只读状态, 不判触发
                  seen     (无)                       → 清未读 (打开窗口看过后调)
                  clear    (无)                       → 清空监控结果
                """
                act = (qs.get('action', 'state') or 'state').strip()
                if act == 'search':
                    self._json({'ok': True,
                                'results': _bmo.search(qs.get('kw', ''), limit=10)})
                elif act == 'quote':
                    self._json({'ok': True, 'code': _bmo._bare(qs.get('code', '')),
                                'price': _bmo.quote_one(qs.get('code', ''))})
                elif act == 'add':
                    # v2.286 (用户 m12101): repeat=1 ⇒ 反复提醒, 每次触及都进结果并响铃
                    # (同一根 K 线内只报一次); 不传 = 一次性, 触及即结案。
                    self._json(_bmo.add(qs.get('code', ''), qs.get('target', 0),
                                        qs.get('note', ''),
                                        repeat=(qs.get('repeat', '0') in ('1', 'true', 'yes'))))
                elif act == 'remove':
                    self._json(_bmo.remove(qs.get('id', '')))
                elif act == 'check':
                    self._json(_bmo.check())
                elif act == 'seen':
                    self._json(_bmo.mark_seen())
                elif act == 'clear':
                    self._json(_bmo.clear_results())
                else:
                    self._json(_bmo.state())

            elif path == '/api/regime':
                """v2.311 (2026-11-14 用户 m18234): 市场环境（TSP 同名模块移植）。

                四维子分(赚钱/投机/抗跌/趋势) → 综合分 0~100 → 五态
                (强势>=70 / 偏强>=55 / 震荡>=45 / 偏弱>=30 / 弱势)。
                算法与 TSP backend/app/services/regime_builder.py 逐字一致,
                数据用本地 5589 个 K 线 CSV + index_daily 现算(冷启约 20s, 热缓存 10 分钟)。

                参数:
                  what=history(默认, 返回 rows) | states(状态分布) | coverage | latest
                       | phases(情绪周期分段) | phase_summary(阶段概览) | mainline(主线排行)
                  days=30~1000 (默认 250)
                  kind=concept|industry  （仅 what=mainline）
                  top=1~30             （仅 what=mainline，默认 10）
                  force=1 强制重算
                """
                try:
                    _w = (qs.get('what', 'history') or 'history').strip()
                    _d = (qs.get('days', '') or '').strip()
                    _f = (qs.get('force', '0') or '0').strip() in ('1', 'true', 'yes')
                    _dd = 250
                    if _d:
                        try:
                            _dd = int(_d)
                        except ValueError:
                            _dd = 250
                    if _w == 'states':
                        self._json(_rg.states(250, force=_f))
                    elif _w == 'coverage':
                        self._json(_rg.coverage(force=_f))
                    elif _w == 'latest':
                        self._json(_rg.latest(force=_f))
                    # v2.312 (2026-11-14 用户 m18508): 情绪周期 + 主线排行 —— 照搬
                    #   TSP /api/regime/phases | /phase_summary(前端自己从 latest+phases 拼)
                    #   | /mainline 三个接口的返回结构。
                    elif _w == 'phases':
                        _pd = _rg.phases(_dd, force=_f)
                        # v2.326 (用户 m23308): 落档 —— 下次点开「情绪周期」先拿它秒显
                        try:
                            _arc.save_window('phase_segs_%d' % _dd, _pd, force=_f)
                        except Exception as _ae:
                            _swlog('arc_phase_segs', _ae)
                        self._json(_pd)
                    elif _w == 'phase_summary':
                        _ps = _rg.phase_summary(_dd, force=_f)
                        try:
                            _arc.save_window('phase_sum_%d' % _dd, _ps, force=_f)
                        except Exception as _ae:
                            _swlog('arc_phase_sum', _ae)
                        self._json(_ps)
                    elif _w == 'mainline':
                        _k = 'industry' if (qs.get('kind') or '').strip() == 'industry' else 'concept'
                        _t = 10
                        try:
                            _t = int((qs.get('top') or '10').strip())
                        except (TypeError, ValueError):
                            _t = 10
                        _mld = _rg.mainline(_dd, kind=_k, top=_t, force=_f)
                        try:
                            _arc.save_window('phase_ml_%d_%s' % (_dd, _k), _mld, force=_f)
                        except Exception as _ae:
                            _swlog('arc_phase_ml', _ae)
                        self._json(_mld)
                    else:
                        _hd = _rg.history(_dd, force=_f)
                        # v2.326: 市场环境(也是情绪周期页的 history)落档 —— 冷算 12 秒, 不能干等
                        try:
                            _arc.save_window('regime_%d' % _dd, _hd, force=_f)
                        except Exception as _ae:
                            _swlog('arc_regime', _ae)
                        self._json(_hd)
                except Exception as _e:
                    self._json({'ok': False, 'error': str(_e)[:200]})

            elif path == '/api/board_rotation':
                """v2.281 (2026-11-12 用户 m10425/m10906): 板块涨幅轮动矩阵 (RPS 轮动分析)。

                「最近 N 个交易日 × 每个板块」的涨幅排名矩阵 —— 每列一个日期,
                列内按当天板块均涨幅降序。数据全从本地 K 线现算, **不需要攒历史**:
                  ① 每天每只票的 (收 - 昨收)/昨收  → 涨幅
                  ② 个股 → 板块 (行业按 1/2 级聚合 / 概念 explode)
                  ③ 按 (日期, 板块) 求简单平均 → 与 board_rank 的当日口径同源
                参数: kind=industry|concept, level=1|2, days=7~30, force=1 清缓存
                """
                kind = qs.get('kind', 'industry')
                if kind not in _brt.KINDS:
                    kind = 'industry'
                try:
                    level = int(qs.get('level', '2'))
                except (TypeError, ValueError):
                    level = 2
                try:
                    days = int(qs.get('days', str(_brt.DAYS_DEF)))
                except (TypeError, ValueError):
                    days = _brt.DAYS_DEF
                if qs.get('force', '0') == '1':
                    _brt.invalidate()
                self._json(_brt.rotation(kind, level, days, force=False))

            elif path == '/api/yuntu':
                """v2.353 (2026-11-20 用户 m36569): 大盘云图 —— 面积成比例的方块树图。

                与 /api/board_rank 的区别:
                  board_rank = 一行一个板块的**排行榜**(文本)
                  yuntu      = 把板块/个股摊成**方块**, 面积 ∝ 流通市值(或成交额) → 看全市场热度分布

                参数: kind=industry(默认, 一级90→二级257→个股) | concept(只有 351 个板块一层),
                      force=1 清缓存重算

                ★ 概念为什么不做个股层: 一只票平均属于 9.5 个概念, 摊进方块等于同一只票画 9 遍、
                  市值重复计 9 倍, 面积毫无意义(实测 4.9MB / 5.3 万个矩形)。概念只看板块。
                """
                _yk = qs.get('kind', 'industry')
                if _yk not in _yt.KINDS:
                    _yk = 'industry'
                if qs.get('force', '0') == '1':
                    _yt.invalidate(_yk)
                _yd = _yt.build(_yk)
                # v2.357 (用户 m37552): 每次构建成功都落一份存档 —— 以前只有启动预热
                # 写一次, 之后 60 秒 TTL 重算出来的新数据从不落盘, 前端打开读到的
                # 永远是开机那一刻的旧图。save_window 自带去重(内容没变不写盘)。
                try:
                    _arc.save_window('yuntu_' + _yk, _yd)
                except Exception:
                    pass
                self._json(_yd)

            elif path == '/api/idx_pairs':
                """v2.359 (用户 m38119): 特色指标 —— 两条指数收盘价 + 比值走势。

                参数: a=分子代码 b=分母代码 limit=只留最后 N 个点
                      不带 a/b 时返回三组预设的**元信息**(起点/点数/ready), 给左列选项用。

                ★ 历史从短板开始: 两个日期集合取**交集**(见 idx_store.pair_view),
                  所以「科创综指/中证红利」自然从 2019-12-31 起, 不是从 2005 起。
                """
                _ia = (qs.get('a', '') or '').strip().lower()
                _ib = (qs.get('b', '') or '').strip().lower()
                if not _ia or not _ib:
                    self._json({'ok': True, 'pairs': _ids.pairs_meta()})
                    return
                try:
                    _lim = int(qs.get('limit', '0') or 0)
                except (TypeError, ValueError):
                    _lim = 0
                try:
                    _idx_auto_sync()          # v2.359c: 收盘后首次打开就后台补数
                except Exception:
                    pass
                _d5 = _ids.pair_view(_ia, _ib, limit=_lim)
                try:
                    _arc.save_window('idx_pair', _d5)
                except Exception:
                    pass
                self._json(_d5)

            elif path == '/api/idx_list':
                """v2.359 (用户 m38119): 指数一览 —— 七大指数的最近 N 根收盘价。

                参数: limit=每只保留多少根(默认 260 ≈ 一年), codes=逗号分隔覆盖默认列表
                ★ 只读本地存档, 一次网络请求都不发(用户 m38119: 历史必须落本地)。
                """
                try:
                    _lim = int(qs.get('limit', '260') or 260)
                except (TypeError, ValueError):
                    _lim = 260
                try:
                    _idx_auto_sync()
                except Exception:
                    pass
                _cd = (qs.get('codes', '') or '').strip()
                _d6 = {'ok': True, 'rows': _ids.list_view(limit=_lim)}
                if _cd:
                    _d6['rows'] = []
                    for _c in [x.strip().lower() for x in _cd.split(',') if x.strip()]:
                        _rr = _ids.load(_c)
                        _tt = _rr[-_lim:] if _lim else _rr
                        _d6['rows'].append({
                            'code': _c, 'name': _ids.name_of(_c), 'short': _ids.short_of(_c),
                            'n': len(_rr),
                            'start': _rr[0]['date'] if _rr else '',
                            'end': _rr[-1]['date'] if _rr else '',
                            'last': _tt[-1]['close'] if _tt else None,
                            'dates': [x['date'] for x in _tt],
                            'closes': [x['close'] for x in _tt],
                        })
                try:
                    _arc.save_window('idx_list', _d6)
                except Exception:
                    pass
                self._json(_d6)

            elif path == '/api/idx_slots':
                # v2.360 (用户 m38439): 顶栏两格的候选 + 全表名字, 前端一次拿全。
                #   为什么放后端: 名字/候选若前端再写一份, 就是第七处硬编码。
                _sp = _idxr.slots_payload()
                _sp['ok'] = True
                self._json(_sp)

            elif path == '/api/idx_eb':
                """v2.369 (用户 m40973): 股债利差 —— 复刻 legulegu.com 的同名指标。

                口径（逐条照乐咕，已用 5248 个点逐位对拍通过）：
                  · 公式一 pe    : 1/指数加权滚动PE_TTM − 10年期国债收益率(%)/100  ⇒ 小数
                  · 公式二 dvttm : 10年期国债收益率 − 指数等权股息率TTM           ⇒ 百分点
                  · 公式三 dv    : 10年期国债收益率 − 指数静态股息率             ⇒ 百分点
                  · 通道 = W 个月（不是交易日数！）滚动均值 ± 2×总体标准差，
                    W=30(2.5年) / 36(3年) / 60(5年)，默认 30

                参数:
                  code=000300|000016|000905   标的（默认 000300 沪深300）
                  formula=pe|dvttm|dv         公式（默认 pe；后两个只有沪深300 有）
                  ma=30|36|60                 通道窗口（月）
                  limit=N                     只留最后 N 个点
                  mode=meta                  只返回三档元信息 + 深度（不返回序列）
                  mode=sync                  同步一次源表（会联网，约 10~15 秒）

                数据来源（详见 quant_terminal/equity_bond.py 的 docstring）：
                  · 沪深300 三套公式全量 ⇒ 乐咕自家 API（免费档只开 000300.SH）
                  · 上证50 / 中证500 只有 pe ⇒ 中证 index-perf 的 peg 字段 + 国债自算
                  · 10 年期国债 = 东财 RPTA_WEB_TREASURYYIELD 的 EMM00166466
                """
                _mode = (qs.get('mode', '') or '').strip().lower()
                try:
                    if _mode == 'meta':
                        _d7 = _eb.meta()
                        _d7['ov'] = _eb.overview()
                    elif _mode == 'sync':
                        _r7 = _eb.sync(deep=bool(qs.get('deep')) )
                        _d7 = {'ok': True, 'sync': _r7, 'ov': _eb.overview()}
                    else:
                        _c7 = (qs.get('code', '000300') or '000300').strip()
                        _f7 = (qs.get('formula', 'pe') or 'pe').strip().lower()
                        try:
                            _m7 = int(qs.get('ma', '30') or 30)
                        except (TypeError, ValueError):
                            _m7 = 30
                        try:
                            _l7 = int(qs.get('limit', '0') or 0)
                        except (TypeError, ValueError):
                            _l7 = 0
                        _d7 = _eb.series(_c7, _f7, _m7, _l7)
                        if _d7.get('ok'):
                            _d7['ov'] = _eb.overview()
                except Exception as _e7:
                    _swlog('idx_eb', _e7)
                    _d7 = {'ok': False, 'error': '股债利差计算失败: %s' % repr(_e7)[:150]}
                if _d7.get('ok'):
                    try:
                        _arc.save_window('idx_eb', _d7)
                    except Exception:
                        pass
                self._json(_d7)

            elif path == '/api/pe_ratio':
                """v2.370 (用户 m41587): 市盈率走势图 —— 复刻 legulegu.com/stockdata/hs300-ttm-lyr。

                六条线（三条算法 × 静态/滚动两族）：
                  · 等权PE      = mean(PE)，只取 PE>0 且 **PE<=300**（乐咕的截尾规则，
                                  直接全平均会高 6.97；见 pe_ratio 模块 docstring）
                  · 整体法PE    = Σ总市值 ÷ Σ(总市值/PE)，只取 PE>0（真正的市值加权）
                  · 中位数PE    = median(PE)，只取 PE>0
                  · 百分位      = 当前值在**全部历史**中的分位（0~1），与乐咕 ttmPeQuantile 同义

                参数:
                  code=000300|000016|000905|000852|000001|399001|399006|000688|000015|000922|000985
                  src=day|csi|lg_month|lg_week   指定数据源；不给则自动挑最长的
                       day     = 东财成分股+全市场估值自算日频（**2018-01-02 起**）
                       csi     = 中证官网 peg（**2013-01-01 起**，只有一条整体法线，
                                 深市 399 开头中证不提供）
                       lg_month/lg_week = 乐咕月/周频（2005 起，15 字段含官方百分位；
                                 **站点 2026-11-22 起 504，抓不到就自动跳过**）
                  limit=N     只留最后 N 个点
                  mode=meta      只回覆盖矩阵（每只指数有哪些源、多少行），不返回序列
                  mode=overview  左列列表（含末值）
                  mode=sync      同步源表（会联网；deep=1 时自算日频按逐日全量回补≈5 分钟/指数）

                数据来源与口径差异见 quant_terminal/pe_ratio.py 的模块 docstring。
                """
                _pm = (qs.get('mode', '') or '').strip().lower()
                if _pm == 'meta':
                    self._json(_pr.meta())
                    return
                if _pm == 'overview':
                    self._json(_pr.overview())
                    return
                if _pm == 'sync':
                    try:
                        _pcs = (qs.get('codes', '') or '').strip()
                        _plist = [x for x in _pcs.split(',') if x] or None
                        _pd = bool((qs.get('deep', '') or '') in ('1', 'true', 'yes'))
                        _ps = _pr.sync(_plist, deep=_pd)
                        self._json({'ok': True, 'sync': _ps})
                    except Exception as _e5:
                        _swlog('pe_ratio_sync', _e5)
                        self._json({'ok': False, 'error': '同步失败: %r' % (_e5,)})
                    return
                try:
                    _pc = (qs.get('code', '') or '000300').strip()
                    _psrc = (qs.get('src', '') or '').strip() or None
                    try:
                        _plim = int(qs.get('limit', '0') or 0)
                    except (TypeError, ValueError):
                        _plim = 0
                    _d8 = _pr.series(_pc, _psrc, _plim)
                except Exception as _e6:
                    _swlog('pe_ratio', _e6)
                    _d8 = {'ok': False, 'error': '市盈率序列计算失败: %r' % (_e6,)}
                if _d8.get('ok'):
                    try:
                        _arc.save_window('pe_ratio', _d8)
                    except Exception:
                        pass
                self._json(_d8)

            elif path == '/api/idx_meta':
                """v2.359: 单只指数的本地存档概况（行数/起止日/文件 mtime），不联网。"""
                _c = (qs.get('code', '') or '').strip().lower()
                if not _c:
                    self._json({'ok': False, 'error': '缺少 code'})
                else:
                    _m = _ids.meta(_c)
                    _m['ok'] = True
                    self._json(_m)

            elif path == '/api/idx_head':
                """v2.367 (用户 m40047): 特色指标「沪深300头部/全流通市值」日线。

                口径（用户原话）：「头部（就是这五只）沪深300ETF占沪深300ETF自由流通
                市值的比例，简化为『沪深300头部/全流通市值』」
                  · 分子 = 5 只沪深300ETF 的**规模**合计（东财 f117，取不到时用
                    份额×当日收盘价，单位统一成亿元）
                  · 分母 = 沪深300 **流通市值**（东财 f117，亿元）
                粒度（我定的，理由见 etf_src 模块 docstring）：
                  · 历史段 = 季报份额（gmbd，2012 起）配当日收盘价 → 逐日
                  · 今天起每日收盘后由 snapshot_today() 记一条真值快照，越攒越准
                  · 两者之间**线性插值**，不能只做前向填充（2026-09-30→11-21 份额翻倍，
                    只前向填差 2.2 倍）
                参数: limit=只留最后 N 个点
                """
                try:
                    _lim = int(qs.get('limit', '0') or 0)
                except (TypeError, ValueError):
                    _lim = 0
                try:
                    _hd = _etf.head_ratio_series(limit=_lim)
                except Exception as _e:
                    _hd = None
                    _swlog('idx_head', _e)
                if not _hd:
                    self._json({'ok': False, 'error': '指标暂不可用（取不到 ETF 或沪深300 数据）'})
                    return
                try:
                    _arc.save_window('idx_head', _hd)
                except Exception:
                    pass
                self._json(_hd)

            elif path == '/api/board_rank':
                """v2.277 (2026-11-11 用户 m09653): 行业/概念「板块」排行 —— 从 TSP 移植。

                与 /api/sector_ma 的区别 (两个都要留着, 口径完全不同):
                  /api/sector_ma   = 均线广度: 多少成分股站上 N 日线 → 回答"多头排列有多强"
                  /api/board_rank  = 当日涨幅: 板块均涨幅/涨跌家数/成交额/换手/量比
                                     + 龙头分(6因子) + 强度分/风险分 → 回答"今天谁最强"

                参数: kind=industry|concept, level=1(上级90类)|2(细分257类, 概念不分级),
                      sort=heat|avgPct|leader|amount|down, force=1 清缓存重算
                """
                kind = qs.get('kind', 'industry')
                if kind not in _brd.KINDS:
                    kind = 'industry'
                sort = qs.get('sort', 'heat')
                if sort not in _brd.SORTS:
                    sort = 'heat'
                try:
                    level = int(qs.get('level', '2'))
                except (TypeError, ValueError):
                    level = 2
                if qs.get('force', '0') == '1':
                    _brd.invalidate()
                _rkd = _brd.rank(kind, level, sort)
                # v2.323: 只存「热度」那一种排序 —— 别的排序前端自己能排(数据同一份),
                #   存 5 份纯属浪费 200KB×5
                if sort == 'heat':
                    _arc.save_window('board_%s_%d' % (kind, level), _rkd)
                self._json(_rkd)

            elif path == '/api/board_members':
                """v2.277: 某个板块的成分股明细 (按龙头分降序) —— 板块窗口右侧聚焦区数据源

                参数: kind, name(板块名, URL 编码), level, limit
                """
                kind = qs.get('kind', 'industry')
                if kind not in _brd.KINDS:
                    kind = 'industry'
                name = qs.get('name', '')
                if not name:
                    self._json({'ok': False, 'error': '缺少板块名称', 'members': []})
                    return
                try:
                    level = int(qs.get('level', '2'))
                except (TypeError, ValueError):
                    level = 2
                try:
                    limit = int(qs.get('limit', '300'))
                except (TypeError, ValueError):
                    limit = 300
                self._json(_brd.members(kind, name, level, limit))

            elif path == '/api/ma_members':
                """v2.12 板块下钻: 点击行业/概念名称 → 该板块多头排列成分股实时列表
                多头判定与板块分析完全一致 (seq2 排列检查, 均线条数=当前选择的档位)
                """
                kind = qs.get('kind', 'sector')  # sector | concept | parent
                name = qs.get('name', '')
                lines = qs.get('lines', '4') if qs.get('lines', '4') in ('3', '4', '5', '6') else '4'
                mp = LINES_MAP.get(lines, (5, 10, 21, 55))
                if not name:
                    self._json({'error': '缺少板块名称'})
                    return
                ind_map, con_map = load_industry_concept_maps()
                rt = get_realtime_prices_cached()
                if not _is_trade_window():
                    rt = None  # v2.89: 与面板 /api/sector_ma 同口径 (盘后走收盘价; 原来下钻恒传实时价 = 盘后和面板可能对不上)
                # v2.89 (2026-09-12): 三级取数 — 把「下钻 = 全表重算」消掉
                #   ① 内存缓存 _SECTOR_MA_CACHE (面板刚刷过就是它, 同口径同快照) — 0 成本
                #   ② trend_data.json 落盘 (面板/扫描算完顺手写进去的整份结果) — 读盘毫秒级
                #   ③ 兜底全表重算 (该档该口径从没算过才走到; 算完 ① 的钩子顺手落盘, 下次免算)
                def _pick(rows):
                    """找目标板块 (v2.61f 修复: 先全表精确扫描, 精确不到才走包含兜底)
                    原实现单循环短路: r['name'] in name (如 '贸易' in '石油加工贸易') 会命中排在
                    前面的相似短名行并 break, 遮蔽后面真正的精确行 → 点 石油加工贸易 显示 贸易 成分股"""
                    if not rows:
                        return None
                    for r in rows:
                        if r['name'] == name:
                            return r
                    for r in rows:
                        if name in r['name'] or r['name'] in name:
                            return r
                    return None
                if kind == 'parent':
                    # v2.177 (2026-10-22): 上级行业(90类)下钻 —— 口径与细分完全一致:
                    #   现价站上 MA5 即入选 (不要求均线有序)。成分股来自 industry_parent 反查表,
                    #   状态/多头占比取 parent_trend.json 落盘那一行 (与上级窗口显示逐字一致)。
                    from quant_terminal.engine import load_industry_parent_map as _lpm
                    _pmap = _lpm() or {}      # {6位代码: 上级行业名} —— 反查成 上级 → [代码]
                    _codes = sorted([_k for _k, _v in _pmap.items() if _v == name])
                    if len(_codes) < 3:
                        self._json({'error': f'未找到上级行业: {name}', 'kind': kind})
                        return
                    _inw = _is_trade_window()
                    _rt6 = rt or {}
                    codes = []
                    for _c in _codes:
                        _pre = mcap.prefixed(_c)[:2]
                        try:
                            _tail = _read_csv_tail(os.path.join(KLINE_DIR, _pre + _c + '.csv'), 250)
                        except Exception:
                            _tail = []
                        _cl = [r['close'] for r in _tail]
                        if not _cl:
                            continue
                        _last = _rt6.get(_c) if (_inw and _rt6) else _cl[-1]
                        if not _last:
                            _last = _cl[-1]
                        if len(_cl) >= 5:
                            _base_cl = (_cl[:-1] + [_last]) if _inw else _cl
                            _ma5 = sum(_base_cl[-5:]) / 5.0
                            if _last > _ma5:
                                codes.append(_c)
                        else:
                            codes.append(_c)      # 次新不足 5 根: 先纳入, 后面按价格过滤
                    _prow = None
                    try:
                        _pd = pt.parent_trend(lines, False)
                        for _r in (_pd.get('rows') or []):
                            if _r.get('name') == name:
                                _prow = _r
                                break
                    except Exception as _e:
                        print('[ma_members] 上级趋势读取失败: %s' % _e)
                    src_lv = 'parent'
                    row = {'name': name,
                           'status': (_prow or {}).get('status') or '',
                           'bullish_pct': (_prow or {}).get('bullish_pct') or 0}
                else:
                    kind_key = 'sector' if kind == 'sector' else 'concept'
                    src_lv = 'cache'
                    rows = _sector_ma_cache_get(kind_key, mp, rt)          # ①
                    row = _pick(rows)
                    if row is None:
                        src_lv = 'disk'
                        rows = trend_slot_rows(kind_key, mp, rt)           # ②
                        row = _pick(rows)
                    if row is None:                                        # ③
                        src_lv = 'calc'
                        if kind == 'sector':
                            rows = analyze_sector_ma(ind_map, mp, realtime_prices=rt)
                        else:
                            rows = analyze_concept_ma(con_map, mp, realtime_prices=rt)
                        row = _pick(rows)
                    if row is None:
                        self._json({'error': f'未找到板块: {name}', 'kind': kind})
                        return
                    # v2.97 (2026-09-16): 下钻名单换口径 —— 现价站上 MA5 即入选 (不要求均线有序)。
                    #   老内存缓存 / 老落盘槽没有 drill_stocks → 退回旧的严格多头链条, 绝不筛空;
                    #   重扫一次 (或点「更新板块」) 即切换。
                    codes = row.get('drill_stocks')
                    if codes is None:
                        codes = row.get('bullish_stocks', [])
                # 实时价 + 名称
                engine = QuantEngine()
                engine.load_data()
                members = []
                for c in codes:
                    prefix = mcap.prefixed(c)[:2]
                    full = prefix + c
                    # v2.13 修复: 行业/概念从全市场 map 取 (ind_map/con_map 覆盖全部K线票)
                    # 原来只用选股池 engine.pool → 池外票(双创等)行业/概念为空
                    info = engine.pool.get(full, {})
                    num = c
                    members.append({
                        'code': full, 'name': info.get('name', c),
                        'sector': engine.ind_map.get(num, info.get('sector', '')),
                        'concept': (engine.con_map.get(num, '') or info.get('concept', '')),
                        'price': 0, 'change_pct': 0,
                    })
                # 批量拉实时价
                sina_codes = [_sc_of(c) for c in codes]
                rt_all = fetch_sina(sina_codes)
                for m in members:
                    sc = _sc_of(m['code'])
                    rd = rt_all.get(sc, {})
                    if rd:
                        m['price'] = rd.get('price', 0)
                        m['change_pct'] = rd.get('change_pct', 0)
                        if rd.get('name'): m['name'] = rd['name']
                members = [m for m in members if m['price'] > 0]
                # v2.97 (2026-09-16): 「均线」列 —— 现价 vs MA5/10/21/55/120/250, 逐条**独立**比大小
                #   (不要求 收盘>MA5>MA10... 有序)。1=站上 0=站下 -1=K线不足算不出(次新)。
                #   口径与面板 analyze_*_ma 对齐: 盘中用实时价替换最后一根收盘再算均线。
                in_win = _is_trade_window()
                for m in members:
                    try:
                        tail = _read_csv_tail(os.path.join(KLINE_DIR, m['code'] + '.csv'), 250)
                    except Exception:
                        tail = []
                    closes = [r['close'] for r in tail]
                    if in_win and closes:
                        closes = closes[:-1] + [m['price']]
                    last = closes[-1] if closes else 0.0
                    mv = {}
                    flags = []
                    for p in DRILL_MA_COLS:
                        if len(closes) < p:
                            flags.append(-1)          # -1 = K线不足, 算不出
                        else:
                            mv[p] = sum(closes[-p:]) / p
                            flags.append(1 if last > mv[p] else 0)
                    m['ma'] = flags
                    # v2.98 (2026-09-16): 「均线序列」—— 算得出的均线按**值**从大到小排。
                    #   只发 [周期, 值] 对, 染哪一段由前端按规范序 5,10,21,55,120,250 判连续前缀。
                    #   次新只发算得出的那几条 (只发前缀, 不会有"缺中间"的情况)。
                    m['seq'] = [[p, round(mv[p], 3)] for p in sorted(mv, key=lambda k: -mv[k])]
                members.sort(key=lambda x: -x['change_pct'])
                self._json({'name': row['name'], 'kind': kind, 'status': row['status'],
                            'src': src_lv,  # v2.89: cache=内存缓存 / disk=读落盘 / calc=兜底重算
                            'bullish_pct': row['bullish_pct'], 'count': len(members),
                            'ma_cols': list(DRILL_MA_COLS),  # v2.97: 「均线」列表头顺序
                            'members': members, 'lines': lines})

            elif path == '/api/sector_kline':
                """v2.13 板块/概念合成指数日K: 点击行业/概念 → 日K界面渲染指数K线
                成分股 = map 全量成分 (与板块MA同口径), 等权涨幅合成, 1000点起
                """
                kind = qs.get('kind', 'sector')  # sector | concept | parent
                name = qs.get('name', '')
                if not name:
                    self._json({'error': '缺少板块名称'})
                    return
                # v2.246 (用户 m21198): limit=N 只回**尾部 N 根**。
                #   悬停浮窗要的是最近 50 根，整包 147KB / 1217 根纯属浪费；
                #   不带 limit 的老调用方(点板块看 K 线)逐字节不变。
                try:
                    _lim = int(qs.get('limit') or 0)
                except Exception:
                    _lim = 0

                def _tail(d):
                    if not _lim or _lim <= 0 or not isinstance(d, dict):
                        return d
                    o = dict(d)
                    n = int(_lim)
                    for _k in ('dates', 'ohlc', 'volume'):
                        _v = o.get(_k)
                        if isinstance(_v, list) and len(_v) > n:
                            o[_k] = _v[-n:]
                    # count 也要跟着改 —— 否则响应里写着 1217 根、实际只给 50 根，自相矛盾
                    if isinstance(o.get('dates'), list):
                        o['count'] = len(o['dates'])
                    _ms = o.get('mas')
                    if isinstance(_ms, dict):
                        _m2 = {}
                        for _k2, _arr in _ms.items():
                            _m2[_k2] = (_arr[-n:] if isinstance(_arr, list)
                                        and len(_arr) > n else _arr)
                        o['mas'] = _m2
                    return o

                # v2.177 (2026-10-22): 优先同花顺**官方指数点数** —— 与同花顺完全对齐。
                #   本地 sector_index 已建 710 个指数(5 年)，直接读；只有匹配不到官方指数
                #   (腾讯独有板块名 / 未建库的新题材) 才回退成分股等权合成。
                try:
                    from quant_terminal import sector_kline as _sk
                    _off = _sk.official_kline(kind, name)
                except Exception as _oe:
                    print('[sector_kline] 官方指数读取失败 %s: %s' % (name, _oe))
                    _off = None
                if _off:
                    self._json(_tail(_off))
                    return
                ind_map, con_map = load_industry_concept_maps()
                if kind == 'concept':
                    # P2-7 fix: 概念值是'A|B|C'多token串, 精确==匹配永不命中;
                    # 原来兜底 `name in sec` 只对单概念串有效, `sec in name` 还误配成分股
                    codes = sorted({c for c, sec in con_map.items()
                                    if name in (sec or '').split('|')})
                    if len(codes) < 3:   # 罕见: 名称本身带|或轻微差异, 保留包含兜底
                        codes = sorted({c for c, sec in con_map.items()
                                        if (sec and (name in sec)) and name in sec.split('|')})
                else:
                    codes = sorted({c for c, sec in ind_map.items() if sec == name})
                    # 名称包含匹配兜底
                    if len(codes) < 3:
                        codes = sorted({c for c, sec in ind_map.items()
                                        if name in sec or sec in name})
                if len(codes) < 3:
                    self._json({'error': f'未找到板块: {name}', 'kind': kind})
                    return
                data = composite_sector_kline(codes, name=name, kind=kind)
                if data is None:
                    self._json({'error': f'成分股K线不足: {name}', 'kind': kind})
                    return
                self._json(_tail(data))

            elif path == '/api/sim/state':
                account = SimAccount()
                today = datetime.now().strftime('%Y-%m-%d')
                # 今日买卖记录 (回放记录带 bt_date, 天然被排除)
                buy_today = {}
                sell_recs = {}
                for rec in account.records:
                    if rec.get('time', '')[:10] != today:
                        continue
                    c = rec.get('code')
                    if not c:
                        continue
                    if rec.get('action') in ('买入', '加仓'):
                        bi = buy_today.setdefault(c, {'shares': 0, 'amount': 0.0, 'fee': 0.0})
                        bi['shares'] += rec.get('shares', 0)
                        bi['amount'] += rec.get('price', 0) * rec.get('shares', 0)
                        bi['fee'] += rec.get('fee', 0)
                    elif rec.get('action') == '卖出':
                        sell_recs.setdefault(c, []).append(rec)
                buy_today = {c: {'shares': bi['shares'],
                                 'avg': round(bi['amount'] / bi['shares'], 3),
                                 'fee': bi['fee']}
                             for c, bi in buy_today.items() if bi['shares'] > 0}
                # 现价/昨收: 持仓+今日卖出 一并拉; 昨收优先用本地K线最后一根<今日的收盘
                # (v2.32: 新浪昨收盘后15:00会滚动成今日收盘, 盘后看当日盈亏会全变0)
                prices = {}
                prev_closes = {}
                need = {p['code'] for p in account.positions} | set(sell_recs.keys())
                if need:
                    sina_codes = [c.lower() for c in sorted(need)]
                    rt = fetch_sina(sina_codes)
                    for c in sorted(need):
                        q = rt.get(c.lower(), {})
                        px = q.get('price', 0)
                        if px <= 0:
                            pos = account._find_pos(c)
                            px = pos['avg_cost'] if pos else sell_recs[c][-1].get('price', 0)
                        prices[c] = round(px, 2)
                        pcv = 0.0
                        kl = load_csv_kline(c)
                        if kl:
                            for bar in reversed(kl):
                                if bar['date'] < today:
                                    pcv = bar['close']
                                    break
                        if pcv <= 0:
                            pcv = q.get('prev_close', 0)
                        prev_closes[c] = round(pcv, 2) if pcv > 0 else round(px, 2)
                # v2.32 (2026-09-01) 当日盈亏服务端权威计算 (同花顺口径):
                #   今日买入部分按买入均价起算(买入后当天涨幅), 跨夜持仓按昨收;
                #   今日卖出部分计入 (先冲减今日买入, 余量按昨收) — 卖出后当日盈亏不清零
                day_pnl = 0.0
                day_pnl_by_code = {}
                for p in account.positions:
                    c = p['code']
                    px = prices.get(c, p['avg_cost'])
                    pcv = prev_closes.get(c, px)
                    bi = buy_today.get(c)
                    b_sh = bi['shares'] if bi else 0
                    b_avg = bi['avg'] if bi else 0.0
                    b_fee = bi['fee'] if bi else 0.0
                    s_sh = sum(r.get('shares', 0) for r in sell_recs.get(c, []))
                    rt_sh = min(b_sh, s_sh)            # 同花顺口径: 先算今日买入后卖出的
                    pos_from_buy = max(0, b_sh - s_sh)  # 持仓中今日买入未卖部分
                    pos_from_prev = p['shares'] - pos_from_buy
                    pnl = 0.0
                    if pos_from_buy > 0: pnl += (px - b_avg) * pos_from_buy
                    if pos_from_prev > 0: pnl += (px - pcv) * pos_from_prev
                    if c in sell_recs:
                        sell_amt = sum(r.get('price', 0) * r.get('shares', 0) for r in sell_recs[c])
                        sell_fee = sum(r.get('fee', 0) for r in sell_recs[c])
                        pnl += sell_amt - b_avg * rt_sh - pcv * (s_sh - rt_sh) - sell_fee
                    day_pnl += pnl - b_fee           # 今日买入手续费计入当日亏损
                    day_pnl_by_code[c] = round(pnl - b_fee, 2)
                # 今日卖出且已清仓的股票: 直接按卖出记录算 (无持仓行)
                for c, recs in sell_recs.items():
                    if c in day_pnl_by_code:
                        continue
                    px = prices.get(c, 0)
                    pcv = prev_closes.get(c, px)
                    bi = buy_today.get(c)
                    b_avg = bi['avg'] if bi else 0.0
                    b_sh = bi['shares'] if bi else 0
                    b_fee = bi['fee'] if bi else 0.0
                    s_sh = sum(r.get('shares', 0) for r in recs)
                    rt_sh = min(b_sh, s_sh)
                    sell_amt = sum(r.get('price', 0) * r.get('shares', 0) for r in recs)
                    sell_fee = sum(r.get('fee', 0) for r in recs)
                    pnl = sell_amt - b_avg * rt_sh - pcv * (s_sh - rt_sh) - sell_fee - b_fee
                    day_pnl += pnl
                    day_pnl_by_code[c] = round(pnl, 2)
                state = account.to_dict()
                state['holdings_value'] = round(account.holdings_value(prices), 2)
                state['nav'] = round(account.nav(prices), 2)
                state['prices'] = prices
                state['prev_closes'] = prev_closes
                state['buy_today'] = buy_today
                state['day_pnl_total'] = round(day_pnl, 2)
                state['day_pnl_by_code'] = day_pnl_by_code
                self._json(state)

            elif path == '/api/sim/history':
                # v2.65 (2026-09-09): 模拟盘历史收益 (逐日净值+基准, 落盘 sim_hist.json 增量结算)
                with ACCOUNT_LOCK:
                    try:
                        acc = SimAccount()
                        self._json(get_sim_history(acc))
                    except Exception as e:
                        print(f'[sim/history] 结算异常: {e!r}', file=sys.stderr)
                        self._json({'ok': False, 'error': f'结算异常: {e}'})

            elif path == '/api/sim/backtest/result':
                self._json(SIM_BT_STATE)

            elif path == '/api/sectors':
                # v2.53 (2026-09-05): 板块热度落盘 — 平时读盘返回上次手动刷新结果,
                # force=1 才现拉腾讯(防限流); 每次现拉成功后落盘覆盖
                force = qs.get('force', '0') == '1'
                if not force:
                    hot = _read_json_file(SECTOR_HOT_FILE)
                    if hot and hot.get('sectors'):
                        self._json({'sectors': hot['sectors'],
                                    'source': hot.get('source', '同花顺 fuyao + 腾讯资金面'),
                                    'ts': hot.get('ts', ''), 'cached': True})
                        return
                clear_sector_rt_cache()  # v2.54: force刷新绕过引擎双层缓存, 确保现拉腾讯
                engine = QuantEngine()
                engine.load_data()
                engine.calc_sector_returns()
                # sector_returns 现在是 {name: {change_pct, up_count, total_count, ...}}
                sorted_sectors = sorted(engine.sector_returns.items(),
                                       key=lambda x: -(x[1].get('change_pct', 0) if isinstance(x[1], dict) else (x[1] or 0)))
                result = []
                for name, data in sorted_sectors[:20]:
                    if isinstance(data, dict):
                        result.append({
                            'name': name,
                            'change_pct': data.get('change_pct', 0),
                            'zdf5': data.get('zdf5', 0),
                            'zdf20': data.get('zdf20', 0),
                            'up_count': data.get('up_count', 0),
                            'total_count': data.get('total_count', 0),
                            'net_flow_yi': data.get('net_flow_yi', 0),
                            'lead_stock': data.get('lead_stock', ''),
                            'lead_pct': data.get('lead_pct', 0),
                            # v2.133 (2026-10-01): 领涨股代码 ('sh688265' / 'sz301190') 透给前端 ——
                            #   板块热度 Top8 的「领涨股」要能点开右侧K线 (engine 两个来源: nzg_code / lzg.code)
                            'lead_code': data.get('lead_code', ''),
                        })
                    else:
                        result.append({'name': name, 'change_pct': round(data or 0, 2)})
                now_str = datetime.now().strftime('%m-%d %H:%M:%S')
                _write_json_file(SECTOR_HOT_FILE, {'ts': now_str, 'source': '同花顺 fuyao + 腾讯资金面',
                                                   'sectors': result})
                self._json({'sectors': result, 'source': '同花顺 fuyao + 腾讯资金面',
                            'ts': now_str, 'cached': False})

            elif path == '/api/news':   # v2.111: 财经新闻聚合 (财联社+同花顺); force=1 强制重拉
                self._json(get_news(force=qs.get('force', '0') == '1'))

            elif path == '/api/news/dedup':   # v2.218: 跨源去重账本 (排查用: 判过多少对/正在跑/有无报错)
                _ds = _ndedup.stats()
                _raw = NEWS_RAW['items'] or NEWS_CACHE['items']
                _ds['raw_pool'] = len(_raw)
                try:
                    _pairs = _ndedup._pair_list(_raw)
                    _ds['pairs'] = len(_pairs)
                except Exception:
                    _ds['pairs'] = -1
                self._json({'ok': True, 'dedup': _ds})

            elif path == '/api/news/ai/status':   # v2.112: AI 可用性探活 (**只出 name/model/ready**)
                self._json(ai_news.status())

            elif path == '/api/news/score':   # v2.122: 起一批"对A股有无影响"的批量打标
                # ⚠️ 后台跑 (整池 60~90s), 浏览器轮询 /progress; 已在跑不会叠加
                self._json(ai_news.start_scoring(
                    (get_news() or {}).get('items') or [],
                    force=qs.get('force', '0') == '1'))

            elif path == '/api/news/score/progress':   # v2.122: 打标进度
                self._json(ai_news.score_state())

            else:
                # (2026-08-16 修复: 入口已send_response(200), 直接再发404会双状态行
                #  → 先清headers buffer, 保证单条状态行+前端能读到错误串)
                self._headers_buffer = []
                self.send_response(404)
                self.send_header('Access-Control-Allow-Origin', '*')
                self._json({'error': 'Not found'})

        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
            pass  # 客户端断开
        except Exception as e:
            try:
                self._headers_buffer = []
                self.send_response(500)
                self.send_header('Access-Control-Allow-Origin', '*')
                self._json({'error': str(e)})
            except Exception:
                pass

    def _read_json_body(self) -> dict:
        """读取POST请求体 JSON"""
        try:
            length = int(self.headers.get('Content-Length', 0))
            if length <= 0: return {}
            body = self.rfile.read(length).decode('utf-8')
            return json.loads(body) if body else {}
        except Exception:
            return {}

    def _sim_trade_gate(self) -> dict:
        """v2.226 (2026-10-25): 非交易时间禁止模拟买卖 —— 用户要求。

        只拦**会真改账户**的三个入口: /api/sim/buy、/api/sim/sell、/api/sim/confirm。
        不拦: /api/sim/state(看持仓)、/api/sim/check(只读纪律检查)、
              /api/sim/reset(重置账户)、/api/sim/backtest(离线回放) —— 这些不改持仓或与盘口无关。

        用 is_trading_time() 而非 _in_session_window(): 前者带节假日闸, 否则国庆那天的
        星期五照样能下单。返回 {} 表示放行。

        v2.300 (2026-11-13 用户 m15464): 改用 `sim_tradable_now()` —— 比 is_trading_time()
          多放 **9:25~9:30 集合竞价** 这 5 分钟(竞价已撮出开盘价, 用户要按开盘价买)。
          全局 is_trading_time 仍是 9:30 起, 不受影响。
        """
        if sim_tradable_now():
            return {}
        m = {'MORNING': '盘中', 'AFTERNOON': '盘中', 'NOON_BREAK': '午休',
             'AUCTION': '竞价中',   # v2.300
             'PRE_MARKET': '盘前', 'POST_MARKET': '盘后', 'WEEKEND': '周末', 'HOLIDAY': '休市'}
        st = get_trading_status()
        return {'ok': False,
                'msg': '非交易时间不可买卖 · 现在是%s (%s) · 只在交易日的 9:25-11:30 / 13:00-15:00 可交易'
                       '（9:25~9:30 竞价中按开盘价成交）'
                       % (m.get(st, st), datetime.now().strftime('%m-%d %H:%M'))}

    def _sim_std_code(self, code: str) -> str:
        """标准化股票代码 → SH600519 / SZ000001 / BZ920157

        v2.279 (用户 m10425): 北交所统一归 BZ, 交给 mcap.prefixed 判 ——
          老判据 `9 开头 = SH` 会把 920xxx 拼成 SH920157, 与本地 CSV 名对不上。
        """
        num = code[2:] if code[:2].upper() in ('SH', 'SZ', 'BJ', 'BZ') else code
        return mcap.prefixed(num)

    def _sim_buy(self, qs: dict) -> dict:
        """模拟买入: 拉实时价 + 算纪律参数 (2N止损/0.5N加仓/15%止盈/吊灯)

        v2.300 (2026-11-13 用户 m15464): 9:25~9:30 集合竞价阶段**按开盘价成交**。
          这 5 分钟里 `rt['price']` 还是动态撮合价、不是最终开盘价, 而 9:25 之后
          集合竞价已经撮出开盘价(`rt['open']`), 用户要的就是"竞价结束后以开盘价买入"。
          9:30 之后仍用现价; 开盘价拿不到(<=0, 竞价数据还没生成)才回落现价。
        """
        code = qs.get('code', '')
        if not code: return {'ok': False, 'msg': '缺少代码'}
        full = self._sim_std_code(code)
        sina = full.lower()
        rt = fetch_sina([sina]).get(sina, {})
        price = rt.get('price', 0)
        if price <= 0: return {'ok': False, 'msg': '无实时行情, 无法买入'}
        _t = datetime.now().hour * 100 + datetime.now().minute
        _px_src = ''
        _px_kind = ''          # v2.300: 只有真按开盘价成交才置 'open', 别给前端假信号
        if 925 <= _t < 930:
            # 🔴 防"隔夜开盘价": 数据源没刷新时 rt['open'] 还是**上一个交易日的开盘价**,
            #   照它成交会错到离谱(昨开 39.24 vs 今开 10.5 这种)。用报价日期(qdate)判新鲜度:
            #   对得上今天才用开盘价; 对不上就落回现价(竞价撮合价, 9:25 后已收敛到开盘价)。
            _qd = str(rt.get('qdate') or '')
            _today = datetime.now().strftime('%Y-%m-%d')
            _op = float(rt.get('open') or 0)
            if _op > 0 and (not _qd or _qd == _today):
                price = _op
                _px_src = ' (集合竞价开盘价)'
                _px_kind = 'open'
            elif _op > 0:
                _px_src = ' (竞价报价还是 %s 的, 按现价成交)' % _qd
        name = rt.get('name', full[2:])
        kline = load_csv_kline(full)
        if not kline or len(kline) < 30:
            return {'ok': False, 'msg': '无K线数据, 无法计算止损'}

        tx = TurtleExecutor(kline, 0)
        sl = tx.calc_stop_loss(price)
        addon = tx.calc_addon(price, 1)
        tp = tx.calc_take_profit(price)
        ch = tx.calc_chandelier()
        shares = _safe_int(qs.get('shares'))

        # 2026-08-16: 并发锁 — 包住"加载→操作→保存"全周期 (防双击卖出重复成交/买入丢失)
        with ACCOUNT_LOCK:
            account = SimAccount()
            _res = account.buy(full, name, price, shares=shares,
                               stop_loss=sl['stop_loss'],
                               addon_price=addon['next_addon_price'],
                               take_profit_at=tp['level_price'],
                               chandelier_stop=ch['stop'] if ch['valid'] else 0,
                               n_atr20=sl['n_atr20'], reason='终端手动买入')
        if _px_src and isinstance(_res, dict) and _res.get('ok'):
            _res['msg'] = str(_res.get('msg') or '买入成功') + _px_src
            if _px_kind:
                _res['price_src'] = _px_kind
        return _res

    def _sim_check(self) -> dict:
        """对全部持仓检查纪律 (拉实时价), 返回触发列表 (2026-08-16: 加锁, check_discipline可能写盘)"""
        with ACCOUNT_LOCK:
            account = SimAccount()
            if not account.positions:
                return {'ok': True, 'triggers': [], 'prices': {}, 'msg': '无持仓'}
            sina_codes = [p['code'].lower() for p in account.positions]
            rt = fetch_sina(sina_codes)
            triggers, prices = [], {}
            for p in account.positions:
                q = rt.get(p['code'].lower(), {})
                price = q.get('price', 0)
                prices[p['code']] = round(price, 2) if price > 0 else 0
                if price <= 0: continue
                kline = load_csv_kline(p['code'])
                for tr in account.check_discipline(p, price, kline):
                    tr['code'] = p['code']
                    tr['name'] = p['name']
                    tr['trigger_price'] = price
                    triggers.append(tr)
            return {'ok': True, 'triggers': triggers, 'prices': prices,
                    'msg': f'检查完成: {len(triggers)}个纪律触发'}

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        qs = dict(urllib.parse.parse_qsl(parsed.query))
        try:
            # v2.116 (2026-09-25): AI 对话 —— **必须在 _dispatch_post 之前早返回**。
            #   那个函数第一件事就是 send_response(200), 而这条流要自己写响应头
            #   (application/x-ndjson)。走进去 = 双状态行, 前端拿到的是半截垃圾。
            if path == '/api/ai/chat':
                self._handle_ai_chat()
                return
            # v1 修复 (2026-08-29): 原 do_POST 与 _dispatch_post 各自 send_response(200)
            # → 双状态行, 严格HTTP客户端(PowerShell)报协议违规; 浏览器一直容忍未暴露
            self._dispatch_post(path, qs)
        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
            pass  # 客户端断开
        except Exception as e:
            # (2026-08-16 修复: 原do_POST无异常保护→任何异常直接断连无响应体;
            #  且入口已send_response(200), 再发500需先清headers buffer否则双状态行)
            try:
                self._headers_buffer = []
                self.send_response(500)
                self.send_header('Access-Control-Allow-Origin', '*')
                self._json({'error': str(e)})
            except Exception:
                pass

    def _dispatch_post(self, path, qs):
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path
            qs = dict(urllib.parse.parse_qsl(parsed.query))
            self.send_response(200)
            self.send_header('Access-Control-Allow-Origin', '*')

            # v2.340 (2026-11-18 用户 m25750): 一键更新 —— 在仓库目录跑 git pull。
            #   🔴 为什么不用网页上的"下载 zip 覆盖"：那会把 VERSION 和代码分开更新，
            #      一半新一半旧最难查。git pull 是原子的，pull 完重启即可。
            #   🔴 本地有未提交改动时**拒绝执行**：那说明用户手改过文件，
            #      强拉会把改动冲掉（这个仓库没有备份机制）。
            if path == '/api/version/pull':
                # v2.351: 返回里 how='git'|'zip', 前端据此说不同的重启提示
                self._json(_do_git_pull())
                return
            # v2.349 (2026-11-20 用户 m29038): 悬停分时**批量**拉取 —— 前端攒 200ms 或满 40 个
            #   发一次, 一次网络往返拿 N 只。items 里的写法就是前端缓存键 'kind:code'。
            #   ⚠ 必须是 POST: 40 个板块名拼进 URL 会超长(中文还要 encode 成 3 倍)。
            if path == '/api/mini_minute/batch':
                _pl = self._read_json_body()
                _items = (_pl or {}).get('items') or []
                if not isinstance(_items, list):
                    self._json({'ok': False, 'error': 'items 必须是数组', 'items': {}})
                    return
                try:
                    self._json(mini_minute_batch(_items))
                except Exception as _e:
                    _swlog('mini_batch_route', _e)
                    self._json({'ok': False, 'error': str(_e)[:200], 'items': {}})
                return
            # v2.347 (2026-11-19 用户 m27833): 多股同列的行情快照 —— 前端每 10 秒推一次,
            #   **覆盖式**落盘(force=True)。用户原话:「更新一次写一次, 覆盖前面的数据
            #   而不是不断增量更新」—— archive.save_window 默认有三条去重(内容没变不写、
            #   20 秒内先攒着), 这里必须 force 才能做到"每次都落一份最新的"。
            if path == '/api/idx_sync':
                """v2.359: 把 index_daily 里的指数增量补齐（只拉最近 10 根比对/追加）。

                body: {"codes": ["sh000922", ...]}  省略 = 全部登记代码
                      {"deep": 1} 顺带做深度回补（往前翻页到上市日）
                返回 {ok, items:{code: "appended:N"|"noop"|...}, secs}
                """
                _b = self._read_json_body() or {}
                _cs = _b.get('codes') or list(_ids.IDX_INFO.keys())
                _deep = bool(_b.get('deep'))
                _t0 = time.time()
                _items = {}
                for _c in _cs:
                    _c = str(_c).strip().lower()
                    if not _c:
                        continue
                    try:
                        _items[_c] = (_ids.deepen(_c) and 'deepened') if _deep else _ids.sync(_c)
                    except Exception as _e:
                        _items[_c] = 'fail: %s' % _e
                self._json({'ok': True, 'items': _items,
                            'secs': round(time.time() - _t0, 2)})
                return

            if path == '/api/mtx/save':
                _pl = self._read_json_body()
                if not isinstance(_pl, dict) or not _pl:
                    self._json({'ok': False, 'error': 'payload 为空或不是 JSON'})
                    return
                try:
                    _p = _arc.save_window('mtx_watch', _pl, force=True)
                    self._json({'ok': True, 'saved': bool(_p), 'path': _p})
                except Exception as _e:
                    self._json({'ok': False, 'error': str(_e)[:200]})
                return
            # P3-10: 原POST /api/scan/start 死端点已删 (前端只用GET版, 且此版mode被硬编码忽略)

            if path == '/api/recap/reports':
                # v2.302 (2026-11-13 用户 m16307): 复盘历史存档 —— 保存 / 删除。
                #   🔴 没做 do_DELETE（本服务只实现 GET/POST），所以删除也走 POST，
                #      用 query 里的 _method=del 区分；否则浏览器 DELETE 会拿到 501。
                body = self._read_json_body()
                if (qs.get('_method', '') or '').strip() == 'del' or body.get('_del'):
                    _rid = (qs.get('id', '') or body.get('id', '') or '').strip()
                    self._json({'ok': bool(_mrc.delete_report(_rid))})
                else:
                    _rep = _mrc.save_report(body or {})
                    self._json({'ok': True, 'report': _rep})

            elif path == '/api/datasource/save':
                # v1 (2026-08-29): 保存数据源配置 (校验→备份.bak→原子写→即时生效)
                body = self._read_json_body()
                ok, errs = ds.save(body.get('config', {}))
                if ok:
                    refresh_dirs()  # 本地路径改动即时生效
                # v2.153: 通达信 key 是否成功落进库内 ini（ok=None 表示本次没填新 key）
                self._json({'ok': ok, 'errors': errs, 'tdx': dict(ds.LAST_TDX_SYNC)})

            elif path == '/api/datasource/swap_ai':
                # v2.113: 主/备整槽互换 (含 key)。
                # ⚠️ 必须在服务端做 —— 浏览器手上 key 是掩码, 前端自己换 = 换了等于没换
                #    (见 datasource.swap_ai 的 docstring)。
                body = self._read_json_body()
                ok, errs = ds.swap_ai(body.get('config', {}))
                if ok:
                    refresh_dirs()
                    _p = (ds.load().get('ai') or {}).get('primary') or {}
                    self._json({'ok': True, 'errors': [], 'primary': _p.get('name') or ''})
                else:
                    self._json({'ok': False, 'errors': errs})

            elif path == '/api/datasource/test':
                body = self._read_json_body()
                source = body.get('source', '')
                if source == 'all':
                    keys = ['realtime', 'kline', 'minute', 'sector_board',
                            'limit_pool', 'fundamental',
                            'local.kline_dir', 'local.tdx_export_dir']
                    self._json({k: ds.test(k) for k in keys})
                elif source == 'mini_minute':
                    # v2.239: 浮窗分时横跨同花顺 + 腾讯两个源, 不在 data_sources.json 里 ⇒ 单独测
                    self._json(_mini_test())
                else:
                    self._json(ds.test(source))

            elif path == '/api/datasource/reset':
                # 恢复默认 (当前配置先备份到 .bak, 配置本身删除)
                cfg_backup = ds.load()
                with open(ds.CFG_FILE + '.bak', 'w', encoding='utf-8') as fh:
                    json.dump(cfg_backup, fh, ensure_ascii=False, indent=2)
                if os.path.exists(ds.CFG_FILE):
                    os.remove(ds.CFG_FILE)
                ds._CACHE['mtime'] = 0.0
                refresh_dirs()  # 本地路径回默认也需即时生效
                self._json({'ok': True, 'msg': '已恢复默认配置'})

            elif path == '/api/datasource/restore':
                ok, errs = ds.restore_backup()
                if ok:
                    refresh_dirs()
                self._json({'ok': ok, 'msg': '已恢复上次备份' if ok else errs[0]})

            elif path == '/api/sim/buy':
                _g = self._sim_trade_gate()
                if _g:
                    self._json(_g); return
                body = self._read_json_body()
                code = body.get('code', qs.get('code', ''))
                shares = _safe_int(body.get('shares'))
                self._json(self._sim_buy({'code': code, 'shares': shares}))

            elif path == '/api/sim/sell':
                _g = self._sim_trade_gate()
                if _g:
                    self._json(_g); return
                body = self._read_json_body()
                code = body.get('code', qs.get('code', ''))
                if not code:
                    self._json({'ok': False, 'msg': '缺少代码'}); return
                full = self._sim_std_code(code)
                shares = _safe_int(body.get('shares'))
                reason = body.get('reason', '手动卖出')
                sina = full.lower()
                rt = fetch_sina([sina]).get(sina, {})
                price = rt.get('price', 0)
                # 2026-08-16: 并发锁 (防双击卖出重复成交)
                with ACCOUNT_LOCK:
                    if price <= 0:
                        account = SimAccount()
                        pos = account._find_pos(full)
                        price = pos['avg_cost'] if pos else 0
                    account = SimAccount()
                    self._json(account.sell(full, price, shares, reason=reason))

            elif path == '/api/sim/check':
                self._json(self._sim_check())

            elif path == '/api/sim/confirm':
                _g = self._sim_trade_gate()
                if _g:
                    self._json(_g); return
                body = self._read_json_body()
                code = body.get('code', '')
                action = body.get('action', '')
                if not code or not action:
                    self._json({'ok': False, 'msg': '缺少参数'}); return
                full = self._sim_std_code(code)
                # 2026-08-16: 并发锁 (防纪律确认与手动买卖交错丢更新)
                with ACCOUNT_LOCK:
                    account = SimAccount()
                    pos = account._find_pos(full)
                    if not pos:
                        self._json({'ok': False, 'msg': '无此持仓'}); return
                    sina = full.lower()
                    rt = fetch_sina([sina]).get(sina, {})
                    price = rt.get('price', 0)
                    if price <= 0: price = pos['avg_cost']
                    kline = load_csv_kline(full)
                    self._json(account.confirm(full, action, price, kline))

            elif path == '/api/sim/reset':
                body = self._read_json_body()
                cash = _safe_float(body.get('initial_cash'), 1_000_000)
                with ACCOUNT_LOCK:
                    acc = SimAccount(initial_cash=cash)
                    acc.reset()
                    clear_sim_history_cache()  # v2.65: 重置联动清空历史收益记录
                    self._json({'ok': True, 'msg': f'模拟账户已重置 (初始{cash:,.0f}), 历史收益已清空'})

            elif path == '/api/scan/cache/prices':
                # v2.106 (2026-09-21): 前端刷完实时价回写落盘槽 (只价格字段, 见 _patch_scan_slot)
                # v2.107: body 加 lines = 前端**正在看**的那一档 (缺省 '4' 兼容旧前端)
                body = self._read_json_body()
                mode = str(body.get('mode', ''))
                pool = body.get('pool', 'auto')
                prices = body.get('prices') or {}
                if mode not in SCAN_MODES or pool not in ('auto', 'file'):
                    self._json({'ok': False, 'msg': 'bad mode/pool'}); return
                if not isinstance(prices, dict) or len(prices) > 1000:
                    self._json({'ok': False, 'msg': 'bad prices'}); return
                self._json({'ok': True,
                            'patched': _patch_scan_slot(pool, mode, body.get('lines', '4'), prices)})

            elif path == '/api/watchlist/add':
                body = self._read_json_body()
                code = body.get('code', '')
                if not code:
                    self._json({'ok': False, 'msg': '缺少代码'}); return
                full = self._sim_std_code(code)
                num = full[2:]
                with WATCH_LOCK:
                    wl = load_watchlist()
                    if any(w['code'] == full for w in wl):
                        self._json({'ok': False, 'msg': '已在自选中'}); return
                    # 拉名称
                    sina = full.lower()
                    rt = fetch_sina([sina]).get(sina, {})
                    name = rt.get('name', num)
                    wl.append({'code': full, 'name': name, 'added': datetime.now().strftime('%m-%d %H:%M')})
                    save_watchlist(wl)
                    self._json({'ok': True, 'msg': f'已加自选: {name}'})

            elif path == '/api/watchlist/remove':
                body = self._read_json_body()
                code = body.get('code', '')
                if not code:
                    self._json({'ok': False, 'msg': '缺少代码'}); return
                full = self._sim_std_code(code)
                with WATCH_LOCK:
                    wl = [w for w in load_watchlist() if w['code'] != full]
                    save_watchlist(wl)
                    self._json({'ok': True, 'msg': '已删除自选'})

            elif path == '/api/sim/backtest':
                body = self._read_json_body()
                if SIM_BT_STATE['running']:
                    self._json({'ok': False, 'msg': '回测已在运行'}); return
                start = body.get('start', '')
                end = body.get('end', '')
                mode = body.get('mode', '20')
                min_score = _safe_float(body.get('min_score'), 13)
                market_filter = _safe_int(body.get('market_filter'))
                sector_filter = bool(body.get('sector_filter', False))
                strategy = body.get('strategy', 'trend')
                allow_switch = bool(body.get('allow_switch', False))
                if not start or not end:
                    self._json({'ok': False, 'msg': '需指定日期区间 (YYYY-MM-DD)'}); return

                SIM_BT_STATE.update({'running': True, 'progress': 0, 'total': 0,
                                     'msg': '预加载K线...', 'result': None, 'error': None})

                def _run():
                    try:
                        def on_progress(cur, total, nav):
                            SIM_BT_STATE.update({'progress': cur, 'total': total,
                                                 'msg': f'回放中 {cur}/{total} 净值{nav:,.0f}'})
                        if strategy == 'sector':
                            # 板块趋势策略 (用户核心策略: 板块6线多头进/破坏离场)
                            result = simulate_sector_trend(start, end,
                                                           allow_switch=allow_switch,
                                                           progress_callback=on_progress)
                        else:
                            result = simulate_history(start, end, mode=mode,
                                                      min_score=min_score,
                                                      market_filter=market_filter,
                                                      sector_filter=sector_filter,
                                                      progress_callback=on_progress)
                        SIM_BT_STATE.update({'running': False, 'result': result,
                                             'msg': '完成'})
                    except Exception as e:
                        import traceback
                        SIM_BT_STATE.update({'running': False, 'error': str(e),
                                             'msg': f'失败: {e}'})
                        traceback.print_exc()

                threading.Thread(target=_run, daemon=True).start()
                self._json({'ok': True, 'msg': '历史回放已启动'})

            else:
                self._headers_buffer = []
                self.send_response(404)
                self.send_header('Access-Control-Allow-Origin', '*')
                self._json({'error': 'Not found'})

    # ---- helpers ----
    def _run_kline_update(self):
        """后台更新K线数据 (2026-08-04: 改为同花顺快照导入, 不再联网腾讯更新)

        用户定案: 联网更新停用 (update_daily 从通达信重建map会篡改行业/概念);
        更新按钮 = 按用户每日导出的同花顺快照文件更新 K线+行业/概念
        """
        import subprocess, tempfile
        global UPDATE_STATE
        if UPDATE_STATE['running']:
            return {'ok': False, 'msg': '更新已在运行中'}
        try:
            script = os.path.join(BASE_DIR, 'kline_data', 'import_ths_snapshot.py')
            if not os.path.exists(script):
                return {'ok': False, 'msg': 'import_ths_snapshot.py 不存在'}

            # 创建临时日志文件
            log_fd, log_path = tempfile.mkstemp(suffix='.log', prefix='kline_update_')
            log_file = open(log_path, 'w')

            UPDATE_STATE = {
                'running': True,
                'progress': '启动中...',
                'error': '',
                'log_file': log_path,
                'started_at': datetime.now().strftime('%H:%M:%S'),
            }

            def _run_and_track():
                global UPDATE_STATE
                proc = subprocess.Popen([sys.executable, '-u', script],
                                       cwd=str(BASE_DIR),
                                       stdout=log_file,
                                       stderr=subprocess.STDOUT,
                                       bufsize=0,
                                       # v2.342 (用户 m26567): pythonw 服务自身无控制台，
                                       #   起子进程会被 Windows 配个终端窗口 ⇒ 屏幕黑窗一闪。
                                       creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
                proc.wait()
                log_file.close()
                # v2.131 (2026-10-01): 快照导入跑完 → 用同一批快照把市值两列强制刷成最新值
                #   (用户定案: 点「📥 快照导入」= 强制全部写入最新市值; 快照里没数据的票保持原值不动)
                _mc = {}
                try:
                    _mc = mcap.sync_from_snapshots(force=True, log=lambda s: print(s, flush=True))
                except Exception as _e:
                    _mc = {'error': str(_e)[:60]}
                # 读取最后几行作为进度摘要
                try:
                    with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
                        lines = f.readlines()
                        # v2.16.3c (2026-08-28): ❌ 拒绝信息可能不在最后3行 (其后再有
                        # "Map: 保持旧文件"/"✅ 单位校验"等输出) — 从末尾往前扫全日志,
                        # 只要有 ❌ 就优先展示, 否则用户会误以为"完成"实际被拒写
                        err = ''
                        for l in reversed(lines):
                            if '❌' in l:
                                err = l.strip()
                                break
                        if err:
                            UPDATE_STATE['progress'] = err
                            UPDATE_STATE['error'] = err
                        else:
                            last = [l.strip() for l in lines[-3:] if l.strip()]
                            UPDATE_STATE['progress'] = last[-1] if last else '完成'
                            UPDATE_STATE['error'] = ''
                except Exception as _e:
                    _swlog('kline_update_track', _e)
                    UPDATE_STATE['progress'] = '完成'
                # v2.131: 市值写回摘要附在末尾 (不覆盖导入本身的结论/❌报错)
                if _mc.get('error'):
                    UPDATE_STATE['progress'] = (UPDATE_STATE['progress'][:120] + ' | ⚠市值写回失败: ' + _mc['error'])
                elif _mc.get('snap'):
                    UPDATE_STATE['progress'] = (UPDATE_STATE['progress'][:140] + ' | 市值 %d只' % _mc.get('updated', 0))
                UPDATE_STATE['running'] = False
                # 清理日志
                try: os.unlink(log_path)
                except OSError as _e: _swlog('unlink_update_log', _e)

            t = threading.Thread(target=_run_and_track, daemon=True)
            t.start()

            # 同时启动一个线程定期读取进度
            def _track_progress():
                global UPDATE_STATE
                while UPDATE_STATE['running']:
                    try:
                        with open(log_path, 'r') as f:
                            lines = f.readlines()
                            # 找最后一行有意义的内容
                            for line in reversed(lines):
                                line = line.strip()
                                if line and ('/' in line or '完成' in line):
                                    UPDATE_STATE['progress'] = line[-120:]
                                    break
                    except Exception as _e:
                        _swlog('kline_update_poll', _e)
                    time.sleep(0.5)

            threading.Thread(target=_track_progress, daemon=True).start()

            return {'ok': True, 'msg': 'K线更新已启动'}
        except Exception as e:
            UPDATE_STATE['running'] = False
            return {'ok': False, 'msg': str(e)}

    def _run_kline_update_snapshot(self, limit=0):
        """v2.30 (2026-09-01): ⚡ 批量快照一键更新 — qt.gtimg.cn 快照增量 (全市场15-40秒)

        从 8801 pkg v2.29 移植 (主项目版差异: 本地 .day 缺口检测更准, 腾讯日历作兜底)。
        与快照导入共用 UPDATE_STATE['running'] + UPDATE_LOCK 互斥。
        🔒 只写 daily/*.csv — 绝不碰 industry_map/concept_map (map铁律)。
        口径与 import_ths_snapshot v2.7 一致: 当日真实价, 量=手。
        v2.35 (2026-09-18): 接入北交所 —— 本地基名 BZ###### 统一映射成腾讯 bj######
        (qt symbol 与 fqkline market 两处), 涨跌停阈值 31%, K线兜底改走 newfqkline
        (fqkline 对 bj 只返 1 根会截断)。除权/平移判定与沪深**完全同一套**, 无分支。
        除权检测 v2.30.2: 昨收≠本地收即拦 — 纯派息 (负差≤8%, 末行=昨交易日) → 本地整段平移
        (差=每股派息, 离线零腾讯请求); 送转/缺口/昨收缺失/名称XD → 腾讯 fqkline 801根尾部替换兜底。
        """
        import urllib.request
        global UPDATE_STATE
        if not UPDATE_LOCK.acquire(blocking=False):
            return {'ok': False, 'msg': '更新已在运行中'}
        try:
            if UPDATE_STATE['running']:
                UPDATE_LOCK.release()
                return {'ok': False, 'msg': '更新已在运行中'}
            daily_dir = ds.resolve(ds.get('local', 'kline_dir', os.path.join('kline_data', 'daily')))
            if not os.path.isdir(daily_dir):
                UPDATE_LOCK.release()
                return {'ok': False, 'msg': f'daily 目录不存在: {daily_dir}'}
            tdx_root = os.path.join(BASE_DIR, '数据')
            # v2.32 (2026-09-01): 腾讯 fqkline 域名自动切换 — ifzq 主域被 501 风控时
            # 自动切 web 子域/proxy 代理 (与 engine._fqkline_req 同域名链; 腾讯风控来回轮换)
            _kline_hosts = ('web.ifzq.gtimg.cn', 'ifzq.gtimg.cn', 'proxy.finance.qq.com')
            _kline_host = [0]

            def _http_get_json(market, code, count):
                # v2.35 (2026-09-18): 北交所必须走 newfqkline —— fqkline 对 bj 前缀只返 1 根
                # (web.ifzq/ifzq 两个域名实测都是 {'day':1}), 拿它做尾部替换会把 260 根砍成 1 根。
                # newfqkline 对 bj 返 563~800 根 qfqday; proxy.finance.qq.com 无该端点(404)故跳过。
                api = 'newfqkline' if market == 'bj' else 'fqkline'
                hosts = _kline_hosts[_kline_host[0]:] + _kline_hosts[:_kline_host[0]]
                for host in hosts:
                    if api == 'newfqkline' and host.startswith('proxy'):
                        continue
                    url = ('https://%s/appstock/app/%s/get?param=%s%s,day,,,%s,qfq'
                           % (host, api, market, code, count))
                    try:
                        req = urllib.request.Request(url, headers={
                            'User-Agent': 'Mozilla/5.0', 'Referer': 'https://gu.qq.com/'})
                        j = json.loads(_net.open_url(req, timeout=8)
                                      .read().decode('utf-8', 'ignore'))
                        if (j.get('data') or {}).get(market + code):
                            _kline_host[0] = _kline_hosts.index(host)
                            return j
                    except Exception:
                        continue
                return None

            def _limit_cap(code):
                """除权/缺口阈值 = 涨跌停上限+1% (import_ths_snapshot 同款)"""
                if code.startswith(('SH688', 'SH689', 'SZ30')):
                    return 21.0
                if code.startswith(('BJ', 'BZ')):   # 北交所 30% 板 (本地基名是 BZ920xxx; 'BJ' 为历史遗留写法)
                    return 31.0
                return 11.0

            def _last_date(fpath):
                try:
                    with open(fpath, 'rb') as f:
                        f.seek(0, os.SEEK_END)
                        size = f.tell()
                        f.seek(max(0, size - 512))
                        tail = f.read().decode('utf-8', errors='replace').strip()
                    lines = [l for l in tail.splitlines() if l.strip()]
                    return lines[-1].split(',')[0] if lines else ''
                except Exception:
                    return ''

            def _qt_sym(c):
                """本地 CSV 基名 → 腾讯 symbol (小写)。北交所 'BZ920002' → 'bj920002':
                qt.gtimg.cn 只认 bj 前缀, 发 bz920002 回的是 pv_none_match(空值), 会让
                344 只北交所全被误报「快照缺失」(2026-09-18 实测)。
                沪深 'SH600519' → 'sh600519', 与旧写法 c.lower() 逐字节相同。"""
                c = str(c)
                return ('bj' + c[2:]) if c[:2].upper() == 'BZ' else c.lower()

            def _market_of(c):
                """本地 CSV 基名 → 腾讯K线接口 market 前缀 (沪深 sh/sz, 北交所 bj)"""
                return 'bj' if c[:2].upper() in ('BZ', 'BJ') else ('sh' if c.startswith('SH') else 'sz')

            def _fetch_batch(codes):
                """拉一批快照 (50只) → {code小写: dict}; 失败抛异常由外层重试"""
                url = 'http://qt.gtimg.cn/q=' + ','.join(_qt_sym(c) for c in codes)
                req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                txt = _net.open_url(req, timeout=10).read().decode('gbk', 'ignore')
                out = {}
                for line in txt.split(';'):
                    line = line.strip()
                    if not line.startswith('v_') or '="' not in line:
                        continue
                    key = line[2:line.index('=')].lower()
                    try:
                        f = line.split('="')[1].rstrip('"').split('~')
                    except IndexError:
                        continue
                    if len(f) < 36 or not f[3]:
                        continue
                    # v2.132 (2026-10-08): 顺带取市值两列 —— 用户定案「更新器要写市值」
                    #   qt 字段: f[44]=流通市值 f[45]=总市值 (亿元, 实测 sh600519 → 15698.40)
                    try:
                        _mc_t = float(f[45]) if len(f) > 45 and f[45] not in ('', '--') else None
                    except ValueError:
                        _mc_t = None
                    try:
                        _mc_f = float(f[44]) if len(f) > 44 and f[44] not in ('', '--') else None
                    except ValueError:
                        _mc_f = None
                    out[key] = {'name': f[1], 'price': float(f[3]), 'prev': float(f[4]),
                                'open': float(f[5]), 'vol': float(f[6]), 'high': float(f[33]),
                                'low': float(f[34]), 'time': f[30],
                                'mcap': _mc_t or None, 'fmcap': _mc_f or None}
                return out

            def _sina_fetch_batch(codes):
                """v2.170 (2026-10-21): 新浪兜底（同花顺和腾讯都挂时最后一道）。
                口径: 新浪量=**股** ⇒ ÷100 成本地的手；**没有市值**、没有行情时间 ⇒
                市值留 None（下游按股本不变假设推），时间用本机现在（否则 snap_date 为空，
                下游 `last_date >= snap_date` 会直接 TypeError）。"""
                url = 'http://hq.sinajs.cn/list=' + ','.join(_qt_sym(c) for c in codes)
                req = urllib.request.Request(url, headers={
                    'User-Agent': 'Mozilla/5.0', 'Referer': 'https://finance.sina.com.cn/'})
                # v2.239: 同 _qc_fetch_sina, 境内源先直连 (代理只会添乱), 失败才回退
                with _net.open_url(req, timeout=10) as _r:
                    txt = _r.read().decode('gbk', 'ignore')
                now_s = datetime.now().strftime('%Y%m%d%H%M%S')
                out = {}
                for line in txt.split(';'):
                    line = line.strip()
                    if not line.startswith('var hq_str_') or '="' not in line:
                        continue
                    key = line[len('var hq_str_'):line.index('=')].lower()
                    try:
                        f = line.split('="')[1].rstrip('"').split(',')
                    except IndexError:
                        continue
                    if len(f) < 10 or not f[3]:
                        continue
                    try:
                        out[key] = {'name': f[0], 'price': float(f[3]), 'prev': float(f[2]),
                                    'open': float(f[1]), 'vol': float(f[8]) / 100.0,
                                    'high': float(f[4]), 'low': float(f[5]), 'time': now_s,
                                    'mcap': None, 'fmcap': None}
                    except (TypeError, ValueError):
                        continue
                return out

            def _tencent_rewrite_tail(fn, lines):
                """腾讯 fqkline 前复权 801根 尾部替换 — 除权/缺口票兜底
                本地日期 < 腾讯首根日期的头部保留+接缝缩放对齐腾讯基准;
                688/689 量=股→÷100; 原子写. 成功返回None, 失败返回错误摘要"""
                code = fn[:-4]
                num = code[2:]
                market = _market_of(code)
                last_err = ''
                for attempt in range(2):  # 失败重试1次
                    time.sleep(1.3)  # 限流安全线 1.5 req/s
                    try:
                        j = _http_get_json(market, num, '800')
                        if j is None:
                            last_err = '腾讯域名全挂'; time.sleep(0.5); continue
                        stock_data = j.get('data', {}).get(f'{market}{num}', {})
                        bars = stock_data.get('qfqday') or stock_data.get('day', [])
                        if not bars:
                            last_err = '腾讯无数据'; time.sleep(0.5); continue
                        is_688 = code.startswith('SH688') or code.startswith('SH689')
                        tail = []
                        for b in bars:
                            if len(b) < 6 or '-' not in str(b[0]):
                                continue
                            vals = list(b[:6])
                            try:
                                o, c, h, l = (float(vals[1]), float(vals[2]),
                                              float(vals[3]), float(vals[4]))
                                v = float(vals[5]) / 100 if is_688 else float(vals[5])
                                tail.append(f"{vals[0]},{o:.3f},{c:.3f},{h:.3f},{l:.3f},{v:.0f}")
                            except (ValueError, TypeError):
                                continue
                        if not tail:
                            last_err = 'K线解析为空'; time.sleep(0.5); continue
                        first_date = tail[0].split(',')[0]
                        head = [l for l in lines if l.split(',')[0] < first_date]
                        if head:  # 接缝缩放: 头部×factor 对齐腾讯前复权基准 (前复权基准=现在)
                            prev_close = float(head[-1].split(',')[2])
                            new_open = float(tail[0].split(',')[1])
                            if prev_close > 0 and new_open > 0:
                                # v2.30.2: 接缝跳变拒绝 (同 import_tdx_history L197-202)
                                # 腾讯 qfq 因子错乱票 (SH601225/SZ000937 段间跳变666%/155%) 会在此拦截,
                                # 放弃重写保持原样, 防止头部被错误缩放十几倍写坏
                                delta = abs(new_open / prev_close - 1) * 100
                                if delta > 100:
                                    return f'接缝跳变{delta:.1f}% (本地收{prev_close} vs 腾讯开{new_open}), 放弃重写'
                                factor = new_open / prev_close
                                head = [f"{l.split(',')[0]},{float(l.split(',')[1]) * factor:.3f},"
                                        f"{float(l.split(',')[2]) * factor:.3f},"
                                        f"{float(l.split(',')[3]) * factor:.3f},"
                                        f"{float(l.split(',')[4]) * factor:.3f},{l.split(',')[5]}"
                                        for l in head]
                        filepath = os.path.join(daily_dir, fn)
                        tmp = filepath + '.tmp'
                        with open(tmp, 'w', encoding='utf-8', newline='') as fh:
                            fh.write('\n'.join([lines[0]] + head + tail) + '\n')
                        os.replace(tmp, filepath)
                        return None
                    except Exception as e:
                        last_err = str(e)[:80]
                        if 'Expecting value' in last_err or 'JSONDecode' in last_err or '501' in last_err:
                            time.sleep(3)
                        else:
                            time.sleep(1.5)
                return last_err

            def _create_new_csv(code, snap_d):
                """v2.30.1: 新股/快照有行情但本地无CSV → 腾讯 fqkline 全量建 CSV
                新股历史短(上市首日起), 前复权=真实价; 688/689 量=股 → ÷100 手
                ⚠ Windows 大小写不敏感: 已存在文件一律跳过, 防覆盖已有K线

                v2.249 (2026-11-06 用户 m21803): **实现已抽到 `quant_terminal/newstock.py`**,
                手动按钮与自动任务共用同一份 —— 以后改口径只改一处。
                这里只做参数适配 + 返回值翻译（老调用方判的是「None=成功 / 字符串=原因」）。
                """
                from quant_terminal import newstock as _ns
                # v2.339 (2026-11-17 用户 m25177): 补传 free —— 原来只传了市值两列,
                #   新股建出来的文件第 9 列**永远是空的**, 要等 15:35 那轮快照才补得上。
                #   快照结构里本来就有 free(通达信 FreeLtgb, 万股), 与下面 _patch_caps_after_rewrite
                #   用的是同一个字段。
                ok, why = _ns.create_one(code, cap=snap_d.get('mcap'),
                                         fmcap=snap_d.get('fmcap'),
                                         free=snap_d.get('free'))
                return None if ok else why

            def _patch_caps_after_rewrite(code, date, snap_d):
                """v2.132 (2026-10-08): 腾讯兜底重写尾部只出 6 列 → 末行还是当天就把市值补回
                (只认末行日期 == 快照日期; 走 mcap.write_one, 与快照导入同一套 9 列矩形规则)"""
                if not (snap_d.get('mcap') or snap_d.get('fmcap')):
                    return False
                p = os.path.join(daily_dir, code + '.csv')
                try:
                    sz = os.path.getsize(p)
                    with open(p, 'rb') as fh:
                        fh.seek(max(0, sz - 512))
                        tl = [l for l in fh.read().decode('utf-8', 'replace').strip().splitlines() if l.strip()]
                except OSError:
                    return False
                if not tl or tl[-1].split(',')[0] != date:
                    return False
                wrote, _why = mcap.write_one(code, snap_d.get('mcap'), snap_d.get('fmcap'),
                                             force=True, free=snap_d.get('free'))
                return wrote

            files = sorted(f for f in os.listdir(daily_dir) if f.endswith('.csv'))
            existing = {f[:-4].lower() for f in files}  # v2.30.1: 新股检测用 (limit 截断前全量)
            if limit > 0:
                files = files[:limit]

            UPDATE_STATE = {
                'running': True, 'progress': '0/0 拉取快照中...', 'error': '',
                'log_file': None, 'started_at': datetime.now().strftime('%H:%M:%S'),
                'kind': 'qt_snapshot', 'percent': 2,
            }

            done = {'n': 0}
            fails = []
            caps_n = {'n': 0}   # v2.132: 追加行里写了市值两列的只数 (摘要用)

            def _run():
                global UPDATE_STATE
                try:
                    # ---- 1. 批量拉快照 ----
                    # v2.170 (2026-10-21) 用户 m00787: 这个按钮**优先走同花顺 fuyao**
                    #   (免费、一次请求拿全市场, 实测 1.1 秒)；失败退回腾讯 50 只/批；
                    #   腾讯也全挂才用新浪兜底。三源口径差已各自处理:
                    #   fuyao 量=手(源内已转)、无市值; 腾讯量=手、带市值; 新浪量=股(这里 ÷100)、无市值。
                    snap = {}
                    snap_src = ''
                    try:
                        from quant_terminal import fuyao as _fq
                        if _fq.configured():
                            _rows, _ts = _fq.snapshot_all()
                            _tstr = (time.strftime('%Y%m%d%H%M%S', time.localtime(int(_ts) / 1000.0))
                                     if _ts else datetime.now().strftime('%Y%m%d%H%M%S'))
                            for _fn in files:
                                _r = _rows.get(_fn[:-4][-6:])
                                if not _r or _r.get('close') is None or _r['close'] <= 0:
                                    continue
                                snap[_qt_sym(_fn[:-4])] = {
                                    'name': '', 'price': _r['close'], 'prev': _r['prev_close'],
                                    'open': _r['open'], 'vol': float(_r['volume']),
                                    'high': _r['high'], 'low': _r['low'], 'time': _tstr,
                                    'mcap': None, 'fmcap': None}
                            if snap:
                                snap_src = '同花顺'
                                UPDATE_STATE['percent'] = 10
                                UPDATE_STATE['progress'] = '同花顺快照 %d 只' % len(snap)
                    except Exception as _e:
                        fails.append('同花顺快照失败: %s' % str(_e)[:60])
                    if not snap:
                        for i in range(0, len(files), 50):
                            batch = files[i:i + 50]
                            for k in range(3):  # 重试3次
                                try:
                                    snap.update(_fetch_batch([fn[:-4] for fn in batch]))
                                    break
                                except Exception as e:
                                    if k == 2:
                                        fails.append(f"批次{i // 50}: {str(e)[:60]}")
                                    time.sleep(0.5)
                            UPDATE_STATE['percent'] = 2 + int(i * 8 / max(len(files), 1))
                        if snap:
                            snap_src = '腾讯'
                    if not snap:
                        # 最后兜底: 新浪（只保证价格/量, 没有市值也没有行情时间）
                        fails.append('腾讯快照全失败, 退回新浪')
                        for i in range(0, len(files), 50):
                            batch = files[i:i + 50]
                            try:
                                snap.update(_sina_fetch_batch([fn[:-4] for fn in batch]))
                            except Exception as e:
                                fails.append(f"新浪批次{i // 50}: {str(e)[:60]}")
                            UPDATE_STATE['percent'] = 2 + int(i * 8 / max(len(files), 1))
                        if snap:
                            snap_src = '新浪'
                    if not snap:
                        raise RuntimeError('快照拉取全失败 (同花顺/腾讯/新浪都失败)')
                    UPDATE_STATE['progress'] = f'{snap_src}快照 {len(snap)} 只, 逐只写盘...'
                    # v2.177 (2026-10-22): 新股发现改用同花顺标的表 (快照文件已下线) —
                    #   把「同花顺有、本地没有 K线文件」的代码挑出来, 后面照旧按老路补齐 CSV。
                    new_codes = []
                    if not limit:
                        try:
                            from quant_terminal import sector_kline as _sk2
                            _tk2 = _sk2.tickers() or {}
                            _ex = set(str(x).lower() for x in existing)
                            for _c2 in _tk2.keys():
                                _c2 = str(_c2)
                                if not (len(_c2) == 6 and _c2.isdigit()):
                                    continue
                                if _c2[:3] == '920' or _c2[0] in '48':
                                    _p2 = 'BZ' + _c2
                                elif _c2[0] in '69':
                                    _p2 = 'SH' + _c2
                                else:
                                    _p2 = 'SZ' + _c2
                                if _p2.lower() not in _ex:
                                    new_codes.append(_p2)
                            new_codes = sorted(set(new_codes))
                        except Exception as _e2:
                            print('[snap] 新股发现(同花顺)失败: %s' % _e2, file=sys.stderr, flush=True)

                    # 快照日期/盘中保护 (行情时间 < 15:05 = 盘中, 防半日K线入库后不再刷新)
                    snap_date = snap_ts = None
                    for d in snap.values():
                        if d['time'] and len(d['time']) >= 14:
                            snap_date = f"{d['time'][:4]}-{d['time'][4:6]}-{d['time'][6:8]}"
                            snap_ts = d['time'][8:14]
                            break
                    if snap_ts and snap_ts < '150500':
                        UPDATE_STATE['progress'] = f'⚠ 盘中数据 ({snap_date} {snap_ts[:2]}:{snap_ts[2:4]}), 收盘(15:05)后再更新'
                        UPDATE_STATE['percent'] = 100
                        return
                    # 缺口日历: sz000001 最近11根日K, 末2根 = [昨日, 今日] (上证日K即活交易日历)
                    # (v2.34e: 唯一缺口依据 — 用户不再通达信同步, .day 恒滞后已废弃)
                    cal_prev_day = ''
                    try:
                        j = _http_get_json('sz', '000001', '10')
                        bars0 = (j.get('data', {}).get('sz000001', {}).get('qfqday') or
                                 j.get('data', {}).get('sz000001', {}).get('day') or []) if j else []
                        if len(bars0) >= 2:
                            cal_prev_day = bars0[-2][0]
                    except Exception as _e:
                        _swlog('cal_prev_day', _e)
                    # ---- 2. 逐只增量追加 (读末行/除权/缺口检测/腾讯兜底重写/追加) ----
                    # v2.30.1 三点修复:
                    #   ① 688/689 qt 腾讯量=股 → ÷100 统一手 (历史=手, 快照总手=手)
                    #   ② 名称 XD/XR/DR = 除息/除权日 → 强制腾讯前复权重写 (小额分红 diff<11% 会漏网)
                    #   ③ 快照有行情但本地无CSV (上市新股) → 腾讯 fqkline 建 CSV
                    fixed = {'n': 0}
                    fixed_vol = {'n': 0}   # v2.30.1: 688 末根量单位本地修正数
                    shifted = {'n': 0}     # v2.30.2: 纯派息除息本地整段平移数
                    caps_fixed = {'n': 0}  # v2.132: 腾讯兜底重写后补回市值两列数
                    for fn in files:
                        c = fn[:-4]
                        d = snap.get(_qt_sym(c))
                        if not d:
                            fails.append(f"{c}: 快照缺失")
                            done['n'] += 1
                            continue
                        if d['vol'] <= 0 and d['open'] <= 0:
                            done['n'] += 1  # 停牌, 跳过
                            continue
                        last_date = _last_date(os.path.join(daily_dir, fn))
                        is688 = c.startswith(('SH688', 'SH689'))
                        xd_flag = d.get('name', '').startswith(('XD', 'XR', 'DR'))
                        if last_date >= snap_date and not xd_flag and not is688:
                            done['n'] += 1  # 已最新
                            continue
                        # 除权检测: 快照昨收 vs 本地末行收盘 (涨跌停股 diff≈0 不误判)
                        try:
                            with open(os.path.join(daily_dir, fn), encoding='utf-8') as fh:
                                last = fh.readlines()[-1].strip().split(',')
                            last_close = float(last[2])
                            last_vol = float(last[5]) if len(last) > 5 else 0
                        except Exception:
                            fails.append(f"{c}: 读末行失败")
                            done['n'] += 1
                            continue
                        # v2.170 (2026-10-21): 同花顺/新浪快照**不带市值** ⇒ 按「股本不变」用
                        #   本地末行市值 × 今价/末行价 推出来, 别让追加行把市值两列写成空。
                        #   真除权那条路下面会走腾讯尾部重写, 市值会在那里再补一次 (caps_fixed)。
                        if not (d.get('mcap') or d.get('fmcap')):
                            try:
                                _mt = float(last[6]) if len(last) > 6 and str(last[6]).strip() else 0.0
                                _mf = float(last[7]) if len(last) > 7 and str(last[7]).strip() else 0.0
                                if last_close > 0 and d['price'] > 0:
                                    if _mt > 0:
                                        d['mcap'] = round(_mt * d['price'] / last_close, 2)
                                    if _mf > 0:
                                        d['fmcap'] = round(_mf * d['price'] / last_close, 2)
                            except (TypeError, ValueError, IndexError):
                                pass
                        # v2.273 (用户 m08653): 第 9 列「自由流通股本」**不随价格变** —— 直接沿用末行原值。
                        #   快照源（同花顺/腾讯/新浪）都没有这个字段, 只有通达信 FreeLtgb 有;
                        #   这里不覆盖就会让新追加的行把它写成空 (等于丢数据)。
                        if d.get('free') is None:
                            try:
                                _me = float(last[8]) if len(last) > 8 and str(last[8]).strip() else 0.0
                                if _me > 0:
                                    d['free'] = _me
                            except (TypeError, ValueError, IndexError):
                                pass
                        need_rewrite = ''
                        need_shift = 0.0
                        prev = d['prev']
                        if is688 and snap_src == '腾讯' and last_date == snap_date \
                                and d['vol'] > 0 and last_vol > 0 \
                                and abs(last_vol - d['vol']) / d['vol'] < 0.05:
                            # v2.30.1: 已入库 688 末根量=股(错) → 本地修正 ÷100 (历史均为手, 无需腾讯)
                            # v2.132: 只替换第6列, 后面市值两列原样保留 (旧写法 [:5] 会把它截掉)
                            with open(os.path.join(daily_dir, fn), encoding='utf-8') as fh:
                                lines = fh.read().splitlines()
                            _c = lines[-1].split(',')
                            _c[5] = f"{last_vol / 100:.0f}"
                            lines[-1] = ','.join(_c)
                            tmpf = os.path.join(daily_dir, fn) + '.tmp'
                            with open(tmpf, 'w', encoding='utf-8', newline='') as fh:
                                fh.write('\n'.join(lines) + '\n')
                            os.replace(tmpf, os.path.join(daily_dir, fn))
                            fixed_vol['n'] += 1
                            done['n'] += 1
                            UPDATE_STATE['progress'] = f"{done['n']}/{len(files)} 修正688量({fixed_vol['n']})..."
                            UPDATE_STATE['percent'] = 10 + int(done['n'] * 90 / max(len(files), 1))
                            continue
                        if last_close > 0 and prev > 0:
                            # 除权检测 v2.30.2 (2026-09-03): 昨收≠本地收即拦 — 旧 >11%/21% 阈值
                            # 只拦送转大跳空, 纯派息 0.5-8% 漏网 → 历史永不重算 (9/3 单日 13 只全漏)。
                            # 除息参考价 = 昨收 − 每股派息 → 负差 = 每股派息额 → 本地整段平移
                            # (离线零腾讯请求); 送转 = 比例缩放, 平移法随历史深度放大误差 → 腾讯。
                            # 8% 分界: 现金股息率天花板 ~7% (银行/煤炭), 最小送转 10送1 = 9.09% 起。
                            dy = prev - last_close
                            pct = dy / last_close * 100
                            if abs(dy) <= 0.005:
                                pass                     # 无除息 (同日同源只可能舍入差)
                            elif dy < 0:
                                if pct >= -8.0 and last_date == cal_prev_day \
                                        and last_date < snap_date:
                                    need_shift = dy      # 纯派息, 差带符号直接加 (勿再叠负号)
                                else:
                                    need_rewrite = f'除息/除权(diff{pct:.1f}%)'
                            elif pct > _limit_cap(c):
                                need_rewrite = f'除权(diff{pct:.1f}%)'
                        elif xd_flag:
                            # 昨收缺失 (prev=0) 但 XD/XR/DR 名称 = 除息标记 → 腾讯兜底
                            need_rewrite = f'除息/除权(名称{d.get("name", "")[:2]})'
                        if not need_rewrite and not need_shift and xd_flag:
                            # 名称明证除息日但差≈0 (diff 判不出, 如已调整过的重跑) → 腾讯兜底保底
                            need_rewrite = f'除息/除权(名称{d.get("name", "")[:2]})'
                        if not need_rewrite:
                            # v2.34e: 缺口检测统一腾讯日历 — 上证日K即活日历 (用户不再通达信同步,
                            # .day 恒滞后会误判放行; CSV末行 < 昨交易日 = 中间有洞 → 腾讯重写补全)
                            if cal_prev_day and last_date < cal_prev_day:
                                need_rewrite = f'缺口(末行{last_date})'
                            elif need_shift:
                                # v2.30.2: 纯派息除息 → 本地整段平移 (差=每股派息, 只改除息日之前)
                                # 0轴保护: 历史最低 + 平移量 ≤ 0 = 平移非法 → 改腾讯兜底
                                with open(os.path.join(daily_dir, fn), encoding='utf-8') as fh:
                                    lines = fh.read().splitlines()
                                ok_axis = True
                                for i in range(1, len(lines)):
                                    q = lines[i].split(',')
                                    if q[0] < snap_date and float(q[4]) + need_shift <= 0:
                                        ok_axis = False
                                        break
                                if ok_axis:
                                    for i in range(1, len(lines)):
                                        q = lines[i].split(',')
                                        if q[0] < snap_date:
                                            q[1] = f'{float(q[1]) + need_shift:.3f}'
                                            q[2] = f'{float(q[2]) + need_shift:.3f}'
                                            q[3] = f'{float(q[3]) + need_shift:.3f}'
                                            q[4] = f'{float(q[4]) + need_shift:.3f}'
                                            lines[i] = ','.join(q)
                                    tmpf = os.path.join(daily_dir, fn) + '.tmp'
                                    with open(tmpf, 'w', encoding='utf-8', newline='') as fh:
                                        fh.write('\n'.join(lines) + '\n')
                                    os.replace(tmpf, os.path.join(daily_dir, fn))
                                    shifted['n'] += 1
                                else:
                                    need_rewrite = f'除息(历史最低打穿0轴)'
                        if need_rewrite:  # 除权/缺口票自动腾讯前复权尾部替换兜底
                            with open(os.path.join(daily_dir, fn), encoding='utf-8') as fh:
                                lines = fh.read().splitlines()
                            err = _tencent_rewrite_tail(fn, lines)
                            if err:
                                fails.append(f"{c}: {need_rewrite}腾讯兜底失败 {err}")
                            else:
                                fixed['n'] += 1
                                # v2.132: 重写尾部只有 6 列 → 当天市值补回末行
                                if _patch_caps_after_rewrite(c, snap_date, d):
                                    caps_fixed['n'] += 1
                            done['n'] += 1
                            UPDATE_STATE['progress'] = f"{done['n']}/{len(files)} 追加中(兜底{fixed['n']})..."
                            UPDATE_STATE['percent'] = 10 + int(done['n'] * 90 / max(len(files), 1))
                            continue
                        if last_date >= snap_date:
                            done['n'] += 1  # 已最新(复查无异常, 不再重复追加)
                            continue
                        # v2.30.1: qt 腾讯 688/689 量=股 → ÷100 统一为手 (与历史/快照总手一致)
                        # v2.187 (2026-10-23): 除腾讯外，同花顺/新浪快照的 688 量本就是手，不能再除 100
                        v = (d['vol'] / 100 if is688 else d['vol']) if snap_src == '腾讯' else d['vol']
                        row = (f"{snap_date},{d['open']:.3f},{d['price']:.3f},"
                               f"{d['high']:.3f},{d['low']:.3f},{v:.0f}")
                        filepath = os.path.join(daily_dir, fn)
                        with open(filepath, encoding='utf-8') as fh:
                            lines = fh.read().splitlines()
                        if lines and not lines[-1].strip():
                            lines.pop()
                        # v2.132 (2026-10-08): 表头带市值两列 → 追加行一起写 (用户定案「更新器要写」;
                        #   漏写会让悬停/排序/手机端只能读到上一交易日的旧市值)
                        _hdr0 = lines[0].split(',') if lines else []
                        _fld = row.split(',')
                        if mcap.CAP_T in _hdr0 and mcap.CAP_F in _hdr0:
                            _fld += [mcap.fmt(d.get('mcap')), mcap.fmt(d.get('fmcap'))]
                            caps_n['n'] += 1
                        # v2.273 (用户 m08653): 表头已有第 9 列就一并写 (自由流通股本, 万股)
                        #   先按表头长度补空位到 n-1, 再落值 —— 宁可留空也绝不错列
                        #   (表头只有 9 列但缺市值两列的畸形文件以前会把 free 写到第 7 列去)
                        if mcap.FREE in _hdr0:
                            while len(_fld) < len(_hdr0) - 1:
                                _fld.append('')
                            _fld.append(mcap.fmt(d.get('free')))
                        row = ','.join(_fld)
                        lines.append(row)
                        tmp = filepath + '.tmp'
                        with open(tmp, 'w', encoding='utf-8', newline='') as fh:
                            fh.write('\n'.join(lines) + '\n')
                        os.replace(tmp, filepath)
                        done['n'] += 1
                        UPDATE_STATE['progress'] = f"{done['n']}/{len(files)} 追加中..."
                        UPDATE_STATE['percent'] = 10 + int(done['n'] * 90 / max(len(files), 1))
                    # v2.30.1: 新股自动建 CSV (快照有行情但本地无文件; 未上市全"--"票腾讯无数据静默跳过)
                    new_ok = 0
                    if not limit and new_codes:
                        new_snap = {}
                        for i in range(0, len(new_codes), 50):
                            for k in range(2):
                                try:
                                    new_snap.update(_fetch_batch(new_codes[i:i + 50]))
                                    break
                                except Exception:
                                    time.sleep(0.5)
                        for c in new_codes:
                            d = new_snap.get(_qt_sym(c))
                            if not d or (d['vol'] <= 0 and d['open'] <= 0):
                                continue
                            err = _create_new_csv(c, d)
                            # v2.249a: '已存在' 也归入「正常跳过」—— newstock.create_one
                            #   发现文件已在时返回它（老实现返回的是「已存在(跳过)」，
                            #   同样不被这里当异常；统一后别把它当失败计进 fails）。
                            if err and '无数据' not in err and '解析为空' not in err and '已存在' not in err:
                                fails.append(f"{c}: 建CSV失败 {err}")
                            elif not err:
                                new_ok += 1
                            done['n'] += 1
                            UPDATE_STATE['progress'] = f"{done['n']}/{len(files)} 新股建CSV({new_ok})..."
                            UPDATE_STATE['percent'] = min(99, 10 + int(done['n'] * 90 / max(len(files), 1)))
                    if fails:
                        UPDATE_STATE['progress'] = (f"完成 {done['n']}/{len(files)} ({snap_date}) ✅ 数据源={snap_src or '?'}, "
                                                    f"行业/概念未动; 市值两列 {caps_n['n']} 只; 腾讯兜底修复 {fixed['n']} 只, 除息平移 {shifted['n']} 只, "
                                                    f"688量修正 {fixed_vol['n']} 只"
                                                    + (f", 新建 {new_ok} 只" if new_ok else '') + f", 异常 {len(fails)} 只")
                        UPDATE_STATE['error'] = '; '.join(fails[:5]) + ('...' if len(fails) > 5 else '')
                    else:
                        UPDATE_STATE['progress'] = (f"完成 {done['n']}/{len(files)} ({snap_date}) ✅ 数据源={snap_src or '?'}, "
                                                    f"行业/概念未动, 市值两列 {caps_n['n']} 只"
                                                    + (f", 腾讯兜底修复 {fixed['n']} 只" if fixed['n'] else '')
                                                    + (f", 除息平移 {shifted['n']} 只" if shifted['n'] else '')
                                                    + (f", 688量修正 {fixed_vol['n']} 只" if fixed_vol['n'] else '')
                                                    + (f", 新建 {new_ok} 只" if new_ok else ''))
                        UPDATE_STATE['error'] = ''
                    UPDATE_STATE['percent'] = 100
                except Exception as e:
                    UPDATE_STATE['progress'] = f'快照更新失败: {str(e)[:120]}'
                    UPDATE_STATE['error'] = str(e)[:300]
                    UPDATE_STATE['percent'] = 100
                finally:
                    # v2.132: 市值两列变了 → /api/mcap 的 5 分钟缓存必须作废, 否则悬停/排序还是旧值
                    try:
                        mcap.invalidate()
                    except Exception as _e:
                        _swlog('mcap_invalidate2', _e)
                    UPDATE_STATE['running'] = False
                    UPDATE_LOCK.release()

            threading.Thread(target=_run, daemon=True).start()

            return {'ok': True, 'msg': '批量快照更新已启动'}
        except Exception as e:
            UPDATE_LOCK.release()
            return {'ok': False, 'msg': str(e)}

    def _run_holder_update(self):
        """股东户数增量更新 (v2.25): 子进程跑 fetch_holder_num.py --incremental
        保留旧行, 只追加新 report_date; 进度实时解析日志; 完成刷新内存缓存"""
        import subprocess, tempfile
        global HOLDER_UPDATE_STATE
        with HOLDER_UPDATE_LOCK:
            if HOLDER_UPDATE_STATE['running']:
                return {'ok': False, 'msg': '股东户数更新中...'}
            script = os.path.join(BASE_DIR, 'kline_data', 'fetch_holder_num.py')
            if not os.path.exists(script):
                return {'ok': False, 'msg': 'fetch_holder_num.py 不存在'}
            log_fd, log_path = tempfile.mkstemp(suffix='.log', prefix='holder_update_')
            log_file = open(log_path, 'w')
            HOLDER_UPDATE_STATE = {'running': True, 'msg': '启动中...', 'log_file': log_path}

            def _run_and_track():
                global HOLDER_UPDATE_STATE
                try:
                    proc = subprocess.Popen([sys.executable, '-u', script, '--incremental'],
                                            cwd=str(BASE_DIR),
                                            stdout=log_file, stderr=subprocess.STDOUT,
                                            bufsize=0,
                                            # v2.342 (用户 m26567): 同 run_snapshot 那处，
                                            #   pythonw 服务起子进程会弹出黑窗。
                                            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
                    proc.wait()
                except Exception as _e:
                    _swlog('holder_update_subproc', _e)
                log_file.close()
                try:
                    with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
                        lines = [l.strip() for l in f if l.strip()]
                    HOLDER_UPDATE_STATE['msg'] = lines[-1] if lines else '完成'
                except Exception:
                    HOLDER_UPDATE_STATE['msg'] = '完成'
                HOLDER_UPDATE_STATE['running'] = False
                try: os.unlink(log_path)
                except OSError as _e: _swlog('unlink_holder_log', _e)
                load_holder_data(force=True)  # 更新完成刷新内存缓存

            t = threading.Thread(target=_run_and_track, daemon=True)
            t.start()
            return {'ok': True, 'msg': '股东户数增量更新已启动 (全市场约10分钟)'}

    def _serve_index_kline(self, code):
        """指数K线 (本地日K 2026-08-07建/2026-09-05 v2.57本地化): 七大指数全量日K + MA, 无画线
        v2.57: 读 kline_data/index_daily (缺失自动回补), 盘中当日live合并 — 本地化前每次现拉400根
        (科创综指腾讯仅2025起395根 → 新浪源2020-07-03起1500根全量)"""
        try:
            sc = code.lower()
            from quant_terminal.engine import get_index_bars
            bars = get_index_bars(sc, need=None, with_live=True)
            if not bars:
                self._json({'error': f'无指数K线: {sc}'}); return
            dates = [b['date'] for b in bars]
            ohlc = [[b['open'], b['close'], b['low'], b['high']] for b in bars]  # O,C,L,H
            volume = [b['volume'] for b in bars]
            closes = [x[1] for x in ohlc]
            ma_all = {}
            for p in [5, 10, 21, 55, 120, 250]:
                ma = Indicators.sma(closes, p)
                ma_all[f'ma{p}'] = [round(v, 2) if v is not None else None for v in ma]
            self._json({
                'code': sc, 'count': len(dates), 'live': False,
                'dates': dates, 'ohlc': ohlc, 'volume': volume, 'mas': ma_all,
                'atr20': [], 'draw_lines': {},
                'first_date': dates[0], 'last_date': dates[-1],
                'last_close': closes[-1],
                'last_change': round((closes[-1] - closes[-2]) / closes[-2] * 100, 2) if len(closes) > 1 else 0,
                'is_index': True,
            })
        except Exception as e:
            # v2.34c 修复: 原 'code':num 误写未定义变量 → NameError 被裸except吞 → 指数K线永远"获取失败"
            # 现在异常打日志, 真实原因可查
            print(f'[index_kline] {code} 失败: {e!r}', file=sys.stderr)
            self._json({'error': '指数K线获取失败'})

    def _json(self, data):
        try:
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.end_headers()
            self.wfile.write(json.dumps(data, ensure_ascii=False, default=str).encode('utf-8'))
        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
            pass  # 客户端断开, 静默忽略

    def _serve_html(self, fname):
        fpath = os.path.join(os.path.dirname(__file__), fname)
        # 按扩展名给 Content-Type (echarts.min.js 本地化后需要 js 类型, 2026-08-04)
        ctype = {'html': 'text/html; charset=utf-8', 'js': 'application/javascript; charset=utf-8',
                 'css': 'text/css; charset=utf-8',
                 'woff2': 'font/woff2',
                 # v2.265 (2026-11-09): favicon 要正确的 image/x-icon，否则个别 Chrome 版本
                 #   对 octet-stream 的 .ico 会拒绝使用（任务栏仍回落灰球）。
                 'ico': 'image/x-icon',
                 'png': 'image/png'}.get(fname.rsplit('.', 1)[-1].lower(), 'application/octet-stream')
        self.send_header('Content-Type', ctype)
        # v2.14d 禁缓存: 此前无缓存头→浏览器启发式缓存, 改了页面刷新却拿到旧版(浮窗定位修复不生效的直接原因)
        # v2.150 (2026-10-18): 只有 no-cache 时浏览器仍可能拿缓存副本回显 —— 用户长期被迫 Ctrl+F5。
        #   补齐 no-store + Pragma + Expires 三件套: 只要普通 F5 (甚至回退前进) 就必须整份重取。
        self.send_header('Cache-Control', 'no-cache, no-store, must-revalidate')
        self.send_header('Pragma', 'no-cache')
        self.send_header('Expires', '0')
        self.end_headers()
        if os.path.exists(fpath):
            with open(fpath, 'rb') as f:
                self.wfile.write(f.read())
        else:
            self.wfile.write(f'<h1>{fname} not found</h1>'.encode())

    def _serve_kline_tail(self, code, n):
        """v2.244: 只要最后 n 根日K（悬停浮窗专用，轻量）。

        与 _serve_kline 的区别：不拼实时价、不算 MA/ATR/左峰/画线，只回 dates + bars。
        v2.247 (用户 m21390): 指数(sh000001 等)也支持了 —— 顶部 7 个指数格的悬停浮窗要
          「上日K + 下分时」, 走的就是这里。指数读本地 kline_data/index_daily
          (engine.get_index_bars, with_live=True 盘中叠当日 live bar), 再切尾 N 根。
          响应形状与个股完全一致 ⇒ 前端 _drawDailyChart 不用分两种数据。
        """
        c0 = code.lower() if code[:2].lower() in ('sh', 'sz') else code
        if c0 in INDEX_CODES or (code.lower() in INDEX_CODES):
            try:
                from quant_terminal.engine import get_index_bars
                ib = get_index_bars(c0, need=max(2, n), with_live=True)
            except Exception as e:
                print(f'[kline_tail] index {code} 失败: {e!r}', file=sys.stderr)
                ib = []
            if not ib:
                self._json({'ok': False, 'error': '无指数K线: ' + c0})
                return
            it = ib[-max(2, n):]
            self._json({
                'ok': True,
                'code': c0,
                'is_index': True,
                'count': len(it),
                'dates': [d['date'] for d in it],
                'bars': [[d['open'], d['close'], d['low'], d['high']] for d in it],
                'volume': [d['volume'] for d in it],
                'last_date': it[-1]['date'],
                'last_close': it[-1]['close'],
                'chg': (round((it[-1]['close'] - it[-2]['close']) / it[-2]['close'] * 100, 2)
                        if len(it) > 1 and it[-2]['close'] else 0),
            })
            return
        num = code[2:] if code.startswith(('SZ', 'SH', 'sz', 'sh')) else code
        # v2.279 (用户 m10425): 北交所 920xxx 首位是 9, 老判据会拼成 SH920xxx ⇒ 永远找不到。
        #   mcap.prefixed() 已经把 920xxx / 4xxxxx / 8xxxxx 归到 BZ, 全仓统一用它。
        prefix = mcap.prefixed(num)[:2]
        rows = load_csv_kline(prefix + num)
        if not rows:
            self._json({'ok': False, 'error': '无K线数据: ' + num})
            return
        tail = rows[-max(2, n):]
        self._json({
            'ok': True,
            'code': num,
            'count': len(tail),
            'dates': [d['date'] for d in tail],
            'bars': [[d['open'], d['close'], d['low'], d['high']] for d in tail],
            'volume': [d['volume'] for d in tail],
            'last_date': tail[-1]['date'],
            'last_close': tail[-1]['close'],
            'chg': (round((tail[-1]['close'] - tail[-2]['close']) / tail[-2]['close'] * 100, 2)
                    if len(tail) > 1 and tail[-2]['close'] else 0),
        })

    def _serve_kline(self, code, limit='', mode='', asrc='', bsrc=''):
        """K线数据 + MAs + 关键点标注

        v2.244: limit 非空 ⇒ 只回最后 N 根（悬停浮窗用），跳过 MA/ATR/画线那套重计算。
        # >>> WAVE BEGIN (v2.259) 摘除脚本要求 signature 之外的痕迹全包在标记块里
        v2.259: mode='wave' ⇒ draw_lines 只回趋势线（左峰/止损全不给），配合前端
                buildDrawLines() 的 wave 分支 —— 用户 m01825 要求「列表在波段低吸模式时
                K线图屏蔽左峰和止损, 改画趋势线」。
        # <<< WAVE END
        """
        try:
            _lim = int(limit or 0)
        except Exception:
            _lim = 0
        if _lim > 0:
            return self._serve_kline_tail(code, _lim)
        # 指数代码 (2026-08-07): sh000001等 → 腾讯指数K线 (无本地CSV/画线)
        c0 = code.lower() if code[:2].lower() in ('sh', 'sz') else code
        if c0 in INDEX_CODES or (code.lower() in INDEX_CODES):
            return self._serve_index_kline(code)
        # 标准化代码
        # v2.279 (2026-11-12 用户 m10425): 北交所 920xxx 首位 9 → 老判据拼成 SH920xxx,
        #   本地 CSV 叫 BZ920157 ⇒ 「无K线数据」。改用 mcap.prefixed() 全仓唯一口径。
        #   同时新浪/腾讯的北交所前缀是 **bj** (实测 bj920157 有数据, sh/sz 全空)。
        num = mcap.bare(code)
        prefix = mcap.prefixed(num)[:2]
        full_code = prefix + num
        # Sina API 需要小写前缀 (北交所用 bj)
        sina_code = ('bj' if prefix == 'BZ' else prefix.lower()) + num

        data = load_csv_kline(full_code)
        if not data:
            self._json({'error': f'无K线数据: {full_code}'})
            return

        # 拼接实时价
        today_str = datetime.now().strftime('%Y-%m-%d')
        stock_name = ''
        try:
            q = fetch_sina([sina_code])
            if q and sina_code in q and q[sina_code].get('price', 0) > 0:
                qd = q[sina_code]
                stock_name = qd.get('name', '')
                # v2.65a (2026-09-09 龙版传媒事故): 停牌股新浪 price=昨收>0(骗过price守卫)但 open=0
                # → 原逻辑误拼 当日 O=0 C=H=L=昨收 V=0 畸形bar, 前端画出"今日开盘价0";
                # open<=0 = 当日无行情(停牌/未交易), 跳过拼接 (名字照常取)
                if qd.get('open', 0) > 0:
                    live_bar = {
                        'date': today_str,
                        'open': qd.get('open', data[-1]['close']),
                        'close': qd['price'],
                        'high': max(qd.get('high', 0), qd['price']),
                        'low': min(qd.get('low', qd['price']), qd['price']) if qd.get('low', 0) > 0 else qd['price'],
                        'volume': qd.get('volume', 0) / 100,  # 新浪返回股, CSV存手, 除100对齐
                    }
                    # 避免非交易时段新增重复K线: 最后一根已是今日则替换, 否则仅当数据不同时才追加
                    last = data[-1]
                    if last['date'] == today_str:
                        data[-1] = live_bar
                    elif not (abs(live_bar['open'] - last['open']) < 0.001 and
                              abs(live_bar['close'] - last['close']) < 0.001 and
                              abs(live_bar['high'] - last['high']) < 0.001 and
                              abs(live_bar['low'] - last['low']) < 0.001):
                        # 数据确实不同 (盘中更新 或 新交易日开盘) → 追加
                        data.append(live_bar)
                    # 否则: 非交易时段/未开盘, 新浪返回与昨K线完全相同的数据 → 跳过
        except Exception as _e:
            _swlog('live_bar_merge', _e)

        closes = [d['close'] for d in data]
        # 计算所有MA
        ma_all = {}
        for p in [5, 10, 21, 55, 120, 250]:
            ma = Indicators.sma(closes, p)
            ma_all[f'ma{p}'] = [round(v, 2) if v is not None else None for v in ma]

        # 找左峰
        from quant_terminal.engine import LeftPeakEngine
        de = LeftPeakEngine(data, 20, '20日')
        bo = de.check_breakout()
        peak_price = bo['left_peak']
        key_point = {'price': peak_price, 'date': '20D',
                     'stop_loss': round(bo['recent_low'], 2),
                     'dist_pct': round((data[-1]['close'] - peak_price) / peak_price * 100, 2) if peak_price > 0 else 0,
                     'idx': len(data) - 1}

        # ATR
        atr = Indicators.atr(data, 20)

        # v2.4 K线画线数据 (2026-08-06): 峰①=绿(周期/稍远峰) 峰②=蓝(孤立/最近峰) 止①=黄(峰下5%) 止②=红(海龟)
        from quant_terminal.engine import LeftPeakEngine, find_left_peak_list, find_recent_two_peaks
        highs_all = [d['high'] for d in data]
        lp_peaks, iso_peaks = {}, {}
        for p in [100, 55, 20]:
            de = LeftPeakEngine(data, p, f'{p}日')
            lp_peaks[str(p)] = de.left_peak['price'] if de.left_peak else None
            # 孤立峰按模式窗口找 (55日模式看55日窗口的孤立峰, 而非固定100日)
            iso = find_left_peak_list(highs_all, lookback=p)
            iso_peaks[str(p)] = round(iso['price'], 3) if iso else None
        # 双峰画线 (2026-08-06 用户定案修正): 峰①=当前级别窗口内最高孤立峰(标尺, 按100/55/20切换)
        #   峰②=最近孤立峰(操作锚点); AI/Smart模式: 峰①=次近孤立峰 峰②=最近孤立峰
        # 修正: 原峰①=周期窗口最高(不孤立) — 承德露露案例: 取5天前反弹高点毫无孤立性, 不孤立的峰无意义
        dp = find_recent_two_peaks(highs_all)
        dual_peaks = {
            'peak2': round(dp['peak2']['price'], 3) if dp['peak2'] else None,
            'peak2_prev': round(dp['peak1']['price'], 3) if dp['peak1'] else None,
        }
        for p in [100, 55, 20]:
            pl = find_left_peak_list(highs_all, lookback=p)  # 该级别窗口内最高孤立峰
            dual_peaks[str(p)] = round(pl['price'], 3) if pl else None
        atr_last = atr[-1] if atr[-1] else data[-1]['close'] * 0.03
        recent_low = min(d['low'] for d in data[-20:])
        turtle_stop = max(data[-1]['close'] - 2 * atr_last, recent_low)
        draw_lines = {
            'left_peaks': lp_peaks,
            'isolated_peaks': iso_peaks,
            'dual_peaks': dual_peaks,
            'turtle_stop': round(turtle_stop, 3),
        }
        # >>> WAVE BEGIN（后端）波段低吸模式：只画趋势线，不画左峰/止损（用户 m01825）
        # 口径与选股完全同一套（wave_scan.find_A/cand_bs/pick_line），不另抄一份规格 ——
        #   抄第二份 = 图上画的线与列表里那一行迟早分家。
        # v2.318：趋势线**常驻** —— draw_lines 里永远带 wave_all（六口径各一条）。前端切选股模式/切口径
        #   时只重画 markLine，不再重新 GET /api/kline（那一次 175ms + 重播 1 秒入场动画，纯浪费）。
        #   成本实测：六口径合计 3.88ms/只（中位 0.00ms、最慢 1.5ms）。
        #   ⚠ 多数票本来就没有合格趋势线 ⇒ wave_all 可能是空 dict，前端必须按「可能为空」处理。
        wave_line = None
        wave_all = {}
        try:
            import wave_scan as _WVS
            _t = len(data) - 1
            if _t >= _WVS.MIN_BARS - 1:
                _ie = _t
                for _ca, _cb in _WVS.COMBOS:
                    try:
                        _iA = _WVS.find_A(data, _t, _ca)
                        if _iA is None:
                            continue
                        _bs = _WVS.cand_bs(data, _t, _iA, _ca, _cb)
                        if not _bs:
                            continue
                        _best = _WVS.pick_line(data, _t, _iA, _bs, _ca, _cb)
                        if not _best:
                            continue
                        # ⚠ 单位坑: wave_scan 的 slope 是**百分比%/日**, 不是元/日。
                        #   元/日 = spct * A点价位 / 100。直接当元/日用会外推到天上
                        #   (实测神火 000933: 0.0532 当元/日 → 线值 27.90, 真值 25.20)。
                        _pa = _best['alow']
                        _sl_y = _best['slope'] * _pa / 100.0
                        _lv_end = _pa + _sl_y * (_ie - _best['iA'])
                        wave_all[_WVS.ck(_ca, _cb)] = {
                            'adate': _best['adate'], 'alow': round(_pa, 3),
                            'bdate': _best['bdate'], 'blow': round(_best['blow'], 3),
                            'slope': round(_best['slope'], 4), 'span': _best['span'],
                            'gap': round(_best['gap'], 3),
                            # 线值 = 线在最后一根 K 线那天的价位（前端斜线右端点 + 淡虚线都用它）
                            'ldate': data[_ie]['date'],
                            'lv_end': round(_lv_end, 3),
                            'lv_last': round(_best['lvT'], 3),
                            'a_src': _best.get('a_src', _ca),
                            'b_src': _best.get('b_src', _cb),
                        }
                    except Exception:
                        continue
        except Exception as _we:
            _swlog('wave_all', _we)
        draw_lines['wave_all'] = wave_all
        if mode == 'wave':
            try:
                # v2.271 (2026-11-11 用户 m07846): 口径跟随 —— 前端切了按钮就传
                #   asrc=low|close  bsrc=low|close|open, 这里必须用同一套取价,
                #   否则「列表按 A低/B开 选出来, 图却按 A收/B收 画」两张皮。
                _asrc = asrc or _WVS.DEFAULT_COMBO.split('|')[0]
                _bsrc = bsrc or _WVS.DEFAULT_COMBO.split('|')[1]
                if _asrc not in _WVS.A_SRCS:
                    _asrc = 'low'
                if _bsrc not in _WVS.B_SRCS:
                    _bsrc = 'close'
                # 直接取上面算好的那份，不再重算
                wave_line = wave_all.get(_asrc + '|' + _bsrc)
            except Exception as _we:
                _swlog('wave_line', _we)
                wave_line = None
            # 屏蔽左峰 / 止损: 用户要的就是干净的图
            draw_lines = {'wave_line': wave_line, 'mode': 'wave', 'wave_all': wave_all}
        # <<< WAVE END

        result = {
            'code': num,
            'name': stock_name,
            'count': len(data),
            'live': data[-1]['date'] == today_str,
            'dates': [d['date'] for d in data],
            'ohlc': [[d['open'], d['close'], d['low'], d['high']] for d in data],
            'volume': [d['volume'] for d in data],
            'mas': ma_all,
            'atr20': [round(v, 2) if v else None for v in atr],
            'draw_lines': draw_lines,
            'first_date': data[0]['date'],
            'last_date': data[-1]['date'],
            'last_close': data[-1]['close'],
            'last_change': round((data[-1]['close'] - data[-2]['close']) / data[-2]['close'] * 100, 2) if len(data) > 1 else 0,
        }

        # 标注关键点
        if key_point:
            result['key_point'] = {
                'price': key_point['price'],
                'date': key_point['date'],
                'stop_loss': key_point['stop_loss'],
                'dist_pct': key_point['dist_pct'],
                'idx': key_point['idx'],
            }

        self._json(result)

    def _serve_stock_detail(self, code):
        """单只股票完整四模块分析"""
        # v2.34c 指数防御: 指数代码当个股查会错名 (sh000001→sz000001平安银行等), 前端已走概况卡片, 这里兜底
        if code.lower() in INDEX_CODES:
            self._json({'error': '指数无个股分析', 'is_index': True})
            return
        num = code[2:] if code.startswith(('SZ', 'SH', 'sz', 'sh')) else code
        prefix = 'SH' if (num.startswith('6') or num.startswith('9')) else 'SZ'
        full_code = prefix + num
        # Sina API 需要小写前缀
        sina_code = prefix.lower() + num

        # 取实时价
        rt = fetch_sina([sina_code]).get(sina_code, {})
        price = rt.get('price', 0)
        change_pct = rt.get('change_pct', 0)

        if price <= 0:
            self._json({'error': f'无实时行情: {full_code}'})
            return

        engine = QuantEngine()
        engine.load_data()
        info = engine.pool.get(full_code, {'code': full_code, 'name': rt.get('name', num),
                                            'sector': '', 'concept': ''})
        # v2.13 手动输入代码: 非池内股票补行业/概念 (本地map, 供详情面板显示)
        if not info.get('sector'):
            try:
                ind_map, con_map = load_industry_concept_maps()
                info['sector'] = ind_map.get(num, '')
                info['concept'] = con_map.get(num, '')
            except Exception as _e:
                _swlog('detail_industry_map', _e)

        # 修复bug: 原传 engine.get_index_kline() (不存在) 当第5参被当成mode必崩
        r = engine.scan_single(full_code, info, price, change_pct)

        # v2.13 手动输入代码: 返回平铺结构(同扫描行), 前端直接复用 renderStockDetail
        self._json(self._summarize_result(r, mode='20'))

    # ============================================================
    # v2.40 (2026-09-03): 行业/概念列状态染色 — trend_data.json 查表 (mtime缓存)
    # ============================================================
    _TD_CACHE = {'mtime': -1.0, 'key_map': {}}

    @staticmethod
    def _trend_status_maps(lines='4'):
        """当前档位 板块状态查表: (行业name→{status,pct}, 概念name→{status,pct})
        按 trend_data.json mtime 失效重读, 每档独立缓存 (行多, 每行2次查表不能反复读文件)"""
        mp = LINES_MAP.get(str(lines), (5, 10, 21, 55))
        key = ''.join(str(p) for p in mp)
        try:
            mt = os.path.getmtime(str(TREND_DATA_FILE))
        except Exception:
            return {}, {}
        if mt != QuantHandler._TD_CACHE['mtime']:
            QuantHandler._TD_CACHE['mtime'] = mt
            QuantHandler._TD_CACHE['key_map'] = {}
        if key not in QuantHandler._TD_CACHE['key_map']:
            sm, cm = {}, {}
            try:
                data = _load_trend_data() or {}
                for x in data.get('lines', {}).get(key, {}).get('sector', []):
                    sm[x['name']] = {'status': x.get('status'), 'pct': x.get('bullish_pct')}
                for x in data.get('lines', {}).get(key, {}).get('concept', []):
                    # v2.222 (2026-10-25): 概念浮窗要「一行一个 + 涨跌幅 + 多头/总数 + 占比 + 趋势」，
                    #   数据本来就在 trend_data.json 的概念行里（bc/total/占比/status），指数涨跌幅在
                    #   ma_scan.json —— 一并挂上，前端直接取，不再二次计算。
                    _ip = _idx_pct(x['name'])
                    cm[x['name']] = {'status': x.get('status'), 'pct': x.get('bullish_pct'),
                                     'bc': x.get('bullish_count'), 'tot': x.get('total'),
                                     'ip': _ip.get('pct')}
            except Exception as _e:
                _swlog('trend_status_maps', _e)
            QuantHandler._TD_CACHE['key_map'][key] = (sm, cm)
        return QuantHandler._TD_CACHE['key_map'][key]

    @staticmethod
    def _attach_trend_status(r: dict):
        """行就地附板块趋势状态 (v2.40) — sector_status=状态名或None;
        concept_status={概念名:{status,pct}} 仅含收录概念 (未收录段前端照显灰色)"""
        sm, cm = QuantHandler._trend_status_maps(SCAN_STATE.get('lines', '4'))
        sec = (r.get('sector') or '').strip()
        hit = sm.get(sec)
        r['sector_status'] = hit.get('status') if hit else None
        # v2.42 (2026-09-03): 行业多头占比 — 行业列色=行业面板同口径 (pct>=70红/>=40金/<40灰)
        r['sector_pct'] = hit.get('pct') if hit else None
        cd = {}
        for nm in (r.get('concept') or '').split('|'):
            nm = nm.strip()
            if not nm:
                continue
            hit = cm.get(nm)
            if hit:
                cd[nm] = hit
        r['concept_status'] = cd

    def _summarize_result(self, r: dict, mode: str = None) -> dict:
        """精简扫描结果用于前端展示 (mode可选覆盖: 单票分析强制'20'左峰分支)"""
        QuantHandler._attach_trend_status(r)  # v2.40: 行业/概念状态染色附行
        if r.get('group') in ('dragon', 'pullback'):
            # v2.16 龙头战法: 直传原始行 (无consensus结构, 前端dragon分支渲染)
            out = dict(r)
            out.setdefault('cons_score', r.get('score', 0))
            out.setdefault('cons_rec', '')
            out.update(_m4_flat(r))   # v2.216: 龙头战法两链的 A股适配
            return out
        # v2.308（用户 m17423）：三个新模式也要能按「行业/概念 强多·偏多」筛列表。它们的行本来没有
        #   ind_t/con_t（那是 engine._filter_multihead 给趋势行用的），但 _attach_trend_status 上面刚给
        #   每行附了 sector_status + concept_status（257 行业 / 351 概念全量表）⇒ 这里就地推成 ind_t/con_t。
        #   ⚠ **总是带这两个键**（不命中就是空串）+ mh_ready=1。理由：前端 applyMhFilter 的 v2.152c 降级是
        #     「这一维全没有档位 ⇒ 该维不参与」，对这些模式就成了「勾了偏多但全批都不偏多 ⇒ 原样放行」，
        #     用户看到的是"勾了没反应"。而它们每只票都有 sector_status/concept_status，不存在"没数据"，
        #     空串的含义就是明确的"不达标" ⇒ 带 mh_ready 让前端走严格分支。
        def _mh_of(_r):
            _s = _r.get('sector_status') or ''
            _o = {'ind_t': (_s if _s in ('偏多', '强多头') else '')}
            _best = ''
            for _nm, _v in (_r.get('concept_status') or {}).items():
                _s2 = _v.get('status') if isinstance(_v, dict) else _v
                if _s2 == '强多头':
                    _best = '强多头'
                    break
                if _s2 == '偏多':
                    _best = _best or '偏多'
            _o['con_t'] = _best
            _o['mh_ready'] = 1
            return _o
        # >>> AUCTION BEGIN
        if r.get('mode') == 'auction':
            # 竞价打板 (2026-11-06): 本模式的行**没有 consensus 结构** (auction_scan 产出),
            # 必须在下面的 c = r['consensus'] 之前拦截原样返回, 否则 KeyError。
            out = dict(r)
            out.setdefault('cons_score', 0)
            out.setdefault('cons_rec', '')
            out.update(_m4_flat(r))   # v2.261: 本模式的 A股适配 (由 _m4_bulk 在扫描时算好)
            out.update(_mh_of(out))   # v2.308: 板块档位 (前端 4 个勾选框)
            return out
        # <<< AUCTION END
        # >>> REBOUND BEGIN
        if r.get('mode') == 'rebound':
            # 超跌反弹 (2026-11-07): 本模式的行**没有 consensus 结构** (rebound_scan 产出),
            # 必须在下面的 c = r['consensus'] 之前拦截原样返回, 否则 KeyError。
            out = dict(r)
            out.setdefault('cons_score', r.get('rebound', 0))
            out.setdefault('cons_rec', '超跌反弹')
            out.update(_m4_flat(r))   # v2.261: 本模式的 A股适配 (由 _m4_bulk 在扫描时算好)
            out.update(_mh_of(out))   # v2.308: 板块档位 (前端 4 个勾选框)
            return out
        # <<< REBOUND END
        # >>> WAVE BEGIN
        if r.get('mode') == 'wave':
            # 波段低吸 (2026-11-07): 本模式的行**没有 consensus 结构** (wave_scan 产出),
            # 必须在下面的 c = r['consensus'] 之前拦截原样返回, 否则 KeyError。
            out = dict(r)
            out.setdefault('cons_score', r.get('wave', 0))
            out.setdefault('cons_rec', '波段低吸')
            out.update(_m4_flat(r))   # v2.261: 本模式的 A股适配 (由 _m4_bulk 在扫描时算好)
            out.update(_mh_of(out))   # v2.308: 板块档位 (前端 4 个勾选框)
            return out
        # <<< WAVE END
        c = r['consensus']
        base = {
            'code': r['code'], 'name': r['name'],
            'price': r['price'], 'change_pct': r['change_pct'],
            'sector': r.get('sector', '')[:20], 'concept': r.get('concept', ''),
            # v2.40 (2026-09-03): 行业/概念列状态染色 — 前端按状态四色渲染
            'sector_status': r.get('sector_status'),
            'sector_pct': r.get('sector_pct'),
            'concept_status': r.get('concept_status'),
            'market_cap': r.get('market_cap', ''),
            'cons_score': c['score'], 'cons_rec': c['recommendation'],
            'cons_strength': c['signal_strength'],
            # v2.12 加分制: 基础分/加分/明细 (加分只用于排序, 前端显示 基础+加分)
            'cons_base_score': c.get('base_score', c['score']),
            'cons_bonus': c.get('bonus', 0),
            'cons_bonus_detail': c.get('bonus_detail', {}),
            'cons_bonus_notes': c.get('bonus_notes', []),
            'score_detail': c.get('score_detail', {}),
        }
        # v2.99: 档位字段必须**原样带出**。v2.96 在 engine._filter_multihead 里把命中档位写回 r['ind_t']/r['con_t']，
        #   前端 4 个勾选框(applyMhFilter)靠它决定显示哪几档 —— 但这个 base 是**白名单**，没带上这两个键，行到前端
        #   恒是 undefined，applyMhFilter 每次都走到 `if(!useI&&!useC)return list;` 原样放行。
        #   **v2.96 上线起那 4 个框一次都没生效过**，重扫也没用 —— 数据根本没发出去。
        #   ⚠ 是**有才带**，不是 .get(x,''): _filter_multihead 在「名单从未计算且手动兜底也为空」时原样 return，
        #   那些行两键皆无 —— 若补成空串，前端 hasI/hasC 探针会误判"本批有档位"，一勾就把整表筛空。
        #   缺键 = 该维不参与筛选(v2.96 定的降级语义，保留)。
        if 'ind_t' in r:
            base['ind_t'] = r['ind_t']
        if 'con_t' in r:
            base['con_t'] = r['con_t']
        mode = mode or SCAN_STATE.get('mode', '20')
        if mode == 'ai':
            # v2.66 (2026-09-11) 涨停突破: 峰期 + 被突破峰价/距峰/止损, 加涨停池原始字段
            # 五灯/大盘状态等旧字段不再传 (用户定案彻底舍弃)
            lb = r.get('limit', {}) or {}
            base.update({
                'peak_period': r.get('peak_period', 0),      # 100/55/20 — 兼 cons_score 排序键
                'peak_label': PEAK_LABELS.get(r.get('peak_period', 0), ''),
                'left_peak': r.get('peak_price', 0),         # 「左峰」列 = 被突破的那个峰价
                'dist_pct': r.get('dist_pct', 0),
                'gap_atr': r.get('gap_atr', 0),
                'atr20': r.get('atr20', 0),                  # v2.77: 前端盘中重算 距峰ATR 用
                'sync_pct': r.get('sync_pct'),
                'body_pct': r.get('body_pct'),
                'stop_loss': r.get('stop_loss', 0),          # 跌回峰下 = 突破失败
                'lianban': lb.get('lianban', 0),             # 连板数
                'seal_money': lb.get('seal_money', 0),       # 封板资金(元)
                'turnover_rate': lb.get('turnover', 0),      # 换手率%
                'zhaban': lb.get('zhaban', 0),               # 炸板次数
                'first_time': lb.get('first_time', ''),      # 首次封板时间
                # v2.187 (2026-10-23): 同花顺涨停池多给的三个字段
                'reason': lb.get('reason', ''),               # 涨停原因 (同花顺独有)
                'lianban_text': lb.get('lianban_text', ''),   # 连板文本, 能表达 5天4板
                'max_seal': lb.get('max_seal', 0),            # 峰值封单额(元)
            })
            base.update(_m4_flat(r))   # v2.216: 涨停突破的 A股适配
            return base
        if mode == 'smart':
            # v2.67 (2026-09-11) 涨停回调: 类型(不破收/中/开) + 板数 + 涨停日 收/中/开 + 缩量比
            # 原趋势跟踪的 strategy/unit_shares/五灯字段全部不再传 (用户定案彻底舍弃)
            base.update({
                'kind': r.get('kind', ''),                   # 「类型」列: 不破收/不破中/不破开
                'kind_tier': r.get('kind_tier', 0),          # 1/2/3 — 兼 cons_score 排序键
                'zt_cnt': r.get('zt_cnt', 0),                # 前20日涨停数 → 前端显示 1板/2板/3板
                'zt_date': r.get('zt_date', ''),
                'zt_close': r.get('zt_close', 0),
                'zt_mid': r.get('zt_mid', 0),
                'zt_open': r.get('zt_open', 0),
                'left_peak': r.get('left_peak', 0),          # = 涨停日收盘价(涨停价)
                'dist_pct': r.get('dist_pct', 0),
                # v2.145: 三峰 (豁免涨停+回调阶段, 孤立峰从近到远) — 板数列悬停分三行展示
                'pk_recent': r.get('pk_recent', 0),
                'pk_recent_d': r.get('pk_recent_d', 0),
                'pk_d55': r.get('pk_d55', 0),
                'pk_d55_d': r.get('pk_d55_d', 0),
                'pk_d100': r.get('pk_d100', 0),
                'pk_d100_d': r.get('pk_d100_d', 0),
                'vol_ratio': r.get('vol_ratio', 0),          # 今量占前20日最高量%
                'sync_pct': r.get('sync_pct'),
                'body_pct': r.get('body_pct'),
                'stop_loss': r.get('stop_loss', 0),          # 跌穿涨停日开盘价 = 回调形态破坏
                # v2.235 (2026-10-27 用户 m10478): N字法锚 —— 前20根K线内从近往远
                #   第一根「实体 > 5%(主板) / > 10%(双创)」的涨停日的开盘价。
                #   nz_open = 0 表示窗口内没有达标涨停 ⇒ 前端不染色(仍留在列表里)。
                #   前端只用 nz_open + 实时价重算距离%, 因此只要下发这个价就够。
                'nz_open': r.get('nz_open', 0),
                'nz_date': r.get('nz_date', ''),
                'nz_ent': r.get('nz_ent', 0),
                'nz_pct': r.get('nz_pct'),
                'nz_ok': r.get('nz_ok', 0),
            })
            base.update(_m4_flat(r))   # v2.216: 涨停回调的 A股适配
            return base
        if mode == 'glue':
            # v2.142 (2026-10-15) 均线粘合: 第1列 = 粘合度 max(MA5-MA10%, MA10-MA21%) 兼排序键,
            #   「左峰」位放 MA5, 「距峰」位放 现价-MA5%; 悬停看 三线值 / 三档乖离 / 两个间距 / 量比
            #   (MA55 已完全退出选股因子, 前端悬停与表头同步删)
            base.update({
                'glue': r.get('glue', 0),
                'ma5': r.get('ma5', 0), 'ma10': r.get('ma10', 0),
                'ma21': r.get('ma21', 0),               # v2.142: ma55 已删
                'g510': r.get('g510', 0), 'g1021': r.get('g1021', 0),
                'gap5_21': r.get('gap5_21', 0),
                'p5': r.get('p5', 0), 'p10': r.get('p10', 0),
                'p21': r.get('p21', 0),                 # v2.142: p55 已删
                'left_peak': r.get('left_peak', 0),      # = MA5 (借表头「左峰」那列)
                'dist_pct': r.get('dist_pct', 0),        # = 现价-MA5%
                'vol_ratio': r.get('vol_ratio', 0),      # 今量 ÷ 前5日均量
                # v2.142: 三条均线的 5 日斜率(%) + 「三线走平或向上」总标记 (前端勾选框用)
                's5': r.get('s5', 0), 's10': r.get('s10', 0),
                's21': r.get('s21', 0),
                'ma_up': r.get('ma_up', 1),
                'stop_loss': r.get('stop_loss', 0),      # = MA21 (跌破=三线排列破坏)
            })
            # 档位字段: 前端 4 个勾选框 (applyMhFilter) 靠 ind_t/con_t 做显示层筛选。
            # engine 不写这两键, 这里从 _attach_trend_status 刚附上的 sector_status/concept_status 推。
            # ⚠ 有才带 (缺键 = 该维不参与筛选, 与 v2.99 定的降级语义一致)
            _st = r.get('sector_status')
            if _st in ('偏多', '强多头'):
                base['ind_t'] = _st
            _best = ''
            for _nm, _v in (r.get('concept_status') or {}).items():
                _s = _v.get('status') if isinstance(_v, dict) else _v
                if _s == '强多头':
                    _best = '强多头'
                    break
                if _s == '偏多':
                    _best = _best or '偏多'
            if _best:
                base['con_t'] = _best
            base.update(_m4_flat(r))   # v2.216: 均线粘合的 A股适配
            return base
        # 左峰突破模式
        dm = r.get('breakout', {}); m4 = r.get('m4_verify', {}); tx = r.get('tx_execute', {})
        base.update({
            # v2.36 (2026-09-03): 量价同步率 (纯展示, None=样本不足)
            'sync_pct': r.get('sync_pct'),
            # v2.37 (2026-09-03): 实体量质 (纯展示, None=样本不足)
            'body_pct': r.get('body_pct'),
            'dm_signal': dm.get('signal', ''), 'dm_confidence': dm.get('confidence', 0),
            'left_peak': dm.get('channel', {}).get('left_peak', 0),
            'recent_low': dm.get('channel', {}).get('recent_low', 0),
            'position_pct': dm.get('channel', {}).get('position_pct', 0),
            'volume_ratio': dm.get('channel', {}).get('volume_ratio', 0),
            'dist_pct': round((r['price'] - dm.get('channel', {}).get('left_peak', r['price'])) / dm.get('channel', {}).get('left_peak', 1) * 100, 1),
            'gap_atr': round(abs(dm.get('channel', {}).get('dist_atr', 0)), 2),
            'stop_loss': tx.get('stop_loss', {}).get('stop_loss', 0),
            'atr': tx.get('stop_loss', {}).get('n_atr20', 0),
            'atr20': tx.get('stop_loss', {}).get('n_atr20', 0),   # v2.77: 同 'atr', 统一字段名供前端重算
            'm4_passed': m4.get('passed', 0), 'm4_total': m4.get('total', 3),
            'sector_strength': m4.get('sector_strength', 0), 'sector_ok': m4.get('sector_ok', False),
            'sector_src': m4.get('sector_src', ''),
            'bias_val': m4.get('bias_val', 0), 'bias_ok': m4.get('bias_ok', True),
            'limit_exclude': not m4.get('limit_ok', True), 'limit_label': m4.get('limit_label', ''),
            'unit_shares': tx.get('position', {}).get('unit_shares', 0),
            'max_shares': tx.get('position', {}).get('max_shares', 0),
            'unit_pct': tx.get('position', {}).get('unit_pct', 0),
            'trend_ok': tx.get('trend_filter', {}).get('filter_pass', False),
        })
        return base

    def _handle_ai_news(self, news_id, force=False):
        """v2.112: 新闻 AI 影响分析 —— 每请求一条**私有** SSE 流。

        与 _handle_sse 的本质差异: 那个是进程级**广播** (queue + SSE_CLIENTS,
        一个产出方对 N 个订阅者, 生命周期 = 进程); 这个是请求级 **1:1**,
        handler 线程自己直写 wfile —— 不用 Queue、不用额外线程, 生成完就 return。
        """
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.send_header('Cache-Control', 'no-cache')
        # v2.112a: 必须是 close 不能是 keep-alive —— send_header('Connection','keep-alive')
        #   会把 self.close_connection 置 False, handler 返回后服务端**不关连接**,
        #   而是回头阻塞读下一个请求 (timeout 默认 None = 永久阻塞) → 每条分析泄漏一个线程。
        #   (_handle_sse 那样写没事: 它 while True 永不返回。但这条流是会正常结束的。)
        #   对"无 Content-Length 的 HTTP/1.0 流"来说 close 也才是诚实的: body 本来就靠关连接表示结束。
        self.send_header('Connection', 'close')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

        def _send(ev, payload):
            # ⚠️ payload 必须 JSON 编码: 正文里带换行, 纯文本直写会撕裂 SSE 帧
            self.wfile.write(('event: %s\ndata: %s\n\n'
                              % (ev, json.dumps(payload, ensure_ascii=False))).encode('utf-8'))
            self.wfile.flush()

        try:
            # ★ 首行必须是 retry: —— 服务端 return 会让 EventSource 在 ~3s 后**自动重连**,
            #   于是每分析完一条都白烧一次 LLM 调用、界面还会跳回"分析中"。
            #   推到 10 分钟; 前端在 done/err/onerror 三处都会 es.close(), 正常路径不重连。
            self.wfile.write(b'retry: 600000\n\n')
            self.wfile.flush()

            item = None
            with NEWS_LOCK:
                for x in NEWS_CACHE['items']:
                    if x.get('id') == news_id:
                        item = x
                        break
            if item is None:
                _send('err', {'code': 'stale', 'partial': False,
                              'msg': '这条新闻已不在当前池子里, 请点「\u27f3 获取新闻」刷新后重试'})
                return
            for ev, payload in ai_news.analyze_stream(item, force=force):
                _send(ev, payload)
        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
            # ⚠️ ConnectionAbortedError 必须带上: Windows 上客户端断开常抛 WinError 10053
            #    (= ConnectionAbortedError), 现有 _handle_sse 漏了它、靠 do_GET 外层兜底。
            pass
        finally:
            self.close_connection = True   # v2.112a: 双保险, 别让 handler 返回后还挂着连接
            ai_news.mark_done(news_id)   # 兜住任何没走到 _release 的路径 (异常/断开)

    def _handle_board_rps_ai(self, qs):
        """v2.281 (2026-11-12 用户 m10425/m10906): 板块轮动 AI 分析 —— 流式 NDJSON。

        事件: {ev:'meta', d:{summary,days}} / {ev:'delta', d:{content}}
              {ev:'err', d:{code,msg}} / {ev:'done', d:{}}

        🔴 与 _handle_ai_chat 同一条铁律: **Connection: close** 必写, 且 finally
           里再置 close_connection —— keep-alive 会让 handler 返回后阻塞读下一个请求,
           每轮泄漏一个线程。
        """
        kind = qs.get('kind', 'industry')
        if kind not in _brt.KINDS:
            kind = 'industry'
        try:
            level = int(qs.get('level', '2'))
        except (TypeError, ValueError):
            level = 2
        try:
            days = int(qs.get('days', str(_brt.DAYS_DEF)))
        except (TypeError, ValueError):
            days = _brt.DAYS_DEF
        focus = qs.get('focus', '') or ''
        # 🔴 v2.282 (2026-11-12 用户 m11382): RPS 轮动分析**锁死 DeepSeek**, 前端传什么都不看。
        #    用户原话:「RPS轮动分析这个功能强制固定锁死用deepseek来分析, 这个需要更聪明的模型比较好一点」
        #    DeepSeek 在 ai_news.SLOTS 里是 'backup'; 之前走 primary(智谱 GLM-4-Flash)。
        slot = _bra.RPS_SLOT

        self.send_response(200)
        self.send_header('Content-Type', 'application/x-ndjson; charset=utf-8')
        self.send_header('Cache-Control', 'no-cache')
        self.send_header('Connection', 'close')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

        def _send(ev, payload):
            self.wfile.write((json.dumps({'ev': ev, 'd': payload}, ensure_ascii=False)
                              + '\n').encode('utf-8'))
            self.wfile.flush()

        try:
            _rot = _brt.rotation(kind, level, days, force=False)
            if not _rot.get('ok'):
                _send('err', {'code': 'nodata',
                              'msg': _rot.get('error') or '轮动矩阵没取到数据'})
                self.close_connection = True
                return
            _sys_p, _usr_p, _sig = _bra.build_prompts(
                kind, level, _rot.get('days') or days, _rot.get('dates') or [],
                _rot.get('columns') or {}, _rps_market_block(), focus)
            _send('meta', {'summary': _bra.summary_of(_sig),
                           'days': _rot.get('days') or days,
                           'focus': bool(focus.strip())})

            _cfg = _an2.ai_cfg()
            if not _cfg.get('enabled'):
                _send('err', {'code': 'disabled', 'msg': 'AI 已在数据源设置里关闭'})
                self.close_connection = True
                return
            # v2.282: 只跑这一个槽, **不降级**。以前是 (slot,)+其它槽, 智谱挂着就悄悄换了家。
            _order = (slot,)
            _why = []
            _done = False
            for _s in _order:
                _sc = _cfg.get(_s)
                if not isinstance(_sc, dict):
                    _why.append('AI 槽 [%s] 没配置' % _s)
                    continue
                _slot = dict(_sc)
                _slot['_slot'] = _s
                if not _an2.slot_state(_slot)['ready']:
                    _why.append('%s(未配置 key)' % _slot['name'])
                    continue
                _key, _src = _an2.resolve_key(_slot)
                _ccfg = dict(_cfg)
                _ccfg['max_tokens'] = 6000
                _ccfg['timeout'] = max(int(_ccfg.get('timeout') or 60), 180)
                # v2.282: 先告诉前端"这次是谁答的", 免得再出现"以为是 DeepSeek 其实是智谱"
                _send('who', {'provider': _slot.get('name') or '',
                              'model': _slot.get('model') or '',
                              'slot': _s})
                try:
                    _got = False
                    for _ev, _p in _an2._stream_once(
                            _slot, _key,
                            [{'role': 'system', 'content': _sys_p},
                             {'role': 'user', 'content': _usr_p}], _ccfg):
                        if _ev == 'delta':
                            _txt = (_p or {}).get('text') or (_p or {}).get('content') or ''
                            if _txt:
                                _got = True
                                _send('delta', {'content': _txt})
                        elif _ev == 'done':
                            _full = (_p or {}).get('text') or ''
                            # 上游可能只在 done 里给全文(没流式增量) → 补发一次
                            if _full and not _got:
                                _send('delta', {'content': _full})
                                _got = bool(_full)
                    if _got:
                        _done = True
                        break
                    _why.append('%s(空正文)' % _slot['name'])
                except _an2._AIErr as _e:
                    _why.append('%s(%s)' % (_slot['name'], _e.msg))
                except Exception as _e:
                    _why.append('%s(%s)' % (_slot['name'], type(_e).__name__))
            if not _done:
                _send('err', {'code': 'ai',
                              'msg': ' / '.join(_why) or 'AI 数据源失败'})
            else:
                _send('done', {})
        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
            pass
        except Exception as _e:
            try:
                _send('err', {'code': 'server',
                              'msg': '%s: %s' % (type(_e).__name__, str(_e)[:200])})
            except Exception:
                pass
        finally:
            self.close_connection = True

    def _handle_recap_ai(self, qs):
        """v2.302 (2026-11-13 用户 m16307): AI 大盘复盘 —— 流式 NDJSON。

        照抄 TSP /api/market-recap/analyze（backend/app/api/market_recap.py）:
          装配市场总览 → 复盘提示词 → 流式 LLM → 逐块推 NDJSON。

        事件: {ev:'meta', d:{as_of,emotion_score,emotion_label,summary}}
              {ev:'who',  d:{provider,model,slot}}
              {ev:'delta',d:{content}}
              {ev:'err',  d:{code,msg}}
              {ev:'done', d:{content}}     全文, 前端存历史用

        🔴 与 _handle_board_rps_ai 同一条铁律: **Connection: close** 必写, 且 finally
           里再置 close_connection —— keep-alive 会让 handler 返回后阻塞读下一个请求,
           每轮泄漏一个线程。

        ⚠️ 与 RPS 的区别: RPS 锁死 DeepSeek **不降级**(用户 m11382);
           复盘允许降级(backup → primary), 因为复盘是每天要看的东西, 宁可换个模型也别空白。
        """
        focus = (qs.get('focus', '') or '').strip()
        _slot_pref = (qs.get('slot', '') or '').strip()
        if _slot_pref not in _an2.SLOTS:
            _slot_pref = _mrc.RECAP_SLOT

        self.send_response(200)
        self.send_header('Content-Type', 'application/x-ndjson; charset=utf-8')
        self.send_header('Cache-Control', 'no-cache')
        self.send_header('Connection', 'close')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

        def _send(ev, payload):
            self.wfile.write((json.dumps({'ev': ev, 'd': payload}, ensure_ascii=False)
                              + '\n').encode('utf-8'))
            self.wfile.flush()

        try:
            _ov, _idx = _mrc.gather()
            if not _ov or not _ov.get('ok') or not _ov.get('date'):
                _send('err', {'code': 'nodata',
                              'msg': '暂无市场数据, 无法复盘（先让市场总览跑出数据）'})
                self.close_connection = True
                return

            _sys_p, _usr_p = _mrc.build_prompts(_ov, _idx, focus)
            _send('meta', {
                'as_of': _ov.get('date') or '',
                'emotion_score': (_ov.get('emotion') or {}).get('score', 50),
                'emotion_label': (_ov.get('emotion') or {}).get('label', ''),
                'summary': _mrc.recap_summary(_ov, _idx),
            })

            _cfg = _an2.ai_cfg()
            if not _cfg.get('enabled'):
                _send('err', {'code': 'disabled', 'msg': 'AI 已在数据源设置里关闭'})
                self.close_connection = True
                return

            # 允许降级: 首选槽 → 其余槽（与 RPS 的 _order=(slot,) 不同）
            _order = (_slot_pref,) + tuple(s for s in _an2.SLOTS if s != _slot_pref)
            _why = []
            _done = False
            _full = []
            for _s in _order:
                _sc = _cfg.get(_s)
                if not isinstance(_sc, dict):
                    _why.append('AI 槽 [%s] 没配置' % _s)
                    continue
                _slot = dict(_sc)
                _slot['_slot'] = _s
                if not _an2.slot_state(_slot)['ready']:
                    _why.append('%s(未配置 key)' % _slot['name'])
                    continue
                _key, _src = _an2.resolve_key(_slot)
                _ccfg = dict(_cfg)
                _ccfg['max_tokens'] = 7000
                _ccfg['timeout'] = max(int(_ccfg.get('timeout') or 60), 240)
                _send('who', {'provider': _slot.get('name') or '',
                              'model': _slot.get('model') or '',
                              'slot': _s})
                try:
                    _got = False
                    for _ev, _p in _an2._stream_once(
                            _slot, _key,
                            [{'role': 'system', 'content': _sys_p},
                             {'role': 'user', 'content': _usr_p}], _ccfg):
                        if _ev == 'delta':
                            _txt = (_p or {}).get('text') or (_p or {}).get('content') or ''
                            if _txt:
                                _got = True
                                _full.append(_txt)
                                _send('delta', {'content': _txt})
                        elif _ev == 'done':
                            _t2 = (_p or {}).get('text') or ''
                            if _t2 and not _got:
                                _got = True
                                _full.append(_t2)
                                _send('delta', {'content': _t2})
                    if _got:
                        _done = True
                        break
                    _why.append('%s(空正文)' % _slot['name'])
                    _full = []
                except _an2._AIErr as _e:
                    _why.append('%s(%s)' % (_slot['name'], _e.msg))
                    _full = []
                except Exception as _e:
                    _why.append('%s(%s)' % (_slot['name'], type(_e).__name__))
                    _full = []
            if not _done:
                _send('err', {'code': 'ai', 'msg': ' / '.join(_why) or 'AI 数据源失败'})
            else:
                _send('done', {'content': ''.join(_full)})
        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
            pass
        except Exception as _e:
            try:
                _send('err', {'code': 'server',
                              'msg': '%s: %s' % (type(_e).__name__, str(_e)[:200])})
            except Exception:
                pass
        finally:
            self.close_connection = True

    def _handle_ai_chat(self):

        """v2.116 (2026-09-25): AI 对话 —— 每请求一条**私有**流, 换行分隔 JSON (NDJSON)。

        为什么不用 SSE: 这是**一问一答的 POST**, 由 fetch 驱动而不是 EventSource。
        NDJSON 让前端 reader 读到一行就能 parse 一行, 不必自己拆 SSE 帧。
        (ai_news 那条相反 —— 它由点新闻行触发, 用 EventSource 更顺。)

        🔴 与 _handle_ai_news 同一套纪律, 一条都不能省:
           Connection: **close** (keep-alive 会把 close_connection 置 False,
           handler 返回后服务端回头阻塞读下一个请求 → 每轮对话泄漏一个线程),
           finally 里再置一次 close_connection=True 兜底。
        """
        body = self._read_json_body()
        slot = body.get('slot') or 'primary'
        if slot not in ('primary', 'backup'):
            slot = 'primary'
        messages = body.get('messages') or []

        self.send_response(200)
        self.send_header('Content-Type', 'application/x-ndjson; charset=utf-8')
        self.send_header('Cache-Control', 'no-cache')
        self.send_header('Connection', 'close')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

        def _send(ev, payload):
            # ⚠️ 必须 JSON 编码再拼 —— 正文里的换行会直接把 NDJSON 帧撕裂,
            #   前端 parse 那一行必炸 (而且看起来像"服务端没响应")
            self.wfile.write((json.dumps({'ev': ev, 'd': payload}, ensure_ascii=False)
                              + '\n').encode('utf-8'))
            self.wfile.flush()

        try:
            for ev, payload in ai_chat.chat_stream(slot, messages, _ai_tool_exec):
                _send(ev, payload)
        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
            pass          # 用户关窗 / 切走 —— 正常的, 不是错
        except Exception as e:
            try:
                _send('err', {'code': 'server',
                              'msg': '%s: %s' % (type(e).__name__, str(e)[:200])})
            except Exception:
                pass
        finally:
            self.close_connection = True

    def _handle_sse(self):
        """Server-Sent Events 长连接"""
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.send_header('Cache-Control', 'no-cache')
        self.send_header('Connection', 'keep-alive')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

        q = queue.Queue()
        with SSE_LOCK:
            SSE_CLIENTS.append(q)

        try:
            # 先发当前状态
            init_msg = json.dumps({
                'type': 'init',
                'running': SCAN_STATE['running'],
                'done': SCAN_STATE['done'],
                'progress': SCAN_STATE['progress'],
                'total': SCAN_STATE['total'],
            }, ensure_ascii=False)
            self.wfile.write(f"event: init\ndata: {init_msg}\n\n".encode())
            self.wfile.flush()

            while True:
                msg = q.get(timeout=30)
                self.wfile.write(msg.encode())
                self.wfile.flush()
        except (queue.Empty, BrokenPipeError, ConnectionResetError):
            pass
        finally:
            with SSE_LOCK:
                if q in SSE_CLIENTS:
                    SSE_CLIENTS.remove(q)


# ============================================================
# 启动
# ============================================================
if __name__ == '__main__':
    print('=' * 55)
    print('  BS King Quant Terminal v2.0')
    print('  六大验证理论 | 33条量化规则 | 4模块引擎')
    print('  Livermore + Turtle + VCP + Wyckoff')
    print(f'  => http://127.0.0.1:{PORT}')
    print(f'  => 旧终端 8788 不受影响')
    print('=' * 55)
    # v2.4: 启动即后台预热四档均线缓存 (2026-08-05, 打开终端全档位秒切换)
    threading.Thread(target=_prewarm_all_lines, daemon=True).start()
    # v2.57: 指数日K每日自动落盘 guard (300s tick; QT_NO_IDX_GUARD=1 停用)
    if IDX_GUARD_ON:
        threading.Thread(target=_index_guard_loop, daemon=True).start()
    # v2.63: 启动即回填 RSRS 历史 (换机/重装/删了 macro_daily.json 也能自愈 —
    # 只补缺失日期, 已有实盘值的行不动; 纯本地计算, 放后台不挡启动)
    threading.Thread(target=macro_daily.backfill_rsrs, daemon=True).start()
    # v2.150 (2026-10-18): 标题栏系统右键菜单拦截守护 —— 每 2 秒复查并删空。
    #   Chrome 在窗口状态变化(最大化/还原)后会重建系统菜单, 所以必须周期复查;
    #   QT_NO_CAPTION_GUARD=1 可停用; 非 Windows / 无窗口时静默不做事。
    win_caption.start_guard()
    # 自动任务（v2.152 建立，v2.233 提前 4 分钟，v2.331 加竞价，v2.333 加 09:28 补跑）:
    #   09:27:20 竞价打板 / 11:31 快照 / 11:35 板块 / 15:31 快照+CSV / 15:35 板块。
    #   只在「页面开着 + 到点 + 今日未做过」时执行；QT_NO_AUTO_TASK=1 可整体停用。
    # 🔴 用 bind() 注入执行体，不让 auto_task 自己 import server —— 本文件被 `python -m quant_terminal.server`
    #   起，模块名是 __main__；再 import 一次会在同一进程里第二次执行整个 server.py，造出第二套全局状态。
    # 🔴 竞价那层为什么要绕: 扫描完整链路（算结果+落盘槽+归档+推 SSE+更新 SCAN_STATE）全在 run_scan_async 里，
    #   任务里自己调 auction_scan.scan() 只会算出一份没人看得见的结果。而且必须起 daemon 线程 —— 自动任务在
    #   独立 tick 线程里跑，同步调会把它堵住 1.5~20 秒，期间其它时点全错过。
    #   ★ 与用户手点按钮走的是**同一条路**，所以缓存/归档/气泡/状态条四条副作用自动生效。
    def _auto_scan_auction():
        try:
            _w = bool(SCAN_STATE.get('running'))
        except Exception:
            _w = False
        t = threading.Thread(target=run_scan_async,
                             args=('auction', '4', 'auto'), daemon=True)
        t.start()
        return {'ok': True, 'mode': 'auction', 'lines': '4', 'pool': 'auto',
                'note': ('已有扫描在跑, 本次已接管' if _w else '已发起竞价扫描')}

    auto_task.bind(sector_fn=run_sector_refresh_auto,
                   csv_fn=run_snapshot_from_intraday,
                   state_fn=get_trading_status,
                   scan_auction_fn=_auto_scan_auction)
    auto_task.start()
    # v2.279 (2026-11-12 用户 m10425 「可以先算一下，确实首次点开要等太久」):
    #   板块大窗口冷启动 12.4 秒 (全市场快照 3.7s + 倒读 5589 个 CSV 0.31s + 计算),
    #   热缓存 0.92 秒。启动后后台先跑一遍 board_rank._facts() 把缓存焐热 ——
    #   之后第一次点「行业板块 / 概念板块」就和第二次一样快。
    #   放后台线程不挡 serve_forever; 失败只打日志, 绝不影响终端启动。
    def _prewarm_board():
        """v2.294 (2026-11-13 用户 m14431): **严格分步串行**预热。

        用户原话:「这些东西预热要有个顺序不能同时一起跑免得资源占用过高」——
        原来四段挤在一个 try 里连跑, 启动瞬间 5589 个 CSV 被 board_rank、board_overview
        两个线程池同时抢 (各自 ThreadPoolExecutor(8)), 磁盘队列打满, 反而都变慢。

        现在: 一步跑完 sleep 一下再跑下一步, 并且把"最贵且共用基础数据"的排最前 ——
          ① 市场总览 (自己读 5589 × 末65行, 但它拿到的事实表不能复用, 就让它第一个独占 IO)
          ② 行业板块 (board_rank._facts, 8 行/只 —— 顺带把概念板块的事实表也焐热了)
          ③ 概念板块 (只差一次分组计算, 很快)
          ④ 上级行业趋势 3/4/5/6 档 (读 parent_trend.json + 板块指数缓存, 与上面互不干扰)

        每步单独 try/except: 一步失败不影响后面 —— 这条是硬要求, 预热永远不许拖垮启动。
        """
        _steps = [
            ('市场总览',       2.0,  _pw_overview),
            ('行业板块',       1.5,  _pw_board_ind),
            ('概念板块',       1.5,  _pw_board_con),
            # v2.353 (用户 m36569): 大盘云图 —— 紧跟在两个板块步骤后面。
            #   它复用 board_rank._facts() 的缓存, 此时已焐热 ⇒ 行业那趟实测 0.7 秒
            #   (冷启要 6.9 秒, 其中 6.3 秒花在读 5585 个 CSV 上)。
            ('大盘云图',       0.5,  _pw_yuntu),
            # v2.359 (用户 m38119): 宽基指数存档 —— 特色指标要中证红利 2005 年起、
            #   创业板指 2010 年起, 都是**本地文件**, 缺了会一路联网翻页(首次约 3 秒)。
            ('指数存档',       0.5,  _pw_idx),
            ('上级行业趋势4档', 1.0,  _pw_parent),
            # v2.323 (用户 m22464): 「连板梯队」「龙虎榜」原来是「点开才算」, 冷启要等几秒；
            #   现在预热时就跑并落档。**排在这里而不是最后** —— 两者只要 1~2 秒,
            #   而下面的「市场环境」要扫 5589×275 根 K 线(约 20 秒), 放它后面等于白等。
            ('连板梯队',       0.5,  _pw_ladder),
            ('龙虎榜',         0.5,  _pw_lhb),
            # v2.311 (用户 m18234): 市场环境要扫 5589 个 CSV 的 275 根 K 线, 单跑约 20 秒。
            #   放最后一步 —— 它最贵, 而且结果只给「市场环境」那一页用, 不急。
            ('市场环境',       1.0,  _pw_regime),
        ]
        try:
            time.sleep(2.0)      # 让 serve_forever 先起来, 别跟启动抢 IO
        except Exception:
            return
        for _nm, _gap, _fn in _steps:
            _t = time.time()
            try:
                _msg = _fn()
                print('[预热] %s 完成 %.1fs%s' % (_nm, time.time() - _t, ('  ' + _msg) if _msg else ''))
            except Exception as _e:
                print('[预热] %s 失败 %s: %s' % (_nm, type(_e).__name__, _e))
            try:
                time.sleep(_gap)
            except Exception:
                break

    def _pw_overview():
        _bov.invalidate()
        d = _bov.snapshot(force=False)
        # v2.323: 预热顺手落一份存档 —— 下次启动前端点开「市场总览」直接读盘秒显
        try:
            _arc.save_window('market_overview', d, force=True)
        except Exception:
            pass
        return '%s 只 / %s' % (d.get('n_stock'), d.get('date'))

    def _pw_board_ind():
        _brd.invalidate()
        _brd.rank('industry', 2, 'heat')      # 内部会把 _facts() 填进缓存
        try:
            from quant_terminal import kzz as _kzz2
            _kzz2.bonds()                     # v2.304: 转债正股清单(缓存 12h, 冷启 0.6s)
        except Exception:
            pass
        try:
            _arc.save_window('board_industry_2', _brd.rank('industry', 2, 'heat'), force=True)
        except Exception:
            pass
        c = _brd.cache_info()
        return '%s 只 / %s' % (c.get('n'), c.get('date'))

    def _pw_yuntu():
        """v2.353: 大盘云图两个口径一起焐热 + 各落一份存档。

        行业口径 283KB / 概念口径 44KB —— 都只写一次, 不随 60 秒 TTL 反复落盘。
        """
        _n = 0
        for _k in ('industry', 'concept'):
            try:
                _d4 = _yt.build(_k, force=True)
                _arc.save_window('yuntu_' + _k, _d4, force=True)
                _n += 1
            except Exception:
                pass
        return '%d 个口径' % _n

    def _pw_idx():
        """v2.359: 宽基指数本地存档预热 —— 缺的补深、已全的秒回。"""
        _done = _ids.prewarm(log=lambda m: _swlog('idx_prewarm', m))
        _syn = _idx_auto_sync(blocking=True)      # v2.359c: 顺手把今天的增量补上
        # v2.367: 收盘后记一条「头部占比」真值快照 —— 这是该指标越用越准的唯一来源
        #   （东财只给当前时点的 ETF 规模，历史规模序列不存在）。盘中它自己会拒绝。
        try:
            _sn = _etf.snapshot_today()
            _sn = 'snap-ok' if (_sn or {}).get('ok') else 'snap-skip'
        except Exception as _e:
            _swlog('idx_head_snapshot', _e)
            _sn = 'snap-fail'
        # v2.367: 顺手把「头部占比」日线也算一遍塞进缓存 —— 这条序列冷算 4.7 秒,
        #   每天只变一次(收盘快照更新), 不预热的话第一次点开就是 4 秒多。
        #   **必须放在 snapshot_today 之后** —— 快照写盘会改 ratio json 的 mtime,
        #   先算后写等于白算。
        try:
            _etf.head_ratio_series()
        except Exception as _e2:
            _swlog('idx_head_warm', _e2)
        # v2.369: 股债利差 —— 每日增量归档(盘中跳过, 收盘后才拉)。**不阻塞**:
        #   乐咕 + 中证 + 东财三家串行约 12 秒, 放在预热线程里跑就行。
        try:
            _eb.snapshot_today()
        except Exception as _e3:
            _swlog('idx_eb_warm', _e3)
        # v2.370: 市盈率走势图 —— 只刷当日增量(backfill 起点=今天), 不整表重拉;
        #   全量日频回补由 GET /api/pe_ratio?mode=sync&deep=1 手动触发(≈5 分钟/指数)。
        try:
            _pr.snapshot_today()
        except Exception as _e4:
            _swlog('pe_ratio_warm', _e4)
        return '%d 只/%s/%s' % (len(_done), _syn, _sn)

    def _idx_auto_sync(blocking=False):
        """v2.359c (用户 m38119「后续增量写入补充到本地数据」): 指数存档后台增量补。

        节流: 同一天只跑一次（`_IDX_SYNC['day']` 记住跑过的日期）。
        时机: 只在**收盘后**才补 —— 盘中腾讯给的当日是 forming bar，
              `idx_store.sync()` 内部还有一层 `_drop_forming_today` 兜底，但
              没必要每开一次页面就白跑几秒网络。
        用法: `blocking=True` 给预热用（等它跑完再走下一步），
              默认起 daemon 线程 —— 页面永远不等。
        """
        _today = time.strftime('%Y-%m-%d')
        if _IDX_SYNC.get('day') == _today:
            return 'skip'
        _hm = time.strftime('%H%M')
        if _hm < '1506':
            return 'skip-intraday'
        if _IDX_SYNC.get('running'):
            return 'running'
        _IDX_SYNC['running'] = 1

        def _run():
            _n = _ok = 0
            try:
                for _c in list(_ids.IDX_INFO.keys()):
                    try:
                        _r = _ids.sync(_c)
                        _n += 1
                        if _r != 'fail':
                            _ok += 1
                    except Exception as _e1:
                        _swlog('idx_auto_sync', _e1)
                _IDX_SYNC['day'] = _today
            finally:
                _IDX_SYNC['running'] = 0

        if blocking:
            _run()
            return 'done'
        try:
            threading.Thread(target=_run, daemon=True).start()
        except Exception:
            _IDX_SYNC['running'] = 0
            return 'fail'
        return 'started'

    def _pw_board_con():
        _rd = _brd.rank('concept', 2, 'heat')  # 复用上面的 _facts, 只重算分组
        try:
            _arc.save_window('board_concept_2', _rd, force=True)
        except Exception:
            pass
        return ''

    def _pw_parent():
        # ⚠ 路由里 force=0 之后还要跑 _idx_pct_attach + _top5_attach,
        #   两步也一起做, 否则窗口打开时那两列仍然是冷的。
        _ok = 0
        for _ln in ('3', '4', '5', '6'):
            _pd2 = pt.parent_trend(_ln, False)
            _idx_pct_attach(_pd2.get('rows'))
            _top5_attach(_pd2.get('rows'), _ln, None)
            # v2.324 (用户 m22862): 预热顺手落档 —— 反正 4 档都算过了
            try:
                _arc.save_window('trend_parent_' + _ln, _pd2, force=True)
            except Exception:
                pass
            _ok += 1
        return '%d 档' % _ok

    def _pw_ladder():
        from quant_terminal import ladder_scan as _lad2
        _n = 0
        for _dir in ('up', 'down'):
            try:
                _d2 = _lad2.ladder('', _dir, force=True)
                _arc.save_window('ladder_' + _dir, _d2, force=True)
                _n += 1
            except Exception:
                pass
        return '%d 张' % _n

    def _pw_lhb():
        from quant_terminal import fuyao_lhb as _lhb2
        _n = 0
        for _bd, _nm in (('all', 'all'), ('org', 'org'), ('hot_money', 'hm')):
            try:
                _d3 = _lhb2.lhb_list(None, _bd)
                if isinstance(_d3, dict) and _d3.get('ok'):
                    _arc.save_window('lhb_' + _nm, _d3, force=True)
                    _n += 1
            except Exception:
                pass
        return '%d 张' % _n

    def _pw_regime():
        # v2.311: 市场环境(250 日)。冷启约 20s, 结果落 regime_cache.json(10 分钟 TTL)。
        _h = _rg.history(250, force=False)
        # v2.326 (用户 m23308): 顺手把 250 档的四份存档都落一遍 ——
        #   开机第一次点「市场环境」「情绪周期」就能秒显, 不用再等现算。
        #   500/1000 档不在这里做(要多扫一倍 CSV), 等用户真点了自然会生成。
        try:
            _arc.save_window('regime_250', _h, force=True)
        except Exception as _ae:
            _swlog('pw_arc_regime', _ae)
        for _fn, _nm in ((_rg.phase_summary, 'phase_sum_250'),
                         (_rg.phases, 'phase_segs_250')):
            try:
                _arc.save_window(_nm, _fn(250, force=False), force=True)
            except Exception as _ae:
                _swlog('pw_arc_' + _nm, _ae)
        for _k in ('concept', 'industry'):
            try:
                _arc.save_window('phase_ml_250_' + _k,
                                 _rg.mainline(250, kind=_k, top=10, force=False), force=True)
            except Exception as _ae:
                _swlog('pw_arc_ml_' + _k, _ae)
        return '%d 日' % (_h.get('total') or 0)

    threading.Thread(target=_prewarm_board, daemon=True).start()
    server = ThreadingHTTPServer(('127.0.0.1', PORT), QuantHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\n终端已停止')
        server.shutdown()
