"""
BS King 量化终端 v2.3 — 模拟交易模块
=====================================
SimAccount:      实时模拟盘 (手动买卖 + 纪律自动检查, 初始100万)
simulate_history: 历史回放 (逐日自动执行纪律, 验证纪律有效性)

纪律执行 (v2.1 海龟/吊灯/Zanger):
  - 破止损(2N/通道低点) → 全部卖出
  - 吊灯止损(22日高-3×ATR22) → 全部卖出
  - 0.5N金字塔加仓 (≤4单位) → 加1单位
  - 分批止盈(+15%卖1/4) → 卖1/4
  - 时间止损(10日未突破买入前高点) → 全部卖出
  - 手续费: 佣金万2.5(最低5元) + 印花税卖出万5
持久化: quant_terminal/sim_state.json (原子写)
"""
import os, json, sys, time, threading, bisect
from datetime import datetime
from typing import Dict, List, Optional

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from quant_terminal.engine import (QuantEngine, TurtleExecutor,
                                   load_industry_concept_maps,
                                   read_index_daily, load_csv_kline)
from quant_terminal.backtest import preload_all, build_pool_for_date, slice_kline

STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sim_state.json')
# v2.65: 历史收益结算缓存 (与 sim_state.json 同目录); 权威永远是 records, 此文件可整删重建
HISTORY_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sim_hist.json')
HISTORY_SCHEMA = 1

# (2026-08-16: 模拟盘并发锁 — 原每请求 SimAccount() 读-改-写 sim_state.json 无锁,
#  双击卖出可重复成交/买入丢失(后写覆盖先写). 锁必须从加载前持有到保存后,
#  由 server.py 的 sim/buy|sell|confirm|reset 端点负责包住"加载→操作→保存"全周期)
ACCOUNT_LOCK = threading.Lock()

COMMISSION_RATE = 0.00025   # 佣金 万2.5
MIN_COMMISSION = 5.0        # 最低佣金 5元
STAMP_RATE = 0.0005         # 印花税 卖出万5 (2023.8起减半)

MAX_UNITS = 4               # 海龟: 单股最多4单位
MAX_POSITIONS = 5           # 铁律: 持股≤5只


def _now_str() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def _ts(acc: 'SimAccount') -> str:
    """交易时间戳: 回放时用回放日期, 模拟盘用当前时间"""
    return acc.trade_date if acc.trade_date else _now_str()


class SimAccount:
    """实时模拟盘账户 (100万初始资金)

    persist=False 时纯内存运行 (历史回放用), 不读写 sim_state.json
    """

    def __init__(self, initial_cash: float = 1_000_000, state_file: str = STATE_FILE,
                 persist: bool = True, addon_step: float = 0.5,
                 full_in_out: bool = True):
        self.state_file = state_file
        self.persist = persist
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.positions: List[Dict] = []
        self.records: List[Dict] = []
        self.trade_date = None  # 历史回放时设为回放日期, 否则用当前时间
        self.addon_step = addon_step  # 加仓间隔 (0.5N=海龟原版, 1.0N=A股适配宽松版)
        # v2.76 (2026-09-19 用户定案): 全进全出 — 买=一次买满(前端按总资产20%), 卖=一笔全清,
        #   止盈也一次全清, 取消 0.5N 加仓。默认 True = 终端实盘模拟盘。
        #   ⚠️ 回测/历史回放 (simulate_history) 必须显式传 False 保持海龟原版语义,
        #   否则终端一改规矩就会**静默改掉回测结论**。
        self.full_in_out = full_in_out
        if persist:
            self.load()

    # ---------- 数据持久化 ----------
    def to_dict(self) -> Dict:
        return {'initial_cash': self.initial_cash, 'cash': self.cash,
                'positions': self.positions, 'records': self.records,
                'updated_at': _now_str()}

    def save(self):
        if not self.persist: return
        try:
            tmp = self.state_file + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump(self.to_dict(), f, ensure_ascii=False, indent=1)
            # 备份上一版 (2026-08-04): 防止体检/测试误 reset 后无数据可恢复
            try:
                if os.path.exists(self.state_file):
                    import shutil
                    shutil.copy2(self.state_file, self.state_file + '.bak')
            except Exception:
                pass
            os.replace(tmp, self.state_file)  # 原子写
        except Exception:
            pass

    def load(self):
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    d = json.load(f)
                self.initial_cash = d.get('initial_cash', self.initial_cash)
                self.cash = d.get('cash', self.initial_cash)
                self.positions = d.get('positions', [])
                self.records = d.get('records', [])
        except Exception:
            pass

    def reset(self):
        self.cash = self.initial_cash
        self.positions = []
        self.records = []
        self.save()

    # ---------- 账户视图 ----------
    def holdings_value(self, prices: Dict[str, float]) -> float:
        """持仓市值 (prices: {code: 现价}, code 为6位数字或带前缀均可)"""
        total = 0.0
        for p in self.positions:
            px = prices.get(p['code'])
            if px is None:
                for k, v in prices.items():
                    if str(k).endswith(p['code']): px = v; break
            if px is None: px = p['avg_cost']
            total += px * p['shares']
        return total

    def nav(self, prices: Dict[str, float]) -> float:
        return self.cash + self.holdings_value(prices)

    # ---------- 交易 ----------
    def _find_pos(self, code: str) -> Optional[Dict]:
        for p in self.positions:
            if p['code'] == code or code.endswith(p['code']):
                return p
        return None

    @staticmethod
    def buy_fee(amount: float) -> float:
        return max(MIN_COMMISSION, amount * COMMISSION_RATE)

    @staticmethod
    def sell_fee(amount: float) -> float:
        return max(MIN_COMMISSION, amount * COMMISSION_RATE) + amount * STAMP_RATE

    def suggest_units(self, price: float, stop_loss: float) -> int:
        """单笔风险预算: 股数 = 账户1% ÷ (买价-止损价), 取整百 (最小100)

        仓位上限: 单票≤15%账户 (硬止损收紧后股数放大, 防止超仓)
        """
        if price <= 0 or stop_loss <= 0 or price <= stop_loss: return 100
        risk = self.initial_cash * 0.01
        shares = int(risk / (price - stop_loss))
        shares = (shares // 100) * 100
        cap = int(self.initial_cash * 0.15 / price)
        cap = (cap // 100) * 100
        return min(max(100, shares), max(100, cap))

    def buy(self, code: str, name: str, price: float, shares: int = 0,
            stop_loss: float = 0, addon_price: float = 0,
            take_profit_at: float = 0, chandelier_stop: float = 0,
            n_atr20: float = 0, sector: str = '', reason: str = '手动买入') -> Dict:
        """买入/加仓 (已持仓时走加仓路径, 合并成本)"""
        if price <= 0: return {'ok': False, 'msg': '价格无效'}
        if shares <= 0:
            shares = self.suggest_units(price, stop_loss) if stop_loss else 100
        shares = (shares // 100) * 100
        if shares < 100: return {'ok': False, 'msg': '股数需≥100股(整手)'}

        amount = price * shares
        fee = self.buy_fee(amount)
        adjusted = False
        if amount + fee > self.cash:
            # v2.33 (2026-09-01): 现金不足自动降档 — 能买多少手买多少手 (整百向下取整),
            # 连一手(100股)都买不起才报现金不足 (原实现直接拒绝 → 现金少了加仓被卡死)
            max_sh = int(self.cash / price)
            max_sh = (max_sh // 100) * 100
            while max_sh >= 100 and max_sh * price + self.buy_fee(max_sh * price) > self.cash:
                max_sh -= 100
            if max_sh < 100:
                return {'ok': False, 'msg': f'现金不足: 连一手都买不起 (一手约{price*100:.0f}元+费, 可用{self.cash:.0f})'}
            shares = max_sh
            amount = price * shares
            fee = self.buy_fee(amount)
            adjusted = True

        pos = self._find_pos(code)
        if pos:
            # 加仓: 加权平均成本, 单位+1, 止损上移
            old_amt = pos['avg_cost'] * pos['shares']
            new_amt = old_amt + amount + fee
            pos['shares'] += shares
            pos['avg_cost'] = new_amt / pos['shares']
            pos['units'] = min(MAX_UNITS, pos.get('units', 1) + 1)
            if stop_loss > 0: pos['stop_loss'] = stop_loss
            # 加仓后下一档加仓位 = 本次加仓价 + 步长×N (只上移, 防止同区间连续触发)
            n_val = pos.get('n_atr20', price * 0.03)
            pos['addon_price'] = round(price + self.addon_step * n_val, 2)
            if chandelier_stop > 0: pos['chandelier_stop'] = chandelier_stop
            pos['last_price'] = price
            action = '加仓'
        else:
            if len(self.positions) >= MAX_POSITIONS:
                return {'ok': False, 'msg': f'持仓已达{MAX_POSITIONS}只铁律上限'}
            n_val = n_atr20 or price * 0.03
            if not stop_loss: stop_loss = round(price - 2 * n_val, 2)
            # P3-14 fix: 外部传入的addon_price(引擎纪律真实ATR口径)优先, 无传入时按步长兜底
            if addon_price <= 0: addon_price = round(price + self.addon_step * n_val, 2)
            if not take_profit_at: take_profit_at = round(price * 1.15, 2)
            if not chandelier_stop:
                chandelier_stop = round(price - 3 * n_val, 2)
            pos = {'code': code, 'name': name, 'shares': shares,
                   'avg_cost': round((amount + fee) / shares, 3),
                   # (2026-08-16 修复: 原用_now_str()真实运行日→回放中entry_idx恒-1, 时间止损永不触发; 改用_ts回放日期)
                   'entry_price': round(price, 2), 'entry_date': _ts(self)[:10],
                   'units': 1, 'stop_loss': round(stop_loss, 2),
                   'addon_price': round(addon_price, 2),
                   'take_profit_at': round(take_profit_at, 2),
                   'chandelier_stop': round(chandelier_stop, 2),
                   'n_atr20': round(n_val, 3), 'sector': sector, 'last_price': price}
            self.positions.append(pos)
            action = '买入'

        self.cash -= amount + fee
        self.records.append({'time': _ts(self), 'code': code, 'name': name,
                             'action': action, 'price': round(price, 2),
                             'shares': shares, 'fee': round(fee, 2),
                             'reason': reason, 'cash_after': round(self.cash, 2)})
        self.save()
        tip = ' (现金不足, 自动降至' + str(shares) + '股)' if adjusted else ''
        return {'ok': True, 'msg': f'{action} {name} {shares}股@{price:.2f}{tip}', 'shares': shares}

    def sell(self, code: str, price: float, shares: int = 0,
             reason: str = '手动卖出') -> Dict:
        """卖出 (shares=0 表示全部)"""
        pos = self._find_pos(code)
        if not pos: return {'ok': False, 'msg': '无此持仓'}
        if price <= 0: return {'ok': False, 'msg': '价格无效'}
        shares = shares if shares > 0 else pos['shares']
        shares = min(shares, pos['shares'])
        if shares <= 0: return {'ok': False, 'msg': '股数无效'}
        # P3-23 fix: 部分卖出取整百 (A股卖出须整手; 全部卖出不受限可带零股)
        if shares < pos['shares']:
            shares = (shares // 100) * 100
            if shares <= 0: return {'ok': False, 'msg': '部分卖出须整百股'}

        amount = price * shares
        fee = self.sell_fee(amount)
        cost_share = pos['avg_cost']
        pnl = (price - cost_share) * shares - fee

        pos['shares'] -= shares
        # 🔴 卖出时**绝不能**改写 avg_cost（用户 m21973「模拟盘的收益好像有问题」）。原来按「同花顺摊薄口径」
        #   改写：avg = (旧成本×旧股数 − 卖价×卖出股数) / 剩余股数。盈利卖出会把剩余成本一路压低甚至压成
        #   **负数**，后续每笔 pnl 都拿被压低的成本去算 ⇒ 已实现盈亏被重复计入。实测「骏亚科技」11300 股分
        #   6 笔清仓：记录合计 +80223，真实只赚 23374；最后一笔 400 股报 23360（真实 806）。
        # ✅ 现在 avg_cost 一律保持「加权平均买入成本」⇒ 浮动盈亏 = (现价−成本)×股数；
        #   已实现 = (卖价−成本)×股数−费；两者相加 = 账户总盈亏（不重复）。摊薄成本只是券商的展示习惯。
        self.cash += amount - fee
        self.records.append({'time': _ts(self), 'code': code, 'name': pos['name'],
                             'action': '卖出', 'price': round(price, 2),
                             'shares': shares, 'fee': round(fee, 2),
                             'pnl': round(pnl, 2), 'reason': reason,
                             'cash_after': round(self.cash, 2)})
        if pos['shares'] <= 0:
            self.positions.remove(pos)
        self.save()
        return {'ok': True, 'msg': f'卖出 {pos["name"]} {shares}股@{price:.2f} 盈亏{pnl:+.0f}',
                'pnl': round(pnl, 2)}

    # ---------- 纪律检查 ----------
    def check_discipline(self, pos: Dict, price: float, kline: List[Dict]) -> List[Dict]:
        """对单只持仓执行全部纪律检查, 返回触发列表 (价格跌破任一止损线即触发)

        v2.3修复: 止损/吊灯改 intrabar 触发 (当日最低价破线即触发, 海龟原文),
        成交价 = min(当日收盘, 止损线) — 盘中能卖在止损位, 跌停封死时自然按收盘价
        """
        triggers = []
        if not kline or len(kline) < 30: return triggers
        close = kline[-1]['close'] if price <= 0 else price
        low = kline[-1]['low']

        # 1. 破止损 (2N/通道低点, 随加仓上移) — intrabar: 盘中最低破线即触发
        stop = pos.get('stop_loss', 0)
        if low <= stop:
            fill = min(close, stop)  # 跌停封死(close=low)时按收盘价
            triggers.append({'action': 'stop_loss', 'target': '全部卖出',
                             'reason': f'破止损线{stop:.2f}', 'price': fill,
                             'fill_price': fill})

        # 2. 吊灯止损 (重算, 只上移不下移; 破吊灯线即卖, 原版海龟语义, 8/16用户批准删除"须低于买入价"门槛)
        tx = TurtleExecutor(kline, 0)
        ch = tx.calc_chandelier()
        if ch['valid']:
            ch_stop = max(pos.get('chandelier_stop', 0), ch['stop'])
            if low <= ch_stop:
                fill = min(close, ch_stop)
                triggers.append({'action': 'chandelier', 'target': '全部卖出',
                                 'reason': f'破吊灯线{ch_stop:.2f}', 'price': fill,
                                 'fill_price': fill})

        # 3. 0.5N加仓 (≤4单位) — 收盘确认
        # v2.76: 终端已取消加仓 (full_in_out) — 买就是一次买满, 不再有补仓动作
        if not self.full_in_out and pos.get('units', 1) < MAX_UNITS and close >= pos.get('addon_price', 9e9):
            triggers.append({'action': 'addon', 'target': f'加仓1单位({pos.get("units",1)}→{pos.get("units",1)+1})',
                             'reason': f'到加仓位{pos["addon_price"]:.2f}', 'price': close,
                             'fill_price': close})

        # 4. 止盈 (+15%卖1/4) — 收盘确认
        if close >= pos.get('take_profit_at', 9e9):
            if self.full_in_out:
                target = f'全部卖出({pos["shares"]}股)'
            else:
                sell_shares = max(100, pos['shares'] // 4 // 100 * 100)
                target = f'卖1/4({sell_shares}股)'
            triggers.append({'action': 'take_profit', 'target': target,
                             'reason': f'到止盈位{pos["take_profit_at"]:.2f}', 'price': close,
                             'fill_price': close})

        # 5. 时间止损 (10日未突破买入前高点) — 收盘确认
        entry_date = pos.get('entry_date', '')
        entry_idx = -1
        for i, r in enumerate(kline):
            if r['date'] >= entry_date: entry_idx = i; break
        if entry_idx >= 0:
            ts = tx.calc_time_stop(entry_idx, pos.get('time_stop_days', 10))
            if ts['triggered']:
                triggers.append({'action': 'time_stop', 'target': '全部卖出',
                                 'reason': ts['reason'], 'price': close,
                                 'fill_price': close})

        return triggers

    def confirm(self, code: str, action: str, price: float, kline: List[Dict]) -> Dict:
        """确认执行纪律动作"""
        pos = self._find_pos(code)
        if not pos: return {'ok': False, 'msg': '无此持仓'}
        reason_map = {'stop_loss': '止损', 'chandelier': '吊灯止损',
                      'time_stop': '时间止损', 'take_profit': '分批止盈',
                      'addon': '0.5N加仓'}
        label = reason_map.get(action, action)

        if action in ('stop_loss', 'chandelier', 'time_stop'):
            return self.sell(code, price, reason=f'{label}纪律执行')
        if action == 'take_profit':
            # v2.76: 终端全进全出 → 一次全清 (回测仍按海龟卖1/4)
            if self.full_in_out:
                return self.sell(code, price, reason=f'{label}纪律执行')
            sell_shares = max(100, pos['shares'] // 4 // 100 * 100)
            sell_shares = min(sell_shares, pos['shares'])
            r = self.sell(code, price, sell_shares, reason=f'{label}纪律执行')
            # 修复: 止盈位随当前价上移15% — 防止价格在止盈线上方时每天重复触发卖1/4直到卖光
            # (之前take_profit_at是静态值, 688138连续4天止盈卖光 = 盈利端被吃光)
            if r.get('ok') and self._find_pos(code):
                self._find_pos(code)['take_profit_at'] = round(price * 1.15, 2)
            return r
        if action == 'addon':
            # v2.76: 终端已取消加仓 (触发端也不会再返回 addon), 这里兜一道
            if self.full_in_out:
                return {'ok': False, 'msg': '本模拟盘不设加仓 (买即满仓, 全进全出)'}
            n = pos.get('n_atr20', price * 0.03)
            # 加仓后止损 = max(加仓价-2N, 加仓价×0.88) — 硬止损兜底统一
            new_stop = round(max(price - 2 * n, price * 0.88), 2)
            return self.buy(code, pos['name'], price, stop_loss=new_stop,
                            reason='0.5N加仓纪律执行')
        return {'ok': False, 'msg': f'未知动作: {action}'}


# ============================================================
# 历史板块多头判定 (无未来函数)
# 用本地个股K线合成: 板块内成分股(≥5只)中 close>MA21 且 MA21>MA55 占比≥40% = 当日多头
# 替代"当前时点多头名单" — 名单是8/1快照, 直接用于6月回测=未来函数作弊
# ============================================================

def build_sector_bullish(all_closes: Dict, ind_map: Dict, dates: List[str],
                         min_pct: float = 0.40, verbose: bool = False) -> Dict[str, set]:
    """预计算每个回测日的多头行业集合

    每股每日状态: close > MA21 且 MA21 > MA55 (滑动窗口增量计算)
    板块多头: 成分股(≥5只有效K线)中多头占比 ≥ min_pct
    """
    t0 = time.time()
    # 1. 每股每日多头状态
    stocks = {}  # code -> (行业, {date: bool})
    for code, closes in all_closes.items():
        sec = ind_map.get(code[2:], '')
        if not sec: continue
        n = len(closes)
        if n < 70: continue
        states = {}
        # 滑动窗口: 初始计算 MA21/MA55, 之后增量
        ma21 = sum(x[1] for x in closes[:21]) / 21
        ma55 = sum(x[1] for x in closes[:55]) / 55
        for i in range(55, n):
            c = closes[i][1]
            states[closes[i][0]] = (c > ma21 and ma21 > ma55)
            # 窗口滚动
            ma21 = ma21 + (closes[i][1] - closes[i - 21][1]) / 21
            ma55 = ma55 + (closes[i][1] - closes[i - 55][1]) / 55
        stocks[code] = (sec, states)
    if verbose: print(f'  每股状态: {time.time()-t0:.1f}s')

    # 2. 行业成分分组
    sector_codes = {}
    for code, (sec, _) in stocks.items():
        sector_codes.setdefault(sec, []).append(code)

    # 3. 每日每行业多头占比
    result = {}
    for d in dates:
        bull = set()
        for sec, codes in sector_codes.items():
            if len(codes) < 5: continue
            ok = sum(1 for c in codes if stocks[c][1].get(d, False))
            if ok / len(codes) >= min_pct:
                bull.add(sec)
        result[d] = bull
    if verbose: print(f'  板块多头判定: {time.time()-t0:.1f}s 共{len(result)}日')
    return result


# ============================================================
# 用户核心策略回放: 板块趋势跟踪 (自上而下)
# 进场: 行业指数6线均线(5/10/21/55/120/250)刚走出多头排列(昨日非多头今日多头)
#       → 选板块内趋势良好个股(close>MA21>MA55) 且 乖离<10%(低吸) 买入
# 离场: 所属板块指数不再是6线多头排列 → 全部卖出
# 验证目标: 2025-11至今是否"小亏小赚+大赚, 无大亏"
# ============================================================

def simulate_sector_trend(start: str = '2025-11-01', end: str = '2026-07-31',
                          initial_cash: float = 1_000_000, min_stocks: int = 10,
                          max_positions: int = 5, max_per_sector: int = 2,
                          ma_periods=(5, 10, 21, 55, 120, 250),
                          allow_switch: bool = False, progress_callback=None,
                          sector_mode: str = 'or',
                          concept_min_stocks: int = 10) -> Dict:
    # allow_switch: 换仓机制 — 现金不足时卖浮亏最大的持仓换入新板块候选 (模拟实盘调仓)
    # sector_mode: 'or'=行业或概念任一多头即可买 (双轨, 默认) / 'and'=行业+概念双满足才买 (2026-08-04)
    # concept_min_stocks: 概念质量过滤 — 成分股≥N 才参与多头判定 (默认10; 20=过滤小概念噪声)
    """板块趋势策略回放: 板块6线多头进场, 板块多头破坏离场"""
    from collections import defaultdict as dd
    t0 = time.time()
    all_closes, all_ohlcv = preload_all(verbose=False)
    ind_map, _ = load_industry_concept_maps()
    start, end = _norm_date(start), _norm_date(end)
    all_dates = sorted(set(d for closes in all_closes.values() for d, _ in closes))
    bt_dates = [d for d in all_dates if start <= d <= end]
    if not bt_dates:
        return {'error': f'日期区间无数据: {start}~{end}'}

    # ---- 1. 板块指数合成 (成分股≥min_stocks, 价格等权 — 2026-08-04用户定: 回放保持价格等权) ----
    # 说明: 涨幅等权(同花顺口径)回放 -14.29% vs 价格等权 -0.29% — 策略验证用价格等权
    sector_stocks = dd(list)
    for code, closes in all_closes.items():
        sec = ind_map.get(code[2:], '')
        if sec: sector_stocks[sec].append((code, closes))
    sector_idx = {}   # {行业: [(date, close)]}
    for sec, lst in sector_stocks.items():
        if len(lst) < min_stocks: continue
        date_map = dd(list)
        for code, closes in lst:
            for d, c in closes:
                date_map[d].append(c)
        idx = []
        for d in sorted(date_map):
            vals = date_map[d]
            if len(vals) < min_stocks * 0.5: continue
            idx.append((d, sum(vals) / len(vals)))
        if len(idx) < 260: continue  # 需250日MA
        sector_idx[sec] = idx
    if progress_callback: progress_callback(0, len(sector_idx), 0)  # nav参数必须数值, 进度条格式化用
    print(f'  板块指数: {len(sector_idx)} 个 (成分股≥{min_stocks})')

    # ---- 2. 每日板块6线多头状态 (行业 + 概念双轨) ----
    def _sma(vals, p):
        r = [None] * len(vals)
        for i in range(len(vals)):
            if i >= p - 1:
                r[i] = sum(vals[i - p + 1:i + 1]) / p
        return r

    def _calc_states(idx):
        closes = [c for _, c in idx]
        mas = {p: _sma(closes, p) for p in ma_periods}
        states = {}
        for i in range(max(ma_periods) - 1, len(closes)):
            c = closes[i]
            ok = all(mas[p][i] is not None for p in ma_periods)
            if ok:
                seq = [c] + [mas[p][i] for p in ma_periods]
                ok = all(seq[j] > seq[j + 1] for j in range(len(seq) - 1))
            states[idx[i][0]] = ok
        return states

    sector_state = {sec: _calc_states(idx) for sec, idx in sector_idx.items()}
    # 概念板块 (v2.3概念双轨: 行业 OR 概念任一多头即可 — 用户实盘口径, 先进封装6/12已多头案例)
    _, con_map = load_industry_concept_maps()
    concept_stocks = dd(list)
    for code, cons in con_map.items():
        for c in cons.split('|'):
            c = c.strip()
            if c: concept_stocks[c].append(code)
    concept_idx = {}
    for con, codes in concept_stocks.items():
        # P3-22 fix: 前缀映射含北交所(8/4/920) — 原来8xx/4xx被误归SZ (本地暂无北交所数据, 会被下方过滤, 但映射须正确)
        pcodes = [('BJ' + x if x.startswith(('4', '8', '92')) else ('SH' + x if x.startswith('6') else 'SZ' + x)) for x in codes]
        pcodes = [x for x in pcodes if x in all_closes]
        if len(pcodes) < concept_min_stocks: continue  # 概念质量过滤 (2026-08-04)
        date_map = dd(list)
        for pc in pcodes:
            for d, px in all_closes[pc]:
                date_map[d].append(px)
        idx = []
        for d in sorted(date_map):
            vals = date_map[d]
            if len(vals) < concept_min_stocks * 0.5: continue
            idx.append((d, sum(vals) / len(vals)))
        if len(idx) < 260: continue
        concept_idx[con] = idx
    concept_state = {con: _calc_states(idx) for con, idx in concept_idx.items()}
    # 概念归属: code -> 概念列表 (买入时用)
    code_concepts = {}
    for code, cons in con_map.items():
        lst = [c.strip() for c in cons.split('|') if c.strip() and c.strip() in concept_state]
        if lst: code_concepts[code] = lst
    print(f'  板块多头状态: 行业{len(sector_state)} + 概念{len(concept_state)} 计算完成')

    # ---- 3. 回放 ----
    cash = initial_cash
    positions = []  # {code,name,sector,shares,avg_cost,entry_date}
    records = []
    daily_nav = []
    total_days = len(bt_dates)

    def _sell(pos, price, reason):
        nonlocal cash
        amount = price * pos['shares']
        fee = SimAccount.sell_fee(amount)
        pnl = (price - pos['avg_cost']) * pos['shares'] - fee
        cash += amount - fee
        records.append({'time': bt_date, 'code': pos['code'], 'name': pos['name'],
                        'action': '卖出', 'price': round(price, 2), 'shares': pos['shares'],
                        'pnl': round(pnl, 2), 'reason': reason, 'sector': pos['sector'],
                        'fee': round(fee, 2)})
        positions.remove(pos)

    def _buy(code, name, price, sec, shares):
        nonlocal cash
        amount = price * shares
        fee = SimAccount.buy_fee(amount)
        if amount + fee > cash: return
        cash -= amount + fee
        positions.append({'code': code, 'name': name, 'sector': sec,
                          'shares': shares, 'avg_cost': round((amount + fee) / shares, 3),
                          'entry_date': bt_date,
                          'concepts': code_concepts.get(code[2:], [])})
        records.append({'time': bt_date, 'code': code, 'name': name, 'action': '买入',
                        'price': round(price, 2), 'shares': shares,
                        'reason': f'板块启动[{sec}]', 'sector': sec, 'fee': round(fee, 2)})

    def _price_now(pos):
        kl = slice_kline(all_ohlcv, pos['code'], bt_date)
        return kl[-1]['close'] if kl else pos['avg_cost']

    def _sector_ok(pos, d):
        """行业 OR 概念 任一6线多头 (概念双轨)"""
        if sector_state.get(pos['sector'], {}).get(d, False): return True
        return any(concept_state.get(c, {}).get(d, False) for c in pos.get('concepts', []))

    for di, bt_date in enumerate(bt_dates):
        # ① 离场: 行业和所有概念都非6线多头 → 全部卖出 (概念双轨)
        for pos in list(positions):
            kline = slice_kline(all_ohlcv, pos['code'], bt_date)
            if not _sector_ok(pos, bt_date):
                if kline:
                    _sell(pos, kline[-1]['close'], f'板块[{pos["sector"]}]6线多头破坏离场')

        # ② 进场: 板块刚走出多头排列(今日多头且昨日非多头) — 行业+概念双轨
        # 持仓满时若开启换仓仍扫描 (换仓可腾资金)
        if len(positions) < max_positions or (allow_switch and positions):
            prev_date = bt_dates[di - 1] if di > 0 else bt_date
            candidates = []
            seen = set()
            for sec, states in sector_state.items():
                if not states.get(bt_date, False): continue
                if states.get(prev_date, False): continue  # 昨日已多头=不是"刚走出"
                # 该板块成分股: 个股 close>MA21>MA55 且 乖离<10%
                for code, closes in sector_stocks.get(sec, []):
                    if code in seen or code not in all_closes: continue
                    if sector_mode == 'and':
                        # 双满足: 个股需有至少一个概念当日6线多头
                        if not any(concept_state.get(cn, {}).get(bt_date, False)
                                   for cn in code_concepts.get(code[2:], [])): continue
                    arr = [c for d, c in closes if d <= bt_date]
                    if len(arr) < 60: continue
                    c = arr[-1]
                    ma21 = sum(arr[-21:]) / 21
                    ma55 = sum(arr[-55:]) / 55
                    if not (c > ma21 > ma55): continue
                    bias = (c - ma21) / ma21 * 100
                    if bias > 10: continue  # 低吸: 乖离≤10%
                    # 个股趋势完整度: 6线多头=3, 4线=2, 基础=1
                    strength = 1
                    ma5 = sum(arr[-5:]) / 5
                    ma10 = sum(arr[-10:]) / 10
                    if c > ma5 > ma10: strength += 1
                    if len(arr) >= 250:
                        ma120 = sum(arr[-120:]) / 120
                        ma250 = sum(arr[-250:]) / 250
                        if c > ma120 > ma250: strength += 1
                    candidates.append((strength, -bias, code, c, sec))
                    seen.add(code)
            # 概念板块启动 (先进封装6/12多头案例: 行业未启动但概念启动也可买)
            for con, states in concept_state.items():
                if not states.get(bt_date, False): continue
                if states.get(prev_date, False): continue
                for x in concept_stocks.get(con, []):
                    code = ('SH' + x if x.startswith('6') else 'SZ' + x)
                    if code in seen or code not in all_closes: continue
                    if sector_mode == 'and':
                        # 双满足: 个股所属行业需当日6线多头
                        if not sector_state.get(ind_map.get(x, ''), {}).get(bt_date, False): continue
                    closes = all_closes[code]
                    arr = [c for d, c in closes if d <= bt_date]
                    if len(arr) < 60: continue
                    c = arr[-1]
                    ma21 = sum(arr[-21:]) / 21
                    ma55 = sum(arr[-55:]) / 55
                    if not (c > ma21 > ma55): continue
                    bias = (c - ma21) / ma21 * 100
                    if bias > 10: continue
                    strength = 1
                    ma5 = sum(arr[-5:]) / 5
                    ma10 = sum(arr[-10:]) / 10
                    if c > ma5 > ma10: strength += 1
                    if len(arr) >= 250:
                        ma120 = sum(arr[-120:]) / 120
                        ma250 = sum(arr[-250:]) / 250
                        if c > ma120 > ma250: strength += 1
                    candidates.append((strength, -bias, code, c, con))
                    seen.add(code)
            candidates.sort(key=lambda x: (-x[0], x[1]))
            sec_count = {}
            switch_left = 2  # 每轮最多换2只
            for strength, negbias, code, price, sec in candidates:
                if len(positions) >= max_positions and not allow_switch: break
                if any(p['code'] == code for p in positions): continue
                if sec_count.get(sec, 0) >= max_per_sector: continue
                # 现金底线20%铁律: 无换仓时持仓市值≥80%不再买入 (换仓本身在调仓, 不限制)
                if not allow_switch:
                    nav_now = cash + sum(p['shares'] * _price_now(p) for p in positions)
                    if nav_now > 0:
                        held_pct = sum(p['shares'] * _price_now(p) for p in positions) / nav_now
                        if held_pct >= 0.80: break
                # (2026-08-16 修复: 净值按市价 _price_now 估值, 原用 avg_cost 成本价→
                #  期末持仓浮盈浮亏不计入收益/回撤, 回放结论是"已实现盈亏口径"失真)
                nav = cash + sum(p['shares'] * _price_now(p) for p in positions)
                shares = int(nav * 0.20 / price / 100) * 100
                if shares < 100: continue
                amount = price * shares + SimAccount.buy_fee(price * shares)
                # 换仓: 现金不足时卖"持有≥5天且盈利<15%"的最弱持仓 (滚动止盈换仓, 模拟实盘调仓)
                if amount > cash and allow_switch and positions and switch_left > 0:
                    losers = []
                    for p in positions:
                        d_held = bt_dates.index(bt_date) - bt_dates.index(p['entry_date']) if p['entry_date'] in bt_dates else 99
                        pnl_pct = (_price_now(p) - p['avg_cost']) / p['avg_cost'] * 100
                        if d_held >= 5 and pnl_pct < 15:
                            losers.append((pnl_pct, d_held, p))
                    losers.sort(key=lambda x: x[0])
                    if losers:
                        weakest = losers[0][2]
                        w_price = _price_now(weakest)
                        _sell(weakest, w_price, f'换仓卖出(弱持仓)买入[{sec}]')
                        switch_left -= 1
                        nav = cash + sum(p['shares'] * _price_now(p) for p in positions)
                        shares = int(nav * 0.20 / price / 100) * 100
                        if shares < 100: continue
                        amount = price * shares + SimAccount.buy_fee(price * shares)
                if amount > cash: continue
                _buy(code, code[2:], price, sec, shares)
                sec_count[sec] = sec_count.get(sec, 0) + 1

        # ③ 净值 (市价口径)
        nav = cash + sum(p['shares'] * _price_now(p) for p in positions)
        daily_nav.append({'date': bt_date, 'nav': round(nav, 2),
                          'cash': round(cash, 2), 'pos_count': len(positions)})
        if progress_callback: progress_callback(di + 1, total_days, nav)

    stats = _calc_stats(daily_nav, records, initial_cash)
    stats['duration_sec'] = round(time.time() - t0, 1)
    stats['daily_nav'] = daily_nav
    stats['trades'] = records
    # 分板块盈亏统计
    sec_pnl = dd(lambda: {'pnl': 0, 'trades': 0, 'wins': 0})
    for t in records:
        if t.get('pnl') is not None:
            sec = t.get('sector', '')
            sec_pnl[sec]['pnl'] += t['pnl']
            sec_pnl[sec]['trades'] += 1
            if t['pnl'] > 0: sec_pnl[sec]['wins'] += 1
    stats['sector_pnl'] = {k: dict(v) for k, v in sorted(sec_pnl.items(), key=lambda x: -x[1]['pnl'])}
    return stats


# ============================================================
# 历史回放 (逐日自动执行纪律)
# ============================================================

def _norm_date(s: str) -> str:
    """日期标准化: 2026-6-01 / 2026-07-1 → 2026-06-01"""
    if not s: return s
    parts = str(s).strip().split('-')
    if len(parts) == 3:
        try:
            return '%04d-%02d-%02d' % (int(parts[0]), int(parts[1]), int(parts[2]))
        except ValueError:
            pass
    return str(s).strip()


def simulate_history(start: str, end: str, mode: str = '20',
                     min_score: int = 13, initial_cash: float = 1_000_000,
                     market_filter: int = 0, addon_step: float = 0.5,
                     sector_filter: bool = False, buy_mode: str = 'both',
                     progress_callback=None) -> Dict:
    # buy_mode: 'both'=突破+回踩(现状) | 'pullback'=只买回踩确认(买点模式实验)
    """
    历史回放: 对指定日期区间逐日自动执行纪律并自动买入信号

    每日顺序:
      ① 对持仓执行纪律 (破止损/吊灯/时间止损→卖出; 0.5N→加仓; 止盈→卖1/4; 用当日收盘价)
      ② 当日扫描信号 (scan_single kline_override) 评分≥min_score → 买入1单位
      ③ 记录当日净值
    """
    t0 = time.time()
    all_closes, all_ohlcv = preload_all(verbose=False)

    # 日期标准化 (容错 2026-6-01 / 2026-07-1 等不规范输入)
    start = _norm_date(start)
    end = _norm_date(end)

    # 确定回测日期
    all_dates_set = set()
    for closes in all_closes.values():
        for d, _ in closes: all_dates_set.add(d)
    all_dates_sorted = sorted(all_dates_set)
    bt_dates = [d for d in all_dates_sorted if start <= d <= end]
    if not bt_dates:
        return {'error': f'日期区间无数据: {start}~{end} (可用{all_dates_sorted[0]}~{all_dates_sorted[-1]})'}

    # 回放用纯内存账户 (persist=False): 不读写 sim_state.json, 不污染模拟盘
    # v2.76: 回测**显式**关掉全进全出 → 保持海龟原版语义 (0.5N加仓 / 止盈卖1/4)。
    #   终端模拟盘改规矩**不应**顺带改掉这里的回测结论, 所以这里钉死 False。
    acc = SimAccount(initial_cash=initial_cash, persist=False, addon_step=addon_step,
                     full_in_out=False)

    # v2.3板块铁律: 历史板块多头判定 (无未来函数, 用本地K线合成)
    sector_bull = {}
    if sector_filter:
        ind_map, _ = load_industry_concept_maps()
        sector_bull = build_sector_bullish(all_closes, ind_map, bt_dates)

    engine = QuantEngine()
    engine.ind_map, engine.con_map = load_industry_concept_maps()
    # P3-4 fix: 原 load_data(mode) 白加载选股池文件1-2秒 — 回放池来自build_pool_for_date, scan_single走kline_override不读pool
    # 回测无实时板块数据 → sector_returns 留空, AShareVerifier 会兜底用个股20日涨幅

    daily_nav = []
    total_days = len(bt_dates)

    for di, target_date in enumerate(bt_dates):
        acc.trade_date = target_date  # 交易记录用回放日期
        prices_today = {}  # 当日收盘价
        pos_codes = [p['code'] for p in acc.positions]

        # ① 对持仓执行纪律 (止损/吊灯intrabar触发, 按止损线成交)
        for code in list(pos_codes):
            pos = acc._find_pos(code)
            if not pos: continue
            kline = slice_kline(all_ohlcv, code, target_date)
            if not kline or len(kline) < 30: continue
            close = kline[-1]['close']
            prices_today[code] = close
            triggers = acc.check_discipline(pos, close, kline)
            for tr in triggers:
                acc.confirm(code, tr['action'], tr.get('fill_price', close), kline)

        # ② 当日扫描信号 → 自动买入
        # v2.3修复: 评分降序选最强 + 只买完成信号(BREAKOUT/PULLBACK) + 同行业≤2只
        # v2.3市场过滤: market_filter>0 时, 全市场多头排列池子<阈值 → 当天不新开仓
        # (代理大盘状态: 池子=MA多头排列票数, 6月上半月萎缩到368=震荡期, 过滤假突破磨损)
        if len(acc.positions) < MAX_POSITIONS:
            pool = build_pool_for_date(all_closes, target_date)
            if market_filter > 0 and len(pool) < market_filter:
                pool = []  # 市场温度低 → 不新开仓 (持仓纪律照常执行)
            candidates = []
            for code in pool:
                kline = slice_kline(all_ohlcv, code, target_date)
                if len(kline) < 60: continue
                close = kline[-1]['close']
                prev = kline[-2]['close'] if len(kline) > 1 else close
                chg = (close - prev) / prev * 100 if prev > 0 else 0
                info = {'code': code, 'name': code, 'sector': '', 'concept': ''}
                num = code[2:] if code.startswith(('SZ','SH')) else code
                info['sector'] = engine.ind_map.get(num, '')
                info['concept'] = engine.con_map.get(num, '')
                try:
                    r = engine.scan_single(code, info, close, chg, mode=mode,
                                           kline_override=kline)
                except Exception:
                    continue
                if not r.get('is_valid', True): continue
                if r['consensus']['score'] < min_score: continue
                if buy_mode == 'pullback':
                    # 买点模式: 突破后回踩左峰±3%区间 (近10日曾收盘≥左峰 + 今日收盘∈[0.97,1.03]×左峰)
                    ch_high = r['breakout']['channel']['left_peak']
                    if ch_high <= 0: continue
                    closes_arr = [x['close'] for x in kline]
                    broke = any(closes_arr[i] >= ch_high * 0.995 for i in range(-11, -1))
                    in_zone = ch_high * 0.97 <= close <= ch_high * 1.03
                    if not (broke and in_zone): continue
                else:
                    # 只买完成信号: 突破/回踩 (NEAR_HIGH=未突破, 属观察不属买点)
                    if r['consensus']['signal'] not in ('BREAKOUT', 'PULLBACK'): continue
                # 板块铁律: 个股所属行业为多头 — 用前一交易日状态 (严谨版: 实盘只能看T-1收盘)
                if sector_filter:
                    sec = r.get('sector', '')
                    prev_date = bt_dates[di - 1] if di > 0 else target_date
                    if sec and sec not in sector_bull.get(prev_date, set()):
                        continue
                # 完整保存买入信息 (close/num 不能复用循环残留变量!)
                candidates.append({'score': r['consensus']['score'], 'code': code,
                                   'num': num, 'close': close, 'r': r})
            candidates.sort(key=lambda x: -x['score'])  # 评分降序, 最强优先
            for cand in candidates:
                if len(acc.positions) >= MAX_POSITIONS: break
                code, num, close, r = cand['code'], cand['num'], cand['close'], cand['r']
                if acc._find_pos(code): continue
                # 铁律: 同方向≤2只
                sec = r.get('sector', '')
                if sec:
                    same_sec = sum(1 for p in acc.positions if p.get('sector') == sec)
                    if same_sec >= 2: continue
                disc = r.get('discipline', {})
                sl = r['tx_execute']['stop_loss']['stop_loss']
                shares = acc.suggest_units(close, sl)
                acc.buy(code, num, close, shares=shares,
                        stop_loss=sl,
                        addon_price=disc.get('addon_price', 0),
                        take_profit_at=disc.get('take_profit_at', 0),
                        chandelier_stop=disc.get('chandelier_stop', 0),
                        n_atr20=r['tx_execute']['stop_loss'].get('n_atr20', 0),
                        sector=sec,
                        reason=f'{mode}日突破信号{cand["score"]}分')

        # ③ 当日净值
        nav = acc.nav(prices_today)
        daily_nav.append({'date': target_date, 'nav': round(nav, 2),
                          'cash': round(acc.cash, 2), 'pos_count': len(acc.positions)})
        if progress_callback: progress_callback(di + 1, total_days, nav)

    # 统计
    stats = _calc_stats(daily_nav, acc.records, initial_cash)
    stats['duration_sec'] = round(time.time() - t0, 1)
    stats['daily_nav'] = daily_nav
    stats['trades'] = acc.records
    return stats


def _calc_stats(daily_nav: List[Dict], trades: List[Dict],
                initial_cash: float) -> Dict:
    if not daily_nav:
        return {'error': '无净值数据'}
    start_nav = daily_nav[0]['nav']
    end_nav = daily_nav[-1]['nav']
    total_ret = (end_nav - initial_cash) / initial_cash * 100

    # 最大回撤
    peak = daily_nav[0]['nav']
    max_dd = 0.0
    for d in daily_nav:
        peak = max(peak, d['nav'])
        dd = (peak - d['nav']) / peak * 100
        max_dd = max(max_dd, dd)

    # 交易统计
    sells = [t for t in trades if t.get('pnl') is not None]
    wins = [t for t in sells if t['pnl'] > 0]
    losses = [t for t in sells if t['pnl'] < 0]
    win_rate = len(wins) / len(sells) * 100 if sells else 0
    avg_win = sum(t['pnl'] for t in wins) / len(wins) if wins else 0
    avg_loss = abs(sum(t['pnl'] for t in losses)) / len(losses) if losses else 0
    pl_ratio = avg_win / avg_loss if avg_loss > 0 else 0
    buy_count = len([t for t in trades if t['action'] in ('买入', '加仓')])

    return {
        'start_nav': round(start_nav, 2), 'end_nav': round(end_nav, 2),
        'total_ret_pct': round(total_ret, 2), 'max_drawdown_pct': round(max_dd, 2),
        'trade_count': len(trades), 'sell_count': len(sells),
        'win_rate_pct': round(win_rate, 1), 'avg_win': round(avg_win, 0),
        'avg_loss': round(avg_loss, 0), 'pl_ratio': round(pl_ratio, 2),
        'buy_count': buy_count, 'days': len(daily_nav),
    }


# ═══ 模拟盘历史收益 — 逐日净值结算 + 落盘缓存 ═══
# 权威数据 = sim_state.json 的 records(全部成交，带 time/cash_after)；sim_hist.json 只是「算到哪天」的
#   结算快照，可整删重建(幂等)。
# 规则：· 交易日历 = 本地上证指数日K(index_daily/SH000001.csv，收盘确认后才入库)
#   · 当日 15:00 前成交 → 计入当日；收盘后/节假日成交 → 归属下一交易日
#   · 每日净值 = 当日收盘后现金 + Σ(持仓股数 × 当日收盘价)(个股无K线 → 最近收盘；无文件 → 最近成交价)
#   · 当日盈亏 = 净值(d) − 净值(d−1)；账户首日基线 = 初始资金
#   · 基准对比 = 上证/深成指/创业板收盘区间涨跌(首日的前一交易日为基期)
# 增量：缓存带现金/持仓/已应用记录数，日历或记录有新增才向后推进；缓存缺失/不一致 → 全量重建。

def _hist_read() -> Optional[Dict]:
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception:
        pass
    return None


def _hist_write(h: Dict):
    try:
        tmp = HISTORY_FILE + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(h, f, ensure_ascii=False, indent=1)
        os.replace(tmp, HISTORY_FILE)
    except Exception:
        pass


def clear_sim_history_cache():
    """账户重置联动: 删除历史收益缓存 (记录已清空则历史无意义; 下次开历史自动重建)"""
    for p in (HISTORY_FILE, HISTORY_FILE + '.tmp'):
        try:
            if os.path.exists(p):
                os.remove(p)
        except Exception:
            pass


def _settle_day_idx(rec_time: str, cal: List[str]) -> Optional[int]:
    """一条交易记录归属的交易日 (cal 下标): 当天15:00前→当天, 收盘后/节假日→下一交易日;
    日历外(未来) → None 留待下一轮"""
    d = rec_time[:10]
    t = rec_time[11:19] if len(rec_time) > 10 else ''
    i = bisect.bisect_left(cal, d)
    if i >= len(cal):
        return None
    if cal[i] == d and t > '15:00:00':
        i += 1
        if i >= len(cal):
            return None
    return i


def get_sim_history(acc: 'SimAccount') -> Dict:
    """结算并返回模拟盘历史收益 (结果落盘 sim_hist.json):
    {ok, initial_cash, days:[{d,nav,pnl}], bench:{sh/sz/cyb:[区间涨跌%...]},
     win:{first,as_of,nav_last,total_pnl,total_pct,sh_pct,sz_pct,cyb_pct}, pending, updated_at}"""
    records = list(acc.records or [])
    initial = float(acc.initial_cash or 1_000_000)
    if not records:
        return {'ok': True, 'initial_cash': initial, 'days': [], 'bench': {},
                'win': None, 'pending': 0, 'updated_at': ''}

    cal_rows = read_index_daily('sh000001')
    cal = [r['date'] for r in cal_rows] if cal_rows else []
    if not cal:
        return {'ok': False, 'error': '本地缺上证指数日K (index_daily/SH000001.csv), 无法结算历史收益'}
    # 防御: 收盘确认前 index_daily 不应含当日行; 万一混入占位bar则剔除
    today = datetime.now().strftime('%Y-%m-%d')
    if cal[-1] == today and datetime.now().strftime('%H:%M:%S') < '15:06:00':
        cal = cal[:-1]
        if not cal:
            return {'ok': False, 'error': '交易日历为空'}
    end_i = len(cal) - 1

    # 各记录归属日 (日历外 → None, 留待下一轮结算)
    sidx = [(_settle_day_idx(str(r.get('time', '')), cal)) for r in records]

    # ---- 缓存有效性判定: 通过 → 增量推进; 否则全量重建 ----
    h = _hist_read()
    ok_cached = (h and h.get('schema') == HISTORY_SCHEMA
                 and abs(float(h.get('initial_cash', -1)) - initial) < 0.01
                 and isinstance(h.get('as_of_i'), int) and 0 <= h.get('as_of_i', -1) <= end_i
                 and h.get('as_of') == cal[h.get('as_of_i')]
                 and isinstance(h.get('rec_count'), int) and 0 <= h.get('rec_count') <= len(records))
    if ok_cached:
        k = int(h['as_of_i'])
        # 新记录时间倒挂落到已结算区 → 缓存状态不可信, 回退全量重放
        for j in range(int(h['rec_count']), len(records)):
            if sidx[j] is not None and sidx[j] <= k:
                ok_cached = False
                break
    if ok_cached and int(h['as_of_i']) == end_i:
        # v2.75 (2026-09-19) 自愈: 「结算跑在个股日K落盘之前」
        # 实测 9/18 —— sim_hist 15:37:31 就结完了, 而 5 只持仓的 CSV 到 15:37:33~53 才写入当日 bar;
        # 22 秒之差让 _close_on 全部**静默退回前一交易日收盘**, 当日收益记成 14278 (真值 34960),
        # 且 as_of_i 已 == end_i ⇒ 下面 1001 行短路, 那一行永不重算, 误差还顺延污染后续所有基线。
        # ⇒ 结算时记下实际取价的那根 bar 日期; 现在若 CSV 已补上 as_of 当日 bar ⇒ 缓存不可信, 回退全量重建。
        _as_of = str(h.get('as_of') or '')
        _pd = h.get('px_day') or {}
        for _c, _sh in (h.get('pos') or {}).items():
            if float(_sh) <= 0:
                continue
            _rows = load_csv_kline(str(_c))
            # 只在「当时取价早于 as_of」且「CSV 现在已有 as_of 当日bar」时判陈旧 ——
            # 反向条件可避免盘中 bar (快照更新写的今日行) 触发无限重建
            if _rows and str(_rows[-1]['date']) >= _as_of and str(_pd.get(str(_c), '')) < _as_of:
                ok_cached = False
                break
    if not ok_cached:
        k, cash, pos, last_px, px_day, days, applied = -1, initial, {}, {}, {}, [], 0
    else:
        k = int(h['as_of_i'])
        cash = float(h.get('cash', initial))
        pos = {str(c): float(sh) for c, sh in (h.get('pos') or {}).items()}
        last_px = {str(c): float(p) for c, p in (h.get('last_px') or {}).items()}
        px_day = {str(c): str(v) for c, v in (h.get('px_day') or {}).items()}
        days = list(h.get('days') or [])
        applied = int(h['rec_count'])

    made = 0
    if not (k == end_i and applied >= len(records)):
        # ---- 逐日推进到日历尽头 ----
        # 所需个股K线 = 当前持仓 ∪ 剩余记录涉及代码 (一次读齐, 逐日二分取价)
        need = set(pos) | {str(r.get('code', '')) for r in records[applied:] if r.get('code')}
        klines = {}
        for c in sorted(need):
            rows = load_csv_kline(c)
            if rows:
                klines[c] = ([x['date'] for x in rows], [x['close'] for x in rows])

        def _close_on(code, d):
            """→ (收盘价, 该价所属bar日期); 无本地K线 → (None, '')"""
            arr = klines.get(code)
            if not arr:
                return None, ''
            i = bisect.bisect_right(arr[0], d) - 1
            return (arr[1][i], arr[0][i]) if i >= 0 else (None, '')

        if not days:
            # 全量重建: 首条有效记录所属交易日才开记 (此前无持仓, 净值恒=初始资金)
            first = next((s for s in sidx if s is not None), None)
            if first is None:
                return {'ok': True, 'initial_cash': initial, 'days': [],
                        'bench': {}, 'win': None, 'pending': len(records), 'updated_at': ''}
        else:
            first = None
        for di in range(max(k + 1, first or -1), end_i + 1):
            d = cal[di]
            # ① 当日成交 → 现金/持仓 (cash_after 权威; 极老记录无该字段则按成交额推算)
            for j in range(applied, len(records)):
                if sidx[j] != di:
                    continue
                r = records[j]
                code = str(r.get('code') or '')
                action = r.get('action') or ''
                px = float(r.get('price') or 0)
                sh = float(r.get('shares') or 0)
                if code and px > 0:
                    last_px[code] = px
                if action in ('买入', '加仓'):
                    pos[code] = pos.get(code, 0) + sh
                elif action == '卖出':
                    pos[code] = max(0.0, pos.get(code, 0) - sh)
                ca = r.get('cash_after')
                if ca is not None:
                    cash = float(ca)
                else:
                    fee = float(r.get('fee') or 0)
                    amt = px * sh
                    if action in ('买入', '加仓'):
                        cash -= amt + fee
                    elif action == '卖出':
                        cash += amt - fee
                applied = j + 1
            # ② 当日收盘估值
            v = cash
            for code, sh in pos.items():
                if sh <= 0:
                    continue
                px, pday = _close_on(code, d)
                if px is None or px <= 0:
                    px, pday = last_px.get(code, 0), ''  # 无本地K线 → 最近成交价兜底
                if px > 0:
                    last_px[code] = px
                    if pday:
                        px_day[code] = pday  # v2.75: 记下实际取价的bar日期, 供"结算早于K线落盘"自愈核对
                    v += sh * px
            prev = days[-1]['nav'] if days else initial
            days.append({'d': d, 'nav': round(v, 2), 'pnl': round(v - prev, 2)})
            made += 1
        if made:
            _hist_write({'schema': HISTORY_SCHEMA, 'initial_cash': initial,
                         'as_of': cal[end_i], 'as_of_i': end_i,
                         'cash': round(cash, 2),
                         'pos': {c: sh for c, sh in pos.items() if sh > 0},
                         'last_px': {c: round(p, 4) for c, p in last_px.items()},
                         'px_day': px_day,
                         'rec_count': applied, 'days': days, 'updated_at': _now_str()})

    # ---- 基准对比: 上证/深成指/创业板 (窗口基期 = 首日的前一交易日收盘) ----
    # v2.74 (2026-09-12): 另返回 calendar = 交易日日历 (首日所在月起 → 今), 供前端日历收益图
    # 判「交易日/休市」(周末 + 节假日); 只在有数据的月份区间内翻月, 故从首月给起即可
    bench, win, cal_win = {}, None, []
    if days:
        d0 = days[0]['d']
        cal_win = [d for d in cal if d >= d0[:7] + '-01']
        idx_close = {}
        # v2.75 (2026-09-12): 加科创综指 sh000680 (曲线对比 5 线)
        for code, key in (('sh000001', 'sh'), ('sz399001', 'sz'), ('sz399006', 'cyb'),
                          ('sh000680', 'kc')):
            rows = read_index_daily(code)
            idx_close[key] = {r['date']: r['close'] for r in rows} if rows else {}
        bi = bisect.bisect_left(cal, d0) - 1
        base_d = cal[bi] if bi >= 0 else None
        for key in ('sh', 'sz', 'cyb', 'kc'):
            mp = idx_close.get(key) or {}
            b0 = mp.get(base_d, 0) if base_d else 0
            sers, last_c = [], b0
            for x in days:
                c = mp.get(x['d'], last_c) or last_c  # 缺日沿用前一值
                sers.append(round((c / b0 - 1) * 100, 2) if b0 else None)
                last_c = c
            bench[key] = sers
        total_pnl = round(days[-1]['nav'] - initial, 2)
        # v2.75 (2026-09-12): base_d = 区间基准的基期日 = 首日「前一交易日」收盘
        # (区间基准涨跌% = 基期收盘 → 最新收盘; 前端标「区间基准 (base_d → as_of)」说清楚口径)
        win = {'first': days[0]['d'], 'as_of': days[-1]['d'], 'base_d': base_d, 'nav_last': days[-1]['nav'],
               'total_pnl': total_pnl,
               'total_pct': round(total_pnl / initial * 100, 2) if initial else 0.0,
               'sh_pct': bench.get('sh', [None])[-1],
               'sz_pct': bench.get('sz', [None])[-1],
               'cyb_pct': bench.get('cyb', [None])[-1],
               'kc_pct': bench.get('kc', [None])[-1]}
    return {'ok': True, 'initial_cash': initial, 'days': days, 'bench': bench,
            'win': win, 'calendar': cal_win, 'pending': len(records) - applied,
            'updated_at': (h or {}).get('updated_at', '') if not made else _now_str()}


if __name__ == '__main__':
    # 命令行测试: python -B quant_terminal/simulator.py 2026-05-01 2026-07-31
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('start', nargs='?', default='')
    p.add_argument('end', nargs='?', default='')
    p.add_argument('--mode', default='20')
    p.add_argument('--min-score', type=int, default=13)
    p.add_argument('--history', action='store_true',
                   help='v2.65: 结算并打印模拟盘历史收益 (读 sim_state.json, 落盘 sim_hist.json)')
    a = p.parse_args()
    if a.history:
        acc = SimAccount()
        r = get_sim_history(acc)
        if not r.get('ok'):
            print('错误:', r.get('error')); sys.exit(1)
        days, w = r.get('days') or [], r.get('win')
        print(f'模拟盘历史收益: {len(days)} 个交易日' +
              (f'  (截至 {w["as_of"]}, 待计入 {r.get("pending", 0)} 笔)' if days else ' (无记录)'))
        if w:
            sum_pnl = sum(x['pnl'] for x in days)
            print(f'  首日 {w["first"]} → 末日 {w["as_of"]}  账户资产 {w["nav_last"]:,.2f}')
            print(f'  累计收益 {w["total_pnl"]:+,.2f} ({w["total_pct"]:+.2f}%)   Σ当日盈亏 {sum_pnl:+,.2f}  校验差 {w["total_pnl"] - sum_pnl:+.2f}')
            print(f'  基准区间: 上证 {w["sh_pct"]:+.2f}%  深成指 {w["sz_pct"]:+.2f}%  创业板 {w["cyb_pct"]:+.2f}%')
            print(f'  落盘: {HISTORY_FILE}')
        sys.exit(0)
    if not (a.start and a.end):
        print('用法: python -B quant_terminal/simulator.py 2026-05-01 2026-07-31 [--mode 20] [--min-score 13]')
        sys.exit(1)
    print(f'历史回放: {a.start}~{a.end} mode={a.mode} min_score={a.min_score}')
    result = simulate_history(a.start, a.end, mode=a.mode, min_score=a.min_score)
    if 'error' in result:
        print(f'错误: {result["error"]}'); sys.exit(1)
    print(f'  初始资金: 1,000,000  期末净值: {result["end_nav"]:,.0f} '
          f'总收益: {result["total_ret_pct"]:+.2f}%  最大回撤: {result["max_drawdown_pct"]:.2f}%')
    print(f'  交易{result["trade_count"]}笔(买{result["buy_count"]}) 卖出{result["sell_count"]} 胜率{result["win_rate_pct"]:.0f}% '
          f'盈亏比{result["pl_ratio"]:.2f} 用时{result["duration_sec"]:.0f}s')
