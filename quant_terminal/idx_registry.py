# -*- coding: utf-8 -*-
"""指数登记表 —— **全仓唯一一份**（v2.360，2026-11-21 用户 m38439/m38512）

为什么要有这个文件
------------------
在这之前，指数代码在至少六处各写一份：engine.INDEX_DAILY_CODES、
engine.MarketSentiment.INDEX_MAP、engine.MarketSentiment.fetch() 里的 codes 列表、
server.INDEX_CODES、server.INDEX_NAMES、terminal.html 的 IDX_NAMES / idxNames / MTX_IDX。
用户 m38439 要求「顶栏沪深300、中证500 两格可以右键换成别的指数」后，再加一份就是七处，
改一个漏一个会静默出错（顶栏显示的是旧指数 / 点进去 K 线画的是别人）。所以抽成这里。

用户 m38512 的五条拍板（照抄，别再自行发挥）
--------------------------------------------
1. 中证2000（932000）三边都取不到日K/分时 ⇒ 候选里换成「中证全指」sh000985。
2. **只有沪深300(sh000300) 和 中证500(sz399905) 这两格可换**，其余五格是最常用指数，
   不许换掉。
3. 交互是**原位替换**：右键沪深300 → 候选 → 点「同花顺全A」→ 该格变成全A；
   再右键全A → 候选里含沪深300 → 点回即还原。
4. 选择要**记住**（前端 localStorage，键 qt_idx_slot_v1）。
5. 「拉取指数的时候也要增加这些指数，不管它是否在主页显示」—— 即所有候选指数都要
   参与盘中实时刷新与盘后存档，和其它指数完全一致的节奏（见 BAR_CODES）。

来源（src）决定怎么取数，两条通路彼此不通，别混
------------------------------------------------
- 'tencent'：腾讯 fqkline。7 大 + 上证50/中证1000/中证全指/中证红利 都走这条。
- 'ths'    ：同花顺网页自编指数（48_ 前缀）。**只有同花顺全A 883957** ——
             腾讯/新浪/东财都没有它，fuyao 只有实时快照没有历史。
"""

# 单一登记表：code -> {'name': 顶栏全名, 'short': 短名, 'src': 取数通路}
ALL = {
    # ── 七格默认（其中 sh000300 / sz399905 可被替换）────────────────────
    'sh000001': {'name': '上证指数', 'short': '上证', 'src': 'tencent'},
    'sz399001': {'name': '深证成指', 'short': '深证成指', 'src': 'tencent'},
    'sz399006': {'name': '创业板指', 'short': '创业板指', 'src': 'tencent'},
    'sh000680': {'name': '科创综指', 'short': '科创综指', 'src': 'tencent'},
    'sh000688': {'name': '科创50', 'short': '科创50', 'src': 'tencent'},
    'sh000300': {'name': '沪深300', 'short': '沪深300', 'src': 'tencent'},
    'sz399905': {'name': '中证500', 'short': '中证500', 'src': 'tencent'},

    # ── 可换进那两格的候选（也全部参与实时刷新 + 盘后存档）───────────────
    # 用户点名要的；2018-05-17 起 2067 根，唯一源 = 同花顺网页接口
    '883957':   {'name': '同花顺全A', 'short': '同花顺全A', 'src': 'ths'},
    'sh000852': {'name': '中证1000', 'short': '中证1000', 'src': 'tencent'},
    'sh000985': {'name': '中证全指', 'short': '中证全指', 'src': 'tencent'},   # 换掉拿不到的中证2000
    'sh000016': {'name': '上证50', 'short': '上证50', 'src': 'tencent'},

    # ── 特色指标的分母。不在顶栏，但要和别的一样被刷新/存档 ───────────────
    'sh000922': {'name': '中证红利', 'short': '中证红利', 'src': 'tencent'},
}

# 顶栏七格的固定顺序（用户 m2xx 定的七格，不许随登记表顺序变）
TOP7 = ('sh000001', 'sz399001', 'sz399006', 'sh000680', 'sh000688',
        'sh000300', 'sz399905')

# 哪两格可以右键换、能换成什么（用户 m38512 第 2 条：只有这两格）
SLOTS = {
    # v2.363 (用户 m39345): 中证红利 sh000922 也进候选 —— 它是「特色指标」的分母，
    #   用户要能在顶栏直接看它。放在最后一位，保持原有六项顺序不动。
    'sh000300': ('sh000300', '883957', 'sh000852', 'sh000985', 'sh000016', 'sz399905', 'sh000922'),
    'sz399905': ('sz399905', '883957', 'sh000852', 'sh000985', 'sh000016', 'sh000300', 'sh000922'),
}

# 所有需要本地日K + 实时 + 存档的指数（= 登记表全集，顺序稳定便于日志对齐）
BAR_CODES = tuple(ALL.keys())

# 「指数一览」页的 12 只（用户 m39345：七大 + 可替换候选全都要 + 中证红利）
#   ⚠ 顺序 = 页面左列从上到下的顺序，别随手调。七大在前是因为最常用。
LIST_CODES = (TOP7
              + ('883957', 'sh000852', 'sh000985', 'sh000016', 'sh000922'))


def name_of(code):
    """顶栏全名；登记表没有就返回大写代码，不抛异常。"""
    it = ALL.get((code or '').strip().lower())
    return it['name'] if it else (code or '').upper()


def short_of(code):
    it = ALL.get((code or '').strip().lower())
    return it['short'] if it else (code or '').upper()


def norm(code):
    """把 '883957' / 'SH000001' / ' sh000300 ' 统一成小写登记键；不认识的返回小写原串。"""
    c = str(code or '').strip().lower()
    if c in ALL:
        return c
    # 裸 6 位但表里有两位前缀写法时（例如 '000300'）不猜 —— 指数号段与个股重叠，猜错就是错数据
    return c


def is_index(code):
    """是不是我们认得的指数。**不认识的号段一律 False**（宁可当个股，也不乱认）。"""
    return norm(code) in ALL


def src_of(code):
    """取数通路：'tencent' | 'ths' | ''（不认识的返回空串）。"""
    it = ALL.get(norm(code))
    return it['src'] if it else ''


def slots_payload():
    """给前端用：两格的候选列表 + 显示名，一次拿全，避免前端再写一遍名字。"""
    return {
        'top7': [{'code': c, 'name': name_of(c), 'short': short_of(c)} for c in TOP7],
        'slots': {k: [{'code': c, 'name': name_of(c), 'short': short_of(c)} for c in v]
                  for k, v in SLOTS.items()},
        'names': {c: name_of(c) for c in ALL},
        'shorts': {c: short_of(c) for c in ALL},
    }


def selftest():
    ok = True
    for c in TOP7:
        if c not in ALL:
            print('!! TOP7 里的 %s 不在登记表' % c); ok = False
    for slot, cands in SLOTS.items():
        if slot not in cands:
            print('!! 槽 %s 的候选里必须含它自己（能切回默认）' % slot); ok = False
        for c in cands:
            if c not in ALL:
                print('!! 槽 %s 的候选 %s 不在登记表' % (slot, c)); ok = False
    for c, it in ALL.items():
        if it['src'] not in ('tencent', 'ths'):
            print('!! %s 的 src=%r 不认识' % (c, it['src'])); ok = False
    print('登记表 %d 只 / 顶栏 %d 格 / 可换 %d 格'
          % (len(ALL), len(TOP7), len(SLOTS)))
    print('OK' if ok else 'FAIL')
    return ok


if __name__ == '__main__':
    import io
    import sys
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass
    raise SystemExit(0 if selftest() else 1)
