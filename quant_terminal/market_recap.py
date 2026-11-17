"""AI 大盘复盘 + 历史复盘存档 —— 照抄 TSP(3011) 的 market_recap。

v2.302 (2026-11-13 用户 m16307): 「TSP的"复盘"模块里面有"龙虎榜"这个功能，
  还有AI复盘，历史复盘数据等等，你去抄下来，整合到我们的"龙虎榜"界面」

来源（TSP 侧原文，逐条对齐，只改口径不做发挥）:
  backend/app/services/market_recap.py            系统提示词 + 用户消息装配 + 摘要
  backend/app/services/market_recap_reports.py    历史报告持久化(JSON, 上限 20 条)
  backend/app/services/dragon_tiger.py            build_recap_context() 龙虎榜摘要段
  api/market_recap.py                             NDJSON 协议

流式协议与 board_rps_ai 一致（前端 rpsAi 的解析代码可直接复用思路）:
  {ev:'meta', d:{as_of, emotion_score, emotion_label, summary}}
  {ev:'who',  d:{provider, model, slot}}        这次是谁答的
  {ev:'delta',d:{content}}                      逐块正文
  {ev:'err',  d:{code, msg}}
  {ev:'done', d:{content}}                      全文(前端存历史用)

🔴 口径差异（照抄时最容易踩的两处，已在 TSP 原文注释里核实）:
  1. TSP 的 overview.indices[].change_pct / breadth.up_pct / seal_rate / above_ma*_pct /
     avg_turnover **本身就是百分数**（1.2 = 1.2%），不要再 *100；
     但 TSP 的 board rank 里 `avg_pct` 是**小数**（0.0522），原文 `*100` 是对的。
     我们 board_overview 的 rank 里 `avg_pct` **已经是百分数**（+7.08）⇒ 这里不乘。
  2. TSP 的 amount.total 单位是**元** ⇒ /1e8 得亿元；我们 board_overview 的
     `amount.total` 也是元（16,990 亿 = 1.699e12），同样 /1e8，一致。
"""
from __future__ import annotations

import io
import json
import os
import sys
import threading
import time
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
for _p in (ROOT, HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from quant_terminal import ai_news as _an  # noqa: E402

# 复盘摘要里用简称，全称在列表里放不下（TSP 原文 _INDEX_SHORT 同款）
_INDEXES = [
    ('sh000001', '上证指数', '上'),
    ('sz399001', '深证成指', '深'),
    ('sz399006', '创业板指', '创'),
    ('sh000688', '科创综指', '科'),
]
_INDEX_SHORT = {n: s for _c, n, s in _INDEXES}

# 报告持久化（TSP 用 data/user_data/ai_market_recaps.json，我们放 kline_data 旁边）
_REPORT_FILE = os.path.join(ROOT, 'kline_data', 'ai_market_recaps.json')
MAX_REPORTS = 20
_RPT_LOCK = threading.RLock()

# 默认用 DeepSeek 写复盘（与 RPS 轮动分析同一个槽）；拿不到再降级到 primary。
# 与 board_rps_ai.RPS_SLOT 的区别：那边用户要求**锁死不许降级**，这边允许降级保可用。
RECAP_SLOT = 'backup'


# ================================================================
# 系统提示词（TSP 原文照抄，一字未改）
# ================================================================
_SYSTEM_PROMPT = """你是一位拥有 15 年 A 股一线研究经验的市场分析师,擅长从指数结构、涨跌家数、连板梯队、板块轮动与资金情绪中客观提炼市场主线,产出一份**客观、中立、不包含任何买卖或操作建议**的盘后复盘报告。

## 核心红线(务必遵守)

- **绝对不输出**"进攻/防守/加仓/减仓/轻仓/半仓/重仓/仓位建议/低吸/反包/追高/回避方向"等任何交易指令或倾向性措辞
- 你的角色是**客观陈述**今日市场的结构、情绪、板块轮动特征,以及后续值得客观关注的盘面信号
- 换成"一个中立财经记者能不能写出来"——能写就保留,不能写就删除

## 输出规范

用 **Markdown** 格式输出,严格遵循以下结构。不要输出任何 JSON 或代码块,直接输出 Markdown 正文。

### 1. 🎯 一句话定调(1-2 句)
用一句话概括今日市场的**核心矛盾与状态**(如"放量普涨、情绪修复,主线围绕科技扩散"/"指数走强、个股普跌,赚钱效应偏弱")。结尾用【市场状态:偏强 / 中性 / 偏弱】客观描述当日市场强弱,**不下基调结论、不指挥操作**。

### 2. 📊 盘面总览
- 三大指数(上证/深证/创业板)表现:谁强谁弱、量能配合
- 涨跌家数、涨停/跌停/炸板结构、两市成交额(放量/缩量判断)
- 情绪温度(强势/偏暖/震荡/偏冷/冰点)及一句话依据

### 3. 📈 指数结构
谁在走强、谁在走弱;指数是否同步;关键支撑/压力位(基于当日点位推断);是否存在量价背离。

### 4. 🔥 板块主线
- 领涨板块:背后的逻辑(消息/业绩/资金/技术)、持续性判断
- 领跌板块:客观风险信号、是否扩散
- 连板梯队与投机情绪:最高连板、封板率、炸板率反映的资金活跃程度

### 5. 💰 资金与情绪
成交额结构(增量/存量)、市场宽度(上涨占比、站上均线占比)、量能指标(量比)解读;风险偏好是修复还是转弱。

### 6. 📰 消息催化
结合提供的近期新闻,客观提炼可能影响后续盘面的催化或扰动,明确区分"已兑现"与"待发酵"。**若无新闻数据,则直接从量价异动客观推断可能的催化逻辑并给出结论,不要标注"[推断]"之类的过程标签,更不要编造具体消息。**

### 7. 📌 后续观察要点
- 客观列出明日值得关注的盘面信号(如量能能否维持、某均线得失、某板块持续性)
- 客观描述不同情景下市场结构的可能演变(如"若量能持续放大,普涨格局或延续";"若量能萎缩,结构性行情为主"),**不涉及仓位与买卖方向**
- **不输出**"仓位建议""进攻/防守基调""买卖方向""追高/低吸/反包"等操作指令

### 8. ⚠️ 风险提示
列出需要客观关注的风险点(如量能跟不上、外资流出、连板断层等)。末尾附一行:
"> ⚠️ 本内容由 AI 基于公开行情数据生成,仅客观陈述市场状态,不构成任何投资建议或买卖指令。交易有风险,入市需谨慎。"

## 分析准则(务必遵守)

0. **只输出结论,不输出思考过程**:禁止复述你的分析步骤或方法论。不要写"我先按...做结构化复盘""接下来看...""基于上述数据我认为"这类元话语——直接给结论。读者要的是复盘结果,不是你怎么推导出来的。
1. **数据说话**:每个判断引用具体数值,严禁空泛套话("情绪回暖"必须改成"涨停 68 家较前日 +22,封板率 75%")
2. **客观中立**:看多就客观陈述多头特征,看空就客观陈述空头特征,不下基调、不骑墙;数据不支持时直言无法判断
3. **结构优先**:先看指数同步性与量能结构,再看板块与情绪,最后才是消息
4. **不重复数字**:正文负责解读表格数据背后的含义,不要照抄罗列已提供的大段原始数字
5. **不输出操作指令**:不写"仓位建议""进攻/防守""买卖方向"等任何交易指令
6. **简明客观**:用读者能扫读的密度输出,总字数 1200-2000 字,重在客观信息密度

现在请基于下方数据进行复盘。"""


# ================================================================
# 数据块拼装（TSP 原文逐函数对应）
# ================================================================
def _fmt_pct(v, suffix='%'):
    """v 已是百分数（1.2 = 1.2%）。"""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return '—'
    return ('%+.2f%s' % (f, suffix)) if suffix else ('%.2f' % f)


def indices_block(idx):
    """指数行情精简块。idx = {code: {name, price, change_pct}}"""
    if not idx:
        return '(暂无指数)'
    lines = []
    for code, name, _short in _INDEXES:
        it = idx.get(code)
        if not isinstance(it, dict):
            continue
        price = it.get('price')
        chg = it.get('change_pct')
        price_s = ('%.2f' % float(price)) if price else '—'
        lines.append('- %s: %s  %s' % (it.get('name') or name, price_s, _fmt_pct(chg)))
    return '\n'.join(lines) or '(暂无指数)'


def breadth_block(ov):
    """盘面数据精简块。字段名与 board_overview.snapshot() 对齐。"""
    b = ov.get('breadth') or {}
    amt = ov.get('amount') or {}
    lim = ov.get('limit') or {}
    tr = ov.get('trend') or {}
    act = ov.get('activity') or {}

    # 🔴 口径: TSP 的 amount.total 单位是**元** ⇒ 原文 /1e8 得亿元;
    #   我们 board_overview 的 amount.total **已经是亿元**(实测 16668.0 = 1.6668 万亿)
    #   ⇒ 这里**绝不能再 /1e8**, 否则显示 "成交 0 亿"。
    amount_yi = float(amt.get('total') or 0)

    lines = [
        '- 上涨/下跌/平盘: %s / %s / %s  (上涨占比 %.1f%%)'
        % (b.get('up', 0), b.get('down', 0), b.get('flat', 0), float(b.get('up_pct') or 0)),
        '- 涨停/炸板/跌停: %s / %s / %s  (封板率 %.0f%%, 最高连板 %s)'
        % (lim.get('limit_up', 0), lim.get('broken', 0), lim.get('limit_down', 0),
           float(lim.get('seal_rate') or 0), lim.get('max_boards', 0)),
    ]
    tiers = lim.get('tiers') or []
    if tiers:
        # TSP 原样: 只取前 5 档。我们的 tiers 是降序(高板在前), 直接切前5
        lines.append('- 连板梯队: ' + '、'.join(
            '%s板×%s' % (t.get('boards'), t.get('count')) for t in tiers[:5]))
    lines.append('- 两市成交额: %.0f 亿元' % amount_yi)
    lines.append('- 均线站位: MA5 %.0f%% / MA20 %.0f%% / MA60 %.0f%%'
                 % (float(tr.get('above_ma5_pct') or 0),
                    float(tr.get('above_ma20_pct') or 0),
                    float(tr.get('above_ma60_pct') or 0)))
    lines.append('- 量能: 平均换手 %.2f%%, 量比5日均 %.2f'
                 % (float(act.get('avg_turnover') or 0),
                    float(act.get('vr') or act.get('vol_ratio') or 0)))
    return '\n'.join(lines)


def sector_block(rank, label):
    """板块排名精简块(领涨/领跌 top5)。

    🔴 口径: TSP 的 avg_pct 是小数所以要 *100; 我们 board_overview 的 rank.avg_pct
       已经是百分数(+7.08) ⇒ **不能乘**。这是移植时最容易抄错的一处。
    """
    if not rank:
        return '- 领涨%s: (暂无数据)\n- 领跌%s: (暂无数据)' % (label, label)

    def _fmt(items):
        if not items:
            return '—'
        out = []
        for it in items[:5]:
            ld = it.get('leader') or {}
            out.append('%s(%s,领涨:%s)' % (
                it.get('name'),
                _fmt_pct(it.get('avg_pct')),
                ld.get('name') or '—'))
        return '、'.join(out)

    return ('- 领涨%s: %s\n- 领跌%s: %s'
            % (label, _fmt(rank.get('leading')), label, _fmt(rank.get('lagging'))))


def emotion_block(ov):
    emo = ov.get('emotion') or {}
    radar = ov.get('radar') or []
    lines = ['- 情绪温度: %s (%s)' % (emo.get('score', 50), emo.get('label', '—'))]
    if radar:
        lines.append('- 六维雷达: ' + '、'.join(
            '%s%s' % (r.get('label'), r.get('value', 0)) for r in radar))
    return '\n'.join(lines)


def lhb_context():
    """龙虎榜摘要段（TSP dragon_tiger.build_recap_context 逐句照抄）。

    金额单位: fuyao_lhb 的 net_value/buying **已经是亿元**（_yi 转过），
    而 TSP 原文是按**元**取的再 /1e8 —— 所以这里的 /1e8 必须去掉。
    失败返回空串，不影响复盘主流程。
    """
    try:
        from quant_terminal import fuyao_lhb as _lhb
        d = _lhb.lhb_list(None, 'all')
        if not d or not d.get('ok'):
            return ''
        items = [x for x in (d.get('rows') or []) if not x.get('_rd')]
        if not items:
            return ''
        trade_date = d.get('date') or ''
        lines = ['(数据日期: %s)' % trade_date]

        top_buy = sorted([i for i in items if (i.get('net_value') or 0) > 0],
                         key=lambda x: x.get('net_value') or 0, reverse=True)[:5]
        if top_buy:
            lines.append('净买入居前: ' + '; '.join(
                '%s(%s) 净买%.2f亿 涨跌%.1f%%%s' % (
                    i.get('name'), i.get('ticker'), float(i.get('net_value') or 0),
                    float(i.get('change') or 0),
                    ' [%d日榜]' % i.get('range_days') if (i.get('range_days') or 1) > 1 else '')
                for i in top_buy))

        top_sell = sorted([i for i in items if (i.get('net_value') or 0) < 0],
                          key=lambda x: x.get('net_value') or 0)[:5]
        if top_sell:
            lines.append('净卖出居前: ' + '; '.join(
                '%s 净卖%.2f亿' % (i.get('name'), abs(float(i.get('net_value') or 0)))
                for i in top_sell))

        org = [i for i in items if i.get('org_net_value')]
        if org:
            lines.append('机构净买居前: ' + '; '.join(
                '%s 机构净买%.2f亿' % (i.get('name'), float(i.get('org_net_value') or 0))
                for i in sorted(org, key=lambda x: x.get('org_net_value') or 0,
                                reverse=True)[:5]))

        hm = d.get('hot_money') or []
        if hm:
            lines.append('活跃游资: ' + '; '.join(
                '%s 净买%.2f亿' % (h.get('name'), float(h.get('buying') or 0))
                for h in sorted(hm, key=lambda x: x.get('buying') or 0, reverse=True)[:5]))
        return '\n'.join(lines)
    except Exception:
        return ''


def build_prompts(ov, idx, focus=''):
    """→ (system, user)。TSP _build_user_prompt 的段序原样保留。"""
    as_of = ov.get('date') or '今日'
    parts = [
        '复盘日期: %s' % as_of,
        '',
        '## 主要指数',
        indices_block(idx),
        '',
        '## 盘面数据',
        breadth_block(ov),
        '',
        '## 市场情绪',
        emotion_block(ov),
        '',
        '## 概念板块排名',
        sector_block(ov.get('concept_rank'), '概念'),
        '',
        '## 行业板块排名',
        sector_block(ov.get('industry_rank'), '行业'),
    ]

    ctx = lhb_context()
    if ctx:
        parts.extend(['', '## 龙虎榜资金动向', ctx])

    # 消息催化：我们终端暂无新闻聚合注入这条链路，按 TSP 的降级文案写死
    parts.extend([
        '',
        '## 近期市场新闻',
        '(暂无新闻数据:本功能新闻检索能力将在后续版本接入。'
        '消息催化一节请直接从量价异动给出可能的催化逻辑结论,不要编造具体消息,也不要复述本说明。)',
    ])

    focus = (focus or '').strip()
    if focus:
        parts.extend([
            '',
            '## 用户追加关注点',
            focus,
            '',
            '请在完成上述八节的基础上补充说明上面的关注点(仍然不要给出任何买卖/操作建议)。',
        ])
    return _SYSTEM_PROMPT, '\n'.join(parts)


def recap_summary(ov, idx):
    """一句话摘要（meta 事件 / 历史列表共用）。指数用简称，与前端摘要条一致。"""
    emo = ov.get('emotion') or {}
    lim = ov.get('limit') or {}
    amt = ov.get('amount') or {}
    total = amt.get('total') or 0        # 亿元（见 breadth_block 的口径说明）

    pieces = []
    for code, name, short in _INDEXES[:3]:
        it = (idx or {}).get(code) or {}
        if it:
            pieces.append('%s%s' % (short, _fmt_pct(it.get('change_pct'))))
    idx_str = '、'.join(pieces) or '指数缺失'
    return '%s | 情绪%s(%s) | 涨停%s | 成交%.0f亿' % (
        idx_str, emo.get('score', 50), emo.get('label', '—'),
        lim.get('limit_up', 0), total)


# ================================================================
# 数据装配（server 侧调用，避免跨模块循环 import）
# ================================================================
def gather():
    """→ (overview, indices)。overview 用 board_overview 的缓存快照（与市场总览同源）。"""
    from quant_terminal import board_overview as _bov
    ov = _bov.snapshot(force=False) or {}
    idx = {}
    try:
        from quant_terminal import datasource as _ds
        codes = [c for c, _n, _s in _INDEXES]
        rt = _ds.fetch_realtime(codes) or {}
        for c in codes:
            it = rt.get(c)
            if isinstance(it, dict) and it.get('price'):
                idx[c] = {'name': it.get('name') or '', 'price': it.get('price'),
                          'change_pct': it.get('change_pct')}
    except Exception:
        idx = {}
    return ov, idx


# ================================================================
# 历史报告持久化（TSP market_recap_reports 的等价实现）
# ================================================================
def _rpt_load():
    try:
        if not os.path.exists(_REPORT_FILE):
            return []
        with io.open(_REPORT_FILE, 'r', encoding='utf-8') as f:
            d = json.load(f)
        return d if isinstance(d, list) else []
    except Exception:
        return []


def _rpt_save(items):
    try:
        os.makedirs(os.path.dirname(_REPORT_FILE), exist_ok=True)
        tmp = _REPORT_FILE + '.tmp'
        with io.open(tmp, 'w', encoding='utf-8', newline='\n') as f:
            json.dump(items, f, ensure_ascii=False, indent=1)
        os.replace(tmp, _REPORT_FILE)
        return True
    except Exception:
        return False


def list_reports():
    with _RPT_LOCK:
        items = _rpt_load()
    items.sort(key=lambda x: str(x.get('created_at') or ''), reverse=True)
    return items[:MAX_REPORTS]


def save_report(rec):
    """rec: {as_of, focus, content, summary, emotion_score, emotion_label}"""
    with _RPT_LOCK:
        items = _rpt_load()
        out = {
            'id': 'mkr_' + uuid.uuid4().hex[:12],
            'as_of': str(rec.get('as_of') or ''),
            'focus': str(rec.get('focus') or ''),
            'content': str(rec.get('content') or ''),
            'summary': str(rec.get('summary') or ''),
            'emotion_score': rec.get('emotion_score'),
            'emotion_label': str(rec.get('emotion_label') or ''),
            'created_at': time.strftime('%Y-%m-%dT%H:%M:%S'),
        }
        items.insert(0, out)
        items = items[:MAX_REPORTS]
        _rpt_save(items)
    return out


def delete_report(rid):
    rid = str(rid or '')
    if not rid:
        return False
    with _RPT_LOCK:
        items = _rpt_load()
        keep = [x for x in items if str(x.get('id')) != rid]
        if len(keep) == len(items):
            return False
        _rpt_save(keep)
    return True


# ================================================================
# 自测
# ================================================================
def _selftest():
    ov, idx = gather()
    sysp, usrp = build_prompts(ov, idx, '量能是否持续')
    print('overview ok=%s date=%s' % (ov.get('ok'), ov.get('date')))
    print('indices: %s' % indices_block(idx).replace('\n', ' | '))
    print('summary: %s' % recap_summary(ov, idx))
    print('system %d chars / user %d chars' % (len(sysp), len(usrp)))
    print('--- user prompt ---')
    print(usrp)
    print('--- lhb context ---')
    print(lhb_context() or '(空)')


if __name__ == '__main__':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass
    _selftest()
