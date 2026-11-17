# -*- coding: utf-8 -*-
"""v2.281 (2026-11-12 用户 m10425/m10906): RPS 轮动矩阵的 AI 分析
   从 TSP(3011) `backend/app/services/concept_rotation_analyzer.py` 移植。

【移植时改掉的两处口径】
   ① TSP 的涨幅是**小数**（0.0522 = +5.22%），它的 `_fmt_pct` 要 `v*100`。
      我方 `board_rotation.rotation()` 返回的已经是**百分数**（+5.22）⇒ 这里**不再乘**。
   ② TSP 的行业是 `"一级-二级-三级"` 三段名，按 level 取第几段；
      我方 `industry_parent.csv` 给的是 `细分行业 → 上级行业` 的映射（257 → 90），
      层级在 `board_rotation` 里就已经聚合好了 ⇒ 这里不再拆名字。

【结构（与 TSP 保持同一套）】
   ① `signals(dates, columns)` —— 从矩阵算五类信号（主线/新晋/退潮/机构/游资）
   ② `build_prompts(...)`     —— system + user 两段 prompt
   ③ `analyze(...)`           —— 调 8799 现有的 AI 通道（ai_news 的降级链），返回全文
   红线：**绝对不输出买卖/操作建议**，只客观陈述轮动特征（用户要的是研究视角）。
"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
for _p in (_ROOT, _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import math  # noqa: E402

from quant_terminal import ai_news as _an  # noqa: E402

# 每类信号最多喂几条给 AI（控制 token；与 TSP 的 _TOP_N 一致）
_TOP_N = 8


def _dim(kind):
    return '行业' if kind == 'industry' else '概念'


# ================================================================ 信号
def signals(dates, columns):
    """从「日期 → [[板块名, 涨幅%], ...]」矩阵算五类轮动信号。

    dates: 最新在最前（与 rotation() 一致）
    返回 {'persistent_leaders': [...], 'rising': [...], 'fading': [...],
          'institutional': [...], 'hot_money': [...]}
    每条含 name / ranks（左旧右新）/ pcts / avg_rank / rank_std
    """
    if not dates or not columns:
        return {}
    dates_asc = list(reversed(dates))

    data = {}
    for d in dates_asc:
        for idx, pair in enumerate(columns.get(d) or []):
            try:
                nm, pv = pair[0], pair[1]
            except Exception:
                continue
            data.setdefault(nm, {})[d] = (idx + 1, pv)

    def _stats(rp):
        ranks = [r for r, _ in rp]
        pcts = [p for _, p in rp]
        avg = sum(ranks) / len(ranks) if ranks else 0.0
        var = sum((r - avg) ** 2 for r in ranks) / len(ranks) if ranks else 0.0
        return {'ranks': ranks, 'pcts': [round(p, 4) for p in pcts],
                'avg_rank': round(avg, 1), 'rank_std': round(math.sqrt(var), 1)}

    persistent, rising, fading, institutional, hot = [], [], [], [], []
    for nm, by_date in data.items():
        # 🔴 缺失日必须**按日期归位**补 (999, 0) —— 一律追加末尾会把
        #   「只在最近几日上榜」的新晋读成退潮。TSP 注释里专门标了这条。
        rp = [by_date.get(d, (999, 0.0)) for d in dates_asc]
        s = _stats(rp)
        s['name'] = nm
        ranks = s['ranks']
        latest_r, earliest_r = ranks[-1], ranks[0]
        recent = ranks[-min(3, len(ranks)):]
        recent_avg = sum(recent) / len(recent)

        if recent_avg <= 10 and latest_r <= 10:
            persistent.append(s)
        jump = earliest_r - latest_r
        if earliest_r > 30 and latest_r <= 20 and jump >= 20:
            rising.append(s)
        drop = latest_r - earliest_r
        if earliest_r <= 10 and latest_r > 30 and drop >= 20:
            fading.append(s)
        if s['rank_std'] <= 5 and s['avg_rank'] <= 20:
            institutional.append(s)
        if s['rank_std'] >= 20:
            hot.append(s)

    persistent.sort(key=lambda x: x['avg_rank'])
    rising.sort(key=lambda x: x['ranks'][0] - x['ranks'][-1], reverse=True)
    fading.sort(key=lambda x: x['ranks'][-1] - x['ranks'][0], reverse=True)
    institutional.sort(key=lambda x: (x['rank_std'], x['avg_rank']))
    hot.sort(key=lambda x: x['rank_std'], reverse=True)

    return {'persistent_leaders': persistent[:_TOP_N], 'rising': rising[:_TOP_N],
            'fading': fading[:_TOP_N], 'institutional': institutional[:_TOP_N],
            'hot_money': hot[:_TOP_N]}


# ================================================================ prompt
def _fmt_pct(v):
    if v is None:
        return '—'
    return '%+.2f%%' % float(v)          # ⚠️ 我方已是百分数, 不乘 100


def _system_prompt(kind):
    dim = _dim(kind)
    return f"""你是一位专注 A 股题材轮动的研究分析师,拥有 12 年一线研究经验,擅长从{dim}板块的**涨幅排名矩阵**中客观识别主力资金脉络,区分机构主导的持续性主线与游资驱动的脉冲式轮动,产出一份**客观、中立、不包含任何买卖或操作建议**的轮动分析报告。

## 核心红线(务必遵守)

- **绝对不输出**"跟踪/规避/追高/低吸/观望/操作建议"等任何交易指令或倾向性措辞
- 你的角色是**客观陈述**各{dim}的轮动特征、资金属性(机构 vs 游资)、持续性特征
- 换成"一个中立财经记者能不能写出来"——能写就保留,不能写就删除

## 输出规范

用 **Markdown** 格式输出,严格遵循以下结构。不要输出任何 JSON 或代码块,直接输出 Markdown 正文。

### 1. 🎯 主线研判(2-3 句)
点名当前最核心的 1-2 条主线题材(连续多日霸榜的强势{dim}),用一句话概括其逻辑(政策/产业/业绩/事件驱动),并客观判断是**主升期/加速期/扩散期/见顶期**。结尾用【主线强度:强 / 中 / 弱】客观定性。

### 2. 🆕 新晋强势
列出排名快速跃升的{dim}(从榜单中后段冲进前列的),逐个给出:
- {dim}名 + 近 N 日排名变化(如 `45→20→8`)
- 涨幅加速度(连日递增 = 趋势加强)
- 可能的驱动逻辑(从板块属性推断,不要编造具体消息)
- 客观判断是**主力切入**还是**消息脉冲**

### 3. 📉 退潮预警
列出从高位明显滑落的{dim}(连续排名下滑或涨幅骤降),逐个给出:
- {dim}名 + 排名下滑轨迹
- 退潮性质(高位分歧/资金撤离/补跌)
- 是否扩散风险

### 4. 🏛️ 机构主线 vs 🎰 游资轮动
基于排名稳定性客观区分两类资金行为:
- **机构主线**:排名标准差小、长期稳居前列的{dim} → 持续性特征描述
- **游资轮动**:排名剧烈波动、脉冲式冲高的{dim} → 短线波动特征描述
客观给出当前市场**整体轮动节奏**(快轮动/慢轮动/主线聚焦)的判断。

### 5. 🌐 结合大盘
结合提供的大盘数据(指数涨跌/情绪/量能),客观判断:
- 当前大盘环境对题材轮动是助力还是阻力
- 情绪温度与轮动节奏的匹配度(如情绪冰点但题材活跃 = 抱团;情绪火热但轮动快 = 末段)

### 6. 📌 后续观察清单
- **持续性强(客观特征)**:排名标准差小、多日稳居前列的{dim}(列出名称+排名数据)
- **波动性大(客观特征)**:排名剧烈跳动的{dim}(列出名称+排名数据)
- 客观描述各{dim}的轮动特征,供读者自行观察
- **不输出**"跟踪/规避/追高/低吸/观望"等操作指令;可客观描述结构变化信号(如"主线{dim}连续 2 日跌出前 10,主线强度可能减弱")

### 7. ⚠️ 风险提示
列出需要客观关注的风险(如主线断层、情绪与轮动背离、成交萎缩)。末尾附一行:
"> ⚠️ 本内容由 AI 基于公开行情数据生成,仅客观陈述轮动特征,不构成任何投资建议或买卖指令。交易有风险,入市需谨慎。"

## 分析准则(务必遵守)

0. **只输出结论,不输出思考过程**:禁止复述你的分析步骤。不要写"我先看...""基于上述数据我认为"——直接给结论。
1. **数据说话**:每个判断引用具体排名/涨幅数值,严禁空泛套话("强势"必须改成"连续 4 日稳居前 5,均涨 +4.2%")。
2. **客观中立**:数据不支持的结论就直言"信号不足,暂无法判断",不要硬凑。
3. **区分资金性质**:这是本分析的核心价值——机构 vs 游资的判断必须基于排名稳定性(标准差),不要凭感觉。
4. **不重复数字**:正文负责解读信号含义,不要照抄罗列已提供的全部原始数据。
5. **不输出操作指令**:不写"跟踪/规避/追高/低吸/观望"等任何交易指令;客观陈述轮动特征即可。
6. **客观推断**:若无明确消息,从量价异动客观推断可能逻辑并给结论,不要标注"[推断]"或编造具体新闻。

现在请基于下方{dim}轮动数据进行分析。"""


def _signal_block(title, items):
    if not items:
        return '### %s\n(本类无明显信号)' % title
    out = ['### %s' % title]
    for it in items:
        rs = '→'.join(str(r) if r < 999 else '—' for r in it['ranks'])
        avgp = (sum(it['pcts']) / len(it['pcts'])) if it['pcts'] else 0.0
        out.append('- %s: 排名 %s | 均排名 %s | 排名波动σ %s | 区间均涨 %s'
                   % (it['name'], rs, it['avg_rank'], it['rank_std'], _fmt_pct(avgp)))
    return '\n'.join(out)


def _market_block(mkt):
    """大盘背景 —— mkt 是 dict: {index:[(名,涨幅%)], emotion:{score,label},
       limit:{up,broken,down,max_boards}, amount_yi}"""
    if not mkt:
        return '- (大盘背景未取到)'
    idx = ' / '.join('%s %+.2f%%' % (n, v) for n, v in (mkt.get('index') or [])[:4]) or '指数缺失'
    emo = mkt.get('emotion') or {}
    lim = mkt.get('limit') or {}
    return ('- 指数: %s\n'
            '- 情绪: %s (%s)\n'
            '- 涨停/炸板/跌停: %s / %s / %s  (最高连板 %s)\n'
            '- 两市成交额: %s 亿元'
            % (idx,
               emo.get('score', '—'), emo.get('label', '—'),
               lim.get('up', '—'), lim.get('broken', '—'), lim.get('down', '—'),
               lim.get('max_boards', '—'),
               ('%.0f' % mkt['amount_yi']) if mkt.get('amount_yi') else '—'))


def build_prompts(kind, level, days, dates, columns, mkt=None, focus=''):
    """造 (system, user) 两段 prompt。columns 是 board_rotation.rotation() 的产出。"""
    dim = _dim(kind)
    sig = signals(dates, columns)
    dasc = list(reversed(dates))
    rng = ('%s ~ %s' % (dasc[0], dasc[-1])) if dasc else '—'

    parts = ['# %s涨幅轮动数据 (最近 %d 个交易日: %s)' % (dim, days, rng),
             '',
             '## 大盘背景',
             _market_block(mkt),
             '',
             '## 轮动信号 (排名时间方向: 左→右 = 旧→新, 排名越小越强)',
             '',
             _signal_block('🎯 主线 (连续霸榜)', sig.get('persistent_leaders')),
             '',
             _signal_block('🆕 新晋强势 (排名跃升)', sig.get('rising')),
             '',
             _signal_block('📉 退潮预警 (高位滑落)', sig.get('fading')),
             '',
             _signal_block('🏛️ 机构特征 (排名稳定)', sig.get('institutional')),
             '',
             _signal_block('🎰 游资特征 (排名波动大)', sig.get('hot_money'))]
    f = (focus or '').strip()
    if f:
        parts += ['', '## 用户追加关注点', f, '',
                  '请在完成上述七节的基础上,额外结合这个关注点补充说明'
                  '(仍然不要给出任何买卖/操作建议)。']
    return _system_prompt(kind), '\n'.join(parts), sig


def summary_of(sig):
    """meta 摘要（前端可以立刻显示）"""
    lead = sig.get('persistent_leaders') or []
    names = '、'.join(x['name'] for x in lead[:3]) or '暂无明确主线'
    return '主线: %s | 新晋 %d | 退潮 %d' % (
        names, len(sig.get('rising') or []), len(sig.get('fading') or []))


# ================================================================ 调用
#: v2.282 (2026-11-12 用户 m11382): RPS 轮动分析**锁死**用这个槽。
#: 用户原话:「RPS轮动分析这个功能强制固定锁死用deepseek来分析, 这个需要更聪明的模型比较好一点」
#: —— DeepSeek 在 ai_news 的 SLOTS 里是 'backup' (终端设置里用户自己填的付费 key)。
#: 🔴 这里**故意不降级**: 以前 analyze() 是 (slot,) + 其它槽 逐级降级, 智谱挂着就会
#:    悄悄换成智谱答, 用户以为用的是 DeepSeek。锁死就是宁可报错也不换家。
RPS_SLOT = 'backup'


def analyze(kind, level, days, dates, columns, mkt=None, focus='', slot=None,
            timeout=180):
    """同步跑一次 AI 分析（拿全文）。返回 {ok, text, provider, model, err, summary}

    v2.282: slot 参数默认 None = 用 RPS_SLOT; 传了也只认 RPS_SLOT 存在与否,
    **不再降级到别的槽** —— 见 RPS_SLOT 上面那段说明。
    """
    sys_p, usr_p, sig = build_prompts(kind, level, days, dates, columns, mkt, focus)
    cfg = _an.ai_cfg()
    if not cfg.get('enabled'):
        return {'ok': False, 'text': '', 'err': 'AI 已在数据源设置里关闭', 'summary': summary_of(sig)}
    c = dict(cfg)
    c['max_tokens'] = 6000                 # 七节报告比新闻点评长得多
    c['timeout'] = max(int(timeout), 120)
    s = RPS_SLOT                            # 锁死, 不看 slot 参数
    sc = c.get(s)
    if not isinstance(sc, dict):
        return {'ok': False, 'text': '',
                'err': 'AI 槽 [%s] 没配置' % s, 'summary': summary_of(sig)}
    slot_cfg = dict(sc)
    slot_cfg['_slot'] = s
    if not _an.slot_state(slot_cfg)['ready']:
        return {'ok': False, 'text': '',
                'err': '%s 未配置 key (请在 数据源设置 里填 DeepSeek 的 key)'
                       % slot_cfg.get('name', s),
                'summary': summary_of(sig)}
    key, _src = _an.resolve_key(slot_cfg)
    try:
        for ev, p in _an._stream_once(
                slot_cfg, key,
                [{'role': 'system', 'content': sys_p},
                 {'role': 'user', 'content': usr_p}],
                c):
            if ev == 'done':
                txt = (p or {}).get('text') or ''
                if txt.strip():
                    return {'ok': True, 'text': txt, 'provider': slot_cfg['name'],
                            'model': slot_cfg.get('model') or '',
                            'err': '', 'summary': summary_of(sig)}
                return {'ok': False, 'text': '',
                        'err': '%s 返回空正文' % slot_cfg['name'],
                        'summary': summary_of(sig)}
    except _an._AIErr as e:
        return {'ok': False, 'text': '', 'err': '%s(%s)' % (slot_cfg['name'], e.msg),
                'summary': summary_of(sig)}
    except Exception as e:
        return {'ok': False, 'text': '',
                'err': '%s(%s)' % (slot_cfg['name'], type(e).__name__),
                'summary': summary_of(sig)}
    return {'ok': False, 'text': '', 'err': '%s(没有返回正文)' % slot_cfg['name'],
            'summary': summary_of(sig)}


if __name__ == '__main__':
    from quant_terminal import board_rotation as _br
    for kind, lv in (('industry', 2), ('industry', 1), ('concept', 2)):
        d = _br.rotation(kind, lv, 30)
        if not d.get('ok'):
            print(kind, 'FAIL', d.get('error'))
            continue
        s, u, sig = build_prompts(kind, lv, d['days'], d['dates'], d['columns'])
        print('=== %s L%d  主线%d 新晋%d 退潮%d 机构%d 游资%d ===' % (
            kind, lv, len(sig['persistent_leaders']), len(sig['rising']),
            len(sig['fading']), len(sig['institutional']), len(sig['hot_money'])))
        print('  summary:', summary_of(sig))
        print('  user prompt 长度:', len(u))
        for line in _signal_block('🎯 主线', sig['persistent_leaders'][:3]).split('\n'):
            print('  ', line)
