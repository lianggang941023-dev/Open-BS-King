# -*- coding: utf-8 -*-
"""终端 8799 —— AI 对话窗口的 LLM 层 (v2.116, 2026-09-25)
==========================================================
K 线工具栏的「AI」按钮 → 弹窗自由对话。与 ai_news 的分工:

    ai_news  新闻 → 影响分析   固定格式 / 按新闻 id 缓存 / 单轮
    ai_chat  自由对话          模型自己决定要不要查行情 / 多轮工具循环

**LLM 那层一行没抄**, 全部 import 自 ai_news (它已经把那套打磨过了):
    ai_cfg / resolve_key / slot_state / _stream_once / _thinking_field / _AIErr

设计要点
--------
- **不跨槽降级**: 用户在窗口里选了「主」就只跑主, 主挂了直接报错。
  理由: 手动选槽这个动作本身就是在说"这次用它", 再偷偷换备 = 把选择静默作废。
  (ai_news 那边相反, 那边是自动降级链 —— 两条语义, 别互相套。)
- 选「备」= **付费**(DeepSeek), 前端要标出来, 这里不管钱只管调用。
- 对话**不落盘、不缓存**: 历史由前端每轮全量带上, 服务端零状态。
- 工具执行交给调用方传入的 `exec_fn` —— 本模块**不认识行情**,
  这样它不依赖 server, 也就没有循环导入 (同 ai_news 不 import server 的理由)。

⚠️ 本文件是**纯 LF**、无 BOM, 与 ai_news.py / datasource.py 同款。

实测事实 (2026-09-25 亲自打的, 别再摸索)
-----------------------------------------
- 主备两个模型**都支持 function calling**, 且都能正确点出工具:
      智谱 glm-4-flash      1.8s   tool_calls 分片 1 个
      DeepSeek deepseek-flash 0.9s  tool_calls 分片 **11 个**
  ⇒ 参数必须**按 index 累积**, 简单拼接在多调用/多分片时必错 (见 ai_news._stream_once)。
- DeepSeek 也会吐 `reasoning_content` (实测 75 字) —— "思考中" 的 UI 两个槽都用得上。
"""
import json

from quant_terminal import ai_news as _an

# 工具循环上限: 模型最多连着查 3 轮数据, 第 4 轮**不给工具** → 逼它用手上的信息作答。
# 不设上限的话, 一个"你好"都可能被它查上十几轮 (钱和延迟都受不了)。
MAX_TOOL_ROUNDS = 3

# 对话正文上限。cfg['max_tokens']=1200 是给新闻那套短输出调的,
# 对话要它解释一段逻辑时会被截断 —— 这里单独抬到 2000, 不动全局配置。
CHAT_MAX_TOKENS = 2000

# 前端每轮全量回传历史, 服务端兜一道上限 (防越聊越长, 也防被塞垃圾)。
MAX_HISTORY = 40          # 条 (user+assistant 合计), 超出只留最近的

# ============================================================
# System Prompt —— **这就是产品本身**
# ============================================================
# 与 ai_news 的 prompt 是**两份**, 别合并: 那份被钉死成新闻分析的固定格式,
# 拿来做对话会答非所问。但下面三条铁律是从那份**继承**过来的 (最值钱的部分)。
CHAT_SYSTEM_PROMPT = """你是终端 8799 内置的 AI 助手, 服务对象是一位 A 股中线趋势交易者(持股数周到数月, 专注科技板块)。

他可以问你任何问题 —— 交易概念、市场逻辑、行业分析、数据处理、写代码、日常闲聊都行。正常回答就好, 不要往固定格式上套。

【三条铁律, 不可违背】
1. 不许编造股票代码。不确定某只票的代码, 就用工具查, 或者直接说不确定 —— 编一个六位数字是最危险的事。
2. 不许编造数字。你没查到的行情、涨跌幅、财务数据, 一律不许凭印象写出来。
3. 不许假装看过行情。你的对话里**默认没有任何实时数据** —— 所有行情都必须来自工具返回, 不许说"目前股价在XX附近"这种没根据的话。

【关于工具】
你有 11 个查询工具 —— 股票搜索 / 实时行情 / 日K / 板块(均线·K线·突破三视角) /
大盘与市场情绪 / 新闻池 / 上次的扫描结果 / 模拟盘 / 自选股 / 基本面与股东户数 / 当日分时。
需要数据时**自己调用**。终端里的数据你基本都够得着, 拿不准有什么就先去查, 别凭印象答。
- 涉及"现在多少钱""涨了多少""帮我看看这只票"这类问题 → 必须先调工具拿到真数据再回答。
- 不需要数据的纯概念/逻辑问题 → 直接回答, 别为了显得勤快去查。
- 工具返回什么你就用什么, 不许在返回结果上"顺手补全"你没拿到的字段。
- 查不到就说查不到(比如代码不存在、数据未更新), 别硬圆。

【不给买卖建议】
不推荐买入/卖出/加仓/减仓, 不给目标价, 不预测涨跌。
原因: 他有自己完整的交易框架, 你是**输入**不是替代品。
但**分析**可以做 —— 可以讲逻辑、讲利弊、讲历史规律、讲某个指标怎么算, 只是不给"你该买/该卖"的结论。
他主动问"能买吗"时, 照此说明你只做分析不给建议, 然后给出分析。

【风格】
- 默认用中文。纯文本输出, 不要用 Markdown 标记(没有渲染器, 星号和井号会直接显示出来)。
- 需要分点时用「1. 2. 3.」或「- 」, 别用 ### 标题。
- 简洁优先。他是在看盘间隙瞄一眼, 不要长篇大论; 但该讲清楚的地方别省。"""

# ============================================================
# 工具定义
# ============================================================
# ⚠️ description 是**给模型看的**, 写得含糊它就不会调、或者瞎调。
#    "什么时候该调" 比 "这个工具做什么" 更重要 —— 所以每条都带触发条件。
TOOLS = [
    {
        'type': 'function',
        'function': {
            'name': 'search_stock',
            # ⚠️ 说清楚支持什么: 服务端走的是 server._ai_t_search —— 代码前缀 + 简称**子串**,
            #    **没有拼音首字母**。描述里多写一个能力 = 模型按它去试 = 每次都扑空。
            'description': '按简称(任意位置的连续子串, 如「有研」「稀土」)或 6 位代码前缀查 A 股股票。'
                           '当用户提到某只票但你不知道它的六位代码时, 必须先调这个。'
                           '注意: 不支持拼音首字母。',
            'parameters': {
                'type': 'object',
                'properties': {'keyword': {'type': 'string',
                                           'description': '股票简称片段或代码前缀, 如「有研新材」「6002」'}},
                'required': ['keyword'],
            },
        },
    },
    {
        'type': 'function',
        'function': {
            'name': 'get_realtime',
            'description': '获取 A 股**个股**的实时行情: 最新价、涨跌幅、开盘、最高、最低、成交量。'
                           '任何涉及某只票"现价/涨了跌了/多少钱"的问题都必须先调这个。'
                           '⚠️ 指数(上证指数/深证成指/创业板指/沪深300…)不要用这个, 用 get_market。',
            'parameters': {
                'type': 'object',
                'properties': {'code': {'type': 'string',
                                        'description': '6位股票代码, 如「600206」'}},
                'required': ['code'],
            },
        },
    },
    {
        'type': 'function',
        'function': {
            'name': 'get_kline',
            'description': '获取 A 股**个股或指数**的日K线历史(最近N根)并算好均线 MA5/10/21/55/120/250。'
                           '涉及走势、均线排列、左峰、支撑压力、形态判断时调这个。'
                           '指数也可以传(如「sh000001」或「上证指数」), 它会走指数日K。',
            'parameters': {
                'type': 'object',
                'properties': {
                    'code': {'type': 'string', 'description': '6位股票代码'},
                    'bars': {'type': 'integer',
                             'description': '要最近多少根日K, 默认60, 最多250'},
                },
                'required': ['code'],
            },
        },
    },
    {
        'type': 'function',
        'function': {
            'name': 'get_sector',
            'description': '查行业或概念板块。默认(kind=trend)给均线多头状态(档位/多头占比/'
                           '多头成立日)和该板块里多头排列、站上MA5的成分股名单 —— '
                           '涉及"某行业怎么样""哪些板块强""这票属于什么板块"时调这个。'
                           'kind=kline 给该板块的等权合成指数日K走势(要看板块图形时用); '
                           'kind=breakout 给该板块的三档左峰突破状态。'
                           '名字不确定就先给个关键词, 查不到时它会返回当前最强的几个板块名供你改口。',
            'parameters': {
                'type': 'object',
                'properties': {
                    'name': {'type': 'string',
                             'description': '行业或概念名称, 如「半导体」「白酒」。'
                                            '不确定全名时可以先给个关键词'},
                    'kind': {'type': 'string',
                             'description': '视角: trend(默认, 均线多头状态) / '
                                            'kline(合成指数日K) / breakout(三档左峰突破)'},
                },
                'required': ['name'],
            },
        },
    },
    {
        'type': 'function',
        'function': {
            'name': 'get_market',
            # v2.120: 由 get_index 扩成"指数 + 市场情绪" —— 它们回答的是同一个问题。
            # ⚠️ 终端只跟这 7 个指数 (server.INDEX_NAMES)。描述里别写"所有指数" ——
            #    模型会去报上证50/中证1000 这种, 每次都扑空。
            'description': '查大盘。**留空**时一次给全两样: 终端跟踪的 7 个指数(上证指数、深证成指、'
                           '创业板指、科创50、沪深300、中证500、科创综指)的点位涨跌, '
                           '**外加**市场情绪面板: 情绪分、上涨板块占比、量能档、成交额与预测、'
                           '沪深大盘趋势档。问到"大盘/指数/今天市场怎么样""情绪如何"'
                           '"放量还是缩量""成交额多少"都必须调这个 —— 不要用 get_realtime。'
                           '传具体指数名 = 只答那一个指数(不给全市场情绪)。',
            'parameters': {
                'type': 'object',
                'properties': {
                    'name': {'type': 'string',
                             'description': '指数名称或代码, 如「上证指数」「创业板」「sh000001」。'
                                            '要看大盘概况+市场情绪就留空'},
                },
                'required': [],
            },
        },
    },
    {
        'type': 'function',
        'function': {
            'name': 'get_news',
            # ⚠️ 描述里必须写死"不是联网搜索" —— 否则模型会把"池子里没有"
            #    说成"世界上没有这条消息"。工具输出里也写了同一句, 两头堵。
            'description': '查财经新闻。**这不是联网搜索** —— 只能看到终端已经拉取的'
                           '最新约 200 条 (财联社 + 同花顺, 滚动覆盖约一两天)。'
                           '问"今天有什么消息""最近有什么政策""某某公司有什么新闻"时调这个。'
                           'keyword 留空 = 返回最新一批 (适合"今天有什么消息"); '
                           '给关键词则按 标题 / 正文 / 相关个股 搜。'
                           '搜不到时如实说"终端新闻池里没有", 并提醒这不是全网搜索。',
            'parameters': {
                'type': 'object',
                'properties': {
                    'keyword': {'type': 'string',
                                # ⚠️ 实测: 不写明「要短」的话, 模型会把用户**整句话**
                                #    (「科技板块的消息」)原样传下来 → 0 命中。
                                'description': '关键词。**用 2~4 字的短词, 一次一个概念**: '
                                               '「半导体」「算力」「降准」「宁德时代」。'
                                               '**不要传整句话** —— 「科技板块的消息」这种'
                                               '搜不到。搜不到就换个更短的词再调一次。'
                                               '留空 = 最新一批'},
                    'limit': {'type': 'integer',
                              'description': '最多返回几条, 默认 12, 上限 30'},
                },
                'required': [],
            },
        },
    },
    # ── v2.120 新增 5 个 ───────────────────────────────────────────────
    {
        'type': 'function',
        'function': {
            'name': 'get_scan',
            # ⚠️ 必须写死"不会触发扫描" —— 否则模型会以为调一下就能让终端开扫,
            #    然后拿空结果去编。工具输出里也写了同一句, 两头堵。
            'description': '看终端**上次扫描**的选股结果(五框架那几张表)。'
                           '问"今天扫描出什么了""选股结果""有哪些票入选"时调这个。'
                           '⚠️ 它**不会触发扫描** —— 只是读上次的结果。没有结果时'
                           '如实告诉用户"还没扫过, 请在终端上双击模式按钮扫一次"。',
            'parameters': {
                'type': 'object',
                'properties': {
                    'mode': {'type': 'string',
                             'description': '扫描模式, 留空=上次扫的那个。'
                                            '100=趋势主升 / 55=趋势延续 / 20=趋势反转 / '
                                            'ai=涨停突破 / smart=涨停回调 / dragon=龙头战法'},
                    'top': {'type': 'integer',
                            'description': '返回前几只, 默认 20, 范围 5~50'},
                },
                'required': [],
            },
        },
    },
    {
        'type': 'function',
        'function': {
            'name': 'get_sim',
            # ⚠️ 写死"只读" —— 用户问"帮我买一手"时模型必须知道它没有这个能力。
            'description': '看模拟盘: 总资产/收益、持仓明细(成本/现价/浮动盈亏/止损止盈)、'
                           '今日操作记录。问"我模拟盘怎么样""持仓赚了还是亏了"时调这个。'
                           '⚠️ **只读**, 你不能替用户买卖或重置 —— 要下单请让他在终端上操作。',
            'parameters': {'type': 'object', 'properties': {}, 'required': []},
        },
    },
    {
        'type': 'function',
        'function': {
            'name': 'get_watchlist',
            'description': '看用户的自选股列表(含实时价)。问"我自选股里那几只""我关注的票"'
                           '时调这个。要其中某一只的详细走势, 再用 get_kline / get_stock_info。',
            'parameters': {'type': 'object', 'properties': {}, 'required': []},
        },
    },
    {
        'type': 'function',
        'function': {
            'name': 'get_stock_info',
            'description': '查个股的基本面与筹码: EPS、ROE、营收/净利同比、总市值/流通市值, '
                           '以及股东户数(户数变化 + 筹码集中/分散标签, 季度口径)。'
                           '问"这票基本面如何""业绩怎么样""筹码集中还是分散""股东户数"时调这个。'
                           '⚠️ 数据有报告期, 回答时必须带上报告期, 别说成"最新"。',
            'parameters': {
                'type': 'object',
                'properties': {'code': {'type': 'string',
                                        'description': '6位股票代码, 如「600206」'}},
                'required': ['code'],
            },
        },
    },
    {
        'type': 'function',
        'function': {
            'name': 'get_intraday',
            'description': '查**当日分时走势**(只有当天, 给的是采样后的价格路径)。'
                           '问"今天分时怎么走的""盘中走势强不强""一直在均线上方吗"时调这个。'
                           '与 get_realtime 的分工: 那个是"此刻一枪"(现价/涨跌幅), '
                           '这个是"今天怎么走过来的"。',
            'parameters': {
                'type': 'object',
                'properties': {'code': {'type': 'string',
                                        'description': '6位股票代码, 也可以是指数代码'}},
                'required': ['code'],
            },
        },
    },
]


# ============================================================
# 对话主入口
# ============================================================
def _clip_history(messages):
    """只留最近 MAX_HISTORY 条, 且**必须从 user 开始** ——
    tool 消息若失去它前面的 assistant(tool_calls), 上游会以 400 挡回。"""
    ms = [m for m in (messages or []) if isinstance(m, dict) and m.get('role') in
          ('user', 'assistant')]
    ms = ms[-MAX_HISTORY:]
    while ms and ms[0].get('role') != 'user':
        ms.pop(0)
    return ms


def chat_stream(slot_name, messages, exec_fn):
    """产出 (event, payload)。event ∈ {meta, tool, think, delta, done, err}。

    slot_name: 'primary' | 'backup' —— **用户选的**, 不做跨槽降级。
    exec_fn:   exec_fn(name, args_dict) -> str, 由 server 传入 (本模块不认识行情)。
    """
    cfg = _an.ai_cfg()
    if not cfg.get('enabled'):
        yield ('err', {'code': 'disabled', 'msg': 'AI 已在数据源设置里关闭'})
        return
    if slot_name not in _an.SLOTS:
        slot_name = 'primary'
    slot = dict(cfg[slot_name])
    slot['_slot'] = slot_name
    st = _an.slot_state(slot)
    if not st['ready']:
        yield ('err', {'code': 'no_key',
                       'msg': '%s 未配置 key (%s)。到 ⬡ 数据源设置 填写, 或切到另一个槽。'
                              % (slot['name'], st['why'])})
        return
    key, _src = _an.resolve_key(slot)

    hist = _clip_history(messages)
    if not hist:
        yield ('err', {'code': 'empty', 'msg': '没有可发送的内容'})
        return

    yield ('meta', {'provider': slot['name'], 'model': slot['model'],
                    'slot': slot_name, 'rounds': MAX_TOOL_ROUNDS})

    msgs = [{'role': 'system', 'content': CHAT_SYSTEM_PROMPT}] + hist
    ccfg = dict(cfg, max_tokens=CHAT_MAX_TOKENS)

    for rnd in range(MAX_TOOL_ROUNDS + 1):
        last = (rnd == MAX_TOOL_ROUNDS)
        # 最后一轮**撤掉工具** → 模型只能用手上已有的信息作答, 循环必然终止
        tools = None if last else TOOLS
        calls, text = [], ''
        try:
            for ev, p in _an._stream_once(slot, key, msgs, ccfg, tools=tools):
                if ev == 'done':
                    calls = p.get('tool_calls') or []
                    text = p.get('text') or ''
                else:
                    yield (ev, p)          # think / delta 原样透传
        except _an._AIErr as e:
            yield ('err', {'code': e.code, 'msg': e.msg, 'partial': bool(text)})
            return

        if not calls:
            yield ('done', {'text': text, 'provider': slot['name'],
                            'model': slot['model'], 'rounds': rnd})
            return

        # ── 模型要查数据: 执行, 把结果回灌, 再来一轮 ──
        wire = []
        for i, c in enumerate(calls):
            cid = c.get('id') or ('call_%d_%d' % (rnd, i))   # 上游没给就自己造, 空 id 会被拒
            wire.append({'id': cid, 'type': 'function',
                         'function': {'name': c.get('name') or '',
                                      'arguments': c.get('args') or '{}'}})
        # ⚠️ assistant 这条必须原样带上 content(哪怕空串) —— 有些上游校验字段存在性
        msgs.append({'role': 'assistant', 'content': text, 'tool_calls': wire})

        for i, c in enumerate(calls):
            name = c.get('name') or ''
            raw = c.get('args') or '{}'
            try:
                args = json.loads(raw) if raw.strip() else {}
                if not isinstance(args, dict):
                    args = {}
            except Exception:
                args = {}
                out = '参数不是合法 JSON: %s —— 请重新调用, 参数必须是 JSON 对象。' % raw[:80]
                yield ('tool', {'name': name, 'args': raw[:120], 'ok': False})
                msgs.append({'role': 'tool', 'tool_call_id': wire[i]['id'], 'content': out})
                continue
            yield ('tool', {'name': name, 'args': args, 'ok': True})
            try:
                out = exec_fn(name, args)
            except Exception as e:                       # 工具炸了不能带走整轮对话
                out = '工具执行出错: %s: %s' % (type(e).__name__, str(e)[:150])
            msgs.append({'role': 'tool', 'tool_call_id': wire[i]['id'],
                         'content': str(out)[:6000]})    # 结果太长会撑爆上下文

    yield ('err', {'code': 'max_rounds', 'partial': True,
                   'msg': '工具调用超过 %d 轮仍未收敛, 已停止。' % MAX_TOOL_ROUNDS})
