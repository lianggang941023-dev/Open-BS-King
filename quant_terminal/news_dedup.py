# -*- coding: utf-8 -*-
"""新闻跨源去重 (v2.218, 2026-10-24).

背景: 终端同时接财联社 + 同花顺, 两个源大量重复 —— 实测 482 条里
      同花顺有 43 条 (18.5%) 财联社已经报过。两者标题从不**完全**相同
      ("财联社10月24日电," 前缀 / 全角逗号 / "猛跌" vs "下跌"), 纯文字比对
      在 0.95 阈值上只抓得到 13 对, 降到 0.55 又会把
      "东方财富:Q3净利+8.86%" 和 "菲利华:Q3净利+62.34%" 这种同模板不同公司误杀。

策略: 三层, 从便宜到贵。
  ① T1 机器直合并 —— 归一化后相似度 >= 0.95 (就是标点/前缀差异), 零风险不送 AI。
  ② 股票名闸 —— 两边标题的"主体"(冒号前)都是**本地股票名表里的股票名**且不相等,
     直接判不同, 不送 AI (这一条拦掉 27/77 对, 全是同模板不同公司)。
  ③ 剩下的送 AI, 40 对一批。提示词要求"主体/数字/方向/口径"四项全对才算同一件事,
     拿不准判 false —— 漏合并只是多一条, 误合并会让用户漏看新闻。

合并规则 (用户 2026-10-24 定案): **留信息量多的那条**。
  评分 = 正文长度*2 + 标题长度; 平手优先留财联社 (带"财联社X月X日电"时间戳)。

降级: AI 不可用 / 超时 / 解析失败 ⇒ **原样返回, 一条都不合并**,
      并把原因放进 info['warn'], 由前端写到界面底部日志栏。
      判定结果落盘 news_dedup.json, 同一条配对下次不再花钱重判。

接口:
  apply(items)        -> (new_items, info)   主入口, **永不抛异常**
  kick(items)         -> 后台线程补判未知配对
  stats()             -> 缓存规模
"""
import io
import json
import os
import re
import threading
import time
import difflib

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE_FILE = os.path.join(HERE, 'news_dedup.json')
CACHE_KEEP = 4000          # 配对判定结果上限 (约两三天的量)
T1_SIM = 0.95              # 机器直合并阈值
CAND_SIM = 0.50            # 低于此不进候选 (实测 0.50 以下的真重复为 0)
CAND_WINDOW = 1800         # 秒: 只比时间相近的 (±30 分钟)
BATCH = 40                 # 一次送 AI 多少对
FAIL_BACKOFF = 600         # 秒: AI 全链失败后这么久内不再重试 (用户: 「deepseek 也失败就暂时放弃去重」)
WARN_TTL = 600             # 秒: 失败原因进底部日志栏的时限 (过了不再刷, 免得每 5 分钟一条)

_CLS_PREFIX = re.compile(r'^(财联社\d{1,2}月\d{1,2}日电[，,：:]?|（财联社）|\(财联社\)|【[^】]{0,20}】|据[^，,]{2,14}[，,])')
_NONWORD = re.compile(r'[^\u4e00-\u9fa5A-Za-z0-9%]')
_SUBJ = re.compile(r'^([\u4e00-\u9fa5A-Za-z0-9*＆]{2,14})[：:]')

_CACHE = {}                # 'idA|idB' -> 1(同一件事) / 0(不同)
_CACHE_TS = {}             # 'idA|idB' -> 判定时间
_LOCK = threading.RLock()
_RUNNING = {'on': False, 'ts': 0.0, 'msg': '', 'err': '', 'slots': [], 'prov': '',
            'fail_ts': 0.0}
_NAMES = {'ts': 0.0, 'set': set()}


def _norm(s):
    """归一化: 去"(财联社X月X日电,"等前缀, 只留中文/字母/数字/百分号。"""
    s = _CLS_PREFIX.sub('', s or '')
    return _NONWORD.sub('', s)


def _sim(a, b):
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, a, b).ratio()


def _subj(title):
    m = _SUBJ.match((title or '').strip())
    return m.group(1) if m else ''


def _stock_names():
    """本地股票名集合 (同花顺标的表, 5580 条)。失败返回空集 —— 只是少了②这道闸, 不影响正确性。"""
    now = time.time()
    if _NAMES['set'] and (now - _NAMES['ts']) < 3600:
        return _NAMES['set']
    try:
        from quant_terminal import sector_kline as _sk
        t = _sk.tickers() or {}
        s = set(n.strip() for n in t.values() if n and len(n.strip()) >= 2)
        if s:
            _NAMES['set'] = s
            _NAMES['ts'] = now
            return s
    except Exception:
        pass
    return _NAMES['set']


CACHE_VER = 2      # ⚠️ 改 PROMPT 必须 +1: 否则旧判定会被无限复用, 错判永远修不掉


def _key(a, b):
    x, y = str(a), str(b)
    k = (x + '|' + y) if x < y else (y + '|' + x)
    return 'v%d|%s' % (CACHE_VER, k)


def _load():
    global _CACHE
    try:
        with io.open(CACHE_FILE, encoding='utf-8') as f:
            d = json.load(f)
        if isinstance(d, dict):
            _CACHE = {k: int(v) for k, v in d.items() if str(v) in ('0', '1')}
    except Exception:
        _CACHE = {}


def _save():
    try:
        with _LOCK:
            if len(_CACHE) > CACHE_KEEP:
                # 超出上限: 丢掉较早的 (dict 保持插入序, 直接砍头)
                for k in list(_CACHE.keys())[:len(_CACHE) - CACHE_KEEP]:
                    _CACHE.pop(k, None)
            tmp = CACHE_FILE + '.tmp'
            with io.open(tmp, 'w', encoding='utf-8', newline='\n') as f:
                f.write(json.dumps(_CACHE, ensure_ascii=False))
            os.replace(tmp, CACHE_FILE)
    except Exception:
        pass


def _info_score(it):
    """信息量: 正文为主 + 标题为辅。"""
    return len(it.get('body') or '') * 2 + len(it.get('title') or '')


def _pair_list(items):
    """找跨源候选对。返回 [(sim, cls_item, ths_item)] 按相似度降序, 每条 ths 只留最像的那条 cls。"""
    cls = [x for x in items if x.get('src') == 'cls' and x.get('title')]
    ths = [x for x in items if x.get('src') == 'ths' and x.get('title')]
    if not cls or not ths:
        return []
    pre_c = [(x, _norm(x['title']), _norm((x.get('body') or '')[:300])) for x in cls]
    best = {}
    for b in ths:
        bn = _norm(b['title'])
        bb = _norm((b.get('body') or '')[:300])
        bt = b.get('t') or 0
        top = None
        for a, an, ab in pre_c:
            if abs((a.get('t') or 0) - bt) > CAND_WINDOW:
                continue
            s = max(_sim(an, bn), _sim(ab, bb) * 0.95)
            if s >= CAND_SIM and (top is None or s > top[0]):
                top = (s, a, b)
        if top:
            best[b.get('id')] = top
    return sorted(best.values(), key=lambda p: -p[0])


def _prefilter(pairs):
    """T1 直接合并 + 股票名闸。返回 (auto_same, need_ai)。"""
    names = _stock_names()
    auto, todo = [], []
    for s, a, b in pairs:
        if s >= T1_SIM:
            auto.append((a, b))
            continue
        sa, sb = _subj(a.get('title')), _subj(b.get('title'))
        if sa and sb and sa != sb and (sa in names) and (sb in names):
            with _LOCK:
                _CACHE[_key(a.get('id'), b.get('id'))] = 0      # 不同公司, 记下来下次不用再想
            continue
        todo.append((s, a, b))
    return auto, todo


PROMPT = (
    '任务：判断下面 {N} 组（A/B）是否在报道**同一件事**。\n\n'
    '判定「同一件事」(true) 必须同时满足：\n'
    '1) 主体相同（同一家公司/机构/国家/品种）；\n'
    '2) 核心数字与方向一致（数值、同比/环比、增长/下降都要能对上）；\n'
    '3) 口径相同（"前三季度" 与 "第三季度" 是不同口径，不算同一件事）；\n'
    '4) 说的是同一动作/同一结论，不是同一事件的不同进展（如"启动应急响应" vs "开展会商调度"是不同进展）。\n\n'
    '以下一律判 false：\n'
    '- 主体不同（哪怕句式一模一样）；\n'
    '- 同一主体但数字/方向/口径不同；\n'
    '- 同一事件但 A 说的是响应措施、B 说的是会商调度这类不同进展；\n'
    '- 条数统计不同（如"要闻20条" vs "要闻17条"）；\n'
    '- 地震震级不同。\n\n'
    '特别注意下面三种"长得极像但是两件事"的情况，**一律 false**：\n'
    '  ① 累计口径 vs 单季口径：\n'
    '     A "金海通：前三季度净利润同比增长159.03%"\n'
    '     B "金海通：2026年第三季度净利润1.53亿元，同比增长211.52%"  → false\n'
    '  ② 环比 vs 同比（同一季度、同一指标，但比较基期不同）：\n'
    '     A "永鼎股份：Q3净利润环比下降30%"\n'
    '     B "永鼎股份：2026年第三季度净利润2.39亿元，同比增长2165.42%"  → false\n'
    '  ③ 同一主体的不同指标（净利润 vs 营收 vs 毛利率）：\n'
    '     A "东方财富：第三季度净利润同比增长8.86%"\n'
    '     B "东方财富：第三季度营业收入同比增长8.86%"  → false\n'
    '另外注意：**一方信息较少不算不同**。只要主体相同、关键数字/方向能对上、'
    '说的是同一个动作，哪怕 B 只是 A 的缩写版（少了地名、少了后半句结论），也应判 true。\n'
    '例：A "武汉天源：联合体中标36.4亿元储能设备+安装一体化项目" / B "武汉天源：联合体中标金额36.4亿元" → true\n'
    '例：A "WTI原油期货结算价为103.27美元/桶，上涨3.80美元，涨幅3.8%" / B "WTI原油期货结算价为103.27美元/桶" → true\n\n'
    '拿不准就判 false —— 漏合并只是多一条，误合并会让用户漏掉新闻。\n\n'
    '只输出 JSON 数组，每项 {"no":序号,"same":true/false}，不要解释。\n\n'
)


_FREE_HINT = ('bigmodel', 'zhipu', 'glm', '智谱')     # 免费源特征 (base/name/model 任意命���)
_PAID_HINT = ('deepseek',)                             # 付费源特征


def _ai_slots(cfg):
    """v2.219: 决定性去重的降级顺序 —— **免费源优先, 付费源垫底**。

    用户定案: 「优先用免费的智谱做合并去重的任务, 如果智谱调用失败, 再切付费的
    deepseek, deepseek 也失败就暂时放弃去重功能」。

    🔴 为什么不直接用配置里的 SLOTS 顺序: 数据源设置里可以「主备互换」,
       换过之后 primary 就是付费源 —— 按配置顺序走会先烧钱。这里**按源特征判断**,
       不看槽位名 (与 v2.215「获取key 按钮按源识别网址」同一条思路)。

    返回 tuple(槽名...), 只含 config 里确实存在的槽; 一个都认不出时退回 SLOTS 全序
    (宁可多试一次, 也不要因为改名就整个不判)。
    """
    known = [s for s in ('primary', 'backup') if isinstance((cfg or {}).get(s), dict)]
    if not known:
        return ()
    free, paid, other = [], [], []
    for s in known:
        p = cfg[s]
        blob = ('%s %s %s' % (p.get('name') or '', p.get('base') or '', p.get('model') or '')).lower()
        if any(h in blob for h in _FREE_HINT):
            free.append(s)
        elif any(h in blob for h in _PAID_HINT):
            paid.append(s)
        else:
            other.append(s)
    order = tuple(free + other + paid)      # 认不出的居中: 既不敢当免费, 也不至于排最后
    return order or tuple(known)


def _judge_chunk(chunk, cfg, slots=None):
    from quant_terminal import ai_news as A
    lines = []
    for i, (s, a, b) in enumerate(chunk):
        lines.append('%d.\n A: %s\n B: %s' % (i + 1, (a.get('title') or '')[:80],
                                               (b.get('title') or '')[:80]))
    msgs = [{'role': 'system', 'content': '你是财经新闻去重助手。只输出严格 JSON 数组，不要 markdown 代码块。'},
            # ⚠ 不能用 `PROMPT % n` —— 提示词里有 "159.03%" 这类百分号, 会被当格式符。
            {'role': 'user', 'content': PROMPT.replace('{N}', str(len(chunk))) + '\n'.join(lines)}]
    text, prov, err = A._complete(msgs, cfg, 1600, slots=slots)
    if err or not text:
        return None, err or '无返回', prov
    m = re.search(r'\[.*\]', text, re.S)
    if not m:
        return None, '返回里没有 JSON 数组', prov
    try:
        arr = json.loads(m.group(0))
    except Exception as e:
        return None, 'JSON 解析失败: %s' % e, prov
    got = {}
    for x in arr:
        try:
            got[int(x.get('no'))] = 1 if x.get('same') else 0
        except Exception:
            continue
    if not got:
        return None, '解析出 0 项', prov
    return got, '', prov


def _bg(chunk, cfg, slots=None):
    """后台判定一批。结果写缓存, 不碰任何全局 items。"""
    try:
        got, err, prov = _judge_chunk(chunk, cfg, slots=slots)
        if got:
            with _LOCK:
                for no, v in got.items():
                    if 1 <= no <= len(chunk):
                        s, a, b = chunk[no - 1]
                        _CACHE[_key(a.get('id'), b.get('id'))] = v
            _save()
            # 记下**实际用的是哪家** (免费/付费) —— 事后从 /api/news/dedup 能直接看出来
            _RUNNING.update({'msg': '判定完成 %d 对 (%s)' % (len(got), prov or 'AI'),
                             'prov': prov or '', 'fail_ts': 0.0})
        else:
            _RUNNING.update({'err': err or '判定失败', 'prov': '', 'fail_ts': time.time(),
                             'msg': '判定失败 (免费源与付费源都挂了, 本轮不合并): %s' % (err or '')})
    except Exception as e:
        _RUNNING.update({'err': '%s: %s' % (type(e).__name__, e), 'fail_ts': time.time()})
    finally:
        _RUNNING['on'] = False


def kick(items):
    """后台补齐未知配对。已在跑就不叠加; **上轮全链失败则退避一段时间不再重试**。

    v2.219: 用户定案「deepseek 也失败就暂时放弃去重功能」—— 放弃不是永久关掉,
    而是退避 FAIL_BACKOFF 秒: 这期间新闻照常出 (只是不合并), 也不去反复锤一个
    已经挂掉的 AI 接口。退避到期后自动重试一次; 成功则 fail_ts 清零。
    """
    if _RUNNING.get('on'):
        return dict(_RUNNING, ok=False)
    _ft = float(_RUNNING.get('fail_ts') or 0)
    if _ft and (time.time() - _ft) < FAIL_BACKOFF:
        _RUNNING['on'] = False
        _RUNNING['msg'] = 'AI 判定暂停 (上次失败, 还要等 %d 秒再试)' % int(FAIL_BACKOFF - (time.time() - _ft))
        return dict(_RUNNING, ok=False)
    try:
        from quant_terminal import ai_news as A
        cfg = A.ai_cfg()
        if not cfg.get('enabled'):
            _RUNNING.update({'err': 'AI 已关闭', 'msg': 'AI 已关闭, 跳过跨源去重'})
            return dict(_RUNNING, ok=False)
    except Exception as e:
        _RUNNING.update({'err': '%s: %s' % (type(e).__name__, e)})
        return dict(_RUNNING, ok=False)
    pairs = _pair_list(items)
    _, todo = _prefilter(pairs)
    unknown = [p for p in todo if _key(p[1].get('id'), p[2].get('id')) not in _CACHE]
    if not unknown:
        _RUNNING.update({'msg': '没有待判配对', 'err': ''})
        return dict(_RUNNING, ok=False)
    chunk = unknown[:BATCH]
    _slots = _ai_slots(cfg)          # v2.219: 免费优先
    if not _slots:
        _RUNNING.update({'err': '无可用 AI 槽', 'msg': '无可用 AI 槽, 本轮跳过跨源去重'})
        return dict(_RUNNING, ok=False)
    _names = [(cfg.get(s) or {}).get('name') or s for s in _slots]
    _RUNNING.update({'on': True, 'ts': time.time(), 'err': '', 'slots': list(_slots), 'prov': '',
                     'msg': '判定中 %d 对… (顺序 %s)' % (len(chunk), '→'.join(_names))})
    threading.Thread(target=_bg, args=(chunk, cfg, _slots), daemon=True).start()
    return dict(_RUNNING, ok=True, n=len(chunk), left=max(0, len(unknown) - len(chunk)))


def apply(items):
    """主入口。返回 (去重后的 items, info)。**永不抛异常** —— 去重挂了新闻照常出。"""
    info = {'merged': 0, 't1': 0, 'ai': 0, 'pending': 0, 'warn': '', 'src': ''}
    try:
        if not items:
            return items, info
        pairs = _pair_list(items)
        if not pairs:
            info['src'] = '跨源候选 0 对, 无需去重'
            return items, info
        auto, todo = _prefilter(pairs)
        same = [(a, b) for a, b in auto]
        n_t1 = len(same)          # 机器直合并的对数 (含上次已判定的), 后面算进 info
        unknown = []
        for s, a, b in todo:
            v = _CACHE.get(_key(a.get('id'), b.get('id')))
            if v == 1:
                same.append((a, b))
            elif v == 0:
                pass
            else:
                unknown.append((s, a, b))
        if unknown:
            info['pending'] = len(unknown)
            try:
                kick(items)          # 后台补判, 不阻塞本次返回
            except Exception:
                pass
            # v2.219: 上轮 AI 判定彻底失败 (免费源与付费源都挂了) **且就在刚才**,
            #   把原因带进 warn 让前端日志栏看见 —— 用户要的是「失败要有提示」。
            #   ⚠️ 限 10 分钟内: 过期的失败不该每 5 分钟轮询都刷一条。
            _e, _fts = _RUNNING.get('err') or '', float(_RUNNING.get('fail_ts') or 0)
            if _e and not _RUNNING.get('on') and _fts and (time.time() - _fts) < WARN_TTL:
                info['warn'] = '跨源去重暂停 (AI 不可用): %s' % _e
        if not same:
            info['src'] = '候选 %d 对, 已判定合并 0 对, %d 对待判' % (len(pairs), info['pending'])
            return items, info
        # 合并: 每条 ths 只挂到一个 cls 上 (候选生成时已经一对一)
        drop = set()
        bump = {}
        for a, b in same:
            if a.get('id') in drop or b.get('id') in drop:
                continue
            if _info_score(a) >= _info_score(b):
                keep, gone = a, b
            else:
                keep, gone = b, a
            drop.add(gone.get('id'))
            bump[keep.get('id')] = bump.get(keep.get('id'), 0) + 1 + int(gone.get('dup') or 0)
        if not drop:
            return items, info
        out = []
        for it in items:
            if it.get('id') in drop:
                continue
            it2 = it
            if it.get('id') in bump:
                it2 = dict(it)
                it2['dup'] = bump[it.get('id')]
            out.append(it2)
        info['merged'] = len(drop)
        info['t1'] = n_t1
        info['ai'] = len(same) - n_t1
        if n_t1 and info['ai']:
            _how = '机器 %d + AI %d' % (n_t1, info['ai'])
        elif info['ai']:
            _how = 'AI %d' % info['ai']
        else:
            _how = '机器 %d' % n_t1
        info['src'] = '合并 %d 条 (%s)' % (len(drop), _how)
        return out, info
    except Exception as e:
        info['warn'] = '去重跳过 (%s: %s)' % (type(e).__name__, e)
        return items, info


def stats():
    return {'cached_pairs': len(_CACHE), 'running': _RUNNING.get('on'),
            'msg': _RUNNING.get('msg', ''), 'err': _RUNNING.get('err', ''),
            'slots': _RUNNING.get('slots') or [], 'prov': _RUNNING.get('prov', ''),
            'fail_ts': _RUNNING.get('fail_ts') or 0.0,
            'same': sum(1 for v in _CACHE.values() if v == 1),
            'diff': sum(1 for v in _CACHE.values() if v == 0)}


_load()
