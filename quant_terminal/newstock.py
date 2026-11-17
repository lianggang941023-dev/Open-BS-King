# -*- coding: utf-8 -*-
"""v2.249 (2026-11-06 用户 m21716/m21803): 新股自动建档 —— 自动更新也建，不再只靠手动按钮。

## 为什么要有这个模块

原来的两条路各有一个致命缺口：

  * **自动任务**（11:31 / 15:31 快照 → 15:35 写 CSV）拿的是「本地 CSV 文件清单」当全市场代码表
    （`tdx_fetch.py:421 files = os.listdir(daily_dir)`），同花顺回的 5580 只用这份清单去比对，
    对不上的**直接丢弃**；写盘处还写着一行注释「新股不在这里建文件，交给手动那条路」。
    ⇒ 本地没有的永远进不来，死循环。

  * **手动按钮**（`/api/kline/update_snapshot`）里其实有整套建档逻辑
    （`server.py` 的 `new_codes` 发现 + `_create_new_csv`），但它只挂在那个按钮上，
    自动任务从来没调过。

本模块把这套逻辑**抽出来做成公共服务**，自动任务和手动按钮共用一份 ——
免得以后改一处漏一处。

## 口径

* 「全市场」= `sector_kline.tickers()`（同花顺标的表，5580 只，缓存 7 天）。
  用户 m02201 定案：名称表由同花顺提供、每天刷一次，新股最多延后 1 天。
* 建历史走腾讯 `engine._fqkline_req`（内部已做 路径×域名 双维轮换，
  北交所自动走 newfqkline —— 实测 bj920285 能拿 11 根，老 fqkline 只给 1 根）。
* 拉不到的（还没上市交易，如 688827 / 920309 腾讯返 0 根）**静默跳过**，不记为失败 ——
  它们明天再来一次就行，天天报「异常 N 只」只会让人以为坏了。
* 限流：每只之间 sleep 1.3s（沿用原实现的安全线）。一天一般 0~3 只，代价可忽略。
"""
import io
import os
import threading
import time

from . import engine as E
from . import mcap as M

LOCK = threading.Lock()

# 每建一只之间的等待 —— 腾讯 fqkline 有频率风控，原手动实现就是 1.3 秒
GAP = 1.3
# 单轮最多建几只 —— 防某天标的表抽风一次冒出几百只把 15:35 那轮拖死
MAX_ONE_RUN = 40


def _prefix(code6):
    """6 位裸码 → 本地 CSV 基名的前缀形式（与 mcap.prefixed / 全仓规则一致）"""
    c = str(code6)
    if c[:3] == '920' or c[0] in '48':
        return 'BZ' + c
    if c[0] in '69':
        return 'SH' + c
    return 'SZ' + c


def _tencent_sym(base):
    """本地基名 → 腾讯 symbol（engine._fqkline_req 用的形状）"""
    b = str(base)
    if b[:2].upper() in ('BZ', 'BJ'):
        return 'bj' + b[2:]
    if b.startswith('SH'):
        return 'sh' + b[2:]
    return 'sz' + b[2:]


def discover():
    """返回 (待建档列表, 说明) —— 只比对文件名，不碰任何网络。

    待建档项 = `{'code': 'SZ001381', 'num': '001381', 'name': 'C皇冠'}`
    """
    try:
        from . import sector_kline as SK
        tk = SK.tickers() or {}
    except Exception as e:
        return [], f'同花顺标的表不可用: {str(e)[:120]}'
    ddir = M.daily_dir()
    if not os.path.isdir(ddir):
        return [], f'K线目录不存在: {ddir}'
    have = set()
    for f in os.listdir(ddir):
        if f.lower().endswith('.csv'):
            have.add(f[:-4].lower())
    out = []
    for c6, nm in tk.items():
        c6 = str(c6)
        if not (len(c6) == 6 and c6.isdigit()):
            continue
        base = _prefix(c6)
        if base.lower() in have:
            continue
        out.append({'code': base, 'num': c6, 'name': str(nm or '')})
    out.sort(key=lambda x: x['code'])
    return out, ''


def create_one(code, name='', cap=None, fmcap=None, tries=2, free=None):
    """建一只的 CSV。返回 (ok:bool, 原因:str)

    * `code` 传本地基名（'SZ001381' / 'BZ920285'）。裸 6 位也行，会自动补前缀。
    * `cap` / `fmcap` 单位亿元，写进末行第 7/8 列（拿不到就留空，绝不写 0）。
    * v2.273 (用户 m08653): `free` = 自由流通股本(万股), 写进末行第 9 列（拿不到就留空）。
    * 文件已存在一律不覆盖 —— 这是「只补缺」的硬保证。
    """
    code = str(code)
    if len(code) == 6 and code.isdigit():
        code = _prefix(code)
    fp = os.path.join(M.daily_dir(), code + '.csv')
    if os.path.exists(fp):
        return False, '已存在'
    sym = _tencent_sym(code)
    num = code[2:]
    is688 = code.startswith(('SH688', 'SH689'))
    # 北交所与主板都走同一个接口，newfqkline 对 bj 才给全量历史（老 fqkline 只 1 根）
    j = E._fqkline_req(sym, '800')
    bars = []
    if j:
        d = (j.get('data') or {}).get(sym) or {}
        bars = d.get('qfqday') or d.get('day') or []
    if not bars:
        return False, '腾讯无数据'          # 未上市 / 已退市 —— 故意不算失败
    # v2.339 (2026-11-17 用户 m25177): **列定义只此一份** —— 表头行和每一条数据行
    #   都由 HDR 生成, 列号由 HDR.index() 决定, 不再有"字符串模板 + 末尾砍逗号"这种
    #   靠手工数逗号的写法。
    #   踩过的坑: 旧代码 `rows[-1][:-1] + fmt(cap) + ',' + ...` 是「砍掉末尾一个逗号腾位」,
    #   v2.273 把行模板从 8 列(`...,,`)改成 9 列(`...,,,`) 后忘了跟着改 `[:-1]`,
    #   于是只砍掉 1 个逗号、还剩 2 个空位 ⇒ 拼出 10 列, 三个值整体右移一格
    #   (总市值列是空串、流通市值列装的是总市值、自由流通列装的是流通市值)。
    #   BZ920309(N菊乐) 2026-11-17 上市首日就是这样, 全市场只此 1 例 —— 因为
    #   只有新股走这条建档路径。现在列数写错会立刻 IndexError, 不会再静默错列。
    HDR = ['date', 'open', 'close', 'high', 'low', 'volume', M.CAP_T, M.CAP_F, M.FREE]
    _N = len(HDR)
    rows = []
    for b in bars:
        if len(b) < 6 or '-' not in str(b[0]):
            continue
        try:
            v = float(b[5]) / 100 if is688 else float(b[5])
            # 🔴 全矩形：历史行市值两列**留空但仍占位**（不是只写 6 列）。
            #   本仓所有 CSV 都是这个形态（实测 BZ920000 1421 行全是 9 列、
            #   历史行末几列是空串）。先写 6 列再补末行，会留下「前 N-1 行 6 列、
            #   末行 9 列」的畸形文件，读侧按列号取值时容易错位。
            _c = [''] * _N
            _c[0] = str(b[0])
            _c[1] = '%.3f' % float(b[1])
            _c[2] = '%.3f' % float(b[2])
            _c[3] = '%.3f' % float(b[3])
            _c[4] = '%.3f' % float(b[4])
            _c[5] = '%.0f' % v
            rows.append(','.join(_c))
        except (TypeError, ValueError, IndexError):
            continue
    if not rows:
        return False, 'K线解析为空'
    # v2.132 口径：市值/股本只写在末行（其余行空串、占位保持矩形）
    # v2.339: 按列名落值 —— 列序由 HDR 保证, 不会因为多加一列而整体错位。
    _last = rows[-1].split(',')
    if len(_last) != _N:                      # 理论上不可能; 真出现就补齐到 _N, 绝不错列
        _last = (_last + [''] * _N)[:_N]
    _last[HDR.index(M.CAP_T)] = M.fmt(cap)
    _last[HDR.index(M.CAP_F)] = M.fmt(fmcap)
    _last[HDR.index(M.FREE)] = M.fmt(free)
    rows[-1] = ','.join(_last)
    tmp = fp + '.tmp'
    try:
        with io.open(tmp, 'w', encoding='utf-8', newline='\n') as fh:
            fh.write(','.join(HDR) + '\n' + '\n'.join(rows) + '\n')
        os.replace(tmp, fp)
    except Exception as e:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass
        return False, '%s: %s' % (type(e).__name__, str(e)[:80])
    return True, '%d 根 (首 %s)' % (len(rows), rows[0].split(',')[0])


def sync(caps=None, limit=None, verbose=False, log=None):
    """主入口：发现缺口 → 逐个建档。自动任务 15:35 那轮尾部调一次。

    * `caps` = {6位码: (总市值亿, 流通市值亿)}；不传就现取（腾讯 qt，免费、不花积分）
    * 返回 `{'ok','found','skipped','created':[{code,name,bars}],'failed':[{code,name,why}],'note'}`
    """
    say = log or (lambda *a: None)
    res = {'ok': True, 'found': 0, 'skipped': 0, 'created': [], 'failed': [],
           'note': ''}
    if not LOCK.acquire(blocking=False):
        res['ok'] = False
        res['note'] = '另一个建档任务正在跑，跳过'
        return res
    try:
        todo, note = discover()
        res['found'] = len(todo)
        if note:
            # ⚠️ note 同时承担两种含义：①「发现过程本身出错」(该算失败)
            #    ②「有东西被跳过，但结果正常」(如「同花顺 5580 只 / 本地 5578 个」这类说明)。
            #    v2.249a: 原来一律把 ok 打成 False ⇒ 正常跑一轮也显示 ✗。
            #    现在只把「真出错」的措辞当失败（discover 内部出错时 note 以「取不到」开头）。
            res['note'] = note
            if note.startswith('取不到'):
                res['ok'] = False
                return res
        if not todo:
            res['note'] = res['note'] or '无新股'
            return res
        cap_n = min(len(todo), limit or MAX_ONE_RUN)
        if len(todo) > cap_n:
            res['note'] = f'待建 {len(todo)} 只，本轮只做前 {cap_n} 只'
        todo = todo[:cap_n]
        # 市值：没给就现取一次（腾讯 qt，免费）
        if caps is None:
            try:
                caps = _qt_caps([t['num'] for t in todo])
            except Exception as e:
                caps = {}
                say('取新股市值失败（不影响建档）: %s' % str(e)[:100])
        for i, t in enumerate(todo):
            c, nm = t['code'], t['name']
            cc = (caps or {}).get(t['num']) or (None, None)
            ok, why = create_one(c, nm, cap=cc[0], fmcap=cc[1])
            if ok:
                res['created'].append({'code': c, 'name': nm, 'bars': why})
                say('新建 %s %s → %s' % (c, nm, why))
            elif why in ('腾讯无数据', 'K线解析为空', '已存在'):
                res['skipped'] += 1     # 未上市 / 已建 —— 都不算失败
            else:
                res['failed'].append({'code': c, 'name': nm, 'why': why})
                say('建档失败 %s %s → %s' % (c, nm, why))
            if i < len(todo) - 1:
                time.sleep(GAP)
        if verbose:
            say('新股建档：发现 %d，建成 %d，跳过 %d，失败 %d'
                % (res['found'], len(res['created']), res['skipped'], len(res['failed'])))
        return res
    except Exception as e:
        res['ok'] = False
        res['note'] = '%s: %s' % (type(e).__name__, str(e)[:150])
        return res
    finally:
        LOCK.release()


def _qt_caps(nums, batch=50, timeout=8):
    """腾讯 qt 取市值 → {6位码: (总市值亿, 流通市值亿)}。qt 字段 f[44]=流通 f[45]=总。"""
    import urllib.request
    from . import net as _net
    out = {}
    nums = [str(n) for n in nums]
    for i in range(0, len(nums), batch):
        part = nums[i:i + batch]
        q = []
        for c in part:
            if c[:3] == '920' or c[0] in '48':
                q.append('bj' + c)
            elif c[0] in '69':
                q.append('sh' + c)
            else:
                q.append('sz' + c)
        try:
            req = urllib.request.Request('http://qt.gtimg.cn/q=' + ','.join(q),
                                         headers={'User-Agent': 'Mozilla/5.0'})
            txt = _net.open_url(req, timeout=timeout).read().decode('gbk', 'ignore')
        except Exception:
            continue
        for line in txt.split(';'):
            if '="' not in line or '~' not in line:
                continue
            try:
                f = line.split('="', 1)[1].rstrip('"').split('~')
            except IndexError:
                continue
            if len(f) < 46:
                continue
            cd = f[2].strip()
            if not (len(cd) == 6 and cd.isdigit()):
                continue
            try:
                t = float(f[45]) if f[45] not in ('', '--') else None
                fm = float(f[44]) if f[44] not in ('', '--') else None
            except ValueError:
                t = fm = None
            if t or fm:
                out[cd] = (t, fm)
    return out
