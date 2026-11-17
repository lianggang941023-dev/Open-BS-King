# -*- coding: utf-8 -*-
"""
上级行业(90类)趋势全量窗口 —— 落盘模块 v2.100 (2026-09-16)
============================================================
长按「行业趋势」面板标题 2 秒打开的**只读参考窗口**的数据源。

为什么要独立成模块 (而不是塞进 engine/server):
  1. engine.py 的 diff 只剩「1 个新函数 + 3 处单行」, server.py 只剩「1 行 import + 1 条路由」
     —— 评审「绝不影响选股」时一眼可查;
  2. 可以 `python quant_terminal/parent_trend.py force` **在碰 server 和前端之前先单独验证算法**。

🔴 本模块与选股**零关系**:
   * 落盘文件是**自己的** parent_trend.json, 不碰 trend_data.json (那是涨停突破建池/涨停回调建池/
     龙头战法/多头名单四条选股链的数据源, 读方还按 mtime 失效 —— 连"写一份相同内容"都不行);
   * 调用 analyze_sector_ma 时强制 use_pool=False + use_cache=False:
     use_pool=False → 要的是全 90 类, 不按选股池裁;
     use_cache=False → **同时**关掉「查缓存」和「写缓存」。缓存 key 不含 map 身份, 只关 setter
     的话会命中面板预热过的**细分**缓存直接 return, 静默显示 257 行细分;
     setter 关掉也保证**绝不会走到 _persist_slot()** (它内部会写 trend_data.json);
   * 绝不调 _sector_ma_cache_clear() —— 它会顺手清 _MH_CACHE (选股多头名单缓存);
   * 用自己的 _LOCK, 不与选股扫描共享任何锁。

口径: 90 类上级 = industry_map.csv(代码→细分) 用 industry_parent.csv(细分→上级) 折叠一层。
      v5.0 (2026-09-12) 只换了 industry_map.csv 的**数据**(90→257), 代码一行没改 ——
      所以运行时折叠出来的就是原来的老口径 (与 20260902 冻结存档反推比对 5219/5219 一致)。

刷新语义 (用户 2026-09-16 定案):
  * 打开窗口**绝不自动刷新** —— force=False 只读落盘, 秒回;
  * 只有点「▶ 启动刷新」才现算 (force=True), 且**只算当前档位**;
  * 算完落盘, 以后打开都读它, 直到用户再次刷新覆盖;
  * ⚠ 故意**不抄** breakout.py 的 kline_date 自动失效 —— 那边是"K线一更新旧结果自动作废",
    这里用户要的恰恰是"刷完就固化"。kline_last 只记录、只显示, 从不参与判定。
"""
import hashlib
import json
import os
import sys
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from quant_terminal import engine as E  # noqa: E402

# 从 TREND_DATA_FILE 的**目录**推导 —— 自动跟随便携版打包时的 _app_root() 改锚
# (engine.py 的 TREND_DATA_FILE 定义处有这条注意事项), 不留新的打包待办。
PT_FILE = os.path.join(os.path.dirname(E.TREND_DATA_FILE), 'parent_trend.json')

_LOCK = threading.Lock()
STATE = {'running': False, 'lines': '', 'started': '', 'last_err': ''}

VALID_LINES = ('3', '4', '5', '6')


# ============================================================
# 工具
# ============================================================
def _load_json(fpath: str) -> Optional[dict]:
    try:
        with open(fpath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


def _atomic_save(fpath: str, data: dict):
    """原子写。⚠ 与 breakout.py 不同: 这里**不吞异常** —— 用户要的就是"刷完数据落盘",
    写失败必须让调用方知道 (os.replace 是原子的, 失败时旧文件完好无损)。"""
    tmp = fpath + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, fpath)


def _kline_date() -> str:
    """本地K线最新日期 (探针活跃票末行)。★ 只记录、只显示, 绝不参与新鲜度判定。"""
    for c in ('SH600519', 'SZ000001', 'SH600036'):
        try:
            rows = E.load_csv_kline(c)
            if rows:
                return rows[-1]['date']
        except Exception:
            continue
    return ''


def _map_sig(pmap: Dict[str, str]) -> str:
    """口径指纹 = 折叠后 {代码: 上级} 排序后的 md5[:12]。

    ★ 用**内容哈希**不用 mtime: import_ths_snapshot.py 每次跑都会重写这两个 csv,
      mtime 必变 —— 用 mtime 会对"字节完全相同的重导"误报「口径已变」。
    """
    if not pmap:
        return ''
    blob = '\n'.join('%s=%s' % (c, pmap[c]) for c in sorted(pmap))
    return hashlib.md5(blob.encode('utf-8')).hexdigest()[:12]


def _have_lines(d: Optional[dict]) -> List[str]:
    """已刷过的档位标签, 如 ['4线', '5线'] —— 切到没刷过的档位时前端据此提示"""
    ls = (d or {}).get('lines')
    if not isinstance(ls, dict):
        return []
    out = set()
    for k, v in ls.items():
        lab = (v or {}).get('lines') if isinstance(v, dict) else None
        if not lab:
            for ln, mp in E.LINES_MAP.items():
                try:
                    if E._trend_key(mp) == k:
                        lab = ln
                        break
                except Exception:
                    continue
        if lab and str(lab) in VALID_LINES:
            out.add(str(lab) + '线')
    return sorted(out, key=lambda s: int(s[0]))


# ============================================================
# 对外主入口: parent_trend(lines, force, rt)
# ============================================================
def parent_trend(lines: str = '4', force: bool = False, rt: Optional[Dict] = None) -> dict:
    """上级行业(90类)趋势全量表。

    lines: '3'|'4'|'5'|'6' (对应 E.LINES_MAP)
    force: False=只读 parent_trend.json 秒回 (绝不算); True=现算当前档并**合并落盘**
    rt:    {6位代码: 实时价} — 由 server 传入 (本模块不 import server, 免循环依赖)。
           盘中传实时价 → basis='rt'; 非交易窗口传 None → basis='close'。与行业趋势面板同口径。
    """
    if lines not in VALID_LINES:
        return {'ok': False, 'error': "lines 必须是 '3'|'4'|'5'|'6'"}
    mp = E.LINES_MAP[lines]
    key = E._trend_key(mp)

    # ---------------------------------------------------------- 读路径: 绝不算
    if not force:
        d = _load_json(PT_FILE)
        have = _have_lines(d)
        slot = ((d or {}).get('lines') or {}).get(key)
        if not isinstance(slot, dict) or not isinstance(slot.get('rows'), list):
            slot = None
        pmap = E.load_industry_parent_map()
        sig_now = _map_sig(pmap)
        sig_old = (d or {}).get('map_sig') or ''
        return {
            'ok': True, 'cached': True,
            'rows': slot['rows'] if slot else None,
            'lines': lines,
            'have_lines': have,
            'busy': STATE['running'],
            'started': STATE['started'] if STATE['running'] else '',
            # 有档位就报**该档自己的**元信息 —— 只刷当前档, 拿全局 updated_at 会说谎
            'updated_at': (slot or {}).get('updated_at', ''),
            'basis': (slot or {}).get('basis', ''),
            'kline_last': (slot or {}).get('kline_last', ''),
            'file_updated_at': (d or {}).get('updated_at', ''),
            # 口径变了只**提示**, 不自动作废、不自动重算 (用户要求"刷完就固化")
            'map_sig_ok': (not sig_old) or (not sig_now) or (sig_old == sig_now),
            'n_parents': len(set(pmap.values())) if pmap else (d or {}).get('n_parents', 0),
            'parents': E.load_industry_parent(),
            # v2.177: 升降徽标 (与细分/概念窗口同形态) —— 名次差 + 多头数差
            'deltas': (slot or {}).get('deltas') or {},
            'base_date': (slot or {}).get('base_date') or '',
        }

    # ---------------------------------------------------------- 写路径: 只算当前档
    # ⚠ 空 map 守卫放在 _LOCK **之前** —— 数据源坏了就别占锁, 更绝不能写空文件覆盖旧数据
    pmap = E.load_industry_parent_map()
    if not pmap:
        return {'ok': False,
                'error': 'industry_map.csv / industry_parent.csv 缺失或为空 — 未刷新 '
                         '(不影响任何选股)'}

    # ⚠ 非阻塞: 连点/并发时立刻回 busy, 绝不 blocking=True 排队堆积
    if not _LOCK.acquire(blocking=False):
        return {'ok': False, 'busy': True,
                'error': '上级行业刷新进行中, 请稍候...'}
    try:
        STATE['running'] = True
        STATE['lines'] = lines
        STATE['started'] = datetime.now().strftime('%H:%M:%S')
        STATE['last_err'] = ''

        _t0 = time.time()
        rows = E.analyze_sector_ma(pmap, mp, realtime_prices=rt,
                                   use_pool=False, use_cache=False)
        _el = time.time() - _t0
        # ⚠ 算不出东西**不写盘** —— 设计上"不自动重算", 一次写坏就永远修不回来
        if not rows:
            return {'ok': False, 'busy': False,
                    'error': '未算出任何上级行业 (K线缺失?) — 已保留原落盘数据, 未写盘'}

        san = E._sanitize_ma_rows(rows)   # 剔掉 bullish_stocks/drill_stocks 两个大名单
        # v2.177 (2026-10-22): 升降徽标 —— 上级行业也滚双槽基线 (与细分/概念同一套机制,
        #   只是 kind 记为 'parent'), 落 deltas/base_date 进本档 slot 供只读路径秒回。
        _deltas, _base_date = {}, ''
        try:
            _bs = E._load_ma_baseline()
            _slot0 = ((_bs.get('parent') or {}).get(lines) or {})
            _c0 = _slot0.get('cur')
            _today0 = E.baseline_data_date()   # v2.220: 数据日, 不是墙钟日
            _b = ((_slot0.get('prev') or _c0) if (_c0 and _c0.get('date') == _today0)
                  else _c0) or {}
            _base_date = _b.get('date') or ''
            _cur = E._ma_roll_slot('parent', lines, rows)
            E._save_ma_baseline()
            _deltas = _cur.get('deltas') or {}
        except Exception as _e:
            print('[parent_trend] 基线滚动失败: %s' % _e)
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        basis = 'rt' if rt else 'close'
        kd = _kline_date()
        sig = _map_sig(pmap)

        out = _load_json(PT_FILE) or {}
        if not isinstance(out.get('lines'), dict):
            out['lines'] = {}
        # ★ 合并改写, 不是覆盖 —— 刷 4 线不能把之前刷的 5 线冲掉
        out['lines'][key] = {'lines': lines, 'rows': san, 'updated_at': now,
                             'basis': basis, 'kline_last': kd, 'n_rows': len(san),
                             'deltas': _deltas, 'base_date': _base_date}
        out['updated_at'] = now
        out['map_sig'] = sig
        out['n_parents'] = len(set(pmap.values()))
        _atomic_save(PT_FILE, out)

        return {'ok': True, 'cached': False, 'busy': False,
                'rows': san, 'lines': lines,
                'deltas': _deltas, 'base_date': _base_date,
                'updated_at': now, 'basis': basis, 'kline_last': kd,
                'have_lines': _have_lines(out),
                'map_sig_ok': True, 'n_parents': out['n_parents'],
                'elapsed': round(_el, 2),
                'parents': E.load_industry_parent()}
    except Exception as e:
        import traceback
        STATE['last_err'] = traceback.format_exc()
        return {'ok': False, 'busy': False, 'error': '%s: %s' % (type(e).__name__, e)}
    finally:
        STATE['running'] = False
        try:
            _LOCK.release()
        except RuntimeError:
            pass


if __name__ == '__main__':
    # 命令行自测: python quant_terminal/parent_trend.py [3|4|5|6] [force]
    import time
    ln = next((a for a in sys.argv[1:] if a in VALID_LINES), '4')
    fo = ('force' in sys.argv) or ('--force' in sys.argv)
    t0 = time.time()
    r = parent_trend(ln, force=fo)
    print('lines=%s force=%s  耗时 %.1fs' % (ln, fo, time.time() - t0))
    print('ok=%s cached=%s busy=%s' % (r.get('ok'), r.get('cached'), r.get('busy')))
    if not r.get('ok'):
        print('ERR:', r.get('error'))
        sys.exit(1)
    print('updated_at=%s basis=%s kline_last=%s' % (r.get('updated_at'), r.get('basis'),
                                                    r.get('kline_last')))
    print('have_lines=%s n_parents=%s map_sig_ok=%s' % (r.get('have_lines'), r.get('n_parents'),
                                                        r.get('map_sig_ok')))
    rows = r.get('rows')
    if not rows:
        print('rows=None (%s)' % ('该档还没刷过 — 点 ▶ 启动刷新' if r.get('have_lines')
                                  else '从未刷新过 — 点 ▶ 启动刷新'))
        sys.exit(0)
    print('rows=%d' % len(rows))
    if os.path.exists(PT_FILE):
        print('落盘 %s  %d B' % (os.path.basename(PT_FILE), os.path.getsize(PT_FILE)))
    print()
    print('  %-10s %-6s %7s %5s %s' % ('上级行业', '状态', '多头', '占比', '成立日'))
    for x in rows[:20]:
        print('  %-10s %-6s %7s %4s%% %s' % (x['name'], x.get('status') or '-',
                                             '%d/%d' % (x.get('bullish_count', 0), x.get('total', 0)),
                                             x.get('bullish_pct'), x.get('formed_date') or '-'))
