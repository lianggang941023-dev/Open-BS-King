# -*- coding: utf-8 -*-
"""一次性回填：给全部 K 线 CSV 补第 9 列「自由流通股本」(万股)  v2.273

用法:
    python quant_terminal/_backfill_free.py            # 只统计不写盘
    python quant_terminal/_backfill_free.py --apply    # 真写

数据源: 通达信 tqs.get_more_info_batch(field_list=['FreeLtgb']) —— 全市场一次 5.3 秒。
规矩(与市值两列完全一致):
  * 表头没有该列 → 追加到末尾
  * 只有**最新一行**有值, 其余行留空
  * 行保持矩形, 纯 LF
"""
import os
import sys

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)

from quant_terminal import mcap  # noqa: E402

TDX_LIB = os.path.join(BASE, 'tdx', 'lib')


def tdx_syms(ddir):
    """本地基名 SH600000 → 通达信 600000.SH"""
    out = []
    for fn in sorted(os.listdir(ddir)):
        if not fn.lower().endswith('.csv'):
            continue
        base = fn[:-4]
        mk, c = base[:2].upper(), base[2:]
        if not c.isdigit():
            continue
        if mk in ('BZ', 'BJ'):
            mk = 'BJ'
        elif mk not in ('SH', 'SZ'):
            continue
        out.append(c + '.' + mk)
    return out


def main():
    apply = '--apply' in sys.argv
    ddir = mcap.daily_dir()
    syms = tdx_syms(ddir)
    print('本地 CSV: %d 个' % len(syms))

    sys.path.insert(0, TDX_LIB)
    try:
        from tdxaidata import tqs
    except Exception as e:
        print('!! 通达信模块加载失败: %s' % e)
        return 2

    cwd0 = os.getcwd()
    try:
        os.chdir(TDX_LIB)
        probe = tqs.get_more_info_batch(stock_list=syms, field_list=['FreeLtgb'])
    finally:
        try:
            os.chdir(cwd0)
        except Exception:
            pass

    if not isinstance(probe, dict):
        print('!! 通达信返回非字典: %r' % type(probe))
        return 2

    ff = {}
    for k, v in probe.items():
        v = v or {}
        try:
            f = float(v.get('FreeLtgb') or 0)
        except (TypeError, ValueError):
            f = 0.0
        if f > 0:
            ff[str(k).split('.')[0]] = f
    print('通达信拿到 FreeLtgb: %d 只 / 请求 %d 只' % (len(ff), len(syms)))
    if not ff:
        print('!! 一只都没拿到, 停手')
        return 2

    stat = {'updated': 0, 'same': 0, 'no_free': 0, 'no_cap': 0, 'error': 0}
    miss = []
    for fn in sorted(os.listdir(ddir)):
        if not fn.lower().endswith('.csv'):
            continue
        base = fn[:-4]
        code = base[2:] if base[:2].upper() in ('SH', 'SZ', 'BZ', 'BJ') else base
        p = os.path.join(ddir, fn)
        t, fc, cur = mcap.read_three(p)
        free = ff.get(code)
        if not free:
            stat['no_free'] += 1
            miss.append(base)
            continue
        if t is None and fc is None:
            stat['no_cap'] += 1      # 连市值都没有 → 不动这个文件(避免造出畸形)
            continue
        if cur is not None and abs(cur - free) < 1e-6:
            stat['same'] += 1
            continue
        if not apply:
            stat['updated'] += 1
            continue
        wrote, why = mcap.write_one(code, t, fc, force=True, free=free)
        if wrote:
            stat['updated'] += 1
        elif why == 'same':
            stat['same'] += 1
        else:
            stat['error'] += 1
            print('  !! %s %s' % (base, why))

    mcap.invalidate()
    print('%s: %s' % ('已写入' if apply else '待写入(试运行)', stat))
    if miss:
        print('无 FreeLtgb 的票(%d): %s' % (len(miss), ' '.join(miss[:20])))
    return 0


if __name__ == '__main__':
    sys.exit(main())
