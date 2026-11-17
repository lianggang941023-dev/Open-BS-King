# -*- coding: utf-8 -*-
"""摘除脚本还原度校验 (v2.264 2026-11-08 更新)

把 _uninstall_wave.build() / _uninstall_rebound.build() 跑一遍, 结果与各自的
「装这个模式之前」的备份逐行 diff。

v2.264 之后 diff 里有若干行是**预期的**: 用户要求把「行业突破/概念突破」和
「超跌反弹/波段低吸」换个位置, 按钮行整体搬了家。那是比备份更新的改动,
摘除模式时不应该把它一起还原回去。

所以校验口径改成「按行号定位」:
  (A) server.py 必须逐字节一致 (diff == 0 行)
  (B) html 的所有差异行必须落在**按钮区行号窗口**内 (默认 1670~1720)
      窗口外只要有一行差异就 [BAD]

行号窗口取「备份与产物两边取并集」的宽松范围, 因为按钮行搬过家两侧位置略有不同。
"""
import sys, io, difflib, re

sys.path.insert(0, '.')
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import _uninstall_wave as UW
import _uninstall_rebound as UR

srv = io.open('server.py', encoding='utf-8', newline='').read()
html = io.open('terminal.html', encoding='utf-8', newline='').read()

WIN = (1660, 1730)          # 按钮区行号窗口 (1-based, 闭区间)

CASES = (('wave', UW, r'D:\AI Working\BS King\_bak_wave_scan_20261107_212145'),
         ('rebound', UR, r'D:\AI Working\BS King\_bak_rebound_scan_20261107_171838'))

HUNK = re.compile(r'^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@')


def scan_diff(a_lines, b_lines):
    """返回 (改动行数, 越界改动行列表[(位置, 内容)])"""
    total = 0
    outside = []
    cur_a = cur_b = 0
    for l in difflib.unified_diff(a_lines, b_lines, lineterm='', n=0):
        m = HUNK.match(l)
        if m:
            cur_a = int(m.group(1))
            cur_b = int(m.group(3))
            continue
        if l.startswith(('+++', '---')):
            continue
        if l[:1] == '-':
            pos = ('a', cur_a)
            cur_a += 1
            total += 1
        elif l[:1] == '+':
            pos = ('b', cur_b)
            cur_b += 1
            total += 1
        else:
            cur_a += 1
            cur_b += 1
            continue
        if not (WIN[0] <= pos[1] <= WIN[1]):
            outside.append((pos, l))
    return total, outside


all_ok = True
for name, mod, bakdir in CASES:
    s2, h2, ok = mod.build(srv, html, verbose=False)
    bs = io.open(bakdir + r'\server.py', encoding='utf-8', newline='').read()
    bh = io.open(bakdir + r'\terminal.html', encoding='utf-8', newline='').read()

    srv_total, srv_out = scan_diff(bs.split('\n'), s2.split('\n'))
    html_total, html_out = scan_diff(bh.split('\n'), h2.split('\n'))

    srv_clean = (srv_total == 0)
    html_clean = (len(html_out) == 0)
    status = 'PASS' if (ok and srv_clean and html_clean) else 'FAIL'

    print('%-8s build=%-5s %s' % (name, ok, status))
    print('   server.py : %7d vs bak %7d  diff %d 行  %s'
          % (len(s2.encode('utf-8')), len(bs.encode('utf-8')), srv_total,
             'OK' if srv_clean else 'BAD'))
    print('   terminal  : %7d vs bak %7d  diff %d 行  %s (全部在按钮区 %d~%d 内 = v2.264 换位)'
          % (len(h2.encode('utf-8')), len(bh.encode('utf-8')), html_total,
             'OK' if html_clean else 'BAD', WIN[0], WIN[1]))
    for pos, l in srv_out[:10]:
        print('      SRV  L%d  %s' % (pos[1], l[:140]))
    for pos, l in html_out[:10]:
        print('      OUT  %s L%d  %s' % (pos[0], pos[1], l[:140]))
    print()
    if status == 'FAIL':
        all_ok = False

print('总判定:', 'PASS' if all_ok else 'FAIL')
sys.exit(0 if all_ok else 1)
