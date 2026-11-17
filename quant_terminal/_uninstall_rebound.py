# -*- coding: utf-8 -*-
"""超跌反弹 · 一键无损摘除 (v2.257, 2026-11-07)

把 server.py / terminal.html 里所有超跌反弹的痕迹还原干净:
  - 标记块 (# >>> REBOUND BEGIN ... # <<< REBOUND END / <!-- >>> REBOUND BEGIN ... -->)
    按行整块切除 (BEGIN 行 + 内容 + END 行)
  - 非标记块的零星改动, 用「反向替换」逐条还原 (new -> old)
  - 反向替换的每条都断言命中次数, 少一处就整体放弃 (不写盘)

用法:
  python _uninstall_rebound.py              # dry-run, 只报会改什么
  python _uninstall_rebound.py --apply      # 真写 (先自动备份)
  python _uninstall_rebound.py --check      # 只检查当前是否装着

摘除后 rebound_scan.py 这个独立新文件不会自动删 —— 不影响终端, 留着也行;
要删就手动 del rebound_scan.py。同理 rebound_off.flag 留不留都行。
"""
import hashlib
import io
import os
import re
import shutil
import sys
import time

QT = os.path.dirname(os.path.abspath(__file__))
SERVER = os.path.join(QT, 'server.py')
HTML = os.path.join(QT, 'terminal.html')
FLAG = os.path.join(QT, 'rebound_off.flag')


def md5(path):
    return hashlib.md5(open(path, 'rb').read()).hexdigest().upper()


# ============================================================
# 一、标记块切除 (整块, 含 BEGIN/END 那两行)
# ============================================================
CUT_NAMES = ('REBOUND', 'WAVE', 'M4')


# ============================================================
# 二、非标记块的零星改动 (反向替换, new -> old; 每条断言命中次数)
#   注意: 本脚本要还原到「超跌反弹上线前」, 所以波段低吸(v2.258)与
#   A股适配(v2.261)的痕迹也必须一起还原 —— 它们的标记块已由 CUT_NAMES 切掉,
#   但散落在 AUCTION 块里的 _m4_bulk/_m4_flat 那几行切不到, 必须单独删。
# ============================================================
SRV_REVS = [
    ('kline-route',
     "self._serve_kline(code, limit=qs.get('limit', ''), mode=qs.get('mode', ''))",
     "self._serve_kline(code, limit=qs.get('limit', ''))", 1),
    ('serve-kline-sig',
     "    def _serve_kline(self, code, limit='', mode=''):",
     "    def _serve_kline(self, code, limit=''):", 1),
]

SRV_DELS = [
    '            _m4_bulk(results)   # v2.261: A\u80a1\u9002\u914d (\u672c\u6a21\u5f0f\u81ea\u5df1\u4e0d\u7b97)\n',
    '            out.update(_m4_flat(r))   # v2.261: \u672c\u6a21\u5f0f\u7684 A\u80a1\u9002\u914d (\u7531 _m4_bulk \u5728\u626b\u63cf\u65f6\u7b97\u597d)\n',
]

HTML_REVS = [
    ('isScanMode',
     ",'rebound'].indexOf(m)>=0",
     "].indexOf(m)>=0", 1),
    ('btnBright',
     "           'glue':'btn-scan-glue','auction':'btn-scan-auction',\n"
     "           'rebound':'btn-scan-rebound'};   // v2.138: \u5747\u7ebf\u7c98\u5408\u4e5f\u662f\"\u5217\u8868\u80fd\u5c55\u5b83\u9009\u51fa\u7684\u80a1\u7968\"\u7684\u4e00\u6863",
     "           'glue':'btn-scan-glue','auction':'btn-scan-auction'};   // v2.138: \u5747\u7ebf\u7c98\u5408\u4e5f\u662f\"\u5217\u8868\u80fd\u5c55\u5b83\u9009\u51fa\u7684\u80a1\u7968\"\u7684\u4e00\u6863", 1),
    ('pool-ternary',
     "||mode==='rebound')?'auto'",
     ")?'auto'", 3),
    ('labels3',
     ",'rebound':'\u8d85\u8dcc\u53cd\u5f39'};",
     "};", 3),
    ('batch-label',
     "'auction':'\u7ade\u4ef7\u6253\u677f',\n                 'rebound':'\u8d85\u8dcc\u53cd\u5f39'};",
     "'auction':'\u7ade\u4ef7\u6253\u677f'};", 1),
    ('batch-modes',
     ",'rebound'],file:",
     "],file:", 1),
    ('peakMark',
     "(m==='auction'?'a':(m==='rebound'?'r':'0'))));}",
     "(m==='auction'?'a':'0')));}", 1),
    ('sortCol',
     "?'auct_pct':((mode==='rebound')?'rebound':'cons_score'));",
     "?'auct_pct':'cons_score');", 1),
    ('sortDir',
     "((mode==='glue')||(mode==='rebound'))?1:-1",
     "(mode==='glue')?1:-1", 1),
]

# v2.259 把老的这行换成了带 &mode= 的版本, 切块后老行不会自己回来 -> 按锚点插回
HTML_INSERTS = [
    ('loadKline-api',
     "  term('\u52a0\u8f7dK\u7ebf: '+code);\n",
     "  var d=await api('/api/kline?code='+code);\n", 1),
]

def cut_blocks(text):
    """切掉所有 CUT_NAMES 里的 BEGIN ... END 块 (Python 的 # 与 HTML 的 <!-- 两种)。

    v2.261: 把 M4 也纳进来 —— 那是「三个新模式补 A股适配」的共用改动, 由 server.py
    的两处 # >>> M4 标记块包住 (import 那一行 + _m4_bulk 函数体), 与具体模式无关,
    谁摘除都得一起切掉。

    返回 (新文本, 切掉的块数)。
    """
    out = []
    n = 0
    lines = text.split('\n')
    i = 0
    while i < len(lines):
        stripped = lines[i].strip()
        name = None
        for _nm in CUT_NAMES:
            if ('>>> ' + _nm + ' BEGIN') in stripped:
                name = _nm
                break
        if name:
            j = i + 1
            while j < len(lines) and (('<<< ' + name + ' END') not in lines[j]):
                j += 1
            if j >= len(lines):
                raise RuntimeError(
                    '\u7b2c %d \u884c\u6709 %s BEGIN \u4f46\u627e\u4e0d\u5230 %s END, '
                    '\u4e0d\u6458\u9664 (\u6587\u4ef6\u53ef\u80fd\u88ab\u624b\u5de5\u6539\u574f\u4e86)'
                    % (i + 1, name, name))
            n += 1
            i = j + 1
            continue
        out.append(lines[i])
        i += 1
    return '\n'.join(out), n

def build(srv, html, verbose=True):
    """把 server.py / terminal.html 的文本还原成「没有超跌反弹」的样子。

    返回 (srv2, html2, ok)。ok=False 表示有反向替换对不上, 调用方必须放弃写入。
    """
    def _p(*a):
        if verbose:
            print(*a)

    # ---- server.py: 切标记块 + 反向还原（签名/路由那两行不能放进块里）----
    ok = True
    srv2, n1 = cut_blocks(srv)
    _p('=== server.py ===')
    _p('  切掉 %d 个标记块, %d 字节 -> %d 字节'
       % (n1, len(srv.encode('utf-8')), len(srv2.encode('utf-8'))))
    # 先跑一遍波段低吸(v2.258/v2.259) 的还原规则 —— 本脚本要回到「超跌反弹上线前」,
    #   而那时波段低吸还没有, 它留下的零散改动必须一并回正。
    try:
        import _uninstall_wave as _UW
        for name, new, old, want in _UW.SRV_REVS:
            got = srv2.count(new)
            if got == want and got > 0:
                srv2 = srv2.replace(new, old)
            elif got != want:
                ok = False
        for _d in _UW.SRV_DELS:
            srv2 = srv2.replace(_d, '')
    except Exception as _e:
        _p('  [BAD] 波段低吸还原规则加载失败: %s' % _e)
        ok = False

    for name, new, old, want in SRV_REVS:
        got = srv2.count(new)
        # 上一步已经用波段低吸的规则还原过了 -> 这里 0 命中且老形式在位, 算 OK
        _done = (got == 0 and old in srv2)
        _p('  [%s] %-16s 回正 %d/%d%s'
           % ('OK ' if (got == want or _done) else 'BAD', name, got, want, ' (已还原)' if _done else ''))
        if not (got == want or _done):
            ok = False
        if got == want and got > 0:
            srv2 = srv2.replace(new, old)
    for _d in SRV_DELS:
        _n = srv2.count(_d)
        _p('  [%s] %-16s 删除 %d%s' % ('OK ', 'm4-line', _n, ' (已还原)' if _n == 0 else ''))
        srv2 = srv2.replace(_d, '')
    srv2 = re.sub(r"\n[ \t]*\n\n(        result = \{)", r"\n\n\1", srv2)
    srv2 = re.sub(r"\n\n\n\ndef run_scan_async", r"\n\ndef run_scan_async", srv2)

    # ---- terminal.html: 先切标记块, 再反向还原零散改动 ----
    html2, n2 = cut_blocks(html)
    _p('=== terminal.html ===')
    _p('  切掉 %d 个标记块' % n2)

    # 同理: html 也先跑一遍波段低吸的还原规则
    try:
        import _uninstall_wave as _UW2
        for name, new, old, want in _UW2.HTML_REVS:
            got = html2.count(new)
            if got == want and got > 0:
                html2 = html2.replace(new, old)
            elif got != want:
                ok = False
        html2, _nw = re.subn(
            r"  \}else if\(headMode==='wave'\)\{\n(?:(?!  \}else\{)[^\n]*\n)*", '', html2)
        for name, anchor, back, want in _UW2.HTML_INSERTS:
            if html2.count(anchor) != want:
                ok = False
            elif (anchor + back) not in html2:      # 幂等: 已插过就不重复插
                html2 = html2.replace(anchor, anchor + back)
    except Exception as _e2:
        _p('  [BAD] 波段低吸 html 还原规则失败: %s' % _e2)
        ok = False

    for name, new, old, want in HTML_REVS:
        got = html2.count(new)
        _done = (got == 0 and old in html2)
        flag = 'OK ' if (got == want or _done) else 'BAD'
        if not (got == want or _done):
            ok = False
        _p('  [%s] %-16s 回正 %d/%d' % (flag, name, got, want))
        if got == want and got > 0:
            html2 = html2.replace(new, old)

    # v2.259: 把「被块替换掉的老行」按锚点插回去 (锚点行本身不动)
    for name, anchor, back, want in HTML_INSERTS:
        got = html2.count(anchor)
        if got != want:
            ok = False
            _p('  [BAD] %-16s 锚点 %d/%d' % (name, got, want))
            continue
        if (anchor + back) in html2:                # 幂等: 已插过就不重复插
            _p('  [OK ] %-16s 已存在, 跳过' % name)
            continue
        html2 = html2.replace(anchor, anchor + back)
        _p('  [OK ] %-16s 插回 1 行' % name)

    # 兜底: resetTableHead 里 }else if(headMode==='rebound'){ ... } 整段删除
    #   (表头列文案会随需求变, 硬编码整段匹配容易失效 -> 结构化扫尾)
    _before = len(html2)
    html2, _n_rbd = re.subn(
        r"  \}else if\(headMode==='rebound'\)\{\n"
        r"(?:(?!  \}else\{)[^\n]*\n)*",
        '', html2)
    if _n_rbd:
        _p('  [OK ] %-16s 删除 %d 个 rebound 表头分支 (-%d 字节)'
           % ('resetTableHead-rbd', _n_rbd, _before - len(html2)))
    else:
        _p('  [-- ] %-16s 无残留' % 'resetTableHead-rbd')

    # 兜底: 原来那颗 disabled 占位按钮整行都在标记块里, 切块会一起带走 -> 这里补回来
    _PH = ('        <button class="btn" disabled ondblclick="" '
           'id="btn-scan-rebound" style="font-size:8px;padding:2px 5px;flex:1;'
           'opacity:.45;cursor:not-allowed" title="超跌反弹 · 占位, 后续实现">'
           '超跌反弹</button>')
    _PH_W = ('        <button class="btn" disabled ondblclick="" '
            'id="btn-scan-wave" style="font-size:8px;padding:2px 5px;flex:1;'
            'opacity:.45;cursor:not-allowed" title="波段低吸 · 占位, 后续实现">'
            '波段低吸</button>')
    if 'btn-scan-rebound' not in html2 or 'btn-scan-wave' not in html2:
        _lines = html2.split('\n')
        _anchor = -1
        for _i, _ln in enumerate(_lines):
            if '<!-- <<< AUCTION END' in _ln:
                _anchor = _i
                break
        _div = -1
        if _anchor > 0:
            for _i in range(_anchor - 1, max(_anchor - 13, -1), -1):
                if _lines[_i].strip() == '</div>':
                    _div = _i
                    break
        if _div < 0:
            _p('  [BAD] %-16s 找不到竞价按钮组, 补不回占位按钮' % 'placeholder')
            ok = False
        else:
            _ins = []
            if 'btn-scan-rebound' not in html2:
                _ins.append(_PH)
            if 'btn-scan-wave' not in html2:
                _ins.append(_PH_W)
            _lines[_div:_div] = _ins
            html2 = '\n'.join(_lines)
            _p('  [OK ] %-16s 补回 %d 颗 disabled 占位按钮' % ('placeholder', len(_ins)))

    # 兜底: renderTable 里 }else if(mode==='rebound'){ ... } 整段删除
    _before2 = len(html2)
    html2, _n_row = re.subn(
        r"    \}else if\(mode==='rebound'\)\{\n"
        r"(?:(?!    \}else\{)[^\n]*\n)*",
        '', html2)
    if _n_row:
        _p('  [OK ] %-16s 删除 %d 个 rebound 行渲染分支 (-%d 字节)'
           % ('renderTable-rbd', _n_row, _before2 - len(html2)))
    else:
        _p('  [-- ] %-16s 无残留' % 'renderTable-rbd')

    # 兜底: 更严格的残留检查
    #   注意「超跌反弹」这个词在改动前就有 3 处 (预留按钮的注释 + 按钮 title + 按钮文字),
    #   所以不能拿它当残留信号; 只查本模式独有的标识符。
    for _needle in ("viewCached('rebound')", "startScan('rebound')",
                    "btn-bright':('rebound'", "renderReboundBar(",
                    "rebound_info", "rebound-back",
                    "mode==='rebound'", "headMode==='rebound'",
                    "'rebound':'超跌反弹'", "+'rebound'", "'rebound'"):
        if _needle in html2:
            _p('  [注意] 残留: %s' % _needle)
            ok = False

    return srv2, html2, ok


def main():
    mode = 'dry'
    if '--apply' in sys.argv:
        mode = 'apply'
    if '--check' in sys.argv:
        mode = 'check'

    srv = io.open(SERVER, 'r', encoding='utf-8', newline='').read()
    html = io.open(HTML, 'r', encoding='utf-8', newline='').read()

    srv_blocks = srv.count('>>> REBOUND BEGIN')
    html_blocks = html.count('>>> REBOUND BEGIN')
    print('=== 当前状态 ===')
    print('server.py     REBOUND 标记块 %d 个' % srv_blocks)
    print('terminal.html REBOUND 标记块 %d 个' % html_blocks)
    print('压舱开关 %s : %s' % (FLAG, '存在' if os.path.exists(FLAG) else '不存在'))
    print()
    if mode == 'check':
        print('装着' if (srv_blocks or html_blocks) else '已经是干净的')
        return 0
    if not srv_blocks and not html_blocks:
        print('没有找到任何 REBOUND 标记块, 无需摘除')
        return 0

    srv2, html2, ok = build(srv, html, verbose=True)

    if not ok:
        print('\n有反向替换错位 -> 整体放弃, 未写入')
        print('提示: 你可以直接用 _bak_rebound_scan_* 备份目录里的 '
              'server.py / terminal.html 覆盖回去 (那是最干净的还原)')
        return 1

    print('  %d 字节 -> %d 字节'
          % (len(html.encode('utf-8')), len(html2.encode('utf-8'))))
    print('  CRLF=%d (必须为 0)' % html2.count('\r\n'))
    print()

    if mode == 'dry':
        print('dry-run — 加 --apply 才真写')
        return 0

    # ---- 真写: 先备份 ----
    ts = time.strftime('%Y%m%d_%H%M%S')
    bakdir = os.path.join(QT, '_bak_uninstall_rebound_' + ts)
    os.makedirs(bakdir, exist_ok=True)
    shutil.copy2(SERVER, os.path.join(bakdir, 'server.py'))
    shutil.copy2(HTML, os.path.join(bakdir, 'terminal.html'))
    print('备份: %s' % bakdir)

    io.open(SERVER, 'w', encoding='utf-8', newline='').write(srv2)
    io.open(HTML, 'w', encoding='utf-8', newline='').write(html2)
    print('已写入 server.py / terminal.html')
    print()
    print('摘除后字节: server.py %d, terminal.html %d'
          % (len(srv2.encode('utf-8')), len(html2.encode('utf-8'))))
    print()
    print('下一步:')
    print('  1) python -m py_compile server.py')
    print('  2) 重启 8799 (先建 quant_terminal\\.watchdog_off 再全杀)')
    print('  3) 页面上应少了「超跌反弹」按钮')
    return 0


if __name__ == '__main__':
    sys.exit(main())
