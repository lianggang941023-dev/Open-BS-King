"""
BS King Quant Terminal — 版本信息与更新检查
=============================================
v2.340 (2026-11-18 用户 m25750): 建立版本机制。

为什么单独一个模块，而不是塞进 server.py：
  server.py 已经 484 KB、8,968 行。版本号会被三处读（/api/version、数据源窗口、
  命令行 `python -m quant_terminal.version`），塞进 server.py 就得 import 整个服务。

版本号规则
----------
本项目沿用自用版一路用下来的 `v2.NNN`：
  - v2.NNN  小版本，每修一个功能/一个 bug 加一
  - v2.NNNa / v2.NNNb  同一版本的补丁（先 a 后 b）
  - 当前 = 仓库根 `VERSION` 文件里那一行，**改版本只改那个文件**
    这里读文件、不写常量 —— 免得代码里和 VERSION 两处各写一个号、迟早对不上。

`APP_BUILD` 不是版本号，是「这份代码哪一天打的包」，用于排查
「我明明更了怎么还是老界面」这类问题。
"""

import glob
import json
import os
import re
import shutil
import subprocess
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor as _TPE

# 仓库根：本文件在 <root>/quant_terminal/version.py
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VERSION_FILE = os.path.join(BASE_DIR, 'VERSION')

# 更新检查的远端 —— **两条通道，HTTP 优先**。
#
# 2026-11-18 那版只走 git 协议，理由当时是「api/raw 两个域名都被墙」。
# 2026-11-20 重测（用户 m35797 带来一台没装 git 的测试机）发现：
#   raw.githubusercontent.com/…/main/VERSION  直连 0.49s / HTTP 200
#   api.github.com/repos/…/tags               直连 0.72s / HTTP 200
#   api.github.com/repos/…/commits/main       直连 2.45s / 代理 0.70s
#   git ls-remote 最坏 90 秒（代理→直连→重试→代理 四趟）
# ⇒ 墙不是绝对的，且 HTTP 快两个数量级、还**不需要本机装 git**。
#   所以 remote_info() 改成：HTTP 三通道并发 → 全失败才回退 git ls-remote。
#   只有「拉取」这一步（git pull）才真的需要 git（没有就降级 zip 覆盖）。
#
# 提交标题约定（**发版时必须遵守**）：提交信息以 `v2.NNN` 或 `v2.NNNa/b` 开头，
#   例 "v2.341 修 xxx" / "v2.341a 补 xxx"。
#   check_update() 先用正则从远端 HEAD 标题抠号，抠不到退一步读远端 VERSION 文件。
GH_REPO = 'lianggang941023-dev/Open-BS-King'
GH_URL = 'https://github.com/%s.git' % GH_REPO
GH_PAGE = 'https://github.com/%s' % GH_REPO

# 检查更新的最短间隔（秒）。git ls-remote 不吃 GitHub 的 HTTP 限流，
# 但每次都要握手 1 秒左右，60 秒缓存让频繁开关窗口时不用重复等。
CHECK_TTL = 60.0

_lock = threading.Lock()
_last = {'ts': 0.0, 'data': None}


# ── v2.350 (2026-11-20 用户 m35653/m35674): 代码状态扫描 ──────────────
# 为什么需要它: 设置窗口原来显示的两个数都是**人手维护**的 ——
#   `版本` 读根目录 VERSION(忘了改就一直停在 v2.342)、
#   `构建` 读下面那个写死的 APP_BUILD 常量(2026-11-18, 半年没动过)。
#   代码早到 v2.349e 了界面还显示 v2.342, 用户以为改动没生效。
# 现在改成扫代码: 取所有 `v2.NNN[a-z]` 标记里最大的那个 + 这批文件最后的
#   修改时间 ⇒ 我改完任何一个文件, 设置窗口立刻是新号, 不需要谁记得改 VERSION。
# 性能: 缓存按「文件名+大小+mtime」签名失效, os.stat 二十几个文件 ≈ 0.2ms;
#   只有真的有文件变动时才去读文本(约 3.5 MB, 30ms)。
_VER_MARK = re.compile(r'v(\d+)\.(\d{3})([a-z])?')
_CS = {'sig': None, 'd': None}


def _code_files():
    """要扫的正式代码 —— quant_terminal 下**不以 `_` 开头**的 .py, 加上所有 .html。

    🔴 v2.350c (2026-11-20 实测发现的隐患): 这个目录下 201 个 .py 里有 155 个是
       `_` 开头的临时探针/补丁脚本(`_uninstall_wave.py` / `_mkfavicon.py` /
       `_backfill_free.py` …), 它们正文里也写着 `v2.261` / `v2.264` / `v2.273`
       这类标记(实测 49 处)。若不排除, 会有两个坏后果:
         ① 某个探针里写了一个比现在高得多的号, 版本号就被它顶上去 —— 显示的不是发版号;
         ② mtime 取最大值 ⇒ 建一个探针时间前进、**删掉探针时间又倒退回来**,
            「改动生效了」反倒成了噪音。
       ⇒ 只认正式代码, 探针一律不算。
    另外也**排除 version.py 自己**: 它天生全是版本号, 一句话里提到一个高号
    就能把版本顶飞(2026-11-20 自己踩过)。版本号由别的文件声明。
    """
    out = []
    qd = os.path.join(BASE_DIR, 'quant_terminal')
    me = os.path.basename(__file__)          # 'version.py'
    try:
        for p in glob.glob(os.path.join(qd, '*.py')):
            n = os.path.basename(p)
            # 排除探针(_ 开头) + 排除本文件自己
            #   本文件天生全是版本号: APP_BUILD 的注释、selftest 的样例、
            #   docstring 里的说明都写着版本标记。让它参与扫描, 一句「举例」就能
            #   把号顶飞(2026-11-20 就这么自己踩了一次)。
            #   版本号该由别的文件声明 —— terminal.html / server.py 的功能注释。
            if n.startswith('_') or n == me:
                continue
            out.append(p)
        out.extend(glob.glob(os.path.join(qd, '*.html')))
    except Exception:
        pass
    return out


def code_state(force=False):
    """扫代码得出的实况: {'ver','mtime','mtime_str','file','n_files','secs','cached'}

    ver       —— 所有 `v2.NNN[a-z]` 标记里最大的那个(拿不到就是 '')
    mtime_str —— 这批文件里最后修改时刻 'MM-DD HH:MM'(拿不到就是 '')
    file      —— 最后被改的那个文件名(排查「我到底改了哪个」时有用)
    """
    t0 = time.time()
    files = _code_files()
    sig = []
    for p in files:
        try:
            st = os.stat(p)
            sig.append((os.path.basename(p), int(st.st_mtime), st.st_size))
        except OSError:
            continue
    sig = tuple(sorted(sig))
    if (not force) and _CS['sig'] == sig and _CS['d'] is not None:
        d = dict(_CS['d'])
        d['cached'] = True
        return d

    best_txt, best_key = '', None
    newest, newest_name = 0.0, ''
    for p in files:
        try:
            st = os.stat(p)
            if st.st_mtime > newest:
                newest, newest_name = st.st_mtime, os.path.basename(p)
            with open(p, 'r', encoding='utf-8', errors='replace') as f:
                txt = f.read()
        except OSError:
            continue
        for m in _VER_MARK.finditer(txt):
            s = m.group(0)
            k = _vtuple(s)
            if k is None:
                continue
            if best_key is None or k > best_key:
                best_key, best_txt = k, s
    d = {
        'ver': best_txt,
        'mtime': int(newest),
        'mtime_str': (time.strftime('%m-%d %H:%M', time.localtime(newest))
                      if newest else ''),
        'file': newest_name,
        'n_files': len(sig),
        'secs': round(time.time() - t0, 3),
        'cached': False,
    }
    _CS['sig'] = sig
    _CS['d'] = dict(d)
    return d


def _read_version_file():
    """只读 <root>/VERSION 的第一行(发版号)。缺失/读不到返回 '' —— 不抛异常。"""
    try:
        with open(VERSION_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                s = line.strip()
                if s and not s.startswith('#'):
                    return s
    except Exception:
        pass
    return ''


def current():
    """当前版本 = max(VERSION 文件里的发版号, 代码里最大的 v2.NNN 标记)。

    v2.350 (2026-11-20 用户 m35653/m35674「全自动」):
      VERSION 是人手写的, 忘了改就一直停在旧号 —— 它停在 v2.342 而代码早到
      v2.349 了, 设置窗口里的版本号因此长期是错的。现在「谁大用谁」:
      VERSION 只当**发版下限**, 代码里的标记才是实况。
      ⇒ 改完任何一个文件, 设置窗口立刻是新号, 不需要谁记得改 VERSION。

    文件缺失、代码里一个标记都没有时返回 'v0.0.0' 而不是抛异常 ——
    版本号读不到不该让整个服务起不来。
    """
    base = _read_version_file()
    try:
        cv = (code_state() or {}).get('ver') or ''
    except Exception:
        cv = ''
    a, b = _vtuple(cv), _vtuple(base)
    if a is not None and b is not None:
        return cv if a > b else base
    if a is not None:
        return cv
    return base or 'v0.0.0'


def _vtuple(v):
    """'v2.340' → (2, 340, 0, 0)；'v2.340a' → (2, 340, 1, 0)；'v2.340b' → (2, 340, 2, 0)

    第四位留给以后可能出现的 c/d。解析失败返回 None（调用方据此判「不比较」，
    绝不把解析失败当成「已是最新」—— 那会让人以为检查成功了）。"""
    try:
        s = str(v).strip().lstrip('vV')
        patch = 0
        if s and s[-1].isalpha():
            patch = ord(s[-1].lower()) - ord('a') + 1
            s = s[:-1]
        parts = s.split('.')
        major = int(parts[0])
        minor = int(parts[1]) if len(parts) > 1 else 0
        return (major, minor, patch, 0)
    except Exception:
        return None


def compare(local, remote):
    """本地 vs 远端：'newer' / 'same' / 'older' / 'unknown'"""
    a, b = _vtuple(local), _vtuple(remote)
    if a is None or b is None:
        return 'unknown'
    if b > a:
        return 'newer'
    if b == a:
        return 'same'
    return 'older'


# 网络通道清单（按顺序尝试，第一个成功就用）。**代理优先，直连兜底。**
#   2026-11-18 晚实测同一条 ls-remote：直连 27.29 秒后 Recv failure: Connection was reset，
#   代理 1.13 秒成功 —— 差两个数量级，且直连那次是白等。用户梯子常开是既定事实。
_PROXY = 'socks5://127.0.0.1:1080'

# 🔴 v2.342 (2026-11-18 用户 m26567): 必须带 CREATE_NO_WINDOW，否则每跑一次 git
#   就会**弹一个黑窗口闪一下**。根因：服务是 pythonw.exe 起的、本身没有控制台，
#   subprocess 起 git.exe 时 Windows 会为这个「没有控制台的新进程」配终端。
#   ⚠ server.py 的 `_do_git_pull` 与两个 `_run_and_track` 也必须加，缺一处就还会闪。
_NO_WIN = getattr(subprocess, 'CREATE_NO_WINDOW', 0)


# ══════════════════════════════════════════════════════════════════════════
# v2.351 (2026-11-20 用户 m35797 / m35848 / m35885)
#   测试机（另一台 Windows）没装 Git for Windows，只有 WorkBuddy 的 PortableGit
#   且不在 PATH ⇒ subprocess 起 'git' 直接 FileNotFoundError，界面报
#   「本机找不到 git 命令，请先安装 Git for Windows」。
#   下面四组函数分别解决：找 git / 不要 git 也能查更新 / 脏文件分三分类 /
#   更新前先做三方预演（照抄 TSP scripts/upgrade_check.py 的思路）。
# ══════════════════════════════════════════════════════════════════════════

_GIT = {'path': None, 'how': '', 'ts': 0.0}
_GIT_TTL = 60.0


def find_git(force=False):
    """找 git 可执行文件 —— 返回 (绝对路径, 来源说明)。找不到返回 ('', '')。

    三级回退（测试机问题一的修法）：
      ① PATH                    —— shutil.which，正常装过 Git for Windows 的都在这
      ② 常见安装目录            —— Program Files / Program Files (x86) /
                                   %LOCALAPPDATA%\\Programs\\Git
      ③ PortableGit 便携版      —— 各盘根目录下 Portable*/cmd|bin/git.exe
                                   以及 WorkBuddy 一类自带 git 的工具目录

    🔴 为什么必须找而不是直接用 'git'：
      PATH 里没有时 subprocess 抛 FileNotFoundError，被我自己的 except 兜成
      「请先安装 Git for Windows」—— 用户明明装了便携版，却被告知去装。
      找一次缓存 60 秒：一次「更新」要连调十几条 git 命令，别每条都扫盘。
    """
    now = time.time()
    if (not force) and _GIT['path'] is not None and (now - _GIT['ts']) < _GIT_TTL:
        return _GIT['path'], _GIT['how']

    found, how = '', ''
    # ① PATH
    try:
        p = shutil.which('git') or shutil.which('git.exe')
        if p and os.path.isfile(p):
            found, how = p, 'PATH'
    except Exception:
        pass

    # ② 常见安装目录
    if not found:
        cands = []
        for env in ('ProgramFiles', 'ProgramFiles(x86)', 'ProgramW6432'):
            d = os.environ.get(env)
            if d:
                cands.append(os.path.join(d, 'Git', 'cmd', 'git.exe'))
                cands.append(os.path.join(d, 'Git', 'bin', 'git.exe'))
        la = os.environ.get('LOCALAPPDATA')
        if la:
            cands.append(os.path.join(la, 'Programs', 'Git', 'cmd', 'git.exe'))
            cands.append(os.path.join(la, 'Git', 'cmd', 'git.exe'))
        cands.append(r'C:\Program Files\Git\cmd\git.exe')
        cands.append(r'C:\Program Files (x86)\Git\cmd\git.exe')
        for p in cands:
            if p and os.path.isfile(p):
                found, how = p, '安装目录'
                break

    # ③ PortableGit 便携版（各盘根目录下）
    if not found:
        try:
            roots = []
            for ch in 'CDEFGH':
                d = ch + ':\\'
                if os.path.isdir(d):
                    roots.append(d)
            for r in roots:
                for pat in ('Portable*', 'PortableGit*', 'Git*', 'WorkBuddy*', '*PortableGit*'):
                    try:
                        for sub in glob.glob(os.path.join(r, pat)):
                            if not os.path.isdir(sub):
                                continue
                            for rel in (('cmd', 'git.exe'), ('bin', 'git.exe'),
                                        ('PortableGit', 'cmd', 'git.exe'),
                                        ('PortableGit', 'bin', 'git.exe')):
                                p = os.path.join(sub, *rel)
                                if os.path.isfile(p):
                                    found, how = p, '便携版'
                                    break
                            if found:
                                break
                    except Exception:
                        continue
                    if found:
                        break
                if found:
                    break
        except Exception:
            pass

    _GIT['path'] = found
    _GIT['how'] = how
    _GIT['ts'] = now
    return found, how


# ── HTTP 通道（检查更新不需要本机 git）────────────────────────────────────

_UA = 'BSKing/%s' % '2.351'
_RAW_VERSION = 'https://raw.githubusercontent.com/%s/main/VERSION' % GH_REPO
_API_TAGS = 'https://api.github.com/repos/%s/tags' % GH_REPO
_API_HEAD = 'https://api.github.com/repos/%s/commits/main' % GH_REPO


def _http_get(url, timeout=8.0, tries=2):
    """取 URL 正文 —— 先直连、再走系统代理。返回 (bytes, err)。

    🔴 为什么两个通道都要试、而且**直连在前**：
      2026-11-20 实测 raw/api 直连都通（0.49 / 0.72 秒），走代理反而慢
      （0.75 / 0.64 秒）；而 commits 那一条代理快（0.70 vs 2.45）。
      两条路各有快慢，写死任何一条都会在某些场景白等。
      ⇒ 与 _git() 一样：两条都试，谁先成功用谁，全失败把**两条错误**都带回
        （不包装成「未知错误」，否则排查时不知道是墙还是代理没开）。
    """
    last_err = ''
    openers = [('直连', urllib.request.build_opener(urllib.request.ProxyHandler({}))),
               ('系统代理', urllib.request.build_opener())]
    for attempt in range(max(1, tries)):
        for label, op in openers:
            try:
                req = urllib.request.Request(url, headers={
                    'User-Agent': _UA,
                    'Accept': 'application/vnd.github+json',
                })
                with op.open(req, timeout=timeout) as r:
                    return r.read(), ''
            except urllib.error.HTTPError as e:
                # 404 之类是确定性错误，重试没有意义
                return b'', 'HTTP %s %s（%s）' % (e.code, e.reason, url)
            except Exception as e:
                last_err = '%s: %s' % (label, e)
        if attempt + 1 < tries:
            time.sleep(0.4)
    return b'', last_err or 'HTTP 请求失败'


def _raw_version():
    """raw.githubusercontent.com/…/main/VERSION → 'v2.349'（不需要 git，0.5 秒）"""
    b, err = _http_get(_RAW_VERSION, timeout=8.0)
    if err:
        return '', err
    try:
        s = b.decode('utf-8', 'replace').strip().splitlines()[0].strip()
    except Exception:
        s = ''
    return s, ('' if s else '远端 VERSION 内容为空')


def _gh_tags():
    """GitHub REST /tags → (['v2.349', 'v2.342', …] 升序, err)"""
    b, err = _http_get(_API_TAGS, timeout=8.0)
    if err:
        return [], err
    try:
        arr = json.loads(b.decode('utf-8', 'replace'))
    except Exception as e:
        return [], '解析 tags JSON 失败：%s' % e
    out = []
    for it in (arr or []):
        if isinstance(it, dict):
            n = str(it.get('name') or '').strip()
            if n:
                out.append(n)
    out = [t for t in out if _vtuple(t) is not None]
    out.sort(key=lambda x: (_vtuple(x) or (0, 0, 0, 0)))
    return out, ('' if out else '远端没有任何 vX.YYY 形态的 tag')


def _gh_head():
    """GitHub REST /commits/main → ({'sha','title'}, err)"""
    b, err = _http_get(_API_HEAD, timeout=10.0)
    if err:
        return {}, err
    try:
        d = json.loads(b.decode('utf-8', 'replace'))
    except Exception as e:
        return {}, '解析 commits JSON 失败：%s' % e
    return {'sha': str(d.get('sha') or ''),
            'title': str(((d.get('commit') or {}).get('message') or '')).splitlines()[0][:160]}, ''


def remote_info_http():
    """只用 HTTP 拿远端快照 —— 返回 (dict, err)，dict 与 remote_info() 同形。

    三路并发（tags / commits / raw VERSION）：最慢的那条 2.4 秒封顶，
    串行要 3.6 秒以上。任一成功就能给出结论，全失败才返回 err。
    """
    res = {}
    errs = []

    def _t(k, fn):
        try:
            res[k] = fn()
        except Exception as e:
            res[k] = (None, str(e))

    with _TPE(max_workers=3) as ex:
        f1 = ex.submit(_t, 'tags', _gh_tags)
        f2 = ex.submit(_t, 'head', _gh_head)
        f3 = ex.submit(_t, 'ver', _raw_version)
        for f in (f1, f2, f3):
            try:
                f.result(timeout=20)
            except Exception:
                pass

    tags, terr = (res.get('tags') or ([], 'tags 未返回'))[:2]
    head, herr = (res.get('head') or ({}, 'commits 未返回'))[:2]
    ver, verr = (res.get('ver') or ('', 'VERSION 未返回'))[:2]
    for e in (terr, herr, verr):
        if e:
            errs.append(str(e))

    latest, src = '', ''
    if tags:
        latest, src = tags[-1], 'http:tag'
    elif ver and _vtuple(ver) is not None:
        # 没有 tag 时退回 VERSION 文件（发版流程写的不一定打 tag）
        latest, src = ver, 'http:version'
    else:
        ct = str((head or {}).get('title') or '')
        m = re.search(r'v(\d+)\.(\d{2,3})([a-zA-Z]?)', ct)
        if m:
            latest = 'v%s.%s%s' % (m.group(1), m.group(2), (m.group(3) or '').lower())
            src = 'http:commit'

    sha = str((head or {}).get('sha') or '')
    d = {'head': sha, 'head7': _sha7(sha), 'tags': tags,
         'latest': latest, 'src': src, 'err': '' if latest else '；'.join(errs[:3])}
    return d, ('' if latest else d['err'])


# ── 脏文件分类（测试机问题二的修法）──────────────────────────────────────

# 程序自己会重建 / 每天自动追加的 —— 本地改动**可以丢**（远端那份更全）
_RUNTIME_PREFIX = (
    'kline_data/', 'data_archive/', 'dragon_data/', 'quant_terminal/logs/',
    '_bak_', '_bak/', '选股池/', '_运行环境/',
)
_RUNTIME_SUFFIX = ('.log', '.pyc', '.tmp', '.bak', '.orig', '.bakv', '.baknz')
_RUNTIME_NAME = frozenset((
    # v2.351c (2026-11-20): 补两个分类漏项 —— 实测它们会被判成 source/other 而拒绝更新。
    #   VERSION      : 每次发版都会被 pull 改；v2.350 起 current() 取 max(VERSION, 代码最大标记)，
    #                  本地这行的旧值本来就无所谓 ⇒ 当运行时文件处理，stash 后让远端覆盖。
    #   会话状态.md   : 用户自己的会话日志（开源仓库 .gitignore 里已忽略），不能因改过它就拒绝更新。
    'VERSION',
    '会话状态.md',
    'kline_data/.mcap_written.json',
    'kline_data/mini_minute_cache.json',
    'kline_data/bank_minute_cache.json',
    'kline_data/regime_cache.json',
    'kline_data/kzz_cache.json',
    'kline_data/ai_market_recaps.json',
    'kline_data/trend_data.json',
    'quant_terminal/trend_data.json',
    'quant_terminal/parent_trend.json',
    'quant_terminal/monitor_state.json',
    'quant_terminal/scan_cache.json',
    'quant_terminal/sim_state.json',
    'quant_terminal/sim_hist.json',
    'quant_terminal/ma_baseline.json',
    'quant_terminal/intraday_snapshots.json',
    'quant_terminal/ai_news_scores.json',
    'quant_terminal/news_dedup.json',
    'quant_terminal/turnover_data.json',
    'quant_terminal/exrights_log.json',
    'quant_terminal/watchlist_cache.json',
    'quant_terminal/macro_daily.json',
    'quant_terminal/auto_task_state.json',
    'quant_terminal/tdx_calls_log.json',
    'quant_terminal/kline_update_state.json',
    'quant_terminal/sector_breakout_cache.json',
    'quant_terminal/sector_hot.json',
))
# 用户自己填过东西的配置 —— 既不能丢也不能覆盖，只能**拒绝更新**并告诉他改哪儿了
_CONFIG_NAME = frozenset((
    'quant_terminal/data_sources.json',
    'quant_terminal/data_sources.local.json',
    'quant_terminal/concept_blocklist.txt',
    'quant_terminal/watchlist.json',
))
_SOURCE_SUFFIX = ('.py', '.html', '.js', '.vbs', '.bat', '.md', '.ico', '.css')


def classify_dirty(line):
    """把 `git status --porcelain` 的一行分成 (类别, 路径)。

    类别：
      runtime —— 程序自己生成/每天追加的数据（csv、缓存 json、日志、备份）
                 本地改动**可以丢**：pull 前先 stash，pull 后尝试 pop，pop 不上就以远端为准
      config  —— 用户填过内容的配置（data_sources.json 里是真 Key）
                 绝不能自动覆盖 ⇒ 拒绝更新，并把具体文件列给用户
      source  —— .py / .html / .js 等代码
                 本机改过代码 ⇒ 拒绝更新（强拉会静默冲掉），并附 merge-tree 预演结果
      other   —— 其余（含改名、删除、子模块）
                 保守拒绝

    🔴 为什么不能像老版本那样「有任何未提交就整体拒绝」：
      程序每天跑完都会改 kline_data/*.csv 与几个缓存 json，
      ⇒ 只要用过一次终端，`git status` 就永远不干净，更新功能等于永久失效
        （测试机实测 18 个脏文件全是这类）。
    """
    s = (line or '').rstrip()
    if len(s) < 4:
        return ('other', s.strip())
    path = s[3:].strip()
    if ' -> ' in path:              # 改名：只看新名
        path = path.split(' -> ', 1)[1].strip()
    path = path.strip().strip('"')
    lp = path.replace('\\', '/')
    if lp in _CONFIG_NAME:
        return ('config', lp)
    if lp in _RUNTIME_NAME:
        return ('runtime', lp)
    for p in _RUNTIME_PREFIX:
        if lp.startswith(p):
            return ('runtime', lp)
    for x in _RUNTIME_SUFFIX:
        if lp.lower().endswith(x):
            return ('runtime', lp)
    for x in _SOURCE_SUFFIX:
        if lp.lower().endswith(x):
            return ('source', lp)
    return ('other', lp)


def dirty_breakdown():
    """跑一次 `git status --porcelain` 并按类别分桶。返回 (dict, err)。

    dict = {'runtime': [...], 'config': [...], 'source': [...], 'other': [...], 'n': 总数}
    """
    out = {'runtime': [], 'config': [], 'source': [], 'other': [], 'n': 0}
    if not _is_repo():
        return out, '不是 git 副本'
    so, se, rc = _git(['status', '--porcelain'], timeout=30.0, tries=1)
    if rc != 0:
        return out, (se or 'git status 失败')[:300]
    for line in (so or '').splitlines():
        if not line.strip():
            continue
        out['n'] += 1
        k, p = classify_dirty(line)
        out[k].append(p)
    return out, ''


# ── merge-tree 三方预演（测试机问题三的修法）─────────────────────────────

def _git_local(args, timeout=60.0):
    """本地只读 git 命令：单次、**不走代理**、`returncode` 原样返回。

    🔴 为什么不能用 `_git()`：
      `_git()` 的契约是「rc==0 才算成功」，非 0 会重试（代理→直连）再返回 rc=-1。
      但 `git merge-tree --write-tree` **有冲突时本来就返回 1**（0=干净 / 1=冲突 /
      其它=真错）。用 `_git()` 去跑它，等于永远看不到「有冲突」这个正常结论，
      只会白等两趟代理超时、然后拿到一个假的 -1。
    """
    exe, _how = find_git()
    if not exe:
        return '', '本机找不到 git 命令', -2
    cmd = [exe, '-C', BASE_DIR] + list(args)
    try:
        r = subprocess.run(cmd, capture_output=True, text=True,
                           encoding='utf-8', errors='replace', timeout=timeout,
                           creationflags=_NO_WIN)
        return (r.stdout or ''), (r.stderr or ''), r.returncode
    except subprocess.TimeoutExpired:
        return '', 'git %s 超时（%d 秒）' % (args[0] if args else '', int(timeout)), -3
    except FileNotFoundError:
        return '', '本机找不到 git 命令', -2
    except Exception as e:
        return '', 'git 调用失败：%s' % e, -1


def _mt_modern(base, cur, tgt, timeout=90.0):
    """git 2.38+ : `merge-tree --write-tree --name-only --merge-base=<base> A B`

    返回 (冲突路径列表, err)。rc=0 干净 / rc=1 有冲突 / 其它 = 不支持或出错。

    输出格式（实测）：
        5afdc692…            ← 合并结果树的 sha
        a.txt                ← 冲突路径（--name-only 时只有路径）
        (空行)
        Auto-merging a.txt
        CONFLICT (content): Merge conflict in a.txt
    """
    so, se, rc = _git_local(['merge-tree', '--write-tree', '--name-only',
                            '--merge-base=' + base, cur, tgt], timeout=timeout)
    if rc not in (0, 1):
        return None, (se or so or ('rc=%d' % rc))[:300]
    paths = []
    for line in (so or '').splitlines():
        s = line.strip()
        if not s:
            break                    # 空行之后是说明文字，不是路径
        paths.append(s)
    if paths:
        paths = paths[1:]            # 第一行是结果树 sha
    return paths, ''


def _mt_legacy(base, cur, tgt, timeout=90.0):
    """git < 2.38 的老形态：`merge-tree <base> <cur> <tgt>`（纯文本输出）。

    照抄 TSP scripts/upgrade_check.py 的解析：按「changed in both」分块，
    块里出现 `+<<<<<<<` / `+=======` / `+>>>>>>>` 就是真文本冲突。
    """
    so, se, rc = _git_local(['merge-tree', base, cur, tgt], timeout=timeout)
    if rc != 0:
        return None, (se or ('rc=%d' % rc))[:300]
    conflicts, cur_path, cur_bad = [], None, False

    def _flush():
        if cur_path and cur_bad:
            conflicts.append(cur_path)

    for line in [*(so or '').splitlines(), '']:
        if line.strip() in {'changed in both', 'added in both', 'removed in both'}:
            _flush()
            cur_path, cur_bad = None, False
            continue
        parts = line.strip().split()
        if cur_path is None and len(parts) >= 4 and parts[0] in {'base', 'our', 'their'}:
            cur_path = parts[-1]
        if (line.startswith('+<<<<<<< ') or line.startswith('+=======')
                or line.startswith('+>>>>>>> ')):
            cur_bad = True
    _flush()
    return sorted(set(conflicts)), ''


def merge_check(target='origin/main', timeout=120.0):
    """三方预演：把本机 HEAD 与远端 target 合起来会不会撞车。**只读，不改任何文件。**

    返回 dict:
      ok         True = 预演跑成了（不代表没冲突）
      err        失败原因
      base       共同基线 sha 前 7 位
      head7 / tgt7
      cur_n / tgt_n      本机 / 远端各改了多少文件
      overlap    双方改过的同一文件（可能语义兼容，需要人看一眼）
      conflicts  Git 认定有**文本冲突**的文件（必须人工解决）
      dirty      未提交条目（真拉之前得先处理；预演看不见它们）
      how        'modern' / 'legacy' —— 用的哪种 merge-tree 形态

    🔴 为什么这件事必须做在 pull 之前：
      测试机报告的问题三：本机改了 version.py 修问题一，上游 v2.349 也改了
      同一个文件 ⇒ `git pull --ff-only` 直接报
      「error: Your local changes to the following files would be overwritten by merge」，
      护栏（拦在 pull 之前）根本来不及生效。预演能提前说清「你跟远端动了同一个文件」。
    """
    t0 = time.time()
    out = {'ok': False, 'err': '', 'base': '', 'head7': '', 'tgt7': '',
           'cur_n': 0, 'tgt_n': 0, 'overlap': [], 'conflicts': [],
           'dirty': {}, 'how': '', 'secs': 0.0}
    if not _is_repo():
        out['err'] = '不是 git 副本（没有 .git 目录）—— 只能用 zip 覆盖更新'
        return out

    _, herr = find_git()
    so, se, rc = _git(['rev-parse', 'HEAD'], timeout=20.0, tries=1)
    if rc != 0:
        out['err'] = (se or 'git rev-parse HEAD 失败')[:300]
        return out
    cur = (so or '').strip()
    out['head7'] = _sha7(cur)

    # 先把远端 ref 抓下来（本地可能从没 fetch 过 target）
    fetch_args = ['fetch', '--no-tags']
    if target.startswith('origin/'):
        fetch_args += ['origin', target.split('/', 1)[1]]
    else:
        fetch_args += ['origin']
    fso, fse, frc = _git(fetch_args, timeout=timeout, tries=1)
    if frc != 0:
        out['err'] = 'git fetch 失败：' + ((fse or fso or '').strip()[:300])
        return out

    so, se, rc = _git(['rev-parse', target], timeout=20.0, tries=1)
    if rc != 0:
        out['err'] = '找不到目标 %s：%s' % (target, (se or '').strip()[:200])
        return out
    tgt = (so or '').strip()
    out['tgt7'] = _sha7(tgt)

    so, se, rc = _git(['merge-base', cur, tgt], timeout=30.0, tries=1)
    base = (so or '').strip() if rc == 0 else ''
    if not base:
        out['err'] = '当前分支与目标版本没有共同基线（远端可能被强推过）'
        return out
    out['base'] = _sha7(base)

    def _names(a, b):
        so2, _, rc2 = _git(['diff', '--name-only', '%s..%s' % (a, b)], timeout=60.0, tries=1)
        if rc2 != 0:
            return []
        return [x.strip() for x in (so2 or '').splitlines() if x.strip()]

    cp = _names(base, cur)
    tp = _names(base, tgt)
    out['cur_n'] = len(cp)
    out['tgt_n'] = len(tp)
    out['overlap'] = sorted(set(cp) & set(tp))

    paths, merr = _mt_modern(base, cur, tgt, timeout=90.0)
    out['how'] = 'modern'
    if paths is None:
        paths, merr2 = _mt_legacy(base, cur, tgt, timeout=90.0)
        out['how'] = 'legacy'
        if paths is None:
            out['err'] = 'merge-tree 不可用：%s / %s' % (merr, merr2)
            return out
    out['conflicts'] = [p for p in paths if p]

    db, derr = dirty_breakdown()
    out['dirty'] = {k: v for k, v in db.items() if k != 'n' and v}
    out['dirty']['n'] = db.get('n', 0)

    out['ok'] = True
    out['secs'] = round(time.time() - t0, 2)
    return out


def _git_once(args, proxy, timeout):
    # v2.351: 不再写死 'git' —— 测试机 PATH 里没有，但装了 PortableGit。
    #   找不到就走 FileNotFoundError，由 _git() 转成「没有 git」让上层降级 zip。
    exe, _how = find_git()
    if not exe:
        raise FileNotFoundError('git')
    cmd = [exe, '-C', BASE_DIR]
    if proxy:
        cmd += ['-c', 'http.proxy=' + proxy]
    cmd += list(args)
    r = subprocess.run(cmd, capture_output=True, text=True,
                       encoding='utf-8', errors='replace', timeout=timeout,
                       creationflags=_NO_WIN)
    return (r.stdout or ''), (r.stderr or ''), r.returncode


def _git(args, timeout=25.0, tries=2):
    """跑一条 git 命令，返回 (stdout, stderr, returncode)。

    为什么全部走 git 而不是 HTTP：
      见文件头那段实测 —— 只有 github.com 通，api/raw 两个域名都被墙。
      git 协议走 github.com:443，正好是唯一活着的那条路。

    为什么带重试 + 代理回退：
      直连 github.com 会被间歇性 RST（实测 'Recv failure: Connection was reset'），
      重试一次多数能过；还不行就换 socks5 代理再试。
      两条路都失败才认输，并把最后一条错误原样返回（不包装成'未知错误'）。"""
    last_err = ''
    # 🔴 顺序 = 代理优先（实测 1.13 秒）→ 直连兜底（实测 27 秒后被 RST）。
    #   理由见 _PROXY 上方那段。代理没开时第一趟会很快失败（连 1080 端口被拒），
    #   不会像直连那样傻等 27 秒。
    channels = [(_PROXY, '代理'), (None, '直连')]
    for attempt in range(max(1, tries)):
        for proxy, label in channels:
            try:
                out, err, rc = _git_once(args, proxy, timeout)
                if rc == 0:
                    return out, err, rc
                last_err = err.strip() or ('git %s 返回 %d' % (args[0] if args else '', rc))
            except subprocess.TimeoutExpired:
                last_err = 'git %s 超时（%d 秒，%s）' % (
                    args[0] if args else '', int(timeout), label)
            except FileNotFoundError:
                return '', '本机找不到 git 命令，请先安装 Git for Windows', -2
            except Exception as e:
                last_err = 'git 调用失败（%s）：%s' % (label, e)
        if attempt + 1 < tries:
            time.sleep(1.2)
    return '', last_err, -1


def _is_repo():
    """是不是 git 副本。zip 解压版没有 .git ⇒ 只能提示去网页下载。"""
    return os.path.isdir(os.path.join(BASE_DIR, '.git'))


def remote_refs():
    """一次 ls-remote 拿回远端所有 refs，返回 (dict, err)。

    🔴 为什么只调一次：
      实测直连会被 RST，每次调用都要走「代理 → 直连 → 重试 → 代理」四趟，
      单次成功的 ls-remote 就要 1~90 秒。原来 remote_head() 和 remote_tags()
      各调一次，最坏 348 秒 —— 用户点一下要等 6 分钟，不可接受。
      一次全量 ls-remote（不加 refs 过滤）只多传几百 KB，但省掉整轮重试。

    🔴 为什么用显式 URL 而不是 remote 名 'origin'（2026-11-18）：
      自用版（D:\\AI Working\\_运行环境\\终端8799）**没有 .git 目录**，不是 git 副本。
      原来这里写 'origin'，非副本就直接报「不是 git 副本」—— 结果自用版连
      「检查有没有新版本」都做不了。
      实测：在非 git 目录里 `git ls-remote <完整URL>` **完全能跑**（1.19 秒），
      不依赖任何本地仓库配置。所以改成显式 URL，**检查更新与是不是 git 副本解耦**。
      （「下载更新」仍然必须是 git 副本 —— 那步要 pull，见 server.py 的 _do_git_pull。）

    返回形如 {'HEAD': sha, 'refs/heads/main': sha, 'refs/tags/v2.340': sha, ...}"""
    out, err, rc = _git(['ls-remote', GH_URL], timeout=40.0)
    if rc != 0:
        return {}, err.strip() or 'git ls-remote 失败'
    d = {}
    for line in out.splitlines():
        p = line.split()
        if len(p) >= 2:
            d[p[1].strip()] = p[0].strip()
    return d, ''


def _sha7(s):
    return (s or '')[:7]


def remote_info():
    """远端整体快照：{'head','head7','tags','latest','src','err'}。

    tags   —— 远端所有 vX.YYY[ab] 形态的 tag（升序）
    latest —— 最大的那个 tag；一个 tag 都没有时退回从 HEAD 提交标题里抠

    v2.351 (用户 m35797)：**先走 HTTP**（GitHub REST + raw，实测 0.5~2.4 秒、
      不需要本机装 git），三通道全失败才回退 git ls-remote（最坏 90 秒）。
      测试机没装 Git for Windows，原来只走 git ⇒ 连「有没有新版本」都查不了。
    """
    # ① HTTP 通道
    _hd, _herr = remote_info_http()
    if _hd and _hd.get('latest'):
        return _hd

    # ② git 通道（HTTP 全挂时才走）
    refs, err = remote_refs()
    if err:
        _why = '；'.join([x for x in (_herr, err) if x])[:300]
        return {'head': '', 'head7': '', 'tags': [], 'latest': '', 'src': '', 'err': _why}

    head = refs.get('HEAD') or refs.get('refs/heads/main') or ''
    tags = []
    for r in refs:
        if r.startswith('refs/tags/'):
            t = r[len('refs/tags/'):]
            if t.endswith('^{}'):
                t = t[:-3]
            if _vtuple(t) is not None:
                tags.append(t)
    tags = sorted(set(tags), key=lambda x: (_vtuple(x) or (0, 0, 0, 0)))

    latest, src = '', ''
    if tags:
        latest, src = tags[-1], 'tag'
    else:
        rv, verr = _remote_version_by_commit()
        if rv:
            latest, src = rv, 'commit'
        else:
            err = verr or ''
    return {'head': head, 'head7': _sha7(head), 'tags': tags,
            'latest': latest, 'src': src, 'err': err}


def local_head():
    """本地当前提交 sha。"""
    if not _is_repo():
        return ''
    out, err, rc = _git(['rev-parse', 'HEAD'], timeout=15.0, tries=1)
    return out.strip() if rc == 0 else ''


# 旧接口保留（外部或调试可能还在用），内部改成走 remote_info，避免第二次联网
def remote_head():
    """远端 main 的提交 sha —— 兼容用，要求已知 remote_info 结果时别用它。"""
    return remote_info().get('head', '')


def remote_tags():
    """远端版本 tag 列表 —— 兼容用。"""
    d = remote_info()
    return d.get('tags', []), d.get('err', '')
def _remote_version_by_commit():
    """兜底：从远端 HEAD 的提交标题里抠版本号。

    约定 —— 发版提交的标题以 `v2.NNN` 或 `v2.NNNa` 开头（见文件头）。
    只有 ls-remote 无法拿到标题，得先 fetch。这里用 fetch 但不 merge：
      git fetch origin main --no-tags
    再读 FETCH_HEAD 的标题。fetch 是增量的，重复调用几乎零成本。"""
    import re
    if not _is_repo():
        return '', '不是 git 副本'
    _, err, rc = _git(['fetch', 'origin', 'main', '--no-tags'], timeout=60.0)
    if rc != 0:
        return '', (err.strip() or 'git fetch 失败')[:300]
    out, err2, rc2 = _git(['log', '-1', '--pretty=%s', 'FETCH_HEAD'])
    if rc2 != 0:
        return '', (err2.strip() or 'git log FETCH_HEAD 失败')[:300]
    m = re.search(r'v(\d+)\.(\d+)([a-zA-Z]?)', out or '')
    if not m:
        return '', '远端最新提交标题里没有版本号：%s' % (out or '')[:80]
    tag = 'v%s.%s%s' % (m.group(1), m.group(2), (m.group(3) or '').lower())
    return tag, ''


def check_update(force=False):
    """检查有没有新版本。

    返回 dict：
      ok       True = 查到了远端版本（不代表有更新）
      local    本地版本
      remote   远端版本（查不到就是 ''）
      status   'newer' / 'same' / 'older' / 'unknown'
      head     远端 main 的提交 sha 前 7 位（给人看，便于对到底是哪个提交）
      url      仓库地址
      build    本份代码的构建标记
      err      失败原因（ok=False 时才有）
      ts       这次检查的时间戳（秒）
      cached   True 表示这是 CHECK_TTL 内的缓存结果
      src      版本号是从哪儿读到的：'http:tag' / 'http:version' /
               'http:commit'（v2.351 新增，走 GitHub REST 不需要 git）/
               'tag' / 'commit'（git ls-remote 兜底）
      secs     这次实际耗时（秒）—— 网络慢的时候让用户知道为什么慢

    为什么要分 ok 和 status：
      「网络不通」和「已是最新」是两件事。合成一个布尔值的话，
      断网时会显示「已是最新」，人就再也不会去手动更新了。

    为什么用一次网络调用定全局：
      root 直连会被 RST，一次 ls-remote 最坏要走「直连→代理→重试→代理」四趟。
      原来查 head 和查 tag 各调一次 = 最坏 348 秒，用户点一下等六分钟。
      现在一次全量 ls-remote（remote_info）同时拿到 head + tags。"""
    t0 = time.time()
    now = t0
    with _lock:
        if (not force) and _last['data'] is not None and (now - _last['ts']) < CHECK_TTL:
            d = dict(_last['data'])
            d['cached'] = True
            return d

    local = current()
    remote, src, err, head = '', '', '', ''

    # 🔴 这里**不再**要求 _is_repo()（2026-11-18）：
    #   自用版不是 git 副本，但 ls-remote 用显式 URL 照样能查版本 tag
    #   ⇒ 自用版也能看到「有 v2.341 了」。只是点「更新版本」时会被告知
    #     去网页下载（那步要 pull，见 server.py 的 _do_git_pull）。
    ri = remote_info()
    head = ri.get('head') or ''
    remote = ri.get('latest') or ''
    src = ri.get('src') or ''
    err = ri.get('err') or ''
    if not err and not remote:
        err = '远端没有任何版本 tag，且提交标题里也没有版本号'

    # 🔴 只用版本号比较，不用「提交号是否相同」来覆盖结论。
    #   踩过：本地 VERSION 手改成 v2.339、远端 tag 是 v2.340，两边 git HEAD 相同
    #   ⇒ 旧逻辑直接判 same，明明版本号更旧却说已是最新，用户就不会去更新了。
    #   版本号是发版者声明的意图，提交号只是巧合；两者冲突时以版本号为准。
    status = compare(local, remote) if (remote and not err) else 'unknown'

    out = {
        'ok': bool(remote),
        'local': local,
        'remote': remote,
        'status': status,
        'src': src,
        'head': (head or '')[:7],
        'url': GH_PAGE,
        'build': build_date(),
        'err': err,
        'ts': now,
        'secs': round(time.time() - t0, 2),
        'cached': False,
    }
    with _lock:
        _last['ts'] = now
        _last['data'] = dict(out)
    return out




# v2.350b: 原来是写死的 '2026-11-18'，半年没动过 —— 设置窗口里那个
#   `构建 2026-11-18` 因此永远显示旧日期，看着像「更新没生效」。
#   改成从代码文件的最新修改日期推出来：改完任何一个文件它就跟着变。
#   下面这个常量只在**代码状态完全扫不到**时兜底（正常情况走不到）。
APP_BUILD_FALLBACK = '2026-11-18'


def build_date():
    """这份代码的日期 = quant_terminal 下最新修改的那个文件的日期。"""
    try:
        m = (code_state() or {}).get('mtime') or 0
        if m:
            return time.strftime('%Y-%m-%d', time.localtime(m))
    except Exception:
        pass
    return APP_BUILD_FALLBACK


# 兼容旧调用点（外部/调试脚本可能还在 import APP_BUILD）。
#   ⚠ 它是**导入那一刻**的快照，进程跑久了不会自己变 —— 要实况请调 build_date()。
APP_BUILD = build_date()


def info():
    """给前端用的一小份快照（不联网）。

    v2.350: 多带一个 code —— 前端用它显示 `代码 v2.349e · 11-20 03:12`,
    证明「我改的东西真的生效了」, 而不是靠一个半年没动的常量。
    """
    try:
        cs = code_state()
    except Exception:
        cs = {}
    _gp, _gh = find_git()
    return {
        'version': current(),
        'build': build_date(),
        'code': cs,
        'repo': GH_REPO,
        'url': GH_PAGE,
        # v2.351: 本机 git 情况 —— 没有就说明「更新会走 zip 覆盖」，
        #   而不是等用户点了更新才被告知「请先安装 Git for Windows」。
        'git': {'ok': bool(_gp), 'path': _gp, 'how': _gh},
        'repo_ok': _is_repo(),
    }


def selftest():
    print('VERSION 文件 : %s' % VERSION_FILE)
    print('当前版本     : %s' % current())
    print('构建         : %s (自动)' % build_date())
    _cs = code_state(force=True)
    print('代码标记     : %s  (%s, %d 个文件, %.3fs)'
          % (_cs.get('ver'), _cs.get('mtime_str'),
             _cs.get('n_files') or 0, _cs.get('secs') or 0))
    print('解析 v2.340  : %s' % (_vtuple('v2.340'),))
    print('解析 v2.340a : %s' % (_vtuple('v2.340a'),))
    print('解析 v2.340b : %s' % (_vtuple('v2.340b'),))
    print('解析 垃圾    : %s' % (_vtuple('abc'),))
    for a, b in [('v2.340', 'v2.341'), ('v2.340', 'v2.340'),
                 ('v2.340', 'v2.340a'), ('v2.340a', 'v2.340'),
                 ('v2.340', 'v2.339'), ('v2.340', 'x')]:
        print('  %-10s vs %-10s → %s' % (a, b, compare(a, b)))
    _gp, _gh = find_git()
    print('git          : %s (%s)' % (_gp or '未找到 —— 更新会降级 zip 覆盖', _gh or '-'))
    print('是否 git 副本: %s' % _is_repo())
    print()
    print('--- 联网检查 ---')
    r = check_update(force=True)
    for k in ('ok', 'local', 'remote', 'status', 'src', 'head', 'err'):
        print('  %-8s %s' % (k, r[k]))


if __name__ == '__main__':
    selftest()
