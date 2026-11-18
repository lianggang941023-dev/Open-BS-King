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

import json
import os
import subprocess
import threading
import time

# 仓库根：本文件在 <root>/quant_terminal/version.py
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VERSION_FILE = os.path.join(BASE_DIR, 'VERSION')

# 更新检查的远端：git 协议是唯一稳定可用的通道。
#   🔴 2026-11-18 实测（本机 + 用户梯子）：
#     github.com                 直连 200 / 1.8s     ← 只有它通
#     api.github.com             直连 000 / 代理 000  ← 被墙
#     raw.githubusercontent.com  直连 000 / 代理 000  ← 被墙（/raw/ 走的也是它）
#   所以只能用 `git ls-remote` / `git fetch`（走 github.com:443 的 git 协议）。
#   好消息：远端 HEAD 的提交标题里就写着版本号（约定见下），不必单独取 VERSION 文件。
#
# 提交标题约定（**发版时必须遵守**）：
#   提交信息以 `v2.NNN` 或 `v2.NNNa/b` 开头，例：
#     "v2.341 修 xxx"  "v2.341a 补 xxx"
#   check_update() 用正则从远端 HEAD 的标题里抠这个号；抠不到就退一步读远端 VERSION 文件。
GH_REPO = 'lianggang941023-dev/Open-BS-King'
GH_URL = 'https://github.com/%s.git' % GH_REPO
GH_PAGE = 'https://github.com/%s' % GH_REPO

# 检查更新的最短间隔（秒）。git ls-remote 不吃 GitHub 的 HTTP 限流，
# 但每次都要握手 1 秒左右，60 秒缓存让频繁开关窗口时不用重复等。
CHECK_TTL = 60.0

_lock = threading.Lock()
_last = {'ts': 0.0, 'data': None}


def current():
    """读 <root>/VERSION 的第一行；文件缺失时返回 'v0.0.0' 而不是抛异常 ——
    版本号读不到不该让整个服务起不来。"""
    try:
        with open(VERSION_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                s = line.strip()
                if s and not s.startswith('#'):
                    return s
    except Exception:
        pass
    return 'v0.0.0'


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


# 网络通道清单（按顺序尝试，第一个成功就用）
#   🔴 2026-11-18 实测：github.com 直连正常但会被 GFW 间歇性 reset(Connection was reset /
#      Could not connect after 21s)；用户梯子常开，socks5 1080 有时比直连稳，有时反之。
#      所以两条都试，谁先通算谁 —— 不要写死"直连优先"或"代理优先"，网络状况是变的。
#
#   🔴 2026-11-18 晚 再测：同一条 ls-remote，直连 **27.29 秒后 Recv failure: Connection was
#      reset**，代理 **1.13 秒成功**。差了两个数量级，而且直连那次是白等 27 秒。
#      用户梯子常开是既定事实，所以把代理提到第一位：通了立刻返回（1 秒级），
#      代理没开/不通才回落直连（慢，但总比查不了强）。
_PROXY = 'socks5://127.0.0.1:1080'

# 🔴 v2.342 (2026-11-18 用户 m26567): 必须带 CREATE_NO_WINDOW，否则每跑一次 git
#   就会**弹一个黑窗口闪一下**。根因：服务是 pythonw.exe 起的、本身没有控制台，
#   subprocess 起 git.exe 时 Windows 会为这个「没有控制台的新进程」配一个终端，
#   于是屏幕上闪出 Windows Terminal（实测 EnumWindows 能抓到
#   `CASCADIA_HOSTING_WINDOW_CLASS / 'Terminal'`，一闪就没）。
#   ✓ 实测：加了这个 flag 之后控制台窗口 0 个（见 _selftest_console()）。
#   ⚠ 同时 sync 里的 `subprocess.run(['git',...])` 与 server.py 的 `_do_git_pull`
#     也必须加，三处缺一处就还会闪。
_NO_WIN = getattr(subprocess, 'CREATE_NO_WINDOW', 0)


def _git_once(args, proxy, timeout):
    cmd = ['git', '-C', BASE_DIR]
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
    """远端整体快照：{'head','head7','tags','latest','err'}。

    tags   —— 远端所有 vX.YYY[ab] 形态的 tag（升序）
    latest —— 最大的那个 tag；一个 tag 都没有时退回从 HEAD 提交标题里抠"""
    refs, err = remote_refs()
    if err:
        return {'head': '', 'head7': '', 'tags': [], 'latest': '', 'src': '', 'err': err}

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
      src      'tag' / 'commit' —— 版本号是从哪儿读到的
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
        'build': APP_BUILD,
        'err': err,
        'ts': now,
        'secs': round(time.time() - t0, 2),
        'cached': False,
    }
    with _lock:
        _last['ts'] = now
        _last['data'] = dict(out)
    return out




APP_BUILD = '2026-11-18'


def info():
    """给前端用的一小份快照（不联网）。"""
    return {
        'version': current(),
        'build': APP_BUILD,
        'repo': GH_REPO,
        'url': GH_PAGE,
    }


def selftest():
    print('VERSION 文件 : %s' % VERSION_FILE)
    print('当前版本     : %s' % current())
    print('构建         : %s' % APP_BUILD)
    print('解析 v2.340  : %s' % (_vtuple('v2.340'),))
    print('解析 v2.340a : %s' % (_vtuple('v2.340a'),))
    print('解析 v2.340b : %s' % (_vtuple('v2.340b'),))
    print('解析 垃圾    : %s' % (_vtuple('abc'),))
    for a, b in [('v2.340', 'v2.341'), ('v2.340', 'v2.340'),
                 ('v2.340', 'v2.340a'), ('v2.340a', 'v2.340'),
                 ('v2.340', 'v2.339'), ('v2.340', 'x')]:
        print('  %-10s vs %-10s → %s' % (a, b, compare(a, b)))
    print()
    print('--- 联网检查 ---')
    r = check_update(force=True)
    for k in ('ok', 'local', 'remote', 'status', 'src', 'head', 'err'):
        print('  %-8s %s' % (k, r[k]))


if __name__ == '__main__':
    selftest()
