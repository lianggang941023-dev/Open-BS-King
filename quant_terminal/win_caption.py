# -*- coding: utf-8 -*-
"""v2.150 (2026-10-18) 拦截 Chrome 应用窗口标题栏的「系统右键菜单」。

为什么要从 Win32 侧做
---------------------
终端是用 `chrome.exe --start-maximized --app=http://127.0.0.1:8799` 打开的, 窗口标题栏
由 **Chrome 自己绘制**。在标题栏上右键弹出的那一串
    还原 / 移动 / 大小 / 最小化 / 最大化 / 返回 / 前进 / 重新加载 / 打开浏览器窗口 /
    剪切 / 复制 / 粘贴 / 查找 / 打印 / 缩放 / 任务管理器 / 关闭
是 Windows 的**窗口系统菜单**(WM_NCRBUTTONUP → 系统菜单), 属于**非客户区**,
不是页面元素 ⇒ 页面里的 contextmenu 拦截 (v2.148 那套) 对它完全无效。

做法: 把该窗口的系统菜单条目全部 DeleteMenu 删空。

现场实测 (2026-10-18, Windows + Chrome `--app` 窗口):
  · 删空后标题栏右键 **不再弹出任何菜单** (EnumWindows 查 #32768 弹窗 = 0);
  · 标题栏自带的 最小化/最大化/还原/关闭 **照常可用** —— 它们走 WM_SYSCOMMAND,
    不查菜单条目 (实测 SC_MINIMIZE 0xF020 / SC_MAXIMIZE 0xF030 / SC_RESTORE 0xF120 全通);
  · 拖动标题栏移动窗口照常 (走 WM_NCHITTEST, 与菜单无关);
  · Alt+F4 照常关闭 (走 WM_CLOSE, 不查菜单)。
  · 系统菜单在窗口状态变化后会被 Chrome 重建 ⇒ 需要**周期复查**再删, 故起一个守护线程。

安全边界
--------
只作用于标题**恰好等于** `BS King量化看盘选股终端` 的窗口。
Chrome `--app` 窗口的标题 = 页面 `<title>` 原样, 不带 " - Google Chrome" 后缀;
用户普通浏览器里打开同名标签页时标题会带后缀 ⇒ 不会被误伤。

关掉的办法: 启动 server 前设环境变量 `QT_NO_CAPTION_GUARD=1`;
或直接关掉终端窗口再重开 (新窗口的系统菜单是全新的)。
"""
import os

TARGET_TITLE = os.environ.get('QT_CAPTION_TITLE', 'BS King量化看盘选股终端')
TICK_SEC = float(os.environ.get('QT_CAPTION_TICK', '2'))
MF_BYPOSITION = 0x00000400

# ---------------------------------------------------------------- Win32 绑定
_u32 = None
_strp = None      # 防止回调对象被 GC 回收
_ready = False
_msg_ready = False


def _load():
    """惰性加载 user32 绑定。64 位下**必须**声明 argtypes/restype, 否则 HWND 会被截成 32 位。"""
    global _u32, _ready
    if _ready:
        return _u32
    import ctypes
    from ctypes import wintypes

    u = ctypes.WinDLL('user32', use_last_error=True)

    u.EnumWindows.argtypes = [ctypes.WINFUNCTYPE(ctypes.c_bool, wintypes.HWND,
                                                 wintypes.LPARAM), wintypes.LPARAM]
    u.EnumWindows.restype = wintypes.BOOL
    u.IsWindowVisible.argtypes = [wintypes.HWND]
    u.IsWindowVisible.restype = wintypes.BOOL
    u.GetWindowTextLengthW.argtypes = [wintypes.HWND]
    u.GetWindowTextLengthW.restype = ctypes.c_int
    u.GetWindowTextW.argtypes = [wintypes.HWND, wintypes.LPWSTR, ctypes.c_int]
    u.GetWindowTextW.restype = ctypes.c_int
    u.GetSystemMenu.argtypes = [wintypes.HWND, wintypes.BOOL]
    u.GetSystemMenu.restype = wintypes.HMENU
    u.GetMenuItemCount.argtypes = [wintypes.HMENU]
    u.GetMenuItemCount.restype = ctypes.c_int
    u.DeleteMenu.argtypes = [wintypes.HMENU, ctypes.c_uint, ctypes.c_uint]
    u.DeleteMenu.restype = wintypes.BOOL
    u.DrawMenuBar.argtypes = [wintypes.HWND]
    u.DrawMenuBar.restype = wintypes.BOOL

    _u32 = u
    _ready = True
    return u


# ---------------------------------------------------------------- 查找窗口
def find_windows():
    """返回标题恰好等于 TARGET_TITLE 的可见顶层窗口句柄列表。"""
    u = _load()
    if u is None:
        return []
    import ctypes
    from ctypes import wintypes

    hits = []

    def _cb(hwnd, _lp):
        try:
            if not u.IsWindowVisible(hwnd):
                return True
            n = u.GetWindowTextLengthW(hwnd)
            if n <= 0:
                return True
            buf = ctypes.create_unicode_buffer(n + 2)
            u.GetWindowTextW(hwnd, buf, n + 2)
            if buf.value == TARGET_TITLE:
                hits.append(int(hwnd))
        except Exception:
            pass
        return True

    cb = ctypes.WINFUNCTYPE(ctypes.c_bool, wintypes.HWND, wintypes.LPARAM)(_cb)
    u.EnumWindows(cb, 0)
    return hits


def _valid_menu(m):
    """句柄是不是一个「活着的」菜单: IsMenu 为真且条目数可读(>=0)。"""
    u = _load()
    if u is None:
        return False
    try:
        if not u.IsMenu(m):
            return False
        return u.GetMenuItemCount(m) >= 0
    except Exception:
        return False

# ---------------------------------------------------------------- 删菜单
def strip_once(hwnd):
    """把一个窗口的系统菜单删空, 返回删掉的条目数 (0 = 本来就没有/已删干净)。"""
    u = _load()
    if u is None:
        return 0
    # v2.150 实测坑: 窗口起来一段时间后, GetSystemMenu(hwnd, False) 会返回一个
    #   **陈旧句柄** (IsMenu=0 / GetMenuItemCount=-1 / err=1401), 此时下面的
    #   `while count>0` 一次都不进 ⇒ 删菜单静默失效(看着像成功, 其实没动)。
    #   唯一可靠的拿法: 先 GetSystemMenu(hwnd, True) 重建一次, 句柄才有效。
    #   ⚠ 重建 = 把系统菜单恢复成默认(7~23 项), 所以只在句柄确实失效时才重建;
    #   重建后本轮立刻删空, 不会留下可点的菜单。
    m = u.GetSystemMenu(hwnd, False)
    if not m or not _valid_menu(m):
        u.GetSystemMenu(hwnd, True)          # 重建, 换一个有效句柄
        m = u.GetSystemMenu(hwnd, False)
    if not m or not _valid_menu(m):
        return 0
    if u.GetMenuItemCount(m) <= 0:
        return 0
    removed = 0
    guard = 0
    # 逐条删第 0 项, 直到空; guard 防极端情况下死循环
    while u.GetMenuItemCount(m) > 0 and guard < 512:
        if not u.DeleteMenu(m, 0, MF_BYPOSITION):
            break
        removed += 1
        guard += 1
    if removed:
        try:
            u.DrawMenuBar(hwnd)
        except Exception:
            pass
    return removed


# ---------------------------------------------------------------- 最小化
def minimize_windows():
    """让标题等于 TARGET_TITLE 的窗口最小化, 返回受影响的窗口数。

    页面 JS 发不了 Win32 消息 (浏览器没有 window.minimize), 所以由服务端代做。
    走 WM_SYSCOMMAND / SC_MINIMIZE —— 与点标题栏「最小化」完全同一条路径,
    实测在系统菜单已被删空的情况下照样有效 (它不查菜单条目)。
    """
    if os.name != 'nt':
        return 0
    u = _load()
    if u is None:
        return 0
    import ctypes
    from ctypes import wintypes
    _bind_sendmsg()
    n = 0
    for h in find_windows():
        try:
            u.SendMessageW(wintypes.HWND(h), 0x0112, 0xF020, 0)   # WM_SYSCOMMAND / SC_MINIMIZE
            n += 1
        except Exception:
            pass
    return n


def _bind_sendmsg():
    """最小化要用的两个 API 单独绑 (惰性, 只做一次)。"""
    global _msg_ready
    if _msg_ready:
        return
    import ctypes
    from ctypes import wintypes
    u = _load()
    u.SendMessageW.argtypes = [wintypes.HWND, wintypes.UINT, wintypes.WPARAM, wintypes.LPARAM]
    u.SendMessageW.restype = wintypes.LPARAM
    _msg_ready = True

def restore_once(hwnd):
    """还原(重建)系统菜单 —— 仅供手动调试; 重建后条目数因窗口状态而异。"""
    u = _load()
    if u is None:
        return False
    u.GetSystemMenu(hwnd, True)
    return True


# ---------------------------------------------------------------- 守护线程
def start_guard():
    """起后台线程周期复查并清空; 非 Windows 直接返回 None。"""
    if os.name != 'nt':
        return None
    if (os.environ.get('QT_NO_CAPTION_GUARD') or '').strip() in ('1', 'true', 'yes'):
        print('[caption-guard] QT_NO_CAPTION_GUARD=1 ⇒ 未启用', flush=True)
        return None
    import threading
    import time

    def _loop():
        _load()                      # 先绑一次, 失败直接退出线程 (不影响 server)
        first = True
        while True:
            try:
                for h in find_windows():
                    r = strip_once(h)
                    if r:
                        print(f'[caption-guard] 标题栏系统菜单已清空: hwnd={h} 删除 {r} 项'
                              + (' (首次)' if first else ''), flush=True)
                        first = False
            except Exception:
                pass
            time.sleep(TICK_SEC)

    t = threading.Thread(target=_loop, name='caption-guard', daemon=True)
    t.start()
    return t


# ---------------------------------------------------------------- 手动跑
if __name__ == '__main__':
    import sys
    ws = find_windows()
    print(f'找到 {len(ws)} 个窗口: {ws}')
    if '--restore' in sys.argv:
        for h in ws:
            restore_once(h)
            print(f'  hwnd={h} 已请求重建系统菜单')
    else:
        for h in ws:
            removed = strip_once(h)
            m2 = _load().GetSystemMenu(h, False)
            left = _load().GetMenuItemCount(m2) if _valid_menu(m2) else 'n/a'
            print(f'  hwnd={h} 删除 {removed} 项, 剩 {left} 项')
