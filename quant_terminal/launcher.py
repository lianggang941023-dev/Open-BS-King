# -*- coding: utf-8 -*-
"""BS King 量化终端 · 启动器  (2026-11-28, 用户 m10342)

双击图标 ⇒ 像普通 Windows 应用程序一样:

  1) 已经在跑       → 把窗口切到前台, 不再开第二个窗口
  2) 没在跑         → 无窗口拉起 server (127.0.0.1:8799), 等它就绪, 再打开应用窗口
  3) 端口被占但没响应 → 清掉残留进程后重起
  4) 起不来         → 弹窗说明原因 + 给出日志位置

命令行:
  pythonw.exe launcher.py                # 双击调用: 带动画 + 自动打开窗口
  python.exe  launcher.py --check        # 只体检(打印状态), 不启动不打开
  python.exe  launcher.py --no-open      # 只拉起服务, 不打开浏览器
  python.exe  launcher.py --no-splash    # 不显示启动动画

设计要点(踩过的坑):
  * 探测本机端口必须 **绕开系统代理** —— 本机 SSR 代理常挂, urllib 默认会把
    http://127.0.0.1:8799 也交给代理转发, 代理一 hang 就恒判「服务器死了」。
    (watchdog.py 里有同样一段注释, 同一个坑)
  * 拉起服务用 pythonw.exe + CREATE_NO_WINDOW, 绝不闪黑窗。
  * 只在「探测失败」时才清理端口, 健康时绝不动它(否则每次点击都重启服务)。
"""
import os
import sys
import time
import json
import socket
import subprocess
import traceback
import datetime

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PORT = 8799
URL = 'http://127.0.0.1:%d' % PORT
API_STATUS = URL + '/api/status'
HERE = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(HERE, 'launcher.log')
ICO = os.path.join(os.path.dirname(BASE), '_开发产物', '文档', '图标', 'BSKing8799.ico')
if not os.path.isfile(ICO):
    # 开源版/别的机器上没有自用版的图标目录 → 退回仓库自带的 favicon
    ICO = os.path.join(HERE, 'favicon.ico')

PYW = os.path.join(os.path.dirname(sys.executable), 'pythonw.exe')
if not os.path.isfile(PYW):
    PYW = sys.executable

CREATE_NO_WINDOW = 0x08000000
DETACHED = 0x00000008

CHROME_PATHS = [
    r'C:\Program Files\Google\Chrome\Application\chrome.exe',
    r'C:\Program Files (x86)\Google\Chrome\Application\chrome.exe',
    os.path.expandvars(r'%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe'),
]

APP_TITLE_KEY = 'BS King'          # 应用窗口标题里必然含这个

# ---------------------------------------------------------------- 日志


def log(msg):
    line = '[%s] %s' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), msg)
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
    except Exception:
        pass
    try:
        if sys.stdout is not None:
            print(line, flush=True)
    except Exception:
        pass

# ---------------------------------------------------------------- 探测


def probe(timeout=3.0):
    """服务器活着吗 —— 强制直连, 绝不走系统代理。"""
    try:
        import urllib.request
        req = urllib.request.Request(API_STATUS, headers={'User-Agent': 'bsking-launcher'})
        op = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with op.open(req, timeout=timeout) as r:
            return r.status == 200
    except Exception:
        return False


def port_busy(timeout=0.6):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        return s.connect_ex(('127.0.0.1', PORT)) == 0
    finally:
        try:
            s.close()
        except Exception:
            pass


def pid_on_port():
    """netstat 找占用 8799 的 PID(避免依赖 psutil)。"""
    try:
        out = subprocess.run(['netstat', '-ano'], capture_output=True, timeout=15,
                             creationflags=CREATE_NO_WINDOW).stdout
        txt = out.decode('gbk', 'replace')
    except Exception:
        return []
    pids = []
    for line in txt.splitlines():
        if (':%d' % PORT) not in line:
            continue
        parts = line.split()
        if len(parts) < 5:
            continue
        if parts[-2].upper() not in ('LISTENING', 'LISTEN'):
            continue
        if parts[-1].isdigit():
            pids.append(int(parts[-1]))
    return pids


def kill_port():
    pids = pid_on_port()
    for pid in pids:
        try:
            subprocess.run(['taskkill', '/F', '/PID', str(pid)],
                           capture_output=True, timeout=15, creationflags=CREATE_NO_WINDOW)
            log('已清理占用端口的残留进程 pid=%d' % pid)
        except Exception as e:
            log('清理 pid=%d 失败: %s' % (pid, e))
    return pids


def start_server():
    flags = CREATE_NO_WINDOW | DETACHED
    log('拉起服务器: %s -m quant_terminal.server  (cwd=%s)' % (PYW, BASE))
    p = subprocess.Popen([PYW, '-m', 'quant_terminal.server'],
                         cwd=BASE, creationflags=flags,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                         close_fds=True)
    return p.pid


def wait_ready(seconds=90, cb=None):
    t0 = time.time()
    while time.time() - t0 < seconds:
        if probe(1.5):
            return True, time.time() - t0
        if cb:
            try:
                cb(time.time() - t0)
            except Exception:
                pass
        time.sleep(0.8)
    return False, time.time() - t0

# ---------------------------------------------------------------- 窗口


def find_app_window():
    """按标题找已在运行的应用窗口, 返回 hwnd。"""
    try:
        import ctypes
        from ctypes import wintypes
        user32 = ctypes.windll.user32
        found = []

        my_pid = os.getpid()

        @ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)
        def _enum(hwnd, lparam):
            if not user32.IsWindowVisible(hwnd):
                return True
            n = user32.GetWindowTextLengthW(hwnd)
            if n <= 0:
                return True
            buf = ctypes.create_unicode_buffer(n + 1)
            user32.GetWindowTextW(hwnd, buf, n + 1)
            if APP_TITLE_KEY not in buf.value:
                return True
            # 别把自己(启动动画标题也叫 BS King)当成应用窗口
            try:
                pid = wintypes.DWORD(0)
                user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
                if pid.value == my_pid:
                    return True
            except Exception:
                pass
            # 太小的窗口(提示条/残影)不算
            try:
                rc = wintypes.RECT()
                user32.GetWindowRect(hwnd, ctypes.byref(rc))
                if (rc.right - rc.left) < 300 or (rc.bottom - rc.top) < 200:
                    return True
            except Exception:
                pass
            found.append(hwnd)
            return True

        user32.EnumWindows(_enum, 0)
        return found[0] if found else None
    except Exception:
        return None


def focus_window(hwnd):
    try:
        import ctypes
        user32 = ctypes.windll.user32
        user32.ShowWindow(hwnd, 9)          # SW_RESTORE
        user32.SetForegroundWindow(hwnd)
        return True
    except Exception:
        return False


def chrome_exe():
    for p in CHROME_PATHS:
        if p and os.path.isfile(p):
            return p
    return None


def open_app():
    """打开应用窗口(优先 Chrome --app, 与原来快捷方式一致的观感)。"""
    hwnd = find_app_window()
    if hwnd:
        focus_window(hwnd)
        log('窗口已在运行 → 切到前台 hwnd=%s' % hwnd)
        return 'focus'
    ce = chrome_exe()
    if ce:
        subprocess.Popen([ce, '--start-maximized', '--app=' + URL],
                         creationflags=CREATE_NO_WINDOW, close_fds=True)
        log('已用 Chrome --app 打开 %s' % URL)
        return 'chrome'
    try:
        os.startfile(URL)
        log('已用默认浏览器打开 %s' % URL)
        return 'default'
    except Exception as e:
        log('打开浏览器失败: %s' % e)
        return 'fail'

# ---------------------------------------------------------------- 启动动画


class Splash(object):
    def __init__(self):
        self.root = None
        self.lbl = None
        self.sub = None
        self.t0 = time.time()

    def show(self):
        try:
            import tkinter as tk
            r = tk.Tk()
            r.title('BS King')
            r.overrideredirect(True)
            r.configure(bg='#14161c')
            w, h = 460, 150
            sw = r.winfo_screenwidth()
            sh = r.winfo_screenheight()
            x = int((sw - w) / 2)
            y = int((sh - h) / 2.4)
            r.geometry('%dx%d+%d+%d' % (w, h, x, y))
            r.attributes('-topmost', True)
            try:
                if os.path.isfile(ICO):
                    r.iconbitmap(ICO)
            except Exception:
                pass
            tk.Label(r, text='BS King 量化看盘选股终端', bg='#14161c', fg='#f0c040',
                     font=('Microsoft YaHei UI', 13, 'bold')).pack(pady=(26, 4))
            self.lbl = tk.Label(r, text='正在启动服务…', bg='#14161c', fg='#c8ccd6',
                                font=('Microsoft YaHei UI', 10))
            self.lbl.pack()
            self.sub = tk.Label(r, text='首次启动约 5~15 秒', bg='#14161c', fg='#6f7787',
                                font=('Microsoft YaHei UI', 8))
            self.sub.pack(pady=(6, 0))
            self.root = r
            r.update()
            return True
        except Exception as e:
            log('启动动画不可用(不影响启动): %s' % e)
            return False

    def text(self, main=None, sub=None):
        try:
            if self.lbl is not None and main is not None:
                self.lbl.config(text=main)
            if self.sub is not None and sub is not None:
                self.sub.config(text=sub)
            if self.root is not None:
                self.root.update()
        except Exception:
            pass

    def close(self):
        try:
            if self.root is not None:
                self.root.destroy()
        except Exception:
            pass

# ---------------------------------------------------------------- 主流程


def bring_up(no_open=False, splash=None, timeout=90):
    """返回 dict(ok, action, secs, msg)"""
    result = {'ok': False, 'action': '', 'secs': 0.0, 'msg': ''}
    if probe():
        log('服务已在运行 (探测 %s 成功)' % API_STATUS)
        result.update(ok=True, action='already')
        return result

    if port_busy():
        log('端口 %d 被占用但对 /api/status 无响应 → 判定残留进程, 清理后重起' % PORT)
        if splash:
            splash.text('正在清理上一份残留进程…', '需要几秒钟')
        kill_port()
        time.sleep(1.2)

    pid = start_server()
    if splash:
        splash.text('正在启动服务…', '已拉起进程 pid=%d' % pid)

    def _cb(el):
        if splash:
            splash.text('正在启动服务…', '已等待 %.0f 秒 (最多 %d 秒)' % (el, timeout))

    ok, secs = wait_ready(timeout, _cb)
    result['secs'] = round(secs, 1)
    if not ok:
        result.update(ok=False, action='timeout',
                      msg='等了 %.0f 秒服务仍未响应，详见 launcher.log' % secs)
        log('启动超时: %.1f 秒内 /api/status 一直无响应' % secs)
        return result

    log('服务已就绪 (用时 %.1f 秒)' % secs)
    result.update(ok=True, action='started')
    return result


def main():
    args = set(a.lower() for a in sys.argv[1:])
    if '--check' in args:
        info = {'probe': probe(), 'port_busy': port_busy(), 'pids': pid_on_port(),
                'chrome': chrome_exe(), 'window': find_app_window(),
                'log': LOG_FILE, 'base': BASE}
        print(json.dumps(info, ensure_ascii=False, indent=1))
        return 0

    no_open = '--no-open' in args
    no_splash = '--no-splash' in args

    splash = None
    if not no_splash:
        splash = Splash()
        if splash.show():
            splash.text('正在启动服务…', '检查本地服务状态')
        else:
            splash = None

    try:
        res = bring_up(no_open=no_open, splash=splash)
    except Exception:
        log('启动异常:\n' + traceback.format_exc())
        res = {'ok': False, 'action': 'error', 'secs': 0, 'msg': '启动异常, 详见 launcher.log'}

    if res['ok']:
        if not no_open:
            if splash:
                splash.text('服务已就绪，正在打开窗口…', '用时 %.1f 秒' % res['secs'])
            act = open_app()
            res['open'] = act
        if splash:
            time.sleep(0.35)
            splash.close()
        log('完成: %s (%.1f 秒)' % (res['action'], res['secs']))
        return 0

    # 失败: 给用户一个能看懂的弹窗
    if splash:
        try:
            splash.text('启动失败', res.get('msg', '')[:60])
            import tkinter as tk
            from tkinter import messagebox
            messagebox.showerror('BS King 启动失败',
                                 (res.get('msg') or '服务没能起来') +
                                 '\n\n日志: ' + LOG_FILE)
        except Exception:
            pass
        splash.close()
    return 1


if __name__ == '__main__':
    sys.exit(main())
