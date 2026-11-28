@echo off
chcp 936 >nul 2>&1
title BS King 量化终端 - 创建快捷方式
cd /d "%~dp0"

echo.
echo   ============================================================
echo      BS King 量化终端  -  一键创建桌面图标
echo   ============================================================
echo.
echo   这个脚本会做三件事:
echo     1. 找到你电脑上的 Python
echo     2. 在【桌面】和【开始菜单】各建一个 BS King 图标
echo     3. 以后双击图标就能自动起服务 + 打开应用窗口
echo.
echo   仓库目录: %~dp0
echo.

set "PYW="
set "PY="

where py >nul 2>&1
if not errorlevel 1 (
    for /f "delims=" %%i in ('py -c "import sys;print(sys.executable)" 2^>nul') do set "PY=%%i"
)

if not defined PY (
    where python >nul 2>&1
    if not errorlevel 1 (
        for /f "delims=" %%i in ('where python') do if not defined PY set "PY=%%i"
    )
)

if not defined PY (
    echo   [X] 没找到 Python。
    echo.
    echo   请先安装 Python 3.10 或更高版本:  https://www.python.org/downloads/
    echo   安装时务必勾选 "Add python.exe to PATH"。
    echo.
    pause
    exit /b 1
)

for %%i in ("%PY%") do set "PYDIR=%%~dpi"
if exist "%PYDIR%pythonw.exe" set "PYW=%PYDIR%pythonw.exe"
if not defined PYW set "PYW=%PY%"

echo   Python : %PY%
echo   无窗版 : %PYW%
echo.

"%PY%" -c "import requests" >nul 2>&1
if errorlevel 1 (
    echo   [!] 还没装依赖库。请先在本目录执行:
    echo.
    echo         "%PY%" -m pip install -r requirements.txt
    echo.
    echo   装完再运行一次这个脚本。
    echo.
    pause
    exit /b 1
)

"%PY%" "%~dp0quant_terminal\launcher.py" --make-shortcut
echo.
pause