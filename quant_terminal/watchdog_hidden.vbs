' 开机自启 (任务计划 BSKing_Quant_Watchdog 登录时调用)
' 作用: 隐藏窗口运行 watchdog, 守护 8799 量化终端 (server 死 24s 内自动拉起)
' 停用自启: 控制面板删除任务 BSKing_Quant_Watchdog, 或删除本文件
' 临时停守护: 在 quant_terminal\ 目录放 .watchdog_off (删除文件恢复)
Set sh = CreateObject("WScript.Shell")
sh.Run """C:\Users\89302\AppData\Local\Programs\Python\Python312\python.exe"" ""D:\AI Working\BS King\quant_terminal\watchdog.py""", 0, False
