# 第三方组件说明

本仓库的 MIT 许可证（见 [LICENSE](LICENSE)）**仅覆盖本项目自行编写的源代码**。
下列内容的版权归各自所有者，不在 MIT 范围内：

## 1. 通达信 tqs SDK —— `tdx/lib/`

包含 `TdxAiData.dll`、`libTdxAiData.so`、`libTdxAiData.dylib`、`mfc100.dll`。

该 SDK 为**闭源商业软件**，此处仅供调试时连带提供。使用者需自行确认其对本 SDK
的使用授权，并自行申请通达信数据服务 Key（填在 `tdx/lib/tdxaidata/lib/user.ini`）。

**如果你不需要通达信数据源，可以直接删除整个 `tdx/` 目录** —— 除了「板块归属」
「自由流通股本」等少数功能会留空外，终端其余部分照常运行。

## 2. Apache ECharts —— `quant_terminal/echarts.min.js`

Copyright The Apache Software Foundation，遵循 **Apache-2.0** 许可。

## 3. 行情数据 —— `kline_data/` 与 `dragon_data/`

通过第三方公开接口获取（新浪、腾讯、东方财富、同花顺、通达信等），
**不由本项目创作**，也不在本项目的 MIT 授权范围内。

这些数据仅作为运行示例一并提供，方便克隆后立刻跑起来。
使用者若将其用于任何商业用途，须自行承担合规责任。

## 4. 免责声明

本项目仅用于**技术学习与研究**，不构成任何投资建议。
据此进行的任何投资决策及其后果，由使用者自行承担。