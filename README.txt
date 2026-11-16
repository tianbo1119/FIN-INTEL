# FIN-INTEL

**A Free Financial News Intelligence System Built with Python**

FIN-INTEL 是一个个人开发的免费金融新闻情报采集与筛选系统，旨在从多个公开新闻来源收集财经信息，通过去重、筛选和重要性排序，帮助用户快速发现值得关注的金融事件。

## 项目功能

- 多来源金融新闻采集
- 最近 48 小时新闻筛选
- 新闻链接去重
- 新闻历史记录管理
- 新闻重要性评分与排序
- SQLite 本地数据库
- Telegram 机器人新闻推送
- 多群使用与权限管理
- 多群采集任务排队
- 手动触发采集，无需定时任务
- 尽可能采用免费、开放的信息来源

## 技术栈

- Python
- SQLite
- Telegram Bot API
- RSS / HTTP 新闻采集
- BeautifulSoup
- Feedparser

## 项目目录

```text
FIN-INTEL/
├── main.py
├── telegram_bot.py
├── sources.json
├── news_v2.db
├── news_result.txt
├── bot_config.json
├── bot_token.txt
└── README.md
```

实际目录可能随版本更新而变化。Token 和个人配置文件不应公开。

## 运行环境

- Python 3.10 或更高版本
- Windows / Linux
- Telegram Bot（可选，用于新闻推送）

## 安装

克隆项目：

```bash
git clone https://github.com/tianbo1119/FIN-INTEL.git
cd FIN-INTEL
```

创建虚拟环境：

```bash
python -m venv .venv
```

激活虚拟环境：

Windows：

```powershell
.venv\Scripts\Activate.ps1
```

Linux：

```bash
source .venv/bin/activate
```

安装依赖：

```bash
pip install -r requirements.txt
```

## 运行新闻采集

```bash
python main.py
```

## Telegram 机器人

创建自己的 Telegram Bot，并在本地配置：

- `bot_token.txt`：保存自己的机器人 Token。
- `bot_config.json`：设置允许使用的群 ID 和用户 ID。

启动机器人：

```bash
python telegram_bot.py
```

支持命令：

| 命令 | 功能 |
|---|---|
| `/start` | 显示帮助 |
| `/id` | 查看群 ID 和用户 ID |
| `/news` | 手动采集并推送新闻 |
| `/latest` | 查看最近一次采集结果 |

新闻采集由用户手动触发，不会自动定时运行。

## 项目理念

尽可能利用公开信息、免费资源和开源技术，构建一个个人可以运行、维护和扩展的金融新闻情报工具。

这是一个持续开发中的个人开源项目，欢迎交流、测试和改进。

## 免责声明

本项目仅用于金融新闻信息收集与技术研究，不构成投资建议。新闻来源、内容完整性及准确性取决于第三方公开信息源。
