FIN-INTEL 第一版

运行：
python -m pip install -r requirements.txt
python main.py

输出：news_result.txt（只有标题和原文链接）
本地数据库：news.db

说明：RSS 优先；其余网站第一版尝试读取公开首页链接。网站结构或网络策略变化可能导致个别来源失败。当前评分是标题级初筛，不代表事实核验。先实测来源命中率，再接 Telegram。
