# -*- coding: utf-8 -*-
"""
FIN-INTEL
中文金融新闻采集、事件筛选、跨媒体去重与重要性排序

功能：
1. 采集中文金融新闻
2. 只保留最近48小时新闻
3. 扩大各来源候选采集数量
4. 重要性评分与事件分类
5. 跨媒体事件去重
6. 来源均衡排序
7. 最多输出100条新闻
8. 只输出标题和原文链接
9. 不抓取正文，不调用付费API
"""

import json
import re
import sqlite3
import ssl
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import (
    urljoin,
    urlparse,
    urldefrag,
    parse_qsl,
    urlencode,
    urlunparse,
)
from concurrent.futures import ThreadPoolExecutor, as_completed
from difflib import SequenceMatcher

import requests
import feedparser
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ============================================================
# 一、基础配置
# ============================================================

BASE = Path(__file__).resolve().parent

SOURCES_FILE = BASE / "sources.json"
DB_FILE = BASE / "news_v2.db"
OUTPUT_FILE = BASE / "news_result.txt"

LOOKBACK_HOURS = 48

# 最终新闻数量上限
MAX_RESULTS = 100

MAX_WORKERS = 10
ARTICLE_WORKERS = 12
REQUEST_TIMEOUT = 12

# 扩大网站候选采集数量
MAX_HTML_LINKS_PER_SOURCE = 120
MAX_ARTICLE_CHECKS_PER_SOURCE = 80

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml,"
        "application/rss+xml,application/atom+xml,*/*;q=0.8"
    ),
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.5",
    "Connection": "close",
}


# ============================================================
# 二、失败网站备用入口
# ============================================================

FEED_OVERRIDES = {
    "法国国际广播电台中文": [
        "https://www.rfi.fr/cn/rss",
        "http://cn.rfi.fr/general/rss",
    ],
    "金融时报中文网": [
        "https://www.ftchinese.com/rss/news",
        "https://www.ftchinese.com/rss/feed",
        "http://www.ftchinese.com/rss/hotstoryby7day",
    ],
}

URL_OVERRIDES = {
    "证券日报": [
        "https://www.zqrb.cn/",
        "http://www.zqrb.cn/",
        "https://www.zqrb.cn/finance/hongguanjingji/",
        "http://www.zqrb.cn/finance/hongguanjingji/",
    ],
}


# ============================================================
# 三、金融新闻识别词库
# ============================================================

FINANCE_TERMS = (
    "金融", "财经", "经济", "货币", "央行", "利率", "降息", "加息", "降准",
    "人民币", "美元", "外汇", "汇率", "债券", "国债", "银行", "保险", "证券",
    "基金", "信托", "期货", "期权", "股票", "股市", "A股", "港股", "美股",
    "资本市场", "上市公司", "IPO", "再融资", "并购", "重组", "分红", "回购",
    "减持", "增持", "业绩", "财报", "营收", "净利润", "交易所", "证监会",
    "金融监管", "金融风险", "流动性", "信贷", "融资", "财政", "税收", "GDP",
    "CPI", "PPI", "PMI", "社融", "M2", "外资", "北向资金", "融资融券",
    "房地产", "房贷", "房企", "地产", "大宗商品", "黄金", "原油", "铜价",
    "铁矿石", "人工智能", "半导体", "芯片", "算力", "新能源", "制造业",
    "出口", "进口", "贸易", "关税", "产业政策", "国资", "国企", "地方债",
    "地方政府债", "财政部", "美联储", "欧洲央行", "日本央行", "金融市场",
    "商业银行", "存款", "贷款", "支付", "公募", "私募", "资管", "量化",
    "数据中心", "光模块", "储能", "电力市场", "供应链",
)

CRITICAL_TERMS = (
    "降息", "加息", "降准", "利率决议", "货币政策", "财政政策",
    "重大资产重组", "控制权变更", "破产重整", "债务违约",
    "流动性危机", "金融风险", "系统性风险", "金融稳定",
    "资本管制", "外汇干预", "重大改革", "监管新规", "监管办法",
    "实施方案", "政策调整", "反垄断", "立案调查", "行政处罚",
    "风险处置", "银行接管", "银行倒闭", "金融机构破产",
    "主权债务", "债务危机", "贸易制裁", "关税措施",
    "出口管制", "制裁措施", "资产注入", "重整计划",
    "退市", "暂停上市", "巨额亏损", "巨额减值",
    "财务造假", "重大合同", "国家队", "金融支持",
    "资本市场改革", "存款准备金率",
)

POLICY_TERMS = (
    "中国人民银行", "央行", "证监会", "金融监管总局",
    "国务院", "财政部", "国家统计局", "国家发展改革委",
    "中央政治局", "公开市场操作", "逆回购", "MLF", "LPR",
    "社融", "M2", "GDP", "CPI", "PPI", "PMI",
    "地方债", "专项债", "国债", "金融监管", "资本市场",
    "政策", "条例", "办法", "规定", "通知", "意见", "方案", "改革",
)

MARKET_TERMS = (
    "并购", "收购", "重组", "回购", "增持", "减持", "分红",
    "IPO", "再融资", "业绩预增", "业绩预亏", "净利润", "营收",
    "融资", "债券", "基金", "ETF", "券商", "银行", "保险",
    "房地产", "半导体", "芯片", "人工智能", "算力", "新能源",
    "大宗商品", "黄金", "原油", "人民币", "汇率", "外贸",
    "出口", "进口", "关税", "产业链", "数据中心", "电力", "储能",
)

LOW_VALUE_TERMS = (
    "早盘", "午盘", "收盘播报", "收评", "盘前", "盘中", "盘后",
    "涨停复盘", "跌停复盘", "机构观点", "券商观点", "投资者提问",
    "互动平台", "直播", "视频", "图集", "招聘", "广告", "股吧",
    "操作策略", "明日股市", "市场观点", "投资建议",
    "每日必读", "一图看懂", "盘点", "投资情报",
)


# ============================================================
# 四、网络请求
# ============================================================

class TLS12Adapter(HTTPAdapter):
    """TLS 1.2备用连接，仍然验证SSL证书。"""

    def init_poolmanager(self, connections, maxsize, block=False, **kwargs):
        context = ssl.create_default_context()
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.maximum_version = ssl.TLSVersion.TLSv1_2

        kwargs["ssl_context"] = context

        return super().init_poolmanager(
            connections,
            maxsize,
            block,
            **kwargs,
        )


def make_session(tls12=False):
    session = requests.Session()

    retry = Retry(
        total=2,
        connect=2,
        read=1,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )

    if tls12:
        adapter = TLS12Adapter(max_retries=retry)
    else:
        adapter = HTTPAdapter(max_retries=retry)

    session.mount("https://", adapter)
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.headers.update(HEADERS)

    return session


def request_page(url):
    """
    依次尝试：
    1. HTTPS默认TLS
    2. HTTPS TLS 1.2
    3. HTTP回退

    不关闭SSL验证。
    """

    attempts = [(url, False)]

    if url.startswith("https://"):
        attempts.append((url, True))
        attempts.append(("http://" + url[8:], False))

    errors = []

    for target, tls12 in attempts:
        try:
            session = make_session(tls12)

            response = session.get(
                target,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
            )

            response.raise_for_status()

            response.encoding = (
                response.apparent_encoding or response.encoding
            )

            return response

        except Exception as exc:
            errors.append(
                f"{type(exc).__name__}: {exc}"
            )

    raise RuntimeError(" | ".join(errors[-3:]))


# ============================================================
# 五、标题、链接、时间处理
# ============================================================

def clean_title(value):
    value = re.sub(r"<[^>]+>", " ", str(value or ""))
    value = re.sub(r"[\u200b-\u200f\ufeff]", "", value)
    value = re.sub(r"\s+", " ", value).strip()

    value = re.sub(
        r"^(焦点|快讯|滚动|突发|独家|重磅|头条)\s*[丨|｜：:、-]\s*",
        "",
        value,
    )

    value = re.sub(
        r"\s*[丨|｜]\s*(快讯|滚动|早报|晚报)$",
        "",
        value,
    )

    value = re.sub(r"\s*(来源：|作者：).*$", "", value)
    value = re.sub(r"\s+\d+\s*小时前$", "", value)
    value = re.sub(r"[\|\-—_：: ]+$", "", value)

    return value[:180]


TRACKING_PARAMS = {
    "from",
    "spm",
    "source",
    "sharefrom",
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
}


def normalize_url(url):
    url = urldefrag(str(url or "").strip())[0]

    if not url:
        return ""

    try:
        p = urlparse(url)

        query = [
            (k, v)
            for k, v in parse_qsl(p.query, keep_blank_values=True)
            if k.lower() not in TRACKING_PARAMS
            and not k.lower().startswith("utm_")
        ]

        return urlunparse((
            p.scheme,
            p.netloc,
            p.path,
            p.params,
            urlencode(query),
            "",
        ))

    except Exception:
        return url


def local_now():
    return datetime.now().astimezone()


def parse_datetime(value):
    if not value:
        return None

    value = str(value).strip().replace("Z", "+00:00")
    value = re.sub(r"（.*?）|\(.*?\)", "", value).strip()

    try:
        dt = datetime.fromisoformat(value)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=local_now().tzinfo)

        return dt.astimezone()

    except Exception:
        pass

    formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d",
        "%Y年%m月%d日 %H:%M",
        "%Y年%m月%d日",
    )

    for fmt in formats:
        try:
            dt = datetime.strptime(value[:19], fmt)
            return dt.replace(tzinfo=local_now().tzinfo)

        except Exception:
            continue

    match = re.search(
        r"(20\d{2})[-年/](\d{1,2})[-月/](\d{1,2})",
        value,
    )

    if match:
        try:
            dt = datetime(
                int(match.group(1)),
                int(match.group(2)),
                int(match.group(3)),
            )

            return dt.replace(tzinfo=local_now().tzinfo)

        except Exception:
            return None

    return None


def rss_entry_datetime(entry):
    for key in (
        "published_parsed",
        "updated_parsed",
        "created_parsed",
    ):
        parsed = entry.get(key)

        if parsed:
            try:
                dt = datetime(
                    *parsed[:6],
                    tzinfo=timezone.utc,
                )

                return dt.astimezone()

            except Exception:
                pass

    for key in (
        "published",
        "updated",
        "created",
        "pubDate",
        "date",
    ):
        dt = parse_datetime(entry.get(key))

        if dt:
            return dt

    return None


def within_window(published, now):
    if published is None:
        return False

    try:
        pub = published.astimezone()
        current = now.astimezone()

    except Exception:
        return False

    if pub > current + timedelta(minutes=5):
        return False

    return pub >= current - timedelta(hours=LOOKBACK_HOURS)


def is_financial(title):
    upper = title.upper()

    return any(
        term.upper() in upper
        for term in FINANCE_TERMS
    )


def is_low_value(title):
    upper = title.upper()

    return any(
        term.upper() in upper
        for term in LOW_VALUE_TERMS
    )


# ============================================================
# 六、文章发布时间提取
# ============================================================

DATE_SELECTORS = (
    ('meta[property="article:published_time"]', "content"),
    ('meta[name="publishdate"]', "content"),
    ('meta[name="publish_date"]', "content"),
    ('meta[name="PubDate"]', "content"),
    ('meta[name="date"]', "content"),
    ('meta[itemprop="datePublished"]', "content"),
    ("time[datetime]", "datetime"),
)


def extract_jsonld_dates(soup):
    dates = []

    for node in soup.find_all(
        "script",
        type="application/ld+json",
    ):
        raw = node.string or node.get_text()

        if not raw:
            continue

        try:
            data = json.loads(raw)

        except Exception:
            continue

        stack = data if isinstance(data, list) else [data]

        while stack:
            item = stack.pop()

            if isinstance(item, list):
                stack.extend(item)

            elif isinstance(item, dict):

                for key in ("datePublished", "dateCreated"):
                    dt = parse_datetime(item.get(key))

                    if dt:
                        dates.append(dt)

                for value in item.values():
                    if isinstance(value, (dict, list)):
                        stack.append(value)

    return dates


def extract_page_datetime(soup):
    dates = []

    for selector, attr in DATE_SELECTORS:
        node = soup.select_one(selector)

        if node:
            dt = parse_datetime(node.get(attr))

            if dt:
                dates.append(dt)

    dates.extend(extract_jsonld_dates(soup))

    for node in soup.select(
        "time[datetime], time, .time, .date, "
        ".publish-time, .pubtime, .article-time"
    ):
        value = (
            node.get("datetime")
            or node.get("content")
            or node.get_text(" ", strip=True)
        )

        if "更新" in value:
            continue

        dt = parse_datetime(value)

        if dt:
            dates.append(dt)

    if not dates:
        return None

    return min(dates)


# ============================================================
# 七、RSS采集
# ============================================================

def parse_rss_url(url, source, now):
    response = request_page(url)

    feed = feedparser.parse(response.content)

    if getattr(feed, "bozo", False) and not feed.entries:
        raise RuntimeError("RSS解析失败或没有可用条目")

    result = []

    for entry in feed.entries[:200]:

        title = clean_title(entry.get("title"))
        link = normalize_url(entry.get("link"))
        published = rss_entry_datetime(entry)

        if not title:
            continue

        if not link.startswith(("http://", "https://")):
            continue

        # 这里只做金融主题过滤。
        # 普通市场新闻留给后续评分系统降权。
        if not is_financial(title):
            continue

        if not within_window(published, now):
            continue

        result.append({
            "title": title,
            "url": link,
            "source": source["name"],
            "source_weight": int(source.get("weight", 3)),
            "published": published,
        })

    return result


def discover_rss(source):
    urls = []

    for homepage in (
        URL_OVERRIDES.get(
            source.get("name", ""),
            [source["url"]],
        )
    )[:2]:

        try:
            response = request_page(homepage)

            soup = BeautifulSoup(
                response.text,
                "html.parser",
            )

            for node in soup.select(
                'link[rel="alternate"][href]'
            ):
                typ = (node.get("type") or "").lower()

                if "rss" in typ or "atom" in typ or "xml" in typ:
                    urls.append(
                        urljoin(
                            response.url,
                            node.get("href"),
                        )
                    )

        except Exception:
            continue

    return urls


def parse_rss_source(source, now):
    urls = []

    urls.extend(
        FEED_OVERRIDES.get(
            source.get("name", ""),
            [],
        )
    )

    if source.get("type") == "rss":
        urls.append(source["url"])

    urls.extend(discover_rss(source))

    urls = list(dict.fromkeys(urls))
    errors = []

    for url in urls:
        try:
            rows = parse_rss_url(url, source, now)

            if rows:
                return rows

        except Exception as exc:
            errors.append(
                f"{url}: {type(exc).__name__}: {exc}"
            )

    if errors:
        raise RuntimeError(
            "RSS备用入口失败：" + "；".join(errors[-2:])
        )

    return []


# ============================================================
# 八、HTML网站采集
# ============================================================

BAD_URL_PARTS = (
    "/search", "/login", "/register", "/about",
    "/contact", "/tag/", "/tags/", "/author/",
    "/video/", "/photo/", "javascript:", "mailto:",
    "weibo.com", "weixin.qq.com", "douyin.com",
)


def source_home_urls(source):
    return URL_OVERRIDES.get(
        source.get("name", ""),
        [source["url"]],
    )


def same_site(url, homepage):
    host = urlparse(url).netloc.lower().removeprefix("www.")
    base = urlparse(homepage).netloc.lower().removeprefix("www.")

    return bool(
        host and base
        and (host == base or host.endswith("." + base))
    )


def get_html_candidates(source):
    candidates = []
    seen = set()
    errors = []

    for homepage in source_home_urls(source):

        try:
            response = request_page(homepage)

            soup = BeautifulSoup(
                response.text,
                "html.parser",
            )

            for anchor in soup.select("a[href]"):

                title = clean_title(
                    anchor.get_text(" ", strip=True)
                )

                url = normalize_url(
                    urljoin(
                        response.url,
                        anchor.get("href", ""),
                    )
                )

                if len(title) < 7 or len(title) > 115:
                    continue

                # 不在采集阶段提前排除普通市场新闻。
                if not is_financial(title):
                    continue

                if not url.startswith(("http://", "https://")):
                    continue

                if not same_site(url, response.url):
                    continue

                if url in seen:
                    continue

                if any(
                    part in url.lower()
                    for part in BAD_URL_PARTS
                ):
                    continue

                path = urlparse(url).path.strip("/")

                if len(path) < 6:
                    continue

                if path.count("/") < 1 and not re.search(
                    r"\d{6,}",
                    path,
                ):
                    continue

                seen.add(url)

                candidates.append((title, url))

            if candidates:
                break

        except Exception as exc:
            errors.append(
                f"{homepage}: {type(exc).__name__}: {exc}"
            )

    if not candidates and errors:
        raise RuntimeError(
            "首页访问失败：" + "；".join(errors[-2:])
        )

    return candidates[:MAX_HTML_LINKS_PER_SOURCE]


def fetch_article_record(candidate, source, now):
    title, url = candidate

    try:
        response = request_page(url)

        soup = BeautifulSoup(
            response.text,
            "html.parser",
        )

        published = extract_page_datetime(soup)

        if not within_window(published, now):
            return None

        # 优先使用文章页正式标题。
        page_title = ""

        for selector in (
            'meta[property="og:title"]',
            'meta[name="twitter:title"]',
        ):
            node = soup.select_one(selector)

            if node:
                page_title = clean_title(
                    node.get("content")
                )

                if page_title:
                    break

        if not page_title and soup.title:
            page_title = clean_title(
                soup.title.get_text(" ", strip=True)
            )

        if (
            page_title
            and 7 <= len(page_title) <= 150
            and is_financial(page_title)
        ):
            title = page_title

        if not is_financial(title):
            return None

        return {
            "title": title,
            "url": normalize_url(response.url),
            "source": source["name"],
            "source_weight": int(source.get("weight", 3)),
            "published": published,
        }

    except Exception:
        return None


def parse_html_source(source, now):
    candidates = get_html_candidates(source)

    if not candidates:
        return []

    candidates = candidates[:MAX_ARTICLE_CHECKS_PER_SOURCE]

    result = []

    with ThreadPoolExecutor(
        max_workers=ARTICLE_WORKERS
    ) as pool:

        jobs = [
            pool.submit(
                fetch_article_record,
                candidate,
                source,
                now,
            )
            for candidate in candidates
        ]

        for future in as_completed(jobs):
            try:
                item = future.result()

                if item:
                    result.append(item)

            except Exception:
                continue

    return result


# ============================================================
# 九、单个来源采集调度
# ============================================================

def collect_one(source, now):
    name = source.get("name", "未知来源")

    # RSS源优先RSS，失败或无近期数据时回退HTML。
    if source.get("type") == "rss" or name in FEED_OVERRIDES:

        try:
            rows = parse_rss_source(source, now)

            if rows:
                return rows

        except Exception as rss_error:

            try:
                return parse_html_source(source, now)

            except Exception as html_error:
                raise RuntimeError(
                    f"RSS失败：{rss_error}；"
                    f"HTML回退失败：{html_error}"
                )

        if source.get("type") == "rss":
            return parse_html_source(source, now)

        return []

    return parse_html_source(source, now)


# ============================================================
# 十、跨媒体标题去重
# ============================================================

def title_key(title):
    text = str(title or "").lower()

    text = re.sub(
        r"(每日经济新闻|证券日报|证券时报|中国证券报|早参|晚参|快讯|滚动)",
        "",
        text,
    )

    text = re.sub(
        r"\d{4}年?\d{1,2}月?\d{1,2}日?",
        "",
        text,
    )

    text = re.sub(
        r"[^\u4e00-\u9fffA-Za-z0-9]",
        "",
        text,
    )

    return text


def bigrams(text):
    return {
        text[i:i + 2]
        for i in range(max(0, len(text) - 1))
    }


def similar_title(a, b):
    ka = title_key(a)
    kb = title_key(b)

    if not ka or not kb:
        return False

    if ka == kb or ka in kb or kb in ka:
        return True

    ratio = SequenceMatcher(
        None,
        ka,
        kb,
    ).ratio()

    if ratio >= 0.82:
        return True

    ga = bigrams(ka)
    gb = bigrams(kb)

    if not ga or not gb:
        return False

    jaccard = len(ga & gb) / len(ga | gb)

    return jaccard >= 0.68


# ============================================================
# 十一、重要性评分、事件识别与跨媒体去重
# ============================================================

# 明确属于营销/软文的标题直接剔除。
HARD_REJECT_TERMS = (
    "基金经理：", "基金经理表示", "基金经理谈",
    "让投资触手可及", "做中国市场长跑者",
    "品牌焕新", "品牌升级", "新品发布", "产品推介",
    "营销活动", "客户答谢", "财富讲堂", "投资者教育活动",
    "专题培训班", "业务培训班", "培训会", "培训活动",
    "招聘公告", "校园招聘", "社会招聘",
    "签约仪式", "战略合作签约仪式", "参观考察",
)

SOFT_REJECT_TERMS = (
    "基金早参", "券商早参", "市场早报", "市场晚报",
    "基金排名", "收益榜", "业绩榜", "产品发行",
    "新基金发行", "基金募集", "基金经理变更",
    "投资者问答", "互动易回复", "机构调研接待",
    "收盘播报", "午间播报", "盘中播报",
)

OFFICIAL_SOURCES = (
    "中国人民银行", "中国证监会", "国家金融监督管理总局",
    "国家统计局", "中国政府网", "国家发展改革委",
    "财政部", "国务院",
)

HIGH_CREDIBILITY_SOURCES = (
    "新华网", "人民网", "央视网", "中国新闻网财经",
    "央广网", "证券时报", "上海证券报", "第一财经",
    "财联社", "21世纪经济报道", "每日经济新闻",
    "联合早报", "金融时报中文网", "BBC中文", "中国证券报",
    "德国之声中文", "法国国际广播电台中文",
    "纽约时报中文网",
)

POLICY_EVENT_TERMS = (
    "降息", "加息", "降准", "存款准备金率", "利率决议",
    "LPR", "货币政策", "财政政策", "专项债", "特别国债",
    "资本市场改革", "监管新规", "监管办法", "政策落地",
    "政策出台", "政策调整", "政策发布", "实施方案",
    "行政处罚", "立案调查", "风险处置", "金融稳定",
)

MACRO_EVENT_TERMS = (
    "GDP", "CPI", "PPI", "PMI", "社融", "M2",
    "房地产开发投资", "固定资产投资", "工业增加值",
    "进出口", "出口额", "贸易顺差", "失业率",
    "制造业景气", "居民消费价格", "生产者价格",
    "经济增长", "经济增速", "经济数据",
)

CORPORATE_EVENT_TERMS = (
    "重大资产重组", "资产重组", "控制权变更", "控制权转让",
    "借壳", "资产注入", "重大并购", "收购", "被收购",
    "破产重整", "重整计划", "债务违约", "债券违约",
    "退市", "强制退市", "立案调查", "财务造假",
    "业绩预增", "业绩预亏", "业绩暴增", "业绩大幅下滑",
    "巨额亏损", "巨额减值", "重大合同", "中标",
    "停产", "复产", "重大投资", "分拆上市",
)

INDUSTRY_EVENT_TERMS = (
    "量产", "商业化", "突破", "技术突破", "投产", "扩产",
    "人工智能合作年", "合作年", "合作协议", "重大项目", "订单",
    "供不应求", "涨价", "降价", "断供", "停供",
    "出口管制", "制裁", "技术封锁", "供应链中断",
    "算力中心", "数据中心", "先进制程", "先进封装",
    "HBM", "光模块", "光芯片", "人形机器人",
    "储能项目", "电网投资", "核电项目",
)

GLOBAL_EVENT_TERMS = (
    "美联储", "欧洲央行", "日本央行", "英国央行",
    "关税", "贸易制裁", "出口管制", "金融制裁",
    "主权债务", "债务危机", "银行倒闭", "银行接管",
    "战争", "停火", "制裁措施", "供应链中断",
)

ROUTINE_MARKET_TERMS = (
    "午评", "收评", "早评", "收盘", "早盘", "午盘",
    "三大指数", "超3500只个股", "个股飘红", "个股飘绿",
    "涨停复盘", "跌停复盘", "板块异动", "异动速递",
    "资金流向日报", "龙虎榜",
)

EVENT_STOP_TERMS = (
    "中国", "我国", "国内", "全球", "今日", "昨日", "今天",
    "最新", "消息", "发布", "公布", "宣布", "表示", "指出",
    "记者", "报道", "观察", "解读", "分析", "关注",
    "持续", "进一步", "加快", "推动", "促进", "助力",
    "相关", "多个", "首次", "再度", "正式", "有望",
    "迎来", "之后", "以来", "方面", "领域", "市场",
    "经济", "金融", "行业", "企业", "公司",
)


def _contains_any(text, terms):
    upper = str(text or "").upper()

    return [
        term
        for term in terms
        if term.upper() in upper
    ]


def is_hard_reject(title):
    """只剔除明显营销、招聘、培训等非新闻内容。"""

    return bool(
        _contains_any(title, HARD_REJECT_TERMS)
    )


def source_quality(item):
    """来源可信度分层。"""

    name = str(item.get("source", ""))

    if any(term in name for term in OFFICIAL_SOURCES):
        return 12

    if any(term in name for term in HIGH_CREDIBILITY_SOURCES):
        return 8

    return int(item.get("source_weight", 3)) * 2


def classify_news(title):
    """按标题可验证的事件线索分类。"""

    groups = {
        "policy": _contains_any(title, POLICY_EVENT_TERMS),
        "macro": _contains_any(title, MACRO_EVENT_TERMS),
        "corporate": _contains_any(title, CORPORATE_EVENT_TERMS),
        "industry": _contains_any(title, INDUSTRY_EVENT_TERMS),
        "global": _contains_any(title, GLOBAL_EVENT_TERMS),
        "critical": _contains_any(title, CRITICAL_TERMS),
    }

    return groups


def score_item(item, now):
    """
    以事件实质性为主、来源可信度为辅。

    不因标题含有“基金/银行/AI/芯片”等泛行业词，
    就自动判定为重大新闻。
    """

    title = item["title"]

    groups = classify_news(title)

    score = source_quality(item)

    # 重大政策、监管和金融稳定事件。
    score += min(len(groups["critical"]), 3) * 13
    score += min(len(groups["policy"]), 3) * 10

    # 重要宏观数据。
    score += min(len(groups["macro"]), 3) * 8

    # 上市公司实质性事件。
    score += min(len(groups["corporate"]), 3) * 9

    # 产业链实质事件。
    score += min(len(groups["industry"]), 3) * 7

    # 国际重大事件。
    score += min(len(groups["global"]), 3) * 6

    # 官方机构直接发布的政策或数据标题。
    if any(word in title for word in (
        "中国人民银行", "央行", "中国证监会", "证监会",
        "金融监管总局", "国务院", "财政部", "国家统计局",
        "国家发展改革委", "美联储", "欧洲央行", "日本央行",
    )):
        score += 7

    # 明确量化信息作为辅助信号。
    if re.search(
        r"\d+(?:\.\d+)?\s*(?:%|亿元|万亿元|亿美元|万亿美元|亿股|万股|个基点)",
        title,
    ):
        score += 3

    # 普通行情播报和营销软文降权。
    if _contains_any(title, SOFT_REJECT_TERMS):
        score -= 18

    if _contains_any(title, ROUTINE_MARKET_TERMS):
        score -= 10

    if is_low_value(title):
        score -= 8

    # 纯产品宣传、培训、招聘不进入最终结果。
    if is_hard_reject(title):
        score -= 100

    if len(title) < 10:
        score -= 3

    # 时效性作为轻量辅助。
    try:
        age_hours = max(
            0,
            (
                now - item["published"].astimezone()
            ).total_seconds() / 3600,
        )

    except Exception:
        age_hours = LOOKBACK_HOURS

    score += max(0, 6 - int(age_hours / 8))

    item["category"] = ",".join(
        name
        for name, hits in groups.items()
        if hits
    ) or "general"

    return int(score)


def event_key(title):
    """生成较稳定的事件标题指纹。"""

    text = title_key(title)

    text = re.sub(
        r"(快讯|滚动|早报|晚报|午评|收评|早评|午间|晚间|最新)",
        "",
        text,
    )

    for term in EVENT_STOP_TERMS:
        text = text.replace(term, "")

    return text


def event_tokens(title):
    """提取事件中的专有实体和高信息量关键词。"""

    text = event_key(title)

    tokens = set()

    anchors = (
        "中国稀土", "中稀凉山", "英伟达", "DeepSeek",
        "国家统计局", "中国人民银行", "央行", "证监会",
        "美联储", "欧洲央行", "日本央行", "房地产",
        "半导体", "光模块", "光芯片", "人工智能",
        "数据中心", "储能", "新能源汽车", "人民币",
        "美元", "国债", "地方债", "银行", "券商",
        "上市公司", "重大资产重组", "资产重组",
        "降息", "降准", "加息", "关税", "出口管制",
        "债务违约", "破产重整", "业绩预亏", "业绩预增",
    )

    for word in anchors:
        if word in title:
            tokens.add(word)

    tokens.update(bigrams(text))

    return tokens


def similar_event(a, b):
    """
    判断两条标题是否可能描述同一事件。

    提高相似度门槛，减少不同事件误合并。
    """

    ka = event_key(a)
    kb = event_key(b)

    if not ka or not kb:
        return False

    if ka == kb or ka in kb or kb in ka:
        return True

    ratio = SequenceMatcher(None, ka, kb).ratio()

    if ratio >= 0.88:
        return True

    ta = event_tokens(a)
    tb = event_tokens(b)

    if not ta or not tb:
        return False

    overlap = ta & tb
    union = ta | tb

    jaccard = len(overlap) / max(1, len(union))

    shared_anchors = {
        token
        for token in overlap
        if len(token) >= 3 and token in (
            "中国稀土", "中稀凉山", "英伟达", "DeepSeek",
            "国家统计局", "中国人民银行", "美联储",
            "欧洲央行", "日本央行", "半导体", "光模块",
            "光芯片", "人工智能", "数据中心", "新能源汽车",
            "人民币", "重大资产重组", "资产重组",
            "出口管制", "债务违约", "破产重整",
        )
    }

    if len(shared_anchors) >= 1 and jaccard >= 0.45:
        return True

    return jaccard >= 0.68


def preferred_record_key(item):
    """同一事件保留更权威、信息更具体的报道。"""

    title = item["title"]

    detail_bonus = 0

    if re.search(
        r"\d+(?:\.\d+)?\s*(?:%|亿元|万亿元|亿美元|个基点)",
        title,
    ):
        detail_bonus += 2

    if _contains_any(title, (
        "拟", "获批", "落地", "实施", "公布", "正式",
        "重组", "收购", "违约", "处罚", "调查",
    )):
        detail_bonus += 2

    return (
        source_quality(item),
        detail_bonus,
        int(item.get("score", 0)),
        item["published"],
    )


def merge_and_rank(items, now):
    """
    新闻筛选与排序：

    1. 剔除明显营销、招聘、培训内容。
    2. 根据事件重要性和来源可信度评分。
    3. 低于质量线的新闻不输出。
    4. 对同一事件进行跨媒体去重。
    5. 按重要性排序，同时兼顾来源多样性。
    6. 最多输出 MAX_RESULTS 条。
    """

    candidates = []

    for item in items:
        title = clean_title(item.get("title", ""))

        if not title or not item.get("url"):
            continue

        item["title"] = title

        if is_hard_reject(title):
            continue

        item["score"] = score_item(item, now)

        # 保留最低质量门槛。
        if item["score"] < 18:
            continue

        candidates.append(item)

    # 第一轮：重要性排序。
    candidates.sort(
        key=lambda x: (
            x["score"],
            source_quality(x),
            x["published"],
        ),
        reverse=True,
    )

    # 第二轮：跨媒体事件去重。
    selected = []

    for item in candidates:
        duplicate_index = None

        for index, chosen in enumerate(selected):
            if similar_event(
                item["title"],
                chosen["title"],
            ):
                duplicate_index = index
                break

        if duplicate_index is None:
            selected.append(item)

        else:
            old = selected[duplicate_index]

            if preferred_record_key(item) > preferred_record_key(old):
                selected[duplicate_index] = item

    # 第三轮：重新按重要性排序。
    selected.sort(
        key=lambda x: (
            x["score"],
            source_quality(x),
            x["published"],
        ),
        reverse=True,
    )

    # 第四轮：来源均衡。
    # 每一轮每个来源最多选一条。
    source_groups = {}

    for item in selected:
        source_name = item.get("source", "未知来源")

        source_groups.setdefault(
            source_name,
            [],
        ).append(item)

    source_names = sorted(
        source_groups.keys(),
        key=lambda name: (
            source_groups[name][0]["score"],
            source_quality(source_groups[name][0]),
            source_groups[name][0]["published"],
        ),
        reverse=True,
    )

    balanced = []

    round_index = 0

    while len(balanced) < MAX_RESULTS:

        added_this_round = False

        for source_name in source_names:
            group = source_groups[source_name]

            if round_index < len(group):
                balanced.append(group[round_index])
                added_this_round = True

                if len(balanced) >= MAX_RESULTS:
                    break

        if not added_this_round:
            break

        round_index += 1

    return balanced[:MAX_RESULTS]


# ============================================================
# 十二、数据库
# ============================================================

def init_db():
    with sqlite3.connect(DB_FILE) as conn:

        conn.execute("""
            CREATE TABLE IF NOT EXISTS articles (
                url TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                source TEXT NOT NULL,
                published TEXT NOT NULL,
                score INTEGER NOT NULL,
                collected_at TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_articles_published
            ON articles(published)
        """)


def save_articles(items, now):
    collected_at = now.isoformat(timespec="seconds")

    with sqlite3.connect(DB_FILE) as conn:

        before = conn.total_changes

        conn.executemany("""
            INSERT INTO articles (
                url, title, source, published, score, collected_at
            )
            VALUES (?, ?, ?, ?, ?, ?)

            ON CONFLICT(url) DO UPDATE SET
                title=excluded.title,
                source=excluded.source,
                published=excluded.published,
                score=excluded.score,
                collected_at=excluded.collected_at
        """, [
            (
                item["url"],
                item["title"],
                item["source"],
                item["published"].astimezone().isoformat(
                    timespec="seconds"
                ),
                int(item["score"]),
                collected_at,
            )
            for item in items
        ])

        return conn.total_changes - before


# ============================================================
# 十三、主程序
# ============================================================

def main():
    now = local_now()

    print("=" * 62)
    print("FIN-INTEL | 中文金融重点新闻")
    print("只输出新闻标题和原文链接")
    print(f"发布时间范围：最近 {LOOKBACK_HOURS} 小时")
    print(f"最多展示：{MAX_RESULTS} 条")
    print("=" * 62)

    if not SOURCES_FILE.exists():
        print(f"[错误] 找不到来源文件：{SOURCES_FILE}")
        return

    try:
        sources = json.loads(
            SOURCES_FILE.read_text(encoding="utf-8-sig")
        )

        if not isinstance(sources, list):
            raise ValueError("sources.json 顶层必须是数组")

    except Exception as exc:
        print(f"[错误] sources.json 读取失败：{exc}")
        return

    all_items = []

    success_sources = 0
    failed_sources = []

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as pool:

        jobs = {
            pool.submit(
                collect_one,
                source,
                now,
            ): source
            for source in sources
        }

        for future in as_completed(jobs):

            source = jobs[future]

            try:
                rows = future.result()

                all_items.extend(rows)

                success_sources += 1

                print(
                    f"[完成] {source.get('name', '未知来源')}："
                    f"{len(rows)} 条有效新闻"
                )

            except Exception as exc:

                name = source.get("name", "未知来源")

                failed_sources.append(name)

                print(
                    f"[失败] {name}："
                    f"{type(exc).__name__}: {exc}"
                )

    # --------------------------------------------------------
    # URL去重
    # --------------------------------------------------------

    unique = {}

    for item in all_items:

        url = normalize_url(item["url"])

        item["url"] = url

        old = unique.get(url)

        if old is None:
            unique[url] = item

        elif (
            item["source_weight"],
            item["published"],
        ) > (
            old["source_weight"],
            old["published"],
        ):
            unique[url] = item

    # --------------------------------------------------------
    # 重要性评分、事件去重和来源均衡
    # --------------------------------------------------------

    ranked = merge_and_rank(
        list(unique.values()),
        now,
    )

    # --------------------------------------------------------
    # 数据库保存
    # --------------------------------------------------------

    init_db()

    saved = save_articles(ranked, now)

    # --------------------------------------------------------
    # 输出文件
    # --------------------------------------------------------

    output = "\n\n".join(
        f"{index:02d}. {item['title']}\n{item['url']}"
        for index, item in enumerate(ranked, 1)
    )

    if not output:
        output = (
            "本次没有找到发布时间明确、"
            "且符合金融筛选条件的近期新闻。"
        )

    OUTPUT_FILE.write_text(
        output,
        encoding="utf-8",
    )

    # --------------------------------------------------------
    # 运行统计
    # --------------------------------------------------------

    print("\n" + "=" * 62)

    print(f"来源总数：{len(sources)}")
    print(f"采集成功来源：{success_sources}")
    print(f"采集失败来源：{len(failed_sources)}")

    if failed_sources:
        print(
            "失败来源：" + "、".join(failed_sources)
        )

    print(f"原始采集新闻：{len(all_items)} 条")
    print(f"有效新闻（URL去重后）：{len(unique)} 条")
    print(f"本次入库/更新：{saved} 条")
    print(
        f"最终展示：{len(ranked)} 条"
        f"（上限 {MAX_RESULTS}）"
    )

    print("=" * 62)

    print(output)

    print(f"\n结果文件：{OUTPUT_FILE}")
    print(f"数据库：{DB_FILE}")


if __name__ == "__main__":
    main()