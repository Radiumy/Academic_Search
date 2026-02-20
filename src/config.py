from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from pathlib import Path
import yaml
import random
import os
import re

DEFAULT_KEYWORDS_PATH = Path(__file__).parent.parent / "config" / "keywords.yaml"
DEFAULT_URL_PATTERNS_PATH = Path(__file__).parent.parent / "config" / "url_patterns.yaml"
DEFAULT_ANTI_CRAWLER_PATH = Path(__file__).parent.parent / "config" / "anti_crawler.yaml"

@dataclass
class Keywords:
    positive: List[str] = field(default_factory=list)
    blacklist: List[str] = field(default_factory=list)
    blacklist_regex: List[str] = field(default_factory=list)
    blacklist_domain: List[str] = field(default_factory=list)
    _compiled_regex: List[re.Pattern] = field(default_factory=list, init=False, repr=False)

    def __init__(self, keywords_path: str = None):
        path = Path(keywords_path) if keywords_path else DEFAULT_KEYWORDS_PATH
        if path.exists():
            with open(path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f) or {}
            self.positive = data.get('positive', [])
            # Support both old 'negative' and new 'blacklist' keys
            self.blacklist = data.get('blacklist', data.get('negative', []))
            self.blacklist_regex = data.get('blacklist_regex', [])
            self.blacklist_domain = data.get('blacklist_domain', [])
        else:
            self.positive = []
            self.blacklist = []
            self.blacklist_regex = []
            self.blacklist_domain = []

        # Normalize: lowercase ASCII keywords
        self.positive = [kw.lower() if kw.isascii() else kw for kw in self.positive]
        self.blacklist = [kw.lower() if kw.isascii() else kw for kw in self.blacklist]
        self.blacklist_domain = [d.lower() for d in self.blacklist_domain]

        # Compile regex patterns for performance
        self._compiled_regex = []
        for pattern in self.blacklist_regex:
            try:
                self._compiled_regex.append(re.compile(pattern, re.IGNORECASE))
            except re.error:
                pass  # Skip invalid regex patterns

    def is_blacklisted(self, url: str) -> bool:
        """Check if URL matches any blacklist pattern"""
        url_lower = url.lower()

        # Check domain blacklist
        for domain in self.blacklist_domain:
            if domain in url_lower:
                return True

        # Check regex blacklist
        for pattern in self._compiled_regex:
            if pattern.search(url):
                return True

        return False

@dataclass
class URLPatterns:
    category_patterns: List[str] = field(default_factory=list)
    profile_patterns: List[str] = field(default_factory=list)
    profile_regex_patterns: List[str] = field(default_factory=list)
    exclude_patterns: List[str] = field(default_factory=list)
    max_path_depth: int = 4
    allowed_roots: List[str] = field(default_factory=list)
    llm_extraction: dict = field(default_factory=dict)

    def __init__(self, patterns_path: str = None):
        path = Path(patterns_path) if patterns_path else DEFAULT_URL_PATTERNS_PATH
        if path.exists():
            with open(path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f) or {}
            self.category_patterns = data.get('category_patterns', [])
            self.profile_patterns = data.get('profile_patterns', [])
            self.profile_regex_patterns = data.get('profile_regex_patterns', [])
            self.exclude_patterns = data.get('exclude_patterns', [])
            self.max_path_depth = data.get('url_constraints', {}).get('max_path_depth', 4)
            self.allowed_roots = data.get('url_constraints', {}).get('allowed_roots', [])
            self.llm_extraction = data.get('llm_extraction', {})
        else:
            self.category_patterns = []
            self.profile_patterns = []
            self.profile_regex_patterns = []
            self.exclude_patterns = []
            self.max_path_depth = 4
            self.allowed_roots = []
            self.llm_extraction = {}

@dataclass
class School:
    name: str
    code: str
    rank: int
    department: str
    url: str
    tier: int
    keywords: Optional[List[str]] = None

    def __post_init__(self):
        # If no per-school keywords override, leave as None
        # (caller should use Keywords object instead)
        if self.keywords is not None:
            self.keywords = [kw.lower() if kw.isascii() else kw for kw in self.keywords]

class Config:
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)

        self.schools = [School(**school) for school in data['schools']]


@dataclass
class AntiCrawlerConfig:
    """反爬虫配置类"""
    stealth: Dict[str, Any] = field(default_factory=dict)
    browser_context: Dict[str, Any] = field(default_factory=dict)
    headers: Dict[str, Any] = field(default_factory=dict)
    rate_limiting: Dict[str, Any] = field(default_factory=dict)
    proxy: Dict[str, Any] = field(default_factory=dict)
    extra_args: List[str] = field(default_factory=list)
    init_scripts: List[str] = field(default_factory=list)
    site_specific: Dict[str, Any] = field(default_factory=dict)

    def __init__(self, config_path: str = None):
        path = Path(config_path) if config_path else DEFAULT_ANTI_CRAWLER_PATH
        if path.exists():
            with open(path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f) or {}
            self.stealth = data.get('stealth', {})
            self.browser_context = data.get('browser_context', {})
            self.headers = data.get('headers', {})
            self.rate_limiting = data.get('rate_limiting', {})
            self.proxy = data.get('proxy', {})
            self.extra_args = data.get('extra_args', [])
            self.init_scripts = data.get('init_scripts', [])
            self.site_specific = data.get('site_specific', {})
        else:
            # 默认配置
            self.stealth = {'enabled': True, 'user_agent': {'mode': 'random'}}
            self.browser_context = {
                'viewport': {'width': 1920, 'height': 1080},
                'page_timeout': 60000,
                'wait_for': 'body',
                'delay_before_return_html': 2,
            }
            self.headers = {}
            self.rate_limiting = {'delay_range': [2, 5], 'jitter_range': [0.5, 1.5], 'max_retries': 3}
            self.proxy = {'enabled': False, 'servers': []}
            self.extra_args = [
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox"
            ]
            self.init_scripts = []
            self.site_specific = {}

    def get_random_delay(self) -> float:
        """获取随机请求延迟"""
        delay_range = self.rate_limiting.get('delay_range', [2, 5])
        jitter_range = self.rate_limiting.get('jitter_range', [0.5, 1.5])
        base_delay = random.uniform(delay_range[0], delay_range[1])
        jitter = random.uniform(jitter_range[0], jitter_range[1])
        return base_delay + jitter

    def get_random_header(self, header_type: str) -> str:
        """获取随机 HTTP 头"""
        options = self.headers.get(header_type, [])
        if options:
            return random.choice(options)
        return ""

    def get_site_config(self, domain: str) -> Dict[str, Any]:
        """获取特定网站的配置"""
        return self.site_specific.get(domain, {})
