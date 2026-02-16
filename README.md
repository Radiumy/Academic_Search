# Academic POI Crawler

一个用于爬取大学和研究机构教授/研究人员信息的自动化工具。

## 项目结构

```
crawler-git/
├── main.py                    # 主入口程序
├── config/
│   └── schools.yaml          # 学校配置
├── src/
│   ├── config.py             # 配置加载模块
│   ├── crawler.py            # 旧版爬虫逻辑
│   ├── person_crawler.py    # 新版爬虫逻辑（推荐）
│   ├── test_searchxng.py    # SearXNG 测试工具
│   ├── dfs_worker.py        # DFS 工作器
│   ├── qdrant-author.py     # Qdrant 作者数据导入
│   ├── qdrant-pubs.py       # Qdrant 论文数据导入
│   └── logging_config.py    # 日志配置
├── searxng_instances.json   # SearXNG 实例配置
├── requirements.txt         # Python 依赖
└── venv/                    # 虚拟环境
```

## 安装

### 环境要求
- Python 3.12+
- pip

### 安装步骤

1. **克隆项目**
   ```bash
   git clone <repo-url>
   cd crawler-git
   ```

2. **创建虚拟环境**（可选但推荐）
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # 或
   venv\Scripts\activate     # Windows
   ```

3. **安装依赖**
   ```bash
   pip install -r requirements.txt
   ```

4. **安装 Playwright 浏览器**（crawl4ai 需要）
   ```bash
   playwright install chromium
   ```

## 配置

### 学校配置 (config/schools.yaml)

```yaml
schools:
  - name: "清华大学"
    code: "tsinghua"
    rank: 1
    department: "计算机科学与技术系"
    url: "https://www.cs.tsinghua.edu.cn/csen/"
    tier: 1
```

配置字段说明：
- `name`: 学校名称
- `code`: 学校代码（用于目录命名）
- `rank`: 世界排名
- `department`: 目标院系
- `url`: 官网 URL
- `tier`: 学校层级（1 为最高优先级）

### SearXNG 实例配置 (searxng_instances.json)

配置可用的 SearXNG 搜索实例，用于隐私保护的搜索功能。

## 使用方法

### 基本用法

```bash
python main.py --config config/schools.yaml
```

### 高级选项

```bash
# 指定 OpenAI API Key
python main.py --config config/schools.yaml --openai-key YOUR_API_KEY

# 仅处理特定层级的学校
python main.py --config config/schools.yaml --tier 1

# 指定自定义 OpenAI API 端点
python main.py --config config/schools.yaml --openai-base https://your-api-endpoint/v1

# 设置超时和重试
python main.py --config config/schools.yaml --openai-timeout 120 --openai-retries 5

# 使用代理
python main.py --config config/schools.yaml --openai-proxy http://proxy:8080
```

## 输出

爬取的数据将保存在 `data/` 目录下，按学校代码组织：

```
data/
├── tsinghua/
│   ├── wang_xiaodong.json
│   ├── li_ming.json
│   └── ...
├── peking/
│   └── ...
└── ...
```

每个 JSON 文件包含以下字段：
```json
{
  "name": "姓名",
  "affiliations": ["机构列表"],
  "current_position": "当前职位",
  "email": "电子邮件",
  "personal_page": "个人主页",
  "group_page": "团队主页",
  "google_scholar": "Google Scholar 主页",
  "research_interests": ["研究方向"],
  "papers": ["论文列表"],
  "related_pages": ["相关页面"]
}
```

## 依赖

核心依赖包括：
- **crawl4ai** - 异步网页爬取框架
- **openai** - OpenAI API 客户端
- **flair** - NLP 命名实体识别
- **qdrant-client** - 向量数据库客户端
- **aiohttp** - 异步 HTTP 客户端
- **beautifulsoup4** - HTML 解析
- **PyYAML** - YAML 配置解析
- **tenacity** - 重试机制

详见 [requirements.txt](requirements.txt)

## 注意事项

1. **API Key**：需要有效的 OpenAI API Key 才能使用 GPT 信息提取功能
2. **网络访问**：确保可以访问目标网站和 OpenAI API
3. **礼貌爬取**：系统已内置速率限制，请勿修改过小以免对目标网站造成负担
4. **数据验证**：提取的数据可能需要人工验证