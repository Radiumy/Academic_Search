from dataclasses import dataclass, field
from typing import List, Optional
from pathlib import Path
import yaml

DEFAULT_KEYWORDS_PATH = Path(__file__).parent.parent / "config" / "keywords.yaml"

@dataclass
class Keywords:
    positive: List[str] = field(default_factory=list)
    negative: List[str] = field(default_factory=list)

    def __init__(self, keywords_path: str = None):
        path = Path(keywords_path) if keywords_path else DEFAULT_KEYWORDS_PATH
        if path.exists():
            with open(path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f) or {}
            self.positive = data.get('positive', [])
            self.negative = data.get('negative', [])
        else:
            self.positive = []
            self.negative = []

        # Normalize: lowercase ASCII keywords
        self.positive = [kw.lower() if kw.isascii() else kw for kw in self.positive]
        self.negative = [kw.lower() if kw.isascii() else kw for kw in self.negative]

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
