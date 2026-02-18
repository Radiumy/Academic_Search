from dataclasses import dataclass, field
from typing import List, Optional
import yaml

@dataclass
class School:
    name: str
    code: str
    rank: int
    department: str
    url: str
    tier: int
    keywords: Optional[List[str]] = field(default_factory=lambda: ['faculty', 'people', 'directory', 'members', 'staff', 'team'])

    def __post_init__(self):
        # Ensure keywords are lowercase for matching
        self.keywords = [kw.lower() for kw in (self.keywords or ['faculty', 'people', 'directory', 'members', 'staff', 'team'])]

class Config:
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)
        
        self.schools = [School(**school) for school in data['schools']] 