from dataclasses import dataclass
from typing import List
import yaml

@dataclass
class School:
    name: str
    code: str
    rank: int
    department: str
    url: str
    tier: int

class Config:
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)
        
        self.schools = [School(**school) for school in data['schools']] 