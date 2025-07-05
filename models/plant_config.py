from dataclasses import dataclass
from typing import List, Dict

@dataclass
class PlantConfig:
    project_id: str
    dataset_id: str
    location: str = "US"

@dataclass
class OnboardingPlan:
    source_plant: PlantConfig
    target_plant: PlantConfig
    tables_to_migrate: List[str]
    views_to_migrate: List[str]
    table_mapping: Dict[str, str]
    view_mapping: Dict[str, str]
    dry_run: bool = True
