from dataclasses import dataclass
from typing import List


@dataclass
class LegalAnnotationIds:
    article_ids: List[int]
    section_ids: List[int]
    subpoint_ids: List[int]
