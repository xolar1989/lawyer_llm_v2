from dataclasses import dataclass
from typing import List, Mapping, Any

from preprocessing.chunks.chunk import Chunk, AbstractChunks


@dataclass
class SubpointChunks(AbstractChunks):
    article_id: str
    section_id: str | None
    subpoint_id: str

    def to_dict(self) -> dict:
        return {
            "article_id": self.article_id,
            "section_id": self.section_id,
            "subpoint_id": self.subpoint_id,
            "parts": [part.to_dict() for part in self.parts],
            "ELI": self.ELI,
            "invoke_id": self.invoke_id,
            "model_id": self.model_id
        }

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]) -> 'SubpointChunks':
        return cls(
            article_id=dict_object["article_id"],
            section_id=dict_object.get("section_id"),
            subpoint_id=dict_object["subpoint_id"],
            parts=[Chunk.from_dict(p) for p in dict_object.get("parts", [])],
            ELI=dict_object["ELI"],
            invoke_id=dict_object["invoke_id"],
            model_id=dict_object["model_id"]
        )