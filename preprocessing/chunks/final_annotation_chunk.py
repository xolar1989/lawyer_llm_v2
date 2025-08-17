from dataclasses import dataclass, asdict
from typing import Optional, Mapping, Any

from preprocessing.chunks.article_chunks import ArticleChunks
from preprocessing.chunks.chunk import AbstractChunks, Chunk
from preprocessing.chunks.section_chunks import SectionChunks
from preprocessing.chunks.subpoint_chunks import SubpointChunks
from preprocessing.mongo_db.mongodb import MongodbObject


@dataclass(eq=False)
class FinalAnnotationChunk(MongodbObject):


    chunk_id: str
    ELI: str
    article_id: str
    section_id: Optional[str]
    subpoint_id: Optional[str]
    text: str

    def to_dict(self) -> Mapping[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]) -> "FinalAnnotationChunk":
        return cls(**dict_object)

    def __eq__(self, other):
        if not isinstance(other, FinalAnnotationChunk):
            return NotImplemented
        return (
                self.ELI == other.ELI and
                self.article_id == other.article_id and
                self.section_id == other.section_id and
                self.subpoint_id == other.subpoint_id and
                self.text == other.text
        )

    def __hash__(self):
        return hash((
            self.ELI,
            self.article_id,
            self.section_id,
            self.subpoint_id,
            self.text
        ))

    @classmethod
    def build_id(cls, chunks: AbstractChunks, part: int, is_span: bool):
        if isinstance(chunks, ArticleChunks):
            chunk_id = f"{chunks.ELI}_art{chunks.unit_id}"
            return f"{chunk_id}_span_{part}" if is_span else f"{chunk_id}_{part}"
        elif isinstance(chunks, SectionChunks):
            return f"{chunks.ELI}_art{chunks.article_id}_section{chunks.section_id}_{part}"
        elif isinstance(chunks, SubpointChunks):
            return f"{chunks.ELI}_art{chunks.article_id}_section{chunks.section_id}_point{chunks.subpoint_id}_{part}"


    @classmethod
    def from_chunk(cls, chunks: AbstractChunks, exact_chunk: Chunk, part: int, is_span: bool):
        chunk_id = cls.build_id(chunks, part, is_span)

        if isinstance(chunks, ArticleChunks):
            return cls(
                chunk_id=chunk_id,
                ELI=chunks.ELI,
                article_id=chunks.unit_id,
                section_id=None,
                subpoint_id=None,
                text=exact_chunk.text
            )
        elif isinstance(chunks, SectionChunks):
            return cls(
                chunk_id=chunk_id,
                ELI=chunks.ELI,
                article_id=chunks.article_id,
                section_id=chunks.section_id,
                subpoint_id=None,
                text=exact_chunk.text
            )
        elif isinstance(chunks, SubpointChunks):
            return cls(
                chunk_id=chunk_id,
                ELI=chunks.ELI,
                article_id=chunks.article_id,
                section_id=chunks.section_id,
                subpoint_id=chunks.subpoint_id,
                text=exact_chunk.text
            )
