from typing import Set, Tuple

from preprocessing.chunk_matching.matching_strategy import RelevantLegalUnit
from preprocessing.chunks.chunk import AbstractChunks, Chunk
from preprocessing.chunks.section_chunks import SectionChunks
from preprocessing.pdf_structure.elements.section import Section


class ExactSectionMatchStrategy:

    def match(
            self,
            chunks: AbstractChunks,
            exact_chunk: Chunk,
            legal_units_of_annotation: Set[Tuple[str, str, str | None, str | None]]
    ) -> bool:
        if not isinstance(chunks, SectionChunks):
            return False
        else:
            return (chunks.ELI, chunks.article_id, chunks.section_id, None) == next(iter(legal_units_of_annotation))

    def get_legal_units_of_annotation(self, annotation: RelevantLegalUnit) -> Set[Tuple[str, str, str | None, str | None]]:
        relevant_units_for_annotation = set()
        if isinstance(annotation.legal_unit, Section):
            relevant_units_for_annotation.add((annotation.ELI, annotation.unit_id, annotation.section_id, None))
        return relevant_units_for_annotation
