from typing import Set, Tuple

from preprocessing.chunk_matching.matching_strategy import RelevantLegalUnit
from preprocessing.chunks.chunk import AbstractChunks, Chunk
from preprocessing.chunks.subpoint_chunks import SubpointChunks
from preprocessing.pdf_structure.elements.subpoint import Subpoint


class ExactPointMatchStrategy:

    def match(
            self,
            chunks: AbstractChunks,
            exact_chunk: Chunk,
            legal_units_of_annotation: Set[Tuple[str, str, str | None, str | None]]
    ) -> bool:
        if not isinstance(chunks, SubpointChunks):
            return False
        else:
            return (chunks.ELI, chunks.article_id, chunks.section_id, chunks.subpoint_id) == next(iter(legal_units_of_annotation))

    def get_legal_units_of_annotation(self, annotation: RelevantLegalUnit) -> Set[Tuple[str, str, str | None, str | None]]:
        relevant_units_for_annotation = set()
        if isinstance(annotation.legal_unit, Subpoint):
            relevant_units_for_annotation.add((annotation.ELI, annotation.unit_id, annotation.section_id, annotation.subpoint_id))
        return relevant_units_for_annotation