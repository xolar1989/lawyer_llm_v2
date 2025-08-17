from typing import Set, Tuple, cast

from preprocessing.chunk_matching.matching_strategy import RelevantLegalUnit
from preprocessing.chunks.article_chunks import ArticleChunks
from preprocessing.chunks.chunk import AbstractChunks, Chunk
from preprocessing.pdf_structure.elements.article import Article


class ExactArticleMatchStrategy:

    def match(
            self,
            chunks: AbstractChunks,
            exact_chunk: Chunk,
            legal_units_of_annotation: Set[Tuple[str, str, str | None, str | None]]
    ) -> bool:
        if not isinstance(chunks, ArticleChunks):
            return False
        else:
            return (chunks.ELI, chunks.unit_id, None, None) == next(iter(legal_units_of_annotation))

    def get_legal_units_of_annotation(self, annotation: RelevantLegalUnit) -> Set[Tuple[str, str, str | None, str | None]]:
        relevant_units_for_annotation = set()
        if isinstance(annotation.legal_unit, Article):
            relevant_units_for_annotation.add((annotation.ELI, annotation.unit_id, None, None))
        return relevant_units_for_annotation
