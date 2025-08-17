from typing import List

from transformers import PreTrainedTokenizerBase

from preprocessing.chunks.abstract_chunker import AbstractChunker
from preprocessing.chunks.article_chunks import ArticleChunks
from preprocessing.chunks.chunk import Chunk
from preprocessing.chunks.section_chunker import SectionChunker
from preprocessing.chunks.subpoint_chunker import SubpointChunker
from preprocessing.pdf_structure.elements.article import Article
from preprocessing.pdf_structure.elements.section import Section
from preprocessing.pdf_structure.elements.subpoint import Subpoint
from preprocessing.qustions_and_answers_objects.legal_annotation_ids import LegalAnnotationIds


class ArticleChunker(AbstractChunker):

    def __init__(self, tokenizer: PreTrainedTokenizerBase, max_token_length: int, overlap: int,
                 special_token: str,
                 point_chunker: SubpointChunker, section_chunker: SectionChunker,
                 ELI: str, invoke_id: str, model_id: str
                 ):
        super().__init__(tokenizer, max_token_length, overlap, special_token, ELI, invoke_id, model_id)
        self.point_chunker = point_chunker
        self.section_chunker = section_chunker

    def chunk_unit_for_chunking(self, **kwargs):
        pass

    def get_prefix(self, article: Article, exact_unit: bool):
        if exact_unit:
            return f"{self.special_token} {self.unit_prefix(article)}"
        else:
            return self.unit_prefix(article)

    def chunk_unit(self, article: Article, exact_unit: bool) -> ArticleChunks:
        article_chunk_parts: List[Chunk] = []
        is_article_text_used = False

        current_build_by_units: List[LegalAnnotationIds] = []

        prefix = self.get_prefix(article, exact_unit)

        article_chunks_text = [Chunk(f"{prefix} {chunk_text}",
                                     [LegalAnnotationIds([article.unit_id], [], [])])
                               for chunk_text in
                               list(self.chunk_text(self.normalize_text(article.text),
                                                    window=self.max_token_length - self.calc_tokens(f"{prefix} "),
                                                    overlap=self.overlap
                                                    )
                                    )
                               ]
        if len(article_chunks_text) > 1:
            article_chunk_parts.extend(article_chunks_text)
            is_article_text_used = True
            current_article_part_chunk_text = f"{self.get_part_of_article_text(article, int(self.overlap / 2))}"
        else:
            current_article_part_chunk_text = f"{self.normalize_text(article.text)}"

        for legal_unit in article.legal_units_indeed:
            if isinstance(legal_unit, Subpoint):
                current_article_part_chunk_text, current_build_by_units, article_chunk_parts, is_article_text_used = \
                    self.point_chunker.chunk_unit_for_chunking(
                        current_chunk_text=current_article_part_chunk_text,
                        current_build_by_units=current_build_by_units,
                        chunk_parts=article_chunk_parts,
                        subpoint=legal_unit,
                        section=None,
                        article=article,
                        is_root_used=is_article_text_used,
                        prefix=prefix
                    )
                w = 4
            elif isinstance(legal_unit, Section):
                current_article_part_chunk_text, current_build_by_units, article_chunk_parts, is_article_text_used = \
                    self.section_chunker.chunk_unit_for_chunking(
                        current_chunk_text=current_article_part_chunk_text,
                        current_build_by_units=current_build_by_units,
                        chunk_parts=article_chunk_parts,
                        section=legal_unit,
                        article=article,
                        is_root_used=is_article_text_used,
                        prefix=prefix
                    )
        if len(article.legal_units_indeed) == 0:
            current_build_by_units = [LegalAnnotationIds([article.unit_id], [], [])]
        if len(current_build_by_units) > 0:
            article_chunk_parts.append(
                Chunk(f"{prefix} " + current_article_part_chunk_text, current_build_by_units)
            )
        article_with_chunks = ArticleChunks(
            parts=article_chunk_parts,
            unit_id=article.unit_id,
            ELI=self.ELI,
            invoke_id=self.invoke_id,
            model_id=self.model_id
        )
        return article_with_chunks

