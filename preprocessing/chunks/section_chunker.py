from itertools import chain
from typing import List

from transformers import PreTrainedTokenizerBase

from preprocessing.chunks.abstract_chunker import AbstractChunker
from preprocessing.chunks.chunk import Chunk
from preprocessing.chunks.section_chunks import SectionChunks
from preprocessing.chunks.subpoint_chunker import SubpointChunker
from preprocessing.pdf_structure.elements.article import Article
from preprocessing.pdf_structure.elements.section import Section
from preprocessing.qustions_and_answers_objects.legal_annotation_ids import LegalAnnotationIds


class SectionChunker(AbstractChunker):
    def __init__(self, tokenizer: PreTrainedTokenizerBase, max_token_length: int, overlap: int,
                 point_chunker: SubpointChunker, special_token: str,
                    ELI: str, invoke_id: str, model_id: str
                 ):
        super().__init__(tokenizer, max_token_length, overlap, special_token, ELI, invoke_id, model_id)
        self.point_chunker = point_chunker

    def build_section_text(self, section: Section):
        parts = chain(
            [section.text],                   # main § text
            (sp.text for sp in section.subpoints)   # any pkt / lit points
        )

        return " ".join(p.strip() for p in parts if p).strip()

    def chunk_unit(self, section: Section, article: Article) -> SectionChunks:
        # TODO think about changing this logic?
        section_text = self.normalize_text(self.build_section_text(section))

        prefix = f"{self.get_text_of(f'{self.unit_prefix(article)} {article.text}',int(self.overlap / 2) - 10)} {self.special_token} {self.unit_prefix(section)} "
        legal_annotation_ids = LegalAnnotationIds([article.unit_id], [section.unit_id], [])
        chunks: List[Chunk] = []
        for _slice in self.chunk_text(
                text=section_text,
                window=self.max_token_length - self.calc_tokens(prefix),
                overlap=self.overlap
            ):
            chunk_text = prefix + _slice
            chunks.append(Chunk(chunk_text, [legal_annotation_ids]))
        return SectionChunks(
            parts=chunks,
            article_id=article.unit_id,
            section_id=section.unit_id,
            ELI=self.ELI,
            invoke_id=self.invoke_id,
            model_id=self.model_id
        )

    def _parse_section_text(self, current_chunk_text: str, chunk_parts: List[Chunk],
                            section: Section, article: Article, prefix: str):
        legal_annotation_ids = LegalAnnotationIds([article.unit_id], [section.unit_id], [])
        # is not used article the current_article_part_chunk_text -> the whole text of article not cut
        # is used article current_article_part_chunk_text -> article cut
        prefix_text = f"{self.get_text_of(f'{prefix} {current_chunk_text}',int(self.overlap / 2) - 5)} {self.unit_prefix(section)} "
        section_chunks_text = [
            chunk
            for chunk in self.chunk_text(
                (
                    f"{prefix} "
                    f"{current_chunk_text} "
                    f"{self.unit_prefix(section)} "
                    f"{self.normalize_text(section.text)}"
                ),
                window=self.max_token_length - self.calc_tokens(prefix_text),
                overlap=self.overlap,
            )
        ]
        chunk_parts.append(Chunk(section_chunks_text[0], [legal_annotation_ids]))
        chunk_parts.extend(
            list(map(lambda point_text_chunk:
                     Chunk(prefix_text + point_text_chunk, [legal_annotation_ids]),
                     section_chunks_text[1:len(section_chunks_text) - 1]
                     )
                 )
        )
        prefix_text = self.get_text_of(
            self.normalize_text(article.text), int(self.overlap / 2)) + f" {self.unit_prefix(section)} "
        current_chunk_text = prefix_text + section_chunks_text[len(section_chunks_text) - 1]
        return current_chunk_text, chunk_parts

    def chunk_unit_for_chunking(self, current_chunk_text: str, current_build_by_units: List[LegalAnnotationIds],
                   chunk_parts: List[Chunk], section: Section, article: Article, is_root_used: bool, prefix: str):
        legal_annotation_ids = LegalAnnotationIds([article.unit_id], [section.unit_id], [])
        if self.is_within_window_size(
                f"{prefix} {current_chunk_text} {self.unit_prefix(section)} {self.normalize_text(section.text)} "):
            is_root_used = True
            current_chunk_text += f"{self.unit_prefix(section)} {self.normalize_text(section.text)} "
        elif is_root_used:
            chunk_parts.append(Chunk(f"{prefix} {current_chunk_text}".strip(), current_build_by_units))
            current_chunk_text: str = self.get_part_of_article_text(article, int(self.overlap / 2)) + " "
            current_build_by_units: List[LegalAnnotationIds] = []
            if self.is_within_window_size(
                    f"{prefix} {current_chunk_text} {self.unit_prefix(section)} {self.normalize_text(section.text)} "):
                current_chunk_text += f"{self.unit_prefix(section)} {self.normalize_text(section.text)} "
                r = 4
            else:
                current_chunk_text, chunk_parts = self._parse_section_text(
                    current_chunk_text=current_chunk_text,
                    chunk_parts=chunk_parts,
                    section=section,
                    article=article,
                    prefix=prefix
                )
        else:
            is_root_used = True
            current_chunk_text, chunk_parts = self._parse_section_text(
                current_chunk_text=current_chunk_text,
                chunk_parts=chunk_parts,
                section=section,
                article=article,
                prefix=prefix
            )
        if len(section.subpoints) == 0:
            current_build_by_units.append(legal_annotation_ids)

        with_section_part_used = False
        for subpoint in section.subpoints:
            current_chunk_text, current_build_by_units, chunk_parts, with_section_part_used = \
                self.point_chunker.chunk_unit_for_chunking(
                    current_chunk_text=current_chunk_text,
                    current_build_by_units=current_build_by_units,
                    chunk_parts=chunk_parts,
                    subpoint=subpoint,
                    section=section,
                    article=article,
                    is_root_used=with_section_part_used,
                    prefix=prefix
                )
        return current_chunk_text, current_build_by_units, chunk_parts, is_root_used

