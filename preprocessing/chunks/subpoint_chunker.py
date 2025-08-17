from typing import List

from preprocessing.chunks.abstract_chunker import AbstractChunker
from preprocessing.chunks.chunk import Chunk
from preprocessing.chunks.subpoint_chunks import SubpointChunks
from preprocessing.pdf_structure.elements.article import Article
from preprocessing.pdf_structure.elements.section import Section
from preprocessing.pdf_structure.elements.subpoint import Subpoint
from preprocessing.qustions_and_answers_objects.legal_annotation_ids import LegalAnnotationIds


class SubpointChunker(AbstractChunker):

    def chunk_unit(self, point: Subpoint, section: Section | None, article: Article) -> SubpointChunks:
        article_part = self.normalize_text(self.get_text_of(f'{self.unit_prefix(article)} {article.text}', int(self.overlap / 2) - 10))

        section_part = self.normalize_text(self.get_text_of(f'{self.unit_prefix(section)} {section.text}',
                                        int(self.overlap / 2) - 10)) + " " if section else ""
        prefix = f"{article_part} {section_part}{self.special_token} {self.unit_prefix(point)} "

        legal_annotation_ids = LegalAnnotationIds(
            [article.unit_id],
            [section.unit_id] if section else [],
            [point.unit_id]
        )
        chunks: List[Chunk] = []
        for _slice in self.chunk_text(
                text=point.text,
                window=self.max_token_length - self.calc_tokens(prefix),
                overlap=self.overlap
        ):
            chunk_text = prefix + _slice
            chunks.append(Chunk(chunk_text, [legal_annotation_ids]))
        return SubpointChunks(
            parts=chunks,
            article_id=article.unit_id,
            section_id=section.unit_id if section else None,
            subpoint_id=point.unit_id,
            ELI=self.ELI,
            invoke_id=self.invoke_id,
            model_id=self.model_id
        )

    def chunk_unit_for_chunking(self, current_chunk_text: str, current_build_by_units: List[LegalAnnotationIds],
                                chunk_parts: List[Chunk], subpoint: Subpoint,
                                article: Article, section: Section | None,
                                is_root_used: bool, prefix: str):
        legal_annotation_ids = LegalAnnotationIds([article.unit_id], [section.unit_id],
                                                  [subpoint.unit_id]) if section else \
            LegalAnnotationIds([article.unit_id], [], [subpoint.unit_id])

        if self.is_within_window_size(
                f"{prefix} {current_chunk_text} {self.unit_prefix(subpoint)} {self.normalize_text(subpoint.text)} "
        ):
            current_chunk_text += f"{self.unit_prefix(subpoint)} {self.normalize_text(subpoint.text)} "
            is_root_used = True
            current_build_by_units.append(legal_annotation_ids)
        else:
            if is_root_used:
                chunk_parts.append(
                    Chunk(f"{prefix} {current_chunk_text}".strip(), current_build_by_units)
                )
                current_chunk_text: str = self.get_part_of_article_text(article,
                                                                        int(self.overlap / 2)) + " "
                if section:
                    current_chunk_text += self.get_part_of_section_text(section, int(self.overlap / 2)) + " "
                current_build_by_units: List[LegalAnnotationIds] = []
            is_root_used = True
            if self.is_within_window_size(
                    f"{prefix} {current_chunk_text} {self.unit_prefix(subpoint)} {self.normalize_text(subpoint.text)} "
            ):
                current_chunk_text += f"{self.unit_prefix(subpoint)} {self.normalize_text(subpoint.text)} "
                current_build_by_units.append(legal_annotation_ids)
            else:
                ## TODO it works correctly
                prefix_text = self.get_text_of(self.normalize_text(
                    f'{prefix} {article.text}'),
                    int(self.overlap / 2) - 5) + " "
                if section:
                    prefix_text += self.get_text_of(self.normalize_text(
                        f'{self.unit_prefix(section)} {section.text}'), int(self.overlap / 2) - 5) + " "
                prefix_text += self.unit_prefix(subpoint) + " "
                subpoint_chunks_text = [
                    chunk_text
                    for chunk_text in self.chunk_text(
                        self.normalize_text(
                            prefix + f" {current_chunk_text} " +
                            self.unit_prefix(subpoint) + " " + subpoint.text
                        ),
                        window=self.max_token_length - self.calc_tokens(prefix_text),
                        overlap=self.overlap,
                    )
                ]
                chunk_parts.append(Chunk(subpoint_chunks_text[0], [legal_annotation_ids]))
                chunk_parts.extend(list(map(lambda point_text_chunk:
                                            Chunk(prefix_text + point_text_chunk, [legal_annotation_ids]),
                                            subpoint_chunks_text[1:len(subpoint_chunks_text) - 1]))
                                   )
                prefix_text = f"{self.get_text_of(self.normalize_text(article.text), int(self.overlap / 2) - 5)} "
                if section:
                    prefix_text += f"{self.get_part_of_section_text(section, int(self.overlap / 2) - 5)} "
                prefix_text += f"{self.unit_prefix(subpoint)} "
                current_chunk_text = prefix_text + subpoint_chunks_text[len(subpoint_chunks_text) - 1]
                current_build_by_units.append(legal_annotation_ids)

        return current_chunk_text, current_build_by_units, chunk_parts, is_root_used
