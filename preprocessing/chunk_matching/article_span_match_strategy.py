from typing import Set, Tuple, List

from preprocessing.chunk_matching.matching_strategy import RelevantLegalUnit
from preprocessing.chunks.chunk import AbstractChunks, Chunk
from preprocessing.pdf_structure.elements.article import Article
from preprocessing.pdf_structure.elements.section import Section
from preprocessing.pdf_structure.elements.subpoint import Subpoint
from preprocessing.qustions_and_answers_objects.legal_annotation_ids import LegalAnnotationIds
from preprocessing.qustions_and_answers_objects.legal_question_with_annotations import LegalQuestionWithAnnotations


class ArticleSpanMatchStrategy:

    @staticmethod
    def get_ids_of_annotation(legal_annotation_ids: LegalAnnotationIds):
        article_id = legal_annotation_ids.article_ids[0]
        section_id = legal_annotation_ids.section_ids[0] if len(legal_annotation_ids.section_ids) == 1 else None
        subpoint_id = legal_annotation_ids.subpoint_ids[0] if len(legal_annotation_ids.subpoint_ids) == 1 else None
        return article_id, section_id, subpoint_id

    def get_legal_units_from_chunk(self, legal_unit_chunks: AbstractChunks, chunk: Chunk) -> Set[
        Tuple[str, str, str | None, str | None]]:
        relevant_legal_units: Set[Tuple[str, str, str | None, str | None]] = set()
        for legal_annotation_ids in chunk.build_by:
            article_id, section_id, subpoint_id = self.get_ids_of_annotation(legal_annotation_ids)
            relevant_legal_units.add((legal_unit_chunks.ELI, article_id, section_id, subpoint_id))
        return relevant_legal_units

    @staticmethod
    def explode_legal_units_for_annotation(relevant_legal_unit: RelevantLegalUnit) -> \
            Set[Tuple[str, str, str | None, str | None]]:
        legal_units: Set[Tuple[str, str, str | None, str | None]] = set()
        if isinstance(relevant_legal_unit.legal_unit, Article):
            article_id = relevant_legal_unit.legal_unit.unit_id
            if len(relevant_legal_unit.legal_unit.legal_units_indeed) == 0:
                legal_units.add((relevant_legal_unit.ELI, article_id, None, None))
            else:
                for nested_legal_unit in relevant_legal_unit.legal_unit.legal_units_indeed:
                    if isinstance(nested_legal_unit, Subpoint):
                        legal_units.add((relevant_legal_unit.ELI, article_id, None, nested_legal_unit.unit_id))
                    elif isinstance(nested_legal_unit, Section):
                        section_id = nested_legal_unit.unit_id
                        if len(nested_legal_unit.subpoints) == 0:
                            legal_units.add((relevant_legal_unit.ELI, article_id, section_id, None))
                        else:
                            for subpoint in nested_legal_unit.subpoints:
                                legal_units.add((relevant_legal_unit.ELI, article_id, section_id, subpoint.unit_id))
        elif isinstance(relevant_legal_unit.legal_unit, Section):
            article_id = relevant_legal_unit.unit_id
            section_id = relevant_legal_unit.legal_unit.unit_id
            if len(relevant_legal_unit.legal_unit.subpoints) == 0:
                legal_units.add((relevant_legal_unit.ELI, article_id, section_id, None))
            else:
                for subpoint in relevant_legal_unit.legal_unit.subpoints:
                    legal_units.add((relevant_legal_unit.ELI, article_id, section_id, subpoint.unit_id))
        else:
            article_id, section_id, subpoint_id = relevant_legal_unit.get_ids()
            legal_units.add((relevant_legal_unit.ELI, article_id, section_id, subpoint_id))

        return legal_units


    def match(
            self,
            chunks: AbstractChunks,
            exact_chunk: Chunk,
            legal_units_of_annotation: Set[Tuple[str, str, str | None, str | None]]
    ) -> bool:
        chunk_legal_units_build_by = self.get_legal_units_from_chunk(chunks, exact_chunk)
        return bool(legal_units_of_annotation.intersection(chunk_legal_units_build_by))

    def get_legal_units_of_annotation(self, annotation: RelevantLegalUnit) -> Set[Tuple[str, str, str | None, str | None]]:
        return self.explode_legal_units_for_annotation(annotation)