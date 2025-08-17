from itertools import chain
from typing import Set, List
from preprocessing.chunk_matching.matching_strategy import RelevantLegalUnit, MatchingStrategy
from preprocessing.chunks.chunk import AbstractChunks, Chunk
from preprocessing.chunks.final_annotation_chunk import FinalAnnotationChunk
from preprocessing.pdf_structure.elements.article import Article
from preprocessing.pdf_structure.elements.section import Section
from preprocessing.pdf_structure.elements.subpoint import Subpoint
from preprocessing.qustions_and_answers_objects.legal_annotation_ids import LegalAnnotationIds
from preprocessing.qustions_and_answers_objects.legal_question_with_annotations import LegalQuestionWithAnnotations
from preprocessing.qustions_and_answers_objects.legal_rephrased_question import LegalRephrasedQuestion


class ChunkFinder:
    def __init__(self, strategy: MatchingStrategy):
        self.strategy = strategy

    @staticmethod
    def have_legal_units_range(ids: List[str]):
        return len(ids) == 2

    @staticmethod
    def is_article_unit(ids: LegalAnnotationIds):
        return len(ids.article_ids) == 1 and len(ids.section_ids) == 0 and len(ids.subpoint_ids) == 0

    @staticmethod
    def is_section_unit(ids: LegalAnnotationIds):
        return len(ids.article_ids) == 1 and len(ids.section_ids) == 1 and len(ids.subpoint_ids) == 0

    def get_relevant_unit_ids_for_article_span_split(self, legal_question: LegalRephrasedQuestion) -> Set[
        RelevantLegalUnit]:
        relevant_legal_units: Set[RelevantLegalUnit] = set()
        for answer_or_question_annotation in chain(
                legal_question.question_annotations,
                legal_question.answer_annotations):

            if self.have_legal_units_range(answer_or_question_annotation.units_ids.article_ids) or \
                    self.is_article_unit(answer_or_question_annotation.units_ids):
                for legal_unit in answer_or_question_annotation.legal_units:
                    if not isinstance(legal_unit, Article):
                        raise ValueError(f"Invalid state: {legal_unit} should be Article")
                    relevant_legal_units.add(
                        RelevantLegalUnit(answer_or_question_annotation.legal_act_document.ELI, legal_unit.unit_id,
                                          None, None,
                                          legal_unit)
                    )
            else:
                if self.have_legal_units_range(answer_or_question_annotation.units_ids.section_ids) or \
                        self.is_section_unit(answer_or_question_annotation.units_ids):
                    article_id = answer_or_question_annotation.units_ids.article_ids[0]
                    for legal_unit in answer_or_question_annotation.legal_units:
                        if not isinstance(legal_unit, Section):
                            raise ValueError(f"Invalid state: {legal_unit} should be Section")

                        relevant_legal_units.add(
                            RelevantLegalUnit(answer_or_question_annotation.legal_act_document.ELI, article_id,
                                              legal_unit.unit_id,
                                              None, legal_unit)
                        )
                else:
                    article_id = answer_or_question_annotation.units_ids.article_ids[0]
                    ## TODO error here

                    section_id = answer_or_question_annotation.units_ids.section_ids[0] if len(answer_or_question_annotation.units_ids.section_ids) > 0 else None
                    for legal_unit in answer_or_question_annotation.legal_units:
                        if not isinstance(legal_unit, Subpoint):
                            raise ValueError(f"Invalid state: {legal_unit} should be Subpoint")
                        relevant_legal_units.add(
                            RelevantLegalUnit(answer_or_question_annotation.legal_act_document.ELI, article_id, section_id,
                                              legal_unit.unit_id, legal_unit)
                        )

        return relevant_legal_units


    def get_chunks(self, legal_question: LegalRephrasedQuestion, chunks_list: List[AbstractChunks],
                   is_span: bool = False) -> Set[FinalAnnotationChunk]:
        relevant_chunks_for_question: Set[FinalAnnotationChunk] = set()
        relevant_annotations = self.get_relevant_unit_ids_for_article_span_split(legal_question)

        for annotation in relevant_annotations:
            relevant_units_for_annotation = self.strategy.get_legal_units_of_annotation(annotation)
            if len(relevant_units_for_annotation) > 0:
                for span_chunks in chunks_list:
                    for part, chunk in enumerate(span_chunks.parts, start=1):
                        if self.strategy.match(span_chunks, chunk, relevant_units_for_annotation):
                            relevant_chunks_for_question.add(
                                FinalAnnotationChunk.from_chunk(span_chunks, chunk, part, is_span)
                            )

        return relevant_chunks_for_question
