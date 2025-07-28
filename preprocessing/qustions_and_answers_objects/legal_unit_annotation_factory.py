import re
from typing import List, cast

from preprocessing.legal_acts.legal_act_document import LegalActDocument
from preprocessing.pdf_structure.elements.article import Article
from preprocessing.pdf_structure.elements.legal_unit import LegalUnit
from preprocessing.pdf_structure.elements.section import Section
from preprocessing.pdf_structure.elements.subpoint import Subpoint
from preprocessing.qustions_and_answers_objects.legal_annotation_ids import LegalAnnotationIds
from preprocessing.qustions_and_answers_objects.legal_unit_annotation import LegalUnitAnnotation
from preprocessing.qustions_and_answers_objects.llm_legal_annotations import UnitReference


class LegalUnitIdRetrievingException(Exception):
    pass


class LegalUnitNotFoundException(Exception):
    pass


class InvalidStateOfReference(Exception):
    pass


class LegalUnitAnnotationFactory:

    def __init__(self, legal_act_document: LegalActDocument):
        self.legal_act_document = legal_act_document

    def _get_legal_part_unit(self, legal_units: List[LegalUnit], unit_ids: List[str]) -> List[LegalUnit]:
        # TODO #/document/16794311?unitId=zal(1) i don't handle it should be done
        selected_units: List[LegalUnit] = []
        if len(unit_ids) == 2:
            start_id, end_id = unit_ids
            collecting = False
            for legal_unit in legal_units:
                if legal_unit.unit_id == start_id:
                    collecting = True
                if collecting:
                    selected_units.append(legal_unit)
                if legal_unit.unit_id == end_id and collecting:
                    return selected_units
            if collecting:
                raise LegalUnitNotFoundException(
                    f"Start unit '{start_id}' found, but end unit '{end_id}' was not. document: {self.legal_act_document.legal_act_name}")
            else:
                raise LegalUnitNotFoundException(
                    f"Start unit '{start_id}' not found in legal_units. , document: {self.legal_act_document.legal_act_name}")
        elif len(unit_ids) == 1:
            unit_id = unit_ids[0]
            for legal_unit in legal_units:
                if legal_unit.unit_id == unit_id:
                    return [legal_unit]
            raise LegalUnitNotFoundException(
                f"There isn't legal_unit of id: {unit_id}, document: {self.legal_act_document.legal_act_name}")
        raise LegalUnitNotFoundException(f"Invalid state, {len(unit_ids)} len of legal units found")

    def get_legal_part_article(self, unit_ids: List[str]) -> List[Article]:
        return cast(List[Article], self._get_legal_part_unit(self.legal_act_document.articles, unit_ids))

    def get_legal_part_section(self, article: Article, section_unit_ids: List[str]) -> List[Section]:
        return cast(List[Section], self._get_legal_part_unit(article.legal_units_indeed, section_unit_ids))

    def get_legal_part_subpoints(self, parent_legal_unit: Article | Section, subpoints_unit_ids: List[str],
                                 reference: UnitReference) -> List[
        Subpoint]:
        if isinstance(parent_legal_unit, Section):
            return cast(List[Subpoint], self._get_legal_part_unit(parent_legal_unit.subpoints, subpoints_unit_ids))
        if isinstance(parent_legal_unit, Article):
            return cast(List[Subpoint],
                        self._get_legal_part_unit(parent_legal_unit.legal_units_indeed, subpoints_unit_ids))
        raise InvalidStateOfReference(f"""
        Probably, there is a problem on question side, given unit: 
        article: {self.get_ids_from_llm_reference(reference.artykul)},
         section_ids: {self.get_ids_from_llm_reference(reference.ustep)},
          subpoints_ids: {self.get_ids_from_llm_reference(reference.punkt)}
          """)

    @staticmethod
    def get_ids_from_llm_reference(unit_id_reference: str):
        if unit_id_reference:
            is_range = re.fullmatch(
                r'\s*(\d+\s*[a-zA-ZĄĆĘŁŃÓŚŹŻąćęłńóśźż⁰¹²³⁴⁵⁶⁷⁸⁹ᵃᵇᶜᵈᵉᶠᶢʰⁱʲᵏˡᵐⁿᵒᵖʳˢᵗᵘᵛʷˣʸᶻᵘⁿᵒʳᵐᴬᴮᶜᴰᴱᶠᴳᴴᴵᴶᴷᴸᴹᴺᴼᴾᵠᴿˢᵀᵁⱽᵂˣʸᶻᴸ]*)\s*[-–]\s*(\d+\s*[a-zA-ZĄĆĘŁŃÓŚŹŻąćęłńóśźż⁰¹²³⁴⁵⁶⁷⁸⁹ᵃᵇᶜᵈᵉᶠᶢʰⁱʲᵏˡᵐⁿᵒᵖʳˢᵗᵘᵛʷˣʸᶻᵘⁿᵒʳᵐᴬᴮᶜᴰᴱᶠᴳᴴᴵᴶᴷᴸᴹᴺᴼᴾᵠᴿˢᵀᵁⱽᵂˣʸᶻᴸ]*)',
                unit_id_reference.strip())
            is_not_range = re.fullmatch(
                r'(\d+\s*[a-zA-ZĄĆĘŁŃÓŚŹŻąćęłńóśźż⁰¹²³⁴⁵⁶⁷⁸⁹ᵃᵇᶜᵈᵉᶠᶢʰⁱʲᵏˡᵐⁿᵒᵖʳˢᵗᵘᵛʷˣʸᶻᵘⁿᵒʳᵐᴬᴮᶜᴰᴱᶠᴳᴴᴵᴶᴷᴸᴹᴺᴼᴾᵠᴿˢᵀᵁⱽᵂˣʸᶻᴸ]*)',
                unit_id_reference.strip())
            if is_range:
                start_unit = is_range.group(1).replace(" ", "")
                end_unit = is_range.group(2).replace(" ", "")
                return [start_unit, end_unit]
            elif is_not_range:
                return [unit_id_reference.replace(" ", "")]
            else:
                raise LegalUnitIdRetrievingException("Llm gave not correct unit ids")
        else:
            return []

    def get_legal_units_from_reference(self, reference: UnitReference) -> \
            List[LegalUnit]:
        if reference.artykul is None:
            raise RuntimeError(f"There is no reference article in {self.legal_act_document.legal_act_name}")
        article_ids = self.get_ids_from_llm_reference(reference.artykul)
        section_ids = self.get_ids_from_llm_reference(reference.ustep)
        subpoints_ids = self.get_ids_from_llm_reference(reference.punkt)
        t = sum(len(lst) > 1 for lst in [article_ids, section_ids, subpoints_ids]) > 1
        too_many_articles = len(article_ids) == 2 and len(section_ids) > 0 and len(subpoints_ids) > 0
        too_many_sections = len(article_ids) == 1 and len(section_ids) == 2 and len(subpoints_ids) > 0

        if too_many_articles or too_many_sections:
            raise LegalUnitIdRetrievingException(
                f"Invalid state of reference from LLM:\n"
                f"  article ids: {article_ids}\n"
                f"  section ids: {section_ids}\n"
                f"  subpoints ids: {subpoints_ids}"
            )
        articles = self.get_legal_part_article(article_ids)
        if len(articles) > 1 or (len(articles) == 1 and len(section_ids) == 0 and len(subpoints_ids) == 0):
            return articles
        if len(section_ids) > 0:
            sections = self.get_legal_part_section(articles[0], section_ids)
            if len(sections) > 1 or (len(sections) == 1 and len(subpoints_ids) == 0):
                return sections
            return self.get_legal_part_subpoints(sections[0], subpoints_ids, reference)
        return self.get_legal_part_subpoints(articles[0], subpoints_ids, reference)

    def create_legal_unit_annotation(self, reference: UnitReference):
        return LegalUnitAnnotation(
            legal_act_document=self.legal_act_document,
            units_ids=LegalAnnotationIds(
                article_ids=self.get_ids_from_llm_reference(reference.artykul),
                section_ids=self.get_ids_from_llm_reference(reference.ustep),
                subpoint_ids=self.get_ids_from_llm_reference(reference.punkt)
            ),
            legal_units=self.get_legal_units_from_reference(reference)
        )
