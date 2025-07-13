from typing import List, Mapping, Any

from preprocessing.pdf_structure.elements.legal_unit import LegalUnit
from preprocessing.pdf_structure.elements.metadata import LegalPartInfo, ArticleMetadata, ChapterInfo
from preprocessing.pdf_structure.elements.section import Section
from preprocessing.pdf_structure.elements.subpoint import Subpoint
from preprocessing.pdf_structure.splits.article_split import ArticleSplit
from preprocessing.pdf_structure.splits.chapter_split import ChapterSplit
from preprocessing.pdf_structure.splits.part_legal_unit_split import PartLegalUnitSplit
from preprocessing.pdf_structure.splits.point_split import PointSplit
from preprocessing.pdf_structure.splits.section_split import SectionSplit
from preprocessing.utils.stages_objects import GeneralInfo


class Article(LegalUnit):

    def __init__(self, unit_id: str, text: str, legal_units_indeed: List[LegalUnit], metadata: ArticleMetadata):
        super().__init__(unit_id, text)
        self.metadata = metadata
        self.legal_units_indeed = legal_units_indeed

    @classmethod
    def build(cls, article_split: ArticleSplit, part_unit_split: PartLegalUnitSplit,
              chapter_split: ChapterSplit, general_info: GeneralInfo, invoke_id: str):
        legal_part_info = None if part_unit_split.is_hidden else LegalPartInfo(
            part_number=part_unit_split.id_unit,
            title=part_unit_split.title
        )

        chapter_info = None if chapter_split.is_hidden else ChapterInfo(
            chapter_number=chapter_split.id_unit,
            title=chapter_split.title
        )
        text_split = article_split.split_item_for_further_processing()
        text = cls.get_current_text(text_split) if text_split else ''
        metadata = ArticleMetadata(
            ELI=general_info.ELI,
            legal_act_name=general_info.title,
            invoke_id=invoke_id,
            legal_part_info=legal_part_info,
            chapter_info=chapter_info
        )
        legal_units_indeed: List[LegalUnit] = []
        for legal_unit_split in article_split.legal_units_indeed:
            if not legal_unit_split.is_current_unit:
                continue
            if isinstance(legal_unit_split, SectionSplit):
                section = Section.build(legal_unit_split, article_split.id_unit)
                legal_units_indeed.append(section)
            elif isinstance(legal_unit_split, PointSplit):
                subpoint = Subpoint.build(legal_unit_split)
                legal_units_indeed.append(subpoint)
        sections = [legal_unit for legal_unit in legal_units_indeed if isinstance(legal_unit, Section)]
        subpoints = [legal_unit for legal_unit in legal_units_indeed if isinstance(legal_unit, Subpoint)]
        if not cls.is_ascending(sections):
            raise RuntimeError(f"The ids of sections are not in ascending order {[item.unit_id for item in sections]},"
                               f" the article id: {article_split.id_unit}")
        if not cls.is_ascending(subpoints):
            raise RuntimeError(f"The ids of subpoints are not in ascending order {[item.unit_id for item in subpoints]},"
                               f" the article : {article_split.id_unit}, subpoints are inside article, section ommited")

        return Article(
            unit_id=article_split.id_unit,
            text=text,
            metadata=metadata,
            legal_units_indeed=legal_units_indeed
        )

    def to_dict(self):
        return {
            'unit_id':self.unit_id,
            'text': self.text,
            'legal_units_indeed': [legal_unit.to_dict() for legal_unit in self.legal_units_indeed],
            'metadata': self.metadata.to_dict(),
            'type': str(Article.__name__)
        }

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        return Article(
            unit_id=dict_object['unit_id'],
            text=dict_object['text'],
            metadata=ArticleMetadata.from_dict(dict_object['metadata']),
            legal_units_indeed=[LegalUnit.from_dict(legal_unit_dict) for legal_unit_dict in dict_object['legal_units_indeed']]
        )