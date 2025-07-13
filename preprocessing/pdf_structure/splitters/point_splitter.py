import re
from typing import List

from preprocessing.pdf_structure.splits.article_split import ArticleSplit
from preprocessing.pdf_structure.splits.point_split import PointSplit
from preprocessing.pdf_structure.splits.section_split import SectionSplit
from preprocessing.pdf_structure.splitters.abstract_document_splitter import AbstractDocumentSplitter, SplitMatch


class PointSplitter(AbstractDocumentSplitter):

    def __init__(self):
        super().__init__()

    def before_upcoming_change_pattern(self):
        return r"\[\s*\d+[a-zA-Z]?\)[^\[\]<>]*\]"

    def upcoming_change_pattern(self):
        return r"<\s*\d+[a-zA-Z]?\)[^<>\[\]]*>"

    def split_function(self, text) -> List[SplitMatch]:
        first_pattern = re.compile(
            r"(?<![-–])(?<!pkt\s)(?<!poz\.\s)(?<!ust\.\s)(?<!art\.\s)^1[a-z]{0,2}\)\s[\s\S]*?(?=(?<![-–])(?<!pkt\s)(?<!ust\.\s)(?<!art\.\s)\d{1,2}[a-z]?\)\s|\Z)",
            re.MULTILINE
        )

        matches = []
        first_match = first_pattern.search(text)
        if not first_match:
            return matches

        rest_text = text[first_match.start():]
        after_offset_results = re.finditer(
            r'(?<![-–])(?<!pkt\s)(?<!poz\.\s)(?<!ust\.\s)(?<!art\.\s)^(\d+[a-z]?\)\s[\s\S]*?)(?=(?<![-–])(?<!pkt\s)(?<!ust\.\s)(?<!art\.\s)\d+[a-z]?\)\s|\Z)',
            rest_text,
            flags=re.DOTALL | re.MULTILINE
        )
        for match in after_offset_results:
            start_index = first_match.start() + match.start()
            end_index = first_match.start() + match.end()
            matches.append(SplitMatch(start_index, end_index, match.group()))
        return matches

    def split(self, art_split: ArticleSplit):

        prev_split_of_art = art_split.split_item_for_further_processing()
        if prev_split_of_art:

            splits_of_arts_part_upcoming_changes = self.split_by_upcoming_changes(prev_split_of_art)
            subpoint_of_art: List[PointSplit] = []
            for index_split in range(len(splits_of_arts_part_upcoming_changes)):
                for match_of_upcoming_split in self.split_function(
                        splits_of_arts_part_upcoming_changes[index_split].text):
                    start_point_index_match = splits_of_arts_part_upcoming_changes[
                                                  index_split].start_index + match_of_upcoming_split.start()
                    end_point_index_match = splits_of_arts_part_upcoming_changes[
                                                index_split].start_index + match_of_upcoming_split.end() - 1

                    subpoint_split = splits_of_arts_part_upcoming_changes[index_split].build_text_split_from_indexes(
                        left_index=start_point_index_match,
                        right_index=end_point_index_match
                    )

                    subpoint_of_art.append(PointSplit(subpoint_split))

            ## TODO check that kind of situation exists
            if len(art_split.legal_units_indeed) > 0 and len(subpoint_of_art) > 0:
                raise RuntimeError(f"It should be like that in Art: {art_split.id_unit}, "
                                   f"sections numbers: {[section.id_unit for section in art_split.legal_units_indeed]}"
                                   f"subpoints numbers: {[subpoint for subpoint in subpoint_of_art]}")
            art_split.legal_units_indeed.extend(subpoint_of_art)

        for section_split in art_split.legal_units_indeed:
            if isinstance(section_split, SectionSplit):
                prev_split_section = section_split.split_item_for_further_processing()

                splits_of_upcoming_changes = self.split_by_upcoming_changes(prev_split_section)

                subpoint_splits_for_each_section: List[PointSplit] = []
                for index_split in range(len(splits_of_upcoming_changes)):
                    for match_of_upcoming_split in self.split_function(splits_of_upcoming_changes[index_split].text):
                        from_group_text = match_of_upcoming_split.group()
                        start_point_index_match = splits_of_upcoming_changes[
                                                      index_split].start_index + match_of_upcoming_split.start()
                        end_point_index_match = splits_of_upcoming_changes[
                                                    index_split].start_index + match_of_upcoming_split.end() - 1

                        subpoint_split = splits_of_upcoming_changes[index_split].build_text_split_from_indexes(
                            left_index=start_point_index_match,
                            right_index=end_point_index_match
                        )

                        subpoint_splits_for_each_section.append(PointSplit(subpoint_split))
                section_split.set_legal_indeed_units(subpoint_splits_for_each_section)
        return art_split