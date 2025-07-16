import re
from typing import List

from preprocessing.pdf_structure.splits.article_split import ArticleSplit
from preprocessing.pdf_structure.splits.section_split import SectionSplit
from preprocessing.pdf_structure.splitters.abstract_document_splitter import AbstractDocumentSplitter


class SectionSplitter(AbstractDocumentSplitter[SectionSplit]):

    def __init__(self):
        super().__init__()

    def before_upcoming_change_pattern(self):
        return r"\[\s*(?:§\s*\d+[a-zA-Z]?[\.]?\s*)?[^\<\>\[\]]+?\]"  # [1.] [§ 1.]

    def upcoming_change_pattern(self):
        return r"\<\s*(?:§\s*\d+[a-zA-Z]?[\.]?\s*)?[^\<\>\[\]]+?\>"  # <1.> <§ 1.>

    def split_function(self, text):
        return re.finditer(
                r'(?<![-–])(?<!str\.\s)(?<!str\.\s\s)(?<!str\.\s\s\s)(?<!\salbo\s)(?<!\salbo\s\s)(?<!\salbo\s\s\s)(?<!\slub\s)(?<!\slub\s\s)(?<!\si\s)(?<!\si\s\s)(?<!\si\s\s\s)(?<!pkt\s)(?<!pkt\s\s)(?<!pkt\s\s\s)(?<!ust\.\s)(?<!ust\.\s\s)(?<!ust\.\s\s\s)(?<!art\.\s)(?<!art\.\s\s)(?<!art\.\s\s\s)(?<!poz\.\s)(?<!poz\.\s\s)(?<!poz\.\s\s\s)^((?:[\<§\[]*\s*)?\d{1,2}[a-zA-Z⁰¹²³⁴⁵⁶⁷⁸⁹]{0,4}\.\s*(?=[A-ZĄĆĘŁŃÓŚŹŻ⁰¹²³⁴⁵⁶⁷⁸⁹⁾\(])[\s\S]*?)(?=(?<![-–])(?<!\salbo\s)(?<!\salbo\s\s)(?<!\salbo\s\s\s)(?<!str\.\s)(?<!str\.\s\s)(?<!str\.\s\s\s)(?<!\slub\s)(?<!\slub\s\s)(?<!\si\s)(?<!\si\s\s)(?<!\si\s\s\s)(?<!pkt\s)(?<!pkt\s\s)(?<!pkt\s\s\s)(?<!ust\.\s)(?<!ust\.\s\s)(?<!ust\.\s\s\s)(?<!art\.\s)(?<!art\.\s\s)(?<!art\.\s\s\s)(?<!poz\.\s)(?<!poz\.\s\s)(?<!poz\.\s\s\s)^((?:[\<§\[]*\s*)?\d{1,2}[a-zA-Z⁰¹²³⁴⁵⁶⁷⁸⁹]{0,4}\.\s*(?=[A-ZĄĆĘŁŃÓŚŹŻ⁰¹²³⁴⁵⁶⁷⁸⁹⁾\(]))|\Z)',
                text,
                flags=re.DOTALL | re.MULTILINE
            )

    def split(self, art_split: ArticleSplit):
        prev_split = art_split.split_item_for_further_processing()

        if art_split.id_unit == '72':
            w = 4

        splits_of_upcoming_changes = self.split_by_upcoming_changes(prev_split)

        sections_splits = []
        for index_split in range(len(splits_of_upcoming_changes)):
            for match_of_upcoming_split in self.split_function(splits_of_upcoming_changes[index_split].text):
                start_index_match = splits_of_upcoming_changes[
                                        index_split].start_index + match_of_upcoming_split.start()
                end_index_match = splits_of_upcoming_changes[
                                      index_split].start_index + match_of_upcoming_split.end() - 1

                passage_split = splits_of_upcoming_changes[index_split].build_text_split_from_indexes(
                    left_index=start_index_match,
                    right_index=end_index_match
                )
                sections_splits.append(SectionSplit(passage_split))
        art_split.set_legal_indeed_units(sections_splits)
        return art_split

    def filter_splits(self, splits: List[SectionSplit]) -> List[SectionSplit]:
        pass
