import re
from typing import List

from preprocessing.pdf_structure.splits.article_split import ArticleSplit
from preprocessing.pdf_structure.splits.chapter_split import ChapterSplit
from preprocessing.pdf_structure.splitters.abstract_document_splitter import AbstractDocumentSplitter


class ArticleSplitter(AbstractDocumentSplitter[ArticleSplit]):

    def __init__(self):
        super().__init__()

    def before_upcoming_change_pattern(self):
        return r"\[\s*Art\.\s*\d+[a-zA-Z]*\.[^\]\[\>]*\]"  # [Art. 1.]

    def upcoming_change_pattern(self):
        return r"<\s*Art\.\s*\d+[a-zA-Z]*\.[^>\]\[]*>"  # <Art. 1.>

    def split_function(self, text):
        return re.finditer(r'(^Art[\.]*\s*\d+[a-zA-Z]*[\.][\s\S]*?)(?=^Art[\.]*\s*\d+[a-zA-Z]*[\.]|\Z)', text, flags=re.DOTALL | re.MULTILINE)

    def split(self, chapter_split: ChapterSplit):
        prev_split = chapter_split.split_item_for_further_processing()

        splits_of_upcoming_changes = self.split_by_upcoming_changes(prev_split)

        art_splits = []
        for index_split in range(len(splits_of_upcoming_changes)):
            for match_of_upcoming_split in self.split_function(splits_of_upcoming_changes[index_split].text):

                from_group_text = match_of_upcoming_split.group()
                start_index_match = splits_of_upcoming_changes[
                                        index_split].start_index + match_of_upcoming_split.start()
                end_index_match = splits_of_upcoming_changes[
                                      index_split].start_index + match_of_upcoming_split.end() - 1

                art_split = splits_of_upcoming_changes[index_split].build_text_split_from_indexes(
                    left_index=start_index_match,
                    right_index=end_index_match
                )

                if not (art_split.text ==
                        prev_split.build_text_split_from_indexes(start_index_match, end_index_match).text
                        and art_split.text == from_group_text):
                    raise RuntimeError(f"It fail due to incompatibility of split indexes art_split: {art_split.text} "
                                       f"prev_split using indexes: {prev_split.build_text_split_from_indexes(start_index_match, end_index_match).text}")
                art_splits.append(ArticleSplit(art_split))

        filtered_art_splits = self.filter_splits(art_splits)
        chapter_split.articles = filtered_art_splits
        return filtered_art_splits

    def filter_splits(self, article_splits: List[ArticleSplit]) -> List[ArticleSplit]:
        filtered_splits = []
        for article_split in article_splits:
            text_split = article_split.split_item_for_further_processing()

            text_of_split = text_split.text.replace("\n", "")
            if not ('wprowadza się następujące zmiany' in text_of_split):
                filtered_splits.append(article_split)
            else:
                w = 4

        return filtered_splits
