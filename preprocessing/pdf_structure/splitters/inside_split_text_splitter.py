from preprocessing.pdf_structure.splits.article_split import ArticleSplit
from preprocessing.pdf_structure.splits.section_split import SectionSplit
from preprocessing.pdf_structure.splits.text_split import TextSplit, StatusOfText
from preprocessing.pdf_structure.splitters.abstract_document_splitter import AbstractDocumentSplitter


class InsideSplitTextSplitter(AbstractDocumentSplitter):

    def before_upcoming_change_pattern(self):
        return r'\[([^\[\]]*?)\]'

    def upcoming_change_pattern(self):
        return r'<([^\[\]]*?)>'

    def split_function(self, text):
        pass

    def update_split_by_upcoming_changes(self, current_split: TextSplit) -> None:
        before_change_splits = self._find_changes_text(
            current_split,
            self.before_upcoming_change_pattern(),
            StatusOfText.BEFORE_UPCOMING_CHANGE)
        after_change_splits = self._find_changes_text(
            current_split,
            self.upcoming_change_pattern(),
            StatusOfText.UPCOMING_CHANGE)

        splits_of_upcoming_changes = self._organise_text(
            prev_split=current_split,
            before_upcoming_changes_splits=before_change_splits,
            after_upcoming_changes_splits=after_change_splits
        )

    def split(self, art_split: ArticleSplit) -> ArticleSplit:
        art_split_without_indeed_items = art_split.split_item_for_further_processing()
        if art_split_without_indeed_items:
            self.update_split_by_upcoming_changes(art_split_without_indeed_items)
        for indeed_split in art_split.legal_units_indeed:
            indeed_split_without_indeed_items = indeed_split.split_item_for_further_processing()
            if indeed_split_without_indeed_items:
                self.update_split_by_upcoming_changes(indeed_split_without_indeed_items)
            if isinstance(indeed_split, SectionSplit):
                for subpoint_split in indeed_split.legal_units_indeed:
                    self.update_split_by_upcoming_changes(subpoint_split.split_item_for_further_processing())
        return art_split