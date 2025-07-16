import re
from abc import abstractmethod, ABC
from typing import List,  TypeVar, Generic

import unicodedata

from preprocessing.pdf_structure.splits.text_split import StatusOfText, TextSplit


T = TypeVar('T')


class AbstractDocumentSplitter(ABC, Generic[T]):
    BEFORE_UPCOMING_CHANGE_PATTERN = r'^\[([^\[\]]*?)\]$'
    UPCOMING_CHANGE_PATTERN = r'^<([^\[\]]*?)>$'

    def __init__(self):
        pass

    @abstractmethod
    def before_upcoming_change_pattern(self):
        pass  # <....>

    @abstractmethod
    def upcoming_change_pattern(self):
        pass  # [....]

    @abstractmethod
    def split_function(self, **kwargs):
        pass

    @abstractmethod
    def split(self, **kwargs):
        pass

    @abstractmethod
    def filter_splits(self, splits: List[T]) -> List[T]:
        pass

    @staticmethod
    def normalize(text: str) -> str:
        # Usuwa znaki diakrytyczne, np. ą → a, ć → c
        text = unicodedata.normalize('NFKD', text)
        return ''.join(c for c in text if not unicodedata.combining(c)).lower()

    def get_pattern_for_text_with_brackets(self, action_type: StatusOfText):
        return self.BEFORE_UPCOMING_CHANGE_PATTERN if action_type == StatusOfText.BEFORE_UPCOMING_CHANGE else self.UPCOMING_CHANGE_PATTERN

    def _find_changes_text(self, text_split: TextSplit, pattern_str: str, action_type: StatusOfText) -> List[TextSplit]:
        pattern = re.compile(pattern_str)

        splits_of_change = []
        for match in pattern.finditer(text_split.text):
            start_index_of_match = text_split.start_index + match.start()
            end_index_of_match = text_split.start_index + match.end() - 1

            splits_of_change.append(
                TextSplit(
                    text_split.get_chars_from_indexes_and_set_status(
                        start_index_of_match,
                        end_index_of_match,
                        action_type),
                    action_type)
            )
        return splits_of_change

    @staticmethod
    def is_splits_overlap(splits_from_prev_split: List[TextSplit], prev_split: TextSplit):
        for i in range(len(splits_from_prev_split) - 1):
            if splits_from_prev_split[i].end_index >= splits_from_prev_split[i + 1].start_index:
                return True
        if splits_from_prev_split[len(splits_from_prev_split) - 1].end_index == prev_split.end_index \
                and splits_from_prev_split[0].start_index == prev_split.start_index:
            return False
        else:
            return True

    def _organise_text(self, prev_split: TextSplit, before_upcoming_changes_splits: List[TextSplit],
                       after_upcoming_changes_splits: List[TextSplit]):
        upcoming_changes_splits = before_upcoming_changes_splits + after_upcoming_changes_splits
        not_matches = self._find_non_matches(upcoming_changes_splits, prev_split)
        all_pieces = sorted(upcoming_changes_splits + not_matches, key=lambda x: (x.start_index, x.end_index))
        if self.is_splits_overlap(all_pieces, prev_split):
            raise RuntimeError("Pieces overlap")
        return all_pieces

    def _find_non_matches(self, upcoming_changes_splits: List[TextSplit], prev_split: TextSplit):

        sorted_upcoming_changes = sorted(upcoming_changes_splits, key=lambda x: (x.start_index, x.end_index))
        non_matches = []
        last_end = prev_split.start_index
        for upcoming_split in sorted_upcoming_changes:
            if last_end < upcoming_split.start_index:
                non_matches.append(
                    TextSplit(
                        prev_split.get_chars_from_indexes_and_set_status(
                            last_end,
                            upcoming_split.start_index - 1,
                            StatusOfText.NOT_MATCH_CHANGE),
                        action_type=StatusOfText.NOT_MATCH_CHANGE
                    )
                )
            last_end = upcoming_split.end_index + 1
        if last_end <= prev_split.end_index:
            non_matches.append(
                TextSplit(
                    prev_split.get_chars_from_indexes_and_set_status(
                        last_end,
                        prev_split.end_index,
                        StatusOfText.NOT_MATCH_CHANGE),
                    action_type=StatusOfText.NOT_MATCH_CHANGE
                )
            )
            w = non_matches[len(non_matches) - 1].chars[len(non_matches[len(non_matches) - 1].chars) - 1]
        return non_matches

    def split_by_upcoming_changes(self, prev_split: TextSplit) -> List[TextSplit]:
        if prev_split.action_type != StatusOfText.NOT_MATCH_CHANGE:
            return [prev_split]

        before_change_splits = self._find_changes_text(prev_split,
                                                       self.before_upcoming_change_pattern(),
                                                       StatusOfText.BEFORE_UPCOMING_CHANGE)
        after_change_splits = self._find_changes_text(prev_split, self.upcoming_change_pattern(),
                                                      StatusOfText.UPCOMING_CHANGE)

        splits_of_upcoming_changes = self._organise_text(
            prev_split=prev_split,
            before_upcoming_changes_splits=before_change_splits,
            after_upcoming_changes_splits=after_change_splits
        )

        for index_split in range(len(splits_of_upcoming_changes)):
            if splits_of_upcoming_changes[index_split].action_type == StatusOfText.BEFORE_UPCOMING_CHANGE or \
                    splits_of_upcoming_changes[index_split].action_type == StatusOfText.UPCOMING_CHANGE:
                pattern = self.get_pattern_for_text_with_brackets(splits_of_upcoming_changes[index_split].action_type)
                is_text_between_brackets = re.match(pattern, splits_of_upcoming_changes[index_split].text)
                start_index_without_brackets = is_text_between_brackets.start(1)
                end_index_without_brackets = is_text_between_brackets.end(1)

                chars_without_brackets = splits_of_upcoming_changes[index_split].chars[
                                         start_index_without_brackets:end_index_without_brackets]

                splits_of_upcoming_changes[index_split] = TextSplit(chars_without_brackets,
                                                                    splits_of_upcoming_changes[index_split].action_type)

        return splits_of_upcoming_changes