import re
from abc import ABC, abstractmethod
from typing import List, Iterable, Tuple

import unicodedata
from transformers import PreTrainedTokenizerBase

from preprocessing.chunks.article_chunks import ArticleChunks
from preprocessing.chunks.chunk import AbstractChunks
from preprocessing.chunks.section_chunks import SectionChunks
from preprocessing.chunks.subpoint_chunks import SubpointChunks
from preprocessing.pdf_structure.elements.article import Article
from preprocessing.pdf_structure.elements.legal_unit import LegalUnit
from preprocessing.pdf_structure.elements.section import Section
from preprocessing.pdf_structure.elements.subpoint import Subpoint


class AbstractChunker(ABC):

    def __init__(self, tokenizer: PreTrainedTokenizerBase, max_token_length: int,
                 overlap: int, special_token: str, ELI: str, invoke_id: str, model_id: str):
        self.tokenizer = tokenizer
        self.max_token_length = max_token_length
        self.overlap = overlap
        self.special_token = special_token
        self.ELI = ELI
        self.invoke_id = invoke_id
        self.model_id = model_id

    @classmethod
    def normalize_text(cls, text: str) -> str:
        _RE_BREAKS = re.compile(r"\s+")
        """Return a whitespace-normalized, layout-free version of *text*."""
        text = unicodedata.normalize("NFC", text)
        text = _RE_BREAKS.sub(" ", text).strip()

        return text


    @abstractmethod
    def chunk_unit(self, **kwargs) -> AbstractChunks:
        pass

    @abstractmethod
    def chunk_unit_for_chunking(self, **kwargs):
        pass

    def unit_prefix(self, legal_unit:LegalUnit):
        if isinstance(legal_unit, Article):
            return f"ART. {legal_unit.unit_id}"
        elif isinstance(legal_unit, Section):
            return f"§ {legal_unit.unit_id}"
        elif isinstance(legal_unit, Subpoint):
            # TODO i need rerun legal unit splitting because in Subpoint ids are included in text while it shouldn't
            return f"PKT. {legal_unit.unit_id}"
            # return ""

    def get_part_of_article_text(self, article: Article, num: int):
        return self.get_text_of(self.normalize_text(article.text), num)

    def get_part_of_section_text(self, section: Section, num: int):
        return self.unit_prefix(section) + " " + self.get_text_of(self.normalize_text(section.text), num)

    def get_text_of(self, text: str, num: int, add_special_tokens: bool = False, skip_special_tokens: bool = True):
        ids = self.tokenizer.encode(text, add_special_tokens=add_special_tokens)
        ids_firsts = ids[:min(num, len(ids))]

        return self.tokenizer.decode(ids_firsts, skip_special_tokens=skip_special_tokens)

    def calc_tokens(self, text: str, add_special_tokens: bool = False):
        prefix_ids = self.tokenizer.encode(text, add_special_tokens=add_special_tokens)
        return len(prefix_ids)

    def is_within_window_size(self, text):
        return len(self.tokenizer.encode(text, add_special_tokens=True)) <= self.max_token_length

    def chunk_text(self, text: str, window: int, overlap: int):
        """
       Yield successive `window`-sized slices of `text`, re-using the last
       `overlap` characters (or tokens) from the previous chunk.

       Example
       -------
        >> list(chunk_text("ABCDEFG", window=3, overlap=1))
       ['ABC', 'CDE', 'EFG']
       """
        special = self.tokenizer.num_special_tokens_to_add(pair=False)  # usually 2

        if overlap >= window - special:
            raise ValueError("overlap too large")

        payload_max = window - special
        step = payload_max - overlap

        payload_ids = self.tokenizer.encode(text, add_special_tokens=False)

        for start in range(0, len(payload_ids), step):
            end = start + payload_max
            chunk_ids = payload_ids[start:end]
            yield self.tokenizer.decode(chunk_ids, skip_special_tokens=True)
            if end >= len(payload_ids):
                break
