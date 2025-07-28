import re
from typing import Optional, List

from pydantic import BaseModel, Field, field_validator


class UnitReference(BaseModel):
    artykul: str = Field(..., description="Numer artykułu")
    ustep: Optional[str] = Field(None, description="Numer ustępu")
    punkt: Optional[str] = Field(None, description="Numer punktu")

    @field_validator("artykul", "ustep", "punkt", mode="before")
    @classmethod
    def to_lowercase(cls, v):
        if v is None:
            return v
        return v.lower()


class LegalActReference(BaseModel):
    tytul: str = Field(..., description="Pełny tytuł ustawy")
    skrot: Optional[str] = Field(None, description="Skrót ustawy")
    id: Optional[str] = Field(None, description="ID ustawy z linku")
    przepisy: List[UnitReference] = Field(..., description="Lista przepisów")

    @staticmethod
    def normalize_title(title: str) -> str:
        # Usuń nadmiarowe spacje i końcową kropkę (jeśli jest)
        title = re.sub(r"\s+", " ", title).strip()



        return title.rstrip(".").strip().lower()

    @field_validator("tytul")
    def clean_title(cls, value: str) -> str:
        if "\\u" in value:
            import codecs
            value = codecs.decode(value, "unicode_escape")
        USTAWA_REGEX = re.compile(
            r"^Ustawa\s+z\s+dnia\s+(\d{1,2}\s+(stycznia|lutego|marca|kwietnia|maja|czerwca|lipca|sierpnia|września|października|listopada|grudnia)|\d{2}\.\d{2})\s+\d{4}\s+r\.\s+[\s\S]*?[.]*$",
            flags=re.DOTALL | re.MULTILINE | re.IGNORECASE
        )
        # Remove leading/trailing spaces and collapse multiple spaces
        value = re.sub(r"\s+", " ", value).strip()  # replace multiple spaces with one


        if not USTAWA_REGEX.match(value):
            raise ValueError(f"Invalid 'tytul' format: {value}")
        return cls.normalize_title(value)


class LegalReferenceList(BaseModel):
    items: List[LegalActReference] = Field(..., description="Lista aktów prawnych")