from typing import List, Mapping, Any

from preprocessing.chunks.final_annotation_chunk import FinalAnnotationChunk
from preprocessing.mongo_db.mongodb import MongodbObject


class QuestionWithAnnotationChunks(MongodbObject):
    def __init__(self, nro: int, invoke_id: str, question_text: str,
                 article_span_chunks: List[FinalAnnotationChunk],
                 article_chunks: List[FinalAnnotationChunk],
                 section_chunks: List[FinalAnnotationChunk],
                 point_chunks: List[FinalAnnotationChunk]
                 ):
        self.nro = nro
        self.invoke_id = invoke_id
        self.question_text = question_text
        self.article_span_chunks = article_span_chunks
        self.article_chunks = article_chunks
        self.section_chunks = section_chunks
        self.point_chunks = point_chunks

    def to_dict(self) -> Mapping[str, Any]:
        return {
            "nro": self.nro,
            "invoke_id": self.invoke_id,
            "question_text": self.question_text,
            "article_span_chunks": [c.to_dict() for c in self.article_span_chunks],
            "article_chunks": [c.to_dict() for c in self.article_chunks],
            "section_chunks": [c.to_dict() for c in self.section_chunks],
            "point_chunks": [c.to_dict() for c in self.point_chunks],
        }

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]) -> "QuestionWithAnnotationChunks":
        return cls(
            nro=dict_object["nro"],
            invoke_id=dict_object.get("invoke_id"),
            question_text=dict_object.get("question_text"),
            article_span_chunks=[FinalAnnotationChunk.from_dict(c) for c in dict_object.get("article_span_chunks")],
            article_chunks=[FinalAnnotationChunk.from_dict(c) for c in dict_object.get("article_chunks")],
            section_chunks=[FinalAnnotationChunk.from_dict(c) for c in dict_object.get("section_chunks")],
            point_chunks=[FinalAnnotationChunk.from_dict(c) for c in dict_object.get("point_chunks")],
        )
