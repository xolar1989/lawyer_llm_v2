from typing import List, Mapping, Any

from preprocessing.mongo_db.mongodb import MongodbObject
from preprocessing.qustions_and_answers_objects.legal_unit_annotation import LegalUnitAnnotation


class LegalQuestionWithAnnotations(MongodbObject):

    def __init__(self, nro: int, invoke_id: str, title: str, question_content: str, answer_content: str,
                 question_annotations: List[LegalUnitAnnotation], answer_annotations: List[LegalUnitAnnotation]
                 ):
        self.nro = nro
        self.invoke_id = invoke_id
        self.title = title
        self.question_content = question_content
        self.answer_content = answer_content
        self.question_annotations = question_annotations
        self.answer_annotations = answer_annotations

    def to_dict(self):
        return {
            'nro': self.nro,
            'invoke_id': self.invoke_id,
            'title': self.title,
            'question_content': self.question_content,
            'answer_content': self.answer_content,
            'question_annotations': [annotation.to_dict() for annotation in self.question_annotations],
            'answer_annotations': [annotation.to_dict() for annotation in self.answer_annotations]
        }

    @classmethod
    def from_dict(cls, dict_object: Mapping[str, Any]):
        return cls(
            nro=dict_object['nro'],
            invoke_id=dict_object['invoke_id'],
            title=dict_object['title'],
            question_content=dict_object['question_content'],
            answer_content=dict_object['answer_content'],
            question_annotations=[LegalUnitAnnotation.from_dict(annotation_dict) for annotation_dict in
                                  dict_object['question_annotations']],
            answer_annotations=[LegalUnitAnnotation.from_dict(annotation_dict) for annotation_dict in
                                dict_object['answer_annotations']]
        )
