from typing import List, Mapping, Any

from transformers import AutoTokenizer

from model.special_tokens import SPECIAL_TOKENS
from preprocessing.qustions_and_answers_objects.legal_question_with_annotations import LegalQuestionWithAnnotations
from preprocessing.qustions_and_answers_objects.legal_rephrased_question_llm import LegalRephrasedQuestionLLM
from preprocessing.qustions_and_answers_objects.legal_unit_annotation import LegalUnitAnnotation


class LegalRephrasedQuestion:

    def __init__(self, nro: int, invoke_id: str, title: str, question_content: str,
                 rephrased_question: str,
                 answer_content: str,
                 question_annotations: List[LegalUnitAnnotation], answer_annotations: List[LegalUnitAnnotation]
                 ):
        self.nro = nro
        self.invoke_id = invoke_id
        self.title = title
        self.rephrased_question = rephrased_question
        self.question_content = question_content
        self.answer_content = answer_content
        self.question_annotations = question_annotations
        self.answer_annotations = answer_annotations

    def to_dict(self):
        return {
            'nro': self.nro,
            'invoke_id': self.invoke_id,
            'title': self.title,
            'rephrased_question': self.rephrased_question,
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
            rephrased_question=dict_object['rephrased_question'],
            answer_content=dict_object['answer_content'],
            question_annotations=[LegalUnitAnnotation.from_dict(annotation_dict) for annotation_dict in
                                  dict_object['question_annotations']],
            answer_annotations=[LegalUnitAnnotation.from_dict(annotation_dict) for annotation_dict in
                                dict_object['answer_annotations']]
        )

    @classmethod
    def from_question(cls, legal_question: LegalQuestionWithAnnotations, rephrased_question: LegalRephrasedQuestionLLM, model_id: str):
        tokenizer = AutoTokenizer.from_pretrained(model_id)
        tokenizer.add_special_tokens({
            "additional_special_tokens": list(SPECIAL_TOKENS.values())
        })

        prefix_ids = tokenizer.encode(rephrased_question.rephrased_question, add_special_tokens=True)

        if len(prefix_ids) > tokenizer.model_max_length:
            raise ValueError(f"legal_question with nro: {legal_question.nro}, its question exceed limit")

        return cls(
            nro=legal_question.nro,
            invoke_id=legal_question.invoke_id,
            title=legal_question.title,
            question_content=legal_question.question_content,
            rephrased_question=rephrased_question.rephrased_question,
            answer_content=legal_question.answer_content,
            question_annotations=legal_question.question_annotations,
            answer_annotations=legal_question.answer_annotations
        )
