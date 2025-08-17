from pydantic import BaseModel

class LegalRephrasedQuestionLLM(BaseModel):
    rephrased_question: str