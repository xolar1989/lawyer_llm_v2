from langchain_community.callbacks import get_openai_callback
from langchain_core.exceptions import OutputParserException
from langchain_core.language_models import BaseChatModel
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate, PromptTemplate, FewShotPromptTemplate
from langchain_core.runnables import Runnable
from openai import RateLimitError
from pydantic import ValidationError

from preprocessing.logging.aws_logger import aws_logger
from preprocessing.mongo_db.mongodb import get_mongodb_collection
from preprocessing.qustions_and_answers_objects.legal_rephrased_question_llm import LegalRephrasedQuestionLLM


class QuestionLLMRephraser:

    def __init__(self, llm: BaseChatModel, invoke_id: str):
        self.llm = llm
        self.invoke_id = invoke_id
        self.parser = PydanticOutputParser(pydantic_object=LegalRephrasedQuestionLLM)
        self.input_variable = "question"

        examples = [
            {
                "input": (
                    "Firma będąca czynnym podatnikiem VAT otrzymała fakturę za usługę transportową od zagranicznego "
                    "kontrahenta z Holandii (posiadającego aktywny numer VAT w systemie VIES). Usługa dotyczyła "
                    "przemieszczenia towarów z Polski do Holandii i jest bezpośrednio związana z prowadzoną przez firmę "
                    "działalnością gospodarczą. Faktura została wystawiona w euro, bez naliczonego podatku VAT. Firma "
                    "zastosuje mechanizm odwrotnego obciążenia (reverse charge). Jak prawidłowo wykazać podatek należny z "
                    "tytułu tej transakcji w deklaracji VAT? Czy w pliku JPK_V7M należy ująć tę transakcję jako import usług "
                    "objęty [art. 28b] ustawy o VAT (pola P_29 i P_30), czy jako import usług z wyłączeniem usług, do których "
                    "stosuje się art. 28b (pola P_27 i P_28)?"
                ),
                "output": (
                    "Jak prawidłowo ująć w pliku JPK_V7M import usług transportowych od kontrahenta z UE – zgodnie z art. 28b ustawy o VAT – "
                    "jako import usług objęty art. 28b (pola P_29 i P_30), czy jako import usług nieobjęty tym przepisem (pola P_27 i P_28)?"
                )
            }
        ]

        example_prompt = PromptTemplate(
            input_variables=["input", "output"],
            template="### Przykład wejścia:\n{input}\n\n### Oczekiwane wyjście:\n{output}\n"
        )

        self.few_shot_prompt = FewShotPromptTemplate(
            examples=examples,
            example_prompt=example_prompt,
            prefix=(
                "Twoim zadaniem jest przekształcenie pytania użytkownika, które może być długie, zawierać zbędny kontekst "
                "lub być nieprecyzyjne.\n\n"
                "Na podstawie podanego tekstu:\n"
                "- Wyodrębnij główne pytanie prawne\n"
                "- Zapisz je w sposób jasny, zwięzły i formalny\n"
                "- Pozostaw odniesienia do przepisów prawa (np. „art. 28b ustawy o VAT”)\n"
                "- Usuń zbędny kontekst, chyba że jest on kluczowy do zrozumienia pytania"
            ),
            suffix="### Wejście użytkownika:\n{question}\n\n### Oczekiwane wyjście:{format_instructions}",
            input_variables=["question"]
        )
        self.chain: Runnable = self.few_shot_prompt | self.llm | self.parser

    def rephrase(self, question_text: str) -> LegalRephrasedQuestionLLM:

        try:
            with get_openai_callback() as cb:
                result: LegalRephrasedQuestionLLM = self.chain.invoke({
                    self.input_variable: question_text,
                    "format_instructions": self.parser.get_format_instructions()
                })
                get_mongodb_collection(
                    db_name="costs_of_runs",
                    collection_name="dataset_preparations_costs"
                ).update_one(
                    {
                        "name": "rephrasing_cost",
                        "invoke_id": self.invoke_id
                    },
                    {
                        "$inc": {
                            "prompt_tokens": cb.prompt_tokens,
                            "completion_tokens": cb.completion_tokens,
                            "total_tokens": cb.total_tokens,
                            "total_cost": (cb.prompt_tokens/(0.25 * 1_000_000)) + (cb.completion_tokens/(2 * 1_000_000))
                        }
                    },
                    upsert=True
                )
            return result
        except ValidationError as e:
            aws_logger.error(f"Error during validation for {question_text}")
            raise e
        except RateLimitError as e:
            aws_logger.error("Rate limit error in rate limit error of openai...")
            raise e
        except OutputParserException as e:
            aws_logger.error("Rate limit error in rate limit error of openai...")
            raise e
        except Exception as e:
            aws_logger.error("Rate limit error in exception, retrying...")
            raise e