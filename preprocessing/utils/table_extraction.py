import re
from abc import ABC
from typing import Dict

from langchain_core.runnables.utils import Input
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI

from preprocessing.utils.defaults import AWS_REGION
from preprocessing.utils.few_shot_examples import TypeOfAttachment
from preprocessing.utils.general import get_secret
from preprocessing.utils.llm_chain import LLMChain, get_table_chains
from preprocessing.utils.llm_chain_type import LLMChainType


class TableUtils(ABC):

    @staticmethod
    def create_function_from_table(llmchain: LLMChain, input: Input, retry: int = 1):
        table_function = llmchain.call_chain(input, retry != 1)
        try:
            exec(table_function.function_definition)
            function_defined = locals()[table_function.function_name]
            checked_could_define_function = tool(function_defined)
            return table_function
        except Exception as e:
            print(f"Error: {e}")
            if retry > 3:
                return None
            else:
                print(f"It retried to create function from table, retry:{retry}")
                return TableUtils.create_function_from_table(llmchain, input, retry=retry + 1)

    @staticmethod
    def summarize_table_and_erase_it_from_passage(llmchain: LLMChain, input: Input, retry: int = 1):
        table_start_pattern = r'\/\s*t\s*a\s*b\s*l\s*e\s*_\s*s\s*t\s*a\s*r\s*t\s*\/'

        legal_act_with_erased_table = llmchain.call_chain(input, retry != 1)
        try:
            if re.search(table_start_pattern, legal_act_with_erased_table.legal_act_part_text) and len(
                    legal_act_with_erased_table.table_summarization) > 0:
                text_with_erased_table_start_annotation = re.sub(table_start_pattern,
                                                                 f"/{legal_act_with_erased_table.table_summarization}/",
                                                                 legal_act_with_erased_table.legal_act_part_text)
                return text_with_erased_table_start_annotation
            else:
                raise ValueError("Table not found in the text or table summarization is empty.")
        except Exception as e:
            print(f"Error: {e}")
            if retry > 3:
                return None
            else:
                print(f"It retried to summarize table and erase it from passage, retry:{retry}")
                return TableUtils.summarize_table_and_erase_it_from_passage(llmchain, input, retry=retry + 1)

    @classmethod
    def add_annotation_of_table(cls, llmchain: LLMChain, input: Input, row: Dict[str, str], amount_of_table: int, retry: int = 1):
        annotation_of_table = llmchain.call_chain(input, retry != 1)
        try:
            if annotation_of_table is None:
                raise ValueError("Table annotation is None.")

            table_start_count = annotation_of_table.text_page.count("/table_start/")

            if table_start_count != amount_of_table:
                raise ValueError(
                    f"Expected {amount_of_table} occurrences of '/table_start/' but found {table_start_count}."
                )

            if annotation_of_table.text_page.strip().endswith("/table_start/") or \
                    annotation_of_table.text_page.strip().startswith("/table_start/"):
                raise ValueError("Table annotation is incorrectly placed at the start or end of the text.")

            return annotation_of_table.text_page
        except Exception as e:
            print(f"Error: {e}")

            ## TODO here raise error because pdfplumber saw table, while there isn't any table
            # overcome this by checking if there is any table in the text by sending pdf?
            if retry > 3:
                print(f"Failed to add annotation of table text {input} after {retry - 1} retries, row: {row}")
                raise ValueError(f"Failed to add annotation of table text {input} after {retry - 1} retries, row: {row}")
            else:
                print(f"It retried to add annotation of table text, retry:{retry}, row:{row}")
                return cls.add_annotation_of_table(llmchain, input, row, amount_of_table, retry=retry + 1)

    @classmethod
    def establish_type_of_annotation(cls, llmchain: LLMChain, input: Input, retry: int = 1) -> TypeOfAttachment:
        annotation_type = llmchain.call_chain(input, retry != 1)
        try:
            if annotation_type is None:
                raise ValueError("Annotation type not found.")
            else:
                return annotation_type
        except Exception as e:
            print(f"Error: {e}")
            if retry > 3:
                raise ValueError(f"Failed to establish type of annotation {input} after {retry - 1} retries.")
            else:
                print(f"It retried to establish type of annotation, retry:{retry}")
                return cls.establish_type_of_annotation(llmchain, input, retry + 1)
