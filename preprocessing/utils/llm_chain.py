import time
from enum import Enum

import openai
from langchain_core.language_models import BaseChatModel
from langchain_core.runnables.utils import Input
from langchain_core.prompts import FewShotChatMessagePromptTemplate, ChatPromptTemplate
from langchain_openai import ChatOpenAI
from typing import Tuple

from openai import RateLimitError

from preprocessing.utils.few_shot_examples import CreateAnnotationOfTable, get_few_shot_examples, TypeOfAttachment
from preprocessing.utils.llm_chain_type import LLMChainType


class LLMChainCallError(Exception):
    pass


class LLMChain:
    def __init__(self, model:BaseChatModel, output_schema, system_massage:Tuple, user_massage:Tuple, system_massage_retry:Tuple=None, few_shot_examples:list=None):
        self.chain = self._build_chain(model, output_schema, system_massage, user_massage, few_shot_examples)
        self.retry_chain = self._build_chain(model, output_schema, system_massage_retry, user_massage, few_shot_examples) if system_massage_retry else None
        l = 4


    def invoke_chain(self, input:Input, retry:bool=False):
        return self.retry_chain.invoke(input) if retry and self.retry_chain else self.chain.invoke(input)

    def call_chain(self, input:Input, retry:bool=False, retries:int = 0, wait_time_s: int= 30, max_retries:int = 5):
        try:
            response = self.invoke_chain(input, retry)

            return response
        except RateLimitError as e:
            time.sleep(wait_time_s)
            if retries < max_retries:
                print(f"Rate limit exceeded. Retrying in {wait_time_s} seconds...")
                return self.call_chain(input, retry, retries + 1, wait_time_s*2, max_retries)
            else:
                raise LLMChainCallError(f"Rate limit exceeded after {retries} retries, error: {e}, during call {input}")
        except Exception as e:
            # Handle any other unexpected error
            raise LLMChainCallError(f"An unexpected error occurred: {e}, during call {input}")

    def _get_prompt_examples(self, few_shot_examples: list):
        prompt_examples_form = ChatPromptTemplate.from_messages(
            [
                ("human", "{input}"),
                ("ai", "{output}")
            ]
        )
        return FewShotChatMessagePromptTemplate(
            examples=few_shot_examples,
            example_prompt=prompt_examples_form
        )

    def _get_prompt(self, system_massage:Tuple, user_massage:Tuple, few_shot_examples:list=None):
        if few_shot_examples:
            return ChatPromptTemplate.from_messages([
                system_massage,
                self._get_prompt_examples(few_shot_examples),
                user_massage
            ])
        else:
            return ChatPromptTemplate.from_messages([
                system_massage,
                user_massage
            ])

    def _build_chain(self,model:BaseChatModel, output_schema, system_massage:Tuple, user_massage:Tuple, few_shot_examples:list=None):
        return self._get_prompt(system_massage=system_massage,
                                user_massage= user_massage,
                                few_shot_examples=few_shot_examples) \
            | model.with_structured_output(output_schema)


def get_table_chains(chain_type: LLMChainType, ai_model: BaseChatModel):
    if chain_type == LLMChainType.ADD_ANNOTATION_OF_TABLE_CHAIN:
        return LLMChain(
            model=ai_model,
            output_schema=CreateAnnotationOfTable,
            system_massage=("system",
                            """"
                             You will get document page with text and/or with table/ tables, where table start add annotation /table_start/ just this not any other annotation, there are one or more tables, beside that keep text not changed but just add this annotation, you cannot remove any signs!!!!!!
                             THERE are must be titles of columns, if there aren't, don't add annotation
                            """ ),
            system_massage_retry=("system",
                                  """"
                                   You will get document page with text and/or with table/ tables, where table start add annotation /table_start/ just this not any other annotation, there are one or more tables, beside that keep text not changed but just add this annotation, you cannot remove any signs!!!!!!
                                    THERE are must be titles of columns, if there aren't, don't add annotation
                                    You previously placed incorrectly /table start/ or not enough amount of annotations,
                                     do it again"""),
            user_massage=("user", "{input}"),
            few_shot_examples=get_few_shot_examples(LLMChainType.ADD_ANNOTATION_OF_TABLE_CHAIN)
        )
    elif chain_type == LLMChainType.WHAT_TYPE_OF_ANNOTATION_CHAIN:
        return LLMChain(
            model=ai_model,
            output_schema=TypeOfAttachment,
            system_massage=("system",  """Twoim zadaniem jest określenie rodzaju załącznika na podstawie następujących kategorii: 
                                       'wzór_dokumentu', 'zawiera_tabele', 'wzory_do_obliczeń' lub 'inne'.\n\n
                                       Wybierz najbardziej odpowiednią kategorię zgodnie z poniższymi definicjami:\n\n
                                       """),
            user_massage=("user", "{input}")
        )
    else:
        raise Exception("Invalid LLMChainType")
