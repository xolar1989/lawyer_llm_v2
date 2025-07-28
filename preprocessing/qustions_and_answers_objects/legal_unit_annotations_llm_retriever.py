import re

from langchain_community.callbacks import get_openai_callback
from langchain_core.exceptions import OutputParserException
from langchain_core.language_models import BaseChatModel
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import Runnable
from bs4 import BeautifulSoup, Tag
from openai import RateLimitError
from pydantic import ValidationError

from preprocessing.logging.aws_logger import aws_logger
from preprocessing.mongo_db.mongodb import get_mongodb_collection
from preprocessing.pdf_elements.chars_maps import superscript_map
from preprocessing.qustions_and_answers_objects.llm_legal_annotations import LegalReferenceList


class LegalUnitAnnotationsLLMRetriever:
    def __init__(self, llm: BaseChatModel, invoke_id: str):
        self.llm = llm
        self.invoke_id = invoke_id
        self.prompt = ChatPromptTemplate.from_messages([
            ("system", "You are a legal document analysis assistant."),
            ("human", """Task:
Analyze the following HTML fragment containing a legal justification and references to Polish legal acts.

Your task is to extract all legal acts referenced in the text, including:

- The full official title in the form: 
     **"Ustawa z dnia [day] [month_name] [year] r. o [name]."** ← ❗ note the period at the end 
   Example: Ustawa z dnia 14 grudnia 1990 r. o zniesieniu i likwidacji niektórych funduszy.
   ❗ Do not use numeric dates like "14.12.1990 r." — always use the full month name in Polish
- The optional abbreviation used in the text (e.g., "u.f.p.")
- The ID of the act from the hyperlink (`#/document/ID`)
- A list of referenced structural units (e.g. Article, Paragraph, Point)
  - Each unit should be returned as an object with `artykul`, `ustep`, and `punkt`
  - ❗ If a range appears (e.g. `pkt(1–3)`, art(1-3) or ust(1-3)), keep it as a string (e.g. `"1–3"`), do not expand into separate items
  - ❌ Do not include or extract references to `litera` (e.g., `lit. a`, `lit. b`), even if they appear in the text
  - ✅ All values should remain exactly as written in the source (including ranges and dashes)

➡️ You will find {len_refs} references in the HTML as `<a class="act">` elements
➡️ The raw href values found are: 
{list_of_refs}

Return the result as a **list of JSON objects** in the format:
{format_instructions}

HTML:
{html}
""")
        ])
        self.parser = PydanticOutputParser(pydantic_object=LegalReferenceList)
        self.chain: Runnable = self.prompt | self.llm | self.parser

    @staticmethod
    def convert_sup_to_unicode(tag):
        for sup in tag.find_all("sup"):
            if sup.string:
                text = str(sup.get_text())
                sup.replace_with(''.join(superscript_map.get(ch) for ch in text.strip()))
            else:
                sup.unwrap()
        return tag

    @staticmethod
    def build_list_of_refs(refs):
        extracted_refs_for_each_document = {}
        for tag in refs:
            tag = LegalUnitAnnotationsLLMRetriever.convert_sup_to_unicode(tag)
            text = re.sub(r'\s+', " ", tag.get_text(strip=True))
            href = tag.get("href", "")

            if not text or text.lower() in {"ustawy"}:
                continue

            # Extract ID from href like "#/document/17030487?unitId=art(16)ust(1)"
            match = re.search(r"#/document/(\d+)", href)
            doc_id = match.group(1) if match else None
            if doc_id is None:
                raise ValueError("Cannot be like this")
            if doc_id in extracted_refs_for_each_document:
                extracted_refs_for_each_document[doc_id].append(text)
            else:
                extracted_refs_for_each_document[doc_id] = [text]
        return extracted_refs_for_each_document

    def retrieve(self, html_element_to_retrieve: Tag) -> LegalReferenceList | None:

        refs = html_element_to_retrieve.find_all("a", class_="act")

        extracted_refs_for_each_document = self.build_list_of_refs(refs)
        if not len(extracted_refs_for_each_document.keys()):
            return None

        list_of_refs = "\n".join([
            f"Document (id = {document_id}):\n" +
            "\n".join([f"{index + 1}. {ref}" for index, ref in enumerate(refs_for_document)])
            for document_id, refs_for_document in extracted_refs_for_each_document.items()
        ])

        try:
            with get_openai_callback() as cb:
                result: LegalReferenceList = self.chain.invoke({
                    "html": html_element_to_retrieve.decode_contents(),
                    "format_instructions": self.parser.get_format_instructions(),
                    "len_refs": len(refs),
                    "list_of_refs": list_of_refs
                })
                get_mongodb_collection(
                    db_name="costs_of_runs",
                    collection_name="dataset_preparations_costs"
                ).update_one(
                    {
                        "name": "creating_annotation_cost",
                        "invoke_id": self.invoke_id
                    },
                    {
                        "$inc": {
                            "prompt_tokens": cb.prompt_tokens,
                            "completion_tokens": cb.completion_tokens,
                            "total_tokens": cb.total_tokens,
                            "total_cost": cb.total_cost
                        }
                    },
                    upsert=True
                )

            if result.items == 0:
                raise ValueError("Length of list of legal_act_references is 0")


            ## TODO i need ask a lawyer regarding that because sometimes in question there is reference to legal act without listing any legal unit
            # if any(len(reference.przepisy) == 0 for reference in result.items):
            #     raise ValueError("Some of the reference have empty list of rules")

            return result
        except ValidationError as e:
            aws_logger.error(f"Error during validation for {html_element_to_retrieve.decode_contents()}")
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