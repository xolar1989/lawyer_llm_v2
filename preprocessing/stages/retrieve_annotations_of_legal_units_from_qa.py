import traceback
from typing import Dict

import pandas as pd
from bs4 import BeautifulSoup
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from tqdm import tqdm

from preprocessing.dask.fargate_dask_cluster import retry_dask_task
from preprocessing.mongo_db.mongodb import get_mongodb_collection
from preprocessing.qustions_and_answers_objects.legal_act_dataset_with_title_embedding import \
    LegalActDatasetWithTitleEmbedding
from preprocessing.qustions_and_answers_objects.legal_question_with_annotations import LegalQuestionWithAnnotations
from preprocessing.qustions_and_answers_objects.legal_unit_annotation_factory import LegalUnitAnnotationFactory
from preprocessing.qustions_and_answers_objects.legal_unit_annotations_llm_retriever import \
    LegalUnitAnnotationsLLMRetriever
from preprocessing.qustions_and_answers_objects.legal_units_retriever import LegalUnitsRetriever, \
    LegalActRetrievingException
from preprocessing.qustions_and_answers_objects.llm_legal_annotations import LegalReferenceList
from preprocessing.qustions_and_answers_objects.question_with_html import QuestionWithHtml
from preprocessing.utils.defaults import AWS_REGION
from preprocessing.utils.general import get_secret
from preprocessing.utils.stage_def import FlowStep
from preprocessing.logging.aws_logger import aws_logger
from dask.distributed import Client, as_completed
import dask.dataframe as dd
from dask import delayed


class RetrieveAnnotationsOfLegalUnitsFromQA(FlowStep):

    @classmethod
    def build_legal_annotations(cls, reference_list: LegalReferenceList, legal_units_retriever: LegalUnitsRetriever):
        legal_annotations = []

        for document_from_llm in reference_list.items:
            # document_from_llm.tytul += "d"
            legal_act_document = legal_units_retriever.get_legal_act_document(document_from_llm)
            legal_unit_factory = LegalUnitAnnotationFactory(legal_act_document)
            for reference in document_from_llm.przepisy:
                legal_annotation = legal_unit_factory.create_legal_unit_annotation(reference)
                legal_annotations.append(legal_annotation)
        return legal_annotations

    @classmethod
    def get_justification_div(cls, question_with_html: QuestionWithHtml):
        soup_for_justification = BeautifulSoup(question_with_html.answer_content, "html.parser")
        justification_div = soup_for_justification.find("div", class_="qa_a-just")
        return justification_div if justification_div is not None else soup_for_justification.find("div", class_="qa_a-cont")

    @classmethod
    def worker_task(cls, question_with_html: QuestionWithHtml, invoke_id: str):
        try:
            legal_act_dataset = LegalActDatasetWithTitleEmbedding.from_db(
                embeddings_model=OpenAIEmbeddings(model="text-embedding-3-large",
                                                  api_key=
                                                  get_secret("OpenAiApiKey", AWS_REGION)[
                                                      "OPEN_API_KEY"]),
                invoke_id=invoke_id
            )

            legal_units_retriever = LegalUnitsRetriever(legal_act_dataset)

            html_annotation_retriever = LegalUnitAnnotationsLLMRetriever(
                ChatOpenAI(model="gpt-4o", temperature=0, api_key=get_secret("OpenAiApiKey", AWS_REGION)["OPEN_API_KEY"]), invoke_id=invoke_id)

            justification_div = cls.get_justification_div(question_with_html)
            question_div = BeautifulSoup(question_with_html.question_content, "html.parser").find("div")

            reference_list_for_justification = html_annotation_retriever.retrieve(justification_div)

            reference_list_for_question = html_annotation_retriever.retrieve(question_div)

            legal_annotations_for_justification = cls.build_legal_annotations(reference_list_for_justification,
                                                                          legal_units_retriever) \
                if reference_list_for_justification else []

            legal_annotations_for_question = cls.build_legal_annotations(reference_list_for_question, legal_units_retriever) \
                if reference_list_for_question else []

            question_with_annotations = LegalQuestionWithAnnotations(
                nro=question_with_html.nro,
                invoke_id=invoke_id,
                title=question_with_html.title,
                question_content=question_with_html.question_content,
                answer_content=question_with_html.answer_content,
                question_annotations=legal_annotations_for_question,
                answer_annotations=legal_annotations_for_justification
            )

            get_mongodb_collection(
                db_name="preparing_dataset_for_embedding",
                collection_name="question_with_annotations"
            ).insert_one(question_with_annotations.to_dict())
        except Exception as e:
            format_error = traceback.format_exc()
            try:
                get_mongodb_collection(
                    db_name="preparing_dataset_for_embedding",
                    collection_name="creating_annotations_for_question_error"
                ).update_one(
                    {
                        "nro": question_with_html.nro,
                        "invoke_id": invoke_id
                    },
                    {
                        "$set":{
                            "nro": question_with_html.nro,
                            "invoke_id": invoke_id,
                            "error_msg": str(e),
                            "traceback": format_error,
                            "type": e.__class__.__name__
                        }
                    },
                    upsert=True
                )
            except Exception as ew:
                aws_logger.error("Failed to log error to MongoDB", exc_info=True)
                pass
            return {
                'nro': question_with_html.nro,
                'invoke_id': invoke_id,
                'status': 'failed'
            }

        return {
            'nro': question_with_html.nro,
            'invoke_id': invoke_id,
            'status': 'success'
        }

    @classmethod
    @FlowStep.step(task_run_name='retrieve_annotations_of_legal_units')
    def run(cls, flow_information: dict, dask_client: Client, workers_count: int):

        invoke_id = "72f213ee-7227-4e99-96f0-63a5766ed1d8"
        df = pd.DataFrame(
            list(get_mongodb_collection(
                db_name="scraping_lex",
                collection_name="questions_with_html"
            ).find_many({}, {"_id": 0}))
        )



        existing_nro_set = set(
            doc["nro"] for doc in get_mongodb_collection(
                db_name="preparing_dataset_for_embedding",
                collection_name="question_with_annotations"
            ).find_many({}, {"nro": 1, "_id": 0})
        )


        ddf_eli_documents = dd.from_pandas(df, npartitions=workers_count)


        ddf_filtered = ddf_eli_documents[~ddf_eli_documents["nro"].isin(existing_nro_set)]

        # w = ddf_filtered.compute()
        # selected_ddf = ddf_eli_documents[
        #     ddf_eli_documents["nro"] == 622784302].compute()
        # r = cls.worker_task(question_with_html=QuestionWithHtml.from_dict(selected_ddf.iloc[0].to_dict()), invoke_id=invoke_id)



        delayed_tasks = ddf_filtered.map_partitions(
            lambda df: [
                delayed(cls.worker_task)(
                    question_with_html=QuestionWithHtml.from_dict(question_dict),
                    invoke_id=invoke_id
                )
                for question_dict in df.to_dict(orient='records')
            ]
        ).compute()

        flat_tasks = [task for sublist in delayed_tasks for task in sublist]

        futures = dask_client.compute(flat_tasks, sync=False)

        results = []
        for future in tqdm(as_completed(futures), total=len(futures), desc=f"Downloading pdfs", unit="document",
                           ncols=100):
            result = future.result()  # Get the result of the completed task
            results.append(result)

        # Log worker information
        for future in futures:
            who_has = dask_client.who_has(future)
            aws_logger.info(f"Task {future.key} executed on workers: {who_has}")

        successful_rows = [r for r in results if r['status'] == 'success']
        failed_rows = [r for r in results if r['status'] == 'failed']

        successful_df = pd.DataFrame(successful_rows).drop(columns=['status'])
        failed_df = pd.DataFrame(failed_rows).drop(columns=['status'])

        successful_result_ddf = dd.from_pandas(successful_df, npartitions=workers_count)
        failed_result_ddf = dd.from_pandas(failed_df, npartitions=workers_count)

        return cls.save_result_to_datalake(successful_result_ddf, flow_information, cls,
                                           result_name="successful_results"), \
            cls.save_result_to_datalake(failed_result_ddf, flow_information, cls, result_name="failed_results")
