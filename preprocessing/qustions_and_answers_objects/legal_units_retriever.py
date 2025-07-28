import numpy as np
from langchain_core.embeddings import Embeddings

from preprocessing.legal_acts.legal_act_document import LegalActDocument
from sklearn.metrics.pairwise import cosine_similarity
from preprocessing.qustions_and_answers_objects.legal_act_dataset_with_title_embedding import \
    LegalActDatasetWithTitleEmbedding
from preprocessing.qustions_and_answers_objects.legal_act_title_embedding_pair import LegalActTitleEmbeddingPair
from preprocessing.qustions_and_answers_objects.llm_legal_annotations import LegalActReference


class LegalActRetrievingException(Exception):
    pass


class LegalUnitsRetriever:

    def __init__(self, legal_act_dataset_with_title_embedding: LegalActDatasetWithTitleEmbedding):
        self.legal_act_dataset_with_title_embedding = legal_act_dataset_with_title_embedding

    def _get_legal_document_with_articles_using_title_compare(self, document_from_llm: LegalActReference) -> LegalActDocument | None:
        def normalize_title(text: str):
            text = text.strip().lower()
            return text if text.endswith('.') else text + '.'

        for index, document_pair in self.legal_act_dataset_with_title_embedding.documents_from_db.items():
            if normalize_title(document_pair.legal_act.legal_act_name) == normalize_title(document_from_llm.tytul.strip().lower()):
                return document_pair.legal_act if len(document_pair.legal_act.articles) > 0 \
                    else document_pair.legal_act.lazy_loading_articles()
        return None

    def _get_legal_document_with_articles_using_embedding_model(self, document_from_llm: LegalActReference) -> LegalActDocument:
        llm_embedding = self.legal_act_dataset_with_title_embedding.embeddings_model.embed_query(document_from_llm.tytul.lower())
        similarities = cosine_similarity([llm_embedding],
                                         [document_pair.title_embedding for index, document_pair in
                                          self.legal_act_dataset_with_title_embedding.documents_from_db.items()])
        best_index = int(np.argmax(similarities))
        best_score = similarities[0][best_index]
        best_match = self.legal_act_dataset_with_title_embedding.documents_from_db[best_index].legal_act.legal_act_name
        if best_score < 0.87:
            raise LegalActRetrievingException(f"There isn't {document_from_llm.tytul} in database")
        return self.legal_act_dataset_with_title_embedding.documents_from_db[best_index].legal_act \
            if len(self.legal_act_dataset_with_title_embedding.documents_from_db[best_index].legal_act.articles) > 0 else \
            self.legal_act_dataset_with_title_embedding.documents_from_db[best_index].legal_act.lazy_loading_articles()

    def get_legal_act_document(self, document_from_llm: LegalActReference) -> LegalActDocument:
        legal_document_using_title_compare = self._get_legal_document_with_articles_using_title_compare(
            document_from_llm)
        if legal_document_using_title_compare:
            return legal_document_using_title_compare

        return self._get_legal_document_with_articles_using_embedding_model(document_from_llm)