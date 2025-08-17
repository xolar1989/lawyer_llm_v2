import pandas as pd
from pandas import CategoricalDtype

from preprocessing.mongo_db.mongodb import get_mongodb_collection
from preprocessing.utils.defaults import DATALAKE_BUCKET, DAG_TABLE_ID, AWS_REGION
from preprocessing.utils.dynamodb_helper import fetch_segment_data, meta_DULegalDocumentsMetaData
from preprocessing.utils.stage_def import FlowStep
from dask.distributed import Client, as_completed
import dask.dataframe as dd

class ExplodeQuestionToQuestionChunkPair(FlowStep):

    meta_scanned = pd.DataFrame({
        "nro": pd.Series(dtype="Int64"),
        "question_text": pd.Series(dtype="string"),
        "article_span_chunks": pd.Series(dtype="object"),
        "article_chunks":      pd.Series(dtype="object"),
        "section_chunks":      pd.Series(dtype="object"),
        "point_chunks":        pd.Series(dtype="object"),
    })

    TYPE_DTYPE = CategoricalDtype(categories=["Article_Span", "Article", "Section", "Point"], ordered=False)


    meta_exploded = pd.DataFrame({
        "id_query": pd.Series(dtype=object),
        "anchor": pd.Series(dtype=object),
        "id_positive": pd.Series(dtype=object),
        "positive": pd.Series(dtype=object),
        "type": pd.Series(dtype=TYPE_DTYPE),
        "contains_citation": pd.Series(dtype="int8"),
        "ELI": pd.Series(dtype=object),
        "article_id": pd.Series(dtype=object),
        "section_id": pd.Series(dtype=object),
        "subpoint_id": pd.Series(dtype=object),
    })

    @classmethod
    def fetch_mongo_segment(cls, df_partition: pd.DataFrame,
                            total_segments: int,
                            columns_order,
                            dtypes,
                            projection=None,
                            batch_size: int = 500) -> pd.DataFrame:
        if df_partition.empty:
            return cls.meta_scanned.iloc[:0].copy()

        seg = int(df_partition["segment"].iloc[0])
        coll = get_mongodb_collection(
            db_name="datasets",
            collection_name="question_with_annotation_chunks"
        )

        cur = coll.find({"nro": {"$mod": [total_segments, seg]}}, projection).batch_size(batch_size)
        rows = list(cur)
        if not rows:
            return cls.meta_scanned.iloc[:0].copy()

        df = pd.DataFrame(rows)

        # Fast path: assume schema is always consistent
        df = df[columns_order]              # select + order columns
        df = df.astype(dtypes, errors="ignore")  # enforce dtypes
        return df

    @classmethod
    def explode_question_partition(cls, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return cls.meta_exploded.iloc[:0].copy()

        recs = []
        for _, row in df.iterrows():
            qid = str(row["nro"])  # id_query = nro as string
            anchor = f"[query]: {row['question_text']}"

            for out_type, contains_cit, chunks in (
                    ("Article_Span", 1, row["article_span_chunks"]),  # spans -> Article + contains_citation=1
                    ("Article", 0, row["article_chunks"]),
                    ("Section", 0, row["section_chunks"]),
                    ("Point",   0, row["point_chunks"]),
            ):
                if not chunks:
                    continue
                for c in chunks:  # each c is a dict from Mongo
                    recs.append({
                        "id_query": qid,
                        "anchor": anchor,
                        "id_positive": c["chunk_id"],            # use chunk_id directly
                        "positive": c.get("text", ""),
                        "type": out_type,
                        "contains_citation": contains_cit,
                        "ELI": c.get("ELI"),
                        "article_id": c.get("article_id"),
                        "section_id": c.get("section_id"),
                        "subpoint_id": c.get("subpoint_id"),
                    })

        if not recs:
            return cls.meta_exploded.iloc[:0].copy()

        out = pd.DataFrame.from_records(recs).reindex(columns=cls.meta_exploded.columns)
        out["type"] = out["type"].astype(cls.TYPE_DTYPE)
        out["contains_citation"] = out["contains_citation"].astype("int8", copy=False)
        return out

    @classmethod
    @FlowStep.step(task_run_name='explode_question_to_question_chunk_pair')
    def run(cls, flow_information: dict, invoke_id: str, dask_client: Client, workers_count: int):
        get_mongodb_collection(
            db_name="datasets",
            collection_name="question_with_annotation_chunks"
        )
        columns_order_scanned = cls.meta_scanned.columns.tolist()
        dtypes_scanned = cls.meta_scanned.dtypes.to_dict()
        segments = list(range(workers_count))
        ddf_segments = dd.from_pandas(pd.DataFrame({"segment": segments}), npartitions=workers_count)
        mongodb_ddf = ddf_segments.map_partitions(
            lambda df: cls.fetch_mongo_segment(
                df_partition=df,
                total_segments=workers_count,
                columns_order=columns_order_scanned,
                dtypes=dtypes_scanned,
                projection={
                    "_id": 0,
                    "nro": 1,
                    "question_text": 1,
                    "article_span_chunks": 1,
                    "article_chunks": 1,
                    "section_chunks": 1,
                    "point_chunks": 1,
                },
            ),
            meta=cls.meta_scanned,
        )


        # rr = mongodb_ddf.compute()

        ddf_exploaded = mongodb_ddf.map_partitions(cls.explode_question_partition, meta=cls.meta_exploded)


        # wwwww= ddf_exploaded.compute()
        #
        # rrrrrr = ddf_exploaded[ddf_exploaded["type"] == "Article_Span"].compute()
        # rrrrrr_art = ddf_exploaded[ddf_exploaded["type"] == "Article"].compute()
        # rrrrrr_section = ddf_exploaded[ddf_exploaded["type"] == "Section"].compute()
        # rrrrrr_point = ddf_exploaded[ddf_exploaded["type"] == "Point"].compute()

        return cls.save_result_to_datalake(ddf_exploaded, flow_information, cls)
