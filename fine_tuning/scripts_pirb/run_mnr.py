from collections import defaultdict
from dataclasses import dataclass, field
import os
from typing import List

from sentence_transformers import (
    SentenceTransformerModelCardData,
    SentenceTransformer,
    SentenceTransformerTrainer,
    SentenceTransformerTrainingArguments,
)
from sentence_transformers.losses import MatryoshkaLoss, MultipleNegativesRankingLoss
from sentence_transformers.training_args import BatchSamplers
from transformers import set_seed, HfArgumentParser


from sentence_transformers.evaluation import (
    InformationRetrievalEvaluator,
    SequentialEvaluator,
)
from sentence_transformers.util import cos_sim
from datasets import load_dataset, concatenate_datasets


SPECIALS = ["[ARTICLE]", "[SECTION]", "[POINT]"]

@dataclass
class ScriptArguments:
    query_prefix: str = field(
        default="[query]: ",
        metadata={"help": "Prefix for retrieval queries"}
    )
    sts_prefix: str = field(
        default="[sts]: ",
        metadata={"help": "Prefix for STS / symmetric tasks"}
    )
    train_dataset_path: str = field(
        default="/opt/ml/input/data/train/",
        metadata={"help": "Path to the dataset, e.g. /opt/ml/input/data/train/"}
    )
    test_dataset_path: str = field(
        default="/opt/ml/input/data/test/",
        metadata={"help": "Path to the dataset, e.g. /opt/ml/input/data/test/"}
    )
    model_id: str = field(
        default=None, metadata={"help": "Model ID to use for Embedding training"}
    )
    eval_only: bool = field(
        default=False, metadata={"help": "If True, skip training and run only evaluation"}
    )
    chunking_type: str = field(
        default=None, metadata={"help": "Type of chunking"}
    )
    num_train_epochs: int = field(
        default=1, metadata={"help": "Number of training epochs"}
    )
    per_device_train_batch_size: int = field(
        default=32, metadata={"help": "Training batch size"}
    )
    per_device_eval_batch_size: int = field(
        default=16, metadata={"help": "Evaluation batch size"}
    )
    gradient_accumulation_steps: int = field(
        default=16, metadata={"help": "Gradient accumulation steps"}
    )
    learning_rate: float = field(
        default=2e-5, metadata={"help": "Learning rate for the optimizer"}
    )

def build_gold_evaluator(
        train_ds,
        test_ds,
        mode: str = "gold_unit_article_chunking",  # or "gold_article_only"
):
    ## TODO add all chunks from legal acts to corpus
    """
    Parameters
    ----------
    train_ds, test_ds : datasets.Dataset
        Splits returned by build_datasets().
    mode : str
        • "gold_unit_article_chunking"  → Gold-Unit recall counts *all* chunks
          whose `contains_citation == 0`.  Corpus = every unique chunk
          (articles + sections + points).

        • "gold_article_only"           → Recall counts **only** rows that are
          *both* `type=="Article"` and `contains_citation==0`.  Corpus = article
          chunks only.  Queries that cite only §/pkt are ignored.
    emb_dim : int
        The embedding dimension printed in the metric names.
    """
    # ------------------------------------------------------------------
    # 1️⃣  Build CORPUS  (depends on mode)
    # ------------------------------------------------------------------
    corpus = {}
    seen = set()
    for row in concatenate_datasets([train_ds, test_ds]):
        if mode == "gold_article_only" and row["type"] != "Article_Span":
            continue  # skip §/pkt chunks in corpus
        cid = row["id_positive"]
        if cid not in seen:
            corpus[cid] = row["positive"]
            seen.add(cid)

    # ------------------------------------------------------------------
    # 2️⃣  Build QUERIES  (first anchor per id_query from test_ds)
    # ------------------------------------------------------------------
    queries = {}
    for row in test_ds:
        qid, anchor = row["id_query"], row["anchor"]
        queries.setdefault(qid, anchor)  # keep first (identical) anchor

    # ------------------------------------------------------------------
    # 3️⃣  Build RELEVANT_DOCS  according to mode
    # ------------------------------------------------------------------
    rel = defaultdict(list)

    for row in test_ds:
        if mode == "gold_article_only" and row["type"] != "Article_Span":
            # skip §/pkt rows in strict article-only metric
            continue

        rel[row["id_query"]].append(row["id_positive"])

    # remove queries that ended up empty (only happens in article-only mode)
    relevant_docs = {
        q: list(set(cids))
        for q, cids in rel.items() if cids
    }

    # ------------------------------------------------------------------
    # 4️⃣  Build evaluator
    # ------------------------------------------------------------------
    name = "gold_article" if mode == "gold_article_only" else "gold_unit"
    return InformationRetrievalEvaluator(
        queries=queries,
        corpus=corpus,
        relevant_docs=relevant_docs,
        name=f"{name}_dim_{1024}",
        score_functions={"cosine": cos_sim},
    )


def build_relevant_chunk_evaluator(train_ds,
                                   test_ds,
                                   chunking_mode: str = "article_based_chunking" # or "legal_unit_chunking"
                                   ):


    # ------------------------------------------------------------------
    # 1️⃣  Build CORPUS  (depends on mode)
    # ------------------------------------------------------------------
    corpus = {}
    seen = set()
    for row in concatenate_datasets([train_ds, test_ds]):

        if chunking_mode == "legal_unit_chunking" and row["contains_citation"] == 1:
            continue  # skip article based chunks for it
        if chunking_mode == "article_based_chunking" and row["type"] != "Article_Span":
            continue  # skip §/pkt chunks in corpus
        cid = row["id_positive"]
        if cid not in seen:
            corpus[cid] = row["positive"]
            seen.add(cid)

    # ------------------------------------------------------------------ #
    # 2️⃣  QUERIES  (first anchor per id_query from test split)
    # ------------------------------------------------------------------ #
    queries = {}
    for row in test_ds:
        queries.setdefault(row["id_query"], row["anchor"])

    # ------------------------------------------------------------------ #
    # 3️⃣  RELEVANT_DOCS according to the mode
    # ------------------------------------------------------------------ #
    rel = defaultdict(list)

    for row in test_ds:
        if chunking_mode == "article_based_chunking":
            # parent articles that contain the cited unit
            if row["type"] == "Article_Span":
                rel[row["id_query"]].append(row["id_positive"])
        elif chunking_mode == "legal_unit_chunking":
            # exact units (article OR section / point)
            if row["contains_citation"] == 0:
                rel[row["id_query"]].append(row["id_positive"])

    relevant_docs = {q: list(set(cids)) for q, cids in rel.items() if cids}

    # ------------------------------------------------------------------ #
    # 4.  Instantiate the evaluator
    # ------------------------------------------------------------------ #
    return InformationRetrievalEvaluator(
        queries        = queries,
        corpus         = corpus,
        relevant_docs  = relevant_docs,
        name           = f"relevant_chunk_dim_{1024}",
        score_functions={"cosine": cos_sim},
    )




def create_evaluator(
        train_dataset, test_dataset, chunking_type
):
    if chunking_type == "article_based_chunking":
        eval_gold_article = build_gold_evaluator(
            train_dataset, test_dataset, mode="gold_article_only"
        )

        eval_gold_unit = build_gold_evaluator(
            train_dataset, test_dataset, mode="gold_unit_for_article_span_chunking"
        )
        contain_eval = build_relevant_chunk_evaluator(train_dataset, test_dataset,
                                                      chunking_mode=chunking_type)
        return SequentialEvaluator([eval_gold_article, eval_gold_unit, contain_eval])
    elif chunking_type == "legal_unit_chunking":
        gold_unit_eval = build_relevant_chunk_evaluator(train_dataset, test_dataset,
                                                        chunking_mode=chunking_type)

        return SequentialEvaluator([gold_unit_eval])
    raise ValueError(f"Invalid chunking type: {chunking_type}")


def training_function(script_args):
    ################
    # Dataset
    ################

    train_dataset = load_dataset(
        "json",
        data_files=os.path.join(script_args.train_dataset_path, "dataset.json"),
        split="train",
    )
    test_dataset = load_dataset(
        "json",
        data_files=os.path.join(script_args.test_dataset_path, "dataset.json"),
        split="train",
    )

    ###################
    # Model & Evaluator
    ###################


    model = SentenceTransformer(
        script_args.model_id,
        trust_remote_code=True,                                 # ← MUST
        device="cuda",
        model_kwargs={
            "attn_implementation": "flash_attention_2",         # ← FA-2
            "trust_remote_code": True
        },
    )
    model.bfloat16()     # flash-attn works only in fp16/bf16

    if script_args.chunking_type == "legal_unit_chunking":
        tr = model._first_module()
        tok = tr.tokenizer
        to_add = [t for t in SPECIALS if t not in tok.get_vocab()]
        print(f"There are tokens to add: {to_add}")
        if to_add:
            tok.add_special_tokens({"additional_special_tokens": to_add})
            tr.auto_model.resize_token_embeddings(len(tok))  # must resize after adding



    evaluator = create_evaluator(
        train_dataset, test_dataset, script_args.chunking_type
    )

    if script_args.eval_only:
        # only evaluation
        res = evaluator(model)  # writes results.json too
        # Be robust across ST versions:
        print(res)
        if isinstance(res, tuple):
            main_score, metrics = res
        elif isinstance(res, dict):
            main_score, metrics = None, res
        else:  # float or something else
            main_score, metrics = res, {}
        print("== EVAL RESULTS ==")
        if metrics:
            for k, v in sorted(metrics.items()):
                print(f"{k}: {v}")
        else:
            print("main_score:", main_score)
        return

    ###################
    # Loss Function
    ###################

    # create Matryoshka loss function with MultipleNegativesRankingLoss
    train_loss = MultipleNegativesRankingLoss(model)

    ################
    # Training
    ################
    training_args = SentenceTransformerTrainingArguments(
        output_dir="/opt/ml/model",  # output directory for sagemaker to upload to s3
        num_train_epochs=script_args.num_train_epochs,  # number of epochs
        per_device_train_batch_size=script_args.per_device_train_batch_size,  # training batch size
        per_device_eval_batch_size=script_args.per_device_eval_batch_size,  # evaluation batch size
        gradient_accumulation_steps=script_args.gradient_accumulation_steps,  # gradient accumulation steps
        warmup_ratio=0.1,  # warmup ratio
        learning_rate=script_args.learning_rate,  # learning rate
        lr_scheduler_type="cosine",  # use constant learning rate scheduler
        optim="adamw_torch_fused",  # use fused adamw optimizer
        tf32=True,  # use tf32 precision
        bf16=True,  # use bf16 precision
        batch_sampler=BatchSamplers.NO_DUPLICATES,  # MultipleNegativesRankingLoss benefits from no duplicate samples in a batch
        eval_strategy="epoch",  # evaluate after each epoch
        save_strategy="epoch",  # save after each epoch
        logging_steps=10,  # log every 10 steps
        save_total_limit=3,  # save only the last 3 models
        load_best_model_at_end=True,  # load the best model when training ends
        metric_for_best_model="eval_relevant_chunk_dim_1024_cosine_recall@10",  # Optimizing for the best ndcg@10 score for the 128 dimension
    )

    trainer = SentenceTransformerTrainer(
        model=model,  # bg-base-en-v1
        args=training_args,  # training arguments
        train_dataset=train_dataset.select_columns(
            ["positive", "anchor"]
        ),  # training dataset
        loss=train_loss,
        evaluator=evaluator,
    )

    ##########################
    # Train model
    ##########################
    # start training, the model will be automatically saved to the hub and the output directory
    trainer.train()

    # save the best model
    trainer.save_model()


if __name__ == "__main__":
    parser = HfArgumentParser((ScriptArguments))
    script_args = parser.parse_args_into_dataclasses()[0]

    # set seed
    set_seed(42)

    # launch training
    training_function(script_args)