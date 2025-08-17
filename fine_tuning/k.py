from pathlib import Path
from dataclasses import dataclass, field
import os
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


@dataclass
class ScriptArguments:
    train_dataset_path: str = field(
        default="/opt/ml/input/data/train/",
        metadata={"help": "Path to the dataset, e.g. /opt/ml/input/data/train/"},
    )
    test_dataset_path: str = field(
        default="/opt/ml/input/data/test/",
        metadata={"help": "Path to the dataset, e.g. /opt/ml/input/data/test/"},
    )
    model_id: str = field(
        default=None, metadata={"help": "Model ID to use for Embedding training"}
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


def create_evaluator(
        train_dataset, test_dataset, matryoshka_dimensions=[768, 512, 256, 128, 64]
):
    corpus_dataset = concatenate_datasets([train_dataset, test_dataset])

    # Convert the datasets to dictionaries
    corpus = dict(
        zip(corpus_dataset["id"], corpus_dataset["positive"])
    )  # Our corpus (cid => document)
    queries = dict(
        zip(test_dataset["id"], test_dataset["anchor"])
    )  # Our queries (qid => question)

    # Create a mapping of relevant document (1 in our case) for each query
    relevant_docs = {}  # Query ID to relevant documents (qid => set([relevant_cids])
    for q_id in queries:
        relevant_docs[q_id] = [q_id]

    matryoshka_evaluators = []
    # Iterate over the different dimensions
    for dim in matryoshka_dimensions:
        ir_evaluator = InformationRetrievalEvaluator(
            queries=queries,
            corpus=corpus,
            relevant_docs=relevant_docs,
            name=f"dim_{dim}",
            truncate_dim=dim,  # Truncate the embeddings to a certain dimension
            score_functions={"cosine": cos_sim},
        )
        matryoshka_evaluators.append(ir_evaluator)

    # Create a sequential evaluator
    return SequentialEvaluator(matryoshka_evaluators)


def training_function(script_args, device_type: str = "cuda", model_args_dir: str = "/opt/ml/model"):
    ################
    # Dataset
    ################

    w = os.path.normpath(os.path.join(script_args.train_dataset_path, "dataset.json"))

    script_args.train_dataset_path = "C:\\Users\\karol\\Desktop\\python-projects\\ai_lawyer_project\\fine_tuning\\datasets\\test-embedding\\train\\dataset.json"
    script_args.test_dataset_path = "C:\\Users\\karol\\Desktop\\python-projects\\ai_lawyer_project\\fine_tuning\\datasets\\test-embedding\\test\\dataset.json"

    # train_dataset = load_dataset(
    #     "json",
    #     data_files=os.path.join(script_args.train_dataset_path, "dataset.json"),
    #     split="train",
    # )
    train_dataset = load_dataset(
        "json",
        data_files=script_args.train_dataset_path,
        split="train",
    )


    # test_dataset = load_dataset(
    #     "json",
    #     data_files=os.path.join(script_args.test_dataset_path, "dataset.json"),
    #     split="train",
    # )
    test_dataset = load_dataset(
        "json",
        data_files=script_args.test_dataset_path,
        split="train",
    )

    ###################
    # Model & Evaluator
    ###################

    matryoshka_dimensions = [768, 512, 256, 128, 64]  # Important: large to small

    model = SentenceTransformer(
        script_args.model_id,
        device=device_type,
        model_kwargs={"attn_implementation": "sdpa"},  # needs Ampere GPU or newer
        model_card_data=SentenceTransformerModelCardData(
            language="en",
            license="apache-2.0",
            model_name="BGE base Financial Matryoshka",
        ),
    )
    evaluator = create_evaluator(
        train_dataset, test_dataset, matryoshka_dimensions=matryoshka_dimensions
    )

    ###################
    # Loss Function
    ###################

    # create Matryoshka loss function with MultipleNegativesRankingLoss
    inner_train_loss = MultipleNegativesRankingLoss(model)
    train_loss = MatryoshkaLoss(
        model, inner_train_loss, matryoshka_dims=matryoshka_dimensions
    )

    ################
    # Training
    ################
    training_args = SentenceTransformerTrainingArguments(
        output_dir=model_args_dir,  # output directory for sagemaker to upload to s3
        num_train_epochs=script_args.num_train_epochs,  # number of epochs
        per_device_train_batch_size=script_args.per_device_train_batch_size,  # training batch size
        per_device_eval_batch_size=script_args.per_device_eval_batch_size,  # evaluation batch size
        gradient_accumulation_steps=script_args.gradient_accumulation_steps,  # gradient accumulation steps
        warmup_ratio=0.1,  # warmup ratio
        learning_rate=script_args.learning_rate,  # learning rate
        lr_scheduler_type="cosine",  # use constant learning rate scheduler
        optim="adamw_torch_fused",  # use fused adamw optimizer
        tf32=False,  # use tf32 precision, TF32 is an accelerated matrix multiplication mode introduced on NVIDIA Ampere GPUs.
        bf16=False,  # use bf16 precision
        batch_sampler=BatchSamplers.NO_DUPLICATES,  # MultipleNegativesRankingLoss benefits from no duplicate samples in a batch
        eval_strategy="epoch",  # evaluate after each epoch
        save_strategy="epoch",  # save after each epoch
        logging_steps=4,  # log every 10 steps
        save_total_limit=3,  # save only the last 3 models
        load_best_model_at_end=True,  # load the best model when training ends
        metric_for_best_model="eval_dim_128_cosine_ndcg@10",  # Optimizing for the best ndcg@10 score for the 128 dimension

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

import sagemaker
import boto3
sess = sagemaker.Session()


def get_training_arguments(local: bool = True):
    if local:
        return {
            "model_id": "BAAI/bge-base-en-v1.5", # model id from the hub
            "train_dataset_path": os.path.join( 'datasets', 'test-embedding', 'data', 'train'), # path inside the container where the training data is stored
            "test_dataset_path": os.path.join( 'datasets', 'test-embedding', 'data', 'test'), # path inside the container where the test data is stored
            "num_train_epochs": 3, # number of training epochs
            "learning_rate": 2e-5, # learning rate
            "per_device_train_batch_size": 8,
            "per_device_eval_batch_size": 4,
            "gradient_accumulation_steps": 8,
        }
    else:
        return {
            "model_id": "BAAI/bge-base-en-v1.5", # model id from the hub
            "train_dataset_path": "/opt/ml/input/data/train/", # path inside the container where the training data is stored
            "test_dataset_path": "/opt/ml/input/data/test/", # path inside the container where the test data is stored
            "num_train_epochs": 3, # number of training epochs
            "learning_rate": 2e-5, # learning rate
        }

sagemaker_session_bucket=None
if sagemaker_session_bucket is None and sess is not None:
    # set to default bucket if a bucket name is not given
    sagemaker_session_bucket = sess.default_bucket()

try:
    role = sagemaker.get_execution_role()
except ValueError:
    iam = boto3.client('iam')
    role = iam.get_role(RoleName='sagemaker_execution_role')['Role']['Arn']

sess = sagemaker.Session(default_bucket=sagemaker_session_bucket)

print(f"sagemaker role arn: {role}")
print(f"sagemaker bucket: {sess.default_bucket()}")
print(f"sagemaker session region: {sess.boto_region_name}")

train_args = get_training_arguments()
w = 4


path = "C:\\Users\\karol\\Desktop\\python-projects\\ai_lawyer_project\\fine_tuning\\datasets\\test-embedding\\train\\dataset.json"
print("✔️ Plik istnieje" if os.path.isfile(path) else "❌ Plik NIE istnieje")

script_args = ScriptArguments(**train_args)
script_args.train_dataset_path = '/datasets/test-embedding/data/train/dataset.json'
dataset_path = Path(script_args.train_dataset_path) / "dataset.json"
print(script_args.train_dataset_path)
training_function(script_args, device_type="cuda", model_args_dir="\\model")