import os
import argparse
from dataclasses import dataclass, field

from sentence_transformers import SentenceTransformerTrainingArguments, SentenceTransformerTrainer, SentenceTransformer, \
    SentenceTransformerModelCardData
from sentence_transformers.losses import MultipleNegativesRankingLoss
from sentence_transformers.training_args import BatchSamplers

from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    set_seed,
    default_data_collator, HfArgumentParser, AutoModel,
)
from datasets import load_from_disk, load_dataset
import torch
from transformers import Trainer, TrainingArguments
import torch.distributed as dist
from torch.distributed.fsdp import FullyShardedDataParallel as FSDP



def safe_save_model_for_hf_trainer(trainer: Trainer, tokenizer: AutoTokenizer, output_dir: str):
    """Helper method to save model for HF Trainer."""
    # see: https://github.com/tatsu-lab/stanford_alpaca/issues/65
    from torch.distributed.fsdp import (
        FullyShardedDataParallel as FSDP,
        FullStateDictConfig,
        StateDictType,
    )

    model = trainer.model
    save_policy = FullStateDictConfig(offload_to_cpu=True, rank0_only=True)
    with FSDP.state_dict_type(model, StateDictType.FULL_STATE_DICT, save_policy):
        cpu_state_dict = model.state_dict()
    if trainer.args.should_save:
        trainer._save(output_dir, state_dict=cpu_state_dict)  # noqa
        tokenizer.save_pretrained(output_dir)


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
        default="google/flan-t5-xl", metadata={"help": "Model ID to use for Embedding training"}
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
    optimizer: str = field(
        default="adamw_hf", metadata={"help": "Learning rate to use for training."}
    )
    seed: int = field(
        default=42, metadata={"help": "Seed to use for training."}
    )
    gradient_checkpointing: bool = field(
        default=True, metadata={"help": "Path to deepspeed config file."}
    )
    fsdp: str = field(
        default=None, metadata={"help": "Whether to use fsdp."}
    )
    fsdp_transformer_layer_cls_to_wrap: str = field(
        default=None, metadata={"help": "Which transformer layer to wrap with fsdp."}
    )


def training_function(args):
    # set seed
    set_seed(args.seed)

    # dataset = load_from_disk(args.dataset_path)
    train_dataset = load_dataset(
        "json",
        data_files=os.path.join(args.train_dataset_path, "dataset.json"),
        split="train",
    )
    test_dataset = load_dataset(
        "json",
        data_files=os.path.join(args.test_dataset_path, "dataset.json"),
        split="train",
    )
    model = SentenceTransformer(
        args.model_id,
        device="cuda",
        model_kwargs={"attn_implementation": "sdpa"},  # needs Ampere GPU or newer
        model_card_data=SentenceTransformerModelCardData(
            language="en",
            license="apache-2.0",
            model_name="BGE base Financial Matryoshka",
        ),
    )

    loss = MultipleNegativesRankingLoss(model)


    # Define training args
    output_dir = "/tmp"
    training_args = SentenceTransformerTrainingArguments(
        # gradient_checkpointing=args.gradient_checkpointing,
        output_dir=output_dir,
        num_train_epochs=args.num_train_epochs,  # number of epochs
        per_device_train_batch_size=args.per_device_train_batch_size,  # training batch size
        per_device_eval_batch_size=args.per_device_eval_batch_size,  # evaluation batch size
        gradient_accumulation_steps=args.gradient_accumulation_steps,  # gradient accumulation steps
        warmup_ratio=0.1,  # warmup ratio
        learning_rate=args.learning_rate,  # learning rate
        lr_scheduler_type="cosine",  # use constant learning rate scheduler
        optim=args.optimizer,  # use fused adamw optimizer
        tf32=True,  # use tf32 precision
        bf16=True,  # use bf16 precision
        batch_sampler=BatchSamplers.NO_DUPLICATES,  # MultipleNegativesRankingLoss benefits from no duplicate samples in a batch
        eval_strategy="epoch",  # evaluate after each epoch
        save_strategy="no",  # save after each epoch
        logging_steps=10,  # log every 10 steps
        metric_for_best_model="eval_dim_128_cosine_ndcg@10",  # Optimizing for the best ndcg@10 score for the 128 dimension
        fsdp=args.fsdp,
        fsdp_transformer_layer_cls_to_wrap=args.fsdp_transformer_layer_cls_to_wrap,
    )


    trainer = SentenceTransformerTrainer(
        model=model,  # bg-base-en-v1
        args=training_args,  # training arguments
        train_dataset=train_dataset.select_columns(
            ["positive", "anchor"]
        ),
        loss=loss
    )

    # ➋ ► DEBUG: verify FSDP wrapping
    if dist.is_available() and dist.is_initialized():
        rank  = dist.get_rank()
        world = dist.get_world_size()
        print(f"[Rank {rank}/{world}]  FSDP active?  {isinstance(trainer.model, dist.fsdp.FullyShardedDataParallel)}")
        shard_params = sum(p.numel() for p in trainer.model.parameters())
        print(f"[Rank {rank}] parameters stored on this GPU: {shard_params/1e6:.2f} M")


    # Start training
    trainer.train()


    print("Training done!")

    # save model and tokenizer for easy inference
    if trainer.is_world_process_zero():        # run only on rank-0
        trainer.save_model("/opt/ml/model")    # writes weights + tokenizer
    # safe_save_model_for_hf_trainer(trainer, tokenizer, "/opt/ml/model/")
    dist.barrier()


def main():
    parser = HfArgumentParser((ScriptArguments))
    script_args = parser.parse_args_into_dataclasses()[0]
    training_function(script_args)


if __name__ == "__main__":
    main()