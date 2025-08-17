from sentence_transformers import SentenceTransformer
from transformers import AutoTokenizer, AutoModel
from sentence_transformers.util import cos_sim

model_id = "BAAI/bge-base-en-v1.5"

tokenizer = AutoTokenizer.from_pretrained(model_id)
model = AutoModel.from_pretrained(model_id)


tok = AutoTokenizer.from_pretrained("sdadas/mmlw-retrieval-roberta-large-v2")

text= "www"
length = len(tok.encode(text, add_special_tokens=False))

print(model)


model = SentenceTransformer(
    "sdadas/mmlw-retrieval-roberta-large-v2",
    trust_remote_code=True,
    device="cuda",
    # model_kwargs={"attn_implementation": "sdpa"},  # needs Ampere GPU or newer
    model_kwargs={"attn_implementation": "flash_attention_2", "trust_remote_code": True}
)
# Flash-Attention works only in 16-bit mode, so we need to cast the model to float16 or bfloat16
model.bfloat16()
print(model)

query_prefix = "[query]: "
queries = [query_prefix + "Jak dożyć 100 lat?"]


tok = AutoTokenizer.from_pretrained("sdadas/mmlw-retrieval-roberta-large-v2")

text= "www"
length_with_prefix = len(tok.encode(queries[0], add_special_tokens=False))
length_without_prefix = len(tok.encode("Jak dożyć 100 lat?", add_special_tokens=False))

length_with_prefix_spec = len(tok.encode(queries[0], add_special_tokens=True))
length_without_prefix_spec = len(tok.encode("Jak dożyć 100 lat?", add_special_tokens=True))



answers = [
    "Trzeba zdrowo się odżywiać i uprawiać sport.",
    "Trzeba pić alkohol, imprezować i jeździć szybkimi autami.",
    "Gdy trwała kampania politycy zapewniali, że rozprawią się z zakazem niedzielnego handlu."
]
queries_emb = model.encode(queries, convert_to_tensor=True, show_progress_bar=False)
answers_emb = model.encode(answers, convert_to_tensor=True, show_progress_bar=False)
best_answer = cos_sim(queries_emb, answers_emb).argmax().item()
print(answers[best_answer])


print(model[0].auto_model.encoder.layer)  # List of transformer layers
