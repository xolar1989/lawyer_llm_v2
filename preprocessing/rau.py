{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "metadata": {
    "collapsed": true,
    "ExecuteTime": {
     "start_time": "2024-12-09T17:21:10.615997Z",
     "end_time": "2024-12-09T17:21:11.389452Z"
    }
   },
   "outputs": [],
   "source": [
    "from huggingface_hub import hf_hub_download\n",
    "import re\n",
    "from PIL import Image"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "outputs": [],
   "source": [
    "from transformers import NougatProcessor, VisionEncoderDecoderModel\n",
    "# from datasets import load_dataset\n",
    "import torch"
   ],
   "metadata": {
    "collapsed": false,
    "ExecuteTime": {
     "start_time": "2024-12-09T17:24:49.549533Z",
     "end_time": "2024-12-09T17:25:07.626959Z"
    }
   }
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "outputs": [
    {
     "data": {
      "text/plain": "preprocessor_config.json:   0%|          | 0.00/479 [00:00<?, ?B/s]",
      "application/vnd.jupyter.widget-view+json": {
       "version_major": 2,
       "version_minor": 0,
       "model_id": "ba10f8a17f2f4ddcbe2dbc430115cada"
      }
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "C:\\Users\\karol\\Desktop\\python-projects\\pytorch-start\\env\\Lib\\site-packages\\huggingface_hub\\file_download.py:159: UserWarning: `huggingface_hub` cache-system uses symlinks by default to efficiently store duplicated files but your machine does not support them in C:\\Users\\karol\\.cache\\huggingface\\hub\\models--facebook--nougat-base. Caching files will still work but in a degraded version that might require more space on your disk. This warning can be disabled by setting the `HF_HUB_DISABLE_SYMLINKS_WARNING` environment variable. For more details, see https://huggingface.co/docs/huggingface_hub/how-to-cache#limitations.\n",
      "To support symlinks on Windows, you either need to activate Developer Mode or to run Python as an administrator. In order to see activate developer mode, see this article: https://docs.microsoft.com/en-us/windows/apps/get-started/enable-your-device-for-development\n",
      "  warnings.warn(message)\n"
     ]
    },
    {
     "data": {
      "text/plain": "tokenizer_config.json:   0%|          | 0.00/4.49k [00:00<?, ?B/s]",
      "application/vnd.jupyter.widget-view+json": {
       "version_major": 2,
       "version_minor": 0,
       "model_id": "17f0bbfd32714288804f5fcea1d9a742"
      }
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/plain": "tokenizer.json:   0%|          | 0.00/2.14M [00:00<?, ?B/s]",
      "application/vnd.jupyter.widget-view+json": {
       "version_major": 2,
       "version_minor": 0,
       "model_id": "f33336af4a95485b831865192e9f5499"
      }
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/plain": "special_tokens_map.json:   0%|          | 0.00/96.0 [00:00<?, ?B/s]",
      "application/vnd.jupyter.widget-view+json": {
       "version_major": 2,
       "version_minor": 0,
       "model_id": "1c55d3175cd14f789783cbdf75ac2a89"
      }
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/plain": "config.json:   0%|          | 0.00/4.77k [00:00<?, ?B/s]",
      "application/vnd.jupyter.widget-view+json": {
       "version_major": 2,
       "version_minor": 0,
       "model_id": "ca6788607d0f4fc2822ea0696c002ca6"
      }
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/plain": "model.safetensors:   0%|          | 0.00/1.40G [00:00<?, ?B/s]",
      "application/vnd.jupyter.widget-view+json": {
       "version_major": 2,
       "version_minor": 0,
       "model_id": "08480c2738384d84b23c375311cba85d"
      }
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/plain": "generation_config.json:   0%|          | 0.00/165 [00:00<?, ?B/s]",
      "application/vnd.jupyter.widget-view+json": {
       "version_major": 2,
       "version_minor": 0,
       "model_id": "fb39a17d01ff4b85b2205531e5ac451d"
      }
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "processor = NougatProcessor.from_pretrained(\"facebook/nougat-base\")\n",
    "model = VisionEncoderDecoderModel.from_pretrained(\"facebook/nougat-base\")"
   ],
   "metadata": {
    "collapsed": false,
    "ExecuteTime": {
     "start_time": "2024-12-09T17:25:10.072422Z",
     "end_time": "2024-12-09T17:28:30.128131Z"
    }
   }
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "outputs": [
    {
     "data": {
      "text/plain": "VisionEncoderDecoderModel(\n  (encoder): DonutSwinModel(\n    (embeddings): DonutSwinEmbeddings(\n      (patch_embeddings): DonutSwinPatchEmbeddings(\n        (projection): Conv2d(3, 128, kernel_size=(4, 4), stride=(4, 4))\n      )\n      (norm): LayerNorm((128,), eps=1e-05, elementwise_affine=True)\n      (dropout): Dropout(p=0.0, inplace=False)\n    )\n    (encoder): DonutSwinEncoder(\n      (layers): ModuleList(\n        (0): DonutSwinStage(\n          (blocks): ModuleList(\n            (0-1): 2 x DonutSwinLayer(\n              (layernorm_before): LayerNorm((128,), eps=1e-05, elementwise_affine=True)\n              (attention): DonutSwinAttention(\n                (self): DonutSwinSelfAttention(\n                  (query): Linear(in_features=128, out_features=128, bias=True)\n                  (key): Linear(in_features=128, out_features=128, bias=True)\n                  (value): Linear(in_features=128, out_features=128, bias=True)\n                  (dropout): Dropout(p=0.0, inplace=False)\n                )\n                (output): DonutSwinSelfOutput(\n                  (dense): Linear(in_features=128, out_features=128, bias=True)\n                  (dropout): Dropout(p=0.0, inplace=False)\n                )\n              )\n              (drop_path): DonutSwinDropPath(p=0.1)\n              (layernorm_after): LayerNorm((128,), eps=1e-05, elementwise_affine=True)\n              (intermediate): DonutSwinIntermediate(\n                (dense): Linear(in_features=128, out_features=512, bias=True)\n                (intermediate_act_fn): GELUActivation()\n              )\n              (output): DonutSwinOutput(\n                (dense): Linear(in_features=512, out_features=128, bias=True)\n                (dropout): Dropout(p=0.0, inplace=False)\n              )\n            )\n          )\n          (downsample): DonutSwinPatchMerging(\n            (reduction): Linear(in_features=512, out_features=256, bias=False)\n            (norm): LayerNorm((512,), eps=1e-05, elementwise_affine=True)\n          )\n        )\n        (1): DonutSwinStage(\n          (blocks): ModuleList(\n            (0-1): 2 x DonutSwinLayer(\n              (layernorm_before): LayerNorm((256,), eps=1e-05, elementwise_affine=True)\n              (attention): DonutSwinAttention(\n                (self): DonutSwinSelfAttention(\n                  (query): Linear(in_features=256, out_features=256, bias=True)\n                  (key): Linear(in_features=256, out_features=256, bias=True)\n                  (value): Linear(in_features=256, out_features=256, bias=True)\n                  (dropout): Dropout(p=0.0, inplace=False)\n                )\n                (output): DonutSwinSelfOutput(\n                  (dense): Linear(in_features=256, out_features=256, bias=True)\n                  (dropout): Dropout(p=0.0, inplace=False)\n                )\n              )\n              (drop_path): DonutSwinDropPath(p=0.1)\n              (layernorm_after): LayerNorm((256,), eps=1e-05, elementwise_affine=True)\n              (intermediate): DonutSwinIntermediate(\n                (dense): Linear(in_features=256, out_features=1024, bias=True)\n                (intermediate_act_fn): GELUActivation()\n              )\n              (output): DonutSwinOutput(\n                (dense): Linear(in_features=1024, out_features=256, bias=True)\n                (dropout): Dropout(p=0.0, inplace=False)\n              )\n            )\n          )\n          (downsample): DonutSwinPatchMerging(\n            (reduction): Linear(in_features=1024, out_features=512, bias=False)\n            (norm): LayerNorm((1024,), eps=1e-05, elementwise_affine=True)\n          )\n        )\n        (2): DonutSwinStage(\n          (blocks): ModuleList(\n            (0-13): 14 x DonutSwinLayer(\n              (layernorm_before): LayerNorm((512,), eps=1e-05, elementwise_affine=True)\n              (attention): DonutSwinAttention(\n                (self): DonutSwinSelfAttention(\n                  (query): Linear(in_features=512, out_features=512, bias=True)\n                  (key): Linear(in_features=512, out_features=512, bias=True)\n                  (value): Linear(in_features=512, out_features=512, bias=True)\n                  (dropout): Dropout(p=0.0, inplace=False)\n                )\n                (output): DonutSwinSelfOutput(\n                  (dense): Linear(in_features=512, out_features=512, bias=True)\n                  (dropout): Dropout(p=0.0, inplace=False)\n                )\n              )\n              (drop_path): DonutSwinDropPath(p=0.1)\n              (layernorm_after): LayerNorm((512,), eps=1e-05, elementwise_affine=True)\n              (intermediate): DonutSwinIntermediate(\n                (dense): Linear(in_features=512, out_features=2048, bias=True)\n                (intermediate_act_fn): GELUActivation()\n              )\n              (output): DonutSwinOutput(\n                (dense): Linear(in_features=2048, out_features=512, bias=True)\n                (dropout): Dropout(p=0.0, inplace=False)\n              )\n            )\n          )\n          (downsample): DonutSwinPatchMerging(\n            (reduction): Linear(in_features=2048, out_features=1024, bias=False)\n            (norm): LayerNorm((2048,), eps=1e-05, elementwise_affine=True)\n          )\n        )\n        (3): DonutSwinStage(\n          (blocks): ModuleList(\n            (0-1): 2 x DonutSwinLayer(\n              (layernorm_before): LayerNorm((1024,), eps=1e-05, elementwise_affine=True)\n              (attention): DonutSwinAttention(\n                (self): DonutSwinSelfAttention(\n                  (query): Linear(in_features=1024, out_features=1024, bias=True)\n                  (key): Linear(in_features=1024, out_features=1024, bias=True)\n                  (value): Linear(in_features=1024, out_features=1024, bias=True)\n                  (dropout): Dropout(p=0.0, inplace=False)\n                )\n                (output): DonutSwinSelfOutput(\n                  (dense): Linear(in_features=1024, out_features=1024, bias=True)\n                  (dropout): Dropout(p=0.0, inplace=False)\n                )\n              )\n              (drop_path): DonutSwinDropPath(p=0.1)\n              (layernorm_after): LayerNorm((1024,), eps=1e-05, elementwise_affine=True)\n              (intermediate): DonutSwinIntermediate(\n                (dense): Linear(in_features=1024, out_features=4096, bias=True)\n                (intermediate_act_fn): GELUActivation()\n              )\n              (output): DonutSwinOutput(\n                (dense): Linear(in_features=4096, out_features=1024, bias=True)\n                (dropout): Dropout(p=0.0, inplace=False)\n              )\n            )\n          )\n        )\n      )\n    )\n    (pooler): AdaptiveAvgPool1d(output_size=1)\n  )\n  (decoder): MBartForCausalLM(\n    (model): MBartDecoderWrapper(\n      (decoder): MBartDecoder(\n        (embed_tokens): MBartScaledWordEmbedding(50000, 1024, padding_idx=1)\n        (embed_positions): MBartLearnedPositionalEmbedding(4098, 1024)\n        (layers): ModuleList(\n          (0-9): 10 x MBartDecoderLayer(\n            (self_attn): MBartAttention(\n              (k_proj): Linear(in_features=1024, out_features=1024, bias=True)\n              (v_proj): Linear(in_features=1024, out_features=1024, bias=True)\n              (q_proj): Linear(in_features=1024, out_features=1024, bias=True)\n              (out_proj): Linear(in_features=1024, out_features=1024, bias=True)\n            )\n            (activation_fn): GELUActivation()\n            (self_attn_layer_norm): LayerNorm((1024,), eps=1e-05, elementwise_affine=True)\n            (encoder_attn): MBartAttention(\n              (k_proj): Linear(in_features=1024, out_features=1024, bias=True)\n              (v_proj): Linear(in_features=1024, out_features=1024, bias=True)\n              (q_proj): Linear(in_features=1024, out_features=1024, bias=True)\n              (out_proj): Linear(in_features=1024, out_features=1024, bias=True)\n            )\n            (encoder_attn_layer_norm): LayerNorm((1024,), eps=1e-05, elementwise_affine=True)\n            (fc1): Linear(in_features=1024, out_features=4096, bias=True)\n            (fc2): Linear(in_features=4096, out_features=1024, bias=True)\n            (final_layer_norm): LayerNorm((1024,), eps=1e-05, elementwise_affine=True)\n          )\n        )\n        (layernorm_embedding): LayerNorm((1024,), eps=1e-05, elementwise_affine=True)\n        (layer_norm): LayerNorm((1024,), eps=1e-05, elementwise_affine=True)\n      )\n    )\n    (lm_head): Linear(in_features=1024, out_features=50000, bias=False)\n  )\n)"
     },
     "execution_count": 4,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "device = \"cuda\" if torch.cuda.is_available() else \"cpu\"\n",
    "model.to(device)"
   ],
   "metadata": {
    "collapsed": false,
    "ExecuteTime": {
     "start_time": "2024-12-09T17:28:47.095143Z",
     "end_time": "2024-12-09T17:28:48.179620Z"
    }
   }
  },
  {
   "cell_type": "code",
   "execution_count": 12,
   "outputs": [],
   "source": [
    "image = Image.open(\"lines_2/ww.png\").convert(\"RGB\")\n",
    "\n",
    "images = [image]\n",
    "\n"
   ],
   "metadata": {
    "collapsed": false,
    "ExecuteTime": {
     "start_time": "2024-12-09T17:36:43.057332Z",
     "end_time": "2024-12-09T17:36:43.082940Z"
    }
   }
  },
  {
   "cell_type": "code",
   "execution_count": 13,
   "outputs": [],
   "source": [
    "pixel_values = processor(images, return_tensors=\"pt\")[\"pixel_values\"].to(device)"
   ],
   "metadata": {
    "collapsed": false,
    "ExecuteTime": {
     "start_time": "2024-12-09T17:36:45.668023Z",
     "end_time": "2024-12-09T17:36:45.738233Z"
    }
   }
  },
  {
   "cell_type": "code",
   "execution_count": 14,
   "outputs": [],
   "source": [
    "outputs = model.generate(\n",
    "    pixel_values.to(device),\n",
    "    min_length=1,\n",
    "    max_new_tokens=30,\n",
    "    bad_words_ids=[[processor.tokenizer.unk_token_id]],\n",
    ")"
   ],
   "metadata": {
    "collapsed": false,
    "ExecuteTime": {
     "start_time": "2024-12-09T17:36:57.176729Z",
     "end_time": "2024-12-09T17:36:58.904005Z"
    }
   }
  },
  {
   "cell_type": "code",
   "execution_count": 23,
   "outputs": [
    {
     "ename": "ImportError",
     "evalue": "\nNougatTokenizerFast requires the NLTK library but it was not found in your environment. You can install it by referring to:\nhttps://www.nltk.org/install.html. Please note that you may need to restart your runtime after installation.\n\nNougatTokenizerFast requires the python-Levenshtein library but it was not found in your environment. You can install it with pip: `pip\ninstall python-Levenshtein`. Please note that you may need to restart your runtime after installation.\n",
     "output_type": "error",
     "traceback": [
      "\u001B[1;31m---------------------------------------------------------------------------\u001B[0m",
      "\u001B[1;31mImportError\u001B[0m                               Traceback (most recent call last)",
      "Cell \u001B[1;32mIn[23], line 2\u001B[0m\n\u001B[0;32m      1\u001B[0m sequence \u001B[38;5;241m=\u001B[39m processor\u001B[38;5;241m.\u001B[39mbatch_decode(outputs, skip_special_tokens\u001B[38;5;241m=\u001B[39m\u001B[38;5;28;01mTrue\u001B[39;00m)[\u001B[38;5;241m0\u001B[39m]\n\u001B[1;32m----> 2\u001B[0m sequence \u001B[38;5;241m=\u001B[39m \u001B[43mprocessor\u001B[49m\u001B[38;5;241;43m.\u001B[39;49m\u001B[43mpost_process_generation\u001B[49m\u001B[43m(\u001B[49m\u001B[43msequence\u001B[49m\u001B[43m,\u001B[49m\u001B[43m \u001B[49m\u001B[43mfix_markdown\u001B[49m\u001B[38;5;241;43m=\u001B[39;49m\u001B[38;5;28;43;01mFalse\u001B[39;49;00m\u001B[43m)\u001B[49m\n",
      "File \u001B[1;32m~\\Desktop\\python-projects\\pytorch-start\\env\\Lib\\site-packages\\transformers\\models\\nougat\\processing_nougat.py:160\u001B[0m, in \u001B[0;36mNougatProcessor.post_process_generation\u001B[1;34m(self, *args, **kwargs)\u001B[0m\n\u001B[0;32m    155\u001B[0m \u001B[38;5;28;01mdef\u001B[39;00m \u001B[38;5;21mpost_process_generation\u001B[39m(\u001B[38;5;28mself\u001B[39m, \u001B[38;5;241m*\u001B[39margs, \u001B[38;5;241m*\u001B[39m\u001B[38;5;241m*\u001B[39mkwargs):\n\u001B[0;32m    156\u001B[0m \u001B[38;5;250m    \u001B[39m\u001B[38;5;124;03m\"\"\"\u001B[39;00m\n\u001B[0;32m    157\u001B[0m \u001B[38;5;124;03m    This method forwards all its arguments to NougatTokenizer's [`~PreTrainedTokenizer.post_process_generation`].\u001B[39;00m\n\u001B[0;32m    158\u001B[0m \u001B[38;5;124;03m    Please refer to the docstring of this method for more information.\u001B[39;00m\n\u001B[0;32m    159\u001B[0m \u001B[38;5;124;03m    \"\"\"\u001B[39;00m\n\u001B[1;32m--> 160\u001B[0m     \u001B[38;5;28;01mreturn\u001B[39;00m \u001B[38;5;28;43mself\u001B[39;49m\u001B[38;5;241;43m.\u001B[39;49m\u001B[43mtokenizer\u001B[49m\u001B[38;5;241;43m.\u001B[39;49m\u001B[43mpost_process_generation\u001B[49m\u001B[43m(\u001B[49m\u001B[38;5;241;43m*\u001B[39;49m\u001B[43margs\u001B[49m\u001B[43m,\u001B[49m\u001B[43m \u001B[49m\u001B[38;5;241;43m*\u001B[39;49m\u001B[38;5;241;43m*\u001B[39;49m\u001B[43mkwargs\u001B[49m\u001B[43m)\u001B[49m\n",
      "File \u001B[1;32m~\\Desktop\\python-projects\\pytorch-start\\env\\Lib\\site-packages\\transformers\\models\\nougat\\tokenization_nougat_fast.py:617\u001B[0m, in \u001B[0;36mNougatTokenizerFast.post_process_generation\u001B[1;34m(self, generation, fix_markdown, num_workers)\u001B[0m\n\u001B[0;32m    592\u001B[0m \u001B[38;5;28;01mdef\u001B[39;00m \u001B[38;5;21mpost_process_generation\u001B[39m(\n\u001B[0;32m    593\u001B[0m     \u001B[38;5;28mself\u001B[39m,\n\u001B[0;32m    594\u001B[0m     generation: Union[\u001B[38;5;28mstr\u001B[39m, List[\u001B[38;5;28mstr\u001B[39m]],\n\u001B[0;32m    595\u001B[0m     fix_markdown: \u001B[38;5;28mbool\u001B[39m \u001B[38;5;241m=\u001B[39m \u001B[38;5;28;01mTrue\u001B[39;00m,\n\u001B[0;32m    596\u001B[0m     num_workers: \u001B[38;5;28mint\u001B[39m \u001B[38;5;241m=\u001B[39m \u001B[38;5;28;01mNone\u001B[39;00m,\n\u001B[0;32m    597\u001B[0m ) \u001B[38;5;241m-\u001B[39m\u001B[38;5;241m>\u001B[39m Union[\u001B[38;5;28mstr\u001B[39m, List[\u001B[38;5;28mstr\u001B[39m]]:\n\u001B[0;32m    598\u001B[0m \u001B[38;5;250m    \u001B[39m\u001B[38;5;124;03m\"\"\"\u001B[39;00m\n\u001B[0;32m    599\u001B[0m \u001B[38;5;124;03m    Postprocess a generated text or a list of generated texts.\u001B[39;00m\n\u001B[0;32m    600\u001B[0m \n\u001B[1;32m   (...)\u001B[0m\n\u001B[0;32m    615\u001B[0m \u001B[38;5;124;03m        Union[str, List[str]]: The postprocessed text or list of postprocessed texts.\u001B[39;00m\n\u001B[0;32m    616\u001B[0m \u001B[38;5;124;03m    \"\"\"\u001B[39;00m\n\u001B[1;32m--> 617\u001B[0m     \u001B[43mrequires_backends\u001B[49m\u001B[43m(\u001B[49m\u001B[38;5;28;43mself\u001B[39;49m\u001B[43m,\u001B[49m\u001B[43m \u001B[49m\u001B[43m[\u001B[49m\u001B[38;5;124;43m\"\u001B[39;49m\u001B[38;5;124;43mnltk\u001B[39;49m\u001B[38;5;124;43m\"\u001B[39;49m\u001B[43m,\u001B[49m\u001B[43m \u001B[49m\u001B[38;5;124;43m\"\u001B[39;49m\u001B[38;5;124;43mlevenshtein\u001B[39;49m\u001B[38;5;124;43m\"\u001B[39;49m\u001B[43m]\u001B[49m\u001B[43m)\u001B[49m\n\u001B[0;32m    619\u001B[0m     \u001B[38;5;28;01mif\u001B[39;00m \u001B[38;5;28misinstance\u001B[39m(generation, \u001B[38;5;28mlist\u001B[39m):\n\u001B[0;32m    620\u001B[0m         \u001B[38;5;28;01mif\u001B[39;00m num_workers \u001B[38;5;129;01mis\u001B[39;00m \u001B[38;5;129;01mnot\u001B[39;00m \u001B[38;5;28;01mNone\u001B[39;00m \u001B[38;5;129;01mand\u001B[39;00m \u001B[38;5;28misinstance\u001B[39m(num_workers, \u001B[38;5;28mint\u001B[39m):\n",
      "File \u001B[1;32m~\\Desktop\\python-projects\\pytorch-start\\env\\Lib\\site-packages\\transformers\\utils\\import_utils.py:1531\u001B[0m, in \u001B[0;36mrequires_backends\u001B[1;34m(obj, backends)\u001B[0m\n\u001B[0;32m   1529\u001B[0m failed \u001B[38;5;241m=\u001B[39m [msg\u001B[38;5;241m.\u001B[39mformat(name) \u001B[38;5;28;01mfor\u001B[39;00m available, msg \u001B[38;5;129;01min\u001B[39;00m checks \u001B[38;5;28;01mif\u001B[39;00m \u001B[38;5;129;01mnot\u001B[39;00m available()]\n\u001B[0;32m   1530\u001B[0m \u001B[38;5;28;01mif\u001B[39;00m failed:\n\u001B[1;32m-> 1531\u001B[0m     \u001B[38;5;28;01mraise\u001B[39;00m \u001B[38;5;167;01mImportError\u001B[39;00m(\u001B[38;5;124m\"\u001B[39m\u001B[38;5;124m\"\u001B[39m\u001B[38;5;241m.\u001B[39mjoin(failed))\n",
      "\u001B[1;31mImportError\u001B[0m: \nNougatTokenizerFast requires the NLTK library but it was not found in your environment. You can install it by referring to:\nhttps://www.nltk.org/install.html. Please note that you may need to restart your runtime after installation.\n\nNougatTokenizerFast requires the python-Levenshtein library but it was not found in your environment. You can install it with pip: `pip\ninstall python-Levenshtein`. Please note that you may need to restart your runtime after installation.\n"
     ]
    }
   ],
   "source": [
    "sequence = processor.batch_decode(outputs, skip_special_tokens=True)[0]\n",
    "sequence = processor.post_process_generation(sequence, fix_markdown=False)"
   ],
   "metadata": {
    "collapsed": false
   }
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 2
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython2",
   "version": "2.7.6"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
