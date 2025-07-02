import base64
import gc
import io
import threading
import time
import traceback
import tracemalloc
from abc import ABC, abstractmethod
from typing import Dict, Any, Tuple, List

import pdfplumber
from langchain_core.messages import BaseMessage
from pypdfium2 import PdfiumError

from preprocessing.logging.aws_logger import aws_logger
from preprocessing.mongo_db.mongodb import get_mongodb_collection


class ExternalTextExtractor(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def call_llm(self, **kwargs) -> BaseMessage:
        pass

    @abstractmethod
    def parse(self, **kwargs) -> str:
        pass

    def get_text_from_chars(self, line_dict: Dict[str, Any]):
        return ''.join(char['text'] for char in sorted(line_dict['chars'], key=lambda c: c['x0']))

    def crop_region_to_base64(self, crop_bbox: Tuple[float, float, float, float], page: pdfplumber.pdf.Page,
                              document_id: str, invoke_id: str,
                              resolution: int = 600) -> str | None:

        ## TODO change this because sometimes it cause PDFIUM error
        retries = 0
        delay = 2
        max_retries = 3
        current_resolution = resolution
        w = None
        # tracemalloc.start()
        while retries < max_retries:
            # snapshot_before = tracemalloc.take_snapshot()
            try:

                cropped_page = page.crop(crop_bbox)
                image_buffer = io.BytesIO()
                img = cropped_page.to_image(resolution=current_resolution).original
                img.save(image_buffer, format="PNG")
                image_buffer.seek(0)
                image_str = base64.b64encode(image_buffer.read()).decode("utf-8")

                    # current, peak = tracemalloc.get_traced_memory()
                    # aws_logger.info(f"📊 Memory usage: current={current / 1024**2:.2f}MB, peak={peak / 1024**2:.2f}MB")
                    # snapshot_after = tracemalloc.take_snapshot()
                    # stats = snapshot_after.compare_to(snapshot_before, 'lineno')
                    # aws_logger.info("📊 Memory usage BEFORE GC:")
                    # for stat in stats[:5]:
                    #     aws_logger.info(str(stat))

                    # Clean up
                img.close()
                # del cropped_page
                # del image_buffer
                # gc.collect()
                #
                # snapshot_after_gc = tracemalloc.take_snapshot()
                # stats_gc = snapshot_after_gc.compare_to(snapshot_before, 'lineno')
                # aws_logger.info("🧹 Memory usage AFTER GC:")
                # for stat in stats_gc[:5]:
                #     aws_logger.info(str(stat))
                #
                # tracemalloc.stop()

                return image_str

            except (PdfiumError, MemoryError, OSError) as e:
                aws_logger.warning(
                    f"⚠️ Render attempt {retries + 1} failed for ELI: {document_id}, page_number: {page.page_number}, "
                    f"bbox: {crop_bbox} (resolution: {current_resolution}) - Error: {e}")
                traceback.print_exc()
                w = e
                retries += 1
                current_resolution -= 150
                time.sleep(delay)
                delay *= 2  # exponential backoff
            finally:
                # if tracemalloc.is_tracing():
                #     tracemalloc.stop()
                try:
                    del img
                    del cropped_page
                    del image_buffer
                except:
                    pass
                gc.collect()

        crop_bbox = [round(coord, 3) for coord in crop_bbox]
        get_mongodb_collection(
            db_name="preprocessing_legal_acts_texts",
            collection_name="legal_act_page_metadata_crop_bbox_image"
        ).update_one(
            {
                "ELI": document_id,
                "page_number": page.page_number,
                "bbox": crop_bbox,
                "invoke_id": invoke_id,
            },
            {"$set": {
                "ELI": document_id,
                "page_number": page.page_number,
                "invoke_id": invoke_id,
                "bbox": crop_bbox,
                "message": str(w)

            }
            },
            upsert=True
        )
        # tracemalloc.stop()
        aws_logger.error(
            f"❌ Failed to render cropped image after {max_retries} attempts "
            f"for ELI: {document_id}, page_number: {page.page_number}, invoke_id: {invoke_id}, "
            f"bbox: {crop_bbox} (last resolution tried: {current_resolution + 150})"
        )
        return None
