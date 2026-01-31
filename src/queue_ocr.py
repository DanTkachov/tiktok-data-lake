"""
Helper script to queue OCR tasks.

Usage:
    python queue_ocr.py
"""

from src.tasks import queue_ocr
from src.tasks import queue_transcriptions


if __name__ == "__main__":
    result_ocr = queue_ocr()

    if result_ocr["queued"] > 0:
        print(f"\n✅ Next steps:")
        print(f"   1. Make sure Redis is running: docker compose up -d")
        print(f"   2. Start OCR workers: celery -A src.tasks worker --queues=ocr --concurrency=4 -n tiktok_ocr_worker --loglevel=info")
        print(f"   3. Monitor progress in the worker logs\n")
    else:
        print("\n✅ All image posts are already OCR'd!\n")

    result = queue_transcriptions()

    if result["queued"] > 0:
        print(f"\n✅ Next steps:")
        print(f"   1. Make sure Redis is running: redis-server")
        print(f"   2. Start transcription workers: celery -A src.tasks worker -Q transcription --concurrency=4")
        print(f"   3. Monitor progress in the worker logs\n")
    else:
        print("\n✅ All videos are already transcribed!\n")

