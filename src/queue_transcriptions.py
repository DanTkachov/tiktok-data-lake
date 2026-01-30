"""
Helper script to queue transcription tasks.

Usage:
    python queue_transcriptions.p
"""

from src.tasks import queue_transcriptions

if __name__ == "__main__":
    result = queue_transcriptions()

    if result["queued"] > 0:
        print(f"\n✅ Next steps:")
        print(f"   1. Make sure Redis is running: redis-server")
        print(f"   2. Start transcription workers: celery -A src.tasks worker -Q transcription --concurrency=4")
        print(f"   3. Monitor progress in the worker logs\n")
    else:
        print("\n✅ All videos are already transcribed!\n")
