import asyncio
import os
from celery import Celery
from src.db import download_video_and_store
from TikTokApi import TikTokApi

app = Celery('tasks', backend='redis://localhost:6379/0', broker='redis://localhost:6379/0')
app.conf.task_routes = {
    'src.tasks.download_task': {'queue': 'downloads', 'rate_limit': '25/m'}
}

# Global state to hold the persistent loop and API session
GLOBAL_LOOP = None
GLOBAL_TIKTOK_API = None
GLOBAL_OCR_MODEL = None


def get_or_create_context():
    """
    Ensures a single asyncio event loop and TikTokApi session exist for this worker process.
    Returns: (loop, tiktok_api)
    """
    global GLOBAL_LOOP, GLOBAL_TIKTOK_API

    if GLOBAL_LOOP is None:
        print("🔄 Creating new persistent event loop for this worker...")
        GLOBAL_LOOP = asyncio.new_event_loop()
        asyncio.set_event_loop(GLOBAL_LOOP)

    if GLOBAL_TIKTOK_API is None:
        print("🚀 Initializing global TikTok API session...")
        
        async def _init_api():
            ms_token = os.environ.get("ms_token", None)
            api = TikTokApi()
            await api.create_sessions(ms_tokens=[ms_token], num_sessions=1, sleep_after=3)
            return api
            
        # Run initialization on our persistent loop
        GLOBAL_TIKTOK_API = GLOBAL_LOOP.run_until_complete(_init_api())
        print(f"✅ TikTok API session ready! Object: {GLOBAL_TIKTOK_API}")

    return GLOBAL_LOOP, GLOBAL_TIKTOK_API


def get_or_create_ocr_model():
    """
    Ensures a single RapidOCR model exists for this worker process.
    Returns: ocr_model
    """
    global GLOBAL_OCR_MODEL

    if GLOBAL_OCR_MODEL is None:
        print("🔍 Initializing global RapidOCR model...")
        from rapidocr_onnxruntime import RapidOCR

        # Initialize RapidOCR (uses ONNX runtime, much more stable than PaddleOCR)
        GLOBAL_OCR_MODEL = RapidOCR()
        print(f"✅ RapidOCR model ready! Object: {GLOBAL_OCR_MODEL}")

    return GLOBAL_OCR_MODEL


@app.task
def add(x, y):
    return x + y


@app.task(queue='downloads')
def download_task(video_id):
    """
    Downloads a single video from TikTok using the persistent global session.
    """
    print(f"🎬 Starting download for video ID: {video_id}")
    
    # Get the persistent loop and API
    loop, api = get_or_create_context()

    async def _download():
        print(f"📥 Calling download_video_and_store for {video_id}...")
        results = await download_video_and_store(
            [video_id],
            tiktok_api=api,
            whisper_model=None
        )
        print(f"✅ Download complete for {video_id}: {results[0].get('status')}")
        return results[0]

    # Use the persistent loop to run the task
    # We DO NOT use asyncio.run() here because that would create a new loop
    result = loop.run_until_complete(_download())
    
    print(f"🏁 Task finished for {video_id}")
    return result


@app.task(queue="transcription")
def transcribe_task(video_id):
    """
    Transcribes a single video from the database.

    Args:
        video_id: ID of the video to transcribe

    Returns:
        dict with status and message
    """
    from src.db import get_connection, transcribe_video

    print(f"🎤 Starting transcription for video ID: {video_id}")

    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Check if already transcribed (idempotency)
        cursor.execute("SELECT transcription_status, content_type FROM video_data WHERE id = ?", (video_id,))
        result = cursor.fetchone()

        if not result:
            conn.close()
            return {"status": "error", "message": f"Video {video_id} not found in database"}

        transcription_status, content_type = result

        if transcription_status == 1:
            conn.close()
            print(f"⏭️  Already transcribed: {video_id}")
            return {"status": "skipped", "message": "Already transcribed"}

        # Skip non-video content (images don't need transcription)
        if content_type != "video":
            conn.close()
            print(f"⏭️  Skipping non-video content: {video_id} (type: {content_type})")
            return {"status": "skipped", "message": f"Content type is {content_type}, not video"}

        # Get video BLOB from database
        cursor.execute("SELECT video_blob FROM videos WHERE id = ?", (video_id,))
        blob_result = cursor.fetchone()

        if not blob_result:
            conn.close()
            return {"status": "error", "message": f"Video BLOB not found for {video_id}"}

        video_bytes = blob_result[0]
        conn.close()

        # Transcribe the video (this function updates the database internally)
        transcription = transcribe_video(video_id, video_bytes, whisper_model=None)

        print(f"✅ Transcription complete for {video_id}: {len(transcription)} characters")
        return {"status": "success", "video_id": video_id, "transcription_length": len(transcription)}

    except Exception as e:
        conn.close()
        print(f"❌ Transcription failed for {video_id}: {e}")
        return {"status": "error", "message": str(e)}

@app.task(queue="ocr")
def ocr_images_task(video_id):
    """
    OCR processing for image posts from the database.

    Args:
        video_id: ID of the image post to OCR

    Returns:
        dict with status and message
    """
    from src.db import get_connection, ocr_images

    print(f"🔍 Starting OCR for video ID: {video_id}")

    # Get the persistent OCR model
    ocr_model = get_or_create_ocr_model()

    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Check if already OCR'd (idempotency)
        cursor.execute("SELECT ocr_status, content_type FROM video_data WHERE id = ?", (video_id,))
        result = cursor.fetchone()

        if not result:
            conn.close()
            return {"status": "error", "message": f"Video {video_id} not found in database"}

        ocr_status, content_type = result

        if ocr_status == 1:
            conn.close()
            print(f"⏭️  Already OCR'd: {video_id}")
            return {"status": "skipped", "message": "Already OCR'd"}

        # Skip non-image content (videos don't need OCR)
        if content_type != "images":
            conn.close()
            print(f"⏭️  Skipping non-image content: {video_id} (type: {content_type})")
            return {"status": "skipped", "message": f"Content type is {content_type}, not images"}

        # Get image ZIP BLOB from database
        cursor.execute("SELECT video_blob FROM videos WHERE id = ?", (video_id,))
        blob_result = cursor.fetchone()

        if not blob_result:
            conn.close()
            return {"status": "error", "message": f"Image BLOB not found for {video_id}"}

        zip_bytes = blob_result[0]
        conn.close()

        # OCR the images using the persistent model (this function updates the database internally)
        ocr_text = ocr_images(video_id, zip_bytes, ocr_model=ocr_model)

        print(f"✅ OCR complete for {video_id}: {len(ocr_text)} characters")
        return {"status": "success", "video_id": video_id, "ocr_text_length": len(ocr_text)}

    except Exception as e:
        conn.close()
        print(f"❌ OCR failed for {video_id}: {e}")
        return {"status": "error", "message": str(e)}


def queue_ocr():
    """
    Queries the database for all un-OCR'd image posts and queues them to Redis.
    This is a coordinator function - run it manually or on a schedule.

    Returns:
        dict with statistics about queued image posts
    """
    from src.db import get_connection

    print("\n" + "=" * 60)
    print("QUEUEING OCR TASKS")
    print("=" * 60)

    conn = get_connection()
    cursor = conn.cursor()

    # Get all image posts that are downloaded but not OCR'd
    cursor.execute("""
        SELECT id FROM video_data
        WHERE download_status = 1
          AND ocr_status = 0
          AND content_type = 'images'
        ORDER BY date_favorited DESC
    """)

    video_ids = [row[0] for row in cursor.fetchall()]
    conn.close()

    if not video_ids:
        print("No image posts found needing OCR")
        print("=" * 60 + "\n")
        return {"total": 0, "queued": 0}

    print(f"Found {len(video_ids)} image posts needing OCR")
    print(f"Queueing tasks to Redis...")

    # Queue all image posts to Redis
    queued_count = 0
    for video_id in video_ids:
        ocr_images_task.delay(video_id)
        queued_count += 1

    print(f"✅ Successfully queued {queued_count} OCR tasks")
    print("=" * 60 + "\n")

    return {"total": len(video_ids), "queued": queued_count}



def queue_transcriptions():
    """
    Queries the database for all untranscribed videos and queues them to Redis.
    This is a coordinator function - run it manually or on a schedule.

    Returns:
        dict with statistics about queued videos
    """
    from src.db import get_connection

    print("\n" + "=" * 60)
    print("QUEUEING TRANSCRIPTION TASKS")
    print("=" * 60)

    conn = get_connection()
    cursor = conn.cursor()

    # Get all videos that are downloaded but not transcribed
    cursor.execute("""
        SELECT id FROM video_data
        WHERE download_status = 1
          AND transcription_status = 0
          AND content_type = 'video'
        ORDER BY date_favorited DESC
    """)

    video_ids = [row[0] for row in cursor.fetchall()]
    conn.close()

    if not video_ids:
        print("No videos found needing transcription")
        print("=" * 60 + "\n")
        return {"total": 0, "queued": 0}

    print(f"Found {len(video_ids)} videos needing transcription")
    print(f"Queueing tasks to Redis...")

    # Queue all videos to Redis
    queued_count = 0
    for video_id in video_ids:
        transcribe_task.delay(video_id)
        queued_count += 1

    print(f"✅ Successfully queued {queued_count} transcription tasks")
    print("=" * 60 + "\n")

    return {"total": len(video_ids), "queued": queued_count}
