import asyncio
import os
from celery import Celery
from src.db import download_video_and_store
from TikTokApi import TikTokApi

app = Celery('tasks', backend='redis://localhost:6379/0', broker='redis://localhost:6379/0')
app.conf.task_routes = {
    'src.tasks.download_task': {'queue': 'downloads', 'rate_limit': '18/m'}  # 60 total per minute
}

# Global state to hold the persistent loop and API session
GLOBAL_LOOP = None
GLOBAL_TIKTOK_API = None


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
def ocr_images(video_id):



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
