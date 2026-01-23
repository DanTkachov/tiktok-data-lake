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
def transcribe_video(video_id):

