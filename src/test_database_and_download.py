import sys
# sys.path.insert(0, '/home/dan/Desktop/tiktok-save')

from src.tasks import download_task
from main import setup_database, get_videos_to_download

# Setup database with mock data
print("=" * 60)
print("SETTING UP TEST DATABASE")
print("=" * 60)
setup_database("../tiktok-data/user_data_mock_100.json")

# Get videos to download
print("\nGetting videos to download...")
video_ids = get_videos_to_download()

if not video_ids:
    print("No videos found to download. Exiting.")
    sys.exit(0)

print(f"Found {len(video_ids)} videos to download")
print(f"Testing with first video: {video_ids[0]}\n")

# Submit download task to Celery
print("=" * 60)
print("SUBMITTING DOWNLOAD TASK TO CELERY")
print("=" * 60)
for video in video_ids:
    result = download_task.delay(video)
    print(f"Task submitted. Task ID: {result.id}")
    print(f"Waiting for task to complete...\n")

    # Wait for result (blocks until task completes)
    try:
        task_result = result.get(timeout=60)  # 60 second timeout
        print("=" * 60)
        print("TASK COMPLETED")
        print("=" * 60)
        print(f"Status: {task_result.get('status')}")
        print(f"Video ID: {task_result.get('video_id')}")
        if task_result.get('status') == 'success':
            print(f"Size: {task_result.get('size_bytes')} bytes")
        else:
            print(f"Message: {task_result.get('message')}")
        print("=" * 60)
    except Exception as e:
        print(f"Error: {e}")
