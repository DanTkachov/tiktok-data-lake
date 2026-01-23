import asyncio
import signal
from TikTokApi import TikTokApi
import os
from src.db import init_database, ingest_json, download_video_and_store, get_connection
import time

# Global flag for graceful shutdown
shutdown_requested = False


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    global shutdown_requested
    if not shutdown_requested:
        shutdown_requested = True
        print("\n\n" + "=" * 60)
        print("SHUTDOWN REQUESTED")
        print("=" * 60)
        print("Received interrupt signal (Ctrl+C)")
        print("Finishing current video download/transcription...")
        print("Please wait, this may take a moment.")
        print("=" * 60 + "\n")


def get_videos_to_download():
    """
    Get list of video IDs that need to be downloaded.

    Returns:
        list: Video IDs where download_status = 0 and video_has_error = 0
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM video_data WHERE download_status = 0 AND video_has_error = 0")
    results = cursor.fetchall()
    conn.close()
    return [row[0] for row in results]


def get_videos_with_errors():
    """
    Get list of video IDs that have errors.

    Returns:
        list: Video IDs where video_has_error = 1
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM video_data WHERE video_has_error = 1")
    results = cursor.fetchall()
    conn.close()
    return [row[0] for row in results]


def get_videos_to_transcribe():
    """
    Get list of video IDs that have been downloaded but not transcribed.

    Returns:
        list: Video IDs where download_status = 1 and transcription_status = 0
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM video_data WHERE download_status = 1 AND transcription_status = 0")
    results = cursor.fetchall()
    conn.close()
    return [row[0] for row in results]


def setup_database(json_file="tiktok-data/user_data_tiktok.json"):
    """
    Initialize database and ingest TikTok JSON export.

    Args:
        json_file: Path to TikTok JSON export file

    Returns:
        dict: Statistics from ingestion (total, inserted, skipped, errors)
    """
    print("\nInitializing database...")
    init_database()

    print("\nIngesting TikTok data...")
    stats = ingest_json(json_file)

    # Print statistics
    print("\n" + "=" * 50)
    print("INGESTION COMPLETE")
    print("=" * 50)
    print(f"Total videos found:     {stats['total']}")
    print(f"Newly inserted:         {stats['inserted']}")
    print(f"Already in database:    {stats['skipped']}")
    print(f"Errors:                 {stats['errors']}")
    print("=" * 50)

    return stats


async def main():
    '''
    Begins the front end and back end application

    Args:
        None

    Returns:
        None

    '''
    print("Hello from tiktok-save!")

    # Setup database and ingest JSON
    setup_database("tiktok-data/user_data_tiktok.json")

    # Get videos to download
    print("\nGetting videos to download...")
    video_ids = get_videos_to_download()

    if not video_ids:
        print("\nNo videos found to download")
        return

    print(f"Found {len(video_ids)} videos to download and transcribe\n")

    # Load WhisperModel once for all videos
    print("Loading Whisper model...")
    from faster_whisper import WhisperModel
    device = "cpu"
    compute_type = "int8"
    whisper_model = WhisperModel("base", device=device, compute_type=compute_type)
    print("Whisper model loaded!\n")

    # Create TikTokApi session once for all videos
    print("Creating TikTok API session...")
    ms_token = os.environ.get("ms_token", None)
    tiktok_api = TikTokApi()
    await tiktok_api.create_sessions(ms_tokens=[ms_token], num_sessions=1, sleep_after=3)
    print("TikTok API session created!\n")

    # Download and transcribe each video
    processed_count = 0
    for video_id in video_ids:
        # Check if shutdown was requested
        if shutdown_requested:
            print("\n" + "=" * 60)
            print("SHUTDOWN COMPLETE")
            print("=" * 60)
            print(f"Processed {processed_count} out of {len(video_ids)} videos")
            print(f"Remaining videos: {len(video_ids) - processed_count}")
            print("Run the program again to continue processing remaining videos.")
            print("=" * 60)
            break

        start_time = time.time()
        download_results = await download_video_and_store(
            [video_id],
            tiktok_api=tiktok_api,
            whisper_model=whisper_model
        )
        elapsed_time = time.time() - start_time
        download_result = download_results[0]

        if download_result.get('status') == 'success':
            print(f"Downloaded video {video_id} in {elapsed_time:.2f}s")
        else:
            status = download_result.get('status')
            message = download_result.get('message', 'No error message')
            print(f"Failed to download video {video_id}: {status} - {message}")

        processed_count += 1


if __name__ == "__main__":
    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    asyncio.run(main())
