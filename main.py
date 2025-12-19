import asyncio
import signal
from TikTokApi import TikTokApi
import os
from src.db import init_database, ingest_json, download_video_and_store, get_connection

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


# @profile
async def main():
    '''
    Begins the front end and back end application

    Args:
        None

    Returns:
        None

    '''
    print("Hello from tiktok-save!")

    # Initialize the database
    print("\nInitializing database...")
    init_database()

    # Ingest the TikTok JSON export
    print("\nIngesting TikTok data...")
    json_file = "tiktok-data/user_data_tiktok.json"
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

    # Download all videos where transcription_status = 0
    print("\nGetting all untranscribed videos...")
    conn = get_connection()
    cursor = conn.cursor()

    # Get all videos that haven't been transcribed yet
    # cursor.execute("SELECT id FROM video_data WHERE download_status = 0 and video_has_error = 0")
    cursor.execute("SELECT id FROM video_data WHERE video_has_error = 1")
    results = cursor.fetchall()
    conn.close()

    if results:
        video_ids = [row[0] for row in results]
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

            import time
            start_time = time.time()
            download_results = await download_video_and_store([video_id], tiktok_api=tiktok_api,
                                                              whisper_model=whisper_model)
            elapsed_time = time.time() - start_time
            download_result = download_results[0]

            if download_result.get('status') == 'success':
                print(f"Downloaded video {video_id} in {elapsed_time:.2f}s")
            else:
                status = download_result.get('status')
                message = download_result.get('message', 'No error message')
                print(f"Failed to download video {video_id}: {status} - {message}")

            processed_count += 1
    else:
        print("\nNo untranscribed videos found in database")


if __name__ == "__main__":
    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    asyncio.run(main())
