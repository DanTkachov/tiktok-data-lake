import asyncio
from src.db import init_database, ingest_json, download_video_and_store, get_connection


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
    print("\n" + "="*50)
    print("INGESTION COMPLETE")
    print("="*50)
    print(f"Total videos found:     {stats['total']}")
    print(f"Newly inserted:         {stats['inserted']}")
    print(f"Already in database:    {stats['skipped']}")
    print(f"Errors:                 {stats['errors']}")
    print("="*50)

    # Test downloading a single video
    print("\nTesting download...")
    conn = get_connection()
    cursor = conn.cursor()

    # Get one undownloaded video
    cursor.execute("SELECT id, tiktok_url FROM video_data WHERE download_status = 0 LIMIT 1")
    result = cursor.fetchone()
    conn.close()

    if result:
        video_id, url = result
        print(f"\nDownloading video {video_id}")
        print(f"URL: {url}")

        # Pass as a list (function expects a list)
        download_results = await download_video_and_store([video_id])

        # Get first result from the list
        download_result = download_results[0]

        print("\n" + "="*50)
        print("DOWNLOAD RESULT")
        print("="*50)
        print(f"Status: {download_result.get('status')}")
        if download_result.get('status') == 'success':
            print(f"Video ID: {download_result.get('video_id')}")
            print(f"Size: {download_result.get('size_bytes'):,} bytes")
            if 'image_count' in download_result:
                print(f"Images: {download_result.get('image_count')}")
        else:
            print(f"Message: {download_result.get('message')}")
        print("="*50)
    else:
        print("\nNo undownloaded videos found in database")


if __name__ == "__main__":
    asyncio.run(main())
