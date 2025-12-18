import sqlite3
import os
from pathlib import Path

import time
import os
from TikTokApi import TikTokApi


DB_PATH = Path(__file__).parent.parent / "db" / "tiktok_archive.db"


def init_database():
    """
    Creates the database if it doesn't exist.
    Also creates all tables if they dont exist

    video_data stores all metadata and a link to the videos in the videos table.
    videos stores the actual downloaded video.

    Args:
        None

    Returns:
        None
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # video_data: stores all metadata from TikTok JSON export
        cursor.execute("""
           CREATE TABLE IF NOT EXISTS video_data (
             id TEXT PRIMARY KEY,
             title TEXT,
             uploader TEXT,
             uploader_id TEXT,
             desc TEXT,
             create_time INTEGER,
             duration INTEGER,
             tiktok_url TEXT,
             content_type TEXT,
             download_status BOOLEAN DEFAULT 0,
             transcription_status BOOLEAN DEFAULT 0,
             transcription TEXT,
             date_favorited INTEGER,
             video_is_deleted BOOLEAN DEFAULT 0,
             video_is_private BOOLEAN DEFAULT 0
           )
           """)

        # videos: stores actual downloaded video/image BLOBs
        # Uses same ID as video_data for 1:1 relationship
        cursor.execute("""
           CREATE TABLE IF NOT EXISTS videos (
             id TEXT PRIMARY KEY,
             video_blob BLOB NOT NULL,
             date_downloaded INTEGER,
             FOREIGN KEY (id) REFERENCES video_data(id)
           )
           """)

        # tags: stores tags for videos (many-to-many relationship)
        cursor.execute("""
           CREATE TABLE IF NOT EXISTS tags (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             video_id TEXT NOT NULL,
             tag TEXT NOT NULL,
             confidence REAL,
             FOREIGN KEY (video_id) REFERENCES video_data(id),
             UNIQUE(video_id, tag)
           )
           """)

        # Create index for efficient tag searching
        cursor.execute("""
           CREATE INDEX IF NOT EXISTS idx_tags_tag ON tags(tag)
           """)

        cursor.execute("""
           CREATE INDEX IF NOT EXISTS idx_tags_video_id ON tags(video_id)
           """)

        conn.commit()
        conn.close()
        print(f"Database initialized at {DB_PATH}")
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")

def get_connection():
    """Returns a connection to the database."""
    return sqlite3.connect(DB_PATH)


def ingest_json(json_file):
    """
    Process the tiktok json that is exported when you ask for your data.
    Used to populate the database that stores your favorited videos.

    Args:
        json_file: Path to JSON file direct from TikTok.
    Returns:
        Dictionary with statistics about the ingestion process
    """
    import json
    from datetime import datetime

    # Load the JSON file
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Get favorite videos only (not liked videos)
    activity = data["Your Activity"]
    videos = activity.get("Favorite Videos", {}).get("FavoriteVideoList", [])

    conn = get_connection()
    cursor = conn.cursor()

    stats = {
        "total": len(videos),
        "inserted": 0,
        "skipped": 0,
        "errors": 0
    }

    for video in videos:
        try:
            # Extract video ID from URL
            # URLs look like: https://www.tiktokv.com/share/video/7568062427057720590/
            link = video.get("link") or video.get("Link")
            if not link:
                stats["errors"] += 1
                continue

            # Extract ID from URL (last segment before trailing slash)
            video_id = link.rstrip('/').split('/')[-1]

            # Check if video already exists (no duplicates, no overwriting)
            cursor.execute("SELECT id FROM video_data WHERE id = ?", (video_id,))
            if cursor.fetchone():
                stats["skipped"] += 1
                continue

            # Parse the date string to timestamp
            date_str = video.get("date") or video.get("Date")
            date_favorited = None
            if date_str:
                try:
                    dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                    date_favorited = int(dt.timestamp())
                except ValueError:
                    pass

            # Insert into database with minimal info
            # Most fields are NULL and will be filled during download
            cursor.execute("""
                INSERT INTO video_data (
                    id, title, uploader, uploader_id, desc, create_time,
                    duration, tiktok_url, download_status, transcription_status,
                    transcription, date_favorited, video_is_deleted, video_is_private
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, 0, 0)
            """, (
                video_id,
                None,  # title - will be filled on download
                None,  # uploader - will be filled on download
                None,  # uploader_id - will be filled on download
                None,  # desc - will be filled on download
                None,  # create_time - will be filled on download
                None,  # duration - will be filled on download
                link,  # tiktok_url - from TikTok export
                None,  # transcription - will be filled later
                date_favorited  # date_favorited - when you favorited it (as timestamp)
            ))

            stats["inserted"] += 1

        except Exception as e:
            print(f"Error processing video {link}: {e}")
            stats["errors"] += 1

    conn.commit()
    conn.close()

    return stats

# Pipeline: Download video ─────────────────────────────────► Store in db
#                 │                                               ▲
#                 │                                               │
#                 │                                               │
#                 └─────────────► Transcribe video ───────────────┘

async def download_video_and_store(video_ids, tiktok_api=None):
    """
    Downloads videos, then immediately stores them in the database as BLOBs.
    Routes to download_image_post if it's an image collection.

    Args:
        video_ids: list of video ids to download
        tiktok_api: Optional TikTokApi session. If None, creates a new session.

    Returns:
        list of dicts with status and message for each video
    """
    results = []

    # Determine which API to use
    if tiktok_api:
        tt_api = tiktok_api
    else:
        ms_token = os.environ.get("ms_token", None)
        tt_api = TikTokApi()
        await tt_api.create_sessions(ms_tokens=[ms_token], num_sessions=1, sleep_after=3)

    # Loop through all videos
    for video_id in video_ids:
        conn = get_connection()
        cursor = conn.cursor()

        try:
            # Get video URL from database
            cursor.execute("SELECT tiktok_url FROM video_data WHERE id = ?", (video_id,))
            result = cursor.fetchone()
            if not result:
                conn.close()
                results.append({"status": "error", "message": f"Video {video_id} not found in database"})
                continue

            tiktok_url = result[0]

            # Convert share URL to proper format
            tiktok_url = tiktok_url.replace('tiktokv', 'tiktok').replace('share', '@')

            # Get video metadata
            video = tt_api.video(url=tiktok_url)
            video_info = await video.info()

            # Check if it's an image post - pawn off to image handler
            if "imagePost" in video_info:
                conn.close()
                image_result = await download_image_post([video_id], tt_api)
                results.append(image_result[0])
                continue

            # Extract metadata
            author = video_info.get('author', {})
            title = video_info.get('music', {}).get('title', '')
            uploader = author.get('uniqueId') or author.get('nickname', '')
            uploader_id = author.get('uniqueId', '')
            desc = video_info.get('desc', '')
            create_time = int(video_info.get('createTime', 0))
            duration = video_info.get('video', {}).get('duration', 0)

            # Download video bytes
            video_bytes = await video.bytes()
            download_timestamp = int(time.time())

            # Update video_data table with metadata
            cursor.execute("""
                UPDATE video_data
                SET title = ?, uploader = ?, uploader_id = ?, desc = ?,
                    create_time = ?, duration = ?, content_type = ?,
                    download_status = 1
                WHERE id = ?
            """, (title, uploader, uploader_id, desc, create_time, duration,
                  "video", video_id))

            # Insert video BLOB into videos table
            cursor.execute("""
                INSERT INTO videos (id, video_blob, date_downloaded)
                VALUES (?, ?, ?)
            """, (video_id, video_bytes, download_timestamp))

            conn.commit()
            conn.close()

            results.append({"status": "success", "video_id": video_id, "size_bytes": len(video_bytes)})

        except Exception as e:
            error_str = str(e).lower()

            if "deleted" in error_str or "removed" in error_str:
                cursor.execute("UPDATE video_data SET video_is_deleted = 1 WHERE id = ?", (video_id,))
                conn.commit()
                conn.close()
                results.append({"status": "deleted", "message": str(e)})

            elif "private" in error_str or "unavailable" in error_str:
                cursor.execute("UPDATE video_data SET video_is_private = 1 WHERE id = ?", (video_id,))
                conn.commit()
                conn.close()
                results.append({"status": "private", "message": str(e)})

            else:
                conn.close()
                results.append({"status": "error", "message": str(e)})

    return results


async def download_image_post(video_ids, tiktok_api=None):
    """
    Downloads image post collections and stores them as ZIP BLOBs in the database.

    Args:
        video_ids: list of video ids to download
        tiktok_api: Optional TikTokApi session. If None, creates a new session.

    Returns:
        list of dicts with status and message for each video
    """
    import zipfile
    from io import BytesIO
    import requests

    results = []

    # Determine which API to use
    if tiktok_api:
        tt_api = tiktok_api
    else:
        ms_token = os.environ.get("ms_token", None)
        tt_api = TikTokApi()
        await tt_api.create_sessions(ms_tokens=[ms_token], num_sessions=1, sleep_after=3)

    # Loop through all videos
    for video_id in video_ids:
        conn = get_connection()
        cursor = conn.cursor()

        try:
            # Get video URL from database
            cursor.execute("SELECT tiktok_url FROM video_data WHERE id = ?", (video_id,))
            result = cursor.fetchone()
            if not result:
                conn.close()
                results.append({"status": "error", "message": f"Video {video_id} not found in database"})
                continue

            tiktok_url = result[0]

            # Convert share URL to proper format
            tiktok_url = tiktok_url.replace('tiktokv', 'tiktok').replace('share', '@')

            # Get video metadata
            video = tt_api.video(url=tiktok_url)
            video_info = await video.info()

            # Check if it's actually an image post
            if "imagePost" not in video_info:
                conn.close()
                results.append({"status": "error", "message": "This is not an image post"})
                continue

            # Extract metadata
            author = video_info.get('author', {})
            title = video_info.get('music', {}).get('title', '')
            uploader = author.get('uniqueId') or author.get('nickname', '')
            uploader_id = author.get('uniqueId', '')
            desc = video_info.get('desc', '')
            create_time = int(video_info.get('createTime', 0))

            # Download images
            images = video_info["imagePost"]["images"]
            image_data = []

            with requests.Session() as s:
                for imageDict in images:
                    imgUrl = imageDict["imageURL"]["urlList"][0]
                    image_data.append(s.get(imgUrl).content)

            # Create ZIP archive in memory
            zip_buffer = BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                for index, image_bytes in enumerate(image_data):
                    zip_file.writestr(f"{index}.jpeg", image_bytes)

            zip_blob = zip_buffer.getvalue()
            download_timestamp = int(time.time())

            # Update video_data table with metadata
            cursor.execute("""
                UPDATE video_data
                SET title = ?, uploader = ?, uploader_id = ?, desc = ?,
                    create_time = ?, content_type = ?, download_status = 1
                WHERE id = ?
            """, (title, uploader, uploader_id, desc, create_time,
                  "images", video_id))

            # Insert ZIP BLOB into videos table
            cursor.execute("""
                INSERT INTO videos (id, video_blob, date_downloaded)
                VALUES (?, ?, ?)
            """, (video_id, zip_blob, download_timestamp))

            conn.commit()
            conn.close()

            results.append({"status": "success", "video_id": video_id, "image_count": len(image_data), "size_bytes": len(zip_blob)})

        except Exception as e:
            error_str = str(e).lower()

            if "deleted" in error_str or "removed" in error_str:
                cursor.execute("UPDATE video_data SET video_is_deleted = 1 WHERE id = ?", (video_id,))
                conn.commit()
                conn.close()
                results.append({"status": "deleted", "message": str(e)})

            elif "private" in error_str or "unavailable" in error_str:
                cursor.execute("UPDATE video_data SET video_is_private = 1 WHERE id = ?", (video_id,))
                conn.commit()
                conn.close()
                results.append({"status": "private", "message": str(e)})

            else:
                conn.close()
                results.append({"status": "error", "message": str(e)})

    return results


def transcribe_video(video_id, bytes_stream):
    """
    Transcribes a video, then stores the transcription in the database, marking the transcription flag as TRUE.

    Args:
        video_id: video id from the database
        bytes_stream: bytesio object of the video bytes

    Returns:
        transcription: a string that is the transcription of the video.

    """
    from openai import OpenAI
    from io import BytesIO

    client = OpenAI()

    # Create BytesIO object if bytes passed directly
    if isinstance(bytes_stream, bytes):
        video_file = BytesIO(bytes_stream)
    else:
        video_file = bytes_stream

    # Whisper needs a filename hint for format detection
    video_file.name = "video.mp4"

    # Transcribe using Whisper
    transcript = client.audio.transcriptions.create(
        model="whisper-1",
        file=video_file
    )

    transcription_text = transcript.text

    # Store transcription in database
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
                   UPDATE video_data
                   SET transcription        = ?,
                       transcription_status = 1
                   WHERE id = ?
                   """, (transcription_text, video_id))

    conn.commit()
    conn.close()

    return transcription_text