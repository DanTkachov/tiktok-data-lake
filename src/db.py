import gc
import json
import os
import sqlite3
import tempfile
import time
import zipfile
from datetime import datetime
from io import BytesIO
from pathlib import Path
import requests
from TikTokApi import TikTokApi
from faster_whisper import WhisperModel
import asyncio


db_path_real = Path(__file__).parent.parent / "db" / "tiktok_archive_real.db"
db_path_mock = Path(__file__).parent.parent / "db" / "tiktok_archive_mock.db"

DB_PATH = db_path_mock


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

        # Enable WAL mode for concurrent access (allows readers and writers simultaneously)
        cursor.execute("PRAGMA journal_mode=WAL;")
        cursor.execute("PRAGMA synchronous=NORMAL;")  # Faster writes, still safe with WAL
        cursor.execute("PRAGMA busy_timeout=5000;")   # Wait 5s if DB is locked instead of failing immediately

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
             video_is_private BOOLEAN DEFAULT 0,
             video_has_error BOOLEAN DEFAULT 0
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
    conn = sqlite3.connect(DB_PATH)
    # Set busy timeout for this connection (WAL mode is persistent once set)
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def ingest_json(json_file):
    """
    Process the tiktok json that is exported when you ask for your data.
    Used to populate the database that stores your favorited videos.

    Args:
        json_file: Path to JSON file direct from TikTok.
    Returns:
        Dictionary with statistics about the ingestion process
    """


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

async def download_video_and_store(video_ids, tiktok_api=None, whisper_model=None):
    """
    Downloads videos, then immediately stores them in the database as BLOBs.
    Routes to download_image_post if it's an image collection.

    Args:
        video_ids: list of video ids to download
        tiktok_api: Optional TikTokApi session. If None, creates a new session.
        whisper_model: Optional WhisperModel instance. If None, creates a new one.

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
    conn = get_connection()

    for video_id in video_ids:
        cursor = conn.cursor()

        try:
            # Get video URL from database
            print(f"🔍 Querying database for video {video_id}")
            cursor.execute("SELECT tiktok_url FROM video_data WHERE id = ?", (video_id,))
            result = cursor.fetchone()
            if not result:
                results.append({"status": "error", "message": f"Video {video_id} not found in database"})
                continue

            tiktok_url = result[0]
            print(f"📍 Found URL: {tiktok_url}")

            # Convert share URL to proper format
            tiktok_url = tiktok_url.replace('tiktokv', 'tiktok').replace('share', '@')
            print(f"🔄 Converted URL: {tiktok_url}")

            # Get video metadata
            print(f"🌐 Fetching video info from TikTok API...")
            video = tt_api.video(url=tiktok_url)

            # Add timeout to prevent infinite hang
            try:
                video_info = await asyncio.wait_for(video.info(), timeout=30.0)
                print(f"✅ Got video info!")
            except asyncio.TimeoutError:
                print(f"❌ Timeout fetching video info after 30 seconds")
                raise Exception("TikTok API timeout - possible auth or rate limit issue")

            # Check if it's an image post - pawn off to image handler
            if "imagePost" in video_info:
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

            # Immediately transcribe video
            # Actually don't, so that redis can use it.
            # _ = transcribe_video(video_id, video_bytes, whisper_model)

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

            results.append({"status": "success", "video_id": video_id, "size_bytes": len(video_bytes)})

        except Exception as e:
            error_str = str(e).lower()

            # Try alternative download method before giving up
            if "imagepost" not in error_str:
                try:
                    alt_result = await alt_video_download(video_id, tt_api, whisper_model)
                    if alt_result.get('status') == 'success':
                        results.append(alt_result)
                        continue
                except Exception as alt_e:
                    pass

            # If alternative method also failed, categorize the error
            conn = get_connection()
            cursor = conn.cursor()

            if "deleted" in error_str or "removed" in error_str:
                cursor.execute("UPDATE video_data SET video_is_deleted = 1, video_has_error = 1 WHERE id = ?", (video_id,))
                conn.commit()
                results.append({"status": "deleted", "message": str(e)})

            elif "private" in error_str or "unavailable" in error_str:
                cursor.execute("UPDATE video_data SET video_is_private = 1, video_has_error = 1 WHERE id = ?", (video_id,))
                conn.commit()
                results.append({"status": "private", "message": str(e)})

            else:
                cursor.execute("UPDATE video_data SET video_has_error = 1 WHERE id = ?", (video_id,))
                conn.commit()
                results.append({"status": "error", "message": str(e)})
        finally:
            conn.close()

    gc.collect()
    return results


async def alt_video_download(video_id, tiktok_api, whisper_model):
    """
    Alternative video download method using bitrate URLs directly.
    Based on https://github.com/financiallyruined/TikTok-Multi-Downloader

    Args:
        video_id: video id from the database
        tiktok_api: TikTokApi session
        whisper_model: WhisperModel instance

    Returns:
        dict with status and message
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Get video URL from database
        cursor.execute("SELECT tiktok_url FROM video_data WHERE id = ?", (video_id,))
        result = cursor.fetchone()
        if not result:
            conn.close()
            return {"status": "error", "message": f"Video {video_id} not found in database"}

        tiktok_url = result[0]

        # Convert share URL to proper format
        tiktok_url = tiktok_url.replace('tiktokv', 'tiktok').replace('share', '@')

        # Get video metadata
        video = tiktok_api.video(url=tiktok_url)
        video_info = await video.info()

        # Extract metadata
        author = video_info.get('author', {})
        title = video_info.get('music', {}).get('title', '')
        uploader = author.get('uniqueId') or author.get('nickname', '')
        uploader_id = author.get('uniqueId', '')
        desc = video_info.get('desc', '')
        create_time = int(video_info.get('createTime', 0))
        duration = video_info.get('video', {}).get('duration', 0)

        # Try alternative download method using bitrate URLs
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.5',
            'HX-Request': 'true',
            'HX-Trigger': 'search-btn',
            'HX-Target': 'tiktok-parse-result',
            'HX-Current-URL': 'https://tiktokio.com/',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://tiktokio.com',
            'Connection': 'keep-alive',
            'Referer': 'https://tiktokio.com/'
        }

        # Get alternative video URLs from bitrate info
        altVideoUrls = video_info["video"]["bitrateInfo"][0]["PlayAddr"]["UrlList"]

        video_bytes = None
        for url in altVideoUrls:
            if url.startswith("https://www.tiktok.com"):
                response = requests.get(url, headers=headers, stream=True)
                video_bytes = response.content
                break

        if not video_bytes:
            conn.close()
            return {"status": "error", "message": "No valid download URL found in bitrate info"}

        download_timestamp = int(time.time())

        # Transcribe video
        _ = transcribe_video(video_id, video_bytes, whisper_model)

        # Update video_data table with metadata and clear error flag
        cursor.execute("""
            UPDATE video_data
            SET title = ?, uploader = ?, uploader_id = ?, desc = ?,
                create_time = ?, duration = ?, content_type = ?,
                download_status = 1, video_has_error = 0
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

        return {"status": "success", "video_id": video_id, "size_bytes": len(video_bytes)}

    except Exception as e:
        error_str = str(e).lower()

        if "deleted" in error_str or "removed" in error_str:
            cursor.execute("UPDATE video_data SET video_is_deleted = 1, video_has_error = 1 WHERE id = ?", (video_id,))
            conn.commit()
            conn.close()
            return {"status": "deleted", "message": str(e)}

        elif "private" in error_str or "unavailable" in error_str:
            cursor.execute("UPDATE video_data SET video_is_private = 1, video_has_error = 1 WHERE id = ?", (video_id,))
            conn.commit()
            conn.close()
            return {"status": "private", "message": str(e)}

        else:
            cursor.execute("UPDATE video_data SET video_has_error = 1 WHERE id = ?", (video_id,))
            conn.commit()
            conn.close()
            return {"status": "error", "message": str(e)}


async def download_image_post(video_ids, tiktok_api=None):
    """
    Downloads image post collections and stores them as ZIP BLOBs in the database.

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
                cursor.execute("UPDATE video_data SET video_is_deleted = 1, video_has_error = 1 WHERE id = ?", (video_id,))
                conn.commit()
                conn.close()
                results.append({"status": "deleted", "message": str(e)})

            elif "private" in error_str or "unavailable" in error_str:
                cursor.execute("UPDATE video_data SET video_is_private = 1, video_has_error = 1 WHERE id = ?", (video_id,))
                conn.commit()
                conn.close()
                results.append({"status": "private", "message": str(e)})

            else:
                cursor.execute("UPDATE video_data SET video_has_error = 1 WHERE id = ?", (video_id,))
                conn.commit()
                conn.close()
                results.append({"status": "error", "message": str(e)})

    return results


def transcribe_video(video_id, bytes_stream, whisper_model=None):
    """
    Transcribes a video, then stores the transcription in the database, marking the transcription flag as TRUE.

    Args:
        video_id: video id from the database
        bytes_stream: bytesio object of the video bytes
        whisper_model: Optional WhisperModel instance. If None, creates a new one.

    Returns:
        transcription: a string that is the transcription of the video.

    """

    # Get bytes from stream
    if isinstance(bytes_stream, bytes):
        video_bytes = bytes_stream
    else:
        video_bytes = bytes_stream.read()

    # Write to temporary file
    with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as temp_file:
        temp_file.write(video_bytes)
        temp_path = temp_file.name

    # Determine which model to use
    if whisper_model:
        model = whisper_model
    else:
        # Detect CUDA availability
        # if torch.cuda.is_available():
        #     device = "cuda"
        #     compute_type = "float16"
        # else:
        #     device = "cpu"
        #     compute_type = "int8"
        device = "cpu"
        compute_type = "int8"

        # Load model (base model is a good balance of speed/accuracy)
        model = WhisperModel("base", device=device, compute_type=compute_type)

    # Transcribe
    segments, info = model.transcribe(temp_path, beam_size=5)

    # Combine all segments into one text
    transcription_text = " ".join([segment.text for segment in segments])

    # Clean up temp file
    os.unlink(temp_path)

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


def cleanup_error_flags():
    """
    Cleans up stale error flags for successfully downloaded videos.
    If a video has download_status = 1 but video_has_error = 1,
    this clears the error flag.

    Args:
        None

    Returns:
        Number of videos cleaned up
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE video_data
        SET video_has_error = 0
        WHERE download_status = 1 AND video_has_error = 1
    """)

    updated_count = cursor.rowcount
    conn.commit()
    conn.close()

    print(f"Cleaned up error flags for {updated_count} videos")
    return updated_count