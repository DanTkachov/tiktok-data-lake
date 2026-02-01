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
import cv2
import numpy as np
from PIL import Image


db_path_real = Path(__file__).parent.parent / "db" / "tiktok_archive_real.db"
db_path_mock = Path(__file__).parent.parent / "db" / "tiktok_archive_mock.db"
db_path_mock_100 = Path(__file__).parent.parent / "db" / "tiktok_archive_mock_100.db"


DB_PATH = db_path_mock_100


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
                ocr_status BOOLEAN DEFAULT 0,
                ocr TEXT,
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
             thumbnail_blob BLOB,
             date_downloaded INTEGER,
             FOREIGN KEY (id) REFERENCES video_data(id)
           )
           """)

        # tags: stores tags for videos (many-to-many relationship)
        # Each row is one tag - can be either automatic (from ML) or manual (from user)
        cursor.execute("""
           CREATE TABLE IF NOT EXISTS tags (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             video_id TEXT NOT NULL,
             automatic_tag TEXT,
             manual_tag TEXT,
             confidence REAL,
             date_added INTEGER,
             FOREIGN KEY (video_id) REFERENCES video_data(id)
           )
           """)

        # Create index for efficient tag searching
        cursor.execute("""
           CREATE INDEX IF NOT EXISTS idx_tags_automatic_tag ON tags(automatic_tag)
           """)

        cursor.execute("""
           CREATE INDEX IF NOT EXISTS idx_tags_manual_tag ON tags(manual_tag)
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


def extract_video_thumbnail(video_bytes, target_width=320):
    """
    Extract the first frame from a video and return it as a JPEG thumbnail.

    Args:
        video_bytes: bytes object containing the video data
        target_width: desired width for the thumbnail (height scaled proportionally)

    Returns:
        bytes: JPEG-encoded thumbnail image
    """
    # Write video bytes to temporary file
    with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as temp_file:
        temp_file.write(video_bytes)
        temp_path = temp_file.name

    try:
        # Open video with opencv
        cap = cv2.VideoCapture(temp_path)

        # Read first frame
        ret, frame = cap.read()
        cap.release()

        if not ret or frame is None:
            raise ValueError("Could not read first frame from video")

        # Calculate new dimensions maintaining aspect ratio
        height, width = frame.shape[:2]
        aspect_ratio = height / width
        target_height = int(target_width * aspect_ratio)

        # Resize frame
        resized = cv2.resize(frame, (target_width, target_height), interpolation=cv2.INTER_AREA)

        # Convert BGR to RGB (opencv uses BGR)
        rgb_frame = cv2.cvtColor(resized, cv2.COLOR_BGR2RGB)

        # Convert to PIL Image
        pil_image = Image.fromarray(rgb_frame)

        # Encode as JPEG with quality 85
        output = BytesIO()
        pil_image.save(output, format='JPEG', quality=85, optimize=True)
        thumbnail_bytes = output.getvalue()

        return thumbnail_bytes

    finally:
        # Clean up temp file
        os.unlink(temp_path)


def extract_image_thumbnail(zip_bytes, target_width=320):
    """
    Extract the first image from a ZIP archive and return it as a JPEG thumbnail.

    Args:
        zip_bytes: bytes object containing the ZIP archive
        target_width: desired width for the thumbnail (height scaled proportionally)

    Returns:
        bytes: JPEG-encoded thumbnail image
    """
    with zipfile.ZipFile(BytesIO(zip_bytes), 'r') as zip_file:
        # Get sorted list of image files
        image_files = sorted([
            name for name in zip_file.namelist()
            if name.lower().endswith(('.jpg', '.jpeg', '.png'))
        ])

        if not image_files:
            raise ValueError("No image files found in ZIP")

        # Read first image
        first_image_bytes = zip_file.read(image_files[0])

        # Open with PIL
        pil_image = Image.open(BytesIO(first_image_bytes))

        # Convert to RGB if needed (handles RGBA, grayscale, etc.)
        if pil_image.mode != 'RGB':
            pil_image = pil_image.convert('RGB')

        # Calculate new dimensions maintaining aspect ratio
        width, height = pil_image.size
        aspect_ratio = height / width
        target_height = int(target_width * aspect_ratio)

        # Resize image
        pil_image = pil_image.resize((target_width, target_height), Image.Resampling.LANCZOS)

        # Encode as JPEG with quality 85
        output = BytesIO()
        pil_image.save(output, format='JPEG', quality=85, optimize=True)
        thumbnail_bytes = output.getvalue()

        return thumbnail_bytes


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
                    transcription, ocr_status, ocr, date_favorited, video_is_deleted, video_is_private
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, 0, ?, ?, 0, 0)
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
                None,  # ocr - will be filled later
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

async def download_video_without_watermark(video_info):
    """
    Downloads a TikTok video WITHOUT watermark using multiple fallback methods.

    Priority order:
    1. bitrateInfo PlayAddr URLs - highest quality, no watermark
    2. playAddr field - standard quality, no watermark
    3. hdplay field - HD quality, no watermark

    Args:
        video_info: Dictionary containing video metadata from TikTok API

    Returns:
        bytes: Video data without watermark

    Raises:
        Exception: If all download methods fail
    """

    # Debug: Log the structure of video_info to help diagnose issues
    try:
        video_data = video_info.get('video', {})
        print(f"  🔍 DEBUG: video_info keys: {list(video_info.keys())}")
        print(f"  🔍 DEBUG: video_data keys: {list(video_data.keys())}")
    except Exception as debug_error:
        print(f"  ⚠️  DEBUG: Could not inspect video_info structure: {debug_error}")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://www.tiktok.com/',
        'Origin': 'https://www.tiktok.com',
        'Connection': 'keep-alive',
    }

    # Method 1: Try bitrateInfo PlayAddr URLs (best quality)
    try:
        print(f"  🎯 Method 1: Trying bitrateInfo PlayAddr URLs...")
        video_data = video_info.get('video', {})
        bitrate_info = video_data.get('bitrateInfo', [])

        if bitrate_info:
            # Try each bitrate option (usually sorted by quality)
            for idx, bitrate_option in enumerate(bitrate_info):
                play_addr = bitrate_option.get('PlayAddr', {})
                url_list = play_addr.get('UrlList', [])

                print(f"    Trying bitrate option {idx + 1}/{len(bitrate_info)} with {len(url_list)} URLs...")

                # Try all URLs in the list
                for url_idx, url in enumerate(url_list):
                    try:
                        print(f"      Attempting URL {url_idx + 1}/{len(url_list)}: {url[:50]}...")
                        response = requests.get(url, headers=headers, timeout=30)

                        if response.status_code == 200 and len(response.content) > 1000:
                            print(f"  ✅ Success with bitrateInfo method! Size: {len(response.content)} bytes")
                            return response.content
                        else:
                            print(f"      ⚠️  Bad response: status={response.status_code}, size={len(response.content)}")
                            # Debug: Show response content for troubleshooting
                            if len(response.content) < 2000:
                                print(f"      🔍 Response content: {response.content[:500]}")
                    except Exception as url_error:
                        print(f"      ⚠️  URL failed: {str(url_error)[:100]}")
                        continue
    except Exception as e:
        print(f"  ⚠️  bitrateInfo method failed: {str(e)[:100]}")

    # Method 2: Try playAddr field
    try:
        print(f"  🎯 Method 2: Trying playAddr field...")
        video_data = video_info.get('video', {})
        play_addr = video_data.get('playAddr') or video_data.get('play_addr')

        if play_addr:
            # playAddr might be a string URL or a dict with UrlList
            if isinstance(play_addr, str):
                url_list = [play_addr]
            elif isinstance(play_addr, dict):
                url_list = play_addr.get('UrlList', [])
            else:
                url_list = []

            for url_idx, url in enumerate(url_list):
                try:
                    print(f"    Attempting URL {url_idx + 1}/{len(url_list)}: {url[:50]}...")
                    response = requests.get(url, headers=headers, timeout=30)

                    if response.status_code == 200 and len(response.content) > 1000:
                        print(f"  ✅ Success with playAddr method! Size: {len(response.content)} bytes")
                        return response.content
                    else:
                        print(f"    ⚠️  Bad response: status={response.status_code}, size={len(response.content)}")
                except Exception as url_error:
                    print(f"    ⚠️  URL failed: {str(url_error)[:50]}")
                    continue
    except Exception as e:
        print(f"  ⚠️  playAddr method failed: {str(e)[:100]}")

    # Method 3: Try hdplay field
    try:
        print(f"  🎯 Method 3: Trying hdplay field...")
        video_data = video_info.get('video', {})
        hdplay = video_data.get('hdplay') or video_data.get('hdPlay')

        if hdplay:
            # hdplay might be a string URL or a dict with UrlList
            if isinstance(hdplay, str):
                url_list = [hdplay]
            elif isinstance(hdplay, dict):
                url_list = hdplay.get('UrlList', [])
            else:
                url_list = []

            for url_idx, url in enumerate(url_list):
                try:
                    print(f"    Attempting URL {url_idx + 1}/{len(url_list)}: {url[:50]}...")
                    response = requests.get(url, headers=headers, timeout=30)

                    if response.status_code == 200 and len(response.content) > 1000:
                        print(f"  ✅ Success with hdplay method! Size: {len(response.content)} bytes")
                        return response.content
                    else:
                        print(f"    ⚠️  Bad response: status={response.status_code}, size={len(response.content)}")
                except Exception as url_error:
                    print(f"    ⚠️  URL failed: {str(url_error)[:50]}")
                    continue
    except Exception as e:
        print(f"  ⚠️  hdplay method failed: {str(e)[:100]}")

    # All methods failed
    raise Exception("Could not download video without watermark - all methods failed")


async def download_video_and_store(video_ids, tiktok_api=None, whisper_model=None):
    """
    Downloads videos WITHOUT WATERMARK, then immediately stores them in the database as BLOBs.
    Routes to download_image_post if it's an image collection.

    Uses watermark-free download methods as primary approach:
    1. bitrateInfo PlayAddr URLs (highest quality, no watermark)
    2. playAddr field (clean video URL)
    3. hdplay field (HD quality without watermark)
    4. Falls back to standard video.bytes() if needed

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

            # Download video bytes WITHOUT WATERMARK using new method
            print(f"📥 Downloading video without watermark...")
            video_bytes = await download_video_without_watermark(video_info)
            download_timestamp = int(time.time())

            # Generate thumbnail from first frame
            try:
                thumbnail_bytes = extract_video_thumbnail(video_bytes)
            except Exception as thumb_error:
                print(f"⚠️  Warning: Could not generate thumbnail for {video_id}: {thumb_error}")
                thumbnail_bytes = None

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
                INSERT INTO videos (id, video_blob, date_downloaded, thumbnail_blob)
                VALUES (?, ?, ?, ?)
            """, (video_id, video_bytes, download_timestamp, thumbnail_bytes))

            conn.commit()

            results.append({"status": "success", "video_id": video_id, "size_bytes": len(video_bytes)})

        except Exception as e:
            error_str = str(e).lower()

            # If watermark-free method failed, categorize the error
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

        # Generate thumbnail from first frame
        try:
            thumbnail_bytes = extract_video_thumbnail(video_bytes)
        except Exception as thumb_error:
            print(f"⚠️  Warning: Could not generate thumbnail for {video_id}: {thumb_error}")
            thumbnail_bytes = None

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
            INSERT INTO videos (id, video_blob, date_downloaded, thumbnail_blob)
            VALUES (?, ?, ?, ?)
        """, (video_id, video_bytes, download_timestamp, thumbnail_bytes))

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

            # Insert ZIP BLOB into videos table (no thumbnail for image posts)
            cursor.execute("""
                INSERT INTO videos (id, video_blob, date_downloaded, thumbnail_blob)
                VALUES (?, ?, ?, ?)
            """, (video_id, zip_blob, download_timestamp, None))

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


def ocr_images(video_id, bytes_stream, ocr_model=None):
    """
    Performs OCR on images from a ZIP archive, then stores the OCR text in the database,
    marking the ocr_status flag as TRUE.

    Args:
        video_id: video id from the database
        bytes_stream: bytes object or BytesIO of the ZIP containing images
        ocr_model: Optional RapidOCR instance. If None, creates a new one.

    Returns:
        ocr_text: a string that is the concatenated OCR text from all images.

    """
    # Get bytes from stream
    if isinstance(bytes_stream, bytes):
        zip_bytes = bytes_stream
    else:
        zip_bytes = bytes_stream.read()

    # Determine which model to use
    if ocr_model:
        model = ocr_model
    else:
        # Import RapidOCR here to avoid loading at module level
        from rapidocr_onnxruntime import RapidOCR

        # Initialize RapidOCR (uses ONNX runtime, much more stable than PaddleOCR)
        model = RapidOCR()

    # Extract images from ZIP and perform OCR
    all_ocr_text = []

    with zipfile.ZipFile(BytesIO(zip_bytes), 'r') as zip_file:
        # Sort filenames to maintain consistent order
        image_names = sorted(zip_file.namelist())

        for image_name in image_names:
            try:
                # Read image bytes from ZIP
                image_bytes = zip_file.read(image_name)

                # Perform OCR directly on raw bytes (RapidOCR handles decoding internally)
                # This preserves alpha channel and image quality better than manual decoding
                # RapidOCR returns: (result, elapse_time)
                result, elapse = model(image_bytes)

                # Extract text from OCR result
                # result format: [[bbox, text, confidence], ...]
                if result:
                    for item in result:
                        # RapidOCR returns: [bbox, text, confidence]
                        bbox, text, confidence = item[0], item[1], item[2]

                        # Only include text with reasonable confidence
                        if confidence > 0.5 and text:
                            all_ocr_text.append(text)

            except Exception as e:
                print(f"⚠️  Error processing image {image_name}: {e}")
                continue

    # Combine all OCR text with spaces
    ocr_text = " ".join(all_ocr_text)

    # Store OCR text in database
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE video_data
        SET ocr = ?,
            ocr_status = 1
        WHERE id = ?
    """, (ocr_text, video_id))

    conn.commit()
    conn.close()

    return ocr_text


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