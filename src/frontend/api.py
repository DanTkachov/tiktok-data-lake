"""
FastAPI application for TikTok Data Lake Frontend

Provides API endpoints for browsing, searching, and streaming videos from the database.
"""

import sys
from pathlib import Path

# Add parent directory to path to import db module
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi import FastAPI, HTTPException, Query, UploadFile, File
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request
import sqlite3
from datetime import datetime
from typing import Optional, List, Dict, Any
import json

from db import get_connection, DB_PATH, init_database

# Fix import path for tasks module
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
import src.tasks as tasks_module

app = FastAPI(
    title="TikTok Data Lake",
    description="Browse and search your TikTok video archive",
    version="1.0.0",
)

# Mount static files
app.mount(
    "/static",
    StaticFiles(directory=str(Path(__file__).parent / "static")),
    name="static",
)

# Templates
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


def format_timestamp(ts: Optional[int]) -> str:
    """Convert Unix timestamp to readable date string."""
    if not ts:
        return "Unknown"
    try:
        dt = datetime.fromtimestamp(ts)
        return dt.strftime("%b %d, %Y")
    except:
        return "Unknown"


def format_duration(seconds: Optional[int]) -> str:
    """Format duration in seconds to readable string."""
    if not seconds:
        return "0:00"
    minutes = seconds // 60
    secs = seconds % 60
    return f"{minutes}:{secs:02d}"


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    """Serve the main frontend page."""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/stats")
async def get_stats():
    """
    Get database statistics.

    Returns counts of total videos, downloaded, transcribed, and OCR'd videos.
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Total videos in database
        cursor.execute("SELECT COUNT(*) FROM video_data")
        total = cursor.fetchone()[0]

        # Downloaded videos
        cursor.execute("SELECT COUNT(*) FROM video_data WHERE download_status = 1")
        downloaded = cursor.fetchone()[0]

        # Transcribed videos (only video content)
        cursor.execute("""
            SELECT COUNT(*) FROM video_data 
            WHERE transcription_status = 1 AND content_type = 'video'
        """)
        transcribed = cursor.fetchone()[0]

        # OCR'd videos (only image posts)
        cursor.execute("""
            SELECT COUNT(*) FROM video_data 
            WHERE ocr_status = 1 AND content_type = 'images'
        """)
        ocr = cursor.fetchone()[0]

        return {
            "total": total,
            "downloaded": downloaded,
            "transcribed": transcribed,
            "ocr": ocr,
        }

    finally:
        conn.close()


@app.get("/api/videos")
async def get_videos(
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    limit: int = Query(250, ge=1, le=500, description="Number of results per page"),
    content_type: Optional[str] = Query(
        None, description="Filter by content type: 'video' or 'images'"
    ),
    download_status: Optional[str] = Query(
        None, description="Filter by download status: 'downloaded' or 'not_downloaded'"
    ),
    transcription_status: Optional[str] = Query(
        None, description="Filter by transcription: 'transcribed' or 'not_transcribed'"
    ),
    ocr_status: Optional[str] = Query(
        None, description="Filter by OCR: 'ocr' or 'not_ocr'"
    ),
    tags_status: Optional[str] = Query(
        None, description="Filter by tag status: 'tagged' or 'untagged'"
    ),
    tags: Optional[List[str]] = Query(
        None,
        description="Filter by tags (AND logic - video must have ALL selected tags)",
    ),
):
    """
    Get a paginated list of all videos.

    Returns video metadata including title, creator, dates, and content type.
    Supports filtering by content type, download status, transcription status, and OCR status.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Calculate offset
    offset = (page - 1) * limit

    try:
        # Build query based on filters
        where_clause = "WHERE 1=1"
        params = []

        # Content type filter
        if content_type:
            where_clause += " AND v.content_type = ?"
            params.append(content_type)

        # Download status filter
        if download_status:
            if download_status == "downloaded":
                where_clause += " AND v.download_status = 1"
            elif download_status == "not_downloaded":
                where_clause += " AND v.download_status = 0"

        # Transcription status filter
        if transcription_status:
            if transcription_status == "transcribed":
                where_clause += " AND v.transcription_status = 1"
            elif transcription_status == "not_transcribed":
                where_clause += " AND v.transcription_status = 0"

        # OCR status filter
        if ocr_status:
            if ocr_status == "ocr":
                where_clause += " AND v.ocr_status = 1"
            elif ocr_status == "not_ocr":
                where_clause += " AND v.ocr_status = 0"

        # Tags status filter (tagged/untagged)
        if tags_status:
            if tags_status == "tagged":
                where_clause += " AND v.id IN (SELECT DISTINCT video_id FROM tags WHERE manual_tag IS NOT NULL)"
            elif tags_status == "untagged":
                where_clause += " AND v.id NOT IN (SELECT DISTINCT video_id FROM tags WHERE manual_tag IS NOT NULL)"

        # Tags filter (AND logic - video must have ALL specified tags)
        if tags:
            # Create placeholders for the IN clause
            placeholders = ", ".join(["?"] * len(tags))
            where_clause += f""" AND v.id IN (
                SELECT video_id 
                FROM tags 
                WHERE manual_tag IN ({placeholders})
                GROUP BY video_id 
                HAVING COUNT(DISTINCT manual_tag) = ?
            )"""
            params.extend(tags)
            params.append(len(tags))

        # Get total count
        count_query = f"SELECT COUNT(*) FROM video_data v {where_clause}"
        cursor.execute(count_query, params)
        total = cursor.fetchone()[0]

        # Get videos with pagination
        query_params = params + [limit, offset]
        cursor.execute(
            f"""
            SELECT 
                v.id,
                v.title,
                v.uploader,
                v.uploader_id,
                v.desc,
                v.create_time,
                v.duration,
                v.tiktok_url,
                v.content_type,
                v.transcription_status,
                v.ocr_status,
                v.date_favorited,
                v.video_is_deleted,
                v.video_is_private,
                v.download_status
            FROM video_data v
            {where_clause}
            ORDER BY v.date_favorited DESC NULLS LAST, v.create_time DESC
            LIMIT ? OFFSET ?
        """,
            query_params,
        )

        rows = cursor.fetchall()

        videos = []
        for row in rows:
            video = {
                "id": row[0],
                "title": row[1] or "Untitled",
                "uploader": row[2] or "Unknown",
                "uploader_id": row[3] or "unknown",
                "description": row[4] or "",
                "create_time": row[5],
                "create_date": format_timestamp(row[5]),
                "duration": row[6] or 0,
                "duration_formatted": format_duration(row[6]),
                "tiktok_url": row[7],
                "content_type": row[8] or "video",
                "has_transcription": bool(row[9]),
                "has_ocr": bool(row[10]),
                "date_favorited": row[11],
                "favorited_date": format_timestamp(row[11]),
                "is_deleted": bool(row[12]),
                "is_private": bool(row[13]),
                "download_status": bool(row[14]),
            }

            # For image posts, use duration field as image count
            if row[8] == "images" and row[6]:
                video["image_count"] = row[6]

            videos.append(video)

        return {
            "videos": videos,
            "pagination": {
                "page": page,
                "limit": limit,
                "total": total,
                "total_pages": (total + limit - 1) // limit,
                "has_next": offset + len(videos) < total,
                "has_prev": page > 1,
            },
        }

    finally:
        conn.close()


@app.get("/api/videos/{video_id}")
async def get_video(video_id: str):
    """
    Get detailed metadata for a specific video.

    Includes title, description, creator info, transcription, and OCR text.
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT 
                v.id,
                v.title,
                v.uploader,
                v.uploader_id,
                v.desc,
                v.create_time,
                v.duration,
                v.tiktok_url,
                v.content_type,
                v.transcription_status,
                v.transcription,
                v.ocr_status,
                v.ocr,
                v.date_favorited,
                v.video_is_deleted,
                v.video_is_private
            FROM video_data v
            WHERE v.id = ? AND v.download_status = 1
        """,
            (video_id,),
        )

        row = cursor.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Video not found")

        # Build base response
        response = {
            "id": row[0],
            "title": row[1] or "Untitled",
            "uploader": row[2] or "Unknown",
            "uploader_id": row[3] or "unknown",
            "description": row[4] or "",
            "create_time": row[5],
            "create_date": format_timestamp(row[5]),
            "duration": row[6] or 0,
            "duration_formatted": format_duration(row[6]),
            "tiktok_url": row[7],
            "content_type": row[8] or "video",
            "has_transcription": bool(row[9]),
            "transcription": row[10] or "",
            "has_ocr": bool(row[11]),
            "ocr": row[12] or "",
            "date_favorited": row[13],
            "favorited_date": format_timestamp(row[13]),
            "is_deleted": bool(row[14]),
            "is_private": bool(row[15]),
        }

        # For image posts, get the image count
        if row[8] == "images":
            try:
                cursor.execute(
                    "SELECT video_blob FROM videos WHERE id = ?", (video_id,)
                )
                blob_row = cursor.fetchone()
                if blob_row:
                    import zipfile
                    from io import BytesIO

                    with zipfile.ZipFile(BytesIO(blob_row[0]), "r") as zf:
                        image_count = len(
                            [
                                name
                                for name in zf.namelist()
                                if name.lower().endswith((".jpg", ".jpeg", ".png"))
                            ]
                        )
                    response["image_count"] = image_count
            except:
                response["image_count"] = row[6] or 0  # Fallback to duration field

        return response

    finally:
        conn.close()


@app.get("/api/videos/{video_id}/thumbnail")
async def get_thumbnail(video_id: str):
    """
    Get the thumbnail for a video.

    Returns JPEG thumbnail image. For image posts, returns the first image.
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Get content type first
        cursor.execute(
            """
            SELECT content_type FROM video_data
            WHERE id = ? AND download_status = 1
        """,
            (video_id,),
        )

        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Video not found")

        content_type = row[0] or "video"

        # Get the thumbnail blob
        cursor.execute("SELECT thumbnail_blob FROM videos WHERE id = ?", (video_id,))
        blob_row = cursor.fetchone()

        if not blob_row or not blob_row[0]:
            # No thumbnail available - for image posts, serve first image instead
            if content_type == "images":
                # Redirect to first image endpoint
                from fastapi.responses import RedirectResponse

                return RedirectResponse(url=f"/api/videos/{video_id}/images/0")
            else:
                raise HTTPException(status_code=404, detail="Thumbnail not found")

        thumbnail_blob = blob_row[0]

        # Return thumbnail directly from database - no caching
        return StreamingResponse(iter([thumbnail_blob]), media_type="image/jpeg")

    finally:
        conn.close()


@app.get("/api/videos/{video_id}/stream")
async def stream_video(video_id: str):
    """
    Stream the video file for playback.

    For videos: Returns MP4 content with video/mp4 MIME type.
    For image posts: Returns ZIP content with application/zip MIME type.
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Get video info first
        cursor.execute(
            """
            SELECT content_type FROM video_data 
            WHERE id = ? AND download_status = 1
        """,
            (video_id,),
        )

        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Video not found")

        content_type = row[0] or "video"

        # Get the blob
        cursor.execute("SELECT video_blob FROM videos WHERE id = ?", (video_id,))
        blob_row = cursor.fetchone()

        if not blob_row:
            raise HTTPException(status_code=404, detail="Video file not found")

        video_blob = blob_row[0]

        # Determine MIME type
        if content_type == "images":
            media_type = "application/zip"
            headers = {"Content-Disposition": f'attachment; filename="{video_id}.zip"'}
        else:
            media_type = "video/mp4"
            headers = {"Content-Type": "video/mp4", "Accept-Ranges": "bytes"}

        def iterfile():
            yield video_blob

        return StreamingResponse(iterfile(), media_type=media_type, headers=headers)

    finally:
        conn.close()


@app.get("/api/videos/{video_id}/images")
async def get_image_list(video_id: str):
    """
    Get list of images in an image post.

    Returns count and list of available image indices.
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Verify it's an image post
        cursor.execute(
            "SELECT content_type FROM video_data WHERE id = ? AND download_status = 1",
            (video_id,),
        )
        row = cursor.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Video not found")

        if row[0] != "images":
            raise HTTPException(status_code=400, detail="Not an image post")

        # Get the ZIP blob
        cursor.execute("SELECT video_blob FROM videos WHERE id = ?", (video_id,))
        blob_row = cursor.fetchone()

        if not blob_row:
            raise HTTPException(status_code=404, detail="Image file not found")

        zip_blob = blob_row[0]

        # Extract image list from ZIP
        import zipfile
        from io import BytesIO

        with zipfile.ZipFile(BytesIO(zip_blob), "r") as zf:
            image_files = sorted(
                [
                    name
                    for name in zf.namelist()
                    if name.lower().endswith((".jpg", ".jpeg", ".png"))
                ]
            )

        return {
            "video_id": video_id,
            "image_count": len(image_files),
            "images": [
                {"index": i, "filename": name} for i, name in enumerate(image_files)
            ],
        }

    finally:
        conn.close()


@app.get("/api/videos/{video_id}/images/{index}")
async def get_image(video_id: str, index: int):
    """
    Serve a specific image from an image post ZIP.

    Args:
        video_id: The video ID
        index: Zero-based index of the image in the ZIP
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Verify it's an image post
        cursor.execute(
            "SELECT content_type FROM video_data WHERE id = ? AND download_status = 1",
            (video_id,),
        )
        row = cursor.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Video not found")

        if row[0] != "images":
            raise HTTPException(status_code=400, detail="Not an image post")

        # Get the ZIP blob
        cursor.execute("SELECT video_blob FROM videos WHERE id = ?", (video_id,))
        blob_row = cursor.fetchone()

        if not blob_row:
            raise HTTPException(status_code=404, detail="Image file not found")

        zip_blob = blob_row[0]

        # Extract specific image from ZIP
        import zipfile
        from io import BytesIO

        with zipfile.ZipFile(BytesIO(zip_blob), "r") as zf:
            image_files = sorted(
                [
                    name
                    for name in zf.namelist()
                    if name.lower().endswith((".jpg", ".jpeg", ".png"))
                ]
            )

            if index < 0 or index >= len(image_files):
                raise HTTPException(status_code=404, detail="Image index out of range")

            image_data = zf.read(image_files[index])

        # Determine MIME type from filename
        filename = image_files[index].lower()
        if filename.endswith(".png"):
            media_type = "image/png"
        else:
            media_type = "image/jpeg"

        return StreamingResponse(
            iter([image_data]),
            media_type=media_type,
            headers={
                "Content-Type": media_type,
                "Cache-Control": "public, max-age=3600",
            },
        )

    except zipfile.BadZipFile:
        raise HTTPException(status_code=500, detail="Invalid ZIP file")
    finally:
        conn.close()


@app.post("/api/videos/{video_id}/tags")
async def add_video_tag(video_id: str, tag: str = Query(..., description="Tag to add")):
    """
    Add a manual tag to a video.

    Creates a new tag entry in the tags table for the specified video.
    """
    import sys

    sys.path.insert(0, str(Path(__file__).parent.parent))
    from tagging import add_tags_to_post

    result = add_tags_to_post(video_id, tag)

    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result["message"])

    return result


@app.get("/api/videos/{video_id}/tags")
async def get_video_tags(video_id: str):
    """
    Get all tags (manual and automatic) for a specific video.

    Returns both manual tags and automatic tags with confidence scores.
    """
    import sys

    sys.path.insert(0, str(Path(__file__).parent.parent))
    from tagging import get_post_tags

    result = get_post_tags(video_id)

    if result["status"] == "error":
        raise HTTPException(
            status_code=500, detail=result.get("message", "Failed to get tags")
        )

    return result


@app.get("/api/tags")
async def get_all_tags_endpoint():
    """
        Get all unique manual and automatic tags used across all videos.

        Returns a list of all tags with their usage counts for the frontend
    to display as filter options.
    """
    import sys

    sys.path.insert(0, str(Path(__file__).parent.parent))
    from tagging import get_all_tags

    result = get_all_tags()

    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result["message"])

    return result


@app.get("/api/search")
async def search_videos(
    q: str = Query(..., min_length=1, description="Search query"),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    limit: int = Query(250, ge=1, le=500, description="Number of results per page"),
    content_type: Optional[str] = Query(
        None, description="Filter by content type: 'video' or 'images'"
    ),
    download_status: Optional[str] = Query(
        None, description="Filter by download status: 'downloaded' or 'not_downloaded'"
    ),
    transcription_status: Optional[str] = Query(
        None,
        description="Filter by transcription status: 'transcribed' or 'not_transcribed'",
    ),
    ocr_status: Optional[str] = Query(
        None, description="Filter by OCR status: 'ocr' or 'not_ocr'"
    ),
    tags: Optional[List[str]] = Query(
        None,
        description="Filter by tags (AND logic - video must have ALL selected tags)",
    ),
):
    """
    Search videos by title, uploader, description, OCR text, and transcription.

    Searches across multiple fields in the database with case-insensitive matching.
    Returns videos that match the query in any field.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Calculate offset
    offset = (page - 1) * limit

    # Add wildcards for LIKE query
    search_term = f"%{q}%"

    # Build query based on filters - search across all text fields
    where_clause = """WHERE v.download_status = 1
            AND (
                (v.title IS NOT NULL AND v.title LIKE ? COLLATE NOCASE)
                OR (v.uploader IS NOT NULL AND v.uploader LIKE ? COLLATE NOCASE)
                OR (v.desc IS NOT NULL AND v.desc LIKE ? COLLATE NOCASE)
                OR (v.transcription IS NOT NULL AND v.transcription LIKE ? COLLATE NOCASE)
                OR (v.ocr IS NOT NULL AND v.ocr LIKE ? COLLATE NOCASE)
            )"""
    params = [search_term, search_term, search_term, search_term, search_term]

    if content_type:
        where_clause += " AND v.content_type = ?"
        params.append(content_type)

    if download_status:
        if download_status == "downloaded":
            where_clause += " AND v.download_status = 1"
        elif download_status == "not_downloaded":
            where_clause += " AND v.download_status = 0"

    if transcription_status:
        if transcription_status == "transcribed":
            where_clause += " AND v.transcription_status = 1"
        elif transcription_status == "not_transcribed":
            where_clause += " AND v.transcription_status = 0"

    if ocr_status:
        if ocr_status == "ocr":
            where_clause += " AND v.ocr_status = 1"
        elif ocr_status == "not_ocr":
            where_clause += " AND v.ocr_status = 0"

    # Tags filter (AND logic - video must have ALL specified tags)
    if tags:
        # Create placeholders for the IN clause
        placeholders = ", ".join(["?"] * len(tags))
        where_clause += f""" AND v.id IN (
            SELECT video_id 
            FROM tags 
            WHERE manual_tag IN ({placeholders})
            GROUP BY video_id 
            HAVING COUNT(DISTINCT manual_tag) = ?
        )"""
        params.extend(tags)
        params.append(len(tags))

    try:
        # Get total count of matching videos
        count_query = f"""SELECT COUNT(*) FROM video_data v
            {where_clause}"""
        cursor.execute(count_query, params)

        total = cursor.fetchone()[0]

        # Get matching videos with pagination
        query_params = params + [limit, offset]
        cursor.execute(
            f"""
            SELECT 
                v.id,
                v.title,
                v.uploader,
                v.uploader_id,
                v.desc,
                v.create_time,
                v.duration,
                v.tiktok_url,
                v.content_type,
                v.transcription_status,
                v.ocr_status,
                v.date_favorited,
                v.video_is_deleted,
                v.video_is_private,
                v.download_status,
                v.transcription,
                v.ocr
            FROM video_data v
            {where_clause}
            ORDER BY v.date_favorited DESC NULLS LAST, v.create_time DESC
            LIMIT ? OFFSET ?
        """,
            query_params,
        )

        rows = cursor.fetchall()

        videos = []
        for row in rows:
            # Determine which field matched
            title = row[1] or ""
            uploader = row[2] or ""
            description = row[4] or ""
            transcription = row[15] or ""
            ocr = row[16] or ""

            match_type = []
            match_text = ""

            if q.lower() in title.lower():
                match_type.append("title")
                match_text = title
            if q.lower() in uploader.lower():
                match_type.append("uploader")
                if not match_text:
                    match_text = uploader
            if q.lower() in description.lower():
                match_type.append("description")
                if not match_text:
                    match_text = description
            if q.lower() in transcription.lower():
                match_type.append("transcription")
                if not match_text:
                    match_text = transcription
            if q.lower() in ocr.lower():
                match_type.append("ocr")
                if not match_text:
                    match_text = ocr

            videos.append(
                {
                    "id": row[0],
                    "title": row[1] or "Untitled",
                    "uploader": row[2] or "Unknown",
                    "uploader_id": row[3] or "unknown",
                    "description": row[4] or "",
                    "create_time": row[5],
                    "create_date": format_timestamp(row[5]),
                    "duration": row[6] or 0,
                    "duration_formatted": format_duration(row[6]),
                    "tiktok_url": row[7],
                    "content_type": row[8] or "video",
                    "has_transcription": bool(row[9]),
                    "has_ocr": bool(row[10]),
                    "date_favorited": row[11],
                    "favorited_date": format_timestamp(row[11]),
                    "is_deleted": bool(row[12]),
                    "is_private": bool(row[13]),
                    "download_status": bool(row[14]),
                    "match_type": match_type,
                    "match_snippet": _get_snippet(match_text, q),
                }
            )

        return {
            "query": q,
            "videos": videos,
            "pagination": {
                "page": page,
                "limit": limit,
                "total": total,
                "total_pages": (total + limit - 1) // limit,
                "has_next": offset + len(videos) < total,
                "has_prev": page > 1,
            },
        }

    finally:
        conn.close()


def _get_snippet(text: str, query: str, context_chars: int = 50) -> str:
    """
    Extract a snippet of text around the search query with context.

    Args:
        text: Full text to search in
        query: Search term
        context_chars: Characters to include before and after match

    Returns:
        Snippet with match highlighted by ellipsis
    """
    if not text or not query:
        return ""

    # Find position (case-insensitive)
    text_lower = text.lower()
    query_lower = query.lower()
    pos = text_lower.find(query_lower)

    if pos == -1:
        # Return first part if no match found
        return text[:100] + "..." if len(text) > 100 else text

    # Calculate start and end positions
    start = max(0, pos - context_chars)
    end = min(len(text), pos + len(query) + context_chars)

    snippet = text[start:end]

    # Add ellipsis if truncated
    if start > 0:
        snippet = "..." + snippet
    if end < len(text):
        snippet = snippet + "..."

    return snippet


# Admin API Endpoints


@app.post("/api/admin/init-database")
async def admin_init_database():
    """
    Initialize the database if it doesn't exist.

    Returns:
        Status message indicating success or if database already exists
    """
    try:
        import os

        if os.path.exists(DB_PATH):
            return {
                "status": "exists",
                "message": "Database already exists",
                "path": str(DB_PATH),
            }

        init_database()
        return {
            "status": "success",
            "message": "Database initialized successfully",
            "path": str(DB_PATH),
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to initialize database: {str(e)}"
        )


@app.post("/api/admin/ingest-json")
async def admin_ingest_json(json_file: UploadFile = File(...)):
    """
    Ingest a TikTok JSON file into the database.

    This endpoint will:
    1. Check if database exists, create it if not
    2. Save the uploaded JSON file temporarily
    3. Ingest the JSON data into the database
    4. Clean up the temporary file

    Args:
        json_file: The TikTok JSON file to ingest

    Returns:
        Status and count of imported records
    """
    try:
        import os
        import tempfile

        # Ensure database exists
        if not os.path.exists(DB_PATH):
            init_database()

        # Save uploaded file temporarily
        with tempfile.NamedTemporaryFile(
            mode="wb", suffix=".json", delete=False
        ) as temp_file:
            content = await json_file.read()
            temp_file.write(content)
            temp_path = temp_file.name

        try:
            # Ingest the JSON file
            # Import ingest_json from db module directly
            from db import ingest_json

            result = ingest_json(temp_path)

            return {
                "status": "success",
                "message": "JSON file ingested successfully",
                "result": result,
            }
        finally:
            # Clean up temp file
            if os.path.exists(temp_path):
                os.remove(temp_path)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to ingest JSON: {str(e)}")


@app.post("/api/admin/queue-transcriptions")
async def admin_queue_transcriptions():
    """
    Queue transcription tasks for all downloaded videos that haven't been transcribed yet.

    Returns:
        Number of videos queued for transcription
    """
    try:
        count = tasks_module.queue_transcriptions()
        return {
            "status": "success",
            "message": f"Queued {count} videos for transcription",
            "count": count,
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to queue transcriptions: {str(e)}"
        )


@app.post("/api/admin/queue-ocr")
async def admin_queue_ocr():
    """
    Queue OCR tasks for all downloaded image posts that haven't been OCR'd yet.

    Returns:
        Number of image posts queued for OCR
    """
    try:
        count = tasks_module.queue_ocr()
        return {
            "status": "success",
            "message": f"Queued {count} image posts for OCR",
            "count": count,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to queue OCR: {str(e)}")


@app.post("/api/admin/queue-downloads")
async def admin_queue_downloads():
    """
    Queue download tasks for all non-downloaded videos in the database.

    Returns:
        Number of videos queued for download
    """
    try:
        count = tasks_module.queue_downloads()
        return {
            "status": "success",
            "message": f"Queued {count} videos for download",
            "count": count,
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to queue downloads: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
