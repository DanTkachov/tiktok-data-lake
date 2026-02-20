import time
from transformers import pipeline
from db import get_connection


def add_tags_to_post(video_id, tag_text):
    """
    Add a manual tag to a video post.

    Args:
        video_id: The video ID to tag
        tag_text: The tag text to add (single tag string)

    Returns:
        dict: {"status": "success"/"error", "message": str}
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Check if video exists
        cursor.execute("SELECT id FROM video_data WHERE id = ?", (video_id,))
        if not cursor.fetchone():
            conn.close()
            return {"status": "error", "message": f"Video {video_id} not found"}

        tag_text = tag_text.strip()

        # Check if tag already exists on this video
        cursor.execute(
            """
            SELECT 1 FROM tags 
            WHERE video_id = ? AND manual_tag = ?
        """,
            (video_id, tag_text),
        )

        if cursor.fetchone():
            conn.close()
            return {
                "status": "error",
                "message": f"Tag '{tag_text}' already exists on this video",
            }

        # Add manual tag with timestamp
        timestamp = int(time.time())
        cursor.execute(
            """
            INSERT INTO tags (video_id, manual_tag, date_added)
            VALUES (?, ?, ?)
        """,
            (video_id, tag_text, timestamp),
        )

        conn.commit()
        conn.close()

        return {
            "status": "success",
            "message": f"Tag '{tag_text}' added to video {video_id}",
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


def remove_tags_from_post(video_id, tag_text):
    """
    Remove a manual tag from a video post.

    Args:
        video_id: The video ID to remove tag from
        tag_text: The tag text to remove (single tag string)

    Returns:
        dict: {"status": "success"/"error", "message": str, "deleted_count": int}
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Delete the manual tag
        cursor.execute(
            """
            DELETE FROM tags
            WHERE video_id = ? AND manual_tag = ?
        """,
            (video_id, tag_text.strip()),
        )

        deleted_count = cursor.rowcount
        conn.commit()
        conn.close()

        if deleted_count > 0:
            return {
                "status": "success",
                "message": f"Removed {deleted_count} instance(s) of tag '{tag_text}' from video {video_id}",
                "deleted_count": deleted_count,
            }
        else:
            return {
                "status": "error",
                "message": f"Tag '{tag_text}' not found on video {video_id}",
                "deleted_count": 0,
            }

    except Exception as e:
        return {"status": "error", "message": str(e), "deleted_count": 0}


def get_post_tags(video_id):
    """
    Get all tags (both manual and automatic) for a video post.

    Args:
        video_id: The video ID to get tags for

    Returns:
        dict: {
            "status": "success"/"error",
            "manual_tags": [str, ...],
            "automatic_tags": [{"tag": str, "confidence": float}, ...]
        }
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Get manual tags
        cursor.execute(
            """
            SELECT manual_tag, date_added
            FROM tags
            WHERE video_id = ? AND manual_tag IS NOT NULL
            ORDER BY date_added DESC
        """,
            (video_id,),
        )

        manual_tags = [row[0] for row in cursor.fetchall()]

        # Get automatic tags with confidence scores
        cursor.execute(
            """
            SELECT automatic_tag, confidence
            FROM tags
            WHERE video_id = ? AND automatic_tag IS NOT NULL
            ORDER BY confidence DESC
        """,
            (video_id,),
        )

        automatic_tags = [
            {"tag": row[0], "confidence": row[1]} for row in cursor.fetchall()
        ]

        conn.close()

        return {
            "status": "success",
            "manual_tags": manual_tags,
            "automatic_tags": automatic_tags,
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "manual_tags": [],
            "automatic_tags": [],
        }


def get_all_tags():
    """
    Get all unique tags (both manual and automatic) used across all videos.
    Useful for tag suggestions/autocomplete in the frontend.

    Returns:
        dict: {
            "status": "success"/"error",
            "manual_tags": [{"tag": str, "count": int}, ...],
            "automatic_tags": [{"tag": str, "count": int}, ...]
        }
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Get all unique manual tags with usage count
        cursor.execute("""
            SELECT manual_tag, COUNT(*) as count
            FROM tags
            WHERE manual_tag IS NOT NULL
            GROUP BY manual_tag
            ORDER BY count DESC, manual_tag ASC
        """)

        manual_tags = [{"tag": row[0], "count": row[1]} for row in cursor.fetchall()]

        # Get all unique automatic tags with usage count
        cursor.execute("""
            SELECT automatic_tag, COUNT(*) as count
            FROM tags
            WHERE automatic_tag IS NOT NULL
            GROUP BY automatic_tag
            ORDER BY count DESC, automatic_tag ASC
        """)

        automatic_tags = [{"tag": row[0], "count": row[1]} for row in cursor.fetchall()]

        conn.close()

        return {
            "status": "success",
            "manual_tags": manual_tags,
            "automatic_tags": automatic_tags,
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "manual_tags": [],
            "automatic_tags": [],
        }
