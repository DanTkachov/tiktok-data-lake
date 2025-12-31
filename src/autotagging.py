from transformers import pipeline
from db import get_connection


# ============================================================
# USER-DEFINED TAGS
# ============================================================
# Edit this list to change which tags are used for classification.
# Tags should be simple, descriptive categories.
# Examples: "recipes", "anime", "tutorial", "comedy", "music"
#
# After changing tags:
# 1. Clear existing tags: DELETE FROM tags;
# 2. Re-run this script: python src/autotagging.py
#
# Confidence threshold is set at line 71 (currently 0.8)
# ============================================================
TAGS = ["recipes", "anime"]


def auto_tag_videos():
    """
    Automatically tags videos using zero-shot classification.
    Uses video title, description, and transcription to classify into user-defined tags.

    Args:
        None

    Returns:
        Dictionary with statistics about tagging process
    """

    # Load zero-shot classifier
    print("Loading zero-shot classification model...")
    classifier = pipeline("zero-shot-classification",
                         model="facebook/bart-large-mnli")
    print("Model loaded!\n")

    # Get all videos that have been transcribed
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, title, desc, transcription
        FROM video_data
        WHERE transcription_status = 1 AND transcription IS NOT NULL
    """)
    videos = cursor.fetchall()

    print(f"Found {len(videos)} videos to tag\n")

    stats = {
        "total": len(videos),
        "tagged": 0,
        "skipped": 0
    }

    for video_id, title, desc, transcription in videos:
        # Combine all text for classification
        text_parts = []
        if title:
            text_parts.append(title)
        if desc:
            text_parts.append(desc)
        if transcription:
            text_parts.append(transcription)

        text = " ".join(text_parts)

        # Skip if no text available
        if not text.strip():
            stats["skipped"] += 1
            continue

        # Classify the text
        result = classifier(text, candidate_labels=TAGS)

        # Insert tags into database
        # result['labels'] contains tags sorted by score
        # result['scores'] contains corresponding confidence scores
        for tag, score in zip(result['labels'], result['scores']):
            # Only add tag if score is above threshold (e.g., 0.5)
            if score > 0.8:
                try:
                    cursor.execute("""
                        INSERT OR IGNORE INTO tags (video_id, tag, confidence)
                        VALUES (?, ?, ?)
                    """, (video_id, tag, score))
                except Exception as e:
                    print(f"Error inserting tag for video {video_id}: {e}")

        conn.commit()
        stats["tagged"] += 1
        print(f"Tagged video {video_id}: {result['labels'][0]} ({result['scores'][0]:.2f})")

    cursor.close()
    conn.close()

    print("\n" + "="*50)
    print("TAGGING COMPLETE")
    print("="*50)
    print(f"Total videos: {stats['total']}")
    print(f"Tagged: {stats['tagged']}")
    print(f"Skipped: {stats['skipped']}")
    print("="*50)

    return stats


if __name__ == "__main__":
    auto_tag_videos()
