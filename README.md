# tiktok-save

A Python utility for backing up your liked and bookmarked videos on TikTok. It will download the videos themselves as mp4 files, and associated metadata for each video as JSON.

# Running the Project

This project uses Redis (via Docker) and Celery for distributed task processing.

## Starting Services

### 1. Start Redis
```bash
docker compose up -d
```

### 2. Start Celery Workers

#### Downloads Queue
```bash
celery -A src.tasks worker --queues=downloads --concurrency=1 -n tiktok_download_worker --loglevel=info
```

#### Transcription Queue
```bash
celery -A src.tasks worker --queues=transcription --concurrency=4 -n tiktok_transcription_worker --loglevel=info
```

#### OCR Queue
```bash
celery -A src.tasks worker --queues=ocr --concurrency=4 -n tiktok_ocr_worker --loglevel=info
```

### 3. Queue Transcriptions
After videos are downloaded, queue them for transcription:
```bash
python queue_transcriptions.py
```

### 4. Run Test Script
```bash
uv run src/test_database_and_download.py
```

## Transcription System

This project automatically transcribes downloaded videos using Whisper (faster-whisper).

### How It Works

1. **Downloads complete** → Videos stored in database with `download_status = 1`
2. **Queue transcriptions** → Run `python queue_transcriptions.py` to find all untranscribed videos
3. **Workers process** → Celery workers pull tasks from Redis and transcribe videos in parallel
4. **Crash recovery** → If workers crash, just re-run `queue_transcriptions.py` - it skips already-transcribed videos

### Key Features

- **Idempotent**: Safe to re-queue videos multiple times
- **Parallel processing**: Run multiple transcription workers concurrently (default: 4)
- **Crash-resistant**: Database is source of truth - Redis queue is ephemeral
- **Auto-skip**: Skips image posts and already-transcribed videos

### Manual Transcription Queue

```python
# From Python REPL or script
from src.tasks import queue_transcriptions
queue_transcriptions()  # Queues all untranscribed videos
```

## Before Starting

You will need a JSON export of your TikTok data. TikTok lets you request this from the app, and it can take a few days for them to prepare this, so if you're planning on using this tool soon, consider requesting it now. You need the **JSON** version.

## Installation

> [!IMPORTANT]
> This has currently only been tested on Python 3.12.8 and playwright 1.52.0. Either install and set up a virtual environment for those two specific versions, or uninstall and reinstall these exact versions.

```bash
$ git clone https://github.com/samirelanduk/tiktok-save .
$ cd tiktok-save
$ pip install playwright==1.52.0
$ python -m playwright install
$ pip install -r requirements.txt
```

If you get permission errors, try using `sudo` or using a virtual environment.

The main dependency here is [TikTok-Api](https://github.com/davidteather/TikTok-Api) - a great unofficial wrapper around the TikTok API. If you have any problems installing things, check the issues/docs there too.

playwright is a headless browser that TikTok-Api uses to access TikTok - you might need `sudo` privileges to install it, even in a virtual environment. If you still encounter issues, try `playwright install-deps`.

## Use

Create a folder for your liked videos and a folder for your bookmarked videos. Then, from the `tiktok-save` directory, run:

```bash
$ ./save.py liked user_data.json liked_videos_path
$ ./save.py bookmarked user_data.json bookmarked_videos_path
```

Here `user_data.json` is the TikTok JSON export, assuming it's in the current directory - provide the path to it if not.

Any failures (where a video no longer exists for example) are saved in a `download_failures.json` file, and won't be re-requested on later downloads. If you want to try previous failures, use the `--failures` argument.

### Parameters

- `<mode>`: Specify the type of videos to download. Options are:
  - `liked`: Download videos that you have liked.
  - `bookmarked`: Download videos that you have bookmarked.

- `<source>`: The path to the TikTok JSON file containing the video information.

- `<location>`: The directory where the downloaded videos and logs will be saved.

- `--failures`: (Optional) If specified, the tool will attempt to download only previously failed videos.

- `--keywords`: (Optional) A list of keywords separated by spaces to filter the videos. Only videos containing these keywords in their description, hashtags, or suggested words will be downloaded. This is useful if you want to download videos related to a specific topic such as recipes.
    eg. `./save.py bookmarks user_data.json liked_videos --keywords recipe cooking food`

## Output

- Downloaded videos will be saved in the specified location.
- Photo galleries will be downloaded into a subfolder with the ID
- Metadata for each video will be saved as a JSON file in a `logs` directory within the specified location.
- Failed downloads will be recorded in `download_failures.json`.
- Unique IDs of failed downloads and their counts will be saved in `uniqueIDs.txt`.