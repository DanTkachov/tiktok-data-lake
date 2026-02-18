#!/usr/bin/env python3
"""
Test script to verify watermark-free download functionality.
This script will test the new download_video_without_watermark function.
"""

import asyncio
import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from TikTokApi import TikTokApi
from db import download_video_without_watermark


async def test_watermark_free_download():
    """Test the watermark-free download function."""

    # You'll need to provide a test TikTok URL
    test_url = "https://www.tiktok.com/@tcourt11/video/7567540378543410446"

    print("🧪 Testing watermark-free download...")
    print(f"📍 Test URL: {test_url}")
    print()

    ms_token = os.environ.get("ms_token", None)

    async with TikTokApi() as api:
        await api.create_sessions(ms_tokens=[ms_token], num_sessions=1, sleep_after=3)

        video = api.video(url=test_url)
        video_info = await video.info()

        print("📊 Video Info Structure:")
        print(f"  Has 'video' key: {'video' in video_info}")

        if 'video' in video_info:
            video_data = video_info['video']
            print(f"  Has 'bitrateInfo': {'bitrateInfo' in video_data}")
            print(f"  Has 'playAddr': {'playAddr' in video_data or 'play_addr' in video_data}")
            print(f"  Has 'hdplay': {'hdplay' in video_data or 'hdPlay' in video_data}")
            print(f"  Has 'downloadAddr': {'downloadAddr' in video_data or 'download_addr' in video_data}")

            # Show bitrateInfo structure if available
            if 'bitrateInfo' in video_data:
                bitrate_info = video_data['bitrateInfo']
                print(f"\n  🎯 bitrateInfo found with {len(bitrate_info)} options:")
                for idx, option in enumerate(bitrate_info):
                    if 'PlayAddr' in option:
                        url_list = option['PlayAddr'].get('UrlList', [])
                        print(f"    Option {idx + 1}: {len(url_list)} URLs available")

        # Actually download the video without watermark
        print("\n" + "="*60)
        print("📥 Downloading video without watermark...")
        print("="*60 + "\n")

        try:
            video_bytes = await download_video_without_watermark(video_info)

            # Save to file
            output_file = "test_video_no_watermark.mp4"
            with open(output_file, "wb") as f:
                f.write(video_bytes)

            print(f"\n✅ Success! Video downloaded without watermark")
            print(f"📁 Saved to: {output_file}")
            print(f"📊 Size: {len(video_bytes):,} bytes ({len(video_bytes) / 1024 / 1024:.2f} MB)")
            print(f"\n🎬 Open '{output_file}' to verify it has no watermark!")

        except Exception as e:
            print(f"\n❌ Error downloading video: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    # Uncomment to run test:
    asyncio.run(test_watermark_free_download())
