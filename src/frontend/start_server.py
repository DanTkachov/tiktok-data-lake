#!/usr/bin/env python3
"""
Startup script for TikTok Data Lake Frontend

Usage:
    python start_server.py

The server will start on http://localhost:8000
"""

import sys
from pathlib import Path

# Add parent directory to path to ensure imports work
sys.path.insert(0, str(Path(__file__).parent.parent))

import uvicorn

if __name__ == "__main__":
    print("=" * 60)
    print("TikTok Data Lake Frontend Server")
    print("=" * 60)
    print("Starting server on http://localhost:8000")
    print("Press Ctrl+C to stop")
    print("=" * 60)

    uvicorn.run(
        "frontend.api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_dirs=[str(Path(__file__).parent)],
    )
