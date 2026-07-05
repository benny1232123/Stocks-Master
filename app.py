"""Stocks-Master application entrypoint.

This file now launches the FastAPI backend instead of Streamlit.
"""
from __future__ import annotations

from backend.main import app


if __name__ == "__main__":
    import os

    import uvicorn

    uvicorn.run(
        "backend.main:app",
        host=os.environ.get("HOST", "0.0.0.0"),
        port=int(os.environ.get("PORT", "8000")),
        reload=os.environ.get("RELOAD", "0") == "1",
    )
