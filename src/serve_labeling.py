"""Lightweight server for the labeling tool.

Serves the HTML and provides API endpoints to list/load JSON export directories.

Usage:
    python -m src.serve_labeling [--port 8000] [--data-root ./data]
"""
import argparse
import json
import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

app = FastAPI(title="Labeling Tool Server")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_STATIC_DIR = _PROJECT_ROOT / "static"
_CHUNKS_DIR = os.environ.get("CHUNKS_DIR", "")


def _data_root():
    return Path(os.environ.get("DATA_ROOT", str(_PROJECT_ROOT / "data")))


@app.get("/api/folders")
def list_folders():
    """List all folders under DATA_ROOT that contain JSON files."""
    root = _data_root()
    folders = []
    if not root.is_dir():
        return folders
    for d in sorted(root.iterdir()):
        if d.is_dir():
            json_count = len(list(d.glob("*.json")))
            if json_count > 0:
                folders.append({"name": d.name, "doc_count": json_count})
    return folders


@app.get("/api/folders/{folder_name}")
def load_folder(folder_name: str):
    """Load all JSON files from a folder and return as a list of docs."""
    folder = _data_root() / folder_name
    if not folder.is_dir():
        return {"error": f"Folder not found: {folder_name}"}

    docs = []
    for f in sorted(folder.glob("*.json")):
        with open(f) as fh:
            obj = json.load(fh)
        meta = obj.get("document_metadata") or {}
        # Extract chunk content from export, or look up from llm_semantic
        doc_id = obj.get("document_id") or meta.get("id", "")
        raw_chunks = obj.get("chunks") or []
        chunk_texts = [c.get("content", "") if isinstance(c, dict) else str(c) for c in raw_chunks]

        # If no chunks in export, try chunks directory
        if not chunk_texts and doc_id and _CHUNKS_DIR:
            chunk_file = Path(_CHUNKS_DIR) / f"{doc_id}.json"
            if chunk_file.exists():
                with open(chunk_file) as cf:
                    chunk_data = json.load(cf)
                chunk_texts = [str(c) for c in chunk_data.get("chunks", [])]

        docs.append({
            "doc_id": doc_id,
            "original_content": obj.get("markdown_content") or "",
            "raw_content": obj.get("raw_content") or "",
            "chunks": chunk_texts,
            "url": meta.get("url", ""),
            "publisher": meta.get("source", ""),
            "title": meta.get("title", ""),
            "domain": meta.get("domain", ""),
        })
    return docs


# Serve static files
app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")


@app.get("/")
@app.get("/labeling_tool.html")
def root():
    return FileResponse(str(_STATIC_DIR / "labeling_tool.html"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--data-root", default=None, help="Root directory containing export folders")
    args = parser.parse_args()
    if args.data_root:
        os.environ["DATA_ROOT"] = args.data_root
    uvicorn.run(app, host="0.0.0.0", port=args.port)
