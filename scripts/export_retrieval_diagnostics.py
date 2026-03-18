#!/usr/bin/env python3
"""
Export all Retrieval Diagnostics data for a given document ID to a JSON file.

Calls the upstream services directly (same as the internal API proxy does):
  - Document Service (port 8893) — metadata, ingest metadata, raw content, markdown
  - Indexing Search API (port 8081) — chunks

The output deduplicates shared metadata: document_metadata, publisher_metadata,
content_metadata, and ingest_metadata are hoisted to the top level (taken from
the first chunk), and embeddings are stripped from chunks.

Each document is fetched, written to a single file in one shot, then released
from memory before moving to the next document.

Usage:
    # Single document:
    python scripts/export_retrieval_diagnostics.py <document_id>

    # Batch from a text file with one document ID per line:
    python scripts/export_retrieval_diagnostics.py --ids doc_ids.txt

    # Batch from JSONL file (reads "doc_id" field from each line):
    python scripts/export_retrieval_diagnostics.py --jsonl path/to/file.jsonl

    # Custom service URLs:
    python scripts/export_retrieval_diagnostics.py <document_id> \
        --doc-service http://localhost:8893 \
        --indexing-service http://localhost:8081

    # Specify output file (single) or output directory (batch):
    python scripts/export_retrieval_diagnostics.py <document_id> -o output.json
    python scripts/export_retrieval_diagnostics.py --ids doc_ids.txt -o output_dir/

Environment variables (optional, overridden by CLI flags):
    PRT_DOCUMENT_ENDPOINT   — Document Service base URL (default: http://localhost:8893)
    INDEXING_SERVICE        — Indexing Search API base URL (default: http://localhost:8081)
    INDEXING_SERVICE_VERSION — Indexing API version (default: v2)
"""

import argparse
import json
import os
import time

import requests

# Fields that are identical across all chunks of the same document.
# These get hoisted to the top level to avoid redundancy.
SHARED_CHUNK_FIELDS = {
    "document_metadata",
    "publisher_metadata",
    "content_metadata",
    "ingest_metadata",
}

# Fields to strip from each chunk (large and usually not needed for diagnostics)
STRIP_CHUNK_FIELDS = {"embedding"}


def fetch(method: str, url: str, *, json_body=None, params=None, timeout: int = 30):
    """Make an HTTP request and return (response, error_string)."""
    try:
        if method == "POST":
            r = requests.post(url, json=json_body, timeout=timeout)
        else:
            r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r, None
    except requests.RequestException as e:
        return None, str(e)


def export_document(document_id: str, doc_base: str, indexing_base: str, indexing_version: str) -> dict:
    """Fetch all retrieval diagnostics data and return a single dict."""
    result = {
        "document_id": document_id,
        "document_metadata": None,
        "publisher_metadata": None,
        "content_metadata": None,
        "ingest_metadata_from_chunks": None,
        "chunks": None,
        "ingest_metadata": None,
        "raw_content": None,
        "markdown_content": None,
        "errors": {},
    }

    # 1. Document metadata
    key = "document_metadata"
    url = f"{doc_base}/v1/content/{document_id}/metadata"
    print(f"  [{key}] GET {url} ... ", end="", flush=True)
    t0 = time.time()
    r, err = fetch("GET", url)
    elapsed = time.time() - t0
    if err:
        print(f"FAILED ({elapsed:.2f}s): {err}")
        result["errors"][key] = err
    elif r.status_code == 404:
        print(f"NOT FOUND ({elapsed:.2f}s)")
    else:
        print(f"OK ({elapsed:.2f}s)")
        result[key] = r.json()

    # 2. Chunks
    key = "chunks"
    url = f"{indexing_base}/documents/{indexing_version}/documents/{document_id}"
    print(f"  [{key}] GET {url} ... ", end="", flush=True)
    t0 = time.time()
    r, err = fetch("GET", url)
    elapsed = time.time() - t0
    if err:
        print(f"FAILED ({elapsed:.2f}s): {err}")
        result["errors"][key] = err
    else:
        data = r.json()
        raw_chunks = data.get("chunks") or data.get("data") or data
        if isinstance(raw_chunks, dict):
            raw_chunks = raw_chunks.get("chunks", [])
        if not isinstance(raw_chunks, list):
            raw_chunks = []
        print(f"OK — {len(raw_chunks)} chunks ({elapsed:.2f}s)")

        # Hoist shared metadata from first chunk
        first_chunk = raw_chunks[0] if raw_chunks else {}
        result["publisher_metadata"] = first_chunk.get("publisher_metadata")
        result["content_metadata"] = first_chunk.get("content_metadata")
        result["ingest_metadata_from_chunks"] = first_chunk.get("ingest_metadata")
        if result["document_metadata"] is None:
            result["document_metadata"] = first_chunk.get("document_metadata")

        # Slim chunks: strip shared fields and embeddings
        remove_keys = SHARED_CHUNK_FIELDS | STRIP_CHUNK_FIELDS
        result["chunks"] = [
            {k: v for k, v in ch.items() if k not in remove_keys}
            for ch in raw_chunks
        ]

    # 3. Ingest metadata
    key = "ingest_metadata"
    url = f"{doc_base}/v1/content/{document_id}/ingest_metadata"
    print(f"  [{key}] GET {url} ... ", end="", flush=True)
    t0 = time.time()
    r, err = fetch("GET", url)
    elapsed = time.time() - t0
    if err:
        print(f"FAILED ({elapsed:.2f}s): {err}")
        result["errors"][key] = err
    else:
        data = r.json()
        result[key] = data if isinstance(data, list) else ([data] if data else [])
        print(f"OK — {len(result[key])} records ({elapsed:.2f}s)")

    # 4. Raw content
    key = "raw_content"
    url = f"{doc_base}/v1/rawcontent/{document_id}/text"
    print(f"  [{key}] GET {url} ... ", end="", flush=True)
    t0 = time.time()
    r, err = fetch("GET", url)
    elapsed = time.time() - t0
    if err:
        print(f"FAILED ({elapsed:.2f}s): {err}")
        result["errors"][key] = err
    elif r.status_code == 404:
        print(f"NOT FOUND ({elapsed:.2f}s)")
    else:
        print(f"OK ({elapsed:.2f}s)")
        result[key] = r.text

    # 5. Markdown content
    key = "markdown_content"
    url = f"{doc_base}/v1/new/content/{document_id}/content"
    print(f"  [{key}] GET {url}?content_type=markdown ... ", end="", flush=True)
    t0 = time.time()
    r, err = fetch("GET", url, params={"content_type": "markdown"})
    elapsed = time.time() - t0
    if err:
        print(f"FAILED ({elapsed:.2f}s): {err}")
        result["errors"][key] = err
    elif r.status_code == 404:
        print(f"NOT FOUND ({elapsed:.2f}s)")
    else:
        data = r.json()
        result[key] = data.get("content") if isinstance(data, dict) else None
        print(f"OK ({elapsed:.2f}s)")

    # Clean up empty errors
    if not result["errors"]:
        del result["errors"]

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Export Retrieval Diagnostics data for a document ID"
    )
    parser.add_argument("document_id", nargs="?", default=None, help="The document ID to export")
    parser.add_argument(
        "--ids",
        default=None,
        help="Text file with one document ID per line",
    )
    parser.add_argument(
        "--jsonl",
        default=None,
        help="JSONL file to read document IDs from (reads 'doc_id' field per line)",
    )
    parser.add_argument(
        "--doc-service",
        default=os.getenv("PRT_DOCUMENT_ENDPOINT", "http://localhost:8893"),
        help="Document Service base URL (default: $PRT_DOCUMENT_ENDPOINT or http://localhost:8893)",
    )
    parser.add_argument(
        "--indexing-service",
        default=os.getenv("INDEXING_SERVICE", "http://localhost:8081"),
        help="Indexing Search API base URL (default: $INDEXING_SERVICE or http://localhost:8081)",
    )
    parser.add_argument(
        "--indexing-version",
        default=os.getenv("INDEXING_SERVICE_VERSION", "v2"),
        help="Indexing API version (default: $INDEXING_SERVICE_VERSION or v2)",
    )
    parser.add_argument(
        "-o", "--output",
        default=None,
        help="Output file (single mode) or directory (batch mode)",
    )
    args = parser.parse_args()

    if not args.document_id and not args.ids and not args.jsonl:
        parser.error("provide a document_id, --ids file, or --jsonl file")

    doc_base = args.doc_service.rstrip("/")
    indexing_base = args.indexing_service.rstrip("/")

    # Batch mode: --ids or --jsonl
    doc_ids = None
    source_label = None

    if args.ids:
        doc_ids = []
        with open(args.ids) as f:
            for line in f:
                line = line.strip()
                if line:
                    doc_ids.append(line)
        source_label = args.ids
    elif args.jsonl:
        doc_ids = []
        with open(args.jsonl) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                doc_ids.append(obj["doc_id"])
        source_label = args.jsonl

    if doc_ids is not None:
        output_dir = args.output or "retrieval_export"
        os.makedirs(output_dir, exist_ok=True)

        print(f"Batch export: {len(doc_ids)} documents from {source_label}")
        print(f"  Document Service:    {doc_base}")
        print(f"  Indexing Search API: {indexing_base} ({args.indexing_version})")
        print(f"  Output directory:    {output_dir}/")
        print()

        for i, doc_id in enumerate(doc_ids):
            print(f"=== [{i+1}/{len(doc_ids)}] {doc_id} ===")
            result = export_document(doc_id, doc_base, indexing_base, args.indexing_version)
            out_path = os.path.join(output_dir, f"{doc_id}.json")
            with open(out_path, "w") as f:
                json.dump(result, f, indent=2, default=str)
            del result  # release memory before next document
            print(f"  -> {out_path}")
            print()

        print(f"Done. {len(doc_ids)} files saved to {output_dir}/")
    else:
        # Single document mode
        doc_id = args.document_id
        output_path = args.output or f"retrieval_{doc_id[:16]}.json"

        print(f"Exporting retrieval diagnostics for document: {doc_id}")
        print(f"  Document Service:    {doc_base}")
        print(f"  Indexing Search API: {indexing_base} ({args.indexing_version})")
        print()

        result = export_document(doc_id, doc_base, indexing_base, args.indexing_version)
        with open(output_path, "w") as f:
            json.dump(result, f, indent=2, default=str)
        del result

        print()
        print(f"Saved to {output_path}")


if __name__ == "__main__":
    main()
