# PrtLabelData

Label data service for collecting and managing ground truth labels. Provides a FastAPI backend with MongoDB storage and a browser-based labeling UI.

## Setup

```bash
poetry install
```

## Running

### Main labeling service (MongoDB-backed)

```bash
uvicorn src.app:app --reload --port 8080
```

Requires MongoDB. Set `MONGO_URI` and `LABELING_DB` environment variables as needed.

### Lightweight viewer (file-based)

```bash
python -m src.serve_labeling --port 8000 --data-root ./data
```

### Docker

```bash
docker-compose up --build
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGO_URI` | `mongodb://localhost:27017` | MongoDB connection string |
| `LABELING_DB` | `labeling_tool` | MongoDB database name |
| `DATA_ROOT` | `./data` | Root directory for export folders (viewer mode) |
| `CHUNKS_DIR` | _(empty)_ | Directory containing chunk JSON files (viewer mode) |
