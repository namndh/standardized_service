# Technical Analysis (TA) Service

## Features

- **REST API** for data enrichment operations
- **Multi-source data loading** from local files or AWS S3.  
  To wrap the logic in the notebook as a service with API endpoints,  
  I make the file name as query paramerter of the API endpoint, then the service will  
  load the Parquet file from local storage or S3 based on the configuration
- **PostgreSQL integration** for output data persistence —  
  The 'mission' which makes the logic in the notebook run completely
- **Docker containerization** with docker-compose setup

## Requirements

- Python 3.12+
- PostgreSQL 15+
- Docker & Docker Compose

## Installation

### Docker Setup

1. **Start services with docker-compose:**
   ```bash
   docker-compose up -d
   ```

This will start:
- PostgreSQL database on port 5433
- TA service on port 80

## Usage

### Start the API Server

**Local:**
```bash
uvicorn ta_apis:app --reload --host 0.0.0.0 --port 8000
```

**Docker:**
```bash
docker-compose up
```

### API Documentation

Once running, access the interactive API documentation at:
- **Swagger UI:** http://localhost:8000/docs (local) or http://localhost/docs (docker)
- **ReDoc:** http://localhost:8000/redoc (local) or http://localhost/redoc (docker)

## API Endpoints

### `POST /ta/enrich`

Enriches a Parquet file with technical analysis features and saves results to the database.

**Parameters:**
- `file_name` (query parameter): Name of the Parquet file (without .parquet extension)

**Response:**
```json
{
  "status": "success",
  "message": "TA features added and saved to DB"
}
```

**Example Usage:**

```bash
# Using curl
curl -X POST "http://localhost:8000/ta/enrich?file_name=data_test"

# Using Python requests
import requests
response = requests.post(
    "http://localhost:8000/ta/enrich",
    params={"file_name": "data_test"}
)
```
