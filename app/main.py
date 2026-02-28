from typing import List

from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.openapi.utils import get_openapi
import os

import pandas as pd

from app.config import DEBUG_MODE, debug_log
from app.pipeline.extractor import extract_data
from app.pipeline.aggregator import merge_data
from app.utils.csv_converter import create_csv

app = FastAPI()

# Enable CORS for all origins, methods, and headers
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_FOLDER = "app/uploads"


@app.on_event("startup")
def startup_event():
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    if DEBUG_MODE:
        print("[DEBUG] Demo mode ON - upload, extraction, aggregation, CSV logs enabled")


def _patch_file_schema(schema: dict) -> None:
    """Add format: binary so Swagger UI shows file pickers instead of text inputs."""
    # Handle properties with file fields
    for prop in schema.get("properties", {}).values():
        if isinstance(prop, dict):
            # Direct file property
            if "contentMediaType" in prop:
                prop["format"] = "binary"
            # Array of files (items field)
            if "items" in prop and isinstance(prop["items"], dict) and "contentMediaType" in prop["items"]:
                prop["items"]["format"] = "binary"
            # anyOf variants
            for sub in (prop or {}).get("anyOf", []):
                if isinstance(sub, dict) and "contentMediaType" in sub:
                    sub["format"] = "binary"
                # Handle anyOf with items
                if "items" in sub and isinstance(sub["items"], dict) and "contentMediaType" in sub["items"]:
                    sub["items"]["format"] = "binary"


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    for path, methods in openapi_schema.get("paths", {}).items():
        if "/upload" not in path or "post" not in methods:
            continue
        rb = methods["post"].get("requestBody", {})
        ref = rb.get("content", {}).get("multipart/form-data", {}).get("schema", {}).get("$ref")
        if ref:
            schema_name = ref.split("/")[-1]
            body = openapi_schema.get("components", {}).get("schemas", {}).get(schema_name, {})
            _patch_file_schema(body)
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi


@app.get("/")
def read_root():
    return {"message": "Financial Structuring API Running 🚀"}


@app.get("/health")
def health_check():
    """Verify API and pipeline modules are ready for demo."""
    status = "ok"
    pipeline = {}

    # Smoke-test pipeline modules
    try:
        _ = extract_data("test.txt", "")
        pipeline["extractor"] = "ok"
    except Exception as e:
        pipeline["extractor"] = str(e)
        status = "degraded"

    try:
        _ = merge_data([{"date": "2024-01-01", "vendor": "test", "amount": 0, "tax": 0, "source": "test"}])
        pipeline["aggregator"] = "ok"
    except Exception as e:
        pipeline["aggregator"] = str(e)
        status = "degraded"

    try:
        create_csv([], os.path.join(UPLOAD_FOLDER, ".health_check"))
        pipeline["csv_converter"] = "ok"
        os.remove(os.path.join(UPLOAD_FOLDER, ".health_check"))
    except Exception as e:
        pipeline["csv_converter"] = str(e)
        status = "degraded"

    return {"status": status, "api": "running", "pipeline": pipeline}


@app.get("/sample")
def sample_output():
    """Return sample structured output and CSV preview without upload. For demo use."""
    sample_extracted = [
        {"date": "2024-01-15", "vendor": "ABC Ltd", "amount": 5000.0, "tax": 500.0, "source": "invoice_001.txt"},
        {"date": "2024-01-20", "vendor": "XYZ Corp", "amount": 1200.0, "tax": 120.0, "source": "receipt_002.csv"},
        {"date": "2024-01-25", "vendor": "Acme Inc", "amount": 3500.0, "tax": 350.0, "source": "statement.pdf"},
    ]
    data = merge_data(sample_extracted)
    csv_preview = pd.DataFrame(data).to_csv(index=False)
    return {
        "message": "Sample output - same structure as POST /upload/",
        "structured": data,
        "csv_preview": csv_preview,
    }


@app.post("/upload/")
async def upload_files(files: List[UploadFile] = File(...)):
    processed_data = []

    for file in files:
        filename = file.filename or "unnamed"
        file_path = os.path.join(UPLOAD_FOLDER, filename)

        content = await file.read()
        with open(file_path, "wb") as f:
            f.write(content)

        debug_log("UPLOAD", f"{filename}: {len(content)} bytes, content_type={file.content_type}")
        preview = content[:100].decode("utf-8", errors="replace") if content else "(empty)"
        debug_log("UPLOAD", f"{filename}: content preview: {repr(preview[:80])}...")

        # Decode content to text using utf-8 with errors ignored
        text_content = content.decode("utf-8", errors="ignore")
        
        # Pass both filename and text to extract_data()
        extracted = extract_data(filename, text_content)
        processed_data.append(extracted)

    merged = merge_data(processed_data)

    csv_path = os.path.join(UPLOAD_FOLDER, "output.csv")
    create_csv(merged, csv_path)
    debug_log("CSV", f"Written to {csv_path} ({len(merged)} rows)")

    return {
        "message": "Files processed successfully",
        "csv_file": "output.csv",
        "data": merged
    }


from fastapi import HTTPException


@app.get("/download")
def download_csv():
    """Return the most recent output.csv file as a download.

    Raises a 404 error if the file doesn't exist so that the response isn't
    unintentionally serialized as JSON when the browser is expecting a file.
    """
    csv_path = os.path.join(UPLOAD_FOLDER, "output.csv")
    if not os.path.exists(csv_path):
        raise HTTPException(status_code=404, detail="CSV not found")
    # FileResponse ensures the browser will prompt a download of the CSV
    return FileResponse(csv_path, media_type="text/csv", filename="output.csv")