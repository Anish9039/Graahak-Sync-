import io
import re
import logging
from typing import List, Dict, Any
from datetime import datetime

import polars as pl
from fastapi import FastAPI, File, UploadFile, HTTPException, Depends, status
from pydantic import BaseModel

# --- 1. CONFIGURATION & LOGGING ---
class Settings:
    APP_NAME: str = "Graahak-Sync Ingestion Engine"
    VERSION: str = "2.0.0"
    
    # 1. CORE IDENTITY FIELDS (We MUST find these or alias them)
    # These are the columns we standardize strictly for the "Golden Record"
    CORE_IDENTITY_MAP: Dict[str, str] = {
        "mobile": "phone", "cell": "phone", "contact": "phone", "phone_no": "phone",
        "mail": "email", "e-mail": "email",
        "name": "full_name", "client": "full_name", "customer": "full_name"
    }

    # 2. DYNAMIC PATTERNS (For handling hundreds of unknown columns)
    # If a column name matches regex, we attempt a specific cast
    PATTERN_RULES: Dict[str, pl.DataType] = {
        r".*(_date|_at|dob|joined)$": pl.Date,       # Ends in _date -> Date
        r".*(price|amount|total|revenue|cost)$": pl.Float64, # Money -> Float
        r"^(is_|has_|active)$": pl.Boolean            # Boolean flags
    }

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("etl_engine")
settings = Settings()

app = FastAPI(title=settings.APP_NAME, version=settings.VERSION)

# --- 2. DATA MODELS ---
# class IngestionStats(BaseModel):
#     company_id: str
#     rows_total: int
#     core_fields_found: List[str]
#     dynamic_fields_mapped: int
#     metadata_fields_packed: int
#     status: str

# --- 2. DATA MODELS ---
class IngestionStats(BaseModel):
    company_id: str
    rows_total: int
    core_fields_found: List[str]
    dynamic_fields_mapped: int
    metadata_fields_packed: int
    status: str
    # ADD THIS LINE:
    data_preview: List[Dict[str, Any]]

# --- 3. THE "GARBAGE EATER" LOGIC ---
class SmartIngestionService:
    """
    Production-grade service that handles:
    1. File Format Abstraction (CSV/Excel)
    2. Header Normalization
    3. Dynamic Typing (Pattern Matching)
    4. Metadata Packing (Preserving unexpected data)
    """

    def process_upload(self, file_content: bytes, filename: str) -> pl.DataFrame:
        """
        Main entry point. Returns a clean, standardized Polars DataFrame.
        """
        logger.info(f"Starting processing for file: {filename}")
        
        # A. Ingest as String (The Safety Net)
        # We read EVERYTHING as Utf8 first to prevent initial parsing crashes.
        try:
            if filename.endswith(".csv"):
                df = pl.read_csv(io.BytesIO(file_content), infer_schema_length=0)
            elif filename.endswith(('.xlsx', '.xls')):
                df = pl.read_excel(io.BytesIO(file_content), infer_schema_length=0)
            else:
                raise ValueError("Unsupported file format. Use CSV or Excel.")
        except Exception as e:
            logger.error(f"File read failed: {e}")
            raise ValueError("Corrupt file or invalid format.")

        if df.height == 0:
            raise ValueError("File is empty.")

        # B. Header Normalization (Standardize inputs)
        # "Mobile No." -> "mobile_no" -> mapped later
        clean_headers = {
            col: re.sub(r'[^a-z0-9_]', '', col.lower().strip().replace(' ', '_')) 
            for col in df.columns
        }
        df = df.rename(clean_headers)

        # C. Identify & Standardize Core Fields
        # We try to find 'phone', 'email', 'name' regardless of what user uploaded
        df = self._map_core_identities(df)

        # D. Dynamic Type Casting & Metadata Packing
        # This handles the "hundreds of columns" requirement
        df = self._apply_dynamic_schema(df)

        return df

    def _map_core_identities(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Scans columns to find Core Identity fields based on synonyms.
        """
        rename_map = {}
        # We use a set to track which target fields (like 'phone') are already present/mapped.
        # This prevents mapping two different columns to 'phone'.
        claimed_targets = set(df.columns)

        # FIX: Iterate over df.columns (a List), NOT the set we are modifying
        for col in df.columns:
            # Check if this column matches a known alias (e.g. 'mobile' -> 'phone')
            for alias, target in settings.CORE_IDENTITY_MAP.items():
                # If we find a keyword match AND the target isn't already claimed
                if alias in col and target not in claimed_targets:
                    rename_map[col] = target
                    claimed_targets.add(target) # Mark this target as "taken"
                    break
        
        if rename_map:
            logger.info(f"Mapped core columns: {rename_map}")
            df = df.rename(rename_map)
        
        return df

    def _apply_dynamic_schema(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        The Logic for "Unforeseen Elements":
        1. Clean Core Fields specifically (Phone Regex).
        2. Auto-Cast other fields based on Name Patterns (Date/Float).
        3. Pack EVERYTHING else into a 'metadata' JSON column.
        """
        expressions = []
        core_cols = ["phone", "email", "full_name"]
        processed_cols = set()

        # 1. Handle Core Fields (Strict Cleaning)
        if "phone" in df.columns:
            expressions.append(
                pl.col("phone")
                .str.replace_all(r"[^0-9]", "")  # Remove non-digits
                .alias("phone")
            )
            processed_cols.add("phone")
        
        if "email" in df.columns:
            expressions.append(pl.col("email").str.strip_chars().alias("email"))
            processed_cols.add("email")

        if "full_name" in df.columns:
            expressions.append(pl.col("full_name").str.strip_chars().alias("full_name"))
            processed_cols.add("full_name")

        # 2. Handle Dynamic Patterns (Prices, Dates)
        for col in df.columns:
            if col in processed_cols: continue

            matched_type = None
            for pattern, dtype in settings.PATTERN_RULES.items():
                if re.match(pattern, col):
                    matched_type = dtype
                    break
            
            if matched_type:
                # Found a pattern (e.g., 'created_at'). Cast it.
                if matched_type == pl.Date:
                    expressions.append(
                        pl.col(col).str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias(col)
                    )
                else:
                    expressions.append(
                        pl.col(col).cast(matched_type, strict=False).alias(col)
                    )
                processed_cols.add(col)

        # 3. The "Metadata Bucket" (Strategy for Unpredictability)
        # Any column NOT processed yet is packed into a JSON struct.
        # This creates a "Schemaless" area within our structured data.
        remaining_cols = [c for c in df.columns if c not in processed_cols]
        
        if remaining_cols:
            # We keep the remaining cols as Strings in the DF for now, 
            # OR we can structurally pack them.
            # Here we simply keep them as Utf8 (Safe Mode) to avoid data loss.
            # Ideally, in DB write, these go into a JSONB column.
            for col in remaining_cols:
                expressions.append(pl.col(col))

        return df.select(expressions)

# --- 4. DATABASE ABSTRACTION ---
class DatabaseRepository:
    """
    Simulates Atomic Replacement Strategy.
    """
    def replace_data(self, company_id: str, df: pl.DataFrame):
        try:
            if df.height == 0:
                raise ValueError("Dataset is empty after cleaning.")

            # SIMULATION OF SQL TRANSACTION
            # 1. BEGIN TRANSACTION
            # 2. DELETE FROM customers WHERE company_id = {company_id}
            # 3. df.write_database(..., if_exists='append')
            # 4. COMMIT
            
            logger.info(f"DB: Dropped old data for {company_id}")
            logger.info(f"DB: Inserted {df.height} rows. Schema: {df.schema}")
            return True
        except Exception as e:
            logger.error(f"DB Transaction failed: {e}")
            raise HTTPException(status_code=500, detail="Database write failed.")

# --- 5. DEPENDENCIES ---
def get_ingestion_service():
    return SmartIngestionService()

def get_db():
    return DatabaseRepository()

# --- 6. API ENDPOINT ---
@app.post("/api/v1/data/replace/{company_id}", response_model=IngestionStats)
async def ingest_dataset(
    company_id: str,
    file: UploadFile = File(...),
    service: SmartIngestionService = Depends(get_ingestion_service),
    db: DatabaseRepository = Depends(get_db)
):
    """
    Production Endpoint for Graahak-Sync.
    Accepts ANY CSV/Excel.
    Standardizes Core Fields.
    Adapts to Unknown Fields.
    Replaces Company Data.
    """
    
    # 1. Validate File Type
    ext = file.filename.split('.')[-1].lower()
    if ext not in ["csv", "xlsx", "xls"]:
        raise HTTPException(status_code=400, detail="Only CSV/Excel allowed.")

    # 2. Read Stream
    try:
        content = await file.read()
    except Exception:
        raise HTTPException(status_code=500, detail="Upload failed.")

    # 3. ETL Processing
    try:
        df_clean = service.process_upload(content, file.filename)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("Critical ETL Error")
        raise HTTPException(status_code=500, detail="Internal Processing Error")

    # 4. Atomic Swap
    db.replace_data(company_id, df_clean)

    # 5. Metrics for Response
    core_found = [c for c in ["phone", "email", "full_name"] if c in df_clean.columns]
    
# ... inside ingest_dataset function ...

    return IngestionStats(
        company_id=company_id,
        status="success",
        rows_total=df_clean.height,
        core_fields_found=core_found,
        dynamic_fields_mapped=len(df_clean.columns) - len(core_found),
        metadata_fields_packed=0,
        message="Full replacement complete.",
        # ADD THIS LINE:
        data_preview=df_clean.head(5).to_dicts() 
    )

# --- 7. RUNNER ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)