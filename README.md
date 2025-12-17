# Omni-Link Ingestion Engine (Graahak-Sync Core)

## 🚀 Vision
A high-performance Data Ingestion and Canonicalization Service that processes messy SME data (Excel/CSV), enforces a strict schema, sanitizes invalid values, and produces a unified Golden Customer Record for identity resolution.
## ⚡ Tech Stack
- **Engine:** Python Polars (Lazy Evaluation for speed)
- **API:** FastAPI (Async I/O)
- **Architecture:** Dynamic Schema Funnel (Pattern Recognition -> Strict Casting -> Metadata Isolation)

## 🛠 Features
- **The Garbage Eater:** Accepts any file format (CSV/XLSX) without crashing.
- **Auto-Mapping:** Fuzzy matches columns (e.g., "Mob No." -> "phone").
- **Atomic Swap:** Simulates database transactions to ensure zero data loss during updates.

## 🏃‍♂️ Quick Start
1. `pip install -r requirements.txt`
2. `python main.py`
3. Swagger Docs: `http://localhost:8000/docs`
