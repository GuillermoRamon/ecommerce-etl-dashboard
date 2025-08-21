from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_ES = PROJECT_ROOT / "data" / "raw" / "spain"
RAW_UK = PROJECT_ROOT / "data" / "raw" / "uk"
INTERMEDIATE_UNIFIED = PROJECT_ROOT / "data" / "intermediate" / "unified"
PROCESSED = PROJECT_ROOT / "data" / "processed"