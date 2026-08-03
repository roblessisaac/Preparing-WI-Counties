#!/usr/bin/env python3
"""Prepare Wisconsin NG911 county GeoParquet datasets for Territory Toolbox.

This pipeline reads the statewide Wisconsin NG911 Site/Structure Address Points
File Geodatabase, preserves the original source attributes, derives a rich and
auditable canonical schema, and writes county-level GeoParquet files optimized
for on-demand spatial reads by the Territory Toolbox Streamlit Analyzer.

Preservation policy
-------------------
* The original ZIP or File Geodatabase is never modified.
* Full-fidelity outputs retain every ordinary source attribute plus Source_FID,
  original geometry, raw source classifications, and derived QA fields.
* Runtime outputs keep the current Analyzer compatibility columns and richer
  fields needed for improved street, subaddress, locality, ZIP+4, record-role,
  parent/child, occupancy, and audit behavior.
* Questionable addresses remain in runtime with quality/eligibility flags.
* Only records that cannot safely participate in spatial processing are placed
  in quarantine.
* No address-, coordinate-, or geometry-based deduplication is performed.

Deliberately conservative behavior
----------------------------------
The pipeline does not assume every unit is an apartment, does not interpret one
ambiguous Place_Type/Placement/Structure value as definitive, does not remove
parent-building rows, and does not treat county presence as proof of complete
coverage. Unknown classifications remain Unknown.

Example test run
----------------
python prepare_wi_ng911_counties.py \
    --input WI_NG911_Site_Structure_Address_Points.zip \
    --output wi_ng911_pipeline \
    --counties Waukesha Adams Dane Milwaukee Crawford

Example full-state run
----------------------
python prepare_wi_ng911_counties.py \
    --input WI_NG911_Site_Structure_Address_Points.zip \
    --output wi_ng911_pipeline

Cloudflare R2
-------------
Only validated files under runtime/ are intended for eventual R2 publication.
The generated manifest is intentionally conservative: files are not marked
public merely because conversion succeeded, and this script performs no upload.

Required third-party packages
-----------------------------
geopandas, pandas, pyogrio, pyarrow, shapely

Recommended versions support GeoParquet 1.1 covering-bbox metadata and
GeoPandas read_parquet(..., bbox=...). The script records when that optimization
is unavailable rather than silently claiming support.
"""

from __future__ import annotations

import argparse
import csv
import gc
import hashlib
import json
import logging
import math
import os
import re
import shutil
import sys
import tempfile
import unicodedata
import zipfile
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

DEPENDENCY_ERRORS: list[str] = []

try:
    import numpy as np
except ImportError as exc:  # pragma: no cover - dependency guard
    np = None  # type: ignore[assignment]
    DEPENDENCY_ERRORS.append(f"numpy: {exc}")

try:
    import pandas as pd
except ImportError as exc:  # pragma: no cover - dependency guard
    pd = None  # type: ignore[assignment]
    DEPENDENCY_ERRORS.append(f"pandas: {exc}")

try:
    import geopandas as gpd
except ImportError as exc:  # pragma: no cover - dependency guard
    gpd = None  # type: ignore[assignment]
    DEPENDENCY_ERRORS.append(f"geopandas: {exc}")

try:
    import pyogrio
except ImportError as exc:  # pragma: no cover - dependency guard
    pyogrio = None  # type: ignore[assignment]
    DEPENDENCY_ERRORS.append(f"pyogrio: {exc}")

try:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
except ImportError as exc:  # pragma: no cover - dependency guard
    pa = None  # type: ignore[assignment]
    pc = None  # type: ignore[assignment]
    pq = None  # type: ignore[assignment]
    DEPENDENCY_ERRORS.append(f"pyarrow: {exc}")

try:
    from shapely.geometry import box
except ImportError as exc:  # pragma: no cover - dependency guard
    box = None  # type: ignore[assignment]
    DEPENDENCY_ERRORS.append(f"shapely: {exc}")


LOGGER = logging.getLogger("wi_ng911_pipeline")
LAYER_NAME = "SiteStructureAddressPoints"
SOURCE_SYSTEM = "Wisconsin NG911 Site/Structure Address Points"
SOURCE_STATE = "WI"
DEFAULT_ROW_GROUP_SIZE = 50_000
DEFAULT_WISCONSIN_BOUNDS = (-93.25, 42.30, -86.65, 47.25)
MAX_DETAIL_REPORT_ROWS_PER_COUNTY = 5_000
HASH_CHUNK_SIZE = 8 * 1024 * 1024

WI_COUNTIES: tuple[str, ...] = (
    "Adams", "Ashland", "Barron", "Bayfield", "Brown", "Buffalo",
    "Burnett", "Calumet", "Chippewa", "Clark", "Columbia", "Crawford",
    "Dane", "Dodge", "Door", "Douglas", "Dunn", "Eau Claire", "Florence",
    "Fond du Lac", "Forest", "Grant", "Green", "Green Lake", "Iowa",
    "Iron", "Jackson", "Jefferson", "Juneau", "Kenosha", "Kewaunee",
    "La Crosse", "Lafayette", "Langlade", "Lincoln", "Manitowoc",
    "Marathon", "Marinette", "Marquette", "Menominee", "Milwaukee",
    "Monroe", "Oconto", "Oneida", "Outagamie", "Ozaukee", "Pepin",
    "Pierce", "Polk", "Portage", "Price", "Racine", "Richland", "Rock",
    "Rusk", "St. Croix", "Sauk", "Sawyer", "Shawano", "Sheboygan",
    "Taylor", "Trempealeau", "Vernon", "Vilas", "Walworth", "Washburn",
    "Washington", "Waukesha", "Waupaca", "Waushara", "Winnebago", "Wood",
)

STATUS_OVERRIDES: dict[str, tuple[str, str]] = {
    "Waukesha": (
        "validated",
        "Statewide records were previously reconciled against Waukesha County data.",
    ),
    "Milwaukee": (
        "county_override",
        "The statewide layer is missing nearly all City of Milwaukee addresses; use the county-specific source.",
    ),
    "Crawford": (
        "incomplete",
        "The statewide source snapshot contains only one Crawford County record.",
    ),
    "Iowa": ("unavailable", "County is absent from the statewide source snapshot."),
    "Kewaunee": ("unavailable", "County is absent from the statewide source snapshot."),
    "Lafayette": ("unavailable", "County is absent from the statewide source snapshot."),
    "Langlade": ("unavailable", "County is absent from the statewide source snapshot."),
    "Oneida": ("unavailable", "County is absent from the statewide source snapshot."),
    "Taylor": ("unavailable", "County is absent from the statewide source snapshot."),
    "Vilas": ("unavailable", "County is absent from the statewide source snapshot."),
}

REQUIRED_SOURCE_FIELDS: tuple[str, ...] = (
    "NGUID", "County", "AddNum_Pre", "Add_Number", "AddNum_Suf", "St_Name",
    "FullStNm", "Inc_Muni", "Post_Comm", "Post_Code", "DateUpdate",
)

OPTIONAL_SOURCE_FIELDS: tuple[str, ...] = (
    "AddCode", "AddDataURI", "Addtl_Loc", "Building", "Country",
    "DiscrpAgID", "ESN", "Effective", "Elev", "Expire", "Floor",
    "LSt_Name", "LSt_PosDir", "LSt_PreDir", "LSt_Type", "LandmkName",
    "Lat", "Long", "MSAGComm", "Nbrhd_Comm", "Place_Type", "Placement",
    "Post_Code4", "Room", "Seat", "St_PosDir", "St_PosMod", "St_PosTyp",
    "St_PreDir", "St_PreMod", "St_PreSep", "St_PreTyp", "State",
    "Uninc_Comm", "auto_id", "abFullStNm", "RCL_NGUID", "MilePost",
    "Unit_PreType", "Unit_Value", "Structure", "Exception",
    "F_gc_src_obj_id",
)

CURRENT_ANALYZER_COLUMNS: tuple[str, ...] = (
    "Canonical_HouseNo", "Canonical_HouseSx", "Canonical_Dir",
    "Canonical_Street", "Canonical_StType", "Canonical_SuffixDir",
    "Canonical_Muni", "Canonical_Zip_Code", "Canonical_UnitType",
    "Canonical_Unit", "Canonical_Status", "Canonical_Full_Address",
    "Canonical_Native_Source_ID", "geometry",
)

RUNTIME_COLUMNS: tuple[str, ...] = (
    # Source and identity
    "Source_System", "Source_Version", "Source_FID", "Source_County",
    "Source_State", "Canonical_County", "Canonical_Native_Source_ID",
    "Source_Record_ID", "NGUID", "RCL_NGUID",
    # House number
    "AddNum_Pre", "Add_Number", "AddNum_Suf", "Canonical_HouseNoPrefix",
    "Canonical_HouseNo", "Canonical_HouseSx", "Canonical_Full_House_Number",
    # Street
    "St_PreMod", "St_PreDir", "St_PreTyp", "St_PreSep", "St_Name",
    "St_PosTyp", "St_PosDir", "St_PosMod", "FullStNm", "abFullStNm",
    "Canonical_Street_PreModifier", "Canonical_Dir",
    "Canonical_Street_PreType", "Canonical_Street_PreSeparator",
    "Canonical_Street", "Canonical_StType", "Canonical_SuffixDir",
    "Canonical_Street_PostModifier", "Canonical_Full_Street",
    "Canonical_Abbreviated_Street", "Canonical_Street_Component_Fallback",
    # Units and subaddresses
    "Unit_PreType", "Unit_Value", "Building", "Floor", "Room", "Seat",
    "Addtl_Loc", "Canonical_UnitType", "Canonical_Unit",
    "Canonical_Building", "Canonical_Floor", "Canonical_Room",
    "Canonical_Seat", "Canonical_Additional_Location",
    "Canonical_Subaddress", "Canonical_Occupancy_Key",
    "Canonical_Unit_Category", "Canonical_Unit_Classification_Confidence",
    # Locality and postal
    "Inc_Muni", "Uninc_Comm", "Nbrhd_Comm", "MSAGComm", "Post_Comm",
    "Post_Code", "Post_Code4", "Canonical_Muni", "Canonical_Postal_City",
    "Canonical_Incorporated_Municipality",
    "Canonical_Unincorporated_Community", "Canonical_Neighborhood",
    "Canonical_MSAG_Community", "Canonical_Locality_Source",
    "Canonical_Postal_City_Fallback_Flag", "Canonical_Zip_Code",
    "Canonical_ZIP4", "Canonical_Full_ZIP", "Canonical_ZIP_Quality_Flag",
    # Classification and landmarks
    "LandmkName", "Place_Type", "Placement", "Structure", "Exception",
    "Canonical_Landmark_Name", "Source_Place_Type", "Source_Placement",
    "Source_Structure", "Source_Exception", "Canonical_Record_Role",
    "Canonical_Record_Role_Confidence", "Canonical_Record_Role_Reasons",
    "Canonical_Status",
    # Dates and eligibility
    "DateUpdate", "Effective", "Expire", "DateUpdate_Raw",
    "DateUpdate_Parsed", "Effective_Raw", "Effective_Parsed", "Expire_Raw",
    "Expire_Parsed", "Canonical_Active_Status",
    "Canonical_Eligibility_Status", "Canonical_Date_Quality_Flags",
    # Address and grouping
    "Canonical_Full_Address", "Canonical_Mailable_Address",
    "Normalized_Address_Key", "Normalized_Address_Without_Unit_Key",
    "Normalized_Building_Address_Key", "Potential_Parent_Record",
    "Potential_Child_Record", "Parent_Group_Key", "Child_Record_Count",
    "Parent_Child_Confidence", "Potential_Double_Count_Flag",
    # Occupancy
    "Canonical_Occupancy_Category", "Canonical_Residential_Unit_Flag",
    "Canonical_Commercial_Unit_Flag", "Canonical_Apartment_Candidate_Flag",
    "Canonical_Occupancy_Confidence", "Canonical_Occupancy_Reasons",
    # QA and geometry
    "Canonical_Quality_Flags", "Canonical_Address_Quality_Status",
    "Canonical_Geometry_Quality_Status",
    "Canonical_Classification_Quality_Status", "Canonical_Postal_Quality_Status",
    "Canonical_Latitude", "Canonical_Longitude", "Source_Lat", "Source_Long",
    "Source_Coordinate_Difference_Meters", "geometry",
)


@dataclass(slots=True)
class PipelineConfig:
    input_path: Path
    output_dir: Path
    as_of_date: date
    as_of_date_source: str
    counties: tuple[str, ...] | None
    overwrite: bool
    runtime_only: bool
    full_fidelity_only: bool
    keep_extracted_gdb: bool
    row_group_size: int
    previous_manifest: Path | None
    fail_fast: bool
    wisconsin_bounds: tuple[float, float, float, float] = DEFAULT_WISCONSIN_BOUNDS


@dataclass(slots=True)
class SourceMetadata:
    input_path: str
    input_kind: str
    input_size_bytes: int
    source_sha256: str
    gdb_path: str
    layer_name: str
    feature_count: int
    source_fields: list[str]
    source_dtypes: list[str]
    geometry_type: str
    crs: str
    total_bounds: tuple[float, float, float, float]
    driver: str
    encoding: str | None
    source_timestamp: str | None
    processing_timestamp_utc: str


@dataclass
class ReportStore:
    county_summary: list[dict[str, Any]] = field(default_factory=list)
    county_name_variants: list[dict[str, Any]] = field(default_factory=list)
    field_completeness: list[dict[str, Any]] = field(default_factory=list)
    classification_values: list[dict[str, Any]] = field(default_factory=list)
    record_role_summary: list[dict[str, Any]] = field(default_factory=list)
    occupancy_summary: list[dict[str, Any]] = field(default_factory=list)
    parent_child_summary: list[dict[str, Any]] = field(default_factory=list)
    date_anomalies: list[dict[str, Any]] = field(default_factory=list)
    coordinate_anomalies: list[dict[str, Any]] = field(default_factory=list)
    duplicate_address_summary: list[dict[str, Any]] = field(default_factory=list)
    duplicate_nguid_report: list[dict[str, Any]] = field(default_factory=list)
    postal_fallback_summary: list[dict[str, Any]] = field(default_factory=list)
    quarantine_summary: list[dict[str, Any]] = field(default_factory=list)
    output_files: list[dict[str, Any]] = field(default_factory=list)
    failures: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class CountyOutput:
    county: str
    source_count: int
    runtime_count: int
    quarantine_count: int
    full_fidelity_path: str | None
    runtime_path: str | None
    quarantine_path: str | None
    full_fidelity_sha256: str | None
    runtime_sha256: str | None
    quarantine_sha256: str | None
    full_fidelity_size: int | None
    runtime_size: int | None
    quarantine_size: int | None
    covering_bbox_written: bool
    bbox_read_validated: bool
    validation_passed: bool
    validation_messages: list[str]
    metrics: dict[str, Any]


class PipelineError(RuntimeError):
    """Base error for source or pipeline failures."""


class ValidationError(PipelineError):
    """Raised when an output fails a critical reconciliation check."""


def require_dependencies() -> None:
    """Fail with an actionable message when required packages are unavailable."""
    if DEPENDENCY_ERRORS:
        details = "\n  - ".join(DEPENDENCY_ERRORS)
        raise PipelineError(
            "Missing required dependencies:\n  - " + details
            + "\nInstall geopandas, pandas, pyogrio, pyarrow, shapely, and numpy."
        )


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(HASH_CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_directory(path: Path) -> str:
    """Hash directory paths and file contents deterministically."""
    digest = hashlib.sha256()
    for child in sorted(p for p in path.rglob("*") if p.is_file()):
        relative = child.relative_to(path).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(child.stat().st_size.to_bytes(8, "big"))
        with child.open("rb") as handle:
            while chunk := handle.read(HASH_CHUNK_SIZE):
                digest.update(chunk)
    return digest.hexdigest()


def atomic_write_json(path: Path, payload: Any) -> None:
    ensure_directory(path.parent)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False, default=str)
        handle.write("\n")
    os.replace(temporary, path)


def atomic_write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    ensure_directory(path.parent)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    if rows:
        fieldnames: list[str] = []
        seen: set[str] = set()
        for row in rows:
            for key in row:
                if key not in seen:
                    seen.add(key)
                    fieldnames.append(key)
        with temporary.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
    else:
        temporary.write_text("", encoding="utf-8")
    os.replace(temporary, path)


def normalize_whitespace(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    text = unicodedata.normalize("NFKC", str(value)).strip()
    if text.lower() in {"nan", "none", "<na>", "nat"}:
        return ""
    return re.sub(r"\s+", " ", text)


def clean_series(frame: "pd.DataFrame", column: str) -> "pd.Series":
    if column not in frame.columns:
        return pd.Series("", index=frame.index, dtype="string")
    values = frame[column].astype("string").fillna("")
    values = values.str.normalize("NFKC").str.strip().str.replace(r"\s+", " ", regex=True)
    return values.replace({"nan": "", "None": "", "<NA>": "", "NaT": ""})


def append_flag_series(base: "pd.Series", mask: "pd.Series", flag: str) -> "pd.Series":
    mask = mask.fillna(False)
    existing = base.fillna("").astype("string")
    addition = pd.Series(np.where(mask, flag, ""), index=base.index, dtype="string")
    return pd.Series(
        np.where(
            addition.eq(""),
            existing,
            np.where(existing.eq(""), addition, existing + " | " + addition),
        ),
        index=base.index,
        dtype="string",
    )


def county_key(value: Any) -> str:
    text = normalize_whitespace(value).upper()
    text = re.sub(r"\bCOUNTY\b\s*$", "", text).strip()
    text = text.replace("SAINT", "ST")
    return re.sub(r"[^A-Z0-9]+", "", text)


COUNTY_KEY_LOOKUP: dict[str, str] = {county_key(name): name for name in WI_COUNTIES}
COUNTY_KEY_LOOKUP.update({
    "STCROIX": "St. Croix",
    "FONDDULAC": "Fond du Lac",
    "LACROSSE": "La Crosse",
    "EAUCLAIRE": "Eau Claire",
    "GREENLAKE": "Green Lake",
})


def normalize_county_name(value: Any) -> str | None:
    return COUNTY_KEY_LOOKUP.get(county_key(value))


def county_slug(county: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", county.lower()).strip("_")


def county_prefix(county: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "_", county.upper()).strip("_")


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_where_for_variants(field_name: str, variants: Sequence[str]) -> str:
    quoted = ", ".join(sql_literal(value) for value in variants)
    return f'"{field_name}" IN ({quoted})'


def zip_latest_timestamp(path: Path) -> datetime | None:
    if not zipfile.is_zipfile(path):
        return None
    latest: datetime | None = None
    with zipfile.ZipFile(path) as archive:
        for info in archive.infolist():
            candidate = datetime(*info.date_time)
            if latest is None or candidate > latest:
                latest = candidate
    return latest


def derive_as_of_date(input_path: Path, explicit: str | None) -> tuple[date, str]:
    if explicit:
        try:
            return date.fromisoformat(explicit), "explicit --as-of-date"
        except ValueError as exc:
            raise PipelineError("--as-of-date must use YYYY-MM-DD format") from exc
    timestamp = zip_latest_timestamp(input_path) if input_path.is_file() else None
    if timestamp:
        return timestamp.date(), "latest internal ZIP timestamp"
    return date.today(), "local processing date fallback"


def setup_logging(output_dir: Path, level: str) -> None:
    log_dir = ensure_directory(output_dir / "logs")
    log_path = log_dir / "preparation.log"
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    LOGGER.setLevel(numeric_level)
    LOGGER.handlers.clear()
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    console.setLevel(numeric_level)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(numeric_level)
    LOGGER.addHandler(console)
    LOGGER.addHandler(file_handler)


def extract_or_locate_gdb(
    input_path: Path,
    output_dir: Path,
    keep_extracted: bool,
) -> tuple[Path, tempfile.TemporaryDirectory[str] | None, str]:
    """Return the GDB path, temporary owner, and source kind."""
    if input_path.is_dir() and input_path.suffix.lower() == ".gdb":
        return input_path.resolve(), None, "file_geodatabase"
    if not input_path.is_file() or not zipfile.is_zipfile(input_path):
        raise PipelineError("Input must be a ZIP archive or an extracted .gdb directory")

    if keep_extracted:
        destination = ensure_directory(output_dir / "source_metadata" / "extracted")
        marker = destination / ".extracted_from_sha256"
        source_hash = sha256_file(input_path)
        existing_gdbs = list(destination.rglob("*.gdb"))
        if marker.exists() and marker.read_text(encoding="utf-8").strip() == source_hash and len(existing_gdbs) == 1:
            return existing_gdbs[0], None, "zip_archive"
        if destination.exists():
            for child in destination.iterdir():
                if child.is_dir():
                    shutil.rmtree(child)
                else:
                    child.unlink()
        with zipfile.ZipFile(input_path) as archive:
            archive.extractall(destination)
        marker.write_text(source_hash, encoding="utf-8")
        gdbs = list(destination.rglob("*.gdb"))
        if len(gdbs) != 1:
            raise PipelineError(f"Expected exactly one .gdb after extraction; found {len(gdbs)}")
        return gdbs[0], None, "zip_archive"

    temporary = tempfile.TemporaryDirectory(prefix="wi_ng911_gdb_")
    destination = Path(temporary.name)
    with zipfile.ZipFile(input_path) as archive:
        archive.extractall(destination)
    gdbs = list(destination.rglob("*.gdb"))
    if len(gdbs) != 1:
        temporary.cleanup()
        raise PipelineError(f"Expected exactly one .gdb after extraction; found {len(gdbs)}")
    return gdbs[0], temporary, "zip_archive"


def inspect_source(
    input_path: Path,
    gdb_path: Path,
    input_kind: str,
) -> SourceMetadata:
    layers = pyogrio.list_layers(gdb_path)
    names = [str(row[0]) for row in layers]
    if LAYER_NAME not in names:
        raise PipelineError(f"Required layer {LAYER_NAME!r} not found. Layers: {names}")
    info = pyogrio.read_info(gdb_path, layer=LAYER_NAME)
    source_fields = [str(value) for value in info["fields"]]
    missing = [field for field in REQUIRED_SOURCE_FIELDS if field not in source_fields]
    if missing:
        raise PipelineError("Source layer is missing required fields: " + ", ".join(missing))
    geometry_type = str(info.get("geometry_type"))
    if geometry_type.lower() != "point":
        raise PipelineError(f"Expected Point geometry; found {geometry_type}")
    crs = str(info.get("crs") or "")
    if not crs:
        raise PipelineError("Source layer has no CRS")
    if input_path.is_file():
        source_hash = sha256_file(input_path)
        input_size = input_path.stat().st_size
    else:
        source_hash = sha256_directory(input_path)
        input_size = sum(p.stat().st_size for p in input_path.rglob("*") if p.is_file())
    source_timestamp_dt = zip_latest_timestamp(input_path) if input_path.is_file() else None
    metadata = SourceMetadata(
        input_path=str(input_path.resolve()),
        input_kind=input_kind,
        input_size_bytes=input_size,
        source_sha256=source_hash,
        gdb_path=str(gdb_path.resolve()),
        layer_name=LAYER_NAME,
        feature_count=int(info.get("features") or 0),
        source_fields=source_fields,
        source_dtypes=[str(value) for value in info.get("dtypes", [])],
        geometry_type=geometry_type,
        crs=crs,
        total_bounds=tuple(float(v) for v in info.get("total_bounds")),
        driver=str(info.get("driver") or ""),
        encoding=info.get("encoding"),
        source_timestamp=source_timestamp_dt.isoformat() if source_timestamp_dt else None,
        processing_timestamp_utc=utc_now_iso(),
    )
    if metadata.feature_count <= 0:
        raise PipelineError("Source layer contains no features")
    return metadata


def write_source_metadata(output_dir: Path, metadata: SourceMetadata) -> None:
    metadata_dir = ensure_directory(output_dir / "source_metadata")
    atomic_write_json(metadata_dir / "source_inventory.json", asdict(metadata))
    field_rows = []
    dtype_lookup = dict(zip(metadata.source_fields, metadata.source_dtypes))
    for ordinal, field_name in enumerate(metadata.source_fields, start=1):
        field_rows.append({
            "ordinal": ordinal,
            "field": field_name,
            "dtype": dtype_lookup.get(field_name, ""),
            "required": field_name in REQUIRED_SOURCE_FIELDS,
            "optional_known": field_name in OPTIONAL_SOURCE_FIELDS,
        })
    atomic_write_csv(metadata_dir / "source_fields.csv", field_rows)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Standardize Wisconsin statewide NG911 address points into county GeoParquet files."
    )
    parser.add_argument("--input", required=True, type=Path, help="Source ZIP or extracted .gdb")
    parser.add_argument("--output", required=True, type=Path, help="Pipeline output directory")
    parser.add_argument("--as-of-date", help="Eligibility reference date in YYYY-MM-DD")
    parser.add_argument("--counties", nargs="+", help="Canonical county names to process")
    parser.add_argument("--overwrite", action="store_true", help="Replace existing county outputs")
    parser.add_argument("--runtime-only", action="store_true", help="Skip full-fidelity county files")
    parser.add_argument("--full-fidelity-only", action="store_true", help="Skip runtime county files")
    parser.add_argument("--keep-extracted-gdb", action="store_true", help="Retain extracted GDB under output/source_metadata/extracted")
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    parser.add_argument("--row-group-size", type=int, default=DEFAULT_ROW_GROUP_SIZE)
    parser.add_argument("--previous-manifest", type=Path)
    parser.add_argument("--fail-fast", action="store_true")
    args = parser.parse_args(argv)
    if args.runtime_only and args.full_fidelity_only:
        parser.error("--runtime-only and --full-fidelity-only are mutually exclusive")
    if args.row_group_size < 1_000:
        parser.error("--row-group-size must be at least 1000")
    return args


def validate_requested_counties(values: Sequence[str] | None) -> tuple[str, ...] | None:
    if not values:
        return None
    normalized: list[str] = []
    for raw in values:
        county = normalize_county_name(raw)
        if not county:
            raise PipelineError(f"Unknown Wisconsin county: {raw!r}")
        if county not in normalized:
            normalized.append(county)
    return tuple(normalized)


def read_inventory(
    gdb_path: Path,
    source_feature_count: int,
    reports: ReportStore,
) -> tuple["pd.DataFrame", dict[str, tuple[str, ...]], set[str]]:
    """Read only County and NGUID for statewide inventory and identity QA."""
    LOGGER.info("Reading statewide County and NGUID inventory (%s records)", f"{source_feature_count:,}")
    inventory = pyogrio.read_dataframe(
        gdb_path,
        layer=LAYER_NAME,
        columns=["County", "NGUID"],
        read_geometry=False,
        datetime_as_string=True,
        use_arrow=True,
    )
    if len(inventory) != source_feature_count:
        raise ValidationError(
            f"Inventory row count {len(inventory):,} does not match source feature count {source_feature_count:,}"
        )
    inventory["Source_County"] = clean_series(inventory, "County")
    inventory["Canonical_County"] = inventory["Source_County"].map(normalize_county_name)
    unknown_values = sorted(inventory.loc[inventory["Canonical_County"].isna(), "Source_County"].unique())
    if unknown_values:
        raise ValidationError("Unrecognized county values: " + ", ".join(repr(v) for v in unknown_values))
    variants: dict[str, tuple[str, ...]] = {}
    for county in WI_COUNTIES:
        values = tuple(sorted(inventory.loc[inventory["Canonical_County"].eq(county), "Source_County"].unique()))
        variants[county] = values
        count = int(inventory["Canonical_County"].eq(county).sum())
        status, reason = STATUS_OVERRIDES.get(
            county,
            (("needs_validation", "Represented in source but not independently validated") if count else ("unavailable", "County is absent from the statewide source snapshot")),
        )
        reports.county_name_variants.append({
            "canonical_county": county,
            "raw_variants": " | ".join(values),
            "source_record_count": count,
            "represented": bool(count),
            "initial_status": status,
            "status_reason": reason,
        })
    nguid = clean_series(inventory, "NGUID")
    missing_mask = nguid.eq("")
    if missing_mask.any():
        LOGGER.error("Statewide inventory contains %s missing NGUID values", f"{int(missing_mask.sum()):,}")
    duplicate_mask = nguid.ne("") & nguid.duplicated(keep=False)
    duplicate_values = set(nguid[duplicate_mask].tolist())
    if duplicate_values:
        duplicate_rows = inventory.loc[duplicate_mask, ["County", "NGUID"]].copy()
        for row in duplicate_rows.itertuples(index=False):
            reports.duplicate_nguid_report.append({"County": row.County, "NGUID": row.NGUID})
        LOGGER.error("Statewide inventory contains %s duplicated NGUID values", f"{len(duplicate_values):,}")
    return inventory, variants, duplicate_values


class WisconsinStatewideNG911Adapter:
    """Source adapter that maps the statewide NG911 schema to Territory Toolbox."""

    def __init__(
        self,
        gdb_path: Path,
        metadata: SourceMetadata,
        config: PipelineConfig,
        county_variants: Mapping[str, Sequence[str]],
        duplicate_nguids: set[str],
    ) -> None:
        self.gdb_path = gdb_path
        self.metadata = metadata
        self.config = config
        self.county_variants = county_variants
        self.duplicate_nguids = duplicate_nguids
        self.source_version = (
            metadata.source_timestamp[:10] if metadata.source_timestamp else config.as_of_date.isoformat()
        )

    def read_county(self, county: str) -> "gpd.GeoDataFrame":
        variants = self.county_variants[county]
        if not variants:
            return gpd.GeoDataFrame(columns=[*self.metadata.source_fields, "geometry"], geometry="geometry", crs=self.metadata.crs)
        where = build_where_for_variants("County", variants)
        LOGGER.info("Reading %s County using source variant(s): %s", county, ", ".join(variants))
        frame = pyogrio.read_dataframe(
            self.gdb_path,
            layer=LAYER_NAME,
            where=where,
            fid_as_index=True,
            datetime_as_string=True,
            use_arrow=True,
            on_invalid="warn",
        )
        frame = frame.reset_index().rename(columns={frame.index.name or "fid": "Source_FID", "fid": "Source_FID"})
        if "Source_FID" not in frame.columns:
            frame.insert(0, "Source_FID", pd.Series(range(len(frame)), dtype="int64"))
        if frame.crs is None:
            frame = frame.set_crs(self.metadata.crs)
        if str(frame.crs).upper() != "EPSG:4326":
            frame = frame.to_crs("EPSG:4326")
        return frame

    def standardize_county(
        self,
        county: str,
        source: "gpd.GeoDataFrame",
        reports: ReportStore,
    ) -> tuple["gpd.GeoDataFrame", "gpd.GeoDataFrame", "gpd.GeoDataFrame", dict[str, Any]]:
        frame = source.copy()
        for field_name in self.metadata.source_fields:
            if field_name not in frame.columns:
                frame[field_name] = pd.NA
        frame["Source_System"] = SOURCE_SYSTEM
        frame["Source_Version"] = self.source_version
        frame["Source_County"] = clean_series(frame, "County")
        frame["Source_State"] = clean_series(frame, "State").replace("", SOURCE_STATE)
        frame["Canonical_County"] = county
        frame["Canonical_Native_Source_ID"] = clean_series(frame, "NGUID")
        frame["Source_Record_ID"] = county_prefix(county) + "-" + frame["Canonical_Native_Source_ID"]

        geometry = frame.geometry
        geometry_present = geometry.notna() & ~geometry.is_empty
        point_mask = geometry_present & geometry.geom_type.eq("Point")
        x_values = pd.Series(np.nan, index=frame.index, dtype="float64")
        y_values = pd.Series(np.nan, index=frame.index, dtype="float64")
        if point_mask.any():
            x_values.loc[point_mask] = geometry.loc[point_mask].x
            y_values.loc[point_mask] = geometry.loc[point_mask].y
        finite_mask = point_mask & np.isfinite(x_values) & np.isfinite(y_values)
        minx, miny, maxx, maxy = self.config.wisconsin_bounds
        envelope_mask = finite_mask & x_values.between(minx, maxx) & y_values.between(miny, maxy)
        frame["Canonical_Longitude"] = x_values
        frame["Canonical_Latitude"] = y_values
        frame["Source_Lat"] = frame["Lat"] if "Lat" in frame.columns else pd.NA
        frame["Source_Long"] = frame["Long"] if "Long" in frame.columns else pd.NA

        nguid = clean_series(frame, "NGUID")
        missing_nguid = nguid.eq("")
        duplicate_nguid = nguid.isin(self.duplicate_nguids)
        quarantine_reasons = pd.Series("", index=frame.index, dtype="string")
        quarantine_reasons = append_flag_series(quarantine_reasons, ~geometry_present, "Missing Geometry")
        quarantine_reasons = append_flag_series(quarantine_reasons, geometry_present & ~point_mask, "Non-Point Geometry")
        quarantine_reasons = append_flag_series(quarantine_reasons, point_mask & ~finite_mask, "Nonfinite Geometry")
        quarantine_reasons = append_flag_series(quarantine_reasons, finite_mask & ~envelope_mask, "Geometry Outside Wisconsin Envelope")
        quarantine_reasons = append_flag_series(quarantine_reasons, missing_nguid, "Missing NGUID")
        quarantine_reasons = append_flag_series(quarantine_reasons, duplicate_nguid, "Duplicate NGUID")
        frame["Quarantine_Reasons"] = quarantine_reasons
        quarantine_mask = quarantine_reasons.ne("")

        self._derive_house(frame)
        self._derive_street(frame)
        self._derive_subaddress(frame)
        self._derive_locality_and_postal(frame)
        self._derive_dates(frame)
        self._derive_source_coordinate_qa(frame)
        self._derive_addresses(frame)
        self._derive_classification(frame)
        self._derive_parent_child(frame)
        self._derive_quality(frame, envelope_mask)

        full_fidelity = gpd.GeoDataFrame(frame, geometry="geometry", crs="EPSG:4326")
        runtime = full_fidelity.loc[~quarantine_mask].copy()
        quarantine = full_fidelity.loc[quarantine_mask].copy()

        missing_runtime_columns = [column for column in RUNTIME_COLUMNS if column not in runtime.columns]
        for column in missing_runtime_columns:
            runtime[column] = pd.NA
        runtime = gpd.GeoDataFrame(runtime.loc[:, list(RUNTIME_COLUMNS)], geometry="geometry", crs="EPSG:4326")

        full_fidelity = spatially_order(full_fidelity)
        runtime = spatially_order(runtime)
        quarantine = spatially_order(quarantine)
        metrics = self._collect_metrics(county, full_fidelity, runtime, quarantine, reports)
        return full_fidelity, runtime, quarantine, metrics

    def _derive_house(self, frame: "gpd.GeoDataFrame") -> None:
        prefix = clean_series(frame, "AddNum_Pre")
        number = clean_series(frame, "Add_Number").str.replace(r"\.0+$", "", regex=True)
        suffix = clean_series(frame, "AddNum_Suf")
        frame["Canonical_HouseNoPrefix"] = prefix
        frame["Canonical_HouseNo"] = prefix + number
        frame["Canonical_HouseSx"] = suffix
        fraction_suffix = suffix.str.fullmatch(r"\d+\s*/\s*\d+", na=False)
        full = frame["Canonical_HouseNo"].astype("string")
        full = pd.Series(
            np.where(
                suffix.eq(""),
                full,
                np.where(fraction_suffix, full + " " + suffix, full + suffix),
            ),
            index=frame.index,
            dtype="string",
        ).str.strip()
        frame["Canonical_Full_House_Number"] = full

    def _derive_street(self, frame: "gpd.GeoDataFrame") -> None:
        mapping = {
            "Canonical_Street_PreModifier": "St_PreMod",
            "Canonical_Dir": "St_PreDir",
            "Canonical_Street_PreType": "St_PreTyp",
            "Canonical_Street_PreSeparator": "St_PreSep",
            "Canonical_Street": "St_Name",
            "Canonical_StType": "St_PosTyp",
            "Canonical_SuffixDir": "St_PosDir",
            "Canonical_Street_PostModifier": "St_PosMod",
        }
        for target, source in mapping.items():
            frame[target] = clean_series(frame, source)
        component_columns = list(mapping.keys())
        assembled = frame[component_columns].fillna("").astype("string").agg(" ".join, axis=1)
        assembled = assembled.str.replace(r"\s+", " ", regex=True).str.strip()
        source_full = clean_series(frame, "FullStNm")
        source_abbrev = clean_series(frame, "abFullStNm")
        frame["Canonical_Full_Street"] = source_full.where(source_full.ne(""), assembled)
        frame["Canonical_Abbreviated_Street"] = source_abbrev.where(source_abbrev.ne(""), frame["Canonical_Full_Street"])
        frame["Canonical_Street_Component_Fallback"] = source_full.eq("") & assembled.ne("")

    def _derive_subaddress(self, frame: "gpd.GeoDataFrame") -> None:
        source_map = {
            "Canonical_UnitType": "Unit_PreType",
            "Canonical_Unit": "Unit_Value",
            "Canonical_Building": "Building",
            "Canonical_Floor": "Floor",
            "Canonical_Room": "Room",
            "Canonical_Seat": "Seat",
            "Canonical_Additional_Location": "Addtl_Loc",
        }
        for target, source in source_map.items():
            frame[target] = clean_series(frame, source)
        type_upper = frame["Canonical_UnitType"].str.upper()
        category = pd.Series("Unknown", index=frame.index, dtype="string")
        category = category.mask(type_upper.str.contains(r"\b(?:APT|APARTMENT)\b", regex=True), "Apartment")
        category = category.mask(type_upper.str.contains(r"\b(?:CONDO|CONDOMINIUM)\b", regex=True), "Condominium")
        category = category.mask(type_upper.str.contains(r"\b(?:SUITE|STE)\b", regex=True), "Suite")
        category = category.mask(type_upper.str.contains(r"\b(?:OFFICE)\b", regex=True), "Office")
        category = category.mask(type_upper.str.contains(r"\b(?:ROOM|RM)\b", regex=True), "Room")
        category = category.mask(type_upper.str.contains(r"\b(?:FLOOR|FL)\b", regex=True), "Floor")
        category = category.mask(type_upper.str.contains(r"\b(?:BUILDING|BLDG)\b", regex=True), "Building")
        category = category.mask(type_upper.str.contains(r"\b(?:LOT)\b", regex=True), "Lot")
        category = category.mask(type_upper.str.contains(r"\b(?:TRAILER|TRLR)\b", regex=True), "Trailer")
        category = category.mask(type_upper.str.contains(r"\b(?:SITE|SPACE)\b", regex=True), "Site")
        category = category.mask(type_upper.str.contains(r"\b(?:UNIT)\b", regex=True), "Unit")
        no_value = frame["Canonical_Unit"].eq("")
        category = category.mask(no_value & type_upper.eq(""), "None")
        frame["Canonical_Unit_Category"] = category
        frame["Canonical_Unit_Classification_Confidence"] = np.where(
            type_upper.ne(""), "High", np.where(no_value, "Not Applicable", "Low")
        )

        pieces = []
        label_pairs = (
            ("Canonical_Building", "Building"),
            ("Canonical_Floor", "Floor"),
            ("Canonical_Unit", None),
            ("Canonical_Room", "Room"),
            ("Canonical_Seat", "Seat"),
            ("Canonical_Additional_Location", None),
        )
        for column, label in label_pairs:
            values = frame[column].astype("string").fillna("")
            if column == "Canonical_Unit":
                unit_type = frame["Canonical_UnitType"].astype("string").fillna("")
                unit_piece = pd.Series(
                    np.where(
                        values.eq(""),
                        "",
                        np.where(unit_type.ne(""), unit_type + " " + values, "Unit " + values),
                    ),
                    index=frame.index,
                    dtype="string",
                )
                pieces.append(unit_piece)
            elif label:
                pieces.append(pd.Series(np.where(values.ne(""), label + " " + values, ""), index=frame.index, dtype="string"))
            else:
                pieces.append(values)
        subaddress = pd.concat(pieces, axis=1).fillna("").agg(" ".join, axis=1)
        frame["Canonical_Subaddress"] = subaddress.str.replace(r"\s+", " ", regex=True).str.strip()
        occupancy_parts = frame[[
            "Canonical_Building", "Canonical_Floor", "Canonical_UnitType",
            "Canonical_Unit", "Canonical_Room", "Canonical_Seat",
            "Canonical_Additional_Location",
        ]].fillna("").astype("string").agg("|".join, axis=1)
        frame["Canonical_Occupancy_Key"] = occupancy_parts.map(normalize_key)

    def _derive_locality_and_postal(self, frame: "gpd.GeoDataFrame") -> None:
        frame["Canonical_Postal_City"] = clean_series(frame, "Post_Comm")
        frame["Canonical_Incorporated_Municipality"] = clean_series(frame, "Inc_Muni")
        frame["Canonical_Unincorporated_Community"] = clean_series(frame, "Uninc_Comm")
        frame["Canonical_Neighborhood"] = clean_series(frame, "Nbrhd_Comm")
        frame["Canonical_MSAG_Community"] = clean_series(frame, "MSAGComm")
        hierarchy = (
            ("Canonical_Postal_City", "Post_Comm"),
            ("Canonical_Unincorporated_Community", "Uninc_Comm"),
            ("Canonical_Incorporated_Municipality", "Inc_Muni"),
            ("Canonical_MSAG_Community", "MSAGComm"),
        )
        locality = pd.Series("", index=frame.index, dtype="string")
        source = pd.Series("", index=frame.index, dtype="string")
        for column, source_name in hierarchy:
            use = locality.eq("") & frame[column].astype("string").ne("")
            locality = locality.mask(use, frame[column])
            source = source.mask(use, source_name)
        frame["Canonical_Muni"] = locality
        frame["Canonical_Locality_Source"] = source
        frame["Canonical_Postal_City_Fallback_Flag"] = locality.ne("") & source.ne("Post_Comm")

        zip5 = clean_series(frame, "Post_Code").map(normalize_zip5)
        zip4 = clean_series(frame, "Post_Code4").map(normalize_zip4)
        frame["Canonical_Zip_Code"] = zip5
        frame["Canonical_ZIP4"] = zip4
        frame["Canonical_Full_ZIP"] = pd.Series(
            np.where(zip5.eq(""), "", np.where(zip4.ne(""), zip5 + "-" + zip4, zip5)),
            index=frame.index,
            dtype="string",
        )
        raw_zip = clean_series(frame, "Post_Code")
        raw_zip4 = clean_series(frame, "Post_Code4")
        zip_flags = pd.Series("", index=frame.index, dtype="string")
        zip_flags = append_flag_series(zip_flags, raw_zip.ne("") & zip5.eq(""), "Invalid ZIP")
        zip_flags = append_flag_series(zip_flags, raw_zip4.ne("") & zip4.eq(""), "Invalid ZIP4")
        zip_flags = append_flag_series(zip_flags, raw_zip.eq(""), "Missing ZIP")
        frame["Canonical_ZIP_Quality_Flag"] = zip_flags

    def _derive_dates(self, frame: "gpd.GeoDataFrame") -> None:
        parsed: dict[str, pd.Series] = {}
        flags = pd.Series("", index=frame.index, dtype="string")
        for source in ("DateUpdate", "Effective", "Expire"):
            raw_column = f"{source}_Raw"
            parsed_column = f"{source}_Parsed"
            raw = clean_series(frame, source)
            frame[raw_column] = raw
            parsed_values = pd.to_datetime(raw.replace("", pd.NA), errors="coerce", format="mixed", utc=True)
            parsed[source] = parsed_values
            frame[parsed_column] = parsed_values
            invalid = raw.ne("") & parsed_values.isna()
            flags = append_flag_series(flags, invalid, f"Invalid {source}")
            if source == "DateUpdate":
                placeholder = parsed_values.dt.date.eq(date(1970, 1, 1))
                flags = append_flag_series(flags, placeholder.fillna(False), "Placeholder DateUpdate 1970-01-01")
        frame["Canonical_Date_Quality_Flags"] = flags
        as_of = pd.Timestamp(self.config.as_of_date, tz="UTC")
        effective = parsed["Effective"]
        expire = parsed["Expire"]
        future = effective.notna() & effective.gt(as_of)
        expired = expire.notna() & expire.le(as_of)
        uncertain = (
            (clean_series(frame, "Effective").ne("") & effective.isna())
            | (clean_series(frame, "Expire").ne("") & expire.isna())
        )
        active_status = pd.Series("Active", index=frame.index, dtype="string")
        active_status = active_status.mask(future, "Future Effective")
        active_status = active_status.mask(expired, "Expired")
        active_status = active_status.mask(uncertain & ~future & ~expired, "Date Uncertain")
        frame["Canonical_Active_Status"] = active_status
        frame["Canonical_Eligibility_Status"] = active_status.map({
            "Active": "Technically Usable",
            "Future Effective": "Review - Future Effective",
            "Expired": "Review - Expired",
            "Date Uncertain": "Review - Date Uncertain",
        }).astype("string")

    def _derive_source_coordinate_qa(self, frame: "gpd.GeoDataFrame") -> None:
        source_lat = pd.to_numeric(frame["Source_Lat"], errors="coerce")
        source_long = pd.to_numeric(frame["Source_Long"], errors="coerce")
        plausible = source_lat.between(40, 50) & source_long.between(-95, -84)
        distance = pd.Series(np.nan, index=frame.index, dtype="float64")
        if plausible.any():
            distance.loc[plausible] = haversine_meters(
                source_long.loc[plausible].to_numpy(),
                source_lat.loc[plausible].to_numpy(),
                frame.loc[plausible, "Canonical_Longitude"].to_numpy(),
                frame.loc[plausible, "Canonical_Latitude"].to_numpy(),
            )
        frame["Source_Coordinate_Difference_Meters"] = distance

    def _derive_addresses(self, frame: "gpd.GeoDataFrame") -> None:
        full_house = frame["Canonical_Full_House_Number"].astype("string").fillna("")
        full_street = frame["Canonical_Full_Street"].astype("string").fillna("")
        street_line = (full_house + " " + full_street).str.replace(r"\s+", " ", regex=True).str.strip()
        locality = frame["Canonical_Muni"].astype("string").fillna("")
        state = frame["Source_State"].astype("string").fillna(SOURCE_STATE)
        full_zip = frame["Canonical_Full_ZIP"].astype("string").fillna("")
        locality_line = (locality + ", " + state + " " + full_zip).str.replace(r"\s+", " ", regex=True).str.strip(" ,")
        base_address = pd.Series(
            np.where(locality_line.ne(""), street_line + ", " + locality_line, street_line),
            index=frame.index,
            dtype="string",
        ).str.strip(" ,")
        subaddress = frame["Canonical_Subaddress"].astype("string").fillna("")
        mailable_street = pd.Series(
            np.where(subaddress.ne(""), street_line + " " + subaddress, street_line),
            index=frame.index,
            dtype="string",
        ).str.replace(r"\s+", " ", regex=True).str.strip()
        mailable = pd.Series(
            np.where(locality_line.ne(""), mailable_street + ", " + locality_line, mailable_street),
            index=frame.index,
            dtype="string",
        ).str.strip(" ,")
        frame["Canonical_Full_Address"] = base_address
        frame["Canonical_Mailable_Address"] = mailable
        frame["Normalized_Address_Key"] = mailable.map(normalize_key)
        frame["Normalized_Address_Without_Unit_Key"] = base_address.map(normalize_key)
        frame["Normalized_Building_Address_Key"] = base_address.map(normalize_key)
        frame["Parent_Group_Key"] = frame["Normalized_Building_Address_Key"]

    def _derive_classification(self, frame: "gpd.GeoDataFrame") -> None:
        landmark = clean_series(frame, "LandmkName")
        place_type = clean_series(frame, "Place_Type")
        placement = clean_series(frame, "Placement")
        structure = clean_series(frame, "Structure")
        exception = clean_series(frame, "Exception")
        frame["Canonical_Landmark_Name"] = landmark
        frame["Source_Place_Type"] = place_type
        frame["Source_Placement"] = placement
        frame["Source_Structure"] = structure
        frame["Source_Exception"] = exception
        combined = (
            landmark + " | " + place_type + " | " + placement + " | " + structure
            + " | " + frame["Canonical_UnitType"].astype("string").fillna("")
        ).str.upper()
        has_subaddress = frame["Canonical_Subaddress"].astype("string").ne("")
        unit_type = frame["Canonical_UnitType"].str.upper()

        occupancy = pd.Series("Unknown", index=frame.index, dtype="string")
        reasons = pd.Series("No confident occupancy rule matched", index=frame.index, dtype="string")
        confidence = pd.Series("Low", index=frame.index, dtype="string")

        apartment = unit_type.str.contains(r"\b(?:APT|APARTMENT|CONDO|CONDOMINIUM)\b", regex=True)
        residential_side = unit_type.str.contains(r"\b(?:UPPER|LOWER|FRONT|REAR)\b", regex=True)
        commercial = unit_type.str.contains(r"\b(?:SUITE|STE|OFFICE)\b", regex=True)
        hotel = combined.str.contains(r"\b(?:HOTEL|MOTEL|INN|LODGE)\b", regex=True) & has_subaddress
        dorm = combined.str.contains(r"\b(?:DORM|DORMITORY|RESIDENCE HALL)\b", regex=True) & has_subaddress
        campground = combined.str.contains(r"\b(?:CAMPGROUND|CAMPSITE|CAMP SITE)\b", regex=True) & has_subaddress
        mobile = combined.str.contains(r"\b(?:MOBILE HOME|TRAILER PARK|MANUFACTURED HOME)\b", regex=True) & has_subaddress
        storage = combined.str.contains(r"\b(?:STORAGE|WAREHOUSE UNIT)\b", regex=True) & has_subaddress

        def set_occ(mask: pd.Series, value: str, reason: str, conf: str) -> None:
            nonlocal occupancy, reasons, confidence
            occupancy = occupancy.mask(mask, value)
            reasons = reasons.mask(mask, reason)
            confidence = confidence.mask(mask, conf)

        set_occ(apartment, "Residential Apartment or Condominium", "Explicit apartment/condominium unit type", "High")
        set_occ(residential_side & occupancy.eq("Unknown"), "Residential Side or Duplex Unit", "Explicit upper/lower/front/rear unit type", "High")
        set_occ(commercial, "Commercial Suite or Office", "Explicit suite/office unit type", "High")
        set_occ(hotel, "Hotel or Motel Room", "Landmark/classification indicates lodging with a subaddress", "Medium")
        set_occ(dorm, "Dormitory Room", "Landmark/classification indicates a residence hall", "Medium")
        set_occ(campground, "Campground Site", "Landmark/classification indicates campground or campsite", "Medium")
        set_occ(mobile, "Mobile-home or Trailer Site", "Landmark/classification indicates mobile-home/trailer community", "Medium")
        set_occ(storage, "Storage Unit", "Landmark/classification indicates storage", "Medium")
        set_occ(has_subaddress & occupancy.eq("Unknown"), "Unknown Unit or Subaddress", "Subaddress exists but source does not establish occupancy type", "Low")
        set_occ(~has_subaddress, "No Unit Classification", "No subaddress fields are populated", "Not Applicable")

        frame["Canonical_Occupancy_Category"] = occupancy
        frame["Canonical_Residential_Unit_Flag"] = occupancy.isin((
            "Residential Apartment or Condominium", "Residential Side or Duplex Unit", "Dormitory Room",
        ))
        frame["Canonical_Commercial_Unit_Flag"] = occupancy.eq("Commercial Suite or Office")
        frame["Canonical_Apartment_Candidate_Flag"] = occupancy.eq("Residential Apartment or Condominium")
        frame["Canonical_Occupancy_Confidence"] = confidence
        frame["Canonical_Occupancy_Reasons"] = reasons

        role = pd.Series("Standalone Address", index=frame.index, dtype="string")
        role_reason = pd.Series("No unit or specialized placement classification", index=frame.index, dtype="string")
        role_conf = pd.Series("Medium", index=frame.index, dtype="string")
        utility = combined.str.contains(r"\b(?:UTILITY|HYDRANT|TOWER|SUBSTATION|TRANSFORMER)\b", regex=True)
        access = combined.str.contains(r"\b(?:ACCESS POINT|DRIVEWAY|GATE|ENTRANCE)\b", regex=True)
        building_entrance = combined.str.contains(r"\bBUILDING ENTRANCE\b", regex=True)
        unit_location = combined.str.contains(r"\bUNIT LOCATION\b", regex=True)
        parcel = combined.str.contains(r"\b(?:PARCEL|SITE LOCATION|PROPERTY ACCESS)\b", regex=True)
        landmark_facility = landmark.ne("") & combined.str.contains(r"\b(?:SCHOOL|CHURCH|HOSPITAL|FACILITY|PARK|CAMP|HOTEL|MOTEL)\b", regex=True)
        residential_unit = frame["Canonical_Residential_Unit_Flag"]
        commercial_unit = frame["Canonical_Commercial_Unit_Flag"]
        unknown_unit = has_subaddress & ~residential_unit & ~commercial_unit

        def set_role(mask: pd.Series, value: str, reason: str, conf: str) -> None:
            nonlocal role, role_reason, role_conf
            role = role.mask(mask, value)
            role_reason = role_reason.mask(mask, reason)
            role_conf = role_conf.mask(mask, conf)

        set_role(residential_unit, "Residential Unit", "Occupancy classification indicates residential unit", "High")
        set_role(commercial_unit, "Commercial Unit", "Occupancy classification indicates suite or office", "High")
        set_role(unknown_unit, "Unknown Unit Address", "Subaddress exists but occupancy type is uncertain", "Low")
        set_role(parcel, "Parcel or Site", "Source classification indicates parcel/site", "Medium")
        set_role(access, "Access Point", "Source placement/classification indicates access or entrance", "Medium")
        set_role(building_entrance, "Building Entrance", "Source placement explicitly indicates building entrance", "High")
        set_role(unit_location, "Unit Location", "Source placement explicitly indicates unit location", "High")
        set_role(utility, "Utility or Infrastructure", "Source classification indicates utility/infrastructure", "Medium")
        set_role(landmark_facility & role.eq("Standalone Address"), "Landmark or Facility", "Landmark and facility terms are populated", "Medium")
        frame["Canonical_Record_Role"] = role
        frame["Canonical_Record_Role_Confidence"] = role_conf
        frame["Canonical_Record_Role_Reasons"] = role_reason
        frame["Canonical_Status"] = role

    def _derive_parent_child(self, frame: "gpd.GeoDataFrame") -> None:
        key = frame["Parent_Group_Key"].astype("string").fillna("")
        has_subaddress = frame["Canonical_Subaddress"].astype("string").ne("")
        valid_key = key.ne("")
        child_count_map = frame.loc[valid_key & has_subaddress].groupby("Parent_Group_Key").size()
        group_count_map = frame.loc[valid_key].groupby("Parent_Group_Key").size()
        child_count = key.map(child_count_map).fillna(0).astype("int64")
        group_count = key.map(group_count_map).fillna(0).astype("int64")
        potential_parent = valid_key & ~has_subaddress & child_count.gt(0)
        potential_child = valid_key & has_subaddress & group_count.gt(1)
        frame["Potential_Parent_Record"] = potential_parent
        frame["Potential_Child_Record"] = potential_child
        frame["Child_Record_Count"] = child_count
        frame["Parent_Child_Confidence"] = np.where(
            potential_parent | potential_child,
            np.where(child_count.ge(2), "High", "Medium"),
            "Not Applicable",
        )
        frame["Potential_Double_Count_Flag"] = potential_parent
        frame.loc[potential_parent, "Canonical_Record_Role"] = "Building Parent"
        frame.loc[potential_parent, "Canonical_Record_Role_Confidence"] = np.where(
            child_count.loc[potential_parent].ge(2), "High", "Medium"
        )
        frame.loc[potential_parent, "Canonical_Record_Role_Reasons"] = (
            "Blank-subaddress record shares a normalized building address with child unit records"
        )
        frame.loc[potential_parent, "Canonical_Status"] = "Building Parent"

    def _derive_quality(self, frame: "gpd.GeoDataFrame", geometry_good: "pd.Series") -> None:
        flags = pd.Series("", index=frame.index, dtype="string")
        full_house = frame["Canonical_Full_House_Number"].astype("string").fillna("")
        full_street = frame["Canonical_Full_Street"].astype("string").fillna("")
        postal_city = frame["Canonical_Postal_City"].astype("string").fillna("")
        locality = frame["Canonical_Muni"].astype("string").fillna("")
        zip5 = frame["Canonical_Zip_Code"].astype("string").fillna("")
        zero_house = full_house.str.fullmatch(r"[A-Z]*0+(?:\.0+)?", na=False)
        flags = append_flag_series(flags, full_house.eq(""), "Missing House Number")
        flags = append_flag_series(flags, zero_house, "Zero House Number")
        flags = append_flag_series(flags, full_street.eq(""), "Missing Street")
        flags = append_flag_series(flags, postal_city.eq(""), "Missing Postal City")
        flags = append_flag_series(flags, frame["Canonical_Postal_City_Fallback_Flag"], "Postal City Fallback Used")
        flags = append_flag_series(flags, locality.eq(""), "Missing Municipality/Locality")
        flags = append_flag_series(flags, zip5.eq(""), "Missing ZIP")
        flags = append_flag_series(flags, frame["Canonical_ZIP_Quality_Flag"].astype("string").ne("") & ~frame["Canonical_ZIP_Quality_Flag"].astype("string").eq("Missing ZIP"), "Invalid ZIP Value")
        flags = append_flag_series(flags, frame["Canonical_Street_Component_Fallback"], "Full Street Fallback Used")
        flags = append_flag_series(flags, frame["Canonical_Unit"].astype("string").ne("") & frame["Canonical_UnitType"].astype("string").eq(""), "Unknown Unit Type")
        flags = append_flag_series(flags, frame["Potential_Double_Count_Flag"], "Parent Building with Child Units")
        flags = append_flag_series(flags, frame["Canonical_Date_Quality_Flags"].astype("string").ne(""), "Invalid or Placeholder Source Date")
        flags = append_flag_series(flags, frame["Source_Coordinate_Difference_Meters"].gt(100), "Source Coordinates Disagree with Geometry")
        flags = append_flag_series(flags, frame["Canonical_Record_Role_Confidence"].isin(("Low", "Unclassified")), "Unknown or Low-Confidence Record Role")
        flags = append_flag_series(flags, ~geometry_good, "Geometry Quality Failure")
        duplicate_address = frame["Normalized_Address_Key"].astype("string").ne("") & frame["Normalized_Address_Key"].duplicated(keep=False)
        flags = append_flag_series(flags, duplicate_address, "Potential Duplicate Normalized Address")
        frame["Canonical_Quality_Flags"] = flags
        frame["Canonical_Address_Quality_Status"] = np.where(
            full_house.ne("") & full_street.ne(""), "Usable", "Review"
        )
        frame["Canonical_Geometry_Quality_Status"] = np.where(geometry_good, "Usable", "Quarantine")
        frame["Canonical_Classification_Quality_Status"] = np.where(
            frame["Canonical_Record_Role_Confidence"].isin(("High", "Medium", "Not Applicable")),
            "Usable",
            "Review",
        )
        frame["Canonical_Postal_Quality_Status"] = np.where(
            postal_city.ne("") & zip5.ne(""),
            "Complete",
            np.where(locality.ne("") | zip5.ne(""), "Partial", "Missing"),
        )
        address_review = frame["Canonical_Address_Quality_Status"].eq("Review")
        frame.loc[address_review & frame["Canonical_Eligibility_Status"].eq("Technically Usable"), "Canonical_Eligibility_Status"] = "Review - Address Quality"

    def _collect_metrics(
        self,
        county: str,
        full: "gpd.GeoDataFrame",
        runtime: "gpd.GeoDataFrame",
        quarantine: "gpd.GeoDataFrame",
        reports: ReportStore,
    ) -> dict[str, Any]:
        source_count = len(full)
        runtime_count = len(runtime)
        quarantine_count = len(quarantine)
        def count_true(column: str) -> int:
            return int(full[column].fillna(False).astype(bool).sum()) if column in full.columns else 0
        zip_complete = float(full["Canonical_Zip_Code"].astype("string").ne("").mean()) if source_count else 0.0
        postal_complete = float(full["Canonical_Postal_City"].astype("string").ne("").mean()) if source_count else 0.0
        locality_complete = float(full["Canonical_Muni"].astype("string").ne("").mean()) if source_count else 0.0
        street_complete = float(full["Canonical_Full_Street"].astype("string").ne("").mean()) if source_count else 0.0
        nguid_complete = float(full["NGUID"].astype("string").fillna("").ne("").mean()) if source_count else 0.0
        latest_update = full["DateUpdate_Parsed"].dropna().max()
        metrics: dict[str, Any] = {
            "source_record_count": source_count,
            "runtime_record_count": runtime_count,
            "quarantine_record_count": quarantine_count,
            "nguid_completeness": nguid_complete,
            "nguid_unique": bool(full["NGUID"].astype("string").fillna("").is_unique),
            "geometry_completeness": float(full.geometry.notna().mean()) if source_count else 0.0,
            "bounds": tuple(float(v) for v in full.total_bounds) if source_count else None,
            "zero_house_number_count": int(full["Canonical_Full_House_Number"].astype("string").str.fullmatch(r"[A-Z]*0+(?:\.0+)?", na=False).sum()),
            "missing_street_count": int(full["Canonical_Full_Street"].astype("string").eq("").sum()),
            "unit_address_count": int(full["Canonical_Subaddress"].astype("string").ne("").sum()),
            "residential_unit_candidate_count": count_true("Canonical_Residential_Unit_Flag"),
            "commercial_unit_candidate_count": count_true("Canonical_Commercial_Unit_Flag"),
            "apartment_candidate_count": count_true("Canonical_Apartment_Candidate_Flag"),
            "parent_building_count": count_true("Potential_Parent_Record"),
            "potential_double_count_count": count_true("Potential_Double_Count_Flag"),
            "landmark_count": int(full["Canonical_Landmark_Name"].astype("string").ne("").sum()),
            "zip_completeness": zip_complete,
            "zip4_count": int(full["Canonical_ZIP4"].astype("string").ne("").sum()),
            "postal_city_completeness": postal_complete,
            "locality_fallback_count": count_true("Canonical_Postal_City_Fallback_Flag"),
            "municipality_locality_completeness": locality_complete,
            "street_completeness": street_complete,
            "invalid_date_count": int(full["Canonical_Date_Quality_Flags"].astype("string").ne("").sum()),
            "future_effective_count": int(full["Canonical_Active_Status"].eq("Future Effective").sum()),
            "expired_count": int(full["Canonical_Active_Status"].eq("Expired").sum()),
            "latest_plausible_dateupdate": latest_update.isoformat() if pd.notna(latest_update) else None,
            "unknown_classification_count": int(full["Canonical_Record_Role_Confidence"].isin(("Low", "Unclassified")).sum()),
        }
        self._populate_reports(county, full, quarantine, reports)
        return metrics

    def _populate_reports(
        self,
        county: str,
        full: "gpd.GeoDataFrame",
        quarantine: "gpd.GeoDataFrame",
        reports: ReportStore,
    ) -> None:
        total = len(full)
        for column in self.metadata.source_fields:
            if column not in full.columns:
                continue
            null_count = int(full[column].isna().sum())
            blank_count = int(clean_series(full, column).eq("").sum()) if full[column].dtype == object or str(full[column].dtype).startswith("string") else null_count
            reports.field_completeness.append({
                "county": county,
                "field": column,
                "record_count": total,
                "null_count": null_count,
                "blank_or_null_count": blank_count,
                "completeness_pct": ((total - blank_count) / total) if total else 0.0,
            })
        for source_field in ("Place_Type", "Placement", "Structure", "Unit_PreType", "Exception"):
            values = clean_series(full, source_field).replace("", "[BLANK]").value_counts(dropna=False).head(500)
            for value, count in values.items():
                reports.classification_values.append({
                    "county": county,
                    "source_field": source_field,
                    "value": value,
                    "count": int(count),
                    "pct": float(count / total) if total else 0.0,
                })
        for value, count in full["Canonical_Record_Role"].value_counts(dropna=False).items():
            reports.record_role_summary.append({"county": county, "record_role": value, "count": int(count), "pct": float(count / total) if total else 0.0})
        for value, count in full["Canonical_Occupancy_Category"].value_counts(dropna=False).items():
            reports.occupancy_summary.append({"county": county, "occupancy_category": value, "count": int(count), "pct": float(count / total) if total else 0.0})
        reports.parent_child_summary.append({
            "county": county,
            "potential_parent_records": int(full["Potential_Parent_Record"].sum()),
            "potential_child_records": int(full["Potential_Child_Record"].sum()),
            "potential_double_count_records": int(full["Potential_Double_Count_Flag"].sum()),
            "parent_groups": int(full.loc[full["Potential_Parent_Record"], "Parent_Group_Key"].nunique()),
        })
        date_anomaly = full[full["Canonical_Date_Quality_Flags"].astype("string").ne("")].head(MAX_DETAIL_REPORT_ROWS_PER_COUNTY)
        for row in date_anomaly[["NGUID", "DateUpdate_Raw", "Effective_Raw", "Expire_Raw", "Canonical_Date_Quality_Flags"]].to_dict("records"):
            reports.date_anomalies.append({"county": county, **row})
        coordinate_anomaly = full[full["Source_Coordinate_Difference_Meters"].gt(100)].nlargest(MAX_DETAIL_REPORT_ROWS_PER_COUNTY, "Source_Coordinate_Difference_Meters")
        for row in coordinate_anomaly[["NGUID", "Source_Lat", "Source_Long", "Canonical_Latitude", "Canonical_Longitude", "Source_Coordinate_Difference_Meters"]].to_dict("records"):
            reports.coordinate_anomalies.append({"county": county, **row})
        duplicate_counts = full.loc[full["Normalized_Address_Key"].astype("string").ne("")].groupby("Normalized_Address_Key").size()
        duplicate_counts = duplicate_counts[duplicate_counts.gt(1)].sort_values(ascending=False).head(MAX_DETAIL_REPORT_ROWS_PER_COUNTY)
        for address_key, count in duplicate_counts.items():
            reports.duplicate_address_summary.append({"county": county, "normalized_address_key": address_key, "record_count": int(count)})
        fallback_counts = full["Canonical_Locality_Source"].replace("", "[NONE]").value_counts()
        for source_name, count in fallback_counts.items():
            reports.postal_fallback_summary.append({"county": county, "locality_source": source_name, "count": int(count), "pct": float(count / total) if total else 0.0})
        if quarantine.empty:
            reports.quarantine_summary.append({"county": county, "quarantine_reason": "[NONE]", "count": 0})
        else:
            reason_counts: dict[str, int] = {}
            for text in quarantine["Quarantine_Reasons"].astype("string").fillna(""):
                for reason in [item.strip() for item in text.split("|") if item.strip()]:
                    reason_counts[reason] = reason_counts.get(reason, 0) + 1
            for reason, count in sorted(reason_counts.items()):
                reports.quarantine_summary.append({"county": county, "quarantine_reason": reason, "count": count})


def normalize_zip5(value: Any) -> str:
    text = normalize_whitespace(value)
    if not text:
        return ""
    text = re.sub(r"\.0+$", "", text)
    digits = re.sub(r"\D", "", text)
    if len(digits) == 4:
        digits = digits.zfill(5)
    return digits if len(digits) == 5 else ""


def normalize_zip4(value: Any) -> str:
    text = normalize_whitespace(value)
    if not text:
        return ""
    text = re.sub(r"\.0+$", "", text)
    digits = re.sub(r"\D", "", text)
    return digits.zfill(4) if 1 <= len(digits) <= 4 else ""


def normalize_key(value: Any) -> str:
    text = unicodedata.normalize("NFKD", normalize_whitespace(value).upper())
    text = "".join(character for character in text if not unicodedata.combining(character))
    return re.sub(r"[^A-Z0-9]+", "", text)


def haversine_meters(lon1: Any, lat1: Any, lon2: Any, lat2: Any) -> "np.ndarray":
    radius = 6_371_008.8
    lon1r = np.radians(np.asarray(lon1, dtype="float64"))
    lat1r = np.radians(np.asarray(lat1, dtype="float64"))
    lon2r = np.radians(np.asarray(lon2, dtype="float64"))
    lat2r = np.radians(np.asarray(lat2, dtype="float64"))
    dlon = lon2r - lon1r
    dlat = lat2r - lat1r
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1r) * np.cos(lat2r) * np.sin(dlon / 2.0) ** 2
    return radius * 2.0 * np.arcsin(np.minimum(1.0, np.sqrt(a)))


def _part1by1_16(values: "np.ndarray") -> "np.ndarray":
    values = values.astype("uint64") & np.uint64(0x0000FFFF)
    values = (values | (values << np.uint64(8))) & np.uint64(0x00FF00FF)
    values = (values | (values << np.uint64(4))) & np.uint64(0x0F0F0F0F)
    values = (values | (values << np.uint64(2))) & np.uint64(0x33333333)
    values = (values | (values << np.uint64(1))) & np.uint64(0x55555555)
    return values


def spatially_order(frame: "gpd.GeoDataFrame") -> "gpd.GeoDataFrame":
    """Apply deterministic Morton ordering to improve row-group locality."""
    if frame.empty:
        return frame.reset_index(drop=True)
    points = frame.geometry
    valid = points.notna() & ~points.is_empty & points.geom_type.eq("Point")
    order = pd.Series(np.iinfo(np.uint64).max, index=frame.index, dtype="uint64")
    if valid.any():
        x = points.loc[valid].x.to_numpy(dtype="float64")
        y = points.loc[valid].y.to_numpy(dtype="float64")
        minx, maxx = float(np.nanmin(x)), float(np.nanmax(x))
        miny, maxy = float(np.nanmin(y)), float(np.nanmax(y))
        xscale = 65535.0 / (maxx - minx) if maxx > minx else 0.0
        yscale = 65535.0 / (maxy - miny) if maxy > miny else 0.0
        xi = np.zeros(len(x), dtype="uint64") if xscale == 0 else np.clip(np.rint((x - minx) * xscale), 0, 65535).astype("uint64")
        yi = np.zeros(len(y), dtype="uint64") if yscale == 0 else np.clip(np.rint((y - miny) * yscale), 0, 65535).astype("uint64")
        morton = _part1by1_16(xi) | (_part1by1_16(yi) << np.uint64(1))
        order.loc[valid] = morton
    work = frame.copy()
    work["_Spatial_Order"] = order
    tie = clean_series(work, "NGUID") if "NGUID" in work.columns else work.index.astype("string")
    work["_Spatial_Tie"] = tie
    work = work.sort_values(["_Spatial_Order", "_Spatial_Tie"], kind="stable").drop(columns=["_Spatial_Order", "_Spatial_Tie"])
    return gpd.GeoDataFrame(work.reset_index(drop=True), geometry="geometry", crs=frame.crs)


def ensure_output_allowed(path: Path, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise PipelineError(f"Output exists; use --overwrite to replace: {path}")


def write_geoparquet_atomic(
    frame: "gpd.GeoDataFrame",
    path: Path,
    row_group_size: int,
    overwrite: bool,
    write_covering_bbox: bool = True,
) -> tuple[Path, bool]:
    ensure_directory(path.parent)
    ensure_output_allowed(path, overwrite)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp.parquet")
    if temporary.exists():
        temporary.unlink()
    covering_written = False
    try:
        frame.to_parquet(
            temporary,
            index=False,
            compression="zstd",
            geometry_encoding="WKB",
            write_covering_bbox=write_covering_bbox,
            schema_version="1.1.0",
            row_group_size=row_group_size,
            write_statistics=True,
        )
        covering_written = write_covering_bbox
    except (TypeError, ValueError, NotImplementedError) as exc:
        if not write_covering_bbox:
            raise
        LOGGER.warning("Covering-bbox GeoParquet write unavailable for %s: %s. Retrying without it.", path.name, exc)
        if temporary.exists():
            temporary.unlink()
        frame.to_parquet(
            temporary,
            index=False,
            compression="zstd",
            geometry_encoding="WKB",
            schema_version="1.0.0",
            row_group_size=row_group_size,
            write_statistics=True,
        )
    return temporary, covering_written


def parquet_null_counts(path: Path, columns: Sequence[str]) -> dict[str, int]:
    parquet_file = pq.ParquetFile(path)
    counts = {column: 0 for column in columns}
    available = set(parquet_file.schema_arrow.names)
    requested = [column for column in columns if column in available]
    for batch in parquet_file.iter_batches(columns=requested, batch_size=65_536):
        for index, column in enumerate(requested):
            counts[column] += int(batch.column(index).null_count)
    return counts


def geometry_map(frame: "gpd.GeoDataFrame") -> dict[str, str]:
    ids = clean_series(frame, "NGUID")
    result: dict[str, str] = {}
    for nguid, geometry in zip(ids, frame.geometry, strict=False):
        if not nguid:
            continue
        payload = b"" if geometry is None else geometry.wkb
        result[nguid] = hashlib.sha256(payload).hexdigest()
    return result


def validate_full_fidelity(
    source: "gpd.GeoDataFrame",
    parquet_path: Path,
    source_fields: Sequence[str],
) -> list[str]:
    messages: list[str] = []
    metadata = pq.ParquetFile(parquet_path).metadata
    if metadata.num_rows != len(source):
        raise ValidationError(f"Full-fidelity row count mismatch: {metadata.num_rows:,} vs {len(source):,}")
    schema_names = set(pq.ParquetFile(parquet_path).schema_arrow.names)
    missing_source_fields = [field for field in source_fields if field not in schema_names]
    if missing_source_fields:
        raise ValidationError("Full-fidelity output lost source fields: " + ", ".join(missing_source_fields))
    source_nulls = {field: int(source[field].isna().sum()) for field in source_fields}
    output_nulls = parquet_null_counts(parquet_path, source_fields)
    mismatches = [field for field in source_fields if source_nulls[field] != output_nulls.get(field, -1)]
    if mismatches:
        raise ValidationError("Full-fidelity null-count mismatch in fields: " + ", ".join(mismatches[:20]))
    output_identity = gpd.read_parquet(parquet_path, columns=["NGUID", "geometry"])
    source_ids = set(clean_series(source, "NGUID"))
    output_ids = set(clean_series(output_identity, "NGUID"))
    if source_ids != output_ids:
        raise ValidationError("Full-fidelity NGUID set does not match source")
    if geometry_map(source) != geometry_map(output_identity):
        raise ValidationError("Full-fidelity geometry hash map does not match source")
    if str(output_identity.crs).upper() != "EPSG:4326":
        raise ValidationError(f"Full-fidelity CRS mismatch: {output_identity.crs}")
    if len(source):
        if not np.allclose(source.total_bounds, output_identity.total_bounds, equal_nan=True, atol=1e-12):
            raise ValidationError("Full-fidelity total bounds changed")
    messages.append("Full-fidelity record, field, null, NGUID, CRS, bounds, and geometry checks passed")
    return messages


def validate_runtime(
    source: "gpd.GeoDataFrame",
    runtime: "gpd.GeoDataFrame",
    quarantine: "gpd.GeoDataFrame",
    runtime_path: Path,
    covering_bbox_written: bool,
) -> tuple[list[str], bool]:
    messages: list[str] = []
    if len(source) != len(runtime) + len(quarantine):
        raise ValidationError(
            f"Reconciliation failed: source {len(source):,} != runtime {len(runtime):,} + quarantine {len(quarantine):,}"
        )
    output = gpd.read_parquet(runtime_path)
    missing = [column for column in CURRENT_ANALYZER_COLUMNS if column not in output.columns]
    if missing:
        raise ValidationError("Runtime output is missing current Analyzer columns: " + ", ".join(missing))
    if len(output) != len(runtime):
        raise ValidationError("Runtime output row count mismatch")
    source_ids = set(clean_series(runtime, "NGUID"))
    output_ids = set(clean_series(output, "NGUID"))
    if source_ids != output_ids:
        raise ValidationError("Runtime NGUID set mismatch")
    record_ids = clean_series(output, "Source_Record_ID")
    if record_ids.eq("").any() or not record_ids.is_unique:
        raise ValidationError("Runtime Source_Record_ID is missing or non-unique")
    if str(output.crs).upper() != "EPSG:4326":
        raise ValidationError(f"Runtime CRS mismatch: {output.crs}")
    bbox_validated = False
    if covering_bbox_written and not runtime.empty:
        sample_point = runtime.geometry.iloc[len(runtime) // 2]
        epsilon = 0.005
        test_bbox = (
            float(sample_point.x - epsilon), float(sample_point.y - epsilon),
            float(sample_point.x + epsilon), float(sample_point.y + epsilon),
        )
        expected = int(runtime.geometry.covered_by(box(*test_bbox)).sum())
        subset = gpd.read_parquet(runtime_path, bbox=test_bbox)
        actual = len(subset)
        if expected != actual:
            raise ValidationError(f"Bounding-box read mismatch: expected {expected:,}, received {actual:,}")
        bbox_validated = True
        messages.append("GeoParquet covering-bbox read returned the expected point subset")
    elif covering_bbox_written:
        bbox_validated = True
        messages.append("Empty runtime file has covering-bbox metadata; no subset test required")
    else:
        messages.append("Covering-bbox metadata was not available; spatial pushdown is not claimed")
    messages.append("Runtime reconciliation, identity, required-column, and CRS checks passed")
    return messages, bbox_validated


def finalize_parquet(temporary: Path, destination: Path) -> None:
    os.replace(temporary, destination)


def file_record(path: Path, county: str, file_type: str) -> dict[str, Any]:
    return {
        "county": county,
        "file_type": file_type,
        "path": str(path),
        "filename": path.name,
        "byte_size": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def process_county(
    county: str,
    adapter: WisconsinStatewideNG911Adapter,
    config: PipelineConfig,
    reports: ReportStore,
) -> CountyOutput:
    source = adapter.read_county(county)
    expected_count = sum(1 for _ in range(len(source)))
    LOGGER.info("%s County source records: %s", county, f"{expected_count:,}")
    full, runtime, quarantine, metrics = adapter.standardize_county(county, source, reports)
    output_paths: dict[str, Path | None] = {"full_fidelity": None, "runtime": None, "quarantine": None}
    hashes: dict[str, str | None] = {"full_fidelity": None, "runtime": None, "quarantine": None}
    sizes: dict[str, int | None] = {"full_fidelity": None, "runtime": None, "quarantine": None}
    messages: list[str] = []
    covering_bbox_written = False
    bbox_validated = False

    full_destination = config.output_dir / "full_fidelity" / f"{county_slug(county)}.parquet"
    runtime_destination = config.output_dir / "runtime" / f"{county_slug(county)}.parquet"
    quarantine_destination = config.output_dir / "quarantine" / f"{county_slug(county)}_quarantine.parquet"

    planned_outputs: list[Path] = []
    if not config.runtime_only:
        planned_outputs.append(full_destination)
    if not config.full_fidelity_only:
        planned_outputs.append(runtime_destination)
    if not quarantine.empty:
        planned_outputs.append(quarantine_destination)
    for planned_output in planned_outputs:
        ensure_output_allowed(planned_output, config.overwrite)

    temporary_files: list[Path] = []
    try:
        if not config.runtime_only:
            full_temp, _ = write_geoparquet_atomic(full, full_destination, config.row_group_size, config.overwrite, True)
            temporary_files.append(full_temp)
            messages.extend(validate_full_fidelity(full, full_temp, adapter.metadata.source_fields))
            finalize_parquet(full_temp, full_destination)
            temporary_files.remove(full_temp)
            output_paths["full_fidelity"] = full_destination
            record = file_record(full_destination, county, "full_fidelity")
            reports.output_files.append(record)
            hashes["full_fidelity"] = record["sha256"]
            sizes["full_fidelity"] = record["byte_size"]

        if not config.full_fidelity_only:
            runtime_temp, covering_bbox_written = write_geoparquet_atomic(runtime, runtime_destination, config.row_group_size, config.overwrite, True)
            temporary_files.append(runtime_temp)
            runtime_messages, bbox_validated = validate_runtime(full, runtime, quarantine, runtime_temp, covering_bbox_written)
            messages.extend(runtime_messages)
            finalize_parquet(runtime_temp, runtime_destination)
            temporary_files.remove(runtime_temp)
            output_paths["runtime"] = runtime_destination
            record = file_record(runtime_destination, county, "runtime")
            reports.output_files.append(record)
            hashes["runtime"] = record["sha256"]
            sizes["runtime"] = record["byte_size"]

        if not quarantine.empty:
            quarantine_temp, _ = write_geoparquet_atomic(quarantine, quarantine_destination, config.row_group_size, config.overwrite, True)
            temporary_files.append(quarantine_temp)
            written_quarantine = gpd.read_parquet(quarantine_temp)
            if len(written_quarantine) != len(quarantine):
                raise ValidationError("Quarantine row count mismatch")
            finalize_parquet(quarantine_temp, quarantine_destination)
            temporary_files.remove(quarantine_temp)
            output_paths["quarantine"] = quarantine_destination
            record = file_record(quarantine_destination, county, "quarantine")
            reports.output_files.append(record)
            hashes["quarantine"] = record["sha256"]
            sizes["quarantine"] = record["byte_size"]

        parquet_path = output_paths["runtime"] or output_paths["full_fidelity"]
        row_groups = pq.ParquetFile(parquet_path).metadata.num_row_groups if parquet_path else 0
        metrics["row_group_count"] = row_groups
        metrics["row_group_size_setting"] = config.row_group_size
        metrics["covering_bbox_written"] = covering_bbox_written
        metrics["bbox_read_validated"] = bbox_validated
        metrics["full_fidelity_file_size"] = sizes["full_fidelity"]
        metrics["runtime_file_size"] = sizes["runtime"]
        metrics["quarantine_file_size"] = sizes["quarantine"]
        metrics["compression_ratio_runtime_to_source_gdb_estimate"] = (
            sizes["runtime"] / max(1, adapter.metadata.input_size_bytes)
            if sizes["runtime"] is not None else None
        )
        return CountyOutput(
            county=county,
            source_count=len(full),
            runtime_count=len(runtime),
            quarantine_count=len(quarantine),
            full_fidelity_path=str(output_paths["full_fidelity"]) if output_paths["full_fidelity"] else None,
            runtime_path=str(output_paths["runtime"]) if output_paths["runtime"] else None,
            quarantine_path=str(output_paths["quarantine"]) if output_paths["quarantine"] else None,
            full_fidelity_sha256=hashes["full_fidelity"],
            runtime_sha256=hashes["runtime"],
            quarantine_sha256=hashes["quarantine"],
            full_fidelity_size=sizes["full_fidelity"],
            runtime_size=sizes["runtime"],
            quarantine_size=sizes["quarantine"],
            covering_bbox_written=covering_bbox_written,
            bbox_read_validated=bbox_validated,
            validation_passed=True,
            validation_messages=messages,
            metrics=metrics,
        )
    finally:
        for temporary in temporary_files:
            temporary.unlink(missing_ok=True)
        del source, full, runtime, quarantine
        gc.collect()


def derive_readiness(county: str, output: CountyOutput | None, represented: bool) -> dict[str, Any]:
    if not represented:
        status, reason = STATUS_OVERRIDES.get(county, ("unavailable", "County is absent from source"))
        return {
            "spatial_readiness": False,
            "address_readiness": False,
            "postal_readiness": False,
            "occupancy_classification_readiness": False,
            "public_availability_status": status,
            "status_reason": reason,
        }
    if output is None or not output.validation_passed:
        return {
            "spatial_readiness": False,
            "address_readiness": False,
            "postal_readiness": False,
            "occupancy_classification_readiness": False,
            "public_availability_status": "failed_validation",
            "status_reason": "County processing or critical validation failed",
        }
    metrics = output.metrics
    spatial = bool(output.runtime_path) and output.quarantine_count == 0 and output.bbox_read_validated
    address = metrics.get("street_completeness", 0) >= 0.99 and metrics.get("nguid_completeness", 0) == 1.0
    postal = metrics.get("zip_completeness", 0) >= 0.90 and (
        metrics.get("postal_city_completeness", 0) >= 0.90
        or metrics.get("municipality_locality_completeness", 0) >= 0.98
    )
    occupancy = metrics.get("unknown_classification_count", 0) / max(1, output.source_count) <= 0.20
    override = STATUS_OVERRIDES.get(county)
    if not output.runtime_path:
        status, reason = "needs_validation", "Full-fidelity output exists, but no runtime file was generated for publication"
    elif override:
        status, reason = override
    else:
        status, reason = "needs_validation", "Files passed technical validation but county completeness has not been independently verified"
    return {
        "spatial_readiness": spatial,
        "address_readiness": address,
        "postal_readiness": postal,
        "occupancy_classification_readiness": occupancy,
        "public_availability_status": status,
        "status_reason": reason,
    }


def build_manifest(
    metadata: SourceMetadata,
    config: PipelineConfig,
    county_variants: Mapping[str, Sequence[str]],
    outcomes: Mapping[str, CountyOutput],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for county in WI_COUNTIES:
        represented = bool(county_variants.get(county))
        output = outcomes.get(county)
        readiness = derive_readiness(county, output, represented)
        row: dict[str, Any] = {
            "canonical_county": county,
            "source_system": SOURCE_SYSTEM,
            "source_version": metadata.source_timestamp[:10] if metadata.source_timestamp else config.as_of_date.isoformat(),
            "source_hash": metadata.source_sha256,
            "source_record_count": output.source_count if output else 0,
            "runtime_record_count": output.runtime_count if output else 0,
            "quarantine_record_count": output.quarantine_count if output else 0,
            "validation_date": date.today().isoformat(),
            **readiness,
            "county_override_status": readiness["public_availability_status"] == "county_override",
            "runtime_filename": Path(output.runtime_path).name if output and output.runtime_path else None,
            "runtime_byte_size": output.runtime_size if output else None,
            "runtime_sha256": output.runtime_sha256 if output else None,
            "full_fidelity_filename": Path(output.full_fidelity_path).name if output and output.full_fidelity_path else None,
            "full_fidelity_sha256": output.full_fidelity_sha256 if output else None,
            "placeholder_r2_url": None,
            "latest_plausible_source_update_date": output.metrics.get("latest_plausible_dateupdate") if output else None,
            "quality_score_summary": quality_score(output.metrics) if output else None,
            "covering_bbox_written": output.covering_bbox_written if output else False,
            "bbox_read_validated": output.bbox_read_validated if output else False,
            "validation_passed": output.validation_passed if output else False,
        }
        rows.append(row)
    return rows


def quality_score(metrics: Mapping[str, Any]) -> float:
    components = [
        float(metrics.get("nguid_completeness", 0)),
        float(metrics.get("geometry_completeness", 0)),
        float(metrics.get("street_completeness", 0)),
        float(metrics.get("municipality_locality_completeness", 0)),
        float(metrics.get("zip_completeness", 0)),
    ]
    return round(sum(components) / len(components) * 100, 2)


def compare_previous_manifest(current: list[dict[str, Any]], previous_path: Path) -> list[dict[str, Any]]:
    if not previous_path.exists():
        raise PipelineError(f"Previous manifest not found: {previous_path}")
    previous_payload = json.loads(previous_path.read_text(encoding="utf-8"))
    previous_rows = previous_payload.get("counties", previous_payload) if isinstance(previous_payload, dict) else previous_payload
    previous_lookup = {row["canonical_county"]: row for row in previous_rows}
    warnings: list[dict[str, Any]] = []
    for row in current:
        old = previous_lookup.get(row["canonical_county"])
        if not old:
            continue
        for field_name in ("source_record_count", "runtime_record_count", "quarantine_record_count"):
            old_value = int(old.get(field_name) or 0)
            new_value = int(row.get(field_name) or 0)
            if old_value and abs(new_value - old_value) / old_value > 0.10:
                warnings.append({
                    "county": row["canonical_county"],
                    "field": field_name,
                    "previous": old_value,
                    "current": new_value,
                    "change_pct": round((new_value - old_value) / old_value * 100, 2),
                    "warning": "Change exceeds 10%; review before publication",
                })
        old_date = old.get("latest_plausible_source_update_date")
        new_date = row.get("latest_plausible_source_update_date")
        if old_date and new_date and str(new_date) < str(old_date):
            warnings.append({
                "county": row["canonical_county"],
                "field": "latest_plausible_source_update_date",
                "previous": old_date,
                "current": new_date,
                "warning": "Current source appears older than previous release",
            })
    return warnings


def write_reports(
    output_dir: Path,
    reports: ReportStore,
    metadata: SourceMetadata,
    config: PipelineConfig,
    manifest_rows: list[dict[str, Any]],
    outcomes: Mapping[str, CountyOutput],
    previous_warnings: list[dict[str, Any]],
) -> None:
    reports_dir = ensure_directory(output_dir / "reports")
    report_map = {
        "county_summary.csv": reports.county_summary,
        "county_name_variants.csv": reports.county_name_variants,
        "field_completeness.csv": reports.field_completeness,
        "classification_values.csv": reports.classification_values,
        "record_role_summary.csv": reports.record_role_summary,
        "occupancy_summary.csv": reports.occupancy_summary,
        "parent_child_summary.csv": reports.parent_child_summary,
        "date_anomalies.csv": reports.date_anomalies,
        "coordinate_anomalies.csv": reports.coordinate_anomalies,
        "duplicate_address_summary.csv": reports.duplicate_address_summary,
        "duplicate_nguid_report.csv": reports.duplicate_nguid_report,
        "postal_fallback_summary.csv": reports.postal_fallback_summary,
        "quarantine_summary.csv": reports.quarantine_summary,
        "output_files.csv": reports.output_files,
        "previous_release_warnings.csv": previous_warnings,
        "failures.csv": reports.failures,
    }
    for filename, rows in report_map.items():
        atomic_write_csv(reports_dir / filename, rows)
    run_summary = {
        "processing_timestamp_utc": utc_now_iso(),
        "source": asdict(metadata),
        "as_of_date": config.as_of_date.isoformat(),
        "as_of_date_source": config.as_of_date_source,
        "requested_counties": config.counties,
        "processed_counties": list(outcomes),
        "successful_counties": [county for county, output in outcomes.items() if output.validation_passed],
        "failures": reports.failures,
        "non_destructive_policy": {
            "address_deduplication": False,
            "coordinate_deduplication": False,
            "parent_record_deletion": False,
            "global_place_type_exclusion": False,
            "source_lat_long_used_for_geometry": False,
        },
    }
    atomic_write_json(reports_dir / "run_summary.json", run_summary)
    manifest_dir = ensure_directory(output_dir / "manifest")
    atomic_write_json(manifest_dir / "coverage_manifest.json", {
        "generated_at_utc": utc_now_iso(),
        "source_hash": metadata.source_sha256,
        "source_version": metadata.source_timestamp[:10] if metadata.source_timestamp else config.as_of_date.isoformat(),
        "counties": manifest_rows,
    })
    atomic_write_csv(manifest_dir / "coverage_manifest.csv", manifest_rows)

    compatibility_notes = {
        "current_analyzer_compatible_columns_included": list(CURRENT_ANALYZER_COLUMNS),
        "targeted_analyzer_changes_still_recommended": [
            "Load county GeoParquet using gpd.read_parquet(..., bbox=...) rather than gpd.read_file.",
            "Prefer Canonical_Full_House_Number over recombining Canonical_HouseNo and Canonical_HouseSx.",
            "Prefer Canonical_Full_Street so St_PreTyp, St_PreMod, St_PreSep, and St_PosMod are not lost.",
            "Prefer Canonical_Subaddress and do not prefix every untyped unit with Apt.",
            "Use Canonical_ZIP4 rather than deriving ZIP4 only from Canonical_Zip_Code.",
            "Use Canonical_Record_Role, Potential_Parent_Record, and occupancy flags to prevent parent-building double counting.",
            "Use Canonical_Apartment_Candidate_Flag rather than grouping every repeated nonblank unit as an apartment.",
            "Use Canonical_Eligibility_Status and Canonical_Quality_Flags in the Excluded Audit instead of preprocessing questionable rows away.",
            "Keep Milwaukee on a county-specific adapter until the City of Milwaukee statewide coverage gap is resolved.",
        ],
    }
    atomic_write_json(output_dir / "source_metadata" / "analyzer_compatibility_notes.json", compatibility_notes)


def add_county_summary_row(reports: ReportStore, output: CountyOutput) -> None:
    metrics = output.metrics
    row = {
        "county": output.county,
        **metrics,
        "full_fidelity_sha256": output.full_fidelity_sha256,
        "runtime_sha256": output.runtime_sha256,
        "quarantine_sha256": output.quarantine_sha256,
        "validation_passed": output.validation_passed,
        "validation_messages": " | ".join(output.validation_messages),
    }
    reports.county_summary.append(row)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        require_dependencies()
        input_path = args.input.expanduser().resolve()
        output_dir = args.output.expanduser().resolve()
        ensure_directory(output_dir)
        setup_logging(output_dir, args.log_level)
        if not input_path.exists():
            raise PipelineError(f"Input does not exist: {input_path}")
        as_of_date, as_of_source = derive_as_of_date(input_path, args.as_of_date)
        requested_counties = validate_requested_counties(args.counties)
        config = PipelineConfig(
            input_path=input_path,
            output_dir=output_dir,
            as_of_date=as_of_date,
            as_of_date_source=as_of_source,
            counties=requested_counties,
            overwrite=args.overwrite,
            runtime_only=args.runtime_only,
            full_fidelity_only=args.full_fidelity_only,
            keep_extracted_gdb=args.keep_extracted_gdb,
            row_group_size=args.row_group_size,
            previous_manifest=args.previous_manifest.expanduser().resolve() if args.previous_manifest else None,
            fail_fast=args.fail_fast,
        )
        LOGGER.info("Starting Wisconsin NG911 pipeline")
        LOGGER.info("As-of date: %s (%s)", config.as_of_date, config.as_of_date_source)
        gdb_path, temporary_owner, input_kind = extract_or_locate_gdb(input_path, output_dir, config.keep_extracted_gdb)
        try:
            metadata = inspect_source(input_path, gdb_path, input_kind)
            write_source_metadata(output_dir, metadata)
            LOGGER.info("Source layer %s: %s Point records, CRS %s", metadata.layer_name, f"{metadata.feature_count:,}", metadata.crs)
            reports = ReportStore()
            inventory, variants, duplicate_nguids = read_inventory(gdb_path, metadata.feature_count, reports)
            del inventory
            gc.collect()
            adapter = WisconsinStatewideNG911Adapter(gdb_path, metadata, config, variants, duplicate_nguids)
            counties_to_process = config.counties or tuple(county for county in WI_COUNTIES if variants[county])
            outcomes: dict[str, CountyOutput] = {}
            for county in counties_to_process:
                if not variants[county]:
                    LOGGER.warning("Skipping %s: absent from source", county)
                    continue
                try:
                    output = process_county(county, adapter, config, reports)
                    outcomes[county] = output
                    add_county_summary_row(reports, output)
                    LOGGER.info(
                        "%s complete: source=%s runtime=%s quarantine=%s",
                        county,
                        f"{output.source_count:,}",
                        f"{output.runtime_count:,}",
                        f"{output.quarantine_count:,}",
                    )
                except Exception as exc:  # county-isolated failure by design
                    LOGGER.exception("%s County failed: %s", county, exc)
                    reports.failures.append({
                        "county": county,
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                        "timestamp_utc": utc_now_iso(),
                    })
                    if config.fail_fast:
                        raise
            manifest_rows = build_manifest(metadata, config, variants, outcomes)
            previous_warnings = compare_previous_manifest(manifest_rows, config.previous_manifest) if config.previous_manifest else []
            write_reports(output_dir, reports, metadata, config, manifest_rows, outcomes, previous_warnings)
            failed = bool(reports.failures)
            LOGGER.info(
                "Pipeline complete: %s successful county file set(s), %s failure(s)",
                len(outcomes), len(reports.failures),
            )
            return 2 if failed else 0
        finally:
            if temporary_owner is not None:
                temporary_owner.cleanup()
    except Exception as exc:
        if LOGGER.handlers:
            LOGGER.exception("Pipeline failed: %s", exc)
        else:
            print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
