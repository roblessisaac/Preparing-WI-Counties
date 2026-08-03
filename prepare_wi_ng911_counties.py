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
import time
import traceback
import unicodedata
import zipfile
from dataclasses import asdict, dataclass, field, replace
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

PIPELINE_VERSION = "2.1.0"
RUNTIME_SCHEMA_VERSION = "2.1.0"
FULL_FIDELITY_SCHEMA_VERSION = "1.1.0"
MANIFEST_SCHEMA_VERSION = "2.1.0"
CLASSIFICATION_RULE_VERSION = "2.1.0"

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

COUNTY_STATUS_OVERRIDES: dict[str, dict[str, Any]] = {
    "Waukesha": {
        "coverage_readiness_status": "validated",
        "production_source_status": "statewide_runtime",
        "public_availability_status": "validated",
        "reason": "Statewide records were previously reconciled against the Waukesha County source.",
        "recommended_source": "Wisconsin statewide NG911 runtime",
    },
    "Milwaukee": {
        "coverage_readiness_status": "incomplete_coverage",
        "production_source_status": "county_override",
        "public_availability_status": "county_override",
        "reason": (
            "The statewide layer is missing nearly all City of Milwaukee addresses; "
            "use the county-specific source."
        ),
        "recommended_source": "Milwaukee County-specific dataset",
    },
    "Crawford": {
        "coverage_readiness_status": "incomplete_coverage",
        "production_source_status": "incomplete_coverage",
        "public_availability_status": "incomplete_coverage",
        "reason": "The statewide source snapshot contains only one Crawford County record.",
        "recommended_source": None,
    },
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


def _runtime_field(
    field_name: str,
    logical_type: str,
    nullable: bool,
    source_or_derivation: str,
    description: str,
    analyzer_use: str,
    example: str = "",
) -> dict[str, Any]:
    return {
        "field_name": field_name,
        "logical_type": logical_type,
        "nullable": nullable,
        "source_or_derivation": source_or_derivation,
        "description": description,
        "analyzer_use": analyzer_use,
        "example": example,
    }


RUNTIME_SCHEMA: tuple[dict[str, Any], ...] = (
    _runtime_field("Source_System", "string", False, "constant", "Authoritative source system.", "provenance", SOURCE_SYSTEM),
    _runtime_field("Source_Version", "string", False, "source archive timestamp", "Source release or as-of date.", "provenance", "2026-07-27"),
    _runtime_field("Source_County", "string", False, "County", "Raw county value from the source.", "county audit", "Waukesha County"),
    _runtime_field("Canonical_County", "string", False, "normalized County", "Canonical Wisconsin county name.", "county selection", "Waukesha"),
    _runtime_field("Canonical_Native_Source_ID", "string", False, "NGUID or FID fallback", "Best stable native identifier.", "record identity"),
    _runtime_field("Source_Record_ID", "string", False, "county + native ID/FID", "Globally unique pipeline record identifier.", "deduplication-safe identity"),
    _runtime_field("NGUID", "string", True, "NGUID", "Original NG911 globally unique identifier.", "audit"),
    _runtime_field("Source_Classification_Value", "string", True, "Place_Type/Placement/Structure/Exception", "Compact source classification trace.", "filter audit"),
    _runtime_field("Canonical_HouseNoPrefix", "string", True, "AddNum_Pre", "House-number prefix, including Wisconsin grid prefixes.", "address construction", "W399S"),
    _runtime_field("Canonical_HouseNo", "string", True, "AddNum_Pre + Add_Number", "Analyzer-compatible primary house number.", "current Analyzer", "W399S10950"),
    _runtime_field("Canonical_HouseSx", "string", True, "AddNum_Suf", "House-number suffix.", "address construction", "A"),
    _runtime_field("Canonical_Full_House_Number", "string", True, "derived", "Complete house number including prefix and suffix.", "preferred address construction", "W399S10950"),
    _runtime_field("Canonical_Street_PreModifier", "string", True, "St_PreMod", "Street pre-modifier.", "full street"),
    _runtime_field("Canonical_Dir", "string", True, "St_PreDir", "Street prefix direction.", "current Analyzer", "N"),
    _runtime_field("Canonical_Street_PreType", "string", True, "St_PreTyp", "Street pre-type such as County Highway.", "full street", "County Highway"),
    _runtime_field("Canonical_Street", "string", True, "St_Name", "Primary street name.", "current Analyzer", "Main"),
    _runtime_field("Canonical_StType", "string", True, "St_PosTyp", "Street type.", "current Analyzer", "ST"),
    _runtime_field("Canonical_SuffixDir", "string", True, "St_PosDir", "Street suffix direction.", "current Analyzer", "E"),
    _runtime_field("Canonical_Street_PostModifier", "string", True, "St_PosMod", "Street post-modifier.", "full street"),
    _runtime_field("Canonical_Full_Street", "string", True, "FullStNm or assembled components", "Complete physical street name without lost modifiers.", "preferred address construction"),
    _runtime_field("Canonical_Abbreviated_Street", "string", True, "abFullStNm or full street", "Source-provided abbreviated street when available.", "mail export"),
    _runtime_field("Canonical_Full_Address", "string", True, "derived physical address", "Base physical address without subaddress.", "current Analyzer"),
    _runtime_field("Canonical_Mailable_Address", "string", True, "derived", "Mailing address including subaddress and postal line.", "Excel/NWS export"),
    _runtime_field("Canonical_UnitType", "string", True, "Unit_PreType", "Unit or subaddress type.", "unit handling", "APT"),
    _runtime_field("Canonical_Unit", "string", True, "Unit_Value", "Unit identifier.", "current Analyzer", "12"),
    _runtime_field("Canonical_Building", "string", True, "Building", "Building identifier.", "subaddress audit"),
    _runtime_field("Canonical_Floor", "string", True, "Floor", "Floor identifier.", "subaddress audit"),
    _runtime_field("Canonical_Room", "string", True, "Room", "Room identifier.", "subaddress audit"),
    _runtime_field("Canonical_Subaddress", "string", True, "derived", "Complete building/floor/unit/room representation.", "preferred unit handling"),
    _runtime_field("Canonical_Muni", "string", True, "postal/locality hierarchy", "Best operational municipality or locality.", "current Analyzer"),
    _runtime_field("Canonical_Postal_City", "string", True, "Post_Comm", "Postal city, kept distinct from municipality.", "mail export"),
    _runtime_field("Canonical_State", "string", False, "State or WI fallback", "Two-letter state abbreviation.", "mail export", "WI"),
    _runtime_field("Canonical_Zip_Code", "string", True, "Post_Code", "Normalized five-digit ZIP.", "current Analyzer", "53186"),
    _runtime_field("Canonical_ZIP4", "string", True, "Post_Code4", "Normalized ZIP+4 extension.", "mail export", "1234"),
    _runtime_field("Canonical_Full_ZIP", "string", True, "derived", "ZIP or ZIP+4.", "mail export", "53186-1234"),
    _runtime_field("Canonical_Postal_City_Fallback_Flag", "boolean", False, "derived", "True when municipality/locality substituted for postal city.", "quality audit"),
    _runtime_field("Canonical_Postal_Quality_Status", "string", False, "derived", "Complete, Partial, or Missing postal quality.", "quality audit"),
    _runtime_field("Canonical_Status", "string", False, "canonical record role", "Analyzer-compatible status label.", "current Analyzer"),
    _runtime_field("Canonical_Record_Role", "string", False, "derived", "Operational role such as Standalone Address or Building Parent.", "filtering"),
    _runtime_field("Canonical_Occupancy_Category", "string", False, "derived", "Conservative residential/commercial/unknown occupancy class.", "apartment handling"),
    _runtime_field("Canonical_Occupancy_Confidence", "string", False, "derived", "Confidence in occupancy classification.", "manual review"),
    _runtime_field("Canonical_Occupancy_Reason", "string", False, "derived rule code", "Machine-readable evidence for occupancy classification.", "audit"),
    _runtime_field("Potential_Parent_Record", "boolean", False, "derived address grouping", "Possible parent-building row.", "double-count prevention"),
    _runtime_field("Potential_Child_Record", "boolean", False, "derived address grouping", "Possible child-unit row.", "double-count prevention"),
    _runtime_field("Parent_Group_Key", "string", True, "normalized building address", "Deterministic inferred grouping key.", "parent-child audit"),
    _runtime_field("Child_Record_Count", "integer", False, "derived", "Number of subaddress rows sharing the building key.", "parent-child audit"),
    _runtime_field("Potential_Parent_Count", "integer", False, "derived", "Number of possible blank-subaddress parents for the group.", "ambiguity audit"),
    _runtime_field("Potential_Double_Count_Flag", "boolean", False, "derived", "Parent record may overlap child units.", "double-count prevention"),
    _runtime_field("Canonical_Analyzer_Eligible", "boolean", False, "derived", "Whether the record can participate in normal Analyzer processing.", "runtime filter"),
    _runtime_field("Canonical_Analyzer_Handling", "string", False, "derived", "Machine-readable Analyzer action.", "runtime filter", "include_standard"),
    _runtime_field("Canonical_Exclusion_Category", "string", False, "derived", "Default exclusion or review category.", "advanced exclusions", "none"),
    _runtime_field("Canonical_Address_Quality_Status", "string", False, "derived", "Usable or Review address status.", "quality audit"),
    _runtime_field("Canonical_Geometry_Quality_Status", "string", False, "derived", "Usable or Quarantine geometry status.", "spatial validation"),
    _runtime_field("Canonical_Classification_Quality_Status", "string", False, "derived", "Usable or Review classification status.", "quality audit"),
    _runtime_field("Canonical_Quality_Flags", "string", True, "derived", "Pipe-delimited non-destructive QA flags.", "Excluded Audit"),
    _runtime_field("Canonical_Critical_Failure_Flag", "boolean", False, "derived", "True only for critical technical failures.", "quarantine reconciliation"),
    _runtime_field("Quarantine_Reasons", "string", True, "derived", "Critical technical failure reasons; blank in runtime.", "reconciliation"),
    _runtime_field("Canonical_Latitude", "float", False, "geometry", "Latitude in EPSG:4326.", "NWS export"),
    _runtime_field("Canonical_Longitude", "float", False, "geometry", "Longitude in EPSG:4326.", "NWS export"),
    _runtime_field("geometry", "geometry", False, "source geometry", "Point geometry in EPSG:4326.", "spatial assignment"),
)

RUNTIME_COLUMNS: tuple[str, ...] = tuple(item["field_name"] for item in RUNTIME_SCHEMA)
RUNTIME_SCHEMA_BY_FIELD: dict[str, dict[str, Any]] = {item["field_name"]: item for item in RUNTIME_SCHEMA}

RUNTIME_DTYPES: dict[str, str] = {
    **{name: "string" for name in RUNTIME_COLUMNS if name != "geometry"},
    "Canonical_Postal_City_Fallback_Flag": "boolean",
    "Potential_Parent_Record": "boolean",
    "Potential_Child_Record": "boolean",
    "Potential_Double_Count_Flag": "boolean",
    "Canonical_Analyzer_Eligible": "boolean",
    "Canonical_Critical_Failure_Flag": "boolean",
    "Child_Record_Count": "Int64",
    "Potential_Parent_Count": "Int64",
    "Canonical_Latitude": "float64",
    "Canonical_Longitude": "float64",
}

STATUS_DEFINITIONS: tuple[dict[str, Any], ...] = (
    {"value": "validated", "definition": "Technical and known coverage validation passed.", "default_analyzer_behavior": "available", "manual_review_recommended": False},
    {"value": "validated_with_warnings", "definition": "Technical validation passed with non-critical warnings.", "default_analyzer_behavior": "available_with_warning", "manual_review_recommended": True},
    {"value": "not_processed", "definition": "Current-run state: the county was not processed in this invocation. This does not replace durable coverage or production-source knowledge.", "default_analyzer_behavior": "unavailable", "manual_review_recommended": False},
    {"value": "needs_validation", "definition": "Durable coverage state: technically represented, but county coverage has not been independently approved.", "default_analyzer_behavior": "not_public", "manual_review_recommended": True},
    {"value": "failed_validation", "definition": "Current-run state: selected county failed processing or critical validation.", "default_analyzer_behavior": "unavailable", "manual_review_recommended": True},
    {"value": "incomplete_coverage", "definition": "Durable coverage or production state: countywide coverage is known incomplete.", "default_analyzer_behavior": "unavailable", "manual_review_recommended": True},
    {"value": "county_override", "definition": "Durable production-source state: use a separate county-specific source.", "default_analyzer_behavior": "use_override", "manual_review_recommended": False},
    {"value": "unavailable", "definition": "No approved production source is available.", "default_analyzer_behavior": "unavailable", "manual_review_recommended": False},
    {"value": "not_present_in_source", "definition": "County is absent from the statewide source snapshot.", "default_analyzer_behavior": "unavailable", "manual_review_recommended": False},
)

CLASSIFICATION_DEFINITIONS: tuple[dict[str, Any], ...] = (
    {"value": "Standalone Address", "definition": "Address without a populated subaddress or stronger specialized source role.", "default_analyzer_behavior": "include_standard", "confidence_meaning": "Medium unless stronger source evidence exists.", "manual_review_recommended": False},
    {"value": "Residential Unit", "definition": "Priority 1: explicit residential apartment, condominium, duplex-side, or supported residential unit evidence.", "default_analyzer_behavior": "include_unit", "confidence_meaning": "Usually High.", "manual_review_recommended": False},
    {"value": "Commercial Unit", "definition": "Priority 1: explicit suite or office unit evidence.", "default_analyzer_behavior": "include_unit", "confidence_meaning": "Usually High.", "manual_review_recommended": False},
    {"value": "Parcel or Site", "definition": "Priority 2: explicit parcel or site source evidence, including records that also contain a generic subaddress.", "default_analyzer_behavior": "exclude_default", "confidence_meaning": "Medium.", "manual_review_recommended": True},
    {"value": "Utility or Infrastructure", "definition": "Priority 2: explicit utility or infrastructure source evidence, including records that also contain a generic subaddress.", "default_analyzer_behavior": "exclude_default", "confidence_meaning": "Medium.", "manual_review_recommended": True},
    {"value": "Other Non-Mailable Site", "definition": "Priority 2: access, entrance, gate, driveway, or other explicit non-mailing source evidence.", "default_analyzer_behavior": "exclude_default", "confidence_meaning": "Medium.", "manual_review_recommended": True},
    {"value": "Unknown Unit Address", "definition": "Priority 3: a subaddress exists without explicit residential, commercial, parcel, utility, access, infrastructure, or other non-mailable evidence.", "default_analyzer_behavior": "manual_review", "confidence_meaning": "Low by design.", "manual_review_recommended": True},
    {"value": "Building Parent", "definition": "Blank-subaddress row sharing a building key with child-unit rows; parent-child logic is applied after source-role classification.", "default_analyzer_behavior": "include_parent_for_review", "confidence_meaning": "Medium or High depending on child count.", "manual_review_recommended": True},
)

REPORT_SCHEMAS: dict[str, tuple[str, ...]] = {
    "county_summary.csv": (
        "county", "county_key", "statewide_inventory_count", "processed_source_count",
        "runtime_count", "quarantine_count", "reconciliation_passed",
        "runtime_column_count", "full_fidelity_column_count",
        "runtime_file_size_bytes", "full_fidelity_file_size_bytes",
        "runtime_size_reduction_bytes", "runtime_size_reduction_pct",
        "geometry_completeness", "nguid_completeness", "missing_nguid_count",
        "duplicate_nguid_record_count", "duplicate_source_record_id_count",
        "street_completeness", "postal_city_completeness", "zip_completeness",
        "municipality_locality_completeness", "parent_count", "child_count",
        "unknown_unit_count", "warning_count", "technical_warning_count",
        "classification_conflict_count", "subaddress_with_parcel_or_site_count",
        "subaddress_with_utility_or_infrastructure_count",
        "subaddress_with_access_or_non_mailable_count", "row_group_count", "covering_bbox_written", "bbox_read_validated",
        "validation_status", "coverage_readiness_status", "production_source_status",
        "readiness_reason", "elapsed_processing_seconds",
        "latest_plausible_source_update_date", "full_fidelity_sha256",
        "runtime_sha256", "quarantine_sha256", "validation_messages",
    ),
    "county_name_variants.csv": (
        "canonical_county", "raw_variants", "statewide_inventory_count", "represented",
        "initial_coverage_status", "status_reason",
    ),
    "field_completeness.csv": ("county", "field", "record_count", "null_count", "blank_or_null_count", "completeness_pct"),
    "classification_values.csv": ("county", "source_field", "value", "count", "pct"),
    "record_role_summary.csv": ("county", "record_role", "count", "pct"),
    "occupancy_summary.csv": ("county", "occupancy_category", "occupancy_confidence", "occupancy_reason", "count", "pct"),
    "parent_child_summary.csv": (
        "county", "potential_parent_records", "potential_child_records",
        "source_parent_candidates", "parent_records_zero_matching_children",
        "parent_records_one_matching_child", "parent_records_multiple_matching_children",
        "children_one_possible_parent", "children_multiple_possible_parents",
        "potential_double_count_records", "conflicting_parent_child_records",
        "parent_groups",
    ),
    "date_anomalies.csv": ("county", "Source_Record_ID", "NGUID", "DateUpdate_Raw", "Effective_Raw", "Expire_Raw", "Canonical_Date_Quality_Flags"),
    "coordinate_anomalies.csv": ("county", "Source_Record_ID", "NGUID", "Source_Lat", "Source_Long", "Canonical_Latitude", "Canonical_Longitude", "Source_Coordinate_Difference_Meters", "anomaly_reason"),
    "duplicate_address_summary.csv": ("county", "normalized_address_key", "record_count"),
    "duplicate_nguid_report.csv": ("canonical_county", "source_county", "NGUID", "duplicate_record_count", "source_fids"),
    "postal_fallback_summary.csv": ("county", "locality_source", "count", "pct"),
    "quarantine_summary.csv": ("county", "quarantine_reason", "count"),
    "classification_conflicts.csv": (
        "county", "source_record_id", "nguid", "source_place_type",
        "source_placement", "source_structure", "source_exception",
        "unit_type", "unit_value", "subaddress", "selected_record_role",
        "classification_reason", "analyzer_handling", "exclusion_category",
        "conflict_type",
    ),
    "output_files.csv": (
        "county", "output_type", "relative_path", "file_size_bytes", "sha256",
        "row_count", "column_count", "crs", "created_timestamp_utc",
        "schema_version", "classification_rule_version", "row_group_count",
    ),
    "previous_release_warnings.csv": ("county", "field", "previous", "current", "change_pct", "warning"),
    "failures.csv": ("county", "processing_stage", "exception_type", "exception_message", "timestamp_utc", "traceback_reference"),
}

MANIFEST_COLUMNS: tuple[str, ...] = (
    "canonical_county", "county_key", "source_system", "source_version", "source_hash",
    "statewide_inventory_count", "processed_source_count", "runtime_count",
    "quarantine_count", "requested_in_run", "processed_in_run", "skipped_by_resume",
    "technical_validation_status", "coverage_readiness_status",
    "production_source_status", "public_availability_status", "status_reason",
    "run_processing_status", "run_processing_reason", "recommended_source",
    "spatial_readiness", "address_readiness",
    "postal_readiness", "occupancy_classification_readiness", "validation_date",
    "runtime_relative_path", "runtime_byte_size", "runtime_sha256",
    "full_fidelity_relative_path", "full_fidelity_byte_size", "full_fidelity_sha256",
    "quarantine_relative_path", "quarantine_byte_size", "quarantine_sha256",
    "latest_plausible_source_update_date", "quality_score_summary",
    "classification_conflict_count", "subaddress_with_parcel_or_site_count",
    "subaddress_with_utility_or_infrastructure_count",
    "subaddress_with_access_or_non_mailable_count",
    "covering_bbox_written", "bbox_read_validated", "validation_passed",
    "pipeline_version", "runtime_schema_version", "full_fidelity_schema_version",
    "manifest_schema_version", "classification_rule_version",
)


@dataclass(slots=True)
class PipelineConfig:
    input_path: Path
    output_dir: Path
    as_of_date: date
    as_of_date_source: str
    counties: tuple[str, ...] | None
    overwrite: bool
    resume: bool
    validate_only: bool
    schema_report: bool
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
    classification_conflicts: list[dict[str, Any]] = field(default_factory=list)
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
    skipped_by_resume: bool = False


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


def atomic_write_csv(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
    fieldnames: Sequence[str] | None = None,
) -> None:
    """Write a stable-schema CSV atomically, including headers when empty."""
    ensure_directory(path.parent)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    if fieldnames is None:
        discovered: list[str] = []
        seen: set[str] = set()
        for row in rows:
            for key in row:
                if key not in seen:
                    seen.add(key)
                    discovered.append(key)
        fieldnames = discovered
    with temporary.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(fieldnames),
            extrasaction="ignore",
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name) for name in fieldnames})
    os.replace(temporary, path)


def portable_relative_path(path: Path, output_root: Path) -> str:
    """Return a validated POSIX path relative to the pipeline output root."""
    try:
        relative = path.resolve().relative_to(output_root.resolve()).as_posix()
    except ValueError as exc:
        raise ValidationError(f"Output path is outside the pipeline root: {path}") from exc
    forbidden = ("/workspaces/", "\\workspaces\\", "/tmp/", "\\temp\\")
    lowered = relative.lower()
    if Path(relative).is_absolute() or re.match(r"^[A-Za-z]:", relative):
        raise ValidationError(f"Portable output path is absolute: {relative}")
    if any(token in lowered for token in forbidden):
        raise ValidationError(f"Portable output path contains an environment-specific segment: {relative}")
    return relative


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
    inventory_payload = asdict(metadata)
    inventory_payload["input_filename"] = Path(metadata.input_path).name
    inventory_payload["input_path_diagnostic"] = inventory_payload.pop("input_path")
    inventory_payload["gdb_path_diagnostic"] = inventory_payload.pop("gdb_path")
    inventory_payload.update({
        "pipeline_version": PIPELINE_VERSION,
        "runtime_schema_version": RUNTIME_SCHEMA_VERSION,
        "full_fidelity_schema_version": FULL_FIDELITY_SCHEMA_VERSION,
        "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
        "classification_rule_version": CLASSIFICATION_RULE_VERSION,
        "portable_path_note": "Absolute source paths are diagnostic only and are not used in portable manifests.",
    })
    atomic_write_json(metadata_dir / "source_inventory.json", inventory_payload)
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
    atomic_write_csv(
        metadata_dir / "source_fields.csv",
        field_rows,
        ("ordinal", "field", "dtype", "required", "optional_known"),
    )


def write_schema_documentation(
    output_dir: Path,
    metadata: SourceMetadata,
    full_fidelity_columns: Sequence[str] | None = None,
) -> None:
    metadata_dir = ensure_directory(output_dir / "source_metadata")
    atomic_write_csv(
        metadata_dir / "runtime_schema.csv",
        list(RUNTIME_SCHEMA),
        ("field_name", "logical_type", "nullable", "source_or_derivation", "description", "analyzer_use", "example"),
    )
    full_columns = list(full_fidelity_columns or ["Source_FID", *metadata.source_fields, *RUNTIME_COLUMNS])
    seen: set[str] = set()
    full_rows: list[dict[str, Any]] = []
    source_dtype_lookup = dict(zip(metadata.source_fields, metadata.source_dtypes))
    for field_name in full_columns:
        if field_name in seen:
            continue
        seen.add(field_name)
        runtime_definition = RUNTIME_SCHEMA_BY_FIELD.get(field_name)
        if field_name in metadata.source_fields:
            source_or_derivation = "original source field"
            description = "Original Wisconsin NG911 source attribute retained without destructive normalization."
            logical_type = source_dtype_lookup.get(field_name, "source-defined")
        elif field_name == "geometry":
            source_or_derivation = "original source geometry"
            description = "Original point geometry normalized to EPSG:4326."
            logical_type = "geometry"
        elif runtime_definition:
            source_or_derivation = runtime_definition["source_or_derivation"]
            description = runtime_definition["description"]
            logical_type = runtime_definition["logical_type"]
        else:
            source_or_derivation = "pipeline-derived"
            description = "Derived preservation or QA field; see the pipeline implementation for the exact rule."
            logical_type = "derived"
        full_rows.append({
            "field_name": field_name,
            "logical_type": logical_type,
            "nullable": True,
            "source_or_derivation": source_or_derivation,
            "description": description,
            "retention_policy": "Preserved in full fidelity",
        })
    atomic_write_csv(
        metadata_dir / "full_fidelity_schema.csv",
        full_rows,
        ("field_name", "logical_type", "nullable", "source_or_derivation", "description", "retention_policy"),
    )
    status_rows = [
        {**row, "manifest_schema_version": MANIFEST_SCHEMA_VERSION}
        for row in STATUS_DEFINITIONS
    ]
    classification_rows = [
        {**row, "classification_rule_version": CLASSIFICATION_RULE_VERSION}
        for row in CLASSIFICATION_DEFINITIONS
    ]
    atomic_write_csv(
        metadata_dir / "status_definitions.csv",
        status_rows,
        (
            "value", "definition", "default_analyzer_behavior",
            "manual_review_recommended", "manifest_schema_version",
        ),
    )
    atomic_write_csv(
        metadata_dir / "classification_definitions.csv",
        classification_rows,
        (
            "value", "definition", "default_analyzer_behavior",
            "confidence_meaning", "manual_review_recommended",
            "classification_rule_version",
        ),
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Standardize Wisconsin statewide NG911 address points into production county GeoParquet files."
    )
    parser.add_argument("--input", required=True, type=Path, help="Source ZIP or extracted .gdb")
    parser.add_argument("--output", required=True, type=Path, help="Pipeline output directory")
    parser.add_argument("--as-of-date", help="Eligibility reference date in YYYY-MM-DD")
    parser.add_argument("--counties", nargs="+", help="Canonical county names; names may include the word County")
    parser.add_argument("--overwrite", action="store_true", help="Replace selected county outputs after new files validate")
    parser.add_argument("--resume", action="store_true", help="Validate and skip complete county outputs; rebuild missing or invalid outputs")
    parser.add_argument("--validate-only", action="store_true", help="Validate existing selected outputs without rebuilding county files")
    parser.add_argument("--schema-report", action="store_true", help="Write schema/status documentation and exit before county processing")
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
    if args.resume and args.overwrite:
        parser.error("--resume and --overwrite are mutually exclusive; resume safely replaces only invalid outputs")
    if args.validate_only and args.overwrite:
        parser.error("--validate-only cannot be combined with --overwrite")
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
) -> tuple["pd.DataFrame", dict[str, tuple[str, ...]], dict[str, int], set[str]]:
    """Read County and NGUID once for statewide inventory and identity QA."""
    LOGGER.info("Reading statewide County and NGUID inventory (%s records)", f"{source_feature_count:,}")
    inventory = pyogrio.read_dataframe(
        gdb_path,
        layer=LAYER_NAME,
        columns=["County", "NGUID"],
        read_geometry=False,
        fid_as_index=True,
        datetime_as_string=True,
        use_arrow=True,
    )
    inventory = inventory.reset_index()
    fid_column = inventory.columns[0]
    if fid_column != "Source_FID":
        inventory = inventory.rename(columns={fid_column: "Source_FID"})
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
    inventory_counts: dict[str, int] = {}
    for county in WI_COUNTIES:
        county_mask = inventory["Canonical_County"].eq(county)
        values = tuple(sorted(inventory.loc[county_mask, "Source_County"].unique()))
        count = int(county_mask.sum())
        variants[county] = values
        inventory_counts[county] = count
        override = COUNTY_STATUS_OVERRIDES.get(county)
        if count == 0:
            initial_status = "not_present_in_source"
            reason = "County is absent from the statewide source snapshot."
        elif override:
            initial_status = str(override["coverage_readiness_status"])
            reason = str(override["reason"])
        else:
            initial_status = "needs_validation"
            reason = "Represented in source but not independently approved for production."
        reports.county_name_variants.append({
            "canonical_county": county,
            "raw_variants": " | ".join(values),
            "statewide_inventory_count": count,
            "represented": bool(count),
            "initial_coverage_status": initial_status,
            "status_reason": reason,
        })

    nguid = clean_series(inventory, "NGUID")
    missing_count = int(nguid.eq("").sum())
    if missing_count:
        LOGGER.warning("Statewide inventory contains %s missing NGUID values; FID fallbacks will preserve them", f"{missing_count:,}")
    duplicate_mask = nguid.ne("") & nguid.duplicated(keep=False)
    duplicate_values = set(nguid[duplicate_mask].tolist())
    if duplicate_values:
        duplicate_rows = inventory.loc[duplicate_mask, ["Source_FID", "Source_County", "Canonical_County", "NGUID"]].copy()
        for nguid_value, group in duplicate_rows.groupby("NGUID", sort=True):
            reports.duplicate_nguid_report.append({
                "canonical_county": " | ".join(sorted(group["Canonical_County"].dropna().astype(str).unique())),
                "source_county": " | ".join(sorted(group["Source_County"].dropna().astype(str).unique())),
                "NGUID": nguid_value,
                "duplicate_record_count": len(group),
                "source_fids": " | ".join(str(value) for value in group["Source_FID"].tolist()),
            })
        LOGGER.warning("Statewide inventory contains %s duplicated NGUID value(s); all records will be preserved with unique Source_Record_ID values", f"{len(duplicate_values):,}")
    return inventory, variants, inventory_counts, duplicate_values


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
        if frame.crs is None:
            raise ValidationError("County source has no CRS.")
        if frame.crs.to_epsg() != 4326:
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
        frame["Canonical_State"] = frame["Source_State"].str.upper().replace("", SOURCE_STATE)
        frame["Canonical_County"] = county

        nguid = clean_series(frame, "NGUID")
        source_fid = clean_series(frame, "Source_FID")
        fallback_sequence = pd.Series(frame.index.astype(str), index=frame.index, dtype="string")
        source_fid_stable = source_fid.where(source_fid.ne(""), fallback_sequence)
        fallback_native_id = "FID-" + source_fid_stable
        native_id = nguid.where(nguid.ne(""), fallback_native_id)
        frame["Canonical_Native_Source_ID"] = native_id
        base_record_id = county_prefix(county) + "-" + native_id
        duplicate_base = base_record_id.duplicated(keep=False)
        record_id = base_record_id.where(
            ~duplicate_base,
            base_record_id + "-FID-" + source_fid_stable,
        )
        if record_id.duplicated(keep=False).any():
            duplicate_sequence = record_id.groupby(record_id).cumcount().astype("string")
            record_id = record_id.where(~record_id.duplicated(keep=False), record_id + "-SEQ-" + duplicate_sequence)
        frame["Source_Record_ID"] = record_id.astype("string")

        geometry = frame.geometry
        geometry_present = geometry.notna() & ~geometry.is_empty
        point_mask = geometry_present & geometry.geom_type.eq("Point")
        x_values = pd.Series(np.nan, index=frame.index, dtype="float64")
        y_values = pd.Series(np.nan, index=frame.index, dtype="float64")
        if point_mask.any():
            x_values.loc[point_mask] = geometry.loc[point_mask].x
            y_values.loc[point_mask] = geometry.loc[point_mask].y
        finite_mask = point_mask & np.isfinite(x_values) & np.isfinite(y_values)
        global_range_mask = finite_mask & x_values.between(-180, 180) & y_values.between(-90, 90)
        minx, miny, maxx, maxy = self.config.wisconsin_bounds
        envelope_mask = global_range_mask & x_values.between(minx, maxx) & y_values.between(miny, maxy)
        frame["Canonical_Longitude"] = x_values
        frame["Canonical_Latitude"] = y_values
        frame["Source_Lat"] = frame["Lat"] if "Lat" in frame.columns else pd.NA
        frame["Source_Long"] = frame["Long"] if "Long" in frame.columns else pd.NA

        quarantine_reasons = pd.Series("", index=frame.index, dtype="string")
        quarantine_reasons = append_flag_series(quarantine_reasons, ~geometry_present, "Missing Geometry")
        quarantine_reasons = append_flag_series(quarantine_reasons, geometry_present & ~point_mask, "Non-Point Geometry")
        quarantine_reasons = append_flag_series(quarantine_reasons, point_mask & ~finite_mask, "Nonfinite Geometry")
        quarantine_reasons = append_flag_series(quarantine_reasons, finite_mask & ~global_range_mask, "Geometry Outside Global Coordinate Range")
        quarantine_reasons = append_flag_series(quarantine_reasons, global_range_mask & ~envelope_mask, "Geometry Outside Wisconsin Envelope")
        frame["Quarantine_Reasons"] = quarantine_reasons
        frame["Canonical_Critical_Failure_Flag"] = quarantine_reasons.ne("")
        quarantine_mask = frame["Canonical_Critical_Failure_Flag"]

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
        runtime = coerce_runtime_schema(runtime)

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
        as_of = pd.Timestamp(self.config.as_of_date, tz="UTC")
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
            future = parsed_values.notna() & parsed_values.gt(as_of + pd.Timedelta(days=1))
            flags = append_flag_series(flags, future, f"Future {source}")
            implausibly_old = parsed_values.notna() & parsed_values.lt(pd.Timestamp("1900-01-01", tz="UTC"))
            flags = append_flag_series(flags, implausibly_old, f"Implausibly Old {source}")
            if source == "DateUpdate":
                placeholder = parsed_values.dt.date.eq(date(1970, 1, 1))
                flags = append_flag_series(flags, placeholder.fillna(False), "Placeholder DateUpdate 1970-01-01")
        effective = parsed["Effective"]
        expire = parsed["Expire"]
        inconsistent = effective.notna() & expire.notna() & expire.lt(effective)
        flags = append_flag_series(flags, inconsistent, "Expire Before Effective")
        frame["Canonical_Date_Quality_Flags"] = flags

        future_effective = effective.notna() & effective.gt(as_of)
        expired = expire.notna() & expire.le(as_of)
        uncertain = (
            (clean_series(frame, "Effective").ne("") & effective.isna())
            | (clean_series(frame, "Expire").ne("") & expire.isna())
            | inconsistent
        )
        active_status = pd.Series("Active", index=frame.index, dtype="string")
        active_status = active_status.mask(future_effective, "Future Effective")
        active_status = active_status.mask(expired, "Expired")
        active_status = active_status.mask(uncertain & ~future_effective & ~expired, "Date Uncertain")
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
        """Derive conservative occupancy and record roles with explicit precedence.

        Priority 1: explicit residential or commercial unit evidence.
        Priority 2: explicit parcel, utility, access, infrastructure, or other
        non-mailable source evidence.
        Priority 3: an otherwise-unclassified populated subaddress.

        A populated subaddress never erases stronger non-mailable source
        evidence. Conflicts are retained as QA flags and reports, not quarantined.
        """
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
        source_classification = pd.concat(
            [place_type, placement, structure, exception], axis=1
        ).fillna("").agg(" | ".join, axis=1).str.replace(
            r"(?:\s*\|\s*)+$", "", regex=True
        ).str.strip(" |")
        frame["Source_Classification_Value"] = source_classification

        source_evidence = (
            landmark + " | " + place_type + " | " + placement + " | "
            + structure + " | " + exception
        ).str.upper()
        combined = (
            source_evidence + " | "
            + frame["Canonical_UnitType"].astype("string").fillna("").str.upper()
        )
        has_subaddress = frame["Canonical_Subaddress"].astype("string").ne("")
        unit_type = frame["Canonical_UnitType"].astype("string").str.upper()

        occupancy = pd.Series(
            "No Unit Classification", index=frame.index, dtype="string"
        )
        reason_code = pd.Series(
            "no_subaddress", index=frame.index, dtype="string"
        )
        reason_text = pd.Series(
            "No subaddress fields are populated", index=frame.index, dtype="string"
        )
        confidence = pd.Series(
            "Not Applicable", index=frame.index, dtype="string"
        )
        occupancy = occupancy.mask(has_subaddress, "Unknown Unit or Subaddress")
        reason_code = reason_code.mask(
            has_subaddress, "subaddress_without_occupancy_or_site_evidence"
        )
        reason_text = reason_text.mask(
            has_subaddress,
            "Subaddress exists but the source does not establish occupancy or specialized-site type",
        )
        confidence = confidence.mask(has_subaddress, "Low")

        apartment = unit_type.str.contains(
            r"\b(?:APT|APARTMENT|CONDO|CONDOMINIUM)\b", regex=True
        )
        residential_side = unit_type.str.contains(
            r"\b(?:UPPER|LOWER|FRONT|REAR)\b", regex=True
        )
        generic_unit = unit_type.str.fullmatch(r"\s*(?:UNIT|UNT)\s*", na=False)
        commercial = unit_type.str.contains(
            r"\b(?:SUITE|STE|OFFICE)\b", regex=True
        )
        hotel = (
            combined.str.contains(r"\b(?:HOTEL|MOTEL|INN|LODGE)\b", regex=True)
            & has_subaddress
        )
        dorm = (
            combined.str.contains(
                r"\b(?:DORM|DORMITORY|RESIDENCE HALL)\b", regex=True
            )
            & has_subaddress
        )
        campground = (
            combined.str.contains(
                r"\b(?:CAMPGROUND|CAMPSITE|CAMP SITE)\b", regex=True
            )
            & has_subaddress
        )
        mobile = (
            combined.str.contains(
                r"\b(?:MOBILE HOME|TRAILER PARK|MANUFACTURED HOME)\b",
                regex=True,
            )
            & has_subaddress
        )
        storage = (
            combined.str.contains(r"\b(?:STORAGE|WAREHOUSE UNIT)\b", regex=True)
            & has_subaddress
        )

        def set_occ(
            mask: pd.Series,
            value: str,
            code: str,
            reason: str,
            conf: str,
        ) -> None:
            nonlocal occupancy, reason_code, reason_text, confidence
            occupancy = occupancy.mask(mask, value)
            reason_code = reason_code.mask(mask, code)
            reason_text = reason_text.mask(mask, reason)
            confidence = confidence.mask(mask, conf)

        set_occ(
            apartment,
            "Residential Apartment or Condominium",
            "explicit_apartment_unit_type",
            "Explicit apartment or condominium unit type",
            "High",
        )
        set_occ(
            residential_side & ~apartment,
            "Residential Side or Duplex Unit",
            "explicit_residential_position_unit_type",
            "Explicit upper, lower, front, or rear unit type",
            "High",
        )
        set_occ(
            commercial,
            "Commercial Suite or Office",
            "explicit_suite_or_office_unit_type",
            "Explicit suite or office unit type",
            "High",
        )
        set_occ(
            hotel & ~commercial,
            "Hotel or Motel Room",
            "lodging_subaddress_evidence",
            "Lodging classification with a subaddress",
            "Medium",
        )
        set_occ(
            dorm & ~commercial,
            "Dormitory Room",
            "dormitory_subaddress_evidence",
            "Residence-hall classification with a subaddress",
            "Medium",
        )
        set_occ(
            campground & ~commercial,
            "Campground Site",
            "campground_subaddress_evidence",
            "Campground classification with a subaddress",
            "Medium",
        )
        set_occ(
            mobile & ~commercial,
            "Mobile-home or Trailer Site",
            "mobile_home_subaddress_evidence",
            "Mobile-home or trailer-community classification with a subaddress",
            "Medium",
        )
        set_occ(
            storage & ~commercial,
            "Storage Unit",
            "storage_subaddress_evidence",
            "Storage classification with a subaddress",
            "Medium",
        )
        set_occ(
            generic_unit
            & has_subaddress
            & occupancy.eq("Unknown Unit or Subaddress"),
            "Unknown Unit or Subaddress",
            "generic_unit_without_occupancy_evidence",
            "Generic Unit type without defensible occupancy evidence",
            "Low",
        )

        frame["Canonical_Occupancy_Category"] = occupancy
        frame["Canonical_Occupancy_Confidence"] = confidence
        frame["Canonical_Occupancy_Reason"] = reason_code
        frame["Canonical_Occupancy_Reasons"] = reason_text
        frame["Canonical_Residential_Unit_Flag"] = occupancy.isin(
            (
                "Residential Apartment or Condominium",
                "Residential Side or Duplex Unit",
                "Dormitory Room",
                "Mobile-home or Trailer Site",
            )
        )
        frame["Canonical_Commercial_Unit_Flag"] = occupancy.eq(
            "Commercial Suite or Office"
        )
        frame["Canonical_Apartment_Candidate_Flag"] = occupancy.eq(
            "Residential Apartment or Condominium"
        )

        # Specialized source evidence intentionally excludes Unit_PreType.
        # An explicit residential/commercial unit can therefore outrank a parcel,
        # utility, or access-site source class without generic Unit text creating
        # false occupancy evidence.
        parcel_evidence = source_evidence.str.contains(
            r"\b(?:PARCEL|SITE LOCATION)\b", regex=True
        )
        utility_evidence = source_evidence.str.contains(
            r"\b(?:UTILITY|HYDRANT|TOWER|SUBSTATION|TRANSFORMER|"
            r"INFRASTRUCTURE)\b",
            regex=True,
        )
        access_evidence = source_evidence.str.contains(
            r"\b(?:ACCESS POINT|DRIVEWAY|GATE|ENTRANCE|PROPERTY ACCESS|"
            r"NON[- ]?MAILABLE|NON[- ]?ADDRESSABLE)\b",
            regex=True,
        )
        residential_unit = frame["Canonical_Residential_Unit_Flag"]
        commercial_unit = frame["Canonical_Commercial_Unit_Flag"]
        explicit_unit = residential_unit | commercial_unit
        any_non_mailable_evidence = (
            parcel_evidence | utility_evidence | access_evidence
        )

        role = pd.Series(
            "Standalone Address", index=frame.index, dtype="string"
        )
        role_reason_code = pd.Series(
            "standalone_without_specialized_evidence",
            index=frame.index,
            dtype="string",
        )
        role_reason = pd.Series(
            "No specialized source role matched",
            index=frame.index,
            dtype="string",
        )
        role_conf = pd.Series("Medium", index=frame.index, dtype="string")

        def set_role(
            mask: pd.Series,
            value: str,
            code: str,
            reason: str,
            conf: str,
        ) -> None:
            nonlocal role, role_reason_code, role_reason, role_conf
            role = role.mask(mask, value)
            role_reason_code = role_reason_code.mask(mask, code)
            role_reason = role_reason.mask(mask, reason)
            role_conf = role_conf.mask(mask, conf)

        # Priority 2 is applied before Priority 3. Utility is applied last among
        # non-mailable categories so explicit infrastructure evidence wins when
        # source values contain several specialized terms.
        set_role(
            parcel_evidence,
            "Parcel or Site",
            "explicit_parcel_or_site_source_class",
            "Source classification indicates parcel or site",
            "Medium",
        )
        set_role(
            access_evidence,
            "Other Non-Mailable Site",
            "explicit_access_point_source_class",
            "Source classification indicates access, entrance, gate, driveway, or another non-mailable site",
            "Medium",
        )
        set_role(
            utility_evidence,
            "Utility or Infrastructure",
            "explicit_utility_source_class",
            "Source classification indicates utility or infrastructure",
            "Medium",
        )

        unknown_unit = (
            has_subaddress
            & ~explicit_unit
            & ~any_non_mailable_evidence
        )
        set_role(
            unknown_unit & ~generic_unit,
            "Unknown Unit Address",
            "subaddress_without_occupancy_or_site_evidence",
            "Subaddress exists but occupancy and specialized-site type remain uncertain",
            "Low",
        )
        set_role(
            unknown_unit & generic_unit,
            "Unknown Unit Address",
            "generic_unit_without_occupancy_evidence",
            "Generic Unit type exists without defensible occupancy or specialized-site evidence",
            "Low",
        )

        # Priority 1 is applied last so defensible occupancy evidence wins over a
        # conflicting source site class.
        set_role(
            residential_unit,
            "Residential Unit",
            "explicit_residential_unit_evidence",
            "Occupancy evidence indicates a residential unit",
            "High",
        )
        set_role(
            commercial_unit,
            "Commercial Unit",
            "explicit_commercial_unit_evidence",
            "Occupancy evidence indicates a suite or office",
            "High",
        )
        role_reason_code = role_reason_code.mask(
            explicit_unit,
            frame["Canonical_Occupancy_Reason"].astype("string"),
        )

        frame["Canonical_Record_Role"] = role
        frame["Canonical_Record_Role_Confidence"] = role_conf
        frame["Canonical_Record_Role_Reason_Code"] = role_reason_code
        frame["Canonical_Record_Role_Reasons"] = role_reason
        frame["Canonical_Status"] = role

        conflict = has_subaddress & any_non_mailable_evidence
        frame["Classification_Conflict_Flag"] = conflict
        frame["Classification_Conflict_Type"] = pd.Series(
            np.where(
                conflict,
                "subaddress_with_explicit_non_mailable_evidence",
                "",
            ),
            index=frame.index,
            dtype="string",
        )
        conflict_category = pd.Series("", index=frame.index, dtype="string")
        conflict_category = conflict_category.mask(
            conflict & parcel_evidence, "parcel_or_site"
        )
        conflict_category = conflict_category.mask(
            conflict & access_evidence, "access_or_non_mailable"
        )
        conflict_category = conflict_category.mask(
            conflict & utility_evidence, "utility_or_infrastructure"
        )
        frame["Classification_Conflict_Evidence_Category"] = conflict_category

        frame["Source_Parent_Candidate"] = (
            ~has_subaddress
            & combined.str.contains(
                r"\b(?:BUILDING WITH UNITS|BUILDING W/? UNITS|"
                r"MULTI[- ]UNIT BUILDING)\b",
                regex=True,
            )
        )

    def _derive_parent_child(self, frame: "gpd.GeoDataFrame") -> None:
        key = frame["Parent_Group_Key"].astype("string").fillna("")
        has_subaddress = frame["Canonical_Subaddress"].astype("string").ne("")
        valid_key = key.ne("")
        child_count_map = frame.loc[valid_key & has_subaddress].groupby("Parent_Group_Key").size()
        blank_parent_count_map = frame.loc[valid_key & ~has_subaddress].groupby("Parent_Group_Key").size()
        group_count_map = frame.loc[valid_key].groupby("Parent_Group_Key").size()
        child_count = key.map(child_count_map).fillna(0).astype("int64")
        parent_count = key.map(blank_parent_count_map).fillna(0).astype("int64")
        group_count = key.map(group_count_map).fillna(0).astype("int64")
        inferred_parent = valid_key & ~has_subaddress & child_count.gt(0)
        source_parent = frame["Source_Parent_Candidate"].fillna(False).astype(bool)
        potential_parent = inferred_parent | source_parent
        potential_child = valid_key & has_subaddress & parent_count.gt(0)
        ambiguity = (potential_child & parent_count.gt(1)) | (potential_parent & child_count.eq(0))
        conflict = potential_parent & has_subaddress

        frame["Potential_Parent_Record"] = potential_parent
        frame["Potential_Child_Record"] = potential_child
        frame["Child_Record_Count"] = child_count
        frame["Potential_Parent_Count"] = parent_count
        frame["Parent_Group_Record_Count"] = group_count
        frame["Parent_Child_Ambiguity_Flag"] = ambiguity
        frame["Parent_Child_Conflict_Flag"] = conflict
        frame["Parent_Child_Confidence"] = np.where(
            ambiguity | conflict,
            "Review",
            np.where(
                potential_parent | potential_child,
                np.where(child_count.ge(2) | parent_count.eq(1), "High", "Medium"),
                "Not Applicable",
            ),
        )
        frame["Potential_Double_Count_Flag"] = inferred_parent
        frame.loc[potential_parent, "Canonical_Record_Role"] = "Building Parent"
        frame.loc[potential_parent, "Canonical_Record_Role_Confidence"] = np.where(
            child_count.loc[potential_parent].ge(2), "High", "Medium"
        )
        frame.loc[potential_parent, "Canonical_Record_Role_Reasons"] = np.where(
            child_count.loc[potential_parent].gt(0),
            "Blank-subaddress record shares a normalized building address with child-unit records",
            "Source classification indicates a parent building but no matching child unit was found",
        )
        frame.loc[potential_parent, "Canonical_Status"] = "Building Parent"

    def _derive_quality(self, frame: "gpd.GeoDataFrame", geometry_good: "pd.Series") -> None:
        flags = pd.Series("", index=frame.index, dtype="string")
        full_house = frame["Canonical_Full_House_Number"].astype("string").fillna("")
        full_street = frame["Canonical_Full_Street"].astype("string").fillna("")
        full_address = frame["Canonical_Full_Address"].astype("string").fillna("")
        mailable_address = frame["Canonical_Mailable_Address"].astype("string").fillna("")
        postal_city = frame["Canonical_Postal_City"].astype("string").fillna("")
        locality = frame["Canonical_Muni"].astype("string").fillna("")
        zip5 = frame["Canonical_Zip_Code"].astype("string").fillna("")
        state = frame["Canonical_State"].astype("string").fillna("")
        nguid = clean_series(frame, "NGUID")
        zero_house = full_house.str.fullmatch(r"[A-Z]*0+(?:\.0+)?", na=False)
        duplicate_nguid = nguid.ne("") & nguid.isin(self.duplicate_nguids)
        duplicate_source_id = frame["Source_Record_ID"].astype("string").duplicated(keep=False)
        flags = append_flag_series(flags, nguid.eq(""), "Missing NGUID - FID Fallback Used")
        flags = append_flag_series(flags, duplicate_nguid, "Duplicate NGUID - Unique Source Record ID Used")
        flags = append_flag_series(flags, duplicate_source_id, "Duplicate Source Record ID")
        flags = append_flag_series(flags, full_house.eq(""), "Missing House Number")
        flags = append_flag_series(flags, zero_house, "Zero House Number")
        flags = append_flag_series(flags, full_street.eq(""), "Missing Street")
        flags = append_flag_series(flags, full_address.eq(""), "Missing Full Physical Address")
        flags = append_flag_series(flags, mailable_address.eq(""), "Missing Mailable Address")
        flags = append_flag_series(flags, postal_city.eq(""), "Missing Postal City")
        flags = append_flag_series(flags, frame["Canonical_Postal_City_Fallback_Flag"], "Postal City Fallback Used")
        flags = append_flag_series(flags, locality.eq(""), "Missing Municipality/Locality")
        flags = append_flag_series(flags, zip5.eq(""), "Missing ZIP")
        flags = append_flag_series(flags, state.ne(SOURCE_STATE), "Unexpected State Abbreviation")
        flags = append_flag_series(flags, frame["Canonical_ZIP_Quality_Flag"].astype("string").ne("") & ~frame["Canonical_ZIP_Quality_Flag"].astype("string").eq("Missing ZIP"), "Invalid ZIP Value")
        flags = append_flag_series(flags, frame["Canonical_Street_Component_Fallback"], "Full Street Fallback Used")
        flags = append_flag_series(flags, frame["Canonical_Occupancy_Reason"].eq("generic_unit_without_occupancy_evidence"), "Generic Unit Without Occupancy Evidence")
        flags = append_flag_series(flags, frame["Potential_Double_Count_Flag"], "Parent Building with Child Units")
        flags = append_flag_series(flags, frame["Parent_Child_Ambiguity_Flag"], "Parent Child Ambiguity")
        flags = append_flag_series(flags, frame["Parent_Child_Conflict_Flag"], "Conflicting Parent Child Evidence")
        flags = append_flag_series(flags, frame["Canonical_Date_Quality_Flags"].astype("string").ne(""), "Source Date Anomaly")
        flags = append_flag_series(flags, frame["Source_Coordinate_Difference_Meters"].gt(100), "Source Coordinates Disagree with Geometry")
        flags = append_flag_series(flags, frame["Canonical_Record_Role_Confidence"].eq("Low"), "Low-Confidence Record Role")
        flags = append_flag_series(
            flags,
            frame["Classification_Conflict_Flag"].fillna(False).astype(bool),
            "Subaddress with Explicit Non-Mailable Source Evidence",
        )
        flags = append_flag_series(flags, ~geometry_good, "Geometry Quality Failure")
        duplicate_address = frame["Normalized_Address_Key"].astype("string").ne("") & frame["Normalized_Address_Key"].duplicated(keep=False)
        flags = append_flag_series(flags, duplicate_address, "Potential Duplicate Normalized Address")
        frame["Canonical_Quality_Flags"] = flags

        frame["Canonical_Address_Quality_Status"] = np.where(
            full_house.ne("") & full_street.ne("") & full_address.ne(""), "Usable", "Review"
        )
        frame["Canonical_Geometry_Quality_Status"] = np.where(geometry_good, "Usable", "Quarantine")
        frame["Canonical_Classification_Quality_Status"] = np.where(
            frame["Canonical_Record_Role_Confidence"].isin(("High", "Medium", "Not Applicable"))
            & ~frame["Parent_Child_Ambiguity_Flag"]
            & ~frame["Parent_Child_Conflict_Flag"],
            "Usable",
            "Review",
        )
        frame["Canonical_Postal_Quality_Status"] = np.where(
            postal_city.ne("") & zip5.ne("") & state.eq(SOURCE_STATE),
            "Complete",
            np.where(locality.ne("") | zip5.ne(""), "Partial", "Missing"),
        )

        role = frame["Canonical_Record_Role"].astype("string")
        handling = pd.Series("include_standard", index=frame.index, dtype="string")
        handling = handling.mask(role.isin(("Residential Unit", "Commercial Unit")), "include_unit")
        handling = handling.mask(role.eq("Unknown Unit Address"), "manual_review")
        handling = handling.mask(role.eq("Building Parent"), "include_parent_for_review")
        handling = handling.mask(role.isin(("Parcel or Site", "Utility or Infrastructure", "Other Non-Mailable Site")), "exclude_default")
        handling = handling.mask(frame["Canonical_Address_Quality_Status"].eq("Review") & handling.eq("include_standard"), "manual_review")
        handling = handling.mask(frame["Canonical_Critical_Failure_Flag"], "quarantine")
        exclusion = pd.Series("none", index=frame.index, dtype="string")
        exclusion = exclusion.mask(role.eq("Parcel or Site"), "parcel_or_site")
        exclusion = exclusion.mask(role.eq("Utility or Infrastructure"), "utility_or_infrastructure")
        exclusion = exclusion.mask(role.eq("Other Non-Mailable Site"), "other_non_mailable_site")
        exclusion = exclusion.mask(role.eq("Building Parent"), "parent_building_review")
        exclusion = exclusion.mask(role.eq("Unknown Unit Address"), "unknown_unit_review")
        exclusion = exclusion.mask(frame["Canonical_Critical_Failure_Flag"], "critical_geometry_failure")
        frame["Canonical_Analyzer_Handling"] = handling
        frame["Canonical_Exclusion_Category"] = exclusion
        frame["Canonical_Analyzer_Eligible"] = ~handling.isin(("exclude_default", "quarantine"))

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

        def completeness(column: str) -> float:
            if not source_count or column not in full.columns:
                return 0.0
            return float(clean_series(full, column).ne("").mean())

        nguid = clean_series(full, "NGUID")
        latest_update = full["DateUpdate_Parsed"].dropna().max()
        geometry_usable = full["Canonical_Geometry_Quality_Status"].eq("Usable")
        duplicate_nguid_records = nguid.ne("") & nguid.isin(self.duplicate_nguids)
        source_record_ids = clean_series(full, "Source_Record_ID")
        warning_records = full["Canonical_Quality_Flags"].astype("string").fillna("").ne("")
        technical_warning_count = (
            quarantine_count
            + int(nguid.eq("").sum())
            + int(duplicate_nguid_records.sum())
            + int(source_record_ids.duplicated(keep=False).sum())
        )
        metrics: dict[str, Any] = {
            "source_record_count": source_count,
            "runtime_record_count": runtime_count,
            "quarantine_record_count": quarantine_count,
            "reconciliation_passed": source_count == runtime_count + quarantine_count,
            "nguid_completeness": float(nguid.ne("").mean()) if source_count else 0.0,
            "missing_nguid_count": int(nguid.eq("").sum()),
            "duplicate_nguid_record_count": int(duplicate_nguid_records.sum()),
            "duplicate_source_record_id_count": int(source_record_ids.duplicated(keep=False).sum()),
            "source_record_id_unique": bool(source_record_ids.ne("").all() and source_record_ids.is_unique),
            "geometry_completeness": float(geometry_usable.mean()) if source_count else 0.0,
            "bounds": tuple(float(v) for v in runtime.total_bounds) if runtime_count else None,
            "zero_house_number_count": int(full["Canonical_Full_House_Number"].astype("string").str.fullmatch(r"[A-Z]*0+(?:\.0+)?", na=False).sum()),
            "missing_street_count": int(full["Canonical_Full_Street"].astype("string").eq("").sum()),
            "missing_full_address_count": int(full["Canonical_Full_Address"].astype("string").eq("").sum()),
            "missing_mailable_address_count": int(full["Canonical_Mailable_Address"].astype("string").eq("").sum()),
            "unit_address_count": int(full["Canonical_Subaddress"].astype("string").ne("").sum()),
            "residential_unit_candidate_count": count_true("Canonical_Residential_Unit_Flag"),
            "commercial_unit_candidate_count": count_true("Canonical_Commercial_Unit_Flag"),
            "apartment_candidate_count": count_true("Canonical_Apartment_Candidate_Flag"),
            "parent_building_count": count_true("Potential_Parent_Record"),
            "potential_child_count": count_true("Potential_Child_Record"),
            "potential_double_count_count": count_true("Potential_Double_Count_Flag"),
            "parent_child_ambiguity_count": count_true("Parent_Child_Ambiguity_Flag"),
            "landmark_count": int(full["Canonical_Landmark_Name"].astype("string").ne("").sum()),
            "zip_completeness": completeness("Canonical_Zip_Code"),
            "zip4_count": int(full["Canonical_ZIP4"].astype("string").ne("").sum()),
            "postal_city_completeness": completeness("Canonical_Postal_City"),
            "locality_fallback_count": count_true("Canonical_Postal_City_Fallback_Flag"),
            "municipality_locality_completeness": completeness("Canonical_Muni"),
            "street_completeness": completeness("Canonical_Full_Street"),
            "invalid_date_count": int(full["Canonical_Date_Quality_Flags"].astype("string").ne("").sum()),
            "future_effective_count": int(full["Canonical_Active_Status"].eq("Future Effective").sum()),
            "expired_count": int(full["Canonical_Active_Status"].eq("Expired").sum()),
            "latest_plausible_dateupdate": latest_update.isoformat() if pd.notna(latest_update) else None,
            "unknown_classification_count": int(full["Canonical_Record_Role"].eq("Unknown Unit Address").sum()),
            "unknown_unit_count": int(full["Canonical_Record_Role"].eq("Unknown Unit Address").sum()),
            "warning_count": int(warning_records.sum()),
            "technical_warning_count": technical_warning_count,
            "classification_conflict_count": count_true("Classification_Conflict_Flag"),
            "subaddress_with_parcel_or_site_count": int(
                full["Classification_Conflict_Evidence_Category"]
                .astype("string")
                .eq("parcel_or_site")
                .sum()
            ),
            "subaddress_with_utility_or_infrastructure_count": int(
                full["Classification_Conflict_Evidence_Category"]
                .astype("string")
                .eq("utility_or_infrastructure")
                .sum()
            ),
            "subaddress_with_access_or_non_mailable_count": int(
                full["Classification_Conflict_Evidence_Category"]
                .astype("string")
                .eq("access_or_non_mailable")
                .sum()
            ),
            "runtime_column_count": len(runtime.columns),
            "full_fidelity_column_count": len(full.columns),
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
            if full[column].dtype == object or str(full[column].dtype).startswith("string"):
                blank_count = int(clean_series(full, column).eq("").sum())
            else:
                blank_count = null_count
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
            reports.record_role_summary.append({
                "county": county,
                "record_role": value,
                "count": int(count),
                "pct": float(count / total) if total else 0.0,
            })
        occupancy_groups = full.groupby(
            ["Canonical_Occupancy_Category", "Canonical_Occupancy_Confidence", "Canonical_Occupancy_Reason"],
            dropna=False,
        ).size().sort_values(ascending=False)
        for (category, confidence, reason), count in occupancy_groups.items():
            reports.occupancy_summary.append({
                "county": county,
                "occupancy_category": category,
                "occupancy_confidence": confidence,
                "occupancy_reason": reason,
                "count": int(count),
                "pct": float(count / total) if total else 0.0,
            })

        conflict_mask = full[
            "Classification_Conflict_Flag"
        ].fillna(False).astype(bool)
        conflict_rows = full.loc[conflict_mask].head(
            MAX_DETAIL_REPORT_ROWS_PER_COUNTY
        )
        for row in conflict_rows[
            [
                "Source_Record_ID",
                "NGUID",
                "Source_Place_Type",
                "Source_Placement",
                "Source_Structure",
                "Source_Exception",
                "Canonical_UnitType",
                "Canonical_Unit",
                "Canonical_Subaddress",
                "Canonical_Record_Role",
                "Canonical_Record_Role_Reason_Code",
                "Canonical_Analyzer_Handling",
                "Canonical_Exclusion_Category",
                "Classification_Conflict_Type",
            ]
        ].to_dict("records"):
            reports.classification_conflicts.append({
                "county": county,
                "source_record_id": row.get("Source_Record_ID"),
                "nguid": row.get("NGUID"),
                "source_place_type": row.get("Source_Place_Type"),
                "source_placement": row.get("Source_Placement"),
                "source_structure": row.get("Source_Structure"),
                "source_exception": row.get("Source_Exception"),
                "unit_type": row.get("Canonical_UnitType"),
                "unit_value": row.get("Canonical_Unit"),
                "subaddress": row.get("Canonical_Subaddress"),
                "selected_record_role": row.get("Canonical_Record_Role"),
                "classification_reason": row.get(
                    "Canonical_Record_Role_Reason_Code"
                ),
                "analyzer_handling": row.get(
                    "Canonical_Analyzer_Handling"
                ),
                "exclusion_category": row.get(
                    "Canonical_Exclusion_Category"
                ),
                "conflict_type": row.get("Classification_Conflict_Type"),
            })

        potential_parent = full["Potential_Parent_Record"].fillna(False).astype(bool)
        potential_child = full["Potential_Child_Record"].fillna(False).astype(bool)
        source_parent = full["Source_Parent_Candidate"].fillna(False).astype(bool)
        child_count = full["Child_Record_Count"].fillna(0).astype(int)
        parent_count = full["Potential_Parent_Count"].fillna(0).astype(int)
        conflict = full["Parent_Child_Ambiguity_Flag"].fillna(False).astype(bool) | full["Parent_Child_Conflict_Flag"].fillna(False).astype(bool)
        reports.parent_child_summary.append({
            "county": county,
            "potential_parent_records": int(potential_parent.sum()),
            "potential_child_records": int(potential_child.sum()),
            "source_parent_candidates": int(source_parent.sum()),
            "parent_records_zero_matching_children": int((source_parent & child_count.eq(0)).sum()),
            "parent_records_one_matching_child": int((potential_parent & child_count.eq(1)).sum()),
            "parent_records_multiple_matching_children": int((potential_parent & child_count.gt(1)).sum()),
            "children_one_possible_parent": int((potential_child & parent_count.eq(1)).sum()),
            "children_multiple_possible_parents": int((potential_child & parent_count.gt(1)).sum()),
            "potential_double_count_records": int(full["Potential_Double_Count_Flag"].fillna(False).astype(bool).sum()),
            "conflicting_parent_child_records": int(conflict.sum()),
            "parent_groups": int(full.loc[potential_parent, "Parent_Group_Key"].nunique()),
        })

        date_anomaly = full[full["Canonical_Date_Quality_Flags"].astype("string").ne("")].head(MAX_DETAIL_REPORT_ROWS_PER_COUNTY)
        date_columns = ["Source_Record_ID", "NGUID", "DateUpdate_Raw", "Effective_Raw", "Expire_Raw", "Canonical_Date_Quality_Flags"]
        for row in date_anomaly[date_columns].to_dict("records"):
            reports.date_anomalies.append({"county": county, **row})

        coordinate_mask = (
            full["Source_Coordinate_Difference_Meters"].gt(100)
            | full["Canonical_Critical_Failure_Flag"].fillna(False).astype(bool)
        )
        coordinate_anomaly = full.loc[coordinate_mask].copy().head(MAX_DETAIL_REPORT_ROWS_PER_COUNTY)
        for row in coordinate_anomaly[
            ["Source_Record_ID", "NGUID", "Source_Lat", "Source_Long", "Canonical_Latitude", "Canonical_Longitude", "Source_Coordinate_Difference_Meters", "Quarantine_Reasons"]
        ].to_dict("records"):
            reason = row.pop("Quarantine_Reasons") or "Source coordinates differ from geometry by more than 100 meters"
            reports.coordinate_anomalies.append({"county": county, **row, "anomaly_reason": reason})

        duplicate_counts = full.loc[full["Normalized_Address_Key"].astype("string").ne("")].groupby("Normalized_Address_Key").size()
        duplicate_counts = duplicate_counts[duplicate_counts.gt(1)].sort_values(ascending=False).head(MAX_DETAIL_REPORT_ROWS_PER_COUNTY)
        for address_key, count in duplicate_counts.items():
            reports.duplicate_address_summary.append({
                "county": county,
                "normalized_address_key": address_key,
                "record_count": int(count),
            })
        fallback_counts = full["Canonical_Locality_Source"].replace("", "[NONE]").value_counts()
        for source_name, count in fallback_counts.items():
            reports.postal_fallback_summary.append({
                "county": county,
                "locality_source": source_name,
                "count": int(count),
                "pct": float(count / total) if total else 0.0,
            })
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



def coerce_runtime_schema(frame: "gpd.GeoDataFrame") -> "gpd.GeoDataFrame":
    """Create the stable, ordered, lean runtime contract for every county."""
    work = frame.copy()
    for column in RUNTIME_COLUMNS:
        if column not in work.columns:
            if column == "geometry":
                raise ValidationError("Runtime source is missing geometry")
            work[column] = pd.NA
    for column, dtype in RUNTIME_DTYPES.items():
        if column not in work.columns:
            continue
        if dtype == "string":
            work[column] = work[column].astype("string").fillna("")
        elif dtype == "boolean":
            work[column] = work[column].fillna(False).astype("boolean")
        elif dtype == "Int64":
            work[column] = pd.to_numeric(work[column], errors="coerce").fillna(0).astype("Int64")
        elif dtype == "float64":
            work[column] = pd.to_numeric(work[column], errors="coerce").astype("float64")
    return gpd.GeoDataFrame(
        work.loc[:, list(RUNTIME_COLUMNS)],
        geometry="geometry",
        crs="EPSG:4326",
    )

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


def geometry_hash_map(frame: "gpd.GeoDataFrame", id_column: str = "Source_Record_ID") -> dict[str, str]:
    ids = clean_series(frame, id_column)
    result: dict[str, str] = {}
    for record_id, geometry in zip(ids, frame.geometry, strict=False):
        if not record_id:
            continue
        payload = b"" if geometry is None else geometry.wkb
        result[record_id] = hashlib.sha256(payload).hexdigest()
    return result


def validate_geometry_frame(frame: "gpd.GeoDataFrame", label: str, require_wisconsin: bool = True) -> None:
    if "geometry" not in frame.columns:
        raise ValidationError(f"{label} has no geometry column")
    if frame.crs is None or frame.crs.to_epsg() != 4326:
        raise ValidationError(f"{label} CRS mismatch: {frame.crs}")
    if frame.geometry.isna().any() or frame.geometry.is_empty.any():
        raise ValidationError(f"{label} contains null or empty geometry")
    if not frame.geometry.geom_type.eq("Point").all():
        raise ValidationError(f"{label} contains non-Point geometry")
    x = frame.geometry.x.to_numpy(dtype="float64")
    y = frame.geometry.y.to_numpy(dtype="float64")
    if not np.isfinite(x).all() or not np.isfinite(y).all():
        raise ValidationError(f"{label} contains nonfinite coordinates")
    if not ((x >= -180) & (x <= 180) & (y >= -90) & (y <= 90)).all():
        raise ValidationError(f"{label} contains coordinates outside valid global ranges")
    if require_wisconsin and len(frame):
        minx, miny, maxx, maxy = DEFAULT_WISCONSIN_BOUNDS
        if not ((x >= minx) & (x <= maxx) & (y >= miny) & (y <= maxy)).all():
            raise ValidationError(f"{label} contains coordinates outside the Wisconsin envelope")


def validate_full_fidelity(
    source: "gpd.GeoDataFrame",
    parquet_path: Path,
    source_fields: Sequence[str],
) -> list[str]:
    messages: list[str] = []
    parquet_file = pq.ParquetFile(parquet_path)
    metadata = parquet_file.metadata
    if metadata.num_rows != len(source):
        raise ValidationError(f"Full-fidelity row count mismatch: {metadata.num_rows:,} vs {len(source):,}")
    schema_names = set(parquet_file.schema_arrow.names)
    missing_source_fields = [field for field in source_fields if field not in schema_names]
    if missing_source_fields:
        raise ValidationError("Full-fidelity output lost source fields: " + ", ".join(missing_source_fields))
    required_derived = ("Source_FID", "Source_Record_ID", "Canonical_Native_Source_ID", "Quarantine_Reasons", "geometry")
    missing_derived = [field for field in required_derived if field not in schema_names]
    if missing_derived:
        raise ValidationError("Full-fidelity output is missing required preservation fields: " + ", ".join(missing_derived))
    source_nulls = {field: int(source[field].isna().sum()) for field in source_fields}
    output_nulls = parquet_null_counts(parquet_path, source_fields)
    mismatches = [field for field in source_fields if source_nulls[field] != output_nulls.get(field, -1)]
    if mismatches:
        raise ValidationError("Full-fidelity null-count mismatch in fields: " + ", ".join(mismatches[:20]))
    output_identity = gpd.read_parquet(parquet_path, columns=["Source_Record_ID", "geometry"])
    source_ids = clean_series(source, "Source_Record_ID")
    output_ids = clean_series(output_identity, "Source_Record_ID")
    if source_ids.eq("").any() or not source_ids.is_unique:
        raise ValidationError("In-memory full-fidelity Source_Record_ID is blank or non-unique")
    if set(source_ids) != set(output_ids):
        raise ValidationError("Full-fidelity Source_Record_ID set does not match source")
    if geometry_hash_map(source) != geometry_hash_map(output_identity):
        raise ValidationError("Full-fidelity geometry hash map does not match source")
    if output_identity.crs is None or output_identity.crs.to_epsg() != 4326:
        raise ValidationError(f"Full-fidelity CRS mismatch: {output_identity.crs}")
    if len(source) and not np.allclose(source.total_bounds, output_identity.total_bounds, equal_nan=True, atol=1e-12):
        raise ValidationError("Full-fidelity total bounds changed")
    messages.append("Full-fidelity record, source-field, null, identity, CRS, bounds, and geometry checks passed")
    return messages


def validate_runtime_arrow_schema(path: Path) -> None:
    schema = pq.ParquetFile(path).schema_arrow
    logical_names = [name for name in schema.names if name != "bbox"]
    if logical_names != list(RUNTIME_COLUMNS):
        raise ValidationError("Runtime schema order does not match the versioned RUNTIME_SCHEMA contract")
    for field_name, dtype in RUNTIME_DTYPES.items():
        arrow_type = schema.field(field_name).type
        if dtype == "string" and not (pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type)):
            raise ValidationError(f"Runtime field {field_name} expected string but found {arrow_type}")
        if dtype == "boolean" and not pa.types.is_boolean(arrow_type):
            raise ValidationError(f"Runtime field {field_name} expected boolean but found {arrow_type}")
        if dtype == "Int64" and not pa.types.is_integer(arrow_type):
            raise ValidationError(f"Runtime field {field_name} expected integer but found {arrow_type}")
        if dtype == "float64" and not pa.types.is_floating(arrow_type):
            raise ValidationError(f"Runtime field {field_name} expected floating point but found {arrow_type}")


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
    runtime_ids = clean_series(runtime, "Source_Record_ID")
    quarantine_ids = clean_series(quarantine, "Source_Record_ID")
    if set(runtime_ids) & set(quarantine_ids):
        raise ValidationError("A Source_Record_ID appears in both runtime and quarantine")
    validate_runtime_arrow_schema(runtime_path)
    output = gpd.read_parquet(runtime_path)
    missing = [column for column in CURRENT_ANALYZER_COLUMNS if column not in output.columns]
    if missing:
        raise ValidationError("Runtime output is missing current Analyzer columns: " + ", ".join(missing))
    if len(output) != len(runtime):
        raise ValidationError("Runtime output row count mismatch")
    output_ids = clean_series(output, "Source_Record_ID")
    if output_ids.eq("").any() or not output_ids.is_unique:
        raise ValidationError("Runtime Source_Record_ID is missing or non-unique")
    if set(runtime_ids) != set(output_ids):
        raise ValidationError("Runtime Source_Record_ID set mismatch")
    if output["Canonical_Critical_Failure_Flag"].fillna(False).astype(bool).any():
        raise ValidationError("Runtime contains records marked with a critical failure")
    validate_geometry_frame(output, "Runtime output", require_wisconsin=True)

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
    messages.append("Runtime reconciliation, stable schema, identity, geometry, Analyzer-column, and CRS checks passed")
    return messages, bbox_validated


def validate_quarantine(quarantine: "gpd.GeoDataFrame", path: Path) -> None:
    output = gpd.read_parquet(path)
    if len(output) != len(quarantine):
        raise ValidationError("Quarantine row count mismatch")
    record_ids = clean_series(output, "Source_Record_ID")
    if record_ids.eq("").any() or not record_ids.is_unique:
        raise ValidationError("Quarantine Source_Record_ID is missing or non-unique")
    if not output["Canonical_Critical_Failure_Flag"].fillna(False).astype(bool).all():
        raise ValidationError("Quarantine contains a record without a critical-failure flag")
    if output["Quarantine_Reasons"].astype("string").fillna("").eq("").any():
        raise ValidationError("Quarantine contains a record without a quarantine reason")
    if output.crs is None or output.crs.to_epsg() != 4326:
        raise ValidationError(f"Quarantine CRS mismatch: {output.crs}")


def finalize_parquet(temporary: Path, destination: Path) -> None:
    os.replace(temporary, destination)


def file_record(
    path: Path,
    output_root: Path,
    county: str,
    output_type: str,
    schema_version: str,
) -> dict[str, Any]:
    parquet_file = pq.ParquetFile(path)
    crs_value = "EPSG:4326"
    return {
        "county": county,
        "output_type": output_type,
        "relative_path": portable_relative_path(path, output_root),
        "file_size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
        "row_count": parquet_file.metadata.num_rows,
        "column_count": len([name for name in parquet_file.schema_arrow.names if name != "bbox"]),
        "crs": crs_value,
        "created_timestamp_utc": utc_now_iso(),
        "schema_version": schema_version,
        "classification_rule_version": CLASSIFICATION_RULE_VERSION,
        "row_group_count": parquet_file.metadata.num_row_groups,
    }


def process_county(
    county: str,
    adapter: WisconsinStatewideNG911Adapter,
    config: PipelineConfig,
    reports: ReportStore,
    expected_inventory_count: int,
    overwrite_existing: bool = False,
) -> CountyOutput:
    started = time.perf_counter()
    source = adapter.read_county(county)
    LOGGER.info("%s County source records: %s", county, f"{len(source):,}")
    if len(source) != expected_inventory_count:
        raise ValidationError(
            f"County read count {len(source):,} does not match statewide inventory count {expected_inventory_count:,}"
        )
    full, runtime, quarantine, metrics = adapter.standardize_county(county, source, reports)
    metrics["_full_fidelity_columns"] = list(full.columns)
    output_paths: dict[str, Path | None] = {"full_fidelity": None, "runtime": None, "quarantine": None}
    temp_paths: dict[str, Path] = {}
    messages: list[str] = []
    covering_bbox_written = False
    bbox_validated = False
    effective_overwrite = config.overwrite or overwrite_existing

    full_destination = config.output_dir / "full_fidelity" / f"{county_slug(county)}.parquet"
    runtime_destination = config.output_dir / "runtime" / f"{county_slug(county)}.parquet"
    quarantine_destination = config.output_dir / "quarantine" / f"{county_slug(county)}.parquet"

    planned_outputs: list[Path] = []
    if not config.runtime_only:
        planned_outputs.append(full_destination)
    if not config.full_fidelity_only:
        planned_outputs.append(runtime_destination)
    if not quarantine.empty:
        planned_outputs.append(quarantine_destination)
    for planned_output in planned_outputs:
        ensure_output_allowed(planned_output, effective_overwrite)

    try:
        if not config.runtime_only:
            full_temp, _ = write_geoparquet_atomic(
                full, full_destination, config.row_group_size, effective_overwrite, True
            )
            temp_paths["full_fidelity"] = full_temp
            messages.extend(validate_full_fidelity(full, full_temp, adapter.metadata.source_fields))

        if not config.full_fidelity_only:
            runtime_temp, covering_bbox_written = write_geoparquet_atomic(
                runtime, runtime_destination, config.row_group_size, effective_overwrite, True
            )
            temp_paths["runtime"] = runtime_temp
            runtime_messages, bbox_validated = validate_runtime(
                full, runtime, quarantine, runtime_temp, covering_bbox_written
            )
            messages.extend(runtime_messages)

        if not quarantine.empty:
            quarantine_temp, _ = write_geoparquet_atomic(
                quarantine, quarantine_destination, config.row_group_size, effective_overwrite, True
            )
            temp_paths["quarantine"] = quarantine_temp
            validate_quarantine(quarantine, quarantine_temp)

        # Finalize only after every planned file has been written and validated.
        if "full_fidelity" in temp_paths:
            finalize_parquet(temp_paths["full_fidelity"], full_destination)
            output_paths["full_fidelity"] = full_destination
        if "runtime" in temp_paths:
            finalize_parquet(temp_paths["runtime"], runtime_destination)
            output_paths["runtime"] = runtime_destination
        if "quarantine" in temp_paths:
            finalize_parquet(temp_paths["quarantine"], quarantine_destination)
            output_paths["quarantine"] = quarantine_destination
        elif effective_overwrite and quarantine_destination.exists():
            quarantine_destination.unlink()

        file_rows: dict[str, dict[str, Any]] = {}
        if output_paths["full_fidelity"]:
            file_rows["full_fidelity"] = file_record(
                full_destination, config.output_dir, county, "full_fidelity", FULL_FIDELITY_SCHEMA_VERSION
            )
        if output_paths["runtime"]:
            file_rows["runtime"] = file_record(
                runtime_destination, config.output_dir, county, "runtime", RUNTIME_SCHEMA_VERSION
            )
        if output_paths["quarantine"]:
            file_rows["quarantine"] = file_record(
                quarantine_destination, config.output_dir, county, "quarantine", FULL_FIDELITY_SCHEMA_VERSION
            )
        reports.output_files.extend(file_rows.values())

        full_size = file_rows.get("full_fidelity", {}).get("file_size_bytes")
        runtime_size = file_rows.get("runtime", {}).get("file_size_bytes")
        quarantine_size = file_rows.get("quarantine", {}).get("file_size_bytes")
        runtime_reduction = (
            int(full_size) - int(runtime_size)
            if full_size is not None and runtime_size is not None else None
        )
        runtime_reduction_pct = (
            round(runtime_reduction / int(full_size) * 100, 2)
            if runtime_reduction is not None and int(full_size) > 0 else None
        )
        runtime_parquet = output_paths["runtime"] or output_paths["full_fidelity"]
        row_groups = pq.ParquetFile(runtime_parquet).metadata.num_row_groups if runtime_parquet else 0
        metrics.update({
            "row_group_count": row_groups,
            "row_group_size_setting": config.row_group_size,
            "covering_bbox_written": covering_bbox_written,
            "bbox_read_validated": bbox_validated,
            "full_fidelity_file_size": full_size,
            "runtime_file_size": runtime_size,
            "quarantine_file_size": quarantine_size,
            "runtime_size_reduction_bytes": runtime_reduction,
            "runtime_size_reduction_pct": runtime_reduction_pct,
            "elapsed_processing_seconds": round(time.perf_counter() - started, 3),
        })
        return CountyOutput(
            county=county,
            source_count=len(full),
            runtime_count=len(runtime),
            quarantine_count=len(quarantine),
            full_fidelity_path=portable_relative_path(full_destination, config.output_dir) if output_paths["full_fidelity"] else None,
            runtime_path=portable_relative_path(runtime_destination, config.output_dir) if output_paths["runtime"] else None,
            quarantine_path=portable_relative_path(quarantine_destination, config.output_dir) if output_paths["quarantine"] else None,
            full_fidelity_sha256=file_rows.get("full_fidelity", {}).get("sha256"),
            runtime_sha256=file_rows.get("runtime", {}).get("sha256"),
            quarantine_sha256=file_rows.get("quarantine", {}).get("sha256"),
            full_fidelity_size=full_size,
            runtime_size=runtime_size,
            quarantine_size=quarantine_size,
            covering_bbox_written=covering_bbox_written,
            bbox_read_validated=bbox_validated,
            validation_passed=True,
            validation_messages=messages,
            metrics=metrics,
        )
    finally:
        for temporary in temp_paths.values():
            temporary.unlink(missing_ok=True)
        del source, full, runtime, quarantine
        gc.collect()


def _coerce_report_value(value: Any) -> Any:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    if text == "":
        return None
    if text.lower() in {"true", "false"}:
        return text.lower() == "true"
    try:
        if re.fullmatch(r"[-+]?\d+", text):
            return int(text)
        if re.fullmatch(r"[-+]?(?:\d+\.\d*|\d*\.\d+)(?:[eE][-+]?\d+)?", text):
            return float(text)
    except ValueError:
        pass
    return value


def load_existing_reports(output_dir: Path, reports: ReportStore) -> None:
    """Load prior per-county report rows so resume preserves completed QA."""
    reports_dir = output_dir / "reports"
    if not reports_dir.exists():
        return
    filename_to_attribute = {
        "county_summary.csv": "county_summary",
        "field_completeness.csv": "field_completeness",
        "classification_values.csv": "classification_values",
        "record_role_summary.csv": "record_role_summary",
        "occupancy_summary.csv": "occupancy_summary",
        "parent_child_summary.csv": "parent_child_summary",
        "date_anomalies.csv": "date_anomalies",
        "coordinate_anomalies.csv": "coordinate_anomalies",
        "duplicate_address_summary.csv": "duplicate_address_summary",
        "postal_fallback_summary.csv": "postal_fallback_summary",
        "quarantine_summary.csv": "quarantine_summary",
        "classification_conflicts.csv": "classification_conflicts",
        "output_files.csv": "output_files",
    }
    for filename, attribute in filename_to_attribute.items():
        report_path = reports_dir / filename
        if not report_path.exists():
            continue
        try:
            frame = pd.read_csv(report_path, dtype=object)
        except pd.errors.EmptyDataError:
            continue
        rows = [
            {key: _coerce_report_value(value) for key, value in row.items()}
            for row in frame.to_dict("records")
        ]
        getattr(reports, attribute).extend(rows)


def remove_county_report_rows(reports: ReportStore, county: str) -> None:
    for attribute in (
        "county_summary", "field_completeness", "classification_values",
        "record_role_summary", "occupancy_summary", "parent_child_summary",
        "date_anomalies", "coordinate_anomalies", "duplicate_address_summary",
        "postal_fallback_summary", "quarantine_summary",
        "classification_conflicts", "output_files",
    ):
        rows = getattr(reports, attribute)
        setattr(reports, attribute, [row for row in rows if row.get("county") != county])


def load_existing_manifest(output_dir: Path) -> dict[str, dict[str, Any]]:
    path = output_dir / "manifest" / "coverage_manifest.json"
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("counties", []) if isinstance(payload, dict) else payload
    return {str(row.get("canonical_county")): row for row in rows if row.get("canonical_county")}


def existing_summary_metrics(reports: ReportStore, county: str) -> dict[str, Any]:
    for row in reports.county_summary:
        if row.get("county") == county:
            return {
                "source_record_count": int(row.get("processed_source_count") or 0),
                "runtime_record_count": int(row.get("runtime_count") or 0),
                "quarantine_record_count": int(row.get("quarantine_count") or 0),
                "reconciliation_passed": bool(row.get("reconciliation_passed")),
                "runtime_column_count": int(row.get("runtime_column_count") or len(RUNTIME_COLUMNS)),
                "full_fidelity_column_count": int(row.get("full_fidelity_column_count") or 0),
                "runtime_file_size": row.get("runtime_file_size_bytes"),
                "full_fidelity_file_size": row.get("full_fidelity_file_size_bytes"),
                "runtime_size_reduction_bytes": row.get("runtime_size_reduction_bytes"),
                "runtime_size_reduction_pct": row.get("runtime_size_reduction_pct"),
                "geometry_completeness": float(row.get("geometry_completeness") or 0),
                "nguid_completeness": float(row.get("nguid_completeness") or 0),
                "missing_nguid_count": int(row.get("missing_nguid_count") or 0),
                "duplicate_nguid_record_count": int(row.get("duplicate_nguid_record_count") or 0),
                "duplicate_source_record_id_count": int(row.get("duplicate_source_record_id_count") or 0),
                "street_completeness": float(row.get("street_completeness") or 0),
                "postal_city_completeness": float(row.get("postal_city_completeness") or 0),
                "zip_completeness": float(row.get("zip_completeness") or 0),
                "municipality_locality_completeness": float(row.get("municipality_locality_completeness") or 0),
                "parent_building_count": int(row.get("parent_count") or 0),
                "potential_child_count": int(row.get("child_count") or 0),
                "unknown_unit_count": int(row.get("unknown_unit_count") or 0),
                "unknown_classification_count": int(row.get("unknown_unit_count") or 0),
                "warning_count": int(row.get("warning_count") or 0),
                "technical_warning_count": int(row.get("technical_warning_count") or 0),
                "classification_conflict_count": int(
                    row.get("classification_conflict_count") or 0
                ),
                "subaddress_with_parcel_or_site_count": int(
                    row.get("subaddress_with_parcel_or_site_count") or 0
                ),
                "subaddress_with_utility_or_infrastructure_count": int(
                    row.get("subaddress_with_utility_or_infrastructure_count") or 0
                ),
                "subaddress_with_access_or_non_mailable_count": int(
                    row.get("subaddress_with_access_or_non_mailable_count") or 0
                ),
                "row_group_count": int(row.get("row_group_count") or 0),
                "covering_bbox_written": bool(row.get("covering_bbox_written")),
                "bbox_read_validated": bool(row.get("bbox_read_validated")),
                "elapsed_processing_seconds": float(row.get("elapsed_processing_seconds") or 0),
                "latest_plausible_dateupdate": row.get("latest_plausible_source_update_date"),
            }
    return {}


def validate_existing_county_output(
    county: str,
    config: PipelineConfig,
    metadata: SourceMetadata,
    expected_inventory_count: int,
    reports: ReportStore,
    existing_manifest: Mapping[str, Mapping[str, Any]],
) -> CountyOutput:
    """Validate existing files before resume skips or validate-only accepts them."""
    manifest_row = dict(existing_manifest.get(county, {}))
    full_relative = manifest_row.get("full_fidelity_relative_path") or f"full_fidelity/{county_slug(county)}.parquet"
    runtime_relative = manifest_row.get("runtime_relative_path") or f"runtime/{county_slug(county)}.parquet"
    quarantine_relative = manifest_row.get("quarantine_relative_path") or f"quarantine/{county_slug(county)}.parquet"
    full_path = config.output_dir / str(full_relative)
    runtime_path = config.output_dir / str(runtime_relative)
    quarantine_path = config.output_dir / str(quarantine_relative)

    if not config.runtime_only and not full_path.exists():
        raise ValidationError(f"Existing full-fidelity file is missing: {full_relative}")
    if not config.full_fidelity_only and not runtime_path.exists():
        raise ValidationError(f"Existing runtime file is missing: {runtime_relative}")

    full_count = pq.ParquetFile(full_path).metadata.num_rows if full_path.exists() else expected_inventory_count
    runtime_count = pq.ParquetFile(runtime_path).metadata.num_rows if runtime_path.exists() else 0
    quarantine_count = pq.ParquetFile(quarantine_path).metadata.num_rows if quarantine_path.exists() else 0
    if full_path.exists() and full_count != expected_inventory_count:
        raise ValidationError(f"Existing full-fidelity count {full_count:,} does not match inventory {expected_inventory_count:,}")
    if runtime_path.exists() and runtime_count + quarantine_count != expected_inventory_count:
        raise ValidationError(
            f"Existing reconciliation failed: {runtime_count:,} runtime + {quarantine_count:,} quarantine != {expected_inventory_count:,} inventory"
        )

    covering_bbox_written = bool(manifest_row.get("covering_bbox_written"))
    bbox_validated = False
    if runtime_path.exists():
        validate_runtime_arrow_schema(runtime_path)
        runtime = gpd.read_parquet(runtime_path)
        validate_geometry_frame(runtime, "Existing runtime output", require_wisconsin=True)
        record_ids = clean_series(runtime, "Source_Record_ID")
        if record_ids.eq("").any() or not record_ids.is_unique:
            raise ValidationError("Existing runtime Source_Record_ID is blank or non-unique")
        if runtime["Canonical_Critical_Failure_Flag"].fillna(False).astype(bool).any():
            raise ValidationError("Existing runtime contains critical-failure rows")
        if covering_bbox_written and len(runtime):
            sample = runtime.geometry.iloc[len(runtime) // 2]
            epsilon = 0.005
            bbox = (sample.x - epsilon, sample.y - epsilon, sample.x + epsilon, sample.y + epsilon)
            expected = int(runtime.geometry.covered_by(box(*bbox)).sum())
            actual = len(gpd.read_parquet(runtime_path, bbox=bbox))
            if expected != actual:
                raise ValidationError("Existing runtime bounding-box read failed validation")
            bbox_validated = True
        elif covering_bbox_written:
            bbox_validated = True
        del runtime
    if full_path.exists():
        full_schema = pq.ParquetFile(full_path).schema_arrow.names
        missing = [field for field in metadata.source_fields if field not in full_schema]
        if missing:
            raise ValidationError("Existing full-fidelity file is missing source fields: " + ", ".join(missing[:20]))
        full_identity = gpd.read_parquet(full_path, columns=["Source_Record_ID", "geometry"])
        if full_identity.crs is None or full_identity.crs.to_epsg() != 4326:
            raise ValidationError("Existing full-fidelity CRS is not EPSG:4326")
        ids = clean_series(full_identity, "Source_Record_ID")
        if ids.eq("").any() or not ids.is_unique:
            raise ValidationError("Existing full-fidelity Source_Record_ID is blank or non-unique")
        del full_identity
    if quarantine_path.exists():
        quarantine = gpd.read_parquet(quarantine_path)
        if not quarantine["Canonical_Critical_Failure_Flag"].fillna(False).astype(bool).all():
            raise ValidationError("Existing quarantine includes a non-critical row")
        del quarantine

    expected_hashes = {
        "full_fidelity": manifest_row.get("full_fidelity_sha256"),
        "runtime": manifest_row.get("runtime_sha256"),
        "quarantine": manifest_row.get("quarantine_sha256"),
    }
    actual_hashes = {
        "full_fidelity": sha256_file(full_path) if full_path.exists() else None,
        "runtime": sha256_file(runtime_path) if runtime_path.exists() else None,
        "quarantine": sha256_file(quarantine_path) if quarantine_path.exists() else None,
    }
    for output_type, expected_hash in expected_hashes.items():
        if expected_hash and actual_hashes[output_type] != expected_hash:
            raise ValidationError(f"Existing {output_type} SHA-256 does not match the manifest")

    metrics = existing_summary_metrics(reports, county)
    metrics.setdefault("source_record_count", expected_inventory_count)
    metrics.setdefault("runtime_record_count", runtime_count)
    metrics.setdefault("quarantine_record_count", quarantine_count)
    metrics.setdefault("reconciliation_passed", runtime_count + quarantine_count == expected_inventory_count)
    metrics.setdefault("runtime_column_count", len(RUNTIME_COLUMNS) if runtime_path.exists() else 0)
    metrics.setdefault("full_fidelity_column_count", len(pq.ParquetFile(full_path).schema_arrow.names) if full_path.exists() else 0)
    metrics.setdefault("runtime_file_size", runtime_path.stat().st_size if runtime_path.exists() else None)
    metrics.setdefault("full_fidelity_file_size", full_path.stat().st_size if full_path.exists() else None)
    metrics.setdefault("quarantine_file_size", quarantine_path.stat().st_size if quarantine_path.exists() else None)
    metrics["covering_bbox_written"] = covering_bbox_written
    metrics["bbox_read_validated"] = bbox_validated
    metrics["row_group_count"] = pq.ParquetFile(runtime_path if runtime_path.exists() else full_path).metadata.num_row_groups
    return CountyOutput(
        county=county,
        source_count=expected_inventory_count,
        runtime_count=runtime_count,
        quarantine_count=quarantine_count,
        full_fidelity_path=portable_relative_path(full_path, config.output_dir) if full_path.exists() else None,
        runtime_path=portable_relative_path(runtime_path, config.output_dir) if runtime_path.exists() else None,
        quarantine_path=portable_relative_path(quarantine_path, config.output_dir) if quarantine_path.exists() else None,
        full_fidelity_sha256=actual_hashes["full_fidelity"],
        runtime_sha256=actual_hashes["runtime"],
        quarantine_sha256=actual_hashes["quarantine"],
        full_fidelity_size=full_path.stat().st_size if full_path.exists() else None,
        runtime_size=runtime_path.stat().st_size if runtime_path.exists() else None,
        quarantine_size=quarantine_path.stat().st_size if quarantine_path.exists() else None,
        covering_bbox_written=covering_bbox_written,
        bbox_read_validated=bbox_validated,
        validation_passed=True,
        validation_messages=["Existing county outputs passed resume/validate-only structural, hash, schema, identity, geometry, and reconciliation checks"],
        metrics=metrics,
        skipped_by_resume=True,
    )


def derive_readiness(
    county: str,
    output: CountyOutput | None,
    represented: bool,
    requested: bool,
    failed: bool,
) -> dict[str, Any]:
    """Separate current-run execution state from durable county readiness."""
    override = COUNTY_STATUS_OVERRIDES.get(county)

    if not represented:
        return {
            "technical_validation_status": "not_present_in_source",
            "coverage_readiness_status": "not_present_in_source",
            "production_source_status": (
                str(override["production_source_status"])
                if override else "unavailable"
            ),
            "public_availability_status": (
                str(override["public_availability_status"])
                if override else "not_present_in_source"
            ),
            "status_reason": (
                str(override["reason"])
                if override
                else "County is absent from the statewide source snapshot."
            ),
            "run_processing_status": "not_present_in_source",
            "run_processing_reason": (
                "County could not be processed because it is absent from the "
                "statewide source snapshot."
            ),
            "recommended_source": (
                override.get("recommended_source") if override else None
            ),
            "spatial_readiness": False,
            "address_readiness": False,
            "postal_readiness": False,
            "occupancy_classification_readiness": False,
        }

    if not requested:
        if override:
            coverage_status = str(override["coverage_readiness_status"])
            production_status = str(override["production_source_status"])
            public_status = str(override["public_availability_status"])
            durable_reason = str(override["reason"])
            recommended_source = override.get("recommended_source")
        else:
            coverage_status = "needs_validation"
            production_status = "not_processed"
            public_status = "needs_validation"
            durable_reason = (
                "Represented in the statewide source but not independently "
                "validated."
            )
            recommended_source = None
        return {
            "technical_validation_status": "not_processed",
            "coverage_readiness_status": coverage_status,
            "production_source_status": production_status,
            "public_availability_status": public_status,
            "status_reason": durable_reason,
            "run_processing_status": "not_processed",
            "run_processing_reason": (
                "County was represented in the source but was not selected in "
                "this run."
            ),
            "recommended_source": recommended_source,
            "spatial_readiness": False,
            "address_readiness": False,
            "postal_readiness": False,
            "occupancy_classification_readiness": False,
        }

    if failed or output is None or not output.validation_passed:
        return {
            "technical_validation_status": "failed_validation",
            "coverage_readiness_status": (
                str(override["coverage_readiness_status"])
                if override else "needs_validation"
            ),
            "production_source_status": (
                str(override["production_source_status"])
                if override else "unavailable"
            ),
            "public_availability_status": (
                str(override["public_availability_status"])
                if override else "failed_validation"
            ),
            "status_reason": (
                str(override["reason"])
                if override
                else "County coverage has not been independently validated."
            ),
            "run_processing_status": "failed_validation",
            "run_processing_reason": (
                "County processing or critical validation failed in this run."
            ),
            "recommended_source": (
                override.get("recommended_source") if override else None
            ),
            "spatial_readiness": False,
            "address_readiness": False,
            "postal_readiness": False,
            "occupancy_classification_readiness": False,
        }

    metrics = output.metrics
    technical_warning_count = int(
        metrics.get("technical_warning_count") or 0
    )
    technical_status = (
        "validated_with_warnings"
        if technical_warning_count
        else "validated"
    )
    spatial = (
        bool(output.runtime_path)
        and output.bbox_read_validated
        and output.quarantine_count == 0
    )
    address = (
        float(metrics.get("street_completeness") or 0) >= 0.99
        and bool(metrics.get("source_record_id_unique", True))
    )
    postal = (
        float(metrics.get("zip_completeness") or 0) >= 0.90
        and (
            float(metrics.get("postal_city_completeness") or 0) >= 0.90
            or float(
                metrics.get("municipality_locality_completeness") or 0
            ) >= 0.98
        )
    )
    unknown_ratio = (
        int(
            metrics.get("unknown_unit_count")
            or metrics.get("unknown_classification_count")
            or 0
        )
        / max(1, output.source_count)
    )
    occupancy = unknown_ratio <= 0.20

    if override:
        coverage_status = str(override["coverage_readiness_status"])
        production_status = str(override["production_source_status"])
        public_status = str(override["public_availability_status"])
        reason = str(override["reason"])
        recommended_source = override.get("recommended_source")
    elif not output.runtime_path:
        coverage_status = "needs_validation"
        production_status = "not_ready"
        public_status = "needs_validation"
        reason = (
            "Full-fidelity output validated, but no runtime file was generated "
            "for production."
        )
        recommended_source = None
    else:
        coverage_status = "needs_validation"
        production_status = "statewide_runtime_candidate"
        public_status = "needs_validation"
        reason = (
            "Technical validation passed; countywide coverage still requires "
            "independent approval."
        )
        recommended_source = (
            "Wisconsin statewide NG911 runtime after approval"
        )

    return {
        "technical_validation_status": technical_status,
        "coverage_readiness_status": coverage_status,
        "production_source_status": production_status,
        "public_availability_status": public_status,
        "status_reason": reason,
        "run_processing_status": (
            "skipped_by_resume"
            if output.skipped_by_resume
            else "processed_successfully"
        ),
        "run_processing_reason": (
            "Existing county outputs passed validation and were skipped by "
            "resume."
            if output.skipped_by_resume
            else "County was processed and validated successfully in this run."
        ),
        "recommended_source": recommended_source,
        "spatial_readiness": spatial,
        "address_readiness": address,
        "postal_readiness": postal,
        "occupancy_classification_readiness": occupancy,
    }

def build_manifest(
    metadata: SourceMetadata,
    config: PipelineConfig,
    county_variants: Mapping[str, Sequence[str]],
    inventory_counts: Mapping[str, int],
    outcomes: Mapping[str, CountyOutput],
    failed_counties: set[str],
    requested_counties: Sequence[str],
) -> list[dict[str, Any]]:
    requested_set = set(requested_counties)
    rows: list[dict[str, Any]] = []
    for county in WI_COUNTIES:
        represented = bool(county_variants.get(county))
        output = outcomes.get(county)
        requested = county in requested_set
        readiness = derive_readiness(county, output, represented, requested, county in failed_counties)
        row: dict[str, Any] = {
            "canonical_county": county,
            "county_key": county_slug(county),
            "source_system": SOURCE_SYSTEM,
            "source_version": metadata.source_timestamp[:10] if metadata.source_timestamp else config.as_of_date.isoformat(),
            "source_hash": metadata.source_sha256,
            "statewide_inventory_count": int(inventory_counts.get(county, 0)),
            "processed_source_count": output.source_count if output else 0,
            "runtime_count": output.runtime_count if output else 0,
            "quarantine_count": output.quarantine_count if output else 0,
            "requested_in_run": requested,
            "processed_in_run": output is not None,
            "skipped_by_resume": output.skipped_by_resume if output else False,
            **readiness,
            "validation_date": date.today().isoformat() if output else None,
            "runtime_relative_path": output.runtime_path if output else None,
            "runtime_byte_size": output.runtime_size if output else None,
            "runtime_sha256": output.runtime_sha256 if output else None,
            "full_fidelity_relative_path": output.full_fidelity_path if output else None,
            "full_fidelity_byte_size": output.full_fidelity_size if output else None,
            "full_fidelity_sha256": output.full_fidelity_sha256 if output else None,
            "quarantine_relative_path": output.quarantine_path if output else None,
            "quarantine_byte_size": output.quarantine_size if output else None,
            "quarantine_sha256": output.quarantine_sha256 if output else None,
            "latest_plausible_source_update_date": output.metrics.get("latest_plausible_dateupdate") if output else None,
            "quality_score_summary": quality_score(output.metrics) if output else None,
            "classification_conflict_count": (
                int(output.metrics.get("classification_conflict_count") or 0)
                if output else 0
            ),
            "subaddress_with_parcel_or_site_count": (
                int(output.metrics.get("subaddress_with_parcel_or_site_count") or 0)
                if output else 0
            ),
            "subaddress_with_utility_or_infrastructure_count": (
                int(
                    output.metrics.get(
                        "subaddress_with_utility_or_infrastructure_count"
                    ) or 0
                )
                if output else 0
            ),
            "subaddress_with_access_or_non_mailable_count": (
                int(
                    output.metrics.get(
                        "subaddress_with_access_or_non_mailable_count"
                    ) or 0
                )
                if output else 0
            ),
            "covering_bbox_written": output.covering_bbox_written if output else False,
            "bbox_read_validated": output.bbox_read_validated if output else False,
            "validation_passed": output.validation_passed if output else False,
            "pipeline_version": PIPELINE_VERSION,
            "runtime_schema_version": RUNTIME_SCHEMA_VERSION,
            "full_fidelity_schema_version": FULL_FIDELITY_SCHEMA_VERSION,
            "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
            "classification_rule_version": CLASSIFICATION_RULE_VERSION,
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
        for field_name in ("statewide_inventory_count", "runtime_count", "quarantine_count"):
            old_value = int(old.get(field_name) or old.get("source_record_count") or 0)
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
                "change_pct": None,
                "warning": "Current source appears older than previous release",
            })
    return warnings


def validate_portable_manifest_rows(rows: Sequence[Mapping[str, Any]]) -> None:
    path_fields = ("runtime_relative_path", "full_fidelity_relative_path", "quarantine_relative_path")
    for row in rows:
        for field_name in path_fields:
            value = row.get(field_name)
            if not value:
                continue
            text = str(value)
            if Path(text).is_absolute() or re.match(r"^[A-Za-z]:", text) or "/workspaces/" in text.lower():
                raise ValidationError(f"Manifest contains a nonportable path in {field_name}: {text}")


def write_reports(
    output_dir: Path,
    reports: ReportStore,
    metadata: SourceMetadata,
    config: PipelineConfig,
    manifest_rows: list[dict[str, Any]],
    outcomes: Mapping[str, CountyOutput],
    previous_warnings: list[dict[str, Any]],
    inventory_counts: Mapping[str, int],
    requested_counties: Sequence[str],
    run_started_utc: str,
    run_ended_utc: str,
    elapsed_seconds: float,
) -> None:
    reports_dir = ensure_directory(output_dir / "reports")
    report_map: dict[str, Sequence[Mapping[str, Any]]] = {
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
        "classification_conflicts.csv": reports.classification_conflicts,
        "output_files.csv": reports.output_files,
        "previous_release_warnings.csv": previous_warnings,
        "failures.csv": reports.failures,
    }
    for filename, rows in report_map.items():
        atomic_write_csv(reports_dir / filename, rows, REPORT_SCHEMAS[filename])

    failed_counties = sorted({str(row.get("county")) for row in reports.failures if row.get("county")})
    successful_counties = sorted(county for county, output in outcomes.items() if output.validation_passed)
    skipped_counties = sorted(county for county, output in outcomes.items() if output.skipped_by_resume)
    available_counties = [county for county in WI_COUNTIES if inventory_counts.get(county, 0)]
    not_processed = [county for county in available_counties if county not in set(requested_counties)]
    run_summary = {
        "pipeline_version": PIPELINE_VERSION,
        "runtime_schema_version": RUNTIME_SCHEMA_VERSION,
        "full_fidelity_schema_version": FULL_FIDELITY_SCHEMA_VERSION,
        "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
        "classification_rule_version": CLASSIFICATION_RULE_VERSION,
        "source_zip_filename": config.input_path.name,
        "source_layer": metadata.layer_name,
        "source_sha256": metadata.source_sha256,
        "source_as_of_date": metadata.source_timestamp[:10] if metadata.source_timestamp else config.as_of_date.isoformat(),
        "as_of_date": config.as_of_date.isoformat(),
        "as_of_date_source": config.as_of_date_source,
        "run_scope": "statewide" if config.counties is None else "selective",
        "run_started_utc": run_started_utc,
        "run_ended_utc": run_ended_utc,
        "elapsed_seconds": round(elapsed_seconds, 3),
        "all_counties_available_in_source": available_counties,
        "requested_counties": list(requested_counties),
        "successful_counties": successful_counties,
        "failed_counties": failed_counties,
        "skipped_counties": skipped_counties,
        "not_processed_counties": not_processed,
        "total_source_records_processed": sum(output.source_count for output in outcomes.values()),
        "total_runtime_records": sum(output.runtime_count for output in outcomes.values()),
        "total_quarantined_records": sum(output.quarantine_count for output in outcomes.values()),
        "total_classification_conflicts": sum(
            int(output.metrics.get("classification_conflict_count") or 0)
            for output in outcomes.values()
        ),
        "total_subaddress_with_parcel_or_site": sum(
            int(
                output.metrics.get(
                    "subaddress_with_parcel_or_site_count"
                ) or 0
            )
            for output in outcomes.values()
        ),
        "total_subaddress_with_utility_or_infrastructure": sum(
            int(
                output.metrics.get(
                    "subaddress_with_utility_or_infrastructure_count"
                ) or 0
            )
            for output in outcomes.values()
        ),
        "total_subaddress_with_access_or_non_mailable": sum(
            int(
                output.metrics.get(
                    "subaddress_with_access_or_non_mailable_count"
                ) or 0
            )
            for output in outcomes.values()
        ),
        "selective_or_statewide_run": "statewide" if config.counties is None else "selective",
        "resume_mode": config.resume,
        "validate_only_mode": config.validate_only,
        "failures": reports.failures,
        "non_destructive_policy": {
            "address_deduplication": False,
            "coordinate_deduplication": False,
            "parent_record_deletion": False,
            "generic_unit_assumed_apartment": False,
            "source_lat_long_used_for_geometry": False,
        },
    }
    atomic_write_json(reports_dir / "run_summary.json", run_summary)

    validate_portable_manifest_rows(manifest_rows)
    manifest_dir = ensure_directory(output_dir / "manifest")
    manifest_payload = {
        "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
        "pipeline_version": PIPELINE_VERSION,
        "runtime_schema_version": RUNTIME_SCHEMA_VERSION,
        "full_fidelity_schema_version": FULL_FIDELITY_SCHEMA_VERSION,
        "classification_rule_version": CLASSIFICATION_RULE_VERSION,
        "generated_at_utc": run_ended_utc,
        "run_scope": run_summary["run_scope"],
        "run_started_utc": run_started_utc,
        "run_ended_utc": run_ended_utc,
        "elapsed_seconds": round(elapsed_seconds, 3),
        "source_hash": metadata.source_sha256,
        "source_version": metadata.source_timestamp[:10] if metadata.source_timestamp else config.as_of_date.isoformat(),
        "all_counties_available_in_source": available_counties,
        "counties_requested": list(requested_counties),
        "counties_processed_successfully": successful_counties,
        "counties_failed": failed_counties,
        "counties_skipped": skipped_counties,
        "counties_not_processed": not_processed,
        "counties": manifest_rows,
    }
    atomic_write_json(manifest_dir / "coverage_manifest.json", manifest_payload)
    atomic_write_csv(manifest_dir / "coverage_manifest.csv", manifest_rows, MANIFEST_COLUMNS)

    compatibility_notes = {
        "runtime_schema_version": RUNTIME_SCHEMA_VERSION,
        "classification_rule_version": CLASSIFICATION_RULE_VERSION,
        "current_analyzer_compatible_columns_included": list(CURRENT_ANALYZER_COLUMNS),
        "runtime_field_count": len(RUNTIME_COLUMNS),
        "targeted_analyzer_changes_still_recommended": [
            "Load county GeoParquet using gpd.read_parquet(..., bbox=...) rather than gpd.read_file.",
            "Prefer Canonical_Full_House_Number and Canonical_Full_Street rather than rebuilding simplified addresses.",
            "Use Canonical_Subaddress; do not prefix every untyped unit with Apt.",
            "Use Canonical_ZIP4 and Canonical_Postal_City distinctly from municipality.",
            "Use Canonical_Analyzer_Handling, Canonical_Exclusion_Category, and Canonical_Analyzer_Eligible for operational filtering.",
            "Use Potential_Parent_Record, Potential_Child_Record, and Potential_Double_Count_Flag to avoid parent-building double counting.",
            "Use Canonical_Occupancy_Category and Canonical_Occupancy_Reason rather than treating every repeated unit as an apartment.",
            "Keep Milwaukee on the county-specific source until the statewide City of Milwaukee coverage gap is resolved.",
        ],
    }
    atomic_write_json(output_dir / "source_metadata" / "analyzer_compatibility_notes.json", compatibility_notes)


def add_county_summary_row(
    reports: ReportStore,
    output: CountyOutput,
    statewide_inventory_count: int,
) -> None:
    metrics = output.metrics
    readiness = derive_readiness(output.county, output, True, True, False)
    row = {
        "county": output.county,
        "county_key": county_slug(output.county),
        "statewide_inventory_count": statewide_inventory_count,
        "processed_source_count": output.source_count,
        "runtime_count": output.runtime_count,
        "quarantine_count": output.quarantine_count,
        "reconciliation_passed": output.source_count == output.runtime_count + output.quarantine_count,
        "runtime_column_count": metrics.get("runtime_column_count", len(RUNTIME_COLUMNS)),
        "full_fidelity_column_count": metrics.get("full_fidelity_column_count"),
        "runtime_file_size_bytes": output.runtime_size,
        "full_fidelity_file_size_bytes": output.full_fidelity_size,
        "runtime_size_reduction_bytes": metrics.get("runtime_size_reduction_bytes"),
        "runtime_size_reduction_pct": metrics.get("runtime_size_reduction_pct"),
        "geometry_completeness": metrics.get("geometry_completeness"),
        "nguid_completeness": metrics.get("nguid_completeness"),
        "missing_nguid_count": metrics.get("missing_nguid_count"),
        "duplicate_nguid_record_count": metrics.get("duplicate_nguid_record_count"),
        "duplicate_source_record_id_count": metrics.get("duplicate_source_record_id_count"),
        "street_completeness": metrics.get("street_completeness"),
        "postal_city_completeness": metrics.get("postal_city_completeness"),
        "zip_completeness": metrics.get("zip_completeness"),
        "municipality_locality_completeness": metrics.get("municipality_locality_completeness"),
        "parent_count": metrics.get("parent_building_count"),
        "child_count": metrics.get("potential_child_count"),
        "unknown_unit_count": metrics.get("unknown_unit_count"),
        "warning_count": metrics.get("warning_count"),
        "technical_warning_count": metrics.get("technical_warning_count"),
        "classification_conflict_count": metrics.get(
            "classification_conflict_count"
        ),
        "subaddress_with_parcel_or_site_count": metrics.get(
            "subaddress_with_parcel_or_site_count"
        ),
        "subaddress_with_utility_or_infrastructure_count": metrics.get(
            "subaddress_with_utility_or_infrastructure_count"
        ),
        "subaddress_with_access_or_non_mailable_count": metrics.get(
            "subaddress_with_access_or_non_mailable_count"
        ),
        "row_group_count": metrics.get("row_group_count"),
        "covering_bbox_written": output.covering_bbox_written,
        "bbox_read_validated": output.bbox_read_validated,
        "validation_status": readiness["technical_validation_status"],
        "coverage_readiness_status": readiness["coverage_readiness_status"],
        "production_source_status": readiness["production_source_status"],
        "readiness_reason": readiness["status_reason"],
        "elapsed_processing_seconds": metrics.get("elapsed_processing_seconds"),
        "latest_plausible_source_update_date": metrics.get("latest_plausible_dateupdate"),
        "full_fidelity_sha256": output.full_fidelity_sha256,
        "runtime_sha256": output.runtime_sha256,
        "quarantine_sha256": output.quarantine_sha256,
        "validation_messages": " | ".join(output.validation_messages),
    }
    reports.county_summary.append(row)


def record_failure(
    reports: ReportStore,
    county: str,
    stage: str,
    exc: Exception,
) -> None:
    traceback_text = traceback.format_exc()
    reports.failures.append({
        "county": county,
        "processing_stage": stage,
        "exception_type": type(exc).__name__,
        "exception_message": str(exc),
        "timestamp_utc": utc_now_iso(),
        "traceback_reference": traceback_text[-4_000:],
    })

def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    run_started_utc = utc_now_iso()
    run_started_perf = time.perf_counter()
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
            resume=args.resume,
            validate_only=args.validate_only,
            schema_report=args.schema_report,
            runtime_only=args.runtime_only,
            full_fidelity_only=args.full_fidelity_only,
            keep_extracted_gdb=args.keep_extracted_gdb,
            row_group_size=args.row_group_size,
            previous_manifest=args.previous_manifest.expanduser().resolve() if args.previous_manifest else None,
            fail_fast=args.fail_fast,
        )
        LOGGER.info(
            "Starting Wisconsin NG911 pipeline v%s (runtime schema %s)",
            PIPELINE_VERSION,
            RUNTIME_SCHEMA_VERSION,
        )
        LOGGER.info("As-of date: %s (%s)", config.as_of_date, config.as_of_date_source)
        gdb_path, temporary_owner, input_kind = extract_or_locate_gdb(
            input_path, output_dir, config.keep_extracted_gdb
        )
        try:
            metadata = inspect_source(input_path, gdb_path, input_kind)
            write_source_metadata(output_dir, metadata)
            write_schema_documentation(output_dir, metadata)
            LOGGER.info(
                "Source layer %s: %s Point records, CRS %s",
                metadata.layer_name,
                f"{metadata.feature_count:,}",
                metadata.crs,
            )
            reports = ReportStore()
            inventory, variants, inventory_counts, duplicate_nguids = read_inventory(
                gdb_path, metadata.feature_count, reports
            )
            del inventory
            gc.collect()

            if config.resume or config.validate_only:
                load_existing_reports(output_dir, reports)
            existing_manifest = load_existing_manifest(output_dir)
            adapter = WisconsinStatewideNG911Adapter(
                gdb_path, metadata, config, variants, duplicate_nguids
            )
            counties_to_process = config.counties or tuple(
                county for county in WI_COUNTIES if variants[county]
            )
            requested_for_manifest = tuple() if config.schema_report else tuple(counties_to_process)
            outcomes: dict[str, CountyOutput] = {}
            failed_counties: set[str] = set()
            skipped_counties: set[str] = set()

            if not config.schema_report:
                total_counties = len(counties_to_process)
                for position, county in enumerate(counties_to_process, start=1):
                    total_elapsed = time.perf_counter() - run_started_perf
                    LOGGER.info(
                        "[%s/%s] Starting %s County (total elapsed %.1fs)",
                        position,
                        total_counties,
                        county,
                        total_elapsed,
                    )
                    if not variants[county]:
                        LOGGER.warning("%s County is absent from the statewide source; no county files were generated", county)
                        continue

                    reprocess_invalid_resume = False
                    if config.resume or config.validate_only:
                        try:
                            existing_output = validate_existing_county_output(
                                county,
                                config,
                                metadata,
                                inventory_counts[county],
                                reports,
                                existing_manifest,
                            )
                            outcomes[county] = existing_output
                            skipped_counties.add(county)
                            if not any(row.get("county") == county for row in reports.county_summary):
                                add_county_summary_row(reports, existing_output, inventory_counts[county])
                            LOGGER.info(
                                "%s County existing outputs validated; county processing skipped",
                                county,
                            )
                            continue
                        except Exception as exc:
                            if config.validate_only:
                                LOGGER.exception("%s County existing-output validation failed: %s", county, exc)
                                record_failure(reports, county, "validate_existing_outputs", exc)
                                failed_counties.add(county)
                                if config.fail_fast:
                                    raise
                                continue
                            LOGGER.warning(
                                "%s County existing outputs are missing or invalid and will be rebuilt: %s",
                                county,
                                exc,
                            )
                            remove_county_report_rows(reports, county)
                            reprocess_invalid_resume = True

                    try:
                        output = process_county(
                            county,
                            adapter,
                            config,
                            reports,
                            inventory_counts[county],
                            overwrite_existing=reprocess_invalid_resume,
                        )
                        outcomes[county] = output
                        add_county_summary_row(reports, output, inventory_counts[county])
                        if output.metrics.get("_full_fidelity_columns"):
                            write_schema_documentation(
                                output_dir,
                                metadata,
                                output.metrics["_full_fidelity_columns"],
                            )
                        LOGGER.info(
                            "%s complete: source=%s runtime=%s quarantine=%s runtime_reduction=%s%% elapsed=%.1fs",
                            county,
                            f"{output.source_count:,}",
                            f"{output.runtime_count:,}",
                            f"{output.quarantine_count:,}",
                            output.metrics.get("runtime_size_reduction_pct"),
                            float(output.metrics.get("elapsed_processing_seconds") or 0),
                        )
                    except Exception as exc:  # county-isolated failure by design
                        LOGGER.exception("%s County failed: %s", county, exc)
                        record_failure(reports, county, "process_county", exc)
                        failed_counties.add(county)
                        if config.fail_fast:
                            raise
                    finally:
                        remaining = total_counties - position
                        LOGGER.info(
                            "Progress: successes=%s failures=%s skipped=%s remaining=%s total_elapsed=%.1fs",
                            len(outcomes),
                            len(failed_counties),
                            len(skipped_counties),
                            remaining,
                            time.perf_counter() - run_started_perf,
                        )
                        gc.collect()
            else:
                LOGGER.info("Schema-report mode: documentation generated; county processing skipped")

            manifest_rows = build_manifest(
                metadata,
                config,
                variants,
                inventory_counts,
                outcomes,
                failed_counties,
                requested_for_manifest,
            )
            previous_warnings = (
                compare_previous_manifest(manifest_rows, config.previous_manifest)
                if config.previous_manifest else []
            )
            run_ended_utc = utc_now_iso()
            elapsed_seconds = time.perf_counter() - run_started_perf
            write_reports(
                output_dir,
                reports,
                metadata,
                config,
                manifest_rows,
                outcomes,
                previous_warnings,
                inventory_counts,
                requested_for_manifest,
                run_started_utc,
                run_ended_utc,
                elapsed_seconds,
            )
            failed = bool(reports.failures)
            LOGGER.info(
                "Pipeline complete: %s successful/validated county file set(s), %s failure(s), %s resume skip(s), elapsed %.1fs",
                len(outcomes),
                len(reports.failures),
                len(skipped_counties),
                elapsed_seconds,
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