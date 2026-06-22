"""Smart Parquet Lake Distinct Extractor.

A Windows desktop application for extracting distinct values from one column in
a Hive-style partitioned Parquet lake:

    source=<stream|fast>/year=<YYYY>/month=<MM>/day=<DD>/*.parquet

The app intentionally reads partition dates from folder names, scans Parquet
schemas from metadata, and reads only the selected column in batches. A
temporary SQLite database is used as the distinct-value store so large exports
do not require an unlimited in-memory Python set.
"""

from __future__ import annotations

import csv
import json
import logging
import math
import os
import queue
import re
import shutil
import sqlite3
import tempfile
import threading
import time
import traceback
from dataclasses import dataclass, field
from datetime import date, datetime, time as datetime_time
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Iterable

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError as exc:
    root = tk.Tk()
    root.withdraw()
    messagebox.showerror(
        "Missing requirement",
        "pyarrow is not installed.\n\nInstall requirements with:\n"
        "python -m pip install pyarrow pandas",
    )
    root.destroy()
    raise SystemExit(1) from exc


APP_TITLE = "Smart Parquet Lake Distinct Extractor"
DEFAULT_LAKE_ROOT = Path(r"D:\Veto Logs Backup\Vs - Code Work\ETL\data\lake")
SUPPORTED_SOURCES = ("stream", "fast")
SOURCE_LABELS = {"stream": "Stream", "fast": "Fast"}
PARTITION_KEYS = {"source", "year", "month", "day"}
BATCH_SIZE = 100_000
SQLITE_INSERT_CHUNK = 10_000
SETTINGS_DIR_NAME = "SmartParquetLakeDistinctExtractor"
SETTINGS_FILE_NAME = "settings.json"
LOG_FILE_NAME = "smart_parquet_lake_distinct_extractor.log"

MONTH_NAMES = (
    "",
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)
MONTH_NAME_TO_NUMBER = {name: number for number, name in enumerate(MONTH_NAMES) if number}

TEMP_DIR_NAMES = {
    "tmp",
    "temp",
    "_temp",
    "_temporary",
    "temporary",
    ".tmp",
    ".temp",
    "_duckdb_tmp",
    "__pycache__",
}


def app_data_dir() -> Path:
    """Return a user-writable folder for settings and logs."""
    appdata = os.getenv("APPDATA")
    if appdata:
        folder = Path(appdata) / SETTINGS_DIR_NAME
    else:
        folder = Path(__file__).resolve().parent / SETTINGS_DIR_NAME
    folder.mkdir(parents=True, exist_ok=True)
    return folder


APP_DATA_DIR = app_data_dir()
SETTINGS_PATH = APP_DATA_DIR / SETTINGS_FILE_NAME
LOG_PATH = APP_DATA_DIR / LOG_FILE_NAME

logging.basicConfig(
    filename=str(LOG_PATH),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


class OperationCancelled(Exception):
    """Raised internally when the user cancels a running worker."""


class FriendlyError(Exception):
    """Raised for concise GUI-facing errors."""


@dataclass
class WorkerMessage:
    """Thread-safe message passed from a worker thread to the Tkinter thread."""

    kind: str
    text: str = ""
    payload: Any = None
    progress: float | None = None


@dataclass
class AppSettings:
    """Small JSON-backed settings model."""

    last_lake_root: str = str(DEFAULT_LAKE_ROOT)
    last_export_folder: str = ""
    last_source: str = "both"
    last_period_type: str = "Specific Day"
    geometry: str = "1100x800"

    @classmethod
    def load(cls) -> "AppSettings":
        try:
            if not SETTINGS_PATH.exists():
                return cls()
            raw = json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                return cls()
            settings = cls()
            settings.last_lake_root = str(raw.get("last_lake_root") or settings.last_lake_root)
            settings.last_export_folder = str(raw.get("last_export_folder") or "")
            settings.last_source = str(raw.get("last_source") or "both")
            settings.last_period_type = str(raw.get("last_period_type") or "Specific Day")
            settings.geometry = str(raw.get("geometry") or settings.geometry)
            return settings
        except Exception:
            logging.exception("Unable to load settings")
            return cls()

    def save(self) -> None:
        try:
            SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
            SETTINGS_PATH.write_text(
                json.dumps(self.__dict__, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception:
            logging.exception("Unable to save settings")


@dataclass
class LakeFileRecord:
    """One valid Parquet file discovered in the partitioned lake."""

    path: Path
    source: str
    year: int
    month: int
    day: int
    partition_date: date
    size: int
    modified_time: float
    row_count: int | None = None
    row_groups: int | None = None

    @property
    def stable_key(self) -> tuple[str, int, float]:
        return (str(self.path), self.size, self.modified_time)


@dataclass
class SkippedFile:
    """A skipped or unreadable file plus the reason shown to the user."""

    path: str
    reason: str


@dataclass
class PartitionScanResult:
    records: list[LakeFileRecord]
    skipped: list[SkippedFile]
    total_parquet_files: int
    elapsed_seconds: float


@dataclass
class SchemaColumn:
    exact_name: str
    normalized_name: str
    data_type: str


@dataclass
class FileSchemaMetadata:
    """Cached metadata for one Parquet file."""

    stable_key: tuple[str, int, float]
    row_count: int
    row_groups: int
    columns: list[SchemaColumn]


@dataclass
class ColumnChoice:
    """A selectable column row in the UI."""

    item_id: str
    normalized_name: str
    exact_name: str | None
    display_name: str
    header_name: str
    file_count: int
    stream_files: int
    fast_files: int
    data_types: str
    available_in: str
    ambiguous: bool = False


@dataclass
class SchemaScanResult:
    column_choices: list[ColumnChoice]
    cache: dict[str, FileSchemaMetadata]
    inspected_files: int
    unreadable_files: int
    matching_files: int
    estimated_rows: int
    skipped: list[SkippedFile]
    elapsed_seconds: float


@dataclass
class ExportSummary:
    output_path: Path | None
    source_selection: str
    period_type: str
    period_label: str
    selected_column: str
    matching_files: int
    files_processed: int
    missing_column_files: int
    unreadable_files: int
    empty_files: int
    rows_examined: int
    null_values: int
    blank_values: int
    duplicates_ignored: int
    distinct_exported: int
    skipped: list[SkippedFile]
    elapsed_seconds: float
    cancelled: bool = False


def write_log_error(context: str, exc: BaseException | None = None) -> None:
    """Write detailed diagnostics to the log without showing a traceback in UI."""
    if exc is None:
        logging.error(context)
    else:
        logging.error("%s\n%s", context, "".join(traceback.format_exception(exc)))


def timestamp() -> str:
    return datetime.now().strftime("%H:%M:%S")


def is_temp_or_hidden_path(path: Path) -> bool:
    """Skip common temporary and hidden paths during recursive discovery."""
    for part in path.parts:
        name = part.strip()
        low = name.lower()
        if not name:
            continue
        if name.startswith("."):
            return True
        if low in TEMP_DIR_NAMES or low.startswith("_temporary"):
            return True
    return False


def parse_partition_path(file_path: Path, lake_root: Path) -> tuple[LakeFileRecord | None, str | None]:
    """Parse source/year/month/day from Hive-style path components by name."""
    try:
        relative = file_path.relative_to(lake_root)
    except ValueError:
        return None, "file is not under the selected lake root"

    partitions: dict[str, str] = {}
    for part in relative.parts[:-1]:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        key = key.strip().lower()
        value = value.strip()
        if key in PARTITION_KEYS:
            partitions[key] = value

    source = partitions.get("source", "").strip().lower()
    if source not in SUPPORTED_SOURCES:
        return None, "missing or unsupported source partition"

    try:
        year = int(partitions.get("year", "").strip())
        month = int(partitions.get("month", "").strip())
        day = int(partitions.get("day", "").strip())
    except ValueError:
        return None, "invalid year/month/day partition value"

    try:
        partition_date = date(year, month, day)
    except ValueError:
        return None, "invalid date partition"

    try:
        stat = file_path.stat()
    except OSError as exc:
        return None, f"cannot read file metadata: {exc}"

    return (
        LakeFileRecord(
            path=file_path,
            source=source,
            year=year,
            month=month,
            day=day,
            partition_date=partition_date,
            size=stat.st_size,
            modified_time=stat.st_mtime,
        ),
        None,
    )


def source_label(source_selection: str) -> str:
    if source_selection == "both":
        return "Both"
    return SOURCE_LABELS.get(source_selection, source_selection.title())


def selected_sources(source_selection: str) -> set[str]:
    if source_selection == "both":
        return set(SUPPORTED_SOURCES)
    return {source_selection.lower()}


def format_bytes(value: int | float) -> str:
    size = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:,.1f} {unit}" if unit != "B" else f"{int(size):,} B"
        size /= 1024
    return f"{size:,.1f} TB"


def elapsed_text(seconds: float) -> str:
    seconds = max(0.0, seconds)
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, remainder = divmod(seconds, 60)
    if minutes < 60:
        return f"{int(minutes)}m {int(remainder)}s"
    hours, minutes = divmod(minutes, 60)
    return f"{int(hours)}h {int(minutes)}m {int(remainder)}s"


def safe_filename_part(value: str) -> str:
    value = value.strip().replace(" ", "_")
    value = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("._ ") or "column"


def create_safe_filename(column_name: str, source: str, period_type: str, period_label: str) -> str:
    column_part = safe_filename_part(column_name)
    source_part = safe_filename_part(source_label(source))
    period_part = safe_filename_part(period_label)
    if period_type == "Whole Data":
        period_part = "All"
    return f"distinct_{column_part}_{source_part}_{period_part}.csv"


def numbered_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    counter = 1
    while True:
        candidate = parent / f"{stem}_{counter}{suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


def describe_period(period_type: str, year: int | None, month: int | None, day: int | None) -> str:
    if period_type == "Whole Data":
        return "All"
    if period_type == "Year" and year:
        return f"{year:04d}"
    if period_type == "Month" and year and month:
        return f"{year:04d}-{month:02d}"
    if period_type == "Specific Day" and year and month and day:
        return f"{year:04d}-{month:02d}-{day:02d}"
    return "Incomplete"


def normalize_column_name(name: str) -> str:
    return str(name).strip()


def visualize_exact_column_name(name: str) -> str:
    """Make leading/trailing spaces visible for ambiguous column names."""
    if name == name.strip():
        return name
    return repr(name)


def normalize_column_value(value: Any) -> tuple[str | None, str | None]:
    """Return (normalized_text, skip_reason) for one Arrow scalar value."""
    if isinstance(value, pa.Scalar):
        if not value.is_valid:
            return None, "null"
        value = value.as_py()

    if value is None:
        return None, "null"

    if isinstance(value, str):
        text = value.strip()
        if text == "":
            return None, "blank"
        return text, None

    if isinstance(value, bytes):
        try:
            text = value.decode("utf-8").strip()
        except UnicodeDecodeError:
            text = value.hex()
        if text == "":
            return None, "blank"
        return text, None

    if isinstance(value, datetime):
        return value.isoformat(sep=" "), None

    if isinstance(value, date):
        return value.isoformat(), None

    if isinstance(value, datetime_time):
        return value.isoformat(), None

    if isinstance(value, Decimal):
        return format(value, "f"), None

    if isinstance(value, bool):
        return "True" if value else "False", None

    if isinstance(value, int):
        return str(value), None

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None, "null"
        fixed = format(value, "f").rstrip("0").rstrip(".")
        if fixed and len(fixed) <= 40:
            return fixed, None
        return format(value, ".15g"), None

    text = str(value).strip()
    if text == "":
        return None, "blank"
    return text, None


def resolve_physical_column(
    parquet_file: pq.ParquetFile,
    normalized_name: str,
    exact_name: str | None,
) -> tuple[str | None, str | None]:
    """Resolve the selected UI column to one physical Parquet column name."""
    names = list(parquet_file.schema_arrow.names)
    if exact_name is not None:
        if exact_name in names:
            return exact_name, None
        return None, "selected exact column is not present"

    matches = [name for name in names if normalize_column_name(name) == normalized_name]
    if not matches:
        return None, "selected column is not present"
    if len(matches) > 1:
        return None, "multiple physical columns match after trimming"
    return matches[0], None


class SQLiteDistinctStore:
    """Temporary disk-backed distinct-value store."""

    def __init__(self) -> None:
        fd, temp_name = tempfile.mkstemp(
            prefix="smart_parquet_distinct_",
            suffix=".sqlite",
        )
        os.close(fd)
        self.path = Path(temp_name)
        self.connection = sqlite3.connect(str(self.path))
        self.connection.execute("PRAGMA journal_mode=OFF")
        self.connection.execute("PRAGMA synchronous=OFF")
        self.connection.execute("PRAGMA temp_store=FILE")
        self.connection.execute(
            """
            CREATE TABLE distinct_values (
                value TEXT PRIMARY KEY COLLATE BINARY
            ) WITHOUT ROWID
            """
        )
        self.connection.commit()

    def insert_many(self, values: Iterable[str]) -> tuple[int, int]:
        rows = [(value,) for value in values]
        if not rows:
            return 0, 0
        before = self.connection.total_changes
        self.connection.executemany(
            "INSERT OR IGNORE INTO distinct_values(value) VALUES (?)",
            rows,
        )
        inserted = self.connection.total_changes - before
        return inserted, len(rows) - inserted

    def commit(self) -> None:
        self.connection.commit()

    def count(self) -> int:
        row = self.connection.execute("SELECT COUNT(*) FROM distinct_values").fetchone()
        return int(row[0])

    def iter_sorted(self, fetch_size: int = 50_000) -> Iterable[list[str]]:
        # Distinct comparison stays case-sensitive via the BINARY primary key.
        # The output uses human-friendly alphabetical ordering, with a BINARY
        # tie-break so values that differ only by case remain deterministic.
        cursor = self.connection.execute(
            "SELECT value FROM distinct_values "
            "ORDER BY value COLLATE NOCASE, value COLLATE BINARY"
        )
        while True:
            rows = cursor.fetchmany(fetch_size)
            if not rows:
                break
            yield [row[0] for row in rows]

    def close_and_delete(self) -> None:
        try:
            self.connection.close()
        finally:
            try:
                self.path.unlink(missing_ok=True)
            except OSError:
                logging.exception("Unable to delete temporary SQLite database: %s", self.path)


class LakePartitionScanner:
    """Scans only file paths and partition folders, not Parquet data."""

    def __init__(
        self,
        root: Path,
        cancel_event: threading.Event,
        post: Callable[[WorkerMessage], None],
    ) -> None:
        self.root = root
        self.cancel_event = cancel_event
        self.post = post

    def scan(self) -> PartitionScanResult:
        started = time.perf_counter()
        if not self.root.exists():
            raise FriendlyError(f"Data lake folder does not exist:\n{self.root}")
        if not self.root.is_dir():
            raise FriendlyError(f"Selected data lake path is not a folder:\n{self.root}")

        records: list[LakeFileRecord] = []
        skipped: list[SkippedFile] = []
        total_parquet = 0
        last_update = 0.0

        self.post(WorkerMessage("status", f"Scanning lake partitions under {self.root}"))
        try:
            iterator = self.root.rglob("*.parquet")
            for file_path in iterator:
                if self.cancel_event.is_set():
                    raise OperationCancelled()
                if is_temp_or_hidden_path(file_path.relative_to(self.root)):
                    continue
                if not file_path.is_file():
                    continue
                if file_path.name.startswith("."):
                    continue
                total_parquet += 1
                record, reason = parse_partition_path(file_path, self.root)
                if record is None:
                    skipped.append(SkippedFile(str(file_path), reason or "invalid partition"))
                else:
                    records.append(record)

                now = time.perf_counter()
                if now - last_update > 0.25:
                    self.post(
                        WorkerMessage(
                            "progress",
                            f"Found {len(records):,} valid Parquet files",
                            {
                                "current_file": file_path.name,
                                "files_done": len(records),
                                "files_total": total_parquet,
                                "operation": "Scanning lake partitions",
                            },
                        )
                    )
                    last_update = now
        except PermissionError as exc:
            raise FriendlyError(f"Permission denied while scanning:\n{exc}") from exc
        except OSError as exc:
            raise FriendlyError(f"Unable to scan selected folder:\n{exc}") from exc

        elapsed = time.perf_counter() - started
        return PartitionScanResult(records, skipped, total_parquet, elapsed)


class ParquetSchemaScanner:
    """Reads Parquet metadata and schemas without loading row data."""

    def __init__(
        self,
        records: list[LakeFileRecord],
        cache: dict[str, FileSchemaMetadata],
        cancel_event: threading.Event,
        post: Callable[[WorkerMessage], None],
    ) -> None:
        self.records = records
        self.cache = cache
        self.cancel_event = cancel_event
        self.post = post

    def scan(self) -> SchemaScanResult:
        started = time.perf_counter()
        skipped: list[SkippedFile] = []
        aggregate: dict[str, dict[str, Any]] = {}
        inspected = 0
        unreadable = 0
        estimated_rows = 0
        total = len(self.records)

        for index, record in enumerate(self.records, start=1):
            if self.cancel_event.is_set():
                raise OperationCancelled()
            self.post(
                WorkerMessage(
                    "progress",
                    f"Inspecting schemas: {index:,} of {total:,}",
                    {
                        "operation": "Scanning Parquet schemas",
                        "current_file": record.path.name,
                        "files_done": index - 1,
                        "files_total": total,
                    },
                )
            )
            try:
                metadata = self._metadata_for(record)
            except Exception as exc:
                unreadable += 1
                skipped.append(SkippedFile(str(record.path), f"unreadable Parquet metadata: {exc}"))
                write_log_error(f"Unreadable Parquet metadata: {record.path}", exc)
                continue

            inspected += 1
            record.row_count = metadata.row_count
            record.row_groups = metadata.row_groups
            estimated_rows += metadata.row_count

            file_seen_normalized: set[str] = set()
            for column in metadata.columns:
                info = aggregate.setdefault(
                    column.normalized_name,
                    {
                        "physical_names": set(),
                        "types": set(),
                        "files": 0,
                        "stream_files": 0,
                        "fast_files": 0,
                        "sources": set(),
                        "by_physical": {},
                    },
                )
                info["physical_names"].add(column.exact_name)
                info["types"].add(column.data_type)
                info["sources"].add(record.source)
                physical = info["by_physical"].setdefault(
                    column.exact_name,
                    {
                        "types": set(),
                        "files": 0,
                        "stream_files": 0,
                        "fast_files": 0,
                        "sources": set(),
                    },
                )
                physical["types"].add(column.data_type)
                physical["sources"].add(record.source)
                physical["files"] += 1
                if record.source == "stream":
                    physical["stream_files"] += 1
                elif record.source == "fast":
                    physical["fast_files"] += 1

                if column.normalized_name not in file_seen_normalized:
                    info["files"] += 1
                    if record.source == "stream":
                        info["stream_files"] += 1
                    elif record.source == "fast":
                        info["fast_files"] += 1
                    file_seen_normalized.add(column.normalized_name)

        choices = self._build_choices(aggregate)
        elapsed = time.perf_counter() - started
        return SchemaScanResult(
            column_choices=choices,
            cache=dict(self.cache),
            inspected_files=inspected,
            unreadable_files=unreadable,
            matching_files=total,
            estimated_rows=estimated_rows,
            skipped=skipped,
            elapsed_seconds=elapsed,
        )

    def _metadata_for(self, record: LakeFileRecord) -> FileSchemaMetadata:
        cache_key = str(record.path)
        cached = self.cache.get(cache_key)
        if cached and cached.stable_key == record.stable_key:
            return cached

        parquet_file = pq.ParquetFile(record.path)
        schema = parquet_file.schema_arrow
        columns = [
            SchemaColumn(
                exact_name=str(name),
                normalized_name=normalize_column_name(str(name)),
                data_type=str(schema.field(index).type),
            )
            for index, name in enumerate(schema.names)
        ]
        metadata = FileSchemaMetadata(
            stable_key=record.stable_key,
            row_count=int(parquet_file.metadata.num_rows),
            row_groups=int(parquet_file.metadata.num_row_groups),
            columns=columns,
        )
        self.cache[cache_key] = metadata
        return metadata

    @staticmethod
    def _availability(sources: set[str]) -> str:
        if "stream" in sources and "fast" in sources:
            return "Both"
        if "stream" in sources:
            return "Stream"
        if "fast" in sources:
            return "Fast"
        return "Unknown"

    def _build_choices(self, aggregate: dict[str, dict[str, Any]]) -> list[ColumnChoice]:
        choices: list[ColumnChoice] = []
        counter = 0
        for normalized_name, info in aggregate.items():
            physical_names = sorted(info["physical_names"])
            ambiguous = len(physical_names) > 1
            if ambiguous:
                for exact_name in physical_names:
                    physical = info["by_physical"][exact_name]
                    counter += 1
                    choices.append(
                        ColumnChoice(
                            item_id=f"column_{counter}",
                            normalized_name=normalized_name,
                            exact_name=exact_name,
                            display_name=visualize_exact_column_name(exact_name),
                            header_name=exact_name,
                            file_count=int(physical["files"]),
                            stream_files=int(physical["stream_files"]),
                            fast_files=int(physical["fast_files"]),
                            data_types=", ".join(sorted(physical["types"])),
                            available_in=self._availability(physical["sources"]),
                            ambiguous=True,
                        )
                    )
            else:
                exact_name = physical_names[0] if physical_names else normalized_name
                counter += 1
                choices.append(
                    ColumnChoice(
                        item_id=f"column_{counter}",
                        normalized_name=normalized_name,
                        exact_name=None,
                        display_name=normalized_name,
                        header_name=exact_name,
                        file_count=int(info["files"]),
                        stream_files=int(info["stream_files"]),
                        fast_files=int(info["fast_files"]),
                        data_types=", ".join(sorted(info["types"])),
                        available_in=self._availability(info["sources"]),
                        ambiguous=False,
                    )
                )

        return sorted(choices, key=lambda item: (item.display_name.lower(), item.display_name))


class DistinctExportWorker:
    """Processes selected Parquet files and writes one distinct-value CSV."""

    def __init__(
        self,
        records: list[LakeFileRecord],
        output_path: Path,
        source_selection: str,
        period_type: str,
        period_label: str,
        column_choice: ColumnChoice,
        cancel_event: threading.Event,
        post: Callable[[WorkerMessage], None],
    ) -> None:
        self.records = records
        self.output_path = output_path
        self.source_selection = source_selection
        self.period_type = period_type
        self.period_label = period_label
        self.column_choice = column_choice
        self.cancel_event = cancel_event
        self.post = post

    def export(self) -> ExportSummary:
        started = time.perf_counter()
        store: SQLiteDistinctStore | None = None
        partial_path = self.output_path.with_name(self.output_path.name + ".partial")
        skipped: list[SkippedFile] = []
        files_processed = 0
        missing_column_files = 0
        unreadable_files = 0
        empty_files = 0
        rows_examined = 0
        null_values = 0
        blank_values = 0
        duplicates_ignored = 0

        estimated_rows = sum(record.row_count or 0 for record in self.records)
        estimated_rows = estimated_rows if estimated_rows > 0 else None

        try:
            if partial_path.exists():
                partial_path.unlink()
            store = SQLiteDistinctStore()

            for file_index, record in enumerate(self.records, start=1):
                if self.cancel_event.is_set():
                    raise OperationCancelled()

                self.post(
                    WorkerMessage(
                        "progress",
                        f"Reading {record.path.name}",
                        {
                            "operation": "Extracting distinct values",
                            "current_file": record.path.name,
                            "files_done": file_index - 1,
                            "files_total": len(self.records),
                            "rows_done": rows_examined,
                            "rows_total": estimated_rows,
                            "distinct": store.count(),
                        },
                    )
                )

                try:
                    parquet_file = pq.ParquetFile(record.path)
                except Exception as exc:
                    unreadable_files += 1
                    skipped.append(SkippedFile(str(record.path), f"unreadable Parquet file: {exc}"))
                    write_log_error(f"Unreadable Parquet file during export: {record.path}", exc)
                    continue

                if parquet_file.metadata.num_rows == 0:
                    empty_files += 1
                    skipped.append(SkippedFile(str(record.path), "empty Parquet file"))
                    continue

                physical_column, reason = resolve_physical_column(
                    parquet_file,
                    self.column_choice.normalized_name,
                    self.column_choice.exact_name,
                )
                if physical_column is None:
                    missing_column_files += 1
                    skipped.append(SkippedFile(str(record.path), reason or "selected column missing"))
                    continue

                try:
                    for batch_index, batch in enumerate(
                        parquet_file.iter_batches(
                            batch_size=BATCH_SIZE,
                            columns=[physical_column],
                            use_threads=True,
                        ),
                        start=1,
                    ):
                        if self.cancel_event.is_set():
                            raise OperationCancelled()
                        values: set[str] = set()
                        array = batch.column(0)
                        rows_examined += batch.num_rows
                        for scalar in array:
                            text, skip_reason = normalize_column_value(scalar)
                            if skip_reason == "null":
                                null_values += 1
                            elif skip_reason == "blank":
                                blank_values += 1
                            elif text is not None:
                                values.add(text)

                        inserted, duplicates = store.insert_many(values)
                        duplicates_ignored += duplicates
                        if batch_index % 10 == 0:
                            store.commit()
                        self.post(
                            WorkerMessage(
                                "progress",
                                f"Processed {rows_examined:,} rows",
                                {
                                    "operation": "Extracting distinct values",
                                    "current_file": record.path.name,
                                    "files_done": file_index - 1,
                                    "files_total": len(self.records),
                                    "rows_done": rows_examined,
                                    "rows_total": estimated_rows,
                                    "batches_done": batch_index,
                                    "distinct": store.count(),
                                },
                            )
                        )
                except OperationCancelled:
                    raise
                except Exception as exc:
                    unreadable_files += 1
                    skipped.append(SkippedFile(str(record.path), f"error reading selected column: {exc}"))
                    write_log_error(f"Error reading selected column from {record.path}", exc)
                    continue

                files_processed += 1
                store.commit()

            distinct_count = store.count()
            if distinct_count == 0:
                elapsed = time.perf_counter() - started
                return ExportSummary(
                    output_path=None,
                    source_selection=self.source_selection,
                    period_type=self.period_type,
                    period_label=self.period_label,
                    selected_column=self.column_choice.header_name,
                    matching_files=len(self.records),
                    files_processed=files_processed,
                    missing_column_files=missing_column_files,
                    unreadable_files=unreadable_files,
                    empty_files=empty_files,
                    rows_examined=rows_examined,
                    null_values=null_values,
                    blank_values=blank_values,
                    duplicates_ignored=duplicates_ignored,
                    distinct_exported=0,
                    skipped=skipped,
                    elapsed_seconds=elapsed,
                )

            self._check_output_folder()
            self.post(WorkerMessage("status", "Writing sorted UTF-8 CSV output"))
            with partial_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow([self.column_choice.header_name])
                written = 0
                for value_batch in store.iter_sorted():
                    if self.cancel_event.is_set():
                        raise OperationCancelled()
                    for value in value_batch:
                        writer.writerow([value])
                    written += len(value_batch)
                    self.post(
                        WorkerMessage(
                            "progress",
                            f"Writing sorted output: {written:,} of {distinct_count:,}",
                            {
                                "operation": "Writing sorted output",
                                "current_file": self.output_path.name,
                                "files_done": len(self.records),
                                "files_total": len(self.records),
                                "rows_done": rows_examined,
                                "rows_total": estimated_rows,
                                "distinct": distinct_count,
                            },
                        )
                    )

            os.replace(partial_path, self.output_path)
            elapsed = time.perf_counter() - started
            return ExportSummary(
                output_path=self.output_path,
                source_selection=self.source_selection,
                period_type=self.period_type,
                period_label=self.period_label,
                selected_column=self.column_choice.header_name,
                matching_files=len(self.records),
                files_processed=files_processed,
                missing_column_files=missing_column_files,
                unreadable_files=unreadable_files,
                empty_files=empty_files,
                rows_examined=rows_examined,
                null_values=null_values,
                blank_values=blank_values,
                duplicates_ignored=duplicates_ignored,
                distinct_exported=distinct_count,
                skipped=skipped,
                elapsed_seconds=elapsed,
            )
        except OperationCancelled:
            elapsed = time.perf_counter() - started
            return ExportSummary(
                output_path=None,
                source_selection=self.source_selection,
                period_type=self.period_type,
                period_label=self.period_label,
                selected_column=self.column_choice.header_name,
                matching_files=len(self.records),
                files_processed=files_processed,
                missing_column_files=missing_column_files,
                unreadable_files=unreadable_files,
                empty_files=empty_files,
                rows_examined=rows_examined,
                null_values=null_values,
                blank_values=blank_values,
                duplicates_ignored=duplicates_ignored,
                distinct_exported=0,
                skipped=skipped,
                elapsed_seconds=elapsed,
                cancelled=True,
            )
        finally:
            try:
                if partial_path.exists():
                    partial_path.unlink()
            except OSError:
                logging.exception("Unable to remove partial output file: %s", partial_path)
            if store is not None:
                store.close_and_delete()

    def _check_output_folder(self) -> None:
        folder = self.output_path.parent
        folder.mkdir(parents=True, exist_ok=True)
        usage = shutil.disk_usage(folder)
        if usage.free < 10 * 1024 * 1024:
            raise FriendlyError(f"Output folder has very little free space:\n{folder}")
        fd, probe = tempfile.mkstemp(prefix=".write_probe_", suffix=".tmp", dir=folder)
        os.close(fd)
        Path(probe).unlink(missing_ok=True)


class SmartParquetLakeDistinctExtractorApp:
    """Main Tkinter application."""

    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.settings = AppSettings.load()
        self.root.title(APP_TITLE)
        self.root.geometry(self.settings.geometry or "1100x800")
        self.root.minsize(950, 700)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        self.records: list[LakeFileRecord] = []
        self.skipped_partitions: list[SkippedFile] = []
        self.schema_cache: dict[str, FileSchemaMetadata] = {}
        self.column_choices: list[ColumnChoice] = []
        self.column_choice_by_item: dict[str, ColumnChoice] = {}
        self.selected_column_choice: ColumnChoice | None = None
        self.column_scan_signature: tuple[Any, ...] | None = None
        self.last_output_path: Path | None = None
        self.worker_queue: queue.Queue[WorkerMessage] = queue.Queue()
        self.cancel_event = threading.Event()
        self.worker_thread: threading.Thread | None = None
        self.busy = False
        self.close_after_cancel = False
        self.operation_started_at: float | None = None
        self.updating_date_controls = False

        self.lake_path_var = tk.StringVar(value=self.settings.last_lake_root or str(DEFAULT_LAKE_ROOT))
        self.source_var = tk.StringVar(value=self.settings.last_source if self.settings.last_source in {"stream", "fast", "both"} else "both")
        self.period_type_var = tk.StringVar(value=self.settings.last_period_type if self.settings.last_period_type in {"Whole Data", "Year", "Month", "Specific Day"} else "Specific Day")
        self.year_var = tk.StringVar()
        self.month_var = tk.StringVar()
        self.day_var = tk.StringVar()
        self.search_var = tk.StringVar()
        self.selected_column_var = tk.StringVar(value="No column selected")
        self.matching_summary_var = tk.StringVar(value="Scan lake partitions to see matching files.")
        self.partition_summary_var = tk.StringVar(value="No lake scan yet.")
        self.operation_var = tk.StringVar(value="Ready.")
        self.current_file_var = tk.StringVar(value="")
        self.files_progress_var = tk.StringVar(value="Files: 0 / 0")
        self.rows_progress_var = tk.StringVar(value="Rows: 0")
        self.batch_progress_var = tk.StringVar(value="Batches: 0")
        self.distinct_progress_var = tk.StringVar(value="Distinct values: 0")
        self.elapsed_var = tk.StringVar(value="Elapsed: 0.0s")

        self.configure_styles()
        self.build_menu()
        self.build_ui()
        self.search_var.trace_add("write", lambda *_: self.populate_column_tree())
        self.refresh_period_control_states()
        self.refresh_control_states()
        self.append_status("Application ready.")
        if not Path(self.lake_path_var.get()).exists():
            self.append_status(
                "Default data lake folder was not found. Choose the correct lake folder before scanning."
            )

    # ------------------------------------------------------------------ UI

    def configure_styles(self) -> None:
        style = ttk.Style(self.root)
        if "vista" in style.theme_names():
            style.theme_use("vista")
        style.configure("Title.TLabel", font=("Segoe UI", 17, "bold"))
        style.configure("Subtitle.TLabel", font=("Segoe UI", 10))
        style.configure("Section.TLabelframe.Label", font=("Segoe UI", 10, "bold"))
        style.configure("Action.TButton", font=("Segoe UI", 10, "bold"), padding=6)
        style.configure("Small.TLabel", font=("Segoe UI", 9))

    def build_menu(self) -> None:
        menu_bar = tk.Menu(self.root)

        file_menu = tk.Menu(menu_bar, tearoff=False)
        file_menu.add_command(label="Choose Data Lake", command=self.choose_lake_folder)
        file_menu.add_command(label="Scan Lake Partitions", command=self.scan_lake_partitions)
        file_menu.add_command(label="Scan Columns", command=self.scan_columns)
        file_menu.add_command(label="Export Distinct Values", command=self.export_distinct_values)
        file_menu.add_command(label="Open Last Output Folder", command=self.open_output_folder)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.on_close)
        menu_bar.add_cascade(label="File", menu=file_menu)

        tools_menu = tk.Menu(menu_bar, tearoff=False)
        tools_menu.add_command(label="Refresh Data Lake", command=self.refresh_data_lake)
        tools_menu.add_command(label="Clear Scan Cache", command=self.clear_scan_cache)
        tools_menu.add_command(label="Reset Application", command=self.reset_application)
        menu_bar.add_cascade(label="Tools", menu=tools_menu)

        help_menu = tk.Menu(menu_bar, tearoff=False)
        help_menu.add_command(label="How to Use", command=self.show_how_to_use)
        help_menu.add_command(label="About", command=self.show_about)
        menu_bar.add_cascade(label="Help", menu=help_menu)

        self.root.config(menu=menu_bar)

    def build_ui(self) -> None:
        outer = ttk.Frame(self.root, padding=12)
        outer.grid(row=0, column=0, sticky="nsew")
        self.root.rowconfigure(0, weight=1)
        self.root.columnconfigure(0, weight=1)
        outer.columnconfigure(0, weight=1)
        outer.rowconfigure(4, weight=1)
        outer.rowconfigure(5, weight=1)

        ttk.Label(outer, text=APP_TITLE, style="Title.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(
            outer,
            text="Extract unique values from one Parquet lake column using source and date partition folders.",
            style="Subtitle.TLabel",
        ).grid(row=1, column=0, sticky="w", pady=(2, 10))

        self.build_lake_section(outer).grid(row=2, column=0, sticky="ew", pady=(0, 8))

        controls = ttk.Frame(outer)
        controls.grid(row=3, column=0, sticky="nsew", pady=(0, 8))
        controls.columnconfigure(0, weight=1)
        controls.columnconfigure(1, weight=1)

        self.build_source_section(controls).grid(row=0, column=0, sticky="nsew", padx=(0, 6))
        self.build_date_section(controls).grid(row=0, column=1, sticky="nsew", padx=(6, 0))

        self.build_column_section(outer).grid(row=4, column=0, sticky="nsew", pady=(0, 8))
        self.build_export_progress_section(outer).grid(row=5, column=0, sticky="nsew")

    def build_lake_section(self, parent: ttk.Frame) -> ttk.LabelFrame:
        frame = ttk.LabelFrame(parent, text="1. Select Data Lake", style="Section.TLabelframe")
        frame.columnconfigure(1, weight=1)

        ttk.Button(frame, text="Choose Data Lake Folder", command=self.choose_lake_folder).grid(
            row=0, column=0, padx=8, pady=8, sticky="w"
        )
        ttk.Entry(frame, textvariable=self.lake_path_var, state="readonly").grid(
            row=0, column=1, sticky="ew", padx=(0, 8), pady=8
        )
        self.scan_lake_button = ttk.Button(
            frame,
            text="Scan Lake Partitions",
            style="Action.TButton",
            command=self.scan_lake_partitions,
        )
        self.scan_lake_button.grid(row=0, column=2, padx=(0, 8), pady=8)
        ttk.Button(frame, text="Refresh", command=self.refresh_data_lake).grid(
            row=0, column=3, padx=(0, 8), pady=8
        )
        ttk.Label(frame, textvariable=self.partition_summary_var, style="Small.TLabel").grid(
            row=1, column=0, columnspan=4, padx=8, pady=(0, 8), sticky="w"
        )
        return frame

    def build_source_section(self, parent: ttk.Frame) -> ttk.LabelFrame:
        frame = ttk.LabelFrame(parent, text="2. Select Source", style="Section.TLabelframe")
        frame.columnconfigure(2, weight=1)
        options = (("Stream", "stream"), ("Fast", "fast"), ("Both", "both"))
        for col, (label, value) in enumerate(options):
            ttk.Radiobutton(
                frame,
                text=label,
                value=value,
                variable=self.source_var,
                command=self.on_source_or_period_changed,
            ).grid(row=0, column=col, padx=8, pady=(8, 4), sticky="w")

        self.source_stats = tk.Text(frame, height=7, wrap="word", state="disabled")
        self.source_stats.grid(row=1, column=0, columnspan=3, padx=8, pady=(4, 8), sticky="nsew")
        frame.rowconfigure(1, weight=1)
        return frame

    def build_date_section(self, parent: ttk.Frame) -> ttk.LabelFrame:
        frame = ttk.LabelFrame(parent, text="3. Select Date Range", style="Section.TLabelframe")
        frame.columnconfigure(1, weight=1)
        period_options = ("Whole Data", "Year", "Month", "Specific Day")
        for col, label in enumerate(period_options):
            ttk.Radiobutton(
                frame,
                text=label,
                value=label,
                variable=self.period_type_var,
                command=self.on_source_or_period_changed,
            ).grid(row=0, column=col, padx=8, pady=(8, 4), sticky="w")

        ttk.Label(frame, text="Year").grid(row=1, column=0, padx=8, pady=4, sticky="w")
        self.year_combo = ttk.Combobox(frame, textvariable=self.year_var, state="readonly", width=12)
        self.year_combo.grid(row=1, column=1, padx=8, pady=4, sticky="ew")
        self.year_combo.bind("<<ComboboxSelected>>", lambda _event: self.on_year_changed())

        ttk.Label(frame, text="Month").grid(row=2, column=0, padx=8, pady=4, sticky="w")
        self.month_combo = ttk.Combobox(frame, textvariable=self.month_var, state="readonly", width=16)
        self.month_combo.grid(row=2, column=1, padx=8, pady=4, sticky="ew")
        self.month_combo.bind("<<ComboboxSelected>>", lambda _event: self.on_month_changed())

        ttk.Label(frame, text="Day").grid(row=3, column=0, padx=8, pady=4, sticky="w")
        self.day_combo = ttk.Combobox(frame, textvariable=self.day_var, state="readonly", width=12)
        self.day_combo.grid(row=3, column=1, padx=8, pady=4, sticky="ew")
        self.day_combo.bind("<<ComboboxSelected>>", lambda _event: self.on_day_changed())

        ttk.Label(frame, textvariable=self.matching_summary_var, style="Small.TLabel").grid(
            row=4, column=0, columnspan=4, padx=8, pady=(4, 4), sticky="w"
        )

        tree_frame = ttk.Frame(frame)
        tree_frame.grid(row=5, column=0, columnspan=4, sticky="nsew", padx=8, pady=(4, 8))
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)
        self.date_tree = ttk.Treeview(tree_frame, show="tree", height=7)
        date_scroll = ttk.Scrollbar(tree_frame, orient="vertical", command=self.date_tree.yview)
        self.date_tree.configure(yscrollcommand=date_scroll.set)
        self.date_tree.grid(row=0, column=0, sticky="nsew")
        date_scroll.grid(row=0, column=1, sticky="ns")
        self.date_tree.bind("<<TreeviewSelect>>", self.on_date_tree_select)
        frame.rowconfigure(5, weight=1)
        return frame

    def build_column_section(self, parent: ttk.Frame) -> ttk.LabelFrame:
        frame = ttk.LabelFrame(parent, text="4. Scan and Select Column", style="Section.TLabelframe")
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(2, weight=1)

        top = ttk.Frame(frame)
        top.grid(row=0, column=0, sticky="ew", padx=8, pady=8)
        top.columnconfigure(1, weight=1)
        self.scan_columns_button = ttk.Button(
            top,
            text="Scan Columns",
            style="Action.TButton",
            command=self.scan_columns,
        )
        self.scan_columns_button.grid(row=0, column=0, padx=(0, 8), sticky="w")
        ttk.Label(top, text="Search").grid(row=0, column=1, sticky="e", padx=(0, 6))
        ttk.Entry(top, textvariable=self.search_var).grid(row=0, column=2, sticky="ew")
        ttk.Button(top, text="Clear Search", command=lambda: self.search_var.set("")).grid(
            row=0, column=3, padx=(8, 0)
        )
        top.columnconfigure(2, weight=1)

        ttk.Label(frame, textvariable=self.selected_column_var, style="Small.TLabel").grid(
            row=1, column=0, padx=8, pady=(0, 4), sticky="w"
        )

        tree_frame = ttk.Frame(frame)
        tree_frame.grid(row=2, column=0, sticky="nsew", padx=8, pady=(0, 8))
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)
        columns = ("files", "stream_files", "fast_files", "data_type", "available_in")
        self.column_tree = ttk.Treeview(
            tree_frame,
            columns=columns,
            show="tree headings",
            selectmode="browse",
            height=10,
        )
        self.column_tree.heading("#0", text="Column Name", anchor="w")
        self.column_tree.heading("files", text="Files", anchor="e")
        self.column_tree.heading("stream_files", text="Stream Files", anchor="e")
        self.column_tree.heading("fast_files", text="Fast Files", anchor="e")
        self.column_tree.heading("data_type", text="Data Type", anchor="w")
        self.column_tree.heading("available_in", text="Available In", anchor="w")
        self.column_tree.column("#0", width=320, stretch=True)
        self.column_tree.column("files", width=70, anchor="e")
        self.column_tree.column("stream_files", width=95, anchor="e")
        self.column_tree.column("fast_files", width=85, anchor="e")
        self.column_tree.column("data_type", width=210, stretch=True)
        self.column_tree.column("available_in", width=90)
        y_scroll = ttk.Scrollbar(tree_frame, orient="vertical", command=self.column_tree.yview)
        x_scroll = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.column_tree.xview)
        self.column_tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        self.column_tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll.grid(row=1, column=0, sticky="ew")
        self.column_tree.bind("<<TreeviewSelect>>", self.on_column_select)

        self.column_count_var = tk.StringVar(value="Displayed columns: 0 | Total discovered columns: 0")
        ttk.Label(frame, textvariable=self.column_count_var, style="Small.TLabel").grid(
            row=3, column=0, padx=8, pady=(0, 8), sticky="w"
        )
        return frame

    def build_export_progress_section(self, parent: ttk.Frame) -> ttk.LabelFrame:
        frame = ttk.LabelFrame(parent, text="5. Export Distinct Values   6. Progress and Messages", style="Section.TLabelframe")
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(3, weight=1)

        buttons = ttk.Frame(frame)
        buttons.grid(row=0, column=0, sticky="ew", padx=8, pady=8)
        self.export_button = ttk.Button(
            buttons,
            text="Export Distinct Values",
            style="Action.TButton",
            command=self.export_distinct_values,
        )
        self.export_button.grid(row=0, column=0, padx=(0, 8))
        self.open_output_button = ttk.Button(
            buttons,
            text="Open Last Output Folder",
            command=self.open_output_folder,
        )
        self.open_output_button.grid(row=0, column=1, padx=(0, 8))
        self.cancel_button = ttk.Button(buttons, text="Cancel", command=self.cancel_current_operation)
        self.cancel_button.grid(row=0, column=2)

        progress = ttk.Frame(frame)
        progress.grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 8))
        progress.columnconfigure(1, weight=1)
        ttk.Label(progress, textvariable=self.operation_var).grid(row=0, column=0, columnspan=4, sticky="w")
        self.progress_bar = ttk.Progressbar(progress, mode="determinate", maximum=100)
        self.progress_bar.grid(row=1, column=0, columnspan=4, sticky="ew", pady=(4, 4))
        ttk.Label(progress, textvariable=self.current_file_var).grid(row=2, column=0, columnspan=4, sticky="w")
        ttk.Label(progress, textvariable=self.files_progress_var).grid(row=3, column=0, sticky="w", padx=(0, 16))
        ttk.Label(progress, textvariable=self.rows_progress_var).grid(row=3, column=1, sticky="w", padx=(0, 16))
        ttk.Label(progress, textvariable=self.batch_progress_var).grid(row=3, column=2, sticky="w", padx=(0, 16))
        ttk.Label(progress, textvariable=self.distinct_progress_var).grid(row=3, column=3, sticky="w")
        ttk.Label(progress, textvariable=self.elapsed_var).grid(row=4, column=0, columnspan=4, sticky="w", pady=(2, 0))

        status_frame = ttk.Frame(frame)
        status_frame.grid(row=3, column=0, sticky="nsew", padx=8, pady=(0, 8))
        status_frame.columnconfigure(0, weight=1)
        status_frame.rowconfigure(0, weight=1)
        self.status_text = tk.Text(status_frame, height=10, wrap="word", state="disabled")
        status_scroll = ttk.Scrollbar(status_frame, orient="vertical", command=self.status_text.yview)
        self.status_text.configure(yscrollcommand=status_scroll.set)
        self.status_text.grid(row=0, column=0, sticky="nsew")
        status_scroll.grid(row=0, column=1, sticky="ns")
        return frame

    # ------------------------------------------------------------- UI events

    def choose_lake_folder(self) -> None:
        initial = self.lake_path_var.get() if Path(self.lake_path_var.get()).exists() else str(DEFAULT_LAKE_ROOT.parent)
        selected = filedialog.askdirectory(title="Choose Parquet Data Lake Folder", initialdir=initial)
        if not selected:
            return
        self.lake_path_var.set(selected)
        self.records = []
        self.skipped_partitions = []
        self.clear_column_scan()
        self.partition_summary_var.set("Lake folder changed. Scan lake partitions before continuing.")
        self.update_source_stats()
        self.refresh_date_controls()
        self.refresh_control_states()
        self.append_status(f"Selected data lake folder: {selected}")

    def refresh_data_lake(self) -> None:
        self.clear_scan_cache()
        self.scan_lake_partitions()

    def clear_scan_cache(self) -> None:
        self.schema_cache.clear()
        self.clear_column_scan()
        self.append_status("Cleared in-memory partition/schema scan cache.")
        self.refresh_control_states()

    def reset_application(self) -> None:
        if self.busy:
            messagebox.showwarning(APP_TITLE, "Cancel the current operation before resetting.")
            return
        self.records = []
        self.skipped_partitions = []
        self.schema_cache.clear()
        self.clear_column_scan()
        self.source_var.set("both")
        self.period_type_var.set("Specific Day")
        self.year_var.set("")
        self.month_var.set("")
        self.day_var.set("")
        self.search_var.set("")
        self.partition_summary_var.set("No lake scan yet.")
        self.matching_summary_var.set("Scan lake partitions to see matching files.")
        self.update_source_stats()
        self.refresh_date_controls()
        self.refresh_period_control_states()
        self.refresh_control_states()
        self.append_status("Application reset.")

    def on_source_or_period_changed(self) -> None:
        if self.updating_date_controls:
            return
        self.clear_column_scan()
        self.refresh_date_controls()
        self.refresh_period_control_states()
        self.update_matching_summary()
        self.refresh_control_states()

    def on_year_changed(self) -> None:
        if self.updating_date_controls:
            return
        self.clear_column_scan()
        self.refresh_date_controls(preserve_year=True)
        self.update_matching_summary()

    def on_month_changed(self) -> None:
        if self.updating_date_controls:
            return
        self.clear_column_scan()
        self.refresh_date_controls(preserve_year=True, preserve_month=True)
        self.update_matching_summary()

    def on_day_changed(self) -> None:
        if self.updating_date_controls:
            return
        self.clear_column_scan()
        self.update_matching_summary()
        self.refresh_control_states()

    def on_date_tree_select(self, _event: tk.Event[Any]) -> None:
        selected = self.date_tree.selection()
        if not selected:
            return
        values = self.date_tree.item(selected[0], "values")
        if not values:
            return
        try:
            chosen = datetime.strptime(values[0], "%Y-%m-%d").date()
        except ValueError:
            return
        self.period_type_var.set("Specific Day")
        self.year_var.set(str(chosen.year))
        self.month_var.set(MONTH_NAMES[chosen.month])
        self.day_var.set(f"{chosen.day:02d}")
        self.clear_column_scan()
        self.refresh_date_controls(preserve_year=True, preserve_month=True, preserve_day=True)
        self.refresh_period_control_states()
        self.update_matching_summary()
        self.refresh_control_states()

    def on_column_select(self, _event: tk.Event[Any]) -> None:
        selected = self.column_tree.selection()
        if not selected:
            self.selected_column_choice = None
            self.selected_column_var.set("No column selected")
            self.refresh_control_states()
            return
        item_id = selected[0]
        choice = self.column_choice_by_item.get(item_id)
        if choice is None:
            return
        self.selected_column_choice = choice
        ambiguity_note = " exact physical column" if choice.ambiguous else ""
        self.selected_column_var.set(
            f"Selected column: {choice.header_name}{ambiguity_note} | "
            f"{choice.file_count:,} files | {choice.available_in}"
        )
        self.refresh_control_states()

    # --------------------------------------------------------------- workers

    def scan_lake_partitions(self) -> None:
        if self.busy:
            return
        root = Path(self.lake_path_var.get()).expanduser()

        def worker(post: Callable[[WorkerMessage], None], cancel_event: threading.Event) -> PartitionScanResult:
            return LakePartitionScanner(root, cancel_event, post).scan()

        self.start_worker("scan_partitions", worker)

    def scan_columns(self) -> None:
        if self.busy:
            return
        try:
            matching = self.current_matching_records(require_complete_period=True)
        except FriendlyError as exc:
            messagebox.showwarning(APP_TITLE, str(exc))
            return
        if not matching:
            messagebox.showwarning(APP_TITLE, "No matching Parquet files for the selected source and date range.")
            return
        cache_snapshot = dict(self.schema_cache)

        def worker(post: Callable[[WorkerMessage], None], cancel_event: threading.Event) -> SchemaScanResult:
            return ParquetSchemaScanner(matching, cache_snapshot, cancel_event, post).scan()

        self.start_worker("scan_columns", worker)

    def export_distinct_values(self) -> None:
        if self.busy:
            return
        try:
            self.validate_export_ready()
            matching = self.current_matching_records(require_complete_period=True)
        except FriendlyError as exc:
            messagebox.showwarning(APP_TITLE, str(exc))
            return

        if self.selected_column_choice is None:
            messagebox.showwarning(APP_TITLE, "Select one column before exporting.")
            return

        output_path = self.choose_export_path(self.selected_column_choice)
        if output_path is None:
            return

        source = self.source_var.get()
        period_type = self.period_type_var.get()
        period_label = describe_period(
            period_type,
            self.selected_year(),
            self.selected_month(),
            self.selected_day(),
        )
        column_choice = self.selected_column_choice

        def worker(post: Callable[[WorkerMessage], None], cancel_event: threading.Event) -> ExportSummary:
            return DistinctExportWorker(
                records=matching,
                output_path=output_path,
                source_selection=source,
                period_type=period_type,
                period_label=period_label,
                column_choice=column_choice,
                cancel_event=cancel_event,
                post=post,
            ).export()

        self.start_worker("export", worker)

    def start_worker(
        self,
        operation: str,
        target: Callable[[Callable[[WorkerMessage], None], threading.Event], Any],
    ) -> None:
        self.busy = True
        self.cancel_event = threading.Event()
        self.worker_queue = queue.Queue()
        self.operation_started_at = time.perf_counter()
        self.operation_var.set("Starting...")
        self.progress_bar.configure(value=0)
        self.refresh_control_states()

        def post(message: WorkerMessage) -> None:
            self.worker_queue.put(message)

        def runner() -> None:
            try:
                result = target(post, self.cancel_event)
                post(WorkerMessage("done", payload={"operation": operation, "result": result}))
            except OperationCancelled:
                post(WorkerMessage("cancelled", "Operation cancelled by user."))
            except FriendlyError as exc:
                post(WorkerMessage("friendly_error", str(exc)))
            except Exception as exc:
                write_log_error(f"Unexpected error during {operation}", exc)
                post(
                    WorkerMessage(
                        "error",
                        f"Unexpected error during {operation}. See log file:\n{LOG_PATH}",
                    )
                )

        self.worker_thread = threading.Thread(target=runner, daemon=True)
        self.worker_thread.start()
        self.root.after(100, self.poll_worker_queue)

    def poll_worker_queue(self) -> None:
        try:
            while True:
                message = self.worker_queue.get_nowait()
                self.handle_worker_message(message)
        except queue.Empty:
            pass

        if self.busy:
            if self.operation_started_at is not None:
                self.elapsed_var.set(f"Elapsed: {elapsed_text(time.perf_counter() - self.operation_started_at)}")
            self.root.after(150, self.poll_worker_queue)
        elif self.close_after_cancel:
            self.root.destroy()

    def handle_worker_message(self, message: WorkerMessage) -> None:
        if message.kind == "status":
            self.append_status(message.text)
        elif message.kind == "progress":
            self.update_progress(message.text, message.payload or {}, message.progress)
        elif message.kind == "done":
            operation = message.payload["operation"]
            result = message.payload["result"]
            self.finish_worker()
            if operation == "scan_partitions":
                self.on_partition_scan_done(result)
            elif operation == "scan_columns":
                self.on_schema_scan_done(result)
            elif operation == "export":
                self.on_export_done(result)
        elif message.kind == "friendly_error":
            self.finish_worker()
            self.append_status(f"ERROR: {message.text}")
            messagebox.showerror(APP_TITLE, message.text)
        elif message.kind == "error":
            self.finish_worker()
            self.append_status(f"ERROR: {message.text}")
            messagebox.showerror(APP_TITLE, message.text)
        elif message.kind == "cancelled":
            self.finish_worker()
            self.append_status("Operation cancelled.")
            messagebox.showinfo(APP_TITLE, "Operation cancelled.")

    def finish_worker(self) -> None:
        self.busy = False
        self.operation_var.set("Ready.")
        self.current_file_var.set("")
        self.refresh_control_states()

    def cancel_current_operation(self) -> None:
        if not self.busy:
            return
        self.cancel_event.set()
        self.operation_var.set("Cancelling at the next safe checkpoint...")
        self.append_status("Cancellation requested.")

    # ---------------------------------------------------------- done handlers

    def on_partition_scan_done(self, result: PartitionScanResult) -> None:
        self.records = result.records
        self.skipped_partitions = result.skipped
        self.clear_column_scan()
        self.refresh_date_controls()
        self.refresh_period_control_states()
        self.update_source_stats()
        self.update_matching_summary()
        self.refresh_control_states()

        stream_count = sum(1 for item in self.records if item.source == "stream")
        fast_count = sum(1 for item in self.records if item.source == "fast")
        dates = sorted({item.partition_date for item in self.records})
        years = sorted({item.year for item in self.records})
        summary = (
            f"Found {len(self.records):,} valid Parquet files "
            f"({stream_count:,} Stream, {fast_count:,} Fast)."
        )
        self.partition_summary_var.set(summary)
        self.append_status("")
        self.append_status("Partition scan complete.")
        self.append_status(f"Selected lake root: {self.lake_path_var.get()}")
        self.append_status(f"Total .parquet files found: {result.total_parquet_files:,}")
        self.append_status(f"Valid Stream files: {stream_count:,}")
        self.append_status(f"Valid Fast files: {fast_count:,}")
        self.append_status(f"Files with invalid partitions: {len(result.skipped):,}")
        if dates:
            self.append_status(f"Earliest available date: {dates[0].isoformat()}")
            self.append_status(f"Latest available date: {dates[-1].isoformat()}")
            self.append_status(f"Number of distinct dates: {len(dates):,}")
            self.append_status(f"Available years: {', '.join(str(year) for year in years)}")
        self.append_status(f"Scan elapsed time: {elapsed_text(result.elapsed_seconds)}")
        self.append_skipped_report(result.skipped, "Invalid partition / skipped files")
        if not self.records:
            messagebox.showwarning(APP_TITLE, "No valid source=stream or source=fast Parquet partitions were found.")

    def on_schema_scan_done(self, result: SchemaScanResult) -> None:
        self.schema_cache = result.cache
        self.column_choices = result.column_choices
        self.column_scan_signature = self.current_filter_signature()
        self.selected_column_choice = None
        self.selected_column_var.set("No column selected")
        self.populate_column_tree()
        self.update_matching_summary()
        self.refresh_control_states()

        self.append_status("")
        self.append_status("Column scan complete.")
        self.append_status(f"Selected source: {source_label(self.source_var.get())}")
        self.append_status(f"Selected period: {self.current_period_display()}")
        self.append_status(f"Matching files: {result.matching_files:,}")
        self.append_status(f"Successfully inspected files: {result.inspected_files:,}")
        self.append_status(f"Unreadable files: {result.unreadable_files:,}")
        self.append_status(f"Number of distinct column names: {len(result.column_choices):,}")
        self.append_status(f"Estimated matching rows: {result.estimated_rows:,}")
        self.append_status(f"Scan elapsed time: {elapsed_text(result.elapsed_seconds)}")
        self.append_skipped_report(result.skipped, "Unreadable schema files")
        if not result.column_choices:
            messagebox.showwarning(APP_TITLE, "No columns were found in the matching Parquet files.")

    def on_export_done(self, summary: ExportSummary) -> None:
        self.append_status("")
        if summary.cancelled:
            self.append_status("Export cancelled. Temporary files were cleaned up.")
            messagebox.showinfo(APP_TITLE, "Export cancelled. Temporary files were cleaned up.")
            return

        self.append_status("Export summary")
        self.append_status(f"Source: {source_label(summary.source_selection)}")
        self.append_status(f"Period type: {summary.period_type}")
        self.append_status(f"Selected period: {summary.period_label}")
        self.append_status(f"Selected column: {summary.selected_column}")
        self.append_status(f"Matching files: {summary.matching_files:,}")
        self.append_status(f"Files processed: {summary.files_processed:,}")
        self.append_status(f"Files skipped because column was missing: {summary.missing_column_files:,}")
        self.append_status(f"Files skipped because unreadable: {summary.unreadable_files:,}")
        self.append_status(f"Empty Parquet files skipped: {summary.empty_files:,}")
        self.append_status(f"Total rows examined: {summary.rows_examined:,}")
        self.append_status(f"Null values ignored: {summary.null_values:,}")
        self.append_status(f"Blank string values ignored: {summary.blank_values:,}")
        self.append_status(f"Duplicate values ignored: {summary.duplicates_ignored:,}")
        self.append_status(f"Distinct values exported: {summary.distinct_exported:,}")
        self.append_status(f"Elapsed time: {elapsed_text(summary.elapsed_seconds)}")
        self.append_skipped_report(summary.skipped, "Export skipped-files report")

        if summary.output_path is None or summary.distinct_exported == 0:
            self.append_status("No distinct values were found. No CSV file was written.")
            messagebox.showwarning(
                APP_TITLE,
                "No distinct values were found. No CSV file was written.",
            )
            return

        self.last_output_path = summary.output_path
        self.settings.last_export_folder = str(summary.output_path.parent)
        self.append_status(f"Output path: {summary.output_path}")
        self.refresh_control_states()
        messagebox.showinfo(
            APP_TITLE,
            f"Export completed successfully.\n\n"
            f"Distinct values: {summary.distinct_exported:,}\n"
            f"Output:\n{summary.output_path}",
        )

    # ------------------------------------------------------------ data logic

    def selected_year(self) -> int | None:
        try:
            return int(self.year_var.get())
        except ValueError:
            return None

    def selected_month(self) -> int | None:
        text = self.month_var.get()
        if not text:
            return None
        if text in MONTH_NAME_TO_NUMBER:
            return MONTH_NAME_TO_NUMBER[text]
        try:
            return int(text)
        except ValueError:
            return None

    def selected_day(self) -> int | None:
        try:
            return int(self.day_var.get())
        except ValueError:
            return None

    def current_filter_signature(self) -> tuple[Any, ...]:
        return (
            str(Path(self.lake_path_var.get()).expanduser()),
            self.source_var.get(),
            self.period_type_var.get(),
            self.selected_year(),
            self.selected_month(),
            self.selected_day(),
            len(self.records),
        )

    def current_period_display(self) -> str:
        return describe_period(
            self.period_type_var.get(),
            self.selected_year(),
            self.selected_month(),
            self.selected_day(),
        )

    def current_matching_records(self, require_complete_period: bool) -> list[LakeFileRecord]:
        if not self.records:
            raise FriendlyError("Scan lake partitions before continuing.")

        sources = selected_sources(self.source_var.get())
        period = self.period_type_var.get()
        year = self.selected_year()
        month = self.selected_month()
        day = self.selected_day()

        if period == "Year" and year is None and require_complete_period:
            raise FriendlyError("Choose a year.")
        if period == "Month" and (year is None or month is None) and require_complete_period:
            raise FriendlyError("Choose a year and month.")
        if period == "Specific Day" and (year is None or month is None or day is None) and require_complete_period:
            raise FriendlyError("Choose a year, month, and day.")

        filtered: list[LakeFileRecord] = []
        for record in self.records:
            if record.source not in sources:
                continue
            if period == "Year" and year is not None and record.year != year:
                continue
            if period == "Month":
                if year is not None and record.year != year:
                    continue
                if month is not None and record.month != month:
                    continue
            if period == "Specific Day":
                if year is not None and record.year != year:
                    continue
                if month is not None and record.month != month:
                    continue
                if day is not None and record.day != day:
                    continue
            filtered.append(record)
        return filtered

    def available_dates_for_source(self) -> list[date]:
        sources = selected_sources(self.source_var.get())
        return sorted({record.partition_date for record in self.records if record.source in sources})

    def refresh_date_controls(
        self,
        preserve_year: bool = False,
        preserve_month: bool = False,
        preserve_day: bool = False,
    ) -> None:
        self.updating_date_controls = True
        try:
            dates = self.available_dates_for_source()
            current_year = self.selected_year() if preserve_year else None
            current_month = self.selected_month() if preserve_month else None
            current_day = self.selected_day() if preserve_day else None

            years = sorted({item.year for item in dates})
            self.year_combo["values"] = [str(year) for year in years]
            if years:
                if current_year not in years:
                    current_year = years[-1]
                self.year_var.set(str(current_year))
            else:
                self.year_var.set("")
                current_year = None

            months = sorted({item.month for item in dates if current_year is None or item.year == current_year})
            self.month_combo["values"] = [MONTH_NAMES[month] for month in months]
            if months:
                if current_month not in months:
                    current_month = months[-1]
                self.month_var.set(MONTH_NAMES[current_month])
            else:
                self.month_var.set("")
                current_month = None

            days = sorted(
                {
                    item.day
                    for item in dates
                    if (current_year is None or item.year == current_year)
                    and (current_month is None or item.month == current_month)
                }
            )
            self.day_combo["values"] = [f"{day:02d}" for day in days]
            if days:
                if current_day not in days:
                    current_day = days[-1]
                self.day_var.set(f"{current_day:02d}")
            else:
                self.day_var.set("")

            self.populate_date_tree(dates)
        finally:
            self.updating_date_controls = False

    def populate_date_tree(self, dates: list[date]) -> None:
        self.date_tree.delete(*self.date_tree.get_children())
        by_year: dict[int, dict[int, list[date]]] = {}
        for item in dates:
            by_year.setdefault(item.year, {}).setdefault(item.month, []).append(item)
        for year in sorted(by_year):
            year_item = self.date_tree.insert("", "end", text=f"Year: {year}", open=True)
            for month in sorted(by_year[year]):
                month_item = self.date_tree.insert(
                    year_item,
                    "end",
                    text=f"Month: {MONTH_NAMES[month]}",
                    open=True,
                )
                for day_value in sorted(by_year[year][month]):
                    self.date_tree.insert(
                        month_item,
                        "end",
                        text=f"Date: {day_value.isoformat()}",
                        values=(day_value.isoformat(),),
                    )

    def refresh_period_control_states(self) -> None:
        period = self.period_type_var.get()
        year_state = "readonly" if period in {"Year", "Month", "Specific Day"} and not self.busy else "disabled"
        month_state = "readonly" if period in {"Month", "Specific Day"} and not self.busy else "disabled"
        day_state = "readonly" if period == "Specific Day" and not self.busy else "disabled"
        self.year_combo.configure(state=year_state)
        self.month_combo.configure(state=month_state)
        self.day_combo.configure(state=day_state)

    def update_matching_summary(self) -> None:
        if not self.records:
            self.matching_summary_var.set("Scan lake partitions to see matching files.")
            return
        try:
            matching = self.current_matching_records(require_complete_period=False)
        except FriendlyError:
            matching = []
        stream_files = sum(1 for item in matching if item.source == "stream")
        fast_files = sum(1 for item in matching if item.source == "fast")
        rows = sum(item.row_count or 0 for item in matching)
        row_text = f" | Estimated rows: {rows:,}" if rows else ""
        self.matching_summary_var.set(
            f"Matching Stream files: {stream_files:,} | "
            f"Matching Fast files: {fast_files:,} | "
            f"Total matching files: {len(matching):,}{row_text}"
        )

    def update_source_stats(self) -> None:
        stream = [item for item in self.records if item.source == "stream"]
        fast = [item for item in self.records if item.source == "fast"]

        def date_range_text(items: list[LakeFileRecord]) -> tuple[str, str]:
            if not items:
                return "N/A", "N/A"
            dates = [item.partition_date for item in items]
            return min(dates).isoformat(), max(dates).isoformat()

        stream_min, stream_max = date_range_text(stream)
        fast_min, fast_max = date_range_text(fast)
        text = (
            f"Stream Parquet files: {len(stream):,}\n"
            f"Fast Parquet files: {len(fast):,}\n"
            f"Total Parquet files: {len(self.records):,}\n"
            f"Earliest Stream date: {stream_min}\n"
            f"Latest Stream date: {stream_max}\n"
            f"Earliest Fast date: {fast_min}\n"
            f"Latest Fast date: {fast_max}"
        )
        self.source_stats.configure(state="normal")
        self.source_stats.delete("1.0", "end")
        self.source_stats.insert("end", text)
        self.source_stats.configure(state="disabled")

    def clear_column_scan(self) -> None:
        self.column_choices = []
        self.column_choice_by_item.clear()
        self.selected_column_choice = None
        self.column_scan_signature = None
        if hasattr(self, "column_tree"):
            self.column_tree.delete(*self.column_tree.get_children())
        if hasattr(self, "column_count_var"):
            self.column_count_var.set("Displayed columns: 0 | Total discovered columns: 0")
        if hasattr(self, "selected_column_var"):
            self.selected_column_var.set("No column selected")

    def populate_column_tree(self) -> None:
        if not hasattr(self, "column_tree"):
            return
        search = self.search_var.get().strip().lower()
        self.column_tree.delete(*self.column_tree.get_children())
        self.column_choice_by_item.clear()
        displayed = 0
        for choice in self.column_choices:
            haystack = f"{choice.display_name} {choice.data_types} {choice.available_in}".lower()
            if search and search not in haystack:
                continue
            tags = ("ambiguous",) if choice.ambiguous else ()
            self.column_tree.insert(
                "",
                "end",
                iid=choice.item_id,
                text=choice.display_name,
                values=(
                    f"{choice.file_count:,}",
                    f"{choice.stream_files:,}",
                    f"{choice.fast_files:,}",
                    choice.data_types,
                    choice.available_in,
                ),
                tags=tags,
            )
            self.column_choice_by_item[choice.item_id] = choice
            displayed += 1
        self.column_count_var.set(
            f"Displayed columns: {displayed:,} | Total discovered columns: {len(self.column_choices):,}"
        )

    def validate_export_ready(self) -> None:
        root = Path(self.lake_path_var.get()).expanduser()
        if not root.exists():
            raise FriendlyError("Selected data lake folder does not exist.")
        if not self.records:
            raise FriendlyError("Scan lake partitions before exporting.")
        matching = self.current_matching_records(require_complete_period=True)
        if not matching:
            raise FriendlyError("No matching Parquet files for the selected source and date range.")
        if self.column_scan_signature != self.current_filter_signature():
            raise FriendlyError("Scan columns again for the current source and date range.")
        if self.selected_column_choice is None:
            raise FriendlyError("Select one column before exporting.")

    # ------------------------------------------------------------- exporting

    def choose_export_path(self, choice: ColumnChoice) -> Path | None:
        period_type = self.period_type_var.get()
        period_label = describe_period(
            period_type,
            self.selected_year(),
            self.selected_month(),
            self.selected_day(),
        )
        suggested = create_safe_filename(choice.header_name, self.source_var.get(), period_type, period_label)
        initial_dir = self.settings.last_export_folder
        if not initial_dir or not Path(initial_dir).exists():
            initial_dir = str(Path.home())
        selected = filedialog.asksaveasfilename(
            title="Save Distinct Values CSV",
            initialdir=initial_dir,
            initialfile=suggested,
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if not selected:
            return None
        path = Path(selected)
        if path.suffix.lower() != ".csv":
            path = path.with_suffix(".csv")
        if path.exists():
            answer = messagebox.askyesnocancel(
                APP_TITLE,
                f"The file already exists:\n{path}\n\n"
                "Choose Yes to overwrite it, No to create a numbered filename, or Cancel.",
            )
            if answer is None:
                return None
            if answer is False:
                path = numbered_path(path)
        self.settings.last_export_folder = str(path.parent)
        return path

    def open_output_folder(self) -> None:
        folder: Path | None = None
        if self.last_output_path is not None:
            folder = self.last_output_path.parent
        elif self.settings.last_export_folder:
            folder = Path(self.settings.last_export_folder)
        if folder is None or not folder.exists():
            messagebox.showinfo(APP_TITLE, "No output folder is available yet.")
            return
        try:
            os.startfile(str(folder))  # type: ignore[attr-defined]
        except OSError as exc:
            messagebox.showerror(APP_TITLE, f"Unable to open output folder:\n{exc}")

    # --------------------------------------------------------------- progress

    def update_progress(self, text: str, payload: dict[str, Any], explicit_progress: float | None) -> None:
        self.operation_var.set(payload.get("operation") or text)
        current_file = payload.get("current_file")
        if current_file:
            self.current_file_var.set(f"Current file: {current_file}")
        files_done = int(payload.get("files_done") or 0)
        files_total = int(payload.get("files_total") or 0)
        rows_done = int(payload.get("rows_done") or 0)
        rows_total = payload.get("rows_total")
        batches_done = int(payload.get("batches_done") or 0)
        distinct = int(payload.get("distinct") or 0)
        self.files_progress_var.set(f"Files: {files_done:,} / {files_total:,}")
        if rows_total:
            self.rows_progress_var.set(f"Rows: {rows_done:,} / {int(rows_total):,}")
        else:
            self.rows_progress_var.set(f"Rows: {rows_done:,}")
        self.batch_progress_var.set(f"Batches: {batches_done:,}")
        self.distinct_progress_var.set(f"Distinct values: {distinct:,}")

        if explicit_progress is not None:
            value = explicit_progress
        elif rows_total:
            value = min(100.0, (rows_done / float(rows_total)) * 100.0)
        elif files_total:
            value = min(100.0, (files_done / float(files_total)) * 100.0)
        else:
            value = 0.0
        self.progress_bar.configure(value=value)

    def append_status(self, message: str) -> None:
        self.status_text.configure(state="normal")
        if message:
            self.status_text.insert("end", f"[{timestamp()}] {message}\n")
        else:
            self.status_text.insert("end", "\n")
        self.status_text.configure(state="disabled")
        self.status_text.see("end")

    def append_skipped_report(self, skipped: list[SkippedFile], title: str) -> None:
        if not skipped:
            return
        self.append_status(f"{title}: {len(skipped):,}")
        for item in skipped:
            self.append_status(f"  - {item.path} | {item.reason}")

    # ------------------------------------------------------------- controls

    def refresh_control_states(self) -> None:
        normal = "normal"
        disabled = "disabled"
        state = disabled if self.busy else normal
        for widget in (
            self.scan_lake_button,
            self.scan_columns_button,
        ):
            widget.configure(state=state)
        self.export_button.configure(
            state=normal
            if (
                not self.busy
                and self.selected_column_choice is not None
                and self.column_scan_signature == self.current_filter_signature()
            )
            else disabled
        )
        self.cancel_button.configure(state=normal if self.busy else disabled)
        self.open_output_button.configure(
            state=normal if not self.busy and (self.last_output_path or self.settings.last_export_folder) else disabled
        )
        self.refresh_period_control_states()

    # --------------------------------------------------------------- help

    def show_how_to_use(self) -> None:
        messagebox.showinfo(
            "How to Use",
            "1. Choose the Parquet data lake folder.\n"
            "2. Click Scan Lake Partitions.\n"
            "3. Choose Stream, Fast, or Both.\n"
            "4. Choose Whole Data, Year, Month, or Specific Day.\n"
            "5. Click Scan Columns and select one column.\n"
            "6. Click Export Distinct Values and choose a CSV path.\n\n"
            "Dates come from source/year/month/day folders, not timestamp columns.",
        )

    def show_about(self) -> None:
        messagebox.showinfo(
            "About",
            f"{APP_TITLE}\n\n"
            "Batch-based PyArrow Parquet reader with a temporary SQLite "
            "distinct-value store.\n\n"
            f"Settings: {SETTINGS_PATH}\nLog: {LOG_PATH}",
        )

    # --------------------------------------------------------------- close

    def on_close(self) -> None:
        self.settings.last_lake_root = self.lake_path_var.get()
        self.settings.last_source = self.source_var.get()
        self.settings.last_period_type = self.period_type_var.get()
        self.settings.geometry = self.root.geometry()
        self.settings.save()
        if self.busy:
            if messagebox.askyesno(
                APP_TITLE,
                "An operation is still running. Cancel it and close when safe?",
            ):
                self.close_after_cancel = True
                self.cancel_current_operation()
            return
        self.root.destroy()


def main() -> None:
    root = tk.Tk()
    app = SmartParquetLakeDistinctExtractorApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
