"""Simple Parquet Exporter

Windows desktop helper to convert raw Parquet files into CSV or Excel.

Workflow:
1. Choose one or more .parquet files.
2. Choose CSV or Excel.
3. Choose combined output or one output per input file.
4. Choose the save file/folder.
5. Export every row and column with progress.

If Excel row limits are exceeded, the app asks whether to split into multiple
sheets or switch to CSV.
"""

from __future__ import annotations

from pathlib import Path
import math
import os
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

try:
    import pandas as pd
except ImportError:
    root = tk.Tk()
    root.withdraw()
    messagebox.showerror(
        "Missing requirement",
        "pandas is not installed.\n\nInstall it with:\npython -m pip install pandas",
    )
    root.destroy()
    raise SystemExit(1)

try:
    import pyarrow.parquet as pq
except ImportError:
    root = tk.Tk()
    root.withdraw()
    messagebox.showerror(
        "Missing requirement",
        "pyarrow is not installed.\n\nInstall it with:\npython -m pip install pyarrow",
    )
    root.destroy()
    raise SystemExit(1)


APP_TITLE = "Simple Parquet Exporter"
CHUNK_SIZE = 100_000
EXCEL_MAX_ROWS = 1_048_576
EXCEL_DATA_ROWS_PER_SHEET = EXCEL_MAX_ROWS - 1


def ensure_openpyxl_available() -> bool:
    try:
        import openpyxl  # noqa: F401
    except ImportError:
        messagebox.showerror(
            "Missing Excel requirement",
            "openpyxl is required for Excel export.\n\n"
            "Install it with:\npython -m pip install openpyxl",
        )
        return False
    return True


def excel_sheet_count(row_count: int) -> int:
    return max(1, math.ceil(row_count / EXCEL_DATA_ROWS_PER_SHEET))


def temp_output_path(output_path: Path) -> Path:
    if output_path.suffix.lower() == ".xlsx":
        return output_path.with_name(f".{output_path.stem}.part.xlsx")
    return output_path.with_name(f".{output_path.name}.part")


def unique_folder_output_path(folder: Path, stem: str, suffix: str) -> Path:
    candidate = folder / f"{stem}{suffix}"
    counter = 1
    while candidate.exists() or temp_output_path(candidate).exists():
        candidate = folder / f"{stem}_{counter}{suffix}"
        counter += 1
    return candidate


class SimpleParquetExporter:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title(APP_TITLE)
        self.root.geometry("900x520")
        self.root.minsize(820, 480)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        self.busy = False
        self.sources: list[dict] = []
        self.output_columns: list[str] = []
        self.output_path: Path | None = None
        self.last_output_folder: Path | None = None
        self.total_rows = 0
        self.total_columns = 0
        self.export_batches = None
        self.export_state = None

        self.input_var = tk.StringVar()
        self.output_var = tk.StringVar()
        self.format_var = tk.StringVar(value="csv")
        self.mode_var = tk.StringVar(value="combined")
        self.info_var = tk.StringVar(value="No Parquet file selected.")
        self.status_var = tk.StringVar(value="Ready.")

        self.configure_styles()
        self.build_ui()
        self.refresh_controls()

    def configure_styles(self) -> None:
        style = ttk.Style(self.root)
        if "vista" in style.theme_names():
            style.theme_use("vista")
        style.configure("Title.TLabel", font=("Segoe UI", 17, "bold"))
        style.configure("Section.TLabelframe.Label", font=("Segoe UI", 10, "bold"))
        style.configure("Action.TButton", font=("Segoe UI", 10, "bold"), padding=8)

    def build_ui(self) -> None:
        outer = ttk.Frame(self.root, padding=16)
        outer.grid(row=0, column=0, sticky="nsew")
        self.root.rowconfigure(0, weight=1)
        self.root.columnconfigure(0, weight=1)
        outer.columnconfigure(0, weight=1)

        ttk.Label(outer, text=APP_TITLE, style="Title.TLabel").grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(
            outer,
            text="Convert every row and column from one or many Parquet files into CSV or Excel.",
        ).grid(row=1, column=0, sticky="w", pady=(2, 14))

        input_frame = ttk.LabelFrame(
            outer, text="1. Choose Parquet file(s)", style="Section.TLabelframe"
        )
        input_frame.grid(row=2, column=0, sticky="ew", pady=(0, 12))
        input_frame.columnconfigure(1, weight=1)

        ttk.Label(input_frame, text="Input:").grid(
            row=0, column=0, sticky="w", padx=(12, 8), pady=(12, 8)
        )
        ttk.Entry(input_frame, textvariable=self.input_var, state="readonly").grid(
            row=0, column=1, sticky="ew", pady=(12, 8)
        )
        self.choose_input_button = ttk.Button(
            input_frame, text="Browse...", command=self.choose_input_files
        )
        self.choose_input_button.grid(row=0, column=2, padx=8, pady=(12, 8))

        ttk.Label(input_frame, textvariable=self.info_var).grid(
            row=1, column=0, columnspan=3, sticky="w", padx=12, pady=(0, 10)
        )

        output_frame = ttk.LabelFrame(
            outer, text="2. Choose output", style="Section.TLabelframe"
        )
        output_frame.grid(row=3, column=0, sticky="ew", pady=(0, 12))
        output_frame.columnconfigure(1, weight=1)

        ttk.Label(output_frame, text="Format:").grid(
            row=0, column=0, sticky="w", padx=(12, 8), pady=(12, 8)
        )
        self.csv_radio = ttk.Radiobutton(
            output_frame,
            text="CSV",
            value="csv",
            variable=self.format_var,
            command=self.reset_output_path,
        )
        self.csv_radio.grid(row=0, column=1, sticky="w", pady=(12, 8))
        self.excel_radio = ttk.Radiobutton(
            output_frame,
            text="Excel (.xlsx)",
            value="excel",
            variable=self.format_var,
            command=self.reset_output_path,
        )
        self.excel_radio.grid(row=0, column=1, sticky="w", padx=(70, 0), pady=(12, 8))

        ttk.Label(output_frame, text="Mode:").grid(
            row=1, column=0, sticky="w", padx=(12, 8), pady=(0, 8)
        )
        self.combined_radio = ttk.Radiobutton(
            output_frame,
            text="Combined single output",
            value="combined",
            variable=self.mode_var,
            command=self.reset_output_path,
        )
        self.combined_radio.grid(row=1, column=1, sticky="w", pady=(0, 8))
        self.separate_radio = ttk.Radiobutton(
            output_frame,
            text="One output per Parquet",
            value="separate",
            variable=self.mode_var,
            command=self.reset_output_path,
        )
        self.separate_radio.grid(row=1, column=1, sticky="w", padx=(180, 0), pady=(0, 8))

        ttk.Label(output_frame, text="Save:").grid(
            row=2, column=0, sticky="w", padx=(12, 8), pady=(0, 12)
        )
        ttk.Entry(output_frame, textvariable=self.output_var, state="readonly").grid(
            row=2, column=1, sticky="ew", pady=(0, 12)
        )
        self.choose_output_button = ttk.Button(
            output_frame, text="Save To...", command=self.choose_output_path
        )
        self.choose_output_button.grid(row=2, column=2, padx=8, pady=(0, 12))

        action_frame = ttk.Frame(outer)
        action_frame.grid(row=4, column=0, sticky="ew", pady=(0, 14))
        action_frame.columnconfigure(0, weight=1)
        self.export_button = ttk.Button(
            action_frame,
            text="Export",
            style="Action.TButton",
            command=self.start_export,
        )
        self.export_button.grid(row=0, column=1, sticky="e")
        self.open_folder_button = ttk.Button(
            action_frame,
            text="Open Output Folder",
            command=self.open_output_folder,
        )
        self.open_folder_button.grid(row=0, column=2, sticky="e", padx=(8, 0))

        progress_frame = ttk.LabelFrame(
            outer, text="Progress", style="Section.TLabelframe"
        )
        progress_frame.grid(row=5, column=0, sticky="ew")
        progress_frame.columnconfigure(0, weight=1)
        self.progress = ttk.Progressbar(progress_frame, mode="determinate", maximum=100)
        self.progress.grid(row=0, column=0, sticky="ew", padx=12, pady=(12, 8))
        ttk.Label(progress_frame, textvariable=self.status_var).grid(
            row=1, column=0, sticky="w", padx=12, pady=(0, 12)
        )

    def refresh_controls(self) -> None:
        state = "disabled" if self.busy else "normal"
        has_input = bool(self.sources)
        has_output = self.output_path is not None

        self.choose_input_button.configure(state=state)
        self.csv_radio.configure(state=state)
        self.excel_radio.configure(state=state)
        self.combined_radio.configure(state=state)
        self.separate_radio.configure(state=state)
        self.choose_output_button.configure(
            state="disabled" if self.busy or not has_input else "normal"
        )
        self.export_button.configure(
            state="disabled" if self.busy or not has_input or not has_output else "normal"
        )
        self.open_folder_button.configure(
            state="disabled" if self.busy or not self.last_output_folder else "normal"
        )

    def choose_input_files(self) -> None:
        selected = filedialog.askopenfilenames(
            title="Choose Parquet file(s)",
            filetypes=(("Parquet files", "*.parquet"), ("All files", "*.*")),
        )
        if not selected:
            return

        paths = [Path(value) for value in selected]
        invalid = [path for path in paths if path.suffix.lower() != ".parquet"]
        if invalid:
            messagebox.showerror(
                "Invalid file",
                "Please choose only .parquet files.\n\n"
                f"Invalid example:\n{invalid[0]}",
            )
            return

        sources = []
        output_columns = []
        seen_columns = set()
        try:
            for path in paths:
                parquet_file = pq.ParquetFile(path)
                columns = list(parquet_file.schema_arrow.names)
                for column in columns:
                    if column not in seen_columns:
                        output_columns.append(column)
                        seen_columns.add(column)
                sources.append(
                    {
                        "path": path,
                        "parquet_file": parquet_file,
                        "rows": int(parquet_file.metadata.num_rows),
                        "columns": columns,
                        "row_groups": int(parquet_file.metadata.num_row_groups),
                    }
                )
        except Exception as exc:
            messagebox.showerror("Parquet read failed", f"Could not read file:\n\n{exc}")
            return

        self.sources = sources
        self.output_columns = output_columns
        self.total_rows = sum(item["rows"] for item in sources)
        self.total_columns = len(output_columns)
        self.input_var.set(self.describe_inputs())
        self.info_var.set(
            f"{len(sources):,} file(s) | {self.total_rows:,} total rows | "
            f"{self.total_columns:,} combined column(s)"
        )
        if len(sources) == 1:
            self.mode_var.set("combined")
        self.reset_output_path()
        self.status_var.set("Ready.")
        self.progress.configure(value=0)
        self.refresh_controls()

    def describe_inputs(self) -> str:
        if not self.sources:
            return ""
        if len(self.sources) == 1:
            return str(self.sources[0]["path"])
        first = self.sources[0]["path"].name
        return f"{len(self.sources):,} files selected | first: {first}"

    def reset_output_path(self) -> None:
        self.output_path = None
        self.output_var.set("")
        self.refresh_controls()

    def choose_output_path(self) -> None:
        if not self.sources:
            messagebox.showerror("No input", "Choose Parquet file(s) first.")
            return

        if self.mode_var.get() == "separate":
            selected = filedialog.askdirectory(
                title="Choose output folder",
                initialdir=str(self.sources[0]["path"].parent),
            )
            if not selected:
                return
            self.output_path = Path(selected)
            self.output_var.set(str(self.output_path))
            self.refresh_controls()
            return

        export_format = self.format_var.get()
        suffix = ".xlsx" if export_format == "excel" else ".csv"
        filetypes = (
            (("Excel workbook", "*.xlsx"), ("All files", "*.*"))
            if export_format == "excel"
            else (("CSV file", "*.csv"), ("All files", "*.*"))
        )
        initial_stem = self.sources[0]["path"].stem
        if len(self.sources) > 1:
            initial_stem = f"{initial_stem}_combined"
        selected = filedialog.asksaveasfilename(
            title="Choose output file",
            initialdir=str(self.sources[0]["path"].parent),
            initialfile=f"{initial_stem}{suffix}",
            defaultextension=suffix,
            filetypes=filetypes,
        )
        if not selected:
            return

        output_path = Path(selected)
        if output_path.suffix.lower() != suffix:
            output_path = output_path.with_suffix(suffix)
        self.output_path = output_path
        self.output_var.set(str(output_path))
        self.refresh_controls()

    def largest_excel_output_rows(self) -> int:
        if self.mode_var.get() == "combined":
            return self.total_rows
        return max((source["rows"] for source in self.sources), default=0)

    def resolve_excel_limit_choice(self) -> bool | None:
        """Return split_excel, or None when the user cancels."""
        if self.format_var.get() != "excel":
            return False

        if not ensure_openpyxl_available():
            return None

        largest_output_rows = self.largest_excel_output_rows()
        if largest_output_rows <= EXCEL_DATA_ROWS_PER_SHEET:
            return False

        sheets = excel_sheet_count(largest_output_rows)
        choice = messagebox.askyesnocancel(
            "Excel row limit",
            (
                f"At least one output will have {largest_output_rows:,} rows.\n\n"
                f"Excel allows {EXCEL_DATA_ROWS_PER_SHEET:,} data rows per sheet.\n\n"
                f"Yes: split large workbook(s) across about {sheets:,} sheets\n"
                "No: switch to CSV export instead\n"
                "Cancel: stop"
            ),
        )
        if choice is None:
            return None

        if choice is False:
            self.format_var.set("csv")
            if self.mode_var.get() == "combined":
                self.output_path = None
                self.output_var.set("")
                messagebox.showinfo(
                    "Choose CSV path",
                    "Please choose where to save the combined CSV file.",
                )
                self.choose_output_path()
                if not self.output_path:
                    return None
            return False

        return True

    def start_export(self) -> None:
        if self.busy:
            return
        if not self.sources or not self.output_path:
            messagebox.showerror("Missing selection", "Choose input and output first.")
            return

        split_excel = self.resolve_excel_limit_choice()
        if split_excel is None:
            return

        self.export_state = {
            "format": self.format_var.get(),
            "mode": self.mode_var.get(),
            "split_excel": split_excel,
            "processed": 0,
            "file_index": -1,
            "current_source": None,
            "context": None,
            "created_paths": [],
        }

        self.busy = True
        self.progress.configure(value=0)
        self.status_var.set("Export started...")
        self.refresh_controls()

        try:
            if self.export_state["mode"] == "combined":
                self.open_output_context(
                    final_path=self.output_path,
                    columns=self.output_columns,
                )
            self.start_next_file()
        except Exception as exc:
            self.handle_export_error(exc)

    def start_next_file(self) -> None:
        state = self.export_state
        state["file_index"] += 1

        if state["file_index"] >= len(self.sources):
            self.finish_export()
            return

        source = self.sources[state["file_index"]]
        state["current_source"] = source

        if state["mode"] == "separate":
            suffix = ".xlsx" if state["format"] == "excel" else ".csv"
            final_path = unique_folder_output_path(self.output_path, source["path"].stem, suffix)
            self.open_output_context(final_path=final_path, columns=source["columns"])

        self.export_batches = source["parquet_file"].iter_batches(
            batch_size=CHUNK_SIZE,
            use_threads=True,
        )
        self.root.after(1, self.export_next_batch)

    def open_output_context(self, final_path: Path, columns: list[str]) -> None:
        temp_path = temp_output_path(final_path)
        if temp_path.exists():
            temp_path.unlink()
        self.export_state["context"] = {
            "temp_path": temp_path,
            "final_path": final_path,
            "columns": columns,
            "header_written": False,
            "excel_writer": None,
            "excel_sheet_index": 1,
            "excel_sheet_rows": 0,
            "excel_sheets_used": 0,
        }

    def export_next_batch(self) -> None:
        try:
            batch = next(self.export_batches)
        except StopIteration:
            self.finish_current_file()
            return
        except Exception as exc:
            self.handle_export_error(exc)
            return

        try:
            rows = batch.to_pandas()
            if self.export_state["mode"] == "combined":
                rows = rows.reindex(columns=self.output_columns)
            self.write_rows(rows)
            self.export_state["processed"] += len(rows)
            percent = 0 if self.total_rows == 0 else (
                self.export_state["processed"] / self.total_rows * 100
            )
            current = self.export_state["current_source"]
            self.progress.configure(value=min(100, percent))
            self.status_var.set(
                f"File {self.export_state['file_index'] + 1:,}/{len(self.sources):,}: "
                f"{current['path'].name} | "
                f"{self.export_state['processed']:,} of {self.total_rows:,} rows "
                f"({percent:.1f}%)"
            )
        except Exception as exc:
            self.handle_export_error(exc)
            return

        self.root.after(1, self.export_next_batch)

    def write_rows(self, rows: pd.DataFrame) -> None:
        state = self.export_state
        context = state["context"]
        if state["format"] == "csv":
            rows.to_csv(
                context["temp_path"],
                mode="a",
                index=False,
                header=not context["header_written"],
                encoding="utf-8-sig",
            )
            context["header_written"] = True
            return

        if context["excel_writer"] is None:
            context["excel_writer"] = pd.ExcelWriter(
                context["temp_path"], engine="openpyxl"
            )

        start = 0
        total = len(rows)
        while start < total or (total == 0 and not context["header_written"]):
            remaining_capacity = EXCEL_DATA_ROWS_PER_SHEET - context["excel_sheet_rows"]
            if remaining_capacity <= 0:
                if not state["split_excel"]:
                    raise ValueError(
                        "Excel row limit exceeded. Export as CSV or allow split sheets."
                    )
                context["excel_sheet_index"] += 1
                context["excel_sheet_rows"] = 0
                remaining_capacity = EXCEL_DATA_ROWS_PER_SHEET

            part = rows.iloc[start : start + remaining_capacity]
            sheet_name = f"Data_{context['excel_sheet_index']:03d}"
            header = context["excel_sheet_rows"] == 0
            startrow = 0 if header else context["excel_sheet_rows"] + 1
            part.to_excel(
                context["excel_writer"],
                sheet_name=sheet_name,
                index=False,
                header=header,
                startrow=startrow,
            )
            context["excel_sheet_rows"] += len(part)
            context["excel_sheets_used"] = max(
                context["excel_sheets_used"], context["excel_sheet_index"]
            )
            context["header_written"] = True
            if total == 0:
                break
            start += len(part)

    def finish_current_file(self) -> None:
        self.export_batches = None
        if self.export_state["mode"] == "separate":
            self.finalize_output_context()
        self.start_next_file()

    def finish_export(self) -> None:
        try:
            if self.export_state["mode"] == "combined":
                self.finalize_output_context()
        except Exception as exc:
            self.handle_export_error(exc)
            return

        created_paths = list(self.export_state["created_paths"])
        rows = self.export_state["processed"]
        mode = self.export_state["mode"]
        self.last_output_folder = self.output_path if mode == "separate" else created_paths[0].parent
        self.finish_after_export("Export complete.")

        if mode == "combined":
            message = f"Export complete: {rows:,} rows saved to {created_paths[0]}"
        else:
            message = (
                f"Export complete: {rows:,} rows saved into "
                f"{len(created_paths):,} output file(s).\n\nFolder:\n{self.output_path}"
            )
        open_now = messagebox.askyesno(
            "Export complete",
            f"{message}\n\nOpen output folder now?",
        )
        if open_now:
            self.open_output_folder()

    def finalize_output_context(self) -> None:
        context = self.export_state["context"]
        if context is None:
            return

        if not context["header_written"]:
            self.write_rows(pd.DataFrame(columns=context["columns"]))

        writer = context.get("excel_writer")
        if writer is not None:
            writer.close()
            context["excel_writer"] = None

        final_path = context["final_path"]
        temp_path = context["temp_path"]
        if final_path.exists():
            overwrite = messagebox.askyesno(
                "Overwrite file?",
                f"This file already exists:\n\n{final_path}\n\nOverwrite it?",
            )
            if not overwrite:
                temp_path.unlink(missing_ok=True)
                raise RuntimeError("Export cancelled. Existing file was not overwritten.")
            final_path.unlink()
        temp_path.rename(final_path)
        self.export_state["created_paths"].append(final_path)
        self.export_state["context"] = None

    def finish_after_export(self, message: str) -> None:
        self.export_batches = None
        self.export_state = None
        self.busy = False
        self.progress.configure(value=100 if message == "Export complete." else 0)
        self.status_var.set(message)
        self.refresh_controls()

    def open_output_folder(self) -> None:
        if not self.last_output_folder:
            messagebox.showinfo("No output yet", "No exported folder is available yet.")
            return
        if not self.last_output_folder.exists():
            messagebox.showerror(
                "Folder not found",
                f"Output folder does not exist:\n\n{self.last_output_folder}",
            )
            return
        os.startfile(self.last_output_folder)

    def cleanup_temp_files(self) -> None:
        if not self.export_state:
            return
        context = self.export_state.get("context")
        if not context:
            return
        try:
            writer = context.get("excel_writer")
            if writer is not None:
                writer.close()
                context["excel_writer"] = None
        except Exception:
            pass
        temp_path = context.get("temp_path")
        if temp_path:
            try:
                Path(temp_path).unlink(missing_ok=True)
            except OSError:
                pass

    def handle_export_error(self, exc: Exception) -> None:
        self.cleanup_temp_files()
        self.export_batches = None
        self.export_state = None
        self.busy = False
        self.progress.configure(value=0)
        self.status_var.set("Export failed.")
        self.refresh_controls()
        messagebox.showerror("Export failed", str(exc))

    def on_close(self) -> None:
        if self.busy:
            close = messagebox.askyesno(
                "Export running",
                "An export is still running. Close and discard partial output?",
            )
            if not close:
                return
            self.cleanup_temp_files()
        self.root.destroy()


def main() -> None:
    root = tk.Tk()
    SimpleParquetExporter(root)
    root.mainloop()


if __name__ == "__main__":
    main()
