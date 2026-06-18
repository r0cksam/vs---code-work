"""Smart CSV Date Extractor

A beginner-friendly Windows desktop application that scans a large CSV file,
finds dates in a timestamp column, and exports rows for one selected date.
"""

from pathlib import Path
from datetime import datetime
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

try:
    import pandas as pd
except ImportError:
    # Show a GUI error even when the script is started by double-clicking.
    _root = tk.Tk()
    _root.withdraw()
    messagebox.showerror(
        "Missing requirement",
        "pandas is not installed.\n\nInstall it with:\npython -m pip install pandas",
    )
    _root.destroy()
    raise SystemExit(1)


APP_TITLE = "Smart CSV Date Extractor"
PREFERRED_DATE_COLUMN = "Timestamp (IST)"
DATE_COLUMN_KEYWORDS = ("timestamp", "date", "time")
CHUNK_SIZE = 100_000
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


def format_datetime_values_as_dates(series):
    """Return ISO date strings plus blank/invalid counts for a pandas Series.

    The common format is attempted first for speed. Remaining values are then
    parsed more flexibly. Invalid values become pandas NA and are not exported.
    """
    cleaned = series.astype("string").str.strip()
    nonempty = cleaned.notna() & cleaned.ne("")

    # A nullable string result preserves the original row index for masking.
    date_strings = pd.Series(pd.NA, index=cleaned.index, dtype="string")

    # Fast path for the format shown in the question.
    parsed_common = pd.to_datetime(
        cleaned.where(nonempty),
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce",
    )
    common_mask = parsed_common.notna()
    if common_mask.any():
        date_strings.loc[common_mask] = parsed_common.loc[common_mask].dt.strftime(
            "%Y-%m-%d"
        )

    # Flexible fallback for date-only values, fractional seconds, ISO T values,
    # and other formats pandas can safely recognize.
    remaining_mask = nonempty & ~common_mask
    if remaining_mask.any():
        remaining_values = cleaned.loc[remaining_mask]

        try:
            parsed_fallback = pd.to_datetime(
                remaining_values,
                format="mixed",
                errors="coerce",
            )

            # pandas versions before format="mixed" support may silently parse
            # nothing when errors="coerce" is used, so retry without it.
            if parsed_fallback.notna().sum() == 0:
                parsed_fallback = pd.to_datetime(
                    remaining_values,
                    errors="coerce",
                )
        except (TypeError, ValueError):
            parsed_fallback = pd.to_datetime(
                remaining_values,
                errors="coerce",
            )

        try:
            fallback_dates = parsed_fallback.dt.strftime("%Y-%m-%d")
        except AttributeError:
            # This can occur with unusual mixed timezone values. Formatting each
            # parsed scalar preserves its displayed/local calendar date.
            fallback_dates = parsed_fallback.map(
                lambda value: (
                    value.strftime("%Y-%m-%d") if not pd.isna(value) else pd.NA
                )
            )

        date_strings.loc[remaining_mask] = fallback_dates.astype("string")

    blank_count = int((~nonempty).sum())
    invalid_count = int((nonempty & date_strings.isna()).sum())
    return date_strings, blank_count, invalid_count


def unique_output_path(source_path, selected_date):
    """Build a non-overwriting output path in the source CSV's folder."""
    source_path = Path(source_path)
    base_name = f"{source_path.stem}_{selected_date}"
    candidate = source_path.with_name(f"{base_name}.csv")

    counter = 1
    while candidate.exists():
        candidate = source_path.with_name(f"{base_name}_{counter}.csv")
        counter += 1

    return candidate


class SmartCSVDateExtractor:
    """Main Tkinter application."""

    def __init__(self, root):
        self.root = root
        self.root.title(APP_TITLE)
        self.root.geometry("940x720")
        self.root.minsize(820, 620)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        # Application data/state.
        self.busy = False
        self.column_choices = []
        self.date_counts = {}
        self.date_groups = {}
        self.tree_item_info = {}

        # Chunk-reader state used by Tkinter's event loop.
        self.scan_reader = None
        self.export_reader = None
        self.scan_state = None
        self.export_state = None

        # Tkinter variables.
        self.file_path_var = tk.StringVar()
        self.date_column_var = tk.StringVar()
        self.year_var = tk.StringVar()
        self.month_var = tk.StringVar()
        self.date_var = tk.StringVar()
        self.activity_var = tk.StringVar(value="Ready. Choose a CSV file to begin.")

        self.configure_styles()
        self.build_ui()
        self.refresh_control_states()
        self.append_status("Application ready.")

    # ------------------------------------------------------------------ UI --

    def configure_styles(self):
        style = ttk.Style(self.root)
        available_themes = style.theme_names()
        if "vista" in available_themes:
            style.theme_use("vista")

        style.configure("Title.TLabel", font=("Segoe UI", 17, "bold"))
        style.configure("Subtitle.TLabel", font=("Segoe UI", 10))
        style.configure("Section.TLabelframe.Label", font=("Segoe UI", 10, "bold"))
        style.configure("Action.TButton", font=("Segoe UI", 10, "bold"), padding=8)

    def build_ui(self):
        outer = ttk.Frame(self.root, padding=16)
        outer.grid(row=0, column=0, sticky="nsew")
        self.root.rowconfigure(0, weight=1)
        self.root.columnconfigure(0, weight=1)

        outer.columnconfigure(0, weight=1)
        outer.rowconfigure(3, weight=1)
        outer.rowconfigure(5, weight=1)

        ttk.Label(outer, text=APP_TITLE, style="Title.TLabel").grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(
            outer,
            text=(
                "Choose a CSV, scan its timestamp column, select one available "
                "date, and export matching rows."
            ),
            style="Subtitle.TLabel",
        ).grid(row=1, column=0, sticky="w", pady=(2, 14))

        # File and date-column controls.
        file_frame = ttk.LabelFrame(
            outer, text="1. CSV file and date column", style="Section.TLabelframe"
        )
        file_frame.grid(row=2, column=0, sticky="ew", pady=(0, 12))
        file_frame.columnconfigure(1, weight=1)

        ttk.Label(file_frame, text="CSV file:").grid(
            row=0, column=0, sticky="w", padx=(12, 8), pady=(12, 6)
        )
        self.path_entry = ttk.Entry(
            file_frame, textvariable=self.file_path_var, state="readonly"
        )
        self.path_entry.grid(row=0, column=1, sticky="ew", pady=(12, 6))

        self.choose_button = ttk.Button(
            file_frame, text="Choose CSV File", command=self.choose_csv_file
        )
        self.choose_button.grid(row=0, column=2, padx=8, pady=(12, 6))

        self.scan_button = ttk.Button(
            file_frame,
            text="Scan Dates",
            command=self.scan_dates,
            style="Action.TButton",
        )
        self.scan_button.grid(row=0, column=3, padx=(0, 12), pady=(12, 6))

        ttk.Label(file_frame, text="Date column:").grid(
            row=1, column=0, sticky="w", padx=(12, 8), pady=(6, 12)
        )
        self.column_combo = ttk.Combobox(
            file_frame,
            textvariable=self.date_column_var,
            state="disabled",
            width=42,
        )
        self.column_combo.grid(
            row=1, column=1, columnspan=3, sticky="ew", pady=(6, 12), padx=(0, 12)
        )
        self.column_combo.bind("<<ComboboxSelected>>", self.on_date_column_changed)

        # Date selectors and grouped date tree.
        dates_frame = ttk.LabelFrame(
            outer, text="2. Select an available date", style="Section.TLabelframe"
        )
        dates_frame.grid(row=3, column=0, sticky="nsew", pady=(0, 12))
        dates_frame.columnconfigure(1, weight=1)
        dates_frame.rowconfigure(0, weight=1)

        selector_frame = ttk.Frame(dates_frame, padding=12)
        selector_frame.grid(row=0, column=0, sticky="nsw")
        selector_frame.columnconfigure(0, weight=1)

        ttk.Label(selector_frame, text="Year").grid(row=0, column=0, sticky="w")
        self.year_combo = ttk.Combobox(
            selector_frame,
            textvariable=self.year_var,
            state="disabled",
            width=22,
        )
        self.year_combo.grid(row=1, column=0, sticky="ew", pady=(3, 12))
        self.year_combo.bind("<<ComboboxSelected>>", self.on_year_selected)

        ttk.Label(selector_frame, text="Month").grid(row=2, column=0, sticky="w")
        self.month_combo = ttk.Combobox(
            selector_frame,
            textvariable=self.month_var,
            state="disabled",
            width=22,
        )
        self.month_combo.grid(row=3, column=0, sticky="ew", pady=(3, 12))
        self.month_combo.bind("<<ComboboxSelected>>", self.on_month_selected)

        ttk.Label(selector_frame, text="Date").grid(row=4, column=0, sticky="w")
        self.date_combo = ttk.Combobox(
            selector_frame,
            textvariable=self.date_var,
            state="disabled",
            width=22,
        )
        self.date_combo.grid(row=5, column=0, sticky="ew", pady=(3, 12))

        ttk.Label(
            selector_frame,
            text="Tip: selecting a date in the tree also fills these menus.",
            wraplength=210,
            justify="left",
        ).grid(row=6, column=0, sticky="w", pady=(4, 0))

        tree_container = ttk.Frame(dates_frame, padding=(0, 12, 12, 12))
        tree_container.grid(row=0, column=1, sticky="nsew")
        tree_container.rowconfigure(0, weight=1)
        tree_container.columnconfigure(0, weight=1)

        self.date_tree = ttk.Treeview(
            tree_container,
            columns=("rows",),
            show="tree headings",
            selectmode="browse",
        )
        self.date_tree.heading("#0", text="Available Dates: Year → Month → Date")
        self.date_tree.heading("rows", text="Matching rows")
        self.date_tree.column("#0", width=430, minwidth=280, stretch=True)
        self.date_tree.column("rows", width=115, minwidth=95, anchor="e", stretch=False)
        self.date_tree.grid(row=0, column=0, sticky="nsew")
        self.date_tree.bind("<<TreeviewSelect>>", self.on_tree_selection)

        tree_scrollbar = ttk.Scrollbar(
            tree_container, orient="vertical", command=self.date_tree.yview
        )
        tree_scrollbar.grid(row=0, column=1, sticky="ns")
        self.date_tree.configure(yscrollcommand=tree_scrollbar.set)

        # Export action.
        action_frame = ttk.Frame(outer)
        action_frame.grid(row=4, column=0, sticky="ew", pady=(0, 12))
        action_frame.columnconfigure(0, weight=1)

        self.export_button = ttk.Button(
            action_frame,
            text="Export Selected Date",
            command=self.export_selected_date,
            style="Action.TButton",
        )
        self.export_button.grid(row=0, column=1, sticky="e")

        # Progress and status log.
        status_frame = ttk.LabelFrame(
            outer, text="Status and messages", style="Section.TLabelframe"
        )
        status_frame.grid(row=5, column=0, sticky="nsew")
        status_frame.columnconfigure(0, weight=1)
        status_frame.rowconfigure(2, weight=1)

        ttk.Label(status_frame, textvariable=self.activity_var).grid(
            row=0, column=0, sticky="ew", padx=12, pady=(10, 4)
        )
        self.progress = ttk.Progressbar(status_frame, mode="indeterminate")
        self.progress.grid(row=1, column=0, sticky="ew", padx=12, pady=(0, 8))

        log_container = ttk.Frame(status_frame)
        log_container.grid(row=2, column=0, sticky="nsew", padx=12, pady=(0, 12))
        log_container.rowconfigure(0, weight=1)
        log_container.columnconfigure(0, weight=1)

        self.status_text = tk.Text(
            log_container,
            height=7,
            wrap="word",
            state="disabled",
            font=("Consolas", 9),
        )
        self.status_text.grid(row=0, column=0, sticky="nsew")

        status_scrollbar = ttk.Scrollbar(
            log_container, orient="vertical", command=self.status_text.yview
        )
        status_scrollbar.grid(row=0, column=1, sticky="ns")
        self.status_text.configure(yscrollcommand=status_scrollbar.set)

    # ----------------------------------------------------------- UI helpers --

    def append_status(self, message):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.status_text.configure(state="normal")
        self.status_text.insert("end", f"[{timestamp}] {message}\n")
        self.status_text.see("end")
        self.status_text.configure(state="disabled")

    def set_column_choices(self, choices, selected=""):
        self.column_choices = list(choices)
        self.column_combo.configure(values=self.column_choices)
        self.date_column_var.set(selected)
        self.refresh_control_states()

    def start_operation(self, activity_message):
        self.busy = True
        self.activity_var.set(activity_message)
        self.progress.start(12)
        self.refresh_control_states()

    def finish_operation(self, activity_message="Ready."):
        self.busy = False
        self.progress.stop()
        self.activity_var.set(activity_message)
        self.refresh_control_states()

    def refresh_control_states(self):
        if self.busy:
            self.choose_button.configure(state="disabled")
            self.scan_button.configure(state="disabled")
            self.export_button.configure(state="disabled")
            self.column_combo.configure(state="disabled")
            self.year_combo.configure(state="disabled")
            self.month_combo.configure(state="disabled")
            self.date_combo.configure(state="disabled")
            return

        self.choose_button.configure(state="normal")
        self.scan_button.configure(
            state="normal" if self.file_path_var.get().strip() else "disabled"
        )
        self.column_combo.configure(
            state="readonly" if self.column_choices else "disabled"
        )

        has_dates = bool(self.date_counts)
        self.export_button.configure(state="normal" if has_dates else "disabled")
        self.year_combo.configure(state="readonly" if has_dates else "disabled")
        self.month_combo.configure(state="readonly" if has_dates else "disabled")
        self.date_combo.configure(state="readonly" if has_dates else "disabled")

    def clear_date_results(self):
        self.date_counts = {}
        self.date_groups = {}
        self.tree_item_info = {}

        self.year_var.set("")
        self.month_var.set("")
        self.date_var.set("")
        self.year_combo.configure(values=[])
        self.month_combo.configure(values=[])
        self.date_combo.configure(values=[])

        for item in self.date_tree.get_children(""):
            self.date_tree.delete(item)

        self.refresh_control_states()

    # ------------------------------------------------------------ File setup --

    def choose_csv_file(self):
        selected = filedialog.askopenfilename(
            title="Choose a CSV file",
            filetypes=(("CSV files", "*.csv"), ("All files", "*.*")),
        )
        if not selected:
            return

        self.file_path_var.set(selected)
        self.date_column_var.set("")
        self.column_choices = []
        self.column_combo.configure(values=[])
        self.clear_date_results()
        self.activity_var.set("File selected. Click Scan Dates.")
        self.append_status(f"Selected file: {selected}")
        self.refresh_control_states()

    def validate_source_path(self):
        raw_path = self.file_path_var.get().strip()
        if not raw_path:
            messagebox.showerror("No file selected", "Please choose a CSV file first.")
            return None

        source_path = Path(raw_path)
        if not source_path.exists() or not source_path.is_file():
            messagebox.showerror(
                "Invalid file path",
                "The selected CSV file does not exist or is not a valid file.",
            )
            self.append_status("Error: selected file path is invalid.")
            return None

        return source_path

    @staticmethod
    def read_header(source_path):
        return pd.read_csv(
            source_path,
            nrows=0,
            encoding="utf-8-sig",
            on_bad_lines="skip",
        )

    @staticmethod
    def create_chunk_reader(source_path, usecols=None):
        return pd.read_csv(
            source_path,
            usecols=usecols,
            chunksize=CHUNK_SIZE,
            dtype=str,
            keep_default_na=False,
            encoding="utf-8-sig",
            on_bad_lines="skip",
            low_memory=False,
        )

    def choose_or_detect_date_column(self, columns):
        columns = [str(column) for column in columns]
        current_selection = self.date_column_var.get().strip()

        # The exact requested name always has priority.
        if PREFERRED_DATE_COLUMN in columns:
            self.set_column_choices([PREFERRED_DATE_COLUMN], PREFERRED_DATE_COLUMN)
            return PREFERRED_DATE_COLUMN

        candidates = [
            column
            for column in columns
            if any(keyword in column.casefold() for keyword in DATE_COLUMN_KEYWORDS)
        ]

        # A manual choice from an earlier prompt is accepted on the next scan.
        if current_selection and current_selection in columns:
            if candidates:
                self.set_column_choices(candidates, current_selection)
            else:
                self.set_column_choices(columns, current_selection)
            return current_selection

        if len(candidates) == 1:
            self.set_column_choices(candidates, candidates[0])
            return candidates[0]

        if len(candidates) > 1:
            self.set_column_choices(candidates, "")
            self.activity_var.set("Choose the correct date column, then click Scan Dates again.")
            self.append_status(
                "Multiple possible date columns found: " + ", ".join(candidates)
            )
            messagebox.showinfo(
                "Choose a date column",
                "More than one possible timestamp/date column was found.\n\n"
                "Select the correct column from the Date column dropdown, then "
                "click Scan Dates again.",
            )
            return None

        # No likely column was found. Show every column so the user can recover
        # manually, while still reporting the missing timestamp-column problem.
        self.set_column_choices(columns, "")
        self.activity_var.set("No likely date column found. Choose one manually.")
        self.append_status(
            "No column name contained timestamp, date, or time. Manual selection enabled."
        )
        messagebox.showerror(
            "Date column not found",
            "No likely timestamp/date column was found.\n\n"
            "Choose the correct column manually from the Date column dropdown, "
            "then click Scan Dates again.",
        )
        return None

    def on_date_column_changed(self, _event=None):
        if self.busy:
            return
        if self.date_counts:
            self.clear_date_results()
            self.activity_var.set("Date column changed. Click Scan Dates again.")
            self.append_status("Date column changed; previous scan results were cleared.")

    # --------------------------------------------------------------- Scanning --

    def scan_dates(self):
        if self.busy:
            return

        source_path = self.validate_source_path()
        if source_path is None:
            return

        try:
            header = self.read_header(source_path)
            columns = list(header.columns)
            if not columns:
                raise pd.errors.EmptyDataError("No columns were found.")

            date_column = self.choose_or_detect_date_column(columns)
            if date_column is None:
                return

            self.clear_date_results()
            self.scan_reader = self.create_chunk_reader(
                source_path, usecols=[date_column]
            )
            self.scan_state = {
                "source_path": source_path,
                "date_column": date_column,
                "rows": 0,
                "valid": 0,
                "blank": 0,
                "invalid": 0,
                "chunks": 0,
                "date_counts": {},
            }

            self.append_status(
                f"Scanning dates in column '{date_column}' using {CHUNK_SIZE:,}-row chunks."
            )
            self.start_operation("Scanning CSV dates...")
            self.root.after(1, self.scan_next_chunk)

        except Exception as exc:
            self.handle_operation_error("scan", exc)

    def scan_next_chunk(self):
        try:
            chunk = next(self.scan_reader)
        except StopIteration:
            self.finish_scan()
            return
        except Exception as exc:
            self.handle_operation_error("scan", exc)
            return

        try:
            state = self.scan_state
            date_column = state["date_column"]
            date_strings, blank_count, invalid_count = format_datetime_values_as_dates(
                chunk[date_column]
            )

            value_counts = date_strings.dropna().value_counts()
            for date_text, count in value_counts.items():
                date_text = str(date_text)
                state["date_counts"][date_text] = (
                    state["date_counts"].get(date_text, 0) + int(count)
                )

            state["chunks"] += 1
            state["rows"] += len(chunk)
            state["blank"] += blank_count
            state["invalid"] += invalid_count
            state["valid"] += len(chunk) - blank_count - invalid_count

            self.activity_var.set(
                f"Scanning... {state['rows']:,} rows checked; "
                f"{len(state['date_counts']):,} distinct dates found."
            )
            if state["chunks"] == 1 or state["chunks"] % 10 == 0:
                self.append_status(
                    f"Scan progress: {state['rows']:,} rows checked."
                )

            # Return control to Tkinter between chunks so the UI can repaint.
            self.root.after(1, self.scan_next_chunk)

        except Exception as exc:
            self.handle_operation_error("scan", exc)

    def finish_scan(self):
        self.close_reader("scan")
        state = self.scan_state

        if not state or not state["date_counts"]:
            self.scan_state = None
            self.clear_date_results()
            self.finish_operation("Scan finished: no valid dates found.")
            self.append_status("Error: no valid dates were found in the selected column.")
            messagebox.showerror(
                "No valid dates found",
                "No valid dates were found in the selected date column.\n\n"
                "Check that the correct column is selected and that its values "
                "contain recognizable dates.",
            )
            return

        self.date_counts = dict(sorted(state["date_counts"].items()))
        self.build_date_groups()
        self.populate_date_controls()

        ignored = state["blank"] + state["invalid"]
        result_message = (
            f"Scan complete: {state['rows']:,} rows checked, "
            f"{state['valid']:,} valid timestamp rows, "
            f"{len(self.date_counts):,} distinct dates."
        )
        if ignored:
            result_message += (
                f" Ignored {state['blank']:,} blank and "
                f"{state['invalid']:,} unparseable timestamp values."
            )

        self.scan_state = None
        self.finish_operation(
            f"Scan complete. Select one of {len(self.date_counts):,} available dates."
        )
        self.append_status(result_message)

    def build_date_groups(self):
        groups = {}
        for date_text in self.date_counts:
            year_text, month_text, _day_text = date_text.split("-")
            year = int(year_text)
            month = int(month_text)
            groups.setdefault(year, {}).setdefault(month, []).append(date_text)

        for year in groups:
            for month in groups[year]:
                groups[year][month].sort()

        self.date_groups = groups

    def populate_date_controls(self):
        years = [str(year) for year in sorted(self.date_groups)]
        self.year_combo.configure(values=years)
        self.month_combo.configure(values=[])
        self.date_combo.configure(values=[])
        self.year_var.set("")
        self.month_var.set("")
        self.date_var.set("")

        self.tree_item_info = {}
        for item in self.date_tree.get_children(""):
            self.date_tree.delete(item)

        for year in sorted(self.date_groups):
            year_id = self.date_tree.insert(
                "", "end", text=f"Year: {year}", values=("",), open=True
            )
            self.tree_item_info[year_id] = ("year", year)

            for month in sorted(self.date_groups[year]):
                month_name = MONTH_NAMES[month]
                month_id = self.date_tree.insert(
                    year_id,
                    "end",
                    text=f"Month: {month_name}",
                    values=("",),
                    open=True,
                )
                self.tree_item_info[month_id] = ("month", year, month)

                for date_text in self.date_groups[year][month]:
                    date_id = self.date_tree.insert(
                        month_id,
                        "end",
                        text=f"Date: {date_text}",
                        values=(f"{self.date_counts[date_text]:,}",),
                    )
                    self.tree_item_info[date_id] = (
                        "date",
                        year,
                        month,
                        date_text,
                    )

        self.refresh_control_states()

    # --------------------------------------------------------- Date selection --

    @staticmethod
    def month_display(month):
        return f"{month:02d} - {MONTH_NAMES[month]}"

    @staticmethod
    def month_number_from_display(display_text):
        try:
            return int(display_text.split("-", 1)[0].strip())
        except (ValueError, AttributeError):
            return None

    def on_year_selected(self, _event=None):
        try:
            year = int(self.year_var.get())
        except ValueError:
            return

        months = [
            self.month_display(month)
            for month in sorted(self.date_groups.get(year, {}))
        ]
        self.month_combo.configure(values=months)
        self.date_combo.configure(values=[])
        self.month_var.set("")
        self.date_var.set("")

    def on_month_selected(self, _event=None):
        try:
            year = int(self.year_var.get())
        except ValueError:
            return

        month = self.month_number_from_display(self.month_var.get())
        if month is None:
            return

        dates = self.date_groups.get(year, {}).get(month, [])
        self.date_combo.configure(values=dates)
        self.date_var.set("")

    def on_tree_selection(self, _event=None):
        if self.busy:
            return

        selected_items = self.date_tree.selection()
        if not selected_items:
            return

        info = self.tree_item_info.get(selected_items[0])
        if not info:
            return

        item_type = info[0]
        if item_type == "year":
            year = info[1]
            self.year_var.set(str(year))
            self.on_year_selected()
        elif item_type == "month":
            year, month = info[1], info[2]
            self.year_var.set(str(year))
            self.on_year_selected()
            self.month_var.set(self.month_display(month))
            self.on_month_selected()
        elif item_type == "date":
            year, month, date_text = info[1], info[2], info[3]
            self.year_var.set(str(year))
            self.on_year_selected()
            self.month_var.set(self.month_display(month))
            self.on_month_selected()
            self.date_var.set(date_text)

    # --------------------------------------------------------------- Exporting --

    def export_selected_date(self):
        if self.busy:
            return

        source_path = self.validate_source_path()
        if source_path is None:
            return

        selected_date = self.date_var.get().strip()
        if not selected_date:
            messagebox.showerror(
                "No date selected",
                "Please select a year, month, and date before exporting.",
            )
            self.append_status("Error: export requested without a selected date.")
            return

        if selected_date not in self.date_counts:
            messagebox.showerror(
                "Invalid date selection",
                "The selected date is not in the current scan results. Scan the file again.",
            )
            self.append_status("Error: selected date is not in current scan results.")
            return

        date_column = self.date_column_var.get().strip()
        if not date_column:
            messagebox.showerror(
                "No date column selected", "Please scan the CSV again."
            )
            return

        output_path = unique_output_path(source_path, selected_date)
        temp_path = output_path.with_name(f".{output_path.name}.part")

        try:
            if temp_path.exists():
                temp_path.unlink()

            self.export_reader = self.create_chunk_reader(source_path)
            self.export_state = {
                "source_path": source_path,
                "date_column": date_column,
                "selected_date": selected_date,
                "output_path": output_path,
                "temp_path": temp_path,
                "rows": 0,
                "matched": 0,
                "blank": 0,
                "invalid": 0,
                "chunks": 0,
                "header_written": False,
            }

            self.append_status(
                f"Exporting rows for {selected_date} to: {output_path.name}"
            )
            self.start_operation(f"Exporting rows for {selected_date}...")
            self.root.after(1, self.export_next_chunk)

        except Exception as exc:
            self.handle_operation_error("export", exc)

    def export_next_chunk(self):
        try:
            chunk = next(self.export_reader)
        except StopIteration:
            self.finish_export()
            return
        except Exception as exc:
            self.handle_operation_error("export", exc)
            return

        try:
            state = self.export_state
            date_column = state["date_column"]
            if date_column not in chunk.columns:
                raise ValueError(
                    f"The date column '{date_column}' is missing from the CSV."
                )

            date_strings, blank_count, invalid_count = format_datetime_values_as_dates(
                chunk[date_column]
            )
            match_mask = date_strings.eq(state["selected_date"]).fillna(False)
            matching_rows = chunk.loc[match_mask]

            if not matching_rows.empty:
                matching_rows.to_csv(
                    state["temp_path"],
                    mode="a" if state["header_written"] else "w",
                    header=not state["header_written"],
                    index=False,
                    encoding="utf-8",
                    lineterminator="\n",
                )
                state["header_written"] = True
                state["matched"] += len(matching_rows)

            state["chunks"] += 1
            state["rows"] += len(chunk)
            state["blank"] += blank_count
            state["invalid"] += invalid_count

            self.activity_var.set(
                f"Exporting... {state['rows']:,} rows checked; "
                f"{state['matched']:,} rows matched."
            )
            if state["chunks"] == 1 or state["chunks"] % 10 == 0:
                self.append_status(
                    f"Export progress: {state['rows']:,} rows checked; "
                    f"{state['matched']:,} matched."
                )

            self.root.after(1, self.export_next_chunk)

        except Exception as exc:
            self.handle_operation_error("export", exc)

    def finish_export(self):
        self.close_reader("export")
        state = self.export_state

        if not state or not state["header_written"] or state["matched"] == 0:
            self.remove_partial_export()
            self.export_state = None
            self.finish_operation("Export finished: no matching rows found.")
            self.append_status("Error: no matching rows were found during export.")
            messagebox.showerror(
                "No matching rows",
                "No rows matched the selected date. The CSV may have changed "
                "since it was scanned.",
            )
            return

        try:
            # Recheck immediately before finalizing so an existing file is never
            # deliberately overwritten.
            final_path = state["output_path"]
            if final_path.exists():
                final_path = unique_output_path(
                    state["source_path"], state["selected_date"]
                )

            state["temp_path"].rename(final_path)
            ignored = state["blank"] + state["invalid"]

            message = (
                f"Export complete: {state['matched']:,} rows saved to {final_path}."
            )
            if ignored:
                message += (
                    f" Ignored {state['blank']:,} blank and "
                    f"{state['invalid']:,} unparseable timestamp values."
                )

            self.export_state = None
            self.finish_operation(
                f"Export complete. Saved {state['matched']:,} rows."
            )
            self.append_status(message)
            messagebox.showinfo(
                "Export successful",
                f"Exported {state['matched']:,} rows.\n\nSaved as:\n{final_path}",
            )

        except Exception as exc:
            self.handle_operation_error("export", exc)

    # ---------------------------------------------------------- Error cleanup --

    def close_reader(self, operation):
        reader_attribute = "scan_reader" if operation == "scan" else "export_reader"
        reader = getattr(self, reader_attribute, None)
        if reader is not None:
            try:
                reader.close()
            except Exception:
                pass
        setattr(self, reader_attribute, None)

    def remove_partial_export(self):
        if self.export_state:
            temp_path = self.export_state.get("temp_path")
            if temp_path:
                try:
                    Path(temp_path).unlink(missing_ok=True)
                except (OSError, TypeError):
                    # missing_ok is unavailable only on very old Python versions;
                    # fall back to an existence check.
                    try:
                        temp_path = Path(temp_path)
                        if temp_path.exists():
                            temp_path.unlink()
                    except OSError:
                        pass

    def friendly_error_message(self, operation, exc):
        action = "scan" if operation == "scan" else "export"

        if isinstance(exc, pd.errors.EmptyDataError):
            return "The CSV file is empty or does not contain a header row."
        if isinstance(exc, UnicodeDecodeError):
            return (
                "The CSV could not be read as UTF-8. Save the source file as UTF-8 "
                "and try again."
            )
        if isinstance(exc, pd.errors.ParserError):
            return (
                "The CSV structure could not be read. It may contain broken quotes "
                "or severely malformed rows."
            )
        if isinstance(exc, PermissionError):
            return (
                "Permission was denied. Close the CSV in other programs and check "
                "that you can read and write in its folder."
            )
        if isinstance(exc, FileNotFoundError):
            return "The selected CSV file could not be found."
        if isinstance(exc, ValueError):
            return str(exc)
        if isinstance(exc, OSError):
            return f"A file-system error occurred while trying to {action}: {exc}"
        return f"An unexpected error occurred while trying to {action}: {exc}"

    def handle_operation_error(self, operation, exc):
        self.close_reader(operation)
        if operation == "export":
            self.remove_partial_export()
            self.export_state = None
        else:
            self.scan_state = None
            self.clear_date_results()

        friendly_message = self.friendly_error_message(operation, exc)
        self.finish_operation("Ready after an error.")
        self.append_status(f"Error: {friendly_message}")
        messagebox.showerror(
            "CSV error" if operation == "scan" else "Export error",
            friendly_message,
        )

    def on_close(self):
        self.close_reader("scan")
        self.close_reader("export")
        self.remove_partial_export()
        self.root.destroy()


def main():
    root = tk.Tk()
    SmartCSVDateExtractor(root)
    root.mainloop()


if __name__ == "__main__":
    main()
