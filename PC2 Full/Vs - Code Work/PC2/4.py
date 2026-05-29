import duckdb
import os
import time
import psutil
import multiprocessing
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Prompt
from rich.table import Table

console = Console()

def get_smart_connection():
    """Detects 32GB RAM and cores to optimize DuckDB."""
    total_ram_gb = psutil.virtual_memory().total / (1024**3)
    cpu_cores = multiprocessing.cpu_count()
    recommended_ram = int(total_ram_gb * 0.75)
    threads = max(1, cpu_cores - 1)
    
    con = duckdb.connect(database=':memory:')
    con.execute(f"SET memory_limit='{recommended_ram}GB';")
    con.execute(f"SET threads={threads};")
    return con, recommended_ram, threads

def process_folder(folder_path, mode, base_file=None):
    folder = Path(folder_path)
    out_folder = folder / "cleaned_output"
    out_folder.mkdir(exist_ok=True)
    log_file_path = out_folder / "dedup_report.txt"

    files = list(folder.glob("*.parquet"))
    if not files:
        console.print("[bold red]No .parquet files found in that directory![/bold red]")
        return

    con, ram, cpu = get_smart_connection()
    
    # Initialize the Summary Table
    table = Table(title="Deduplication Summary", show_header=True, header_style="bold cyan")
    table.add_column("File Name", style="white")
    table.add_column("Original Rows", justify="right", style="dim")
    table.add_column("Cleaned Rows", justify="right", style="green")
    table.add_column("Duplicates Removed", justify="right", style="bold yellow")

    with open(log_file_path, "w") as log_file:
        log_file.write(f"VETO LOGS CLEANING REPORT - {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        log_file.write(f"Resources: {ram}GB RAM allocated, {cpu} threads used.\n")
        log_file.write("-" * 60 + "\n")

        with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), transient=True) as progress:
            for file in files:
                output_path = out_folder / f"clean_{file.name}"
                task = progress.add_task(description=f"Crunching {file.name}...", total=None)
                
                try:
                    # 1. Count before
                    original_count = con.execute(f"SELECT count(*) FROM read_parquet('{file}')").fetchone()[0]
                    
                    # 2. Deduplicate
                    if mode == "1":
                        con.execute(f"COPY (SELECT DISTINCT * FROM read_parquet('{file}')) TO '{output_path}' (FORMAT PARQUET, COMPRESSION 'snappy');")
                    else:
                        con.execute(f"COPY (SELECT * FROM read_parquet('{file}') EXCEPT SELECT * FROM read_parquet('{base_file}')) TO '{output_path}' (FORMAT PARQUET, COMPRESSION 'snappy');")

                    # 3. Count after
                    cleaned_count = con.execute(f"SELECT count(*) FROM read_parquet('{output_path}')").fetchone()[0]
                    removed = original_count - cleaned_count
                    
                    # Add to Table and Log
                    table.add_row(file.name, f"{original_count:,}", f"{cleaned_count:,}", f"{removed:,}")
                    log_file.write(f"{file.name}: Original {original_count}, Cleaned {cleaned_count}, Removed {removed}\n")

                except Exception as e:
                    console.print(f"[bold red]Error on {file.name}:[/bold red] {e}")
                
                progress.remove_task(task)

    # Output the final visual summary
    console.print("\n")
    console.print(table)
    console.print(f"\n[bold green]✅ Batch processing complete![/bold green]")
    console.print(f"[blue]Report saved to:[/blue] {log_file_path}")

def main_menu():
    total_ram = round(psutil.virtual_memory().total / (1024**3), 2)
    cores = multiprocessing.cpu_count()
    
    while True:
        console.print(Panel.fit(
            f"[bold cyan]Veto Logs Dedup System v3.0[/bold cyan]\n"
            f"[gray]Hardware: {total_ram}GB RAM | {cores} Cores detected[/gray]",
            border_style="blue"
        ))
        print("1. Internal Dedup (Self-clean folder)")
        print("2. External Dedup (Subtract from Main/Master file)")
        print("3. Exit")

        choice = Prompt.ask("Choose", choices=["1", "2", "3"], default="3")

        if choice in ["1", "2"]:
            folder_path = Prompt.ask("Enter folder path").strip('"')
            if not os.path.isdir(folder_path):
                console.print("[bold red]Folder not found![/bold red]")
                continue

            base_file = None
            if choice == "2":
                base_file = Prompt.ask("Enter path to MAIN (Base) file").strip('"')
                if not os.path.exists(base_file):
                    console.print("[bold red]Base file not found![/bold red]")
                    continue

            process_folder(folder_path, choice, base_file)
        elif choice == "3":
            break

if __name__ == "__main__":
    main_menu()