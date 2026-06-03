import duckdb
import os
import psutil
import multiprocessing
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table

console = Console()


def get_smart_connection():
    total_ram_gb = psutil.virtual_memory().total / (1024**3)
    cpu_cores = multiprocessing.cpu_count()
    recommended_ram = int(total_ram_gb * 0.75)
    threads = max(1, cpu_cores - 1)

    con = duckdb.connect(database=":memory:")
    con.execute(f"SET memory_limit='{recommended_ram}GB';")
    con.execute(f"SET threads={threads};")
    return con, recommended_ram, threads


def check_duplicates_one_file(file_path):
    con, ram, cpu = get_smart_connection()

    console.print(f"[cyan]Checking duplicates in:[/cyan] {file_path}")

    duplicate_groups = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT COUNT(*) AS duplicate_count
            FROM read_parquet(?)
            GROUP BY ALL
            HAVING COUNT(*) > 1
        )
    """, [file_path]).fetchone()[0]

    extra_duplicates = con.execute("""
        SELECT COALESCE(SUM(duplicate_count - 1), 0)
        FROM (
            SELECT COUNT(*) AS duplicate_count
            FROM read_parquet(?)
            GROUP BY ALL
            HAVING COUNT(*) > 1
        )
    """, [file_path]).fetchone()[0]

    console.print(f"\n[bold yellow]Duplicate row groups found:[/bold yellow] {duplicate_groups:,}")
    console.print(f"[bold red]Extra duplicate rows:[/bold red] {extra_duplicates:,}")

    sample = con.execute("""
        SELECT *
        FROM (
            SELECT *, COUNT(*) AS duplicate_count
            FROM read_parquet(?)
            GROUP BY ALL
            HAVING COUNT(*) > 1
        )
        LIMIT 20
    """, [file_path]).fetchdf()

    console.print("\n[bold cyan]Sample duplicate rows first 20:[/bold cyan]")
    console.print(sample)


def compare_two_files(file1, file2):
    con, ram, cpu = get_smart_connection()

    count1 = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [file1]).fetchone()[0]
    count2 = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [file2]).fetchone()[0]

    common = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT * FROM read_parquet(?)
            INTERSECT
            SELECT * FROM read_parquet(?)
        )
    """, [file1, file2]).fetchone()[0]

    different = (count1 + count2) - (2 * common)

    table = Table(title="Two File Comparison", show_header=True, header_style="bold cyan")
    table.add_column("File 1")
    table.add_column("File 2")
    table.add_column("Rows File 1", justify="right")
    table.add_column("Rows File 2", justify="right")
    table.add_column("Common Rows", justify="right", style="green")
    table.add_column("Different Rows", justify="right", style="yellow")

    table.add_row(
        os.path.basename(file1),
        os.path.basename(file2),
        f"{count1:,}",
        f"{count2:,}",
        f"{common:,}",
        f"{different:,}"
    )

    console.print(table)

    if count1 == count2 and common == count1:
        console.print("[bold green]✅ Files are IDENTICAL[/bold green]")
    else:
        console.print("[bold red]❌ Files are NOT identical[/bold red]")


def clean_one_file(file_path):
    con, ram, cpu = get_smart_connection()

    file = Path(file_path)
    output_path = file.parent / f"clean_{file.name}"

    original_count = con.execute(
        "SELECT COUNT(*) FROM read_parquet(?)",
        [str(file)]
    ).fetchone()[0]

    con.execute("""
        COPY (
            SELECT DISTINCT *
            FROM read_parquet(?)
        )
        TO ?
        (FORMAT PARQUET, COMPRESSION 'snappy')
    """, [str(file), str(output_path)])

    cleaned_count = con.execute(
        "SELECT COUNT(*) FROM read_parquet(?)",
        [str(output_path)]
    ).fetchone()[0]

    removed = original_count - cleaned_count

    table = Table(title="Single File Cleaning Summary", show_header=True, header_style="bold cyan")
    table.add_column("File")
    table.add_column("Original Rows", justify="right")
    table.add_column("Cleaned Rows", justify="right", style="green")
    table.add_column("Duplicates Removed", justify="right", style="yellow")

    table.add_row(
        file.name,
        f"{original_count:,}",
        f"{cleaned_count:,}",
        f"{removed:,}"
    )

    console.print(table)
    console.print(f"[bold green]✅ Clean file saved:[/bold green] {output_path}")


def clean_multiple_files(folder_path):
    folder = Path(folder_path)
    out_folder = folder / "cleaned_output"
    out_folder.mkdir(exist_ok=True)

    files = sorted(folder.glob("*.parquet"))

    if not files:
        console.print("[bold red]No parquet files found![/bold red]")
        return

    con, ram, cpu = get_smart_connection()

    table = Table(title="Multi File Dedup Summary", show_header=True, header_style="bold cyan")
    table.add_column("File Name")
    table.add_column("Original Rows", justify="right")
    table.add_column("Cleaned Rows", justify="right", style="green")
    table.add_column("Duplicates Removed", justify="right", style="yellow")

    for file in files:
        output_path = out_folder / f"clean_{file.name}"

        console.print(f"[cyan]Cleaning:[/cyan] {file.name}")

        original_count = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(file)]
        ).fetchone()[0]

        con.execute("""
            COPY (
                SELECT DISTINCT *
                FROM read_parquet(?)
            )
            TO ?
            (FORMAT PARQUET, COMPRESSION 'snappy')
        """, [str(file), str(output_path)])

        cleaned_count = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(output_path)]
        ).fetchone()[0]

        removed = original_count - cleaned_count

        table.add_row(
            file.name,
            f"{original_count:,}",
            f"{cleaned_count:,}",
            f"{removed:,}"
        )

    console.print(table)
    console.print(f"[bold green]✅ All cleaned files saved in:[/bold green] {out_folder}")


def main_menu():
    total_ram = round(psutil.virtual_memory().total / (1024**3), 2)
    cores = multiprocessing.cpu_count()

    while True:
        console.print(Panel.fit(
            f"[bold cyan]Veto Logs Parquet Tool[/bold cyan]\n"
            f"[gray]Hardware: {total_ram}GB RAM | {cores} Cores detected[/gray]",
            border_style="blue"
        ))

        print("1. Check duplicate rows in one file")
        print("2. Compare two parquet files")
        print("3. Remove duplicates from one file and save clean file")
        print("4. Multi-file duplicate remover")
        print("5. Exit")

        choice = Prompt.ask("Choose", choices=["1", "2", "3", "4", "5"], default="5")

        if choice == "1":
            file_path = Prompt.ask("Enter parquet file path").strip('"')

            if os.path.exists(file_path):
                check_duplicates_one_file(file_path)
            else:
                console.print("[bold red]File not found![/bold red]")

        elif choice == "2":
            file1 = Prompt.ask("Enter first parquet file path").strip('"')
            file2 = Prompt.ask("Enter second parquet file path").strip('"')

            if os.path.exists(file1) and os.path.exists(file2):
                compare_two_files(file1, file2)
            else:
                console.print("[bold red]One or both files not found![/bold red]")

        elif choice == "3":
            file_path = Prompt.ask("Enter parquet file path").strip('"')

            if os.path.exists(file_path):
                clean_one_file(file_path)
            else:
                console.print("[bold red]File not found![/bold red]")

        elif choice == "4":
            folder_path = Prompt.ask("Enter folder path").strip('"')

            if os.path.isdir(folder_path):
                clean_multiple_files(folder_path)
            else:
                console.print("[bold red]Folder not found![/bold red]")

        elif choice == "5":
            break


if __name__ == "__main__":
    main_menu()