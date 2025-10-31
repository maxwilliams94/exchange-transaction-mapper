import argparse
import csv
import sys
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol

import yaml

from .constants import OUTPUT_HEADERS
from .mappers.coinbase import map_coinbase_file
from .mappers.firi import map_firi_file
from .mappers.nbx import map_nbx_file

class RowConverter(Protocol):
    def __call__(self, row: Dict[str, str], context: Dict[str, Any]) -> Optional[Dict[str, Any]]:  # pragma: no cover - structural typing only
        ...


class FileConverter(Protocol):
    def __call__(self, file_path: Path, rows: List[Dict[str, str]], context: Dict[str, Any]) -> List[Dict[str, Any]]:  # pragma: no cover
        ...


CONVERTER_REGISTRY: Dict[str, Dict[str, Any]] = {
    "coinbase": {"file": map_coinbase_file},
    "nbx": {"file": map_nbx_file},
    "firi": {"file": map_firi_file},
}


def pick_converters(file_path: Path) -> tuple[Optional[RowConverter], Optional[FileConverter], str]:
    parent = file_path.parent.name
    entry = CONVERTER_REGISTRY.get(parent)
    if entry:
        file_conv: Optional[FileConverter] = entry.get("file")
        row_conv: Optional[RowConverter] = entry.get("row")
        return row_conv, file_conv, parent
    # Fallback to default row converter
    return None, None, parent



def load_config(config_path: Path) -> Dict[str, Any]:
    """Load and parse YAML configuration file."""
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config or {}
    except FileNotFoundError:
        print(f"Error: Configuration file not found: {config_path}", file=sys.stderr)
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error parsing YAML configuration: {e}", file=sys.stderr)
        sys.exit(1)


def get_csv_files(input_dir: Path) -> List[Path]:
    """Recursively get all CSV files under input directory."""
    if not input_dir.exists():
        print(f"Error: Input directory does not exist: {input_dir}", file=sys.stderr)
        sys.exit(1)
    return [p for p in input_dir.rglob("*.csv") if p.is_file()]


def _sniff_dialect(sample: str):  # returns a dialect instance
    try:
        return csv.Sniffer().sniff(sample)
    except csv.Error:
        # Return an instance of the excel dialect (the class exposes dialect config)
        return csv.excel()


def read_csv_rows(file_path: Path) -> List[Dict[str, str]]:
    """Read all rows from CSV (small/medium files). Used for file-level conversion.
    Row-level streaming happens separately.
    """
    with file_path.open("r", encoding="utf-8", newline="") as fh:
        sample = fh.read(4096)
        fh.seek(0)
        dialect = _sniff_dialect(sample)
        reader = csv.DictReader(fh, dialect=dialect)
        # Normalize headers
        if reader.fieldnames:
            reader.fieldnames = [h.strip() for h in reader.fieldnames]
        return list(reader)


def write_mapped_file(rows: List[Dict[str, Any]], output_path: Path) -> None:
    if not rows:
        print(f"Warning: No mapped rows to write for {output_path.name}")
        return
    with output_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=OUTPUT_HEADERS, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow({key: r.get(key, "") for key in OUTPUT_HEADERS})


def process_file(input_file: Path, output_dir: Path, config: Dict[str, Any], dry_run: bool = False) -> int:
    row_conv, file_conv, source = pick_converters(input_file)
    context: Dict[str, Any] = {"config": config, "source": source}
    mapped_rows: List[Dict[str, Any]] = []
    if file_conv:
        # File-level converter uses full list of rows
        raw_rows = read_csv_rows(input_file)
        if not raw_rows:
            print(f"Warning: {input_file.name} had no parsable rows via DictReader; falling back to custom converter")
        mapped_rows = file_conv(input_file, raw_rows, context)
    elif row_conv:
        # Row-level streaming
        with input_file.open("r", encoding="utf-8", newline="") as fh:
            sample = fh.read(4096)
            fh.seek(0)
            dialect = _sniff_dialect(sample)
            reader = csv.DictReader(fh, dialect=dialect)
            if reader.fieldnames:
                reader.fieldnames = [h.strip() for h in reader.fieldnames]
            for raw_row in reader:
                try:
                    converted = row_conv(raw_row, context)
                    if converted:
                        mapped_rows.append(converted)
                except Exception as exc:
                    print(f"Row conversion error in {input_file.name}: {exc}", file=sys.stderr)
                    if config.get("verbose_errors"):
                        traceback.print_exc()
    else:
        print(f"No converter found for {input_file.name}; skipping", file=sys.stderr)
        return 0

    # Decide output filename: preserve stem + mapped suffix
    out_name = f"{input_file.stem}_mapped.csv"
    output_path = output_dir / out_name
    if dry_run:
        print(f"[dry-run] {input_file.name}: {len(mapped_rows)} mapped rows ({'file' if file_conv else 'row'}-level)")
    else:
        write_mapped_file(mapped_rows, output_path)
        print(f"Processed {input_file.name} -> {out_name} ({len(mapped_rows)} rows)")
    return len(mapped_rows)


def main():
    """Main entry point for the CSV converter."""
    parser = argparse.ArgumentParser(
        description="Convert CSV transaction files using YAML configuration"
    )
    
    parser.add_argument(
        '-i', '--input',
        type=Path,
        required=True,
        help='Input root directory containing nested source folders with CSV files'
    )
    
    parser.add_argument(
        '-o', '--output',
        type=Path,
        required=True,
        help='Output directory for mapped CSV files'
    )
    
    parser.add_argument(
        '-c', '--config',
        type=Path,
        default=Path('config.yaml'),
        help='YAML configuration file (default: config.yaml)'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Parse and convert but do not write output files'
    )
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    if args.verbose:
        print(f"Loaded configuration from {args.config}")
        print(f"Input root: {args.input}")
        print(f"Output directory: {args.output}")
    
    # Create output directory if it doesn't exist
    args.output.mkdir(parents=True, exist_ok=True)
    
    # Get all CSV files from input directory
    csv_files = get_csv_files(args.input)
    
    if not csv_files:
        print(f"No CSV files found in {args.input}", file=sys.stderr)
        sys.exit(1)
    
    print(f"Found {len(csv_files)} CSV file(s) to process")
    
    # Process each CSV file
    for input_file in csv_files:
        try:
            process_file(input_file, args.output, config, dry_run=args.dry_run)
        except Exception as e:
            print(f"Error converting {input_file.name}: {e}", file=sys.stderr)
            if args.verbose:
                traceback.print_exc()
    
    print("Conversion complete")


if __name__ == "__main__":
    main()