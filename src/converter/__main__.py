import argparse
import csv
import fnmatch
import sys
import traceback
from pathlib import Path
from typing import Any, Dict, List, Tuple, cast

import yaml

from .constants import OUTPUT_HEADERS
from .mapping_engine import MappingConfigurationError, apply_row_mapping
from .mappers.coinbase import load_coinbase_rows
from .mappers.firi import map_firi_transactions
from .mappers.nbx import load_nbx_rows
from .mappers.kraken import map_kraken_ledger


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
    """Default CSV loader that normalises headers and trims values."""
    with file_path.open("r", encoding="utf-8", newline="") as fh:
        sample = fh.read(4096)
        fh.seek(0)
        dialect = _sniff_dialect(sample)
        reader = csv.DictReader(fh, dialect=dialect)
        # Normalize headers
        if reader.fieldnames:
            reader.fieldnames = [h.strip() for h in reader.fieldnames]
        rows: List[Dict[str, str]] = []
        for entry in reader:
            normalised = {
                (key or "").strip(): (value or "").strip()
                for key, value in entry.items()
            }
            rows.append(normalised)
        return rows


def write_mapped_file(rows: List[Dict[str, Any]], output_path: Path) -> None:
    if not rows:
        print(f"Warning: No mapped rows to write for {output_path.name}")
        return
    with output_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=OUTPUT_HEADERS, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow({key: r.get(key, "") for key in OUTPUT_HEADERS})


ROW_LOADER_REGISTRY: Dict[str, Any] = {
    "coinbase_transactions": load_coinbase_rows,
    "nbx_semicolon": load_nbx_rows,
}


FILE_HANDLER_REGISTRY: Dict[str, Any] = {
    "firi_transactions": map_firi_transactions,
    "kraken_ledger": map_kraken_ledger,
}


def _match_file_pattern(file_cfg: Dict[str, Any], file_path: Path) -> bool:
    patterns: List[str] = []
    if "pattern" in file_cfg:
        patterns.append(file_cfg["pattern"])
    patterns.extend(file_cfg.get("patterns", []))
    if not patterns:
        return True
    file_name = file_path.name
    return any(fnmatch.fnmatch(file_name, pattern) for pattern in patterns)


def _resolve_file_config(
    config: Dict[str, Any], source: str, file_path: Path
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    sources_config = config.get("sources", {})
    source_config = sources_config.get(source, {})
    for file_cfg in source_config.get("files", []):
        if _match_file_pattern(file_cfg, file_path):
            return file_cfg, source_config
    return {}, source_config


def _validate_expected_columns(
    rows: List[Dict[str, str]], file_cfg: Dict[str, Any], file_path: Path
) -> None:
    expected = cast(List[str], file_cfg.get("expected_columns") or [])
    if not expected:
        return
    if not rows:
        if file_cfg.get("require_rows"):
            raise ValueError(f"{file_path.name}: no rows found but columns were expected")
        print(
            f"Warning: {file_path.name} contained no data rows; skipping column validation"
        )
        return
    available = {key.strip() for key in rows[0].keys() if key}
    missing = [column for column in expected if column not in available]
    if missing:
        raise ValueError(
            f"{file_path.name}: missing required columns: {', '.join(missing)}"
        )


def _load_rows_for_file(
    file_path: Path, file_cfg: Dict[str, Any]
) -> Tuple[List[Dict[str, str]], Dict[str, Any]]:
    loader_key = file_cfg.get("loader")
    if loader_key:
        loader = ROW_LOADER_REGISTRY.get(loader_key)
        if not loader:
            raise ValueError(f"Unknown loader '{loader_key}' for {file_path.name}")
        result = loader(file_path)
        if isinstance(result, tuple):
            rows = cast(List[Dict[str, str]], result[0])
            extra_context = cast(Dict[str, Any], result[1] or {})
        else:  # pragma: no cover - legacy fallback
            rows = cast(List[Dict[str, str]], result)
            extra_context = {}
        return rows, extra_context
    return read_csv_rows(file_path), {}


def _process_with_row_mapping(
    rows: List[Dict[str, str]],
    file_cfg: Dict[str, Any],
    context: Dict[str, Any],
) -> List[Dict[str, Any]]:
    mapped: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows, start=1):
        row_context = dict(context)
        row_context["row_index"] = idx
        converted = apply_row_mapping(row, file_cfg, row_context)
        if converted:
            mapped.append(converted)
    return mapped


def _process_with_file_handler(
    file_path: Path,
    rows: List[Dict[str, str]],
    file_cfg: Dict[str, Any],
    context: Dict[str, Any],
) -> List[Dict[str, Any]]:
    handler_key = file_cfg.get("handler")
    if not handler_key:
        raise ValueError(
            f"File-level mapping for {file_path.name} requires a handler property"
        )
    handler = FILE_HANDLER_REGISTRY.get(handler_key)
    if not handler:
        raise ValueError(f"Unknown handler '{handler_key}' for {file_path.name}")
    return handler(file_path, rows, context)


def _filter_transaction_types(
    rows: List[Dict[str, Any]], file_cfg: Dict[str, Any]
) -> List[Dict[str, Any]]:
    ignore = cast(List[str], file_cfg.get("ignore_transaction_types") or [])
    if not ignore:
        return rows
    ignore_set = {str(value).upper() for value in ignore}
    filtered: List[Dict[str, Any]] = []
    skipped = 0
    for row in rows:
        tx_type = (row.get("TransactionType") or "").upper()
        if tx_type in ignore_set:
            skipped += 1
            continue
        filtered.append(row)
    if skipped:
        print(
            f"Filtered {skipped} row(s) by TransactionType: {', '.join(sorted(ignore_set))}"
        )
    return filtered


def _apply_id_sequence(
    rows: List[Dict[str, Any]], file_cfg: Dict[str, Any]
) -> List[Dict[str, Any]]:
    prefix = file_cfg.get("id_sequence_prefix")
    if not prefix:
        return rows
    padding = int(file_cfg.get("id_sequence_padding", 6))
    for idx, row in enumerate(rows, start=1):
        if padding > 0:
            row["Id"] = f"{prefix}-{idx:0{padding}d}"
        else:
            row["Id"] = f"{prefix}-{idx}"
    return rows


def process_file(
    input_file: Path, output_dir: Path, config: Dict[str, Any], dry_run: bool = False
) -> int:
    source = input_file.parent.name.lower()
    file_cfg, source_cfg = _resolve_file_config(config, source, input_file)

    if not file_cfg:
        print(
            f"No mapping config found for {input_file.name} in source '{source}'; skipping",
            file=sys.stderr,
        )
        return 0

    mode = (file_cfg.get("mode") or "row").lower()

    if mode == "skip":
        reason = file_cfg.get("reason", "skipped via configuration")
        print(f"Skipping {input_file.name}: {reason}")
        return 0

    context: Dict[str, Any] = {
        "config": config,
        "source": source,
        "source_config": source_cfg,
        "file_config": file_cfg,
    }

    rows, extra_context = _load_rows_for_file(input_file, file_cfg)
    if extra_context:
        context.update(extra_context)

    _validate_expected_columns(rows, file_cfg, input_file)

    if mode == "row":
        mapped_rows = _process_with_row_mapping(rows, file_cfg, context)
    elif mode == "file":
        mapped_rows = _process_with_file_handler(input_file, rows, file_cfg, context)
    else:
        raise ValueError(f"Unsupported mapping mode '{mode}' for {input_file.name}")

    mapped_rows = _filter_transaction_types(mapped_rows, file_cfg)
    mapped_rows = _apply_id_sequence(mapped_rows, file_cfg)

    out_name = f"{input_file.stem}_mapped.csv"
    output_path = output_dir / out_name
    if dry_run:
        print(f"[dry-run] {input_file.name}: {len(mapped_rows)} mapped rows")
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
        except MappingConfigurationError as err:
            print(f"Mapping error in {input_file.name}: {err}", file=sys.stderr)
            if args.verbose or config.get("verbose_errors"):
                traceback.print_exc()
        except Exception as e:
            print(f"Error converting {input_file.name}: {e}", file=sys.stderr)
            if args.verbose:
                traceback.print_exc()
    
    print("Conversion complete")


if __name__ == "__main__":
    main()