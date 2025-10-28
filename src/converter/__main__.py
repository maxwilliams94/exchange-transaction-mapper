import argparse
import sys
from pathlib import Path
import yaml
import csv
from typing import Dict, Any, List
import traceback

#!/usr/bin/env python3
"""
CSV transaction converter - reads CSV files from input directory,
transforms them according to YAML configuration, and writes to output directory.
"""



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
    """Get all CSV files from input directory."""
    if not input_dir.exists():
        print(f"Error: Input directory does not exist: {input_dir}", file=sys.stderr)
        sys.exit(1)
    
    csv_files = list(input_dir.glob("*.csv"))
    return csv_files


def convert_csv(input_file: Path, output_file: Path, config: Dict[str, Any]) -> None:
    """
    Convert a single CSV file according to configuration.
    
    Args:
        input_file: Source CSV file path
        output_file: Destination CSV file path
        config: Configuration dictionary from YAML
    """
    print(f"Converting {input_file.name} -> {output_file.name}")
    
    # TODO: Implement actual conversion logic based on config
    # For now, just copy the file structure
    with open(input_file, 'r', encoding='utf-8') as infile:
        reader: csv.DictReader[str] = csv.DictReader(infile)
        field_names = reader.fieldnames = reader.fieldnames or []
        rows = list(reader)
        
        if not rows:
            print(f"Warning: {input_file.name} is empty, skipping")
            return
        
        # Write to output (placeholder - will be replaced with actual mapping)
        with open(output_file, 'w', encoding='utf-8', newline='') as outfile:
            writer: csv.DictWriter[str] = csv.DictWriter(outfile, fieldnames=field_names, dialect="csv")
            writer.writeheader()
            writer.writerows(rows)


def main():
    """Main entry point for the CSV converter."""
    parser = argparse.ArgumentParser(
        description="Convert CSV transaction files using YAML configuration"
    )
    
    parser.add_argument(
        '-i', '--input',
        type=Path,
        required=True,
        help='Input directory containing CSV files'
    )
    
    parser.add_argument(
        '-o', '--output',
        type=Path,
        required=True,
        help='Output directory for converted CSV files'
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
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    if args.verbose:
        print(f"Loaded configuration from {args.config}")
        print(f"Input directory: {args.input}")
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
        output_file = args.output / input_file.name
        try:
            convert_csv(input_file, output_file, config)
        except Exception as e:
            print(f"Error converting {input_file.name}: {e}", file=sys.stderr)
            if args.verbose:
                traceback.print_exc()
    
    print("Conversion complete")


if __name__ == "__main__":
    main()