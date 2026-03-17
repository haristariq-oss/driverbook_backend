#main.py
from __future__ import annotations

import argparse
import os

from pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="ETL Pipeline: PHM or Fuel Features")
    parser.add_argument("--input", required=True, help="Path to input CSV file")
    parser.add_argument("--output", required=False, default="data/output.csv", help="Output CSV path")
    parser.add_argument("--type", required=False, default="phm", choices=["phm", "fuel"], help="Type of CSV: phm or fuel")
    args = parser.parse_args()

    # Auto-prepend data/ if only filename is provided
    output_path = args.output
    if not os.path.dirname(output_path):
        output_path = os.path.join("data", output_path)

    # Auto-detect input path in data/
    input_path = args.input
    if not os.path.exists(input_path) and not os.path.dirname(input_path):
        possible_path = os.path.join("data", input_path)
        if os.path.exists(possible_path):
            input_path = possible_path

    # Run the pipeline
    run_pipeline(input_path, output_path, data_type=args.type)

    print(f"Pipeline completed. Output saved to: {output_path}")


if __name__ == "__main__":
    main()
