"""
Greyhound Racing Telemetry & Feature Ingestion Pipeline
Author: Andrew Daye
Module: Data Ingestion, RegEx Sanitization & Anomaly Filtering
"""

import re
import csv
import json
import logging
from typing import Dict, List, Optional, Any

# Configure structured logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class TelemetryIngestionPipeline:
    def __init__(self, output_format: str = "json"):
        self.output_format = output_format
        # RegEx pattern to parse raw unstructured race/telemetry strings
        # Example pattern matching: Box, Runner Name, Split Time, and Run Time
        self.runner_pattern = re.compile(
            r"Box\s*(?P<box>[1-8]):\s*(?P<name>[A-Z\s]+?)\s*\[(?P<run_time>\d+\.\d+)s\]\s*--\s*Split:\s*(?P<split>\d+\.\d+)",
            re.IGNORECASE
        )

    def parse_raw_telemetry(self, raw_entry: str) -> Optional[Dict[str, Any]]:
        """
        Parses unstructured text records using Regular Expressions.
        Extracts telemetry features while isolating corrupt entries.
        """
        match = self.runner_pattern.search(raw_entry.strip())
        if not match:
            logging.warning(f"Malformed telemetry string rejected: {raw_entry}")
            return None

        data = match.groupdict()
        return {
            "box": int(data["box"]),
            "runner_name": data["name"].strip().upper(),
            "run_time": float(data["run_time"]),
            "split_time": float(data["split"])
        }

    def validate_telemetry_features(self, record: Dict[str, Any]) -> bool:
        """
        Anomaly Detection: Filters corrupted sensor data or timing anomalies.
        Ensures physical limits (e.g., impossible split times or run times) are flagged.
        """
        # Validation boundaries for standard track distances (e.g., 380m - 520m)
        if not (1 <= record["box"] <= 8):
            return False
        if not (3.0 <= record["split_time"] <= 8.5):
            logging.warning(f"Sensor anomaly detected in split: {record['split_time']} for {record['runner_name']}")
            return False
        if not (15.0 <= record["run_time"] <= 38.0):
            logging.warning(f"Timing anomaly detected in run time: {record['run_time']} for {record['runner_name']}")
            return False

        return True

    def process_telemetry_batch(self, raw_records: List[str]) -> List[Dict[str, Any]]:
        """
        Processes batch inputs, filtering anomalies and structuring features.
        """
        cleaned_records = []
        for raw in raw_records:
            parsed = self.parse_raw_telemetry(raw)
            if parsed and self.validate_telemetry_features(parsed):
                cleaned_records.append(parsed)
            else:
                logging.info(f"Record discarded during quality audit: {raw}")
        return cleaned_records

    def export_data(self, dataset: List[Dict[str, Any]], filename: str) -> None:
        """
        Serializes cleaned dataset to disk (JSON or CSV).
        """
        if self.output_format.lower() == "json":
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(dataset, f, indent=4)
            logging.info(f"Successfully serialized {len(dataset)} records to {filename} (JSON).")
        
        elif self.output_format.lower() == "csv":
            if not dataset:
                return
            keys = dataset[0].keys()
            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                writer.writerows(dataset)
            logging.info(f"Successfully exported {len(dataset)} records to {filename} (CSV).")


if __name__ == "__main__":
    # Simulated raw unstructured telemetry feed (incorporating edge cases and anomalies)
    mock_raw_data = [
        "Box 1: FAST HORIZON [29.82s] -- Split: 5.48",
        "Box 2: DESERT CHASE [30.12s] -- Split: 5.62",
        "Box 3: CORRUPT SENSOR [0.00s] -- Split: 0.12",       # Anomaly to catch
        "Box 4: INVALID ENTRY MALFORMED STRING",            # RegEx failure to catch
        "Box 5: CANNINGTON STAR [29.95s] -- Split: 5.51",
        "Box 8: WEST ON CODY [29.74s] -- Split: 5.42"
    ]

    pipeline = TelemetryIngestionPipeline(output_format="json")
    cleaned_data = pipeline.process_telemetry_batch(mock_raw_data)
    pipeline.export_data(cleaned_data, "cleaned_telemetry_sample.json")
