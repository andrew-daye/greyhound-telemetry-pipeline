# Greyhound Racing Telemetry Ingestion Pipeline

An automated data extraction, parsing, and anomaly filtering pipeline engineered for high-frequency sports telemetry feeds.

## Overview
High-frequency racing telemetry and event result feeds often arrive as unstructured strings containing irregular formatting, sensor dropouts, and corrupted timing tokens. This repository provides a sanitized version of the ingestion and data-cleansing module used to prepare raw track data for downstream quantitative probability modeling.

## Core Capabilities
- **Unstructured Text Parsing:** Employs Regular Expressions (RegEx) to isolate track variables, box allocations, split times, and overall run metrics from messy feed formats.
- **Sensor Anomaly Detection:** Programmatically audits parsed metrics against boundary conditions to reject physical sensor failures, zero-values, and invalid timing pulses.
- **Structured Serialization:** Ingests raw inputs and outputs sanitized datasets into standardized JSON or tabular CSV structures ready for statistical analysis.
- **Robust Diagnostics:** Implements logging mechanisms to track validation dropouts and data hygiene auditing.

## Architecture & Logic Flow
1. **Raw Ingestion:** Ingests string feeds via REST API payloads or raw result scrapes.
2. **RegEx Extraction:** Named capture groups extract runner identifiers and continuous timing floats.
3. **Anomaly Filter:** Validates features against physical feasibility bounds (split times, box ranges, run times).
4. **Serialization:** Exports structured, validated records to JSON/CSV for downstream consumption.

## Usage
```bash
python pipeline.py
