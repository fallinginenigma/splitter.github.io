# Script References

This document provides a detailed list of all scripts in the BOP Splitter project, their purpose, and their locations to facilitate faster navigation for agents and developers.

## Main Application

| File | Location | Purpose |
|------|----------|---------|
| `app.py` | `/` | The main entry point for the Streamlit application. Manages the multi-step user interface, session state, and coordinates the workflow from data upload to download. |

## Core Logic Module (`bop_splitter/`)

Located at `/bop_splitter/`, this module contains the deterministic logic for data processing, salience calculation, and splitting.

| File | Purpose |
|------|---------|
| `__init__.py` | Package initialization. |
| `config_profile.py` | Monthly profile save/load. Serialises all user choices (sheet mapping, column mapping, filters, split levels, basis config, SFU-specific months, exclusions, per-BB exceptions, salience overrides) to a portable JSON file. Users download the profile at end-of-month and re-upload it next month to pre-fill every setting, only reviewing what has changed. |
| `databricks_loader.py` | Utilities for connecting to Azure Databricks. Fetches tables as DataFrames and provides connection testing functionality. |
| `exceptions.py` | Manages split exceptions, including global exclusions, BB-specific inclusions/exclusions, fixed quantity allocations, and manual salience overrides. |
| `exporter.py` | Handles the generation of multi-sheet Excel workbooks (`.xlsx`) for the final split results, salience tables, exception logs, and validation reports. |
| `loader.py` | Specialized utilities for loading Excel files. Includes specific logic for parsing and normalizing SAP BEx/HANA BOP export formats (SAS and Monthly sheets). |
| `salience.py` | Functions for computing historical salience (weights) based on various basis modes (last N months, selected months, etc.) and managing weight overrides. |
| `splitter.py` | The core engine that executes the split operation by combining Building Block forecasts with calculated salience weights and applying exception rules. |

## Other Directories

| Directory | Location | Purpose |
|-----------|----------|---------|
| `architecture/` | `/architecture/` | Contains Architecture Standard Operating Procedures (SOPs). |
| `tools/` | `/tools/` | Reserved for standalone utility scripts and internal tools. |
| `.tmp/` | `/.tmp/` | Temporary storage for session logs and intermediate data files. |
