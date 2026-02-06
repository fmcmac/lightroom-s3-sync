# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Single-file Python tool (`lightrooms3sync.py`) that syncs a local Lightroom photo directory to an S3 bucket. It recursively scans a source directory, checks which files already exist in S3, and uploads any missing files. Runs cross-platform (macOS, Linux, Windows).

## Setup

```bash
# Install dependencies and create venv (requires uv)
uv sync
```

## Running

```bash
# Basic run (--source-path is required)
uv run lightroom-s3-sync --source-path ~/Pictures/Lightroom

# Dry run to see what would be uploaded
uv run lightroom-s3-sync --source-path ~/Pictures/Lightroom --dry-run

# Custom bucket, prefix, and threading
uv run lightroom-s3-sync --source-path ~/Pictures/Lightroom --s3-bucket mybucket --s3-prefix "Photos" --threads 8 --batch-size 200
```

Requires AWS credentials configured via `aws configure` or environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`).

## Architecture

All code lives in `lightrooms3sync.py` with four main classes:

- **S3BackupManager** — S3 client wrapper with connection pooling, bulk `list_objects_v2` cache priming, thread-safe in-memory cache, retry-based uploads, and object deletion
- **FileScanner** — Recursive `os.walk`-based file discovery with glob-based exclude patterns
- **BackupVerifier** — Orchestrates batch file processing: checks S3 existence + size, uploads missing/mismatched files (or simulates in dry-run mode)

The `sync_to_s3()` function wires these together: prime cache, scan files, split into batches, process in parallel via `ThreadPoolExecutor`, optionally delete orphaned S3 objects, aggregate `BackupStats` results. Progress is displayed via `rich`.

## Testing

```bash
uv run pytest
```

Tests use `moto` to mock S3 — no AWS credentials needed.

## Key Details

- S3 keys are formed as `{s3_prefix}/{relative_path}` with backslashes converted to forward slashes
- Uploads use exponential backoff retry (3 attempts)
- File sync checks both existence and size (re-uploads on mismatch)
- Managed with `uv`; dependencies defined in `pyproject.toml`, locked in `uv.lock`
