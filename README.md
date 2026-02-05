# lightroom-s3-sync

Syncs a local Lightroom photo directory to an S3 bucket. Recursively scans a source directory, checks which files already exist in S3, and uploads any missing files using multithreaded parallel uploads.

## Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/)
- AWS credentials configured via `aws configure` or environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)

## Setup

```bash
uv sync
```

## Usage

```bash
# Sync a local directory to S3 (--source-path is required)
uv run lightroom-s3-sync --source-path ~/Pictures/Lightroom

# Preview what would be uploaded without uploading anything
uv run lightroom-s3-sync --source-path ~/Pictures/Lightroom --dry-run

# Custom bucket and prefix
uv run lightroom-s3-sync \
  --source-path ~/Pictures/Lightroom \
  --s3-bucket mybucket \
  --s3-prefix "Photos/Lightroom"

# Exclude Lightroom cache files and macOS metadata
uv run lightroom-s3-sync --source-path ~/Pictures/Lightroom \
  --exclude "*.lrdata" --exclude ".DS_Store"

# Tune parallelism
uv run lightroom-s3-sync --source-path ~/Pictures/Lightroom --threads 8 --batch-size 200

# Use an S3-compatible endpoint (MinIO, Backblaze B2, etc.)
uv run lightroom-s3-sync --source-path ~/Pictures/Lightroom \
  --endpoint-url http://localhost:9000 --s3-bucket mybucket
```

### Options

| Flag | Default | Description |
|---|---|---|
| `--source-path` | *(required)* | Local directory to sync |
| `--s3-bucket` | `mcmac.store` | Target S3 bucket |
| `--s3-prefix` | `Pictures/Lightroom` | Key prefix in the bucket |
| `--threads` | `4` | Worker threads for parallel uploads |
| `--batch-size` | `100` | Files per processing batch |
| `--exclude` | *(none)* | Glob pattern to exclude files (repeatable) |
| `--log-file` | auto-timestamped | Custom log file path |
| `--endpoint-url` | *(none)* | Custom S3 endpoint URL |
| `--dry-run` | off | Show what would be uploaded without uploading |
| `--debug` | off | Enable debug logging to console |

## How it works

1. Recursively scans the source directory for all files (respecting `--exclude` patterns)
2. For each file, checks if a corresponding S3 object already exists and has the same size (using a thread-safe cache to avoid repeated HEAD requests)
3. Uploads any missing or size-mismatched files, with exponential-backoff retry (3 attempts)
4. Files are processed in batches across a thread pool for throughput
5. Produces a timestamped log file with full details and a console summary

S3 keys are formed as `{s3_prefix}/{relative_path}`, with backslashes converted to forward slashes for cross-platform compatibility.

On macOS and Windows, the script prevents the system from sleeping during the sync.
