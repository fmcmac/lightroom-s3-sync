# lightroom-s3-sync

Syncs a local Lightroom photo directory to an S3 bucket. Recursively scans a source directory, checks which files already exist in S3, and uploads any missing files using multithreaded parallel uploads.

## Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/)
- AWS credentials configured via `aws configure`, environment variables, or an AWS `credential_process`

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
  --exclude "*.lrdata" --exclude ".DS_Store" --exclude "Thumbs.db"

# Exclude a live Lightroom catalog file as well as preview/cache directories
uv run lightroom-s3-sync --source-path ~/Pictures/Lightroom \
  --exclude "*.lrdata" --exclude ".DS_Store" --exclude "Thumbs.db" \
  --exclude "Lightroom Catalog"

# Tune parallelism
uv run lightroom-s3-sync --source-path ~/Pictures/Lightroom --threads 8 --batch-size 200

# Mirror local to S3 — upload new files and delete S3 objects no longer present locally
uv run lightroom-s3-sync --source-path ~/Pictures/Lightroom --delete

# Ignore small size differences from metadata-only changes (e.g. DNG software tags)
uv run lightroom-s3-sync --source-path ~/Pictures/Lightroom --size-tolerance 64

# Optionally compare same-size files with simple S3 ETags.
# This is slower and is only reliable when ETags are MD5 hashes.
uv run lightroom-s3-sync --source-path ~/Pictures/Lightroom --verify-checksum

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
| `--size-tolerance` | `0` | Ignore size differences up to this many bytes |
| `--verify-checksum` | off | Compare same-size files against simple S3 ETags |
| `--delete` | off | Delete S3 objects not present locally |
| `--dry-run` | off | Show what would be uploaded/deleted without acting |
| `--debug` | off | Enable debug logging to console |

## How it works

1. Bulk-lists all existing S3 objects under the prefix to prime a local cache (avoids per-file HEAD requests)
2. Recursively scans the source directory for all files, pruning files and directories that match `--exclude` patterns
3. For each file, checks whether the corresponding S3 object exists and whether its size differs beyond `--size-tolerance`
4. Uploads missing files or files with size differences, with exponential-backoff retry (3 attempts)
5. With `--verify-checksum`, also compares same-size files against simple S3 ETags and uploads mismatches
6. With `--delete`, removes S3 objects under the configured prefix that no longer exist locally
7. Files are processed in batches across a thread pool for throughput
8. Produces a timestamped log file with full details and a rich progress bar on the console

S3 keys are formed as `{s3_prefix}/{relative_path}`, with backslashes converted to forward slashes for cross-platform compatibility.
The prefix is normalized so listing and delete checks stay inside that configured folder.

Run with `--dry-run` first, especially before using `--delete`.

On macOS and Windows, the script prevents the system from sleeping during the sync.

## Testing

```bash
uv run pytest
```
