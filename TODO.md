# TODO

## Cross-platform

- [x] ~~Remove Windows-specific defaults~~ — `--source-path` is now required
- [x] ~~Rename Windows-centric naming~~ — function, descriptions, and log filenames updated
- [ ] Remove or gate the `ctypes.windll` sleep-prevention code — it's Windows-only and unnecessary on macOS/Linux (currently guarded by `sys.platform` check but clutters the code)
- [ ] Add macOS sleep prevention via `caffeinate` subprocess (equivalent to the Windows `SetThreadExecutionState` call)

## Correctness

- [ ] `S3BackupManager.__init__` accepts `max_pool_connections` but never passes it to the boto3 client config — wire it up via `botocore.config.Config(max_pool_connections=...)`
- [ ] `file_exists()` uses both `@lru_cache` and a manual `_s3_cache` dict — pick one; the dual caching is redundant and the `@lru_cache` on a method with `self` means the instance is part of the cache key
- [ ] `--debug` flag is parsed but never used — connect it to set console log level to DEBUG
- [ ] Log handlers accumulate if `sync_to_s3()` is called more than once in the same process (handlers are added but never removed)

## Features

- [ ] Verify file integrity with size or checksum comparison, not just existence — a zero-byte or partial upload currently looks "synced"
- [ ] Add `--exclude` patterns (e.g., skip `.lrdata`, Lightroom previews, `.DS_Store`)
- [ ] Support S3-compatible endpoints (MinIO, Backblaze B2) via `--endpoint-url`
- [ ] Add a `--delete` mode to remove S3 objects that no longer exist locally
- [ ] Configurable log file location and name via `--log-file`

## Code quality

- [ ] Add tests — at minimum, unit tests for `FileScanner` and `BackupStats`, integration tests with moto for S3 operations
- [ ] Use `s3_client.list_objects_v2` with prefix to batch-check existence instead of individual HEAD requests — much faster for large directories
- [ ] Replace print-based progress with `tqdm` or `rich` for a proper progress bar
