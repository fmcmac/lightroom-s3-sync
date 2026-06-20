import os
import logging
import pytest
from pathlib import Path
from unittest.mock import patch

import boto3
from moto import mock_aws

from lightrooms3sync import (
    BackupStats,
    FileScanner,
    S3BackupManager,
    BackupVerifier,
    format_bytes,
    main,
    make_s3_key,
    normalize_s3_prefix,
    silence_dependency_debug_logs,
    s3_listing_prefix,
    sync_to_s3,
)

BUCKET = "test-bucket"
PREFIX = "Photos"


# --- BackupStats ---


class TestBackupStats:
    def test_defaults(self):
        stats = BackupStats()
        assert stats.total_files_scanned == 0
        assert stats.files_already_in_s3 == 0
        assert stats.files_uploaded_to_s3 == 0
        assert stats.upload_failures == 0
        assert stats.scan_errors == 0
        assert stats.total_bytes_uploaded == 0
        assert stats.files_deleted == 0
        assert stats.delete_failures == 0

    def test_addition(self):
        a = BackupStats(total_files_scanned=5, files_uploaded_to_s3=2, total_bytes_uploaded=100)
        b = BackupStats(total_files_scanned=3, files_already_in_s3=1, files_deleted=4)
        c = a + b
        assert c.total_files_scanned == 8
        assert c.files_uploaded_to_s3 == 2
        assert c.files_already_in_s3 == 1
        assert c.total_bytes_uploaded == 100
        assert c.files_deleted == 4


# --- FileScanner ---


class TestFileScanner:
    def test_scan_finds_files(self, tmp_path):
        (tmp_path / "a.txt").write_text("hello")
        sub = tmp_path / "sub"
        sub.mkdir()
        (sub / "b.jpg").write_bytes(b"\xff\xd8")

        scanner = FileScanner(tmp_path)
        files = scanner.get_all_files()
        names = sorted(p for _, p in files)
        assert names == ["a.txt", "sub/b.jpg"]

    def test_scan_empty_dir(self, tmp_path):
        scanner = FileScanner(tmp_path)
        assert scanner.get_all_files() == []

    def test_scan_nonexistent_dir(self, tmp_path):
        scanner = FileScanner(tmp_path / "nope")
        assert scanner.get_all_files() == []

    def test_exclude_patterns(self, tmp_path):
        (tmp_path / "photo.jpg").write_bytes(b"\xff\xd8")
        (tmp_path / ".DS_Store").write_bytes(b"\x00")
        (tmp_path / "cache.lrdata").write_text("cache")
        (tmp_path / "keep.png").write_bytes(b"\x89PNG")

        scanner = FileScanner(tmp_path, exclude_patterns=[".DS_Store", "*.lrdata"])
        files = scanner.get_all_files()
        names = sorted(p for _, p in files)
        assert names == ["keep.png", "photo.jpg"]

    def test_exclude_patterns_prune_directories(self, tmp_path):
        (tmp_path / "photo.jpg").write_bytes(b"\xff\xd8")
        cache = tmp_path / "Catalog Previews.lrdata"
        cache.mkdir()
        (cache / "preview.dat").write_bytes(b"preview")

        scanner = FileScanner(tmp_path, exclude_patterns=["*.lrdata"])
        files = scanner.get_all_files()
        names = sorted(p for _, p in files)
        assert names == ["photo.jpg"]


# --- format_bytes ---


class TestFormatBytes:
    def test_bytes(self):
        assert format_bytes(500) == "500.0 B"

    def test_kilobytes(self):
        assert format_bytes(2048) == "2.0 KB"

    def test_megabytes(self):
        assert format_bytes(5 * 1024 * 1024) == "5.0 MB"

    def test_gigabytes(self):
        assert format_bytes(3 * 1024**3) == "3.0 GB"


# --- S3 key helpers ---


class TestS3KeyHelpers:
    def test_normalize_s3_prefix(self):
        assert normalize_s3_prefix("/Photos/Lightroom/") == "Photos/Lightroom"
        assert normalize_s3_prefix("") == ""

    def test_s3_listing_prefix_stays_inside_folder(self):
        assert s3_listing_prefix("Photos") == "Photos/"
        assert s3_listing_prefix("/Photos/") == "Photos/"
        assert s3_listing_prefix("") == ""

    def test_make_s3_key(self):
        assert make_s3_key("/Photos/", "sub\\photo.jpg") == "Photos/sub/photo.jpg"
        assert make_s3_key("", "sub\\photo.jpg") == "sub/photo.jpg"


# --- S3BackupManager (moto) ---


@mock_aws
class TestS3BackupManager:
    def _create_bucket(self):
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        return s3

    def test_validate_bucket(self):
        self._create_bucket()
        mgr = S3BackupManager()
        assert mgr.validate_bucket(BUCKET) is True

    def test_validate_bucket_missing(self):
        self._create_bucket()
        mgr = S3BackupManager()
        assert mgr.validate_bucket("no-such-bucket") is False

    def test_file_exists_not_found(self):
        self._create_bucket()
        mgr = S3BackupManager()
        exists, size = mgr.file_exists(BUCKET, "nope.txt")
        assert exists is False
        assert size == 0

    def test_upload_and_exists(self, tmp_path):
        self._create_bucket()
        mgr = S3BackupManager()

        f = tmp_path / "test.txt"
        f.write_text("hello world")

        success, uploaded = mgr.upload_file(BUCKET, "Photos/test.txt", f)
        assert success is True
        assert uploaded == len("hello world")

        exists, size = mgr.file_exists(BUCKET, "Photos/test.txt")
        assert exists is True
        assert size == len("hello world")

    def test_load_prefix_cache(self, tmp_path):
        s3 = self._create_bucket()
        s3.put_object(Bucket=BUCKET, Key="Photos/a.jpg", Body=b"aaa")
        s3.put_object(Bucket=BUCKET, Key="Photos/b.jpg", Body=b"bb")
        s3.put_object(Bucket=BUCKET, Key="Other/c.jpg", Body=b"c")

        mgr = S3BackupManager()
        count = mgr.load_prefix_cache(BUCKET, "Photos")
        assert count == 2

        # Cached entries should be found without HEAD requests
        exists, size = mgr.file_exists(BUCKET, "Photos/a.jpg")
        assert exists is True
        assert size == 3

    def test_list_objects(self):
        s3 = self._create_bucket()
        s3.put_object(Bucket=BUCKET, Key="Photos/a.jpg", Body=b"a")
        s3.put_object(Bucket=BUCKET, Key="Photos/sub/b.jpg", Body=b"b")
        s3.put_object(Bucket=BUCKET, Key="Photos-old/c.jpg", Body=b"c")

        mgr = S3BackupManager()
        keys = mgr.list_objects(BUCKET, "Photos")
        assert sorted(keys) == ["Photos/a.jpg", "Photos/sub/b.jpg"]

    def test_delete_object(self):
        s3 = self._create_bucket()
        s3.put_object(Bucket=BUCKET, Key="Photos/a.jpg", Body=b"a")

        mgr = S3BackupManager()
        assert mgr.delete_object(BUCKET, "Photos/a.jpg") is True

        exists, _ = mgr.file_exists(BUCKET, "Photos/a.jpg")
        assert exists is False

    def test_batch_check_exists(self):
        s3 = self._create_bucket()
        s3.put_object(Bucket=BUCKET, Key="Photos/yes.jpg", Body=b"data")

        mgr = S3BackupManager()
        results = mgr.batch_check_exists(BUCKET, ["Photos/yes.jpg", "Photos/no.jpg"])
        assert results["Photos/yes.jpg"][0] is True
        assert results["Photos/no.jpg"][0] is False


# --- BackupVerifier (moto) ---


@mock_aws
class TestBackupVerifier:
    def _setup(self, tmp_path):
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        mgr = S3BackupManager()
        return s3, mgr

    def test_uploads_missing_files(self, tmp_path):
        s3, mgr = self._setup(tmp_path)

        f = tmp_path / "photo.jpg"
        f.write_bytes(b"\xff\xd8\xff\xe0" * 100)

        verifier = BackupVerifier(mgr, dry_run=False)
        stats = verifier.process_files_batch(
            [(f, "photo.jpg")], BUCKET, PREFIX
        )
        assert stats.files_uploaded_to_s3 == 1
        assert stats.total_bytes_uploaded == f.stat().st_size

    def test_skips_existing_same_size(self, tmp_path):
        s3, mgr = self._setup(tmp_path)

        f = tmp_path / "photo.jpg"
        f.write_bytes(b"content")
        s3.put_object(Bucket=BUCKET, Key="Photos/photo.jpg", Body=b"content")

        verifier = BackupVerifier(mgr, dry_run=False)
        stats = verifier.process_files_batch(
            [(f, "photo.jpg")], BUCKET, PREFIX
        )
        assert stats.files_already_in_s3 == 1
        assert stats.files_uploaded_to_s3 == 0

    def test_skips_existing_same_size_by_default_when_etag_differs(self, tmp_path):
        s3, mgr = self._setup(tmp_path)

        f = tmp_path / "photo.jpg"
        f.write_bytes(b"new-data")
        s3.put_object(Bucket=BUCKET, Key="Photos/photo.jpg", Body=b"old-data")

        verifier = BackupVerifier(mgr, dry_run=False)
        stats = verifier.process_files_batch(
            [(f, "photo.jpg")], BUCKET, PREFIX
        )
        assert stats.files_uploaded_to_s3 == 0
        assert stats.files_already_in_s3 == 1

    def test_reuploads_existing_same_size_when_checksum_verification_enabled(self, tmp_path):
        s3, mgr = self._setup(tmp_path)

        f = tmp_path / "photo.jpg"
        f.write_bytes(b"new-data")
        s3.put_object(Bucket=BUCKET, Key="Photos/photo.jpg", Body=b"old-data")

        verifier = BackupVerifier(mgr, dry_run=False, verify_checksum=True)
        stats = verifier.process_files_batch(
            [(f, "photo.jpg")], BUCKET, PREFIX
        )
        assert stats.files_uploaded_to_s3 == 1
        assert stats.files_already_in_s3 == 0

    def test_reuploads_when_local_larger(self, tmp_path):
        s3, mgr = self._setup(tmp_path)

        f = tmp_path / "photo.jpg"
        f.write_bytes(b"new content that is longer")
        s3.put_object(Bucket=BUCKET, Key="Photos/photo.jpg", Body=b"old")

        verifier = BackupVerifier(mgr, dry_run=False)
        stats = verifier.process_files_batch(
            [(f, "photo.jpg")], BUCKET, PREFIX
        )
        assert stats.files_uploaded_to_s3 == 1
        assert stats.files_already_in_s3 == 0

    def test_reuploads_when_s3_larger(self, tmp_path):
        s3, mgr = self._setup(tmp_path)

        f = tmp_path / "photo.jpg"
        f.write_bytes(b"short")
        s3.put_object(Bucket=BUCKET, Key="Photos/photo.jpg", Body=b"longer content in s3")

        verifier = BackupVerifier(mgr, dry_run=False)
        stats = verifier.process_files_batch(
            [(f, "photo.jpg")], BUCKET, PREFIX
        )
        assert stats.files_already_in_s3 == 0
        assert stats.files_uploaded_to_s3 == 1

    def test_skips_within_size_tolerance(self, tmp_path):
        s3, mgr = self._setup(tmp_path)

        f = tmp_path / "photo.dng"
        f.write_bytes(b"local content!!")  # 15 bytes
        s3.put_object(Bucket=BUCKET, Key="Photos/photo.dng", Body=b"s3 content")  # 10 bytes

        verifier = BackupVerifier(mgr, dry_run=False, size_tolerance=10)
        stats = verifier.process_files_batch(
            [(f, "photo.dng")], BUCKET, PREFIX
        )
        assert stats.files_already_in_s3 == 1
        assert stats.files_uploaded_to_s3 == 0

    def test_dry_run_no_upload(self, tmp_path):
        _, mgr = self._setup(tmp_path)

        f = tmp_path / "photo.jpg"
        f.write_bytes(b"data")

        verifier = BackupVerifier(mgr, dry_run=True)
        stats = verifier.process_files_batch(
            [(f, "photo.jpg")], BUCKET, PREFIX
        )
        assert stats.files_uploaded_to_s3 == 1

        # File should NOT actually be in S3
        exists, _ = mgr.file_exists(BUCKET, "Photos/photo.jpg")
        assert exists is False


# --- sync_to_s3 ---


@mock_aws
class TestSyncToS3:
    def test_dependency_debug_logs_are_silenced(self):
        silence_dependency_debug_logs()

        for logger_name in ("boto3", "botocore", "s3transfer", "urllib3"):
            assert logging.getLogger(logger_name).level == logging.WARNING

    def test_delete_does_not_cross_sibling_prefix(self, tmp_path):
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        s3.put_object(Bucket=BUCKET, Key="Photos/old.jpg", Body=b"old")
        s3.put_object(Bucket=BUCKET, Key="Photos-old/keep.jpg", Body=b"keep")

        (tmp_path / "current.jpg").write_bytes(b"current")

        stats = sync_to_s3(
            str(tmp_path),
            BUCKET,
            "Photos",
            str(tmp_path.parent / "sync.log"),
            dry_run=False,
            delete=True,
        )

        remaining = sorted(
            obj["Key"]
            for obj in s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
        )
        assert remaining == ["Photos-old/keep.jpg", "Photos/current.jpg"]
        assert stats.files_deleted == 1


# --- CLI ---


class TestCLI:
    def test_missing_source_path_exits(self):
        with pytest.raises(SystemExit) as exc_info:
            with patch("sys.argv", ["lightroom-s3-sync"]):
                main()
        assert exc_info.value.code == 2

    def test_help_exits_zero(self):
        with pytest.raises(SystemExit) as exc_info:
            with patch("sys.argv", ["lightroom-s3-sync", "--help"]):
                main()
        assert exc_info.value.code == 0
