"""
This script scans a D drive directory and all subdirectories, verifies each file exists in S3,
and uploads any missing files. Designed for comprehensive backup verification with performance
optimization, detailed logging, and progress tracking.

Usage:
    python d_drive_backup_verifier.py [--source-path "D:\\Pictures\\Lightroom"] [--threads 4] [--dry-run]
"""

import os
import logging
import sys
from pathlib import Path
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from datetime import datetime
import ctypes  # For preventing sleep on Windows
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Dict, Set, Optional
import time
from dataclasses import dataclass
import threading
from functools import lru_cache

# Prevent Windows sleep at script startup (Windows only)
if sys.platform == "win32":
    try:
        ES_CONTINUOUS = 0x80000000
        ES_SYSTEM_REQUIRED = 0x00000001
        ES_AWAYMODE_REQUIRED = 0x00000040
        
        ctypes.windll.kernel32.SetThreadExecutionState(
            ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_AWAYMODE_REQUIRED
        )
    except Exception:
        pass  # Ignore if unable to prevent sleep

@dataclass
class BackupStats:
    """Statistics tracking for the backup verification process."""
    total_files_scanned: int = 0
    files_already_in_s3: int = 0
    files_uploaded_to_s3: int = 0
    upload_failures: int = 0
    scan_errors: int = 0
    total_bytes_uploaded: int = 0
    
    def __add__(self, other):
        """Allow adding stats objects together."""
        return BackupStats(
            total_files_scanned=self.total_files_scanned + other.total_files_scanned,
            files_already_in_s3=self.files_already_in_s3 + other.files_already_in_s3,
            files_uploaded_to_s3=self.files_uploaded_to_s3 + other.files_uploaded_to_s3,
            upload_failures=self.upload_failures + other.upload_failures,
            scan_errors=self.scan_errors + other.scan_errors,
            total_bytes_uploaded=self.total_bytes_uploaded + other.total_bytes_uploaded
        )

class S3BackupManager:
    """Optimized S3 operations for backup verification and upload."""
    
    def __init__(self, max_pool_connections: int = 50):
        self.s3_client = None
        self._s3_cache: Dict[str, bool] = {}
        self._cache_lock = threading.Lock()
        self.max_pool_connections = max_pool_connections
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize S3 client with optimized configuration."""
        try:
            self.s3_client = boto3.client('s3')
            
            # Test basic connectivity
            response = self.s3_client.list_buckets()
            logging.info("S3 client initialized successfully")
            logging.debug(f"Found {len(response.get('Buckets', []))} accessible buckets")
            
        except NoCredentialsError:
            logging.error("AWS credentials not found. Please configure your credentials.")
            logging.error("Run 'aws configure' or set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables")
            raise
        except Exception as e:
            logging.error(f"Failed to initialize S3 client: {e}")
            logging.error("This might be due to:")
            logging.error("1. Network connectivity issues")
            logging.error("2. AWS credentials configuration")
            logging.error("3. AWS region configuration")
            raise
    
    def validate_bucket(self, bucket: str) -> bool:
        """Validate that the S3 bucket exists and is accessible."""
        logging.info(f"Validating S3 bucket '{bucket}'...")
        
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket, MaxKeys=1)
            logging.info(f"S3 bucket '{bucket}' validated successfully")
            logging.debug(f"Bucket contains objects: {'Contents' in response}")
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error'].get('Message', '')
            
            logging.error(f"S3 bucket validation failed for '{bucket}'")
            logging.error(f"Error code: {error_code}")
            logging.error(f"Error message: {error_message}")
            
            if error_code == 'NoSuchBucket':
                logging.error(f"Bucket '{bucket}' does not exist")
            elif error_code == 'AccessDenied':
                logging.error(f"Access denied to bucket '{bucket}'")
                logging.error("Check your AWS permissions")
            elif error_code == 'InvalidBucketName':
                logging.error(f"Invalid bucket name: '{bucket}'")
            else:
                logging.error(f"Unexpected error: {e}")
                
            return False
            
        except Exception as e:
            logging.error(f"Unexpected error validating bucket '{bucket}': {e}")
            logging.error(f"Error type: {type(e).__name__}")
            return False
    
    @lru_cache(maxsize=10000)
    def file_exists(self, bucket: str, key: str) -> bool:
        """Check if file exists in S3 with caching."""
        cache_key = f"{bucket}/{key}"
        
        with self._cache_lock:
            if cache_key in self._s3_cache:
                return self._s3_cache[cache_key]
        
        try:
            self.s3_client.head_object(Bucket=bucket, Key=key)
            exists = True
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                exists = False
            else:
                logging.warning(f"Error checking S3 object {key}: {e}")
                exists = False
        
        with self._cache_lock:
            self._s3_cache[cache_key] = exists
        
        return exists
    
    def upload_file(self, bucket: str, key: str, src_file: Path) -> Tuple[bool, int]:
        """Upload file to S3 with retry logic. Returns (success, bytes_uploaded)."""
        max_retries = 3
        file_size = 0
        
        try:
            file_size = src_file.stat().st_size
        except Exception as e:
            logging.error(f"Could not get file size for {src_file}: {e}")
            return False, 0
        
        for attempt in range(max_retries):
            try:
                self.s3_client.upload_file(Filename=str(src_file), Bucket=bucket, Key=key)
                
                # Update cache
                cache_key = f"{bucket}/{key}"
                with self._cache_lock:
                    self._s3_cache[cache_key] = True
                
                logging.debug(f"Successfully uploaded {src_file.name} ({file_size:,} bytes) to s3://{bucket}/{key}")
                return True, file_size
                
            except Exception as e:
                if attempt == max_retries - 1:
                    logging.error(f"Failed to upload {src_file} to S3 ({key}) after {max_retries} attempts: {e}")
                    return False, 0
                time.sleep(2 ** attempt)  # Exponential backoff
        
        return False, 0
    
    def batch_check_exists(self, bucket: str, keys: List[str]) -> Dict[str, bool]:
        """Batch check existence of multiple S3 objects."""
        results = {}
        
        # Check cache first
        uncached_keys = []
        with self._cache_lock:
            for key in keys:
                cache_key = f"{bucket}/{key}"
                if cache_key in self._s3_cache:
                    results[key] = self._s3_cache[cache_key]
                else:
                    uncached_keys.append(key)
        
        # Check uncached keys
        for key in uncached_keys:
            exists = self.file_exists(bucket, key)
            results[key] = exists
        
        return results

class FileScanner:
    """Handles recursive file scanning with error handling."""
    
    def __init__(self, source_root: Path):
        self.source_root = source_root
    
    def scan_directory(self, directory: Path) -> List[Tuple[Path, str]]:
        """Scan directory recursively and return list of (file_path, relative_path) tuples."""
        files = []
        
        try:
            if not directory.exists():
                logging.error(f"Directory does not exist: {directory}")
                return files
            
            for root, dirs, filenames in os.walk(directory):
                root_path = Path(root)
                
                for filename in filenames:
                    try:
                        file_path = root_path / filename
                        if file_path.is_file():
                            relative_path = file_path.relative_to(self.source_root)
                            files.append((file_path, str(relative_path)))
                    except Exception as e:
                        logging.warning(f"Error processing file {filename} in {root}: {e}")
                        continue
                        
        except Exception as e:
            logging.error(f"Error scanning directory {directory}: {e}")
        
        return files
    
    def get_all_files(self) -> List[Tuple[Path, str]]:
        """Get all files from the source directory."""
        logging.info(f"Scanning files in {self.source_root}...")
        files = self.scan_directory(self.source_root)
        logging.info(f"Found {len(files)} files to process")
        return files

class BackupVerifier:
    """Handles backup verification and upload operations."""
    
    def __init__(self, s3_manager: S3BackupManager, dry_run: bool = False):
        self.s3_manager = s3_manager
        self.dry_run = dry_run
    
    def process_files_batch(self, files_batch: List[Tuple[Path, str]], bucket: str, 
                           s3_prefix: str) -> BackupStats:
        """Process a batch of files for backup verification."""
        stats = BackupStats()
        
        # Prepare S3 keys
        s3_keys = []
        file_mapping = {}
        
        for file_path, relative_path in files_batch:
            # Convert Windows path separators to forward slashes for S3
            s3_relative_path = relative_path.replace('\\', '/')
            s3_key = f"{s3_prefix}/{s3_relative_path}" if s3_prefix else s3_relative_path
            s3_keys.append(s3_key)
            file_mapping[s3_key] = (file_path, relative_path)
        
        stats.total_files_scanned = len(files_batch)
        
        # Batch check S3 existence
        s3_exists_map = self.s3_manager.batch_check_exists(bucket, s3_keys)
        
        # Process each file
        for s3_key in s3_keys:
            file_path, relative_path = file_mapping[s3_key]
            
            try:
                if s3_exists_map.get(s3_key, False):
                    stats.files_already_in_s3 += 1
                    logging.debug(f"File already in S3: {relative_path}")
                else:
                    # File missing from S3, upload it
                    if self.dry_run:
                        logging.info(f"[DRY RUN] Would upload: {relative_path}")
                        stats.files_uploaded_to_s3 += 1
                    else:
                        logging.info(f"Uploading to S3: {relative_path}")
                        success, bytes_uploaded = self.s3_manager.upload_file(bucket, s3_key, file_path)
                        
                        if success:
                            stats.files_uploaded_to_s3 += 1
                            stats.total_bytes_uploaded += bytes_uploaded
                        else:
                            stats.upload_failures += 1
                            
            except Exception as e:
                logging.error(f"Error processing file {relative_path}: {e}")
                stats.scan_errors += 1
        
        return stats

class ProgressTracker:
    """Track and display progress of the backup verification process."""
    
    def __init__(self, total_files: int):
        self.total_files = total_files
        self.processed_files = 0
        self.start_time = time.time()
        self.lock = threading.Lock()
        self.last_update = 0
    
    def update(self, increment: int = 1):
        """Update progress counter."""
        with self.lock:
            self.processed_files += increment
            
            # Only print progress every second to avoid spam
            now = time.time()
            if now - self.last_update >= 1.0 or self.processed_files == self.total_files:
                self._print_progress()
                self.last_update = now
    
    def _print_progress(self):
        """Print current progress."""
        if self.total_files == 0:
            return
        
        progress = (self.processed_files / self.total_files) * 100
        elapsed = time.time() - self.start_time
        
        if self.processed_files > 0:
            eta = (elapsed / self.processed_files) * (self.total_files - self.processed_files)
            eta_str = f", ETA: {eta/60:.1f}m" if eta > 60 else f", ETA: {eta:.0f}s"
        else:
            eta_str = ""
        
        print(f"\rProgress: {self.processed_files:,}/{self.total_files:,} "
              f"({progress:.1f}%) - Elapsed: {elapsed/60:.1f}m{eta_str}", end="", flush=True)

def format_bytes(bytes_count: int) -> str:
    """Format bytes as human readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_count < 1024.0:
            return f"{bytes_count:.1f} {unit}"
        bytes_count /= 1024.0
    return f"{bytes_count:.1f} PB"

def verify_d_drive_backup(source_path: str, s3_bucket: str, s3_prefix: str, 
                         log_file: str, max_workers: int = 4, batch_size: int = 100,
                         dry_run: bool = False) -> BackupStats:
    """
    Main function to verify D drive backup to S3.
    
    Args:
        source_path: Source directory path to scan
        s3_bucket: S3 bucket name
        s3_prefix: S3 prefix for files (e.g., "Pictures/Lightroom")
        log_file: Log file path
        max_workers: Maximum number of worker threads
        batch_size: Batch size for processing files
        dry_run: If True, only simulate uploads without actually uploading
    
    Returns:
        BackupStats: Summary statistics
    """
    # Configure logging with separate levels for file and console
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    # File handler - detailed logging
    file_handler = logging.FileHandler(log_file, mode="w")
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler.setFormatter(file_formatter)
    
    # Console handler - less verbose
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.WARNING if not dry_run else logging.INFO)
    console_formatter = logging.Formatter("%(message)s")
    console_handler.setFormatter(console_formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    if dry_run:
        logging.info("*** DRY RUN MODE - No files will be uploaded ***")
    
    logging.info("Starting D drive backup verification...")
    logging.info(f"Source: {source_path}")
    logging.info(f"S3 Bucket: {s3_bucket}")
    logging.info(f"S3 Prefix: {s3_prefix}")
    logging.info(f"Max Workers: {max_workers}")
    logging.info(f"Batch Size: {batch_size}")
    
    source_root = Path(source_path)
    
    # Validate source path
    if not source_root.exists():
        raise ValueError(f"Source directory does not exist: {source_path}")
    
    # Initialize components
    s3_manager = S3BackupManager(max_pool_connections=max_workers * 2)
    
    # Validate S3 bucket exists and is accessible
    if not s3_manager.validate_bucket(s3_bucket):
        raise ValueError(f"S3 bucket validation failed: {s3_bucket}")
    
    scanner = FileScanner(source_root)
    verifier = BackupVerifier(s3_manager, dry_run)
    
    # Scan all files
    all_files = scanner.get_all_files()
    
    if not all_files:
        logging.warning("No files found to process")
        return BackupStats()
    
    # Initialize progress tracking
    progress_tracker = ProgressTracker(len(all_files))
    total_stats = BackupStats()
    
    # Process files in batches with parallel execution
    logging.info(f"Processing {len(all_files):,} files in batches of {batch_size}...")
    
    for i in range(0, len(all_files), batch_size):
        batch = all_files[i:i + batch_size]
        
        # Process batch in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Split batch into smaller chunks for each worker
            chunk_size = max(1, len(batch) // max_workers)
            chunks = [batch[j:j + chunk_size] for j in range(0, len(batch), chunk_size)]
            
            future_to_chunk = {
                executor.submit(verifier.process_files_batch, chunk, s3_bucket, s3_prefix): chunk
                for chunk in chunks if chunk
            }
            
            for future in as_completed(future_to_chunk):
                try:
                    batch_stats = future.result()
                    total_stats = total_stats + batch_stats
                    progress_tracker.update(batch_stats.total_files_scanned)
                except Exception as e:
                    chunk = future_to_chunk[future]
                    logging.error(f"Error processing batch of {len(chunk)} files: {e}")
                    total_stats.scan_errors += len(chunk)
                    progress_tracker.update(len(chunk))
    
    print()  # New line after progress
    
    # Log final statistics
    logging.info("===== D Drive Backup Verification Summary =====")
    logging.info(f"Total files scanned: {total_stats.total_files_scanned:,}")
    logging.info(f"Files already in S3: {total_stats.files_already_in_s3:,}")
    logging.info(f"Files uploaded to S3: {total_stats.files_uploaded_to_s3:,}")
    logging.info(f"Upload failures: {total_stats.upload_failures:,}")
    logging.info(f"Scan errors: {total_stats.scan_errors:,}")
    logging.info(f"Total bytes uploaded: {format_bytes(total_stats.total_bytes_uploaded)}")
    
    if dry_run:
        logging.info("*** This was a DRY RUN - no files were actually uploaded ***")
    
    logging.info("D drive backup verification complete.")
    
    return total_stats

def main():
    """Main entry point with command-line argument parsing."""
    parser = argparse.ArgumentParser(
        description="D Drive Backup Verification Tool - Scans D drive and ensures all files are backed up to S3",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--source-path', type=str, default=r"D:\Pictures\Lightroom",
                       help='Source directory to scan for backup verification')
    parser.add_argument('--s3-bucket', type=str, default="mcmac.store",
                       help='S3 bucket name for backup storage')
    parser.add_argument('--s3-prefix', type=str, default="Pictures/Lightroom",
                       help='S3 prefix/folder for files (without leading/trailing slashes)')
    parser.add_argument('--threads', type=int, default=4,
                       help='Number of worker threads for parallel processing')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Number of files to process in each batch')
    parser.add_argument('--dry-run', action='store_true',
                       help='Perform a dry run without uploading files (shows what would be uploaded)')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging for troubleshooting')
    
    args = parser.parse_args()
    
    # Generate log file with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f"d_drive_backup_log_{timestamp}.txt"
    
    try:
        if args.dry_run:
            print("DRY RUN MODE - No files will be uploaded")
        
        print(f"D Drive Backup Verification Tool")
        print(f"Source: {args.source_path}")
        print(f"S3 Bucket: s3://{args.s3_bucket}/{args.s3_prefix}")
        print(f"Log file: {log_file}")
        print("-" * 50)
        
        # Run the backup verification process
        stats = verify_d_drive_backup(
            args.source_path, args.s3_bucket, args.s3_prefix, log_file,
            max_workers=args.threads, batch_size=args.batch_size, dry_run=args.dry_run
        )
        
        # Print summary to console
        print(f"\n===== SUMMARY =====")
        print(f"Files scanned: {stats.total_files_scanned:,}")
        print(f"Already in S3: {stats.files_already_in_s3:,}")
        print(f"Uploaded to S3: {stats.files_uploaded_to_s3:,}")
        print(f"Upload failures: {stats.upload_failures:,}")
        print(f"Scan errors: {stats.scan_errors:,}")
        print(f"Data uploaded: {format_bytes(stats.total_bytes_uploaded)}")
        
        if args.dry_run:
            print("\n*** DRY RUN COMPLETED - No files were actually uploaded ***")
        
        print(f"\nDetailed log: {log_file}")
        
        # Exit with error code if there were failures
        if stats.upload_failures > 0 or stats.scan_errors > 0:
            print(f"\nWarning: {stats.upload_failures + stats.scan_errors} errors occurred during processing")
            sys.exit(1)
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(130)
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        print(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        # Allow Windows to sleep again after script finishes
        if sys.platform == "win32":
            try:
                ctypes.windll.kernel32.SetThreadExecutionState(0x80000000)  # ES_CONTINUOUS
            except Exception:
                pass

if __name__ == "__main__":
    main()



