import os
import json
import shutil
from datetime import datetime
import requests
import concurrent.futures
import logging
from tqdm import tqdm

from config.constants import NYC_DATA_API, RAW_DATA_DIR

logger = logging.getLogger(__name__)


def setup_logging(debug_mode=False):
    """Configure logging based on debug mode"""
    log_level = logging.DEBUG if debug_mode else logging.WARNING
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


class Downloader:
    def __init__(self, data_format=NYC_DATA_API["default_format"], batch_size=NYC_DATA_API['batch_size'],
                 output_dir=RAW_DATA_DIR, max_workers=4, debug_mode=False):
        self.data_format = data_format
        self.url = NYC_DATA_API['formats'][data_format]
        self.batch_size = batch_size
        self.output_dir = output_dir
        self.max_workers = max_workers  # Number of parallel downloads
        self.debug_mode = debug_mode

        # Configure logging based on debug mode
        setup_logging(debug_mode)

        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        os.makedirs(self.output_dir, exist_ok=True)
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    @staticmethod
    def get_total_records():
        count_url = f"{NYC_DATA_API['formats']['json']}?$select=count(*)"
        response = requests.get(count_url)
        return int(response.json()[0]["count"])

    def download_batch(self, offset):
        """Download a single batch of data"""
        batch_url = f"{self.url}?$limit={self.batch_size}&$offset={offset}"
        batch_file = os.path.join(
            self.output_dir,
            f"nyc_data_{self.timestamp}_batch_{int(offset / self.batch_size)}.{self.data_format}"
        )

        try:
            # Using stream=True for better memory efficiency with large files
            with requests.get(batch_url, stream=True) as response:
                if response.status_code != 200:
                    logger.error(f"Failed to download batch at offset {offset}: {response.status_code}")
                    return None

                with open(batch_file, 'wb') as f:
                    # Write directly to file in chunks to minimize memory usage
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

            # Only log in debug mode - tqdm will show progress in normal mode
            logger.debug(f"Saved batch to {batch_file}")
            return batch_file
        except Exception as e:
            logger.error(f"Error downloading batch at offset {offset}: {str(e)}")
            return None

    def download_all(self):
        """Download all data in parallel batches"""
        total_records = self.get_total_records()
        # Only log detailed info in debug mode
        logger.debug(f"Total records to download: {total_records}")
        if not self.debug_mode:
            print(f"Found {total_records} records to download")

        offsets = range(0, total_records, self.batch_size)
        batch_files = []

        # Use ThreadPoolExecutor for parallel downloads with tqdm progress bar
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all download tasks
            future_to_offset = {executor.submit(self.download_batch, offset): offset for offset in offsets}

            # Create a progress bar for user feedback
            total_batches = len(future_to_offset)
            with tqdm(total=total_batches, desc="Downloading batches", unit="batch") as progress_bar:
                # Process completed downloads
                for future in concurrent.futures.as_completed(future_to_offset):
                    offset = future_to_offset[future]
                    try:
                        batch_file = future.result()
                        if batch_file:
                            batch_files.append(batch_file)
                    except Exception as e:
                        logger.error(f"Download failed for offset {offset}: {str(e)}")
                    finally:
                        progress_bar.update(1)

        # Create metadata file
        metadata = {
            "source_url": self.url,
            "download_date": self.timestamp,
            "format": self.data_format,
            "total_records": total_records,
            "batch_size": self.batch_size,
            "batch_files": batch_files,
            "successful_batches": len(batch_files)
        }

        metadata_path = os.path.join(
            self.output_dir,
            f"nyc_data_{self.timestamp}_metadata.json"
        )

        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)

        # Final message can be shown even in non-debug mode since the progress bar is complete
        if self.debug_mode:
            logger.debug(f"Download complete: {len(batch_files)} batches. Metadata saved to {metadata_path}")
        else:
            print(f"\nDownload complete: {len(batch_files)} batches. Metadata saved to {metadata_path}")
        return batch_files, metadata_path