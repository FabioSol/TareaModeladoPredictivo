import logging
from datetime import datetime
from config.constants import NYC_DATA_API, RAW_DATA_DIR,PARQUET_DATA_DIR
from src.downloader import Downloader
from src.preprocessor import Preprocessor


class DataPipeline:
    def __init__(self,
                 batch_size=NYC_DATA_API['batch_size'],
                 raw_dir=RAW_DATA_DIR,
                 parquet_dir=PARQUET_DATA_DIR,
                 max_workers=4,
                 debug_mode=False,
                 files_format=NYC_DATA_API["default_format"]):
        """
        Initialize a data pipeline that handles both downloading and processing.

        Args:
            data_format: Format of data to download (default from NYC_DATA_API)
            batch_size: Size of batches to download (default from NYC_DATA_API)
            raw_dir: Directory for raw downloaded data
            processed_dir: Directory for processed data
            max_workers: Number of parallel downloads
            debug_mode: Whether to enable debug logging
            files_format: Format of the input files for processor
        """
        self.raw_dir = raw_dir
        self.parquet_dir = parquet_dir
        self.debug_mode = debug_mode
        self.files_format = files_format
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.debug_mode = debug_mode



        logging.info(f"DataPipeline initialized with raw_dir={raw_dir}, processed_dir={parquet_dir}")

    def run(self, download=True, process=True):
        """
        Run the complete data pipeline.

        Args:
            download: Whether to run the download step
            process: Whether to run the processing step

        Returns:
            Dictionary containing timing information
        """
        results = {
            'start_time': datetime.now(),
            'download_time': None,
            'process_time': None,
            'total_time': None
        }

        # Download data if requested
        if download:
            logging.info("Starting download step")
            download_start = datetime.now()
            Downloader(
                data_format=self.files_format,
                batch_size=self.batch_size,
                output_dir=self.raw_dir,
                max_workers=self.max_workers,
                debug_mode=self.debug_mode,
            ).download_all()
            download_end = datetime.now()
            results['download_time'] = (download_end - download_start).total_seconds()
            logging.info(f"Download completed in {results['download_time']} seconds")

        # Process data if requested
        if process:
            logging.info("Starting processing step")
            process_start = datetime.now()
            Preprocessor(
                files_formats=self.files_format,
                input_dir=self.raw_dir,
                output_dir=self.parquet_dir,
            ).process()
            process_end = datetime.now()
            results['process_time'] = (process_end - process_start).total_seconds()
            logging.info(f"Processing completed in {results['process_time']} seconds")

        results['end_time'] = datetime.now()
        results['total_time'] = (results['end_time'] - results['start_time']).total_seconds()

        logging.info(f"Pipeline completed in {results['total_time']} seconds")
        return results
