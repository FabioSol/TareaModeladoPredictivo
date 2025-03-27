import logging
from datetime import datetime

from src.cleaner import CleaningPipeline

from config.constants import NYC_DATA_API, RAW_DATA_DIR, PARSED_DATA_DIR, CLEANED_DATA_DIR
from src.downloader import Downloader
from src.parser import Parser


class DataPipeline:
    def __init__(self,
                 batch_size=NYC_DATA_API['batch_size'],
                 raw_dir=RAW_DATA_DIR,
                 parsed_dir=PARSED_DATA_DIR,
                 cleaned_dir=CLEANED_DATA_DIR,
                 max_workers=4,
                 debug_mode=False,
                 files_format=NYC_DATA_API["default_format"]):
        """
        Initialize a data pipeline that handles both downloading and processing.

        Args:
            data_format: Format of data to download (default from NYC_DATA_API)
            batch_size: Size of batches to download (default from NYC_DATA_API)
            raw_dir: Directory for raw downloaded data
            processed_dir: Directory for cleaned data
            max_workers: Number of parallel downloads
            debug_mode: Whether to enable debug logging
            files_format: Format of the input files for processor
        """
        self.raw_dir = raw_dir
        self.parsed_dir = parsed_dir
        self.cleaned_dir = cleaned_dir
        self.debug_mode = debug_mode
        self.files_format = files_format
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.debug_mode = debug_mode



        logging.info(f"DataPipeline initialized with raw_dir={raw_dir}, processed_dir={parsed_dir}, cleaned_dir={cleaned_dir}")

    def run(self, download=True, parse=True, clean=True):

        results = {
            'start_time': datetime.now(),
            'download_time': None,
            'parse_time': None,
            'clean_time': None,
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

        if parse:
            logging.info("Starting parsing step")
            parse_start = datetime.now()
            Parser(
                files_formats=self.files_format,
                input_dir=self.raw_dir,
                output_dir=self.parsed_dir,
            ).process()
            parse_end = datetime.now()
            results['parse_time'] = (parse_end - parse_start).total_seconds()
            logging.info(f"Processing completed in {results['parse_time']} seconds")

        if clean:
            logging.info("Starting cleaning step")
            clean_start = datetime.now()
            CleaningPipeline(input_file_path=self.parsed_dir,
                             output_file_path=self.cleaned_dir).process()
            clean_end = datetime.now()
            results['clean_time'] = (clean_end - clean_start).total_seconds()
            logging.info(f"Cleaning completed in {results['clean_time']} seconds")

        results['end_time'] = datetime.now()
        results['total_time'] = (results['end_time'] - results['start_time']).total_seconds()

        logging.info(f"Pipeline completed in {results['total_time']} seconds")
        return results
