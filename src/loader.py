import pyarrow.parquet as pq
import pandas as pd
import random
from config.constants import PARQUET_DATA_DIR

class Loader:
    def __init__(self, filepath):
        self.filepath = filepath
        self.parquet = pq.ParquetFile(filepath)

    @classmethod
    def preprocessed(cls):
        return cls(PARQUET_DATA_DIR+'/combined.parquet')

    def get_all(self)->pd.DataFrame:
        return self.parquet.read().to_pandas()

    def get_batch(self, batch_size: int, skip: int = 0) -> pd.DataFrame:
        batch_list = []
        row_count = 0

        for batch in self.parquet.iter_batches(batch_size):
            if row_count >= skip:
                batch_list.append(batch.to_pandas())
            row_count += batch.num_rows

            if row_count >= skip + batch_size:
                break  # Stop when the batch size is reached

        return pd.concat(batch_list) if batch_list else pd.DataFrame()

    def get_sample(self, sample_size: int=1000, seed: int = None) -> pd.DataFrame:
        """Returns a random sample of data without loading the full dataset."""
        if seed:
            random.seed(seed)

        # Read the Parquet file in batches
        batch_size = 10_000  # Adjust based on memory constraints
        sampled_rows = []

        for batch in self.parquet.iter_batches(batch_size):
            df_batch = batch.to_pandas()

            # Take a random sample from this batch
            if not df_batch.empty:
                sampled_rows.append(df_batch.sample(n=min(sample_size, len(df_batch)), random_state=seed))

            # Stop early if enough samples are collected
            if sum(len(s) for s in sampled_rows) >= sample_size:
                break

        # Combine sampled batches
        df_sample = pd.concat(sampled_rows).head(sample_size)

        return df_sample

