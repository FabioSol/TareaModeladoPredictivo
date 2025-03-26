import glob
import os
import pyarrow as pa
import pyarrow.parquet as pq
import json
from tqdm import tqdm
from config.constants import RAW_DATA_DIR, PARQUET_DATA_DIR, NYC_DATA_API


class Preprocessor:
    def __init__(self, files_formats=NYC_DATA_API["default_format"], input_dir=RAW_DATA_DIR, output_dir=PARQUET_DATA_DIR):
        self.files_formats = files_formats
        self.input_dir = input_dir
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

        self.file_names = sorted(glob.glob(os.path.join(input_dir, f'*batch*.{files_formats}')))

    def process(self):
        # Collect all data with normalized fields
        all_data = []

        for file_path in tqdm(self.file_names, desc=f"Processing {self.files_formats} files"):
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)

                for item in data:
                    normalized_item = {}
                    for key, value in item.items():
                        if key == 'location' and isinstance(value, dict):
                            for loc_key, loc_val in value.items():
                                normalized_item[f"location_{loc_key}"] = str(loc_val) if loc_val is not None else ""
                        else:
                            normalized_item[key] = str(value) if value is not None else ""
                    all_data.append(normalized_item)

            except Exception as e:
                print(f"Error processing {file_path}: {e}")

        # Convert to table and write to parquet
        if all_data:
            output_path = os.path.join(self.output_dir, f"combined.parquet")
            table = pa.Table.from_pylist(all_data)
            pq.write_table(table, output_path)
            print(f"All data written to {output_path}, total rows: {len(all_data)}")
        else:
            print("No data processed successfully")