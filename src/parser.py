import glob
import os
from functools import reduce

import pyarrow as pa
import pyarrow.parquet as pq
import json
from tqdm import tqdm
from config.constants import RAW_DATA_DIR, PARSED_DATA_DIR, NYC_DATA_API


class Parser:
    def __init__(self, files_formats=NYC_DATA_API["default_format"], input_dir=RAW_DATA_DIR, output_dir=PARSED_DATA_DIR):
        self.files_formats = files_formats
        self.input_dir = input_dir
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

        self.file_names = sorted(glob.glob(os.path.join(input_dir, f'*batch*.{files_formats}')))

    def process(self):
        all_data = []

        for file_path in tqdm(self.file_names, desc=f"Processing {self.files_formats} files"):
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                all_data+=data

            except Exception as e:
                print(f"Error processing {file_path}: {e}")

        all_cols = reduce(lambda a, b: a | b, map(lambda x: set(x.keys()), all_data))
        inferred_types = {col: infer_type(next((x[col] for x in all_data if col in x and x[col] is not None), None)) for col in all_cols}
        schema = pa.schema([pa.field(col, inferred_types[col]) for col in inferred_types])
        normalized_data = [{key: (json.dumps(item[key]) if isinstance(item.get(key), (list, dict)) else item.get(key, None)) for key in all_cols} for item in all_data]

        # Convert to table and write to parsed
        if all_data:
            output_path = os.path.join(self.output_dir, f"combined.parquet")
            table = pa.Table.from_pylist(normalized_data,schema=schema)
            pq.write_table(table, output_path)
            print(f"All data written to {output_path}, total rows: {len(all_data)}")
        else:
            print("No data cleaned successfully")
def infer_type(value):
    """Determine the PyArrow type based on Python data type."""
    if isinstance(value, bool):
        return pa.bool_()
    elif isinstance(value, int):
        return pa.int64()
    elif isinstance(value, float):
        return pa.float64()
    elif isinstance(value, str):
        return pa.string()
    elif isinstance(value, list) or isinstance(value, dict):
        return pa.string()  # Store lists & dicts as JSON strings
    else:
        return pa.string()