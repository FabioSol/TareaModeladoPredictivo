import os

import pandas as pd
import numpy as np
from config.constants import PARSED_DATA_DIR, CLEANED_DATA_DIR


class CleaningPipeline:
    def __init__(self, input_file_path: str=PARSED_DATA_DIR, output_file_path: str=CLEANED_DATA_DIR):
        self.input_file_path = input_file_path
        self.output_file_path = output_file_path
        os.makedirs(self.output_file_path, exist_ok=True)
        self.df = None

    def load_data(self):
        """Load the Parquet file into a DataFrame using PyArrow for efficiency."""
        self.df = pd.read_parquet(self.input_file_path+'/combined.parquet', engine='pyarrow')

    def clean_data(self):
        """Clean the dataset by dropping columns, fixing types, and processing dates/times."""
        drop_columns = [
            'vehicle_type_code_5', 'contributing_factor_vehicle_5', 'vehicle_type_code_4',
            'contributing_factor_vehicle_4', 'vehicle_type_code_3', 'contributing_factor_vehicle_3',
            'cross_street_name', 'off_street_name', 'zip_code', 'borough', 'on_street_name',
            'vehicle_type_code2', 'contributing_factor_vehicle_2', 'location', 'collision_id'
        ]
        self.df.drop(columns=[col for col in drop_columns if col in self.df.columns], inplace=True)

        type_mappings = {
            'latitude': pd.Float32Dtype(), 'longitude': pd.Float32Dtype(),
            'number_of_pedestrians_killed': pd.Int64Dtype(), 'vehicle_type_code1': pd.StringDtype(),
            'number_of_persons_injured': pd.Int64Dtype(), 'number_of_cyclist_killed': pd.Int64Dtype(),
            'number_of_motorist_injured': pd.Int64Dtype(), 'number_of_persons_killed': pd.Int64Dtype(),
            'contributing_factor_vehicle_1': pd.StringDtype(),
            'number_of_pedestrians_injured': pd.Int64Dtype(),
            'number_of_motorist_killed': pd.Int64Dtype(), 'number_of_cyclist_injured': pd.Int64Dtype()
        }

        self.df.replace({'': np.nan, 'None': np.nan}, inplace=True)

        for col, dtype in type_mappings.items():
            if col in self.df.columns:
                self.df[col] = self.df[col].astype(dtype, errors='ignore')

        if 'crash_date' in self.df.columns:
            self.df['crash_date'] = pd.to_datetime(self.df['crash_date'], errors='coerce').dt.date
            self.df = self.df[(self.df['crash_date'] >= pd.to_datetime('2021-01-01').date()) &
                              (self.df['crash_date'] <= pd.to_datetime('2024-12-31').date())]

        if 'crash_time' in self.df.columns:
            def time_to_minutes(t):
                try:
                    h, m = map(int, t.split(':'))
                    return h * 60 + m
                except (ValueError, AttributeError):
                    return np.nan

            self.df['crash_time'] = self.df['crash_time'].apply(time_to_minutes).astype(pd.Int64Dtype())

        if 'longitude' in self.df.columns and 'latitude' in self.df.columns:
            self.df = self.df[(self.df['longitude'] != 0) & (self.df['latitude'] != 0)]

        valid_vehicle_types = {
            'sedan', 'station wagon/sport utility vehicle', 'taxi', '4 dr sedan',
            'pick-up truck', 'box truck', 'bus', 'bike', 'tractor truck diesel', 'van'
        }
        if 'vehicle_type_code1' in self.df.columns:
            self.df['vehicle_type_code1'] = self.df['vehicle_type_code1'].str.strip().str.lower()
            self.df['vehicle_type_code1'] = self.df['vehicle_type_code1'].apply(
                lambda x: x if x in valid_vehicle_types else 'undefined')

        valid_factors = {
            'Driver Inattention/Distraction', 'Unspecified', 'Following Too Closely', 'Failure to Yield Right-of-Way',
            'Backing Unsafely', 'Passing or Lane Usage Improper', 'Passing Too Closely', 'Unsafe Lane Changing',
            'Other Vehicular', 'Turning Improperly', 'Traffic Control Disregarded', 'Unsafe Speed',
            'Driver Inexperience', 'Reaction to Uninvolved Vehicle', 'Alcohol Involvement', 'Pavement Slippery',
            'View Obstructed/Limited', 'Pedestrian/Bicyclist/Other Pedestrian Error/Confusion',
            'Oversized Vehicle', 'Aggressive Driving/Road Rage'
        }
        if 'contributing_factor_vehicle_1' in self.df.columns:
            self.df['contributing_factor_vehicle_1'] = self.df['contributing_factor_vehicle_1'].str.strip().str.title()
            self.df['contributing_factor_vehicle_1'] = self.df['contributing_factor_vehicle_1'].apply(
                lambda x: x if x in valid_factors else 'Other')

    def save_data(self):
        """Save the cleaned DataFrame to a Parquet file."""
        self.df.to_parquet(self.output_file_path+'/clean.parquet', engine='pyarrow')

    def process(self):
        """Execute the full cleaning pipeline."""
        self.load_data()
        self.clean_data()
        self.save_data()
        return self.df

