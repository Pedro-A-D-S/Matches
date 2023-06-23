import os
import pandas as pd
import yaml

class CSVtoParquetConverter:
    def __init__(self, input_directory: str, output_directory: str):
        self.input_directory = input_directory
        self.output_directory = output_directory

    def convert_csv_to_parquet(self, csv_path, parquet_output_path) -> None:
        """
        Convert a CSV file to Parquet format.

        Args:
        - csv_path: The path to the input CSV file.
        - parquet_output_path: The desired output path for the Parquet file.
        """
        data = pd.read_csv(csv_path)
        data.to_parquet(parquet_output_path, index=False)

    def convert_multiple_csv_to_parquet(self) -> None:
        """
        Convert multiple CSV files to Parquet format.
        """
        csv_files = [f for f in os.listdir(self.input_directory) if f.endswith('.csv')]
        for csv_file in csv_files:
            csv_path = os.path.join(self.input_directory, csv_file)
            parquet_output_path = os.path.join(self.output_directory, csv_file.replace('.csv',
                                                                                       '.parquet'))
            self.convert_csv_to_parquet(csv_path, parquet_output_path)

if __name__ == '__main__':
    # Load the configuration from the YAML file
    config = yaml.safe_load(open('configuration\config.yaml'))
    
    
    input_directory = config['path_to_raw_data']
    output_directory = config['path_to_parquet_data']

    converter = CSVtoParquetConverter(input_directory, output_directory)
    converter.convert_multiple_csv_to_parquet()

