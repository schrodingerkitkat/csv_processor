import os
import shutil
import pandas as pd
import logging
import glob
from datetime import datetime
import json

class CSVProcessor:
    """
    Class for processing CSV files, including validation, cleaning, saving processed data, and moving original files.

    """


    def __init__(self, input_dir: str, output_dir: str, processed_dir: str, metadata_dir: str) -> None:
        """
        Initialize an instance of the CSVProcessor class.
        Parameters are directory paths for input, output, processed data, and metadata.
        If output, processed or metadata directories do not exist, they will be created.
        Raises a ValueError if the input directory does not exist.

        Raises
        -------
        ValueError
            If input_dir does not exist.
        """

        # Check and handle input_dir
        if not os.path.exists(input_dir):
            raise ValueError(f"Input directory {input_dir} does not exist. Please provide a valid directory.")
        self.input_dir = input_dir
        
        # Handle output_dir, processed_dir, metadata_dir
        for directory in [output_dir, processed_dir, metadata_dir]:
            try:
                os.makedirs(directory, exist_ok=True)
            except Exception as e:
                logging.error(f"Error creating directory {directory}: {e}")
                raise

        self.output_dir = output_dir
        self.processed_dir = processed_dir
        self.metadata_dir = metadata_dir

    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Perform data processing tasks on a DataFrame.

        Parameters
        ----------
        df : pd.DataFrame
            Data to be processed.

        Returns
        -------
        pd.DataFrame
            Processed data.
        """
        # Combine first 
        df['full_name'] = df['first_name'] + ' ' + df['last_name']
        df.columns = [column.replace('_', ' ').title() for column in df.columns]
        return df

    def save_data(self, df: pd.DataFrame, file_name: str) -> None:
        """
        Save processed DataFrame to a CSV file.

        Parameters
        ----------
        df : pd.DataFrame
            Data to be saved.
        file_name : str
            Name of the original file.
        """
        _, filename = os.path.split(file_name)
        df.to_csv(os.path.join(self.output_dir, filename), index=False)

    def validate_data(self, df: pd.DataFrame, file_name: str) -> (bool, dict):
        """
        Validate DataFrame based on predefined rules.

        Parameters
        ----------
        df : pd.DataFrame
            Data to be validated.
        file_name : str
            Name of the original file.

        Returns
        -------
        tuple
            A boolean representing the validity of the data and a dictionary representing the validation report.
        """
        validation_report = {}
        df.columns = df.columns.str.lower().str.replace(' ', '_').str.strip()  # standardizing column names
        required_columns = set(['first_name', 'last_name', 'age'])

        if not required_columns.issubset(df.columns):
            validation_report["missing_required_columns"] = list(required_columns.difference(df.columns))
            return False, validation_report

        if set(df.columns) > required_columns:
            df = df.loc[:, df.columns.isin(required_columns)]
            validation_report["extra_columns"] = list(set(df.columns).difference(required_columns))

        if df[['first_name', 'last_name']].isnull().values.any():
            validation_report["missing_values"] = list(df[['first_name', 'last_name']].isnull().sum())

        if not df['age'].astype(str).str.isdigit().all():
            validation_report["non_integer_age"] = df.loc[~df['age'].astype(str).str.isdigit(), 'age'].values.tolist()

        if (df['age'] < 0).any() or (df['age'] > 120).any():
            validation_report["age_outliers"] = df.loc[(df['age'] < 0) | (df['age'] > 120), 'age'].values.tolist()

        if df.duplicated().any():
            df.drop_duplicates(inplace=True)
            validation_report["duplicates"] = int(df.duplicated().sum())


        return True, validation_report

    def move_and_rename_file(self, source_file: str, destination_folder: str) -> str:
        """
        Move and rename a file.

        Parameters
        ----------
        source_file : str
            The file to be moved and renamed.
        destination_folder : str
            The destination folder.

        Returns
        -------
        str
            The new file path.
        """
        base_name = os.path.basename(source_file)
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        new_file_name = f"{timestamp}_{base_name}"
        destination_file = os.path.join(destination_folder, new_file_name)

        shutil.move(source_file, destination_file)
        return destination_file

    def save_metadata(self, metadata: dict, file_name: str) -> None:
        """
        Save metadata to a JSON file.

        Parameters
        ----------
        metadata : dict
            Metadata to be saved.
        file_name : str
            Name of the original file.
        """
        _, filename = os.path.split(file_name)
        filename, _ = os.path.splitext(filename)
        metadata_file = os.path.join(self.metadata_dir, f"{filename}_metadata.json")

        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=4)

    def process_all_files(self):
        """
        Process all CSV files in the input directory.

        Returns
        -------
        pd.DataFrame
            Concatenated DataFrame of all processed data.
        """
        csv_files = glob.glob(self.input_dir + "/*.csv")
        dataframes = []

        for csv_file in csv_files:
            try:
                # Read the CSV file into a DataFrame
                df = pd.read_csv(csv_file)
                
                # Validate the data
                is_valid, validation_report = self.validate_data(df, csv_file)
                
                if is_valid:
                    # If data is valid, process and save it
                    df = self.process_data(df)
                    self.save_data(df, csv_file)
                    dataframes.append(df)
                    
                    # Move and rename the processed file
                    new_file_path = self.move_and_rename_file(csv_file, self.processed_dir)
                    logging.info(f"{csv_file}: File processed successfully!")
                else:
                    # If data is not valid, log the error and move the file without processing
                    logging.error(f"{csv_file}: File failed validation.")
                    new_file_path = self.move_and_rename_file(csv_file, self.processed_dir)

                # Save metadata and validation report
                metadata = {
                    "file_name": new_file_path,
                    "processed_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "num_records": df.shape[0],
                    "validation_report": validation_report
                }
                self.save_metadata(metadata, csv_file)
            except Exception as e:
                logging.error(f"{csv_file}: Unexpected error occurred during processing. {str(e)}")

        # Return a DataFrame that concatenates all processed data
        return pd.concat(dataframes, ignore_index=True)


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s', 
                        datefmt='%Y-%m-%d %H:%M:%S')
    
    # Specify input and output folders
    INPUT_FOLDER = './input'
    OUTPUT_FOLDER = './output'
    PROCESSED_FOLDER = './processed'
    METADATA_FOLDER = './metadata'

    # Instantiate the CSVProcessor with the specified directories
    processor = CSVProcessor(INPUT_FOLDER, OUTPUT_FOLDER, PROCESSED_FOLDER, METADATA_FOLDER)

    # Create processed and metadata folders if they don't exist
    if not os.path.exists(PROCESSED_FOLDER):
        os.makedirs(PROCESSED_FOLDER)

    if not os.path.exists(METADATA_FOLDER):
        os.makedirs(METADATA_FOLDER)

    # Process all files in the input folder
    all_data = processor.process_all_files()
