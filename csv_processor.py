import os  # python standard library to interact with operating system. Used here to join paths, list files in a directory, and check if a file exists.
import shutil  # python standard library used for file operations. Used here to move files using shutil.move().
import pandas as pd  # 🐼
import logging  # python standard library for logging. Used here to log errors and info.
import chardet  # python library used to detect encoding of a file. Used here to ensure that we correctly read CSV files that may have different encodings.
from datetime import (
    datetime,
)  # python standard library for date and time. Used here to get the current date and time.
import json  # python standard library for handling JSON data. Used here to save metadata to a JSON file.
from typing import (
    Tuple,
)  # python standard library for type hints. Used here to explicitly specify the return type of a function. Explicit is better than implicit.


class CSVProcessor:
    """
    Class for processing CSV files, including validation, cleaning, saving processed data, and moving original files.
    """

    def __init__(
        self, input_dir: str, output_dir: str, processed_dir: str, metadata_dir: str
    ) -> None:
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
            raise ValueError(
                f"Input directory {input_dir} does not exist. Please provide a valid directory."
            )
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
        df["full_name"] = df["first_name"] + " " + df["last_name"]
        df.columns = [column.replace("_", " ").title() for column in df.columns]
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

    def validate_data(self, df: pd.DataFrame, file_name: str) -> Tuple[bool, dict]:
        """
        Validate DataFrame based on predefined rules.

        This function validates the data in the dataframe and generates a report of the validation process.
        The validation includes checking for required columns, the presence of extra columns, missing values,
        invalid age values, and duplicate records. It also standardizes the column names for uniformity.

        Parameters
        ----------
        df : pd.DataFrame
            Data to be validated. The DataFrame should ideally have 'first_name', 'last_name', and 'age' columns.
        file_name : str
            Name of the original file. Used for reference and logging purposes.

        Returns
        -------
        tuple
            A tuple containing a boolean and a dictionary.
            The boolean is True if the data is valid and False if the data is invalid.
            The dictionary is a validation report that contains details of the validation process. It includes:
            - missing_required_columns: List of required columns that were missing in the data.
            - extra_columns: List of extra columns that were present in the data.
            - missing_values: List of columns with missing values.
            - non_integer_age: List of age values that are non-integer.
            - age_outliers: List of age values that are not in the range of 0 to 120.
            - duplicates: Count of duplicate records found in the data.
        """
        validation_report = {}
        df.columns = (
            df.columns.str.lower().str.replace(" ", "_").str.strip()
        )  # standardizing column names
        required_columns = set(["first_name", "last_name", "age"])

        if not required_columns.issubset(df.columns):
            validation_report["missing_required_columns"] = list(
                required_columns.difference(df.columns)
            )
            return False, validation_report

        if set(df.columns) > required_columns:
            df = df.loc[:, df.columns.isin(required_columns)]
            validation_report["extra_columns"] = list(
                set(df.columns).difference(required_columns)
            )

        if df[["first_name", "last_name"]].isnull().values.any():
            validation_report["missing_values"] = list(
                df[["first_name", "last_name"]].isnull().sum()
            )

        if not df["age"].astype(str).str.isdigit().all():
            validation_report["non_integer_age"] = df.loc[
                ~df["age"].astype(str).str.isdigit(), "age"
            ].values.tolist()

        if (df["age"] < 0).any() or (df["age"] > 120).any():
            validation_report["age_outliers"] = df.loc[
                (df["age"] < 0) | (df["age"] > 120), "age"
            ].values.tolist()

        if df.duplicated().any():
            duplicate_count = df.duplicated().sum()
            df.drop_duplicates(inplace=True)
            validation_report["duplicates"] = int(duplicate_count)

        return True, validation_report

    def move_and_rename_file(self, source_file: str, destination_folder: str) -> str:
        """
        Move and rename a file.

        This function moves a file from its original location (source_file) to a new location (destination_folder).
        The file is renamed in the process, with the new name being a timestamp followed by the original filename.
        This ensures that each file has a unique name and that the original order of the files can be determined.
        If the file move operation fails for any reason (such as insufficient permissions or a non-existent source file), an exception is raised.

        Parameters
        ----------
        source_file : str
            The file to be moved and renamed. This should be the absolute path of the file.
        destination_folder : str
            The destination folder. This should be the absolute path of the directory.

        Returns
        -------
        str
            The new file path. This is the absolute path of the file after it has been moved and renamed.

        Raises
        ------
        FileNotFoundError
            If the source_file does not exist.
        PermissionError
            If the program does not have sufficient permissions to move the file.
        """
        # Check if source file exists
        if not os.path.isfile(source_file):
            raise FileNotFoundError(f"The source file does not exist: {source_file}")

        # Check if destination folder exists, if not, create it
        os.makedirs(destination_folder, exist_ok=True)

        base_name = os.path.basename(source_file)

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        new_file_name = f"{timestamp}_{base_name}"

        destination_file = os.path.join(destination_folder, new_file_name)

        # Try moving the file and handle any exceptions

        try:
            shutil.move(source_file, destination_file)
        except PermissionError:
            raise PermissionError(f"No permission to move the file: {source_file}")
        except Exception as e:
            raise Exception(f"Failed to move the file: {source_file}. Error: {str(e)}")

        # Verify if the file move was successful
        if not os.path.isfile(destination_file):
            raise Exception(f"File move was unsuccessful: {source_file}")

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

        with open(metadata_file, "w") as f:
            json.dump(metadata, f, indent=4)

    def process_all_files(self):
        """
        Process all CSV files in the input directory.

        This function goes through all CSV files in the input directory, validates their data, processes the data,
        and saves the processed data. If validation fails, an error is logged and the file is not processed or moved.

        After each file is processed, metadata about the file is saved, including the new file name,
        processing time, number of records, and the validation report.

        The function returns a single DataFrame that concatenates all processed data.

        Returns
        -------
        pd.DataFrame
            Concatenated DataFrame of all processed data.
        """
        file_paths = [
            os.path.join(self.input_dir, file)
            for file in os.listdir(self.input_dir)
            if file.endswith(".csv")
        ]
        dataframes = []
        metadata_collection = []

        for file_path in file_paths:
            error_messages = []
            try:
                # Detect file encoding
                with open(file_path, "rb") as f:
                    encoding_result = chardet.detect(
                        f.read(10000)
                    )  # read only the first 10,000 bytes
                file_encoding = encoding_result["encoding"]

                # Read CSV file into a DataFrame
                df = pd.read_csv(file_path, encoding=file_encoding)

                # Validate the data
                is_valid, validation_report = self.validate_data(df, file_path)

                if is_valid:
                    # If data is valid, process and save it
                    df = self.process_data(df)
                    self.save_data(df, file_path)
                    dataframes.append(df)

                    # Move and rename the processed file
                    new_file_path = self.move_and_rename_file(
                        file_path, self.processed_dir
                    )
                else:
                    error_messages.append("File failed validation.")
                    new_file_path = self.move_and_rename_file(file_path, self.error_dir)

                if validation_report:
                    error_messages.append(str(validation_report))

                if error_messages:
                    logging.error(f"{file_path}: {', '.join(error_messages)}")

                # Save metadata and validation report
                metadata = {
                    "file_name": new_file_path,
                    "processed_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "num_records": df.shape[0],
                    "validation_report": validation_report,
                }
                metadata_collection.append(metadata)

                self.save_metadata(metadata, file_path)
            except Exception as e:
                logging.error(
                    f"{file_path}: Unexpected error occurred during processing. {str(e)}"
                )

        self.save_metadata_collection(metadata_collection)

        # Return a DataFrame that concatenates all processed data
        return pd.concat(dataframes, ignore_index=True)


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Specify input and output folders
    INPUT_FOLDER = "./input"
    OUTPUT_FOLDER = "./output"
    PROCESSED_FOLDER = "./processed"
    METADATA_FOLDER = "./metadata"

    # Instantiate the CSVProcessor with the specified directories
    processor = CSVProcessor(
        INPUT_FOLDER, OUTPUT_FOLDER, PROCESSED_FOLDER, METADATA_FOLDER
    )

    # Process all files in the input folder
    all_data = processor.process_all_files()
