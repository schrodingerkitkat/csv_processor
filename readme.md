# CSV Processor

The CSV Processor is an efficient tool designed to process, validate, and clean CSV files. It is written in Python and utilizes Pandas  to perform various data processing tasks. The objective is to make data processing more accessible to both non-technical users and seasoned data engineers.

## Features

- Validating input data based on predefined rules.
- Handling encoding issues by automatically detecting file encodings.
- Processing data to generate a new column (`full_name`) by combining two columns (`first_name` and `last_name`).
- Saving the processed data to CSV files in a specified output directory.
- Moving and renaming original files to a processed directory for record-keeping.
- Generating and saving metadata and validation reports as JSON files.

## Usage

1. Place your input data as CSV files in an `input` folder. An example folder structure would look like this:

```
.
├── input
│   ├── example1.csv
│   └── example2.csv
├── output
├── processed
└── metadata
```

2. Run the `CSVProcessor.py` script. The script will process all CSV files in the `input` folder and save the results in the respective `output`, `processed`, and `metadata` folders.

3. The processed data will be saved as CSV files in the `output` folder, with the same names as the input files. The `processed` folder will contain the original input files after processing, with a timestamp added to the file names to record the processing time. The `metadata` folder will contain JSON files with the same names as the input files, but appended with `_metadata`; these files store metadata and validation reports for each input file.

4. In case there are any issues during validation, they will be logged and you can review them to investigate the cause of the issues.

## Technical Overview

The core component in this CSV Processor is the `CSVProcessor` class. This class contains methods that handle all aspects of the file processing pipeline:

- `__init__(self, input_dir, output_dir, processed_dir, metadata_dir)`: Initializes the class with the specified input, output, processed data, and metadata directories.
- `process_data(self, df)`: Processes data in a given Pandas DataFrame.
- `save_data(self, df, file_name)`: Saves processed DataFrame to a CSV file.
- `validate_data(self, df, file_name)`: Validates a DataFrame based on predefined rules and returns the validation result (boolean) and a validation report (dictionary).
- `move_and_rename_file(self, source_file, destination_folder)`: Moves and renames a file.
- `save_metadata(self, metadata, file_name)`: Saves metadata to a JSON file.
- `process_all_files(self)`: Processes all CSV files found in the input directory.

The primary method to process and validate data is `process_all_files()`. It contains a loop that iterates over all CSV files in the input directory, validates them, and logs any issues found during validation. After validation, it processes the data in the DataFrame, saves the processed data, moves and renames the input files, and saves the metadata and validation reports.

When running the script, it configures the logging settings, defines the hardcoded directory paths for input, output, processed, and metadata folders, and instantiates the `CSVProcessor` class with the specified directories. The `processor.process_all_files()` method is then called to process all files in the input folder.



## Conclusion

The CSV Processor is a practical tool for processing CSV files, performing validation, and saving the results in an organized manner. It efficiently uses the Pandas library to provide a streamlined processing pipeline that is accessible to both non-technical users and junior data engineers. This tool ensures that the input data is validated and cleaned – which is an essential aspect of any data engineering pipeline.

### Dependencies

- `Python 3.x`
- `pandas`
- `chardet`

To install dependencies, run the following command:

```bash
pip install pandas chardet
```

Please ensure the input, output, processed, and metadata folders are correctly set up before running the script. Happy processing!







