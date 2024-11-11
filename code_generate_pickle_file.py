import os
import pydicom
import pandas as pd
import dask.bag as db
import dask.multiprocessing
import time
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="pydicom")

def process_dicom_file(file_path, unique_study_ids_file, unique_study_ids, processed_studies, records_added):
    try:
        var_dicom_file = pydicom.dcmread(file_path, force=True)
        dicom_data = var_dicom_file.to_json_dict()
        processed_data = {var_key: var_value for var_key in dicom_data.keys() if var_key != "7FE00010" for var_value in dicom_data[var_key].get("Value", [])}
        study_id = str(var_dicom_file.get(0x0020000D, "Unknown"))  # Convert study ID to string

        if study_id not in processed_studies and study_id not in unique_study_ids:
            # Check if the study ID is in the set and DataFrame
            processed_studies.add(study_id)
            unique_study_ids.add(study_id)

            # Check if the study ID is in the text file
            if not os.path.exists(unique_study_ids_file):
                open(unique_study_ids_file, 'w').close()  # Create the file if it doesn't exist

            with open(unique_study_ids_file, 'r') as f:
                if f"Study ID: {study_id}" not in f.read():
                    # If not found, add it along with the file path
                    with open(unique_study_ids_file, 'a') as f:
                        record = f"Study ID: {study_id}, File Path: {file_path}\n"
                        f.write(record)
                        records_added.append(record)

                    return processed_data, 1  # Return processed_data and 1 for unique_images_count

        return {}, 0  # Return an empty dictionary for processed_data and 0 for unique_images_count
    except Exception as e:
        print(f"Error processing DICOM file {file_path}: {e}")
        return {}, 0  # Return an empty dictionary for processed_data and 0 for unique_images_count

def process_directory_and_save(directory, batch_size=1000, output_pickle_path="processed_dicom_data_Xrays_external_drive.pkl", num_processes=8):
    var_uths_dicom_files_DATAFRAME = pd.DataFrame()  # Initialize an empty DataFrame
    file_paths = []
    unique_study_ids_file = "unique_study_ids_Xrays_external_drive.txt"  # File to store unique study IDs
    unique_study_ids = set()  # Set to keep track of unique study IDs separately
    processed_studies = set()  # Set to keep track of processed study IDs
    records_added = []  # List to store records added to the text file

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.lower().endswith(('.dcm', '.DCM')):
                file_path = os.path.join(root, file)
                file_paths.append(file_path)

    # Use Dask Bag for parallel processing
    bag = db.from_sequence(file_paths, npartitions=num_processes)
    processed_data_bag = bag.map(process_dicom_file, unique_study_ids_file, unique_study_ids, processed_studies, records_added)
    processed_data_lists = processed_data_bag.compute(scheduler='processes')

    # Filter out the processed data for which unique_images_count is not zero
    processed_data_list = [data for data, count in processed_data_lists if count > 0]

    # Concatenate results into a single DataFrame
    if processed_data_list:
        try:
            df = pd.DataFrame(processed_data_list)
            var_uths_dicom_files_DATAFRAME = pd.concat([var_uths_dicom_files_DATAFRAME, df], ignore_index=True)
        except Exception as e:
            print(f"Error creating DataFrame: {e}")

    # Save the processed data to a pickle file
    pickle_path = os.path.join(directory, output_pickle_path)
    var_uths_dicom_files_DATAFRAME.to_pickle(pickle_path)
    print(f"Processed data saved to {pickle_path}")

    # Display the total number of unique images processed
    print(f"Total number of unique images processed: {len(processed_data_list)}")

    # Display the total number of files processed
    print(f"Total number of files processed: {len(file_paths)}")

    # Display records added to the text file
    print("Records added to the text file:")
    for record in records_added:
        print(record)

    return var_uths_dicom_files_DATAFRAME

if __name__ == '__main__':
    # Specify the directory path
    directory_path = "/media/elijah/New Volume/X-rays"

    # Specify the output pickle file path
    output_pickle_path = "/home/elijah/Documents/EMI-Project/Datasets/PKL-files/processed_dicom_data_Xrays_external_drive.pkl"

    # Record the start time
    start_time = time.time()

    # Call the modified function
    process_directory_and_save(directory_path, output_pickle_path=output_pickle_path, num_processes=8)

    # Calculate and print the elapsed time
    elapsed_time = time.time() - start_time
    print(f"Elapsed Time: {elapsed_time} seconds")
















