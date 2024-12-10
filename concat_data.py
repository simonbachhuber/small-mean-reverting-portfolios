import pandas as pd
import os

def concatenate_closing_prices(data_directory, output_file):
    """
    Concatenate closing prices from all cryptocurrency CSV files starting from the birth date
    of the youngest currency into one DataFrame.

    Parameters:
    - data_directory: Directory containing CSV files of cryptocurrency data.
    - output_file: Path to save the concatenated DataFrame as a CSV file.

    Returns:
    - A Pandas DataFrame with concatenated closing prices.
    """
    # Step 1: Find the latest starting date (youngest cryptocurrency)
    latest_start_date = None
    for file in os.listdir(data_directory):
        if file.endswith(".csv"):
            file_path = os.path.join(data_directory, file)
            try:
                df = pd.read_csv(file_path)
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    start_date = df['timestamp'].min()
                    if latest_start_date is None or start_date > latest_start_date:
                        latest_start_date = start_date
            except Exception as e:
                print(f"Error reading file {file}: {e}")

    print(f"The latest starting date is {latest_start_date}")

    # Step 2: Concatenate closing prices starting from the latest_start_date
    all_closing_prices = []
    for file in os.listdir(data_directory):
        if file.endswith(".csv"):
            file_path = os.path.join(data_directory, file)
            try:
                df = pd.read_csv(file_path)
                if 'timestamp' in df.columns and 'close' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    # Filter data from the latest_start_date
                    df = df[df['timestamp'] >= latest_start_date]
                    # Select timestamp and close columns, renaming close to the crypto name
                    crypto_name = file.replace("_hourly.csv", "")
                    df = df[['timestamp', 'close']].rename(columns={'close': crypto_name})
                    # Append to the list of DataFrames
                    all_closing_prices.append(df.set_index('timestamp'))
            except Exception as e:
                print(f"Error processing file {file}: {e}")

    # Combine all DataFrames on the timestamp index
    concatenated_df = pd.concat(all_closing_prices, axis=1)

    # Save the concatenated DataFrame to a CSV file
    concatenated_df.to_csv(output_file)
    print(f"Concatenated closing prices saved to {output_file}")

    return concatenated_df

# Example usage
if __name__ == "__main__":
    data_directory = "data"  # Directory where your CSV files are stored
    output_file = "data/closing_prices.csv"  # Output file for the concatenated data
    concatenated_df = concatenate_closing_prices(data_directory, output_file)
    print(concatenated_df.head())