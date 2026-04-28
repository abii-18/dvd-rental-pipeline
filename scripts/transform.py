import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")


def run_pipeline(execution_date):
    # Load daily extract
    input_path = os.path.join(
        RAW_DIR,
        f"rentals_raw_{execution_date}.csv"
    )

    df = pd.read_csv(input_path)

    # Basic cleaning
    df = df.drop_duplicates()

    # Convert to datetime
    df['rental_date'] = pd.to_datetime(df['rental_date'])
    df['return_date'] = pd.to_datetime(df['return_date'])
    df['payment_date'] = pd.to_datetime(df['payment_date'])

    # Fill missing payments (treated as unpaid)
    df['payment_amount'] = df['payment_amount'].fillna(0)

    # Track rentals not yet returned
    df['is_still_open'] = df['return_date'].isna()

    # Compute rental duration (in days)
    df['rental_duration'] = (
        df['return_date'] - df['rental_date']
    ).dt.days

    # Ensure duration is valid for completed rentals
    df.loc[~df['is_still_open'], 'rental_duration'] = df.loc[
        ~df['is_still_open'], 'rental_duration'
    ].fillna(0)

    # Keep integer format for warehouse load
    df['rental_duration'] = df['rental_duration'].fillna(0).astype(int)

    # Flag late returns (only for completed rentals)
    df['is_late_return'] = (
        (df['rental_duration'] > 3) & (~df['is_still_open'])
    )

    # Flag unpaid rentals
    df['is_unpaid'] = df['payment_amount'] == 0

    # Convert booleans to string for Postgres COPY compatibility
    df['is_late_return'] = df['is_late_return'].astype(str)
    df['is_still_open'] = df['is_still_open'].astype(str)
    df['is_unpaid'] = df['is_unpaid'].astype(str)

    # Save processed output
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    output_path = os.path.join(
        PROCESSED_DIR,
        f"rentals_clean_{execution_date}.csv"
    )

    df.to_csv(output_path, index=False)

    return output_path