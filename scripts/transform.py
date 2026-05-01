import pandas as pd
import os
import logging

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def validate_schema(df):
    # check required columns exist
    required_cols = [
        "rental_id", "rental_date", "customer_id",
        "film_title", "payment_amount"
    ]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    return df


def run_data_quality_checks(df):
    # basic dq checks
    if df.empty:
        logger.warning("Empty dataset")
        return None

    if df["rental_id"].isnull().any():
        raise ValueError("Null rental_id found")

    if (df["payment_amount"] < 0).any():
        raise ValueError("Negative payment found")

    return df


def run_pipeline(execution_date):
    try:
        # Load daily extract
        input_path = os.path.join(
            RAW_DIR,
            f"rentals_raw_{execution_date}.csv"
        )

        df = pd.read_csv(input_path)
        logger.info(f"Loaded {len(df)} rows")

        # schema validation
        df = validate_schema(df)

        # Basic cleaning
        df = df.drop_duplicates()

        # Convert to datetime
        df['rental_date'] = pd.to_datetime(df['rental_date'])
        df['return_date'] = pd.to_datetime(df['return_date'])
        df['payment_date'] = pd.to_datetime(df['payment_date'])

        # Fill missing payments (treated as unpaid)
        df['payment_amount'] = df['payment_amount'].fillna(0)

        # dq checks
        df = run_data_quality_checks(df)
        if df is None:
            return None

        # Track rentals not yet returned
        df['is_still_open'] = df['return_date'].isna()

        # Compute rental duration (in days)
        df['rental_duration'] = (
            df['return_date'] - df['rental_date']
        ).dt.days

        # keep null for open rentals
        df.loc[df['is_still_open'], 'rental_duration'] = None

        # convert only completed rentals to int
        df.loc[~df['is_still_open'], 'rental_duration'] = df.loc[
            ~df['is_still_open'], 'rental_duration'
        ].fillna(0).astype(int)

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
        logger.info(f"Saved output: {output_path}")

        return output_path

    except Exception as e:
        logger.error(f"Transform failed: {str(e)}")
        raise