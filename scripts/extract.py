import psycopg2
import pandas as pd
import os
import logging
from datetime import datetime, timedelta

# Database connection config (credentials via environment variable)
DB_CONFIG = {
    "host": "host.docker.internal",
    "port": 5432,
    "database": "dvdrental",
    "user": "postgres",
    "password": os.environ.get("DB_PASSWORD")
}

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_extract(execution_date):
    try:
        # incremental date window
        start_date = execution_date
        end_date = (
            datetime.strptime(execution_date, "%Y-%m-%d") + timedelta(days=1)
        ).strftime("%Y-%m-%d")

        # extract with aggregation to avoid duplicates
        query = """
            SELECT
                r.rental_id,
                r.rental_date,
                r.return_date,
                c.customer_id,
                c.first_name || ' ' || c.last_name AS customer_name,
                c.email,
                f.title AS film_title,
                f.rating,
                f.rental_rate,
                f.length AS film_length_mins,
                cat.name AS category,
                SUM(p.amount) AS payment_amount,
                MAX(p.payment_date) AS payment_date,
                s.store_id
            FROM rental r
            JOIN customer c ON r.customer_id = c.customer_id
            JOIN inventory i ON r.inventory_id = i.inventory_id
            JOIN film f ON i.film_id = f.film_id
            JOIN film_category fc ON f.film_id = fc.film_id
            JOIN category cat ON fc.category_id = cat.category_id
            JOIN payment p ON r.rental_id = p.rental_id
            JOIN store s ON i.store_id = s.store_id
            WHERE r.rental_date >= %s
              AND r.rental_date < %s
            GROUP BY
                r.rental_id, r.rental_date, r.return_date,
                c.customer_id, customer_name, c.email,
                f.title, f.rating, f.rental_rate, f.length,
                cat.name, s.store_id;
        """

        # execute query
        with psycopg2.connect(**DB_CONFIG) as conn:
            df = pd.read_sql(query, conn, params=[start_date, end_date])

        logger.info(f"Extracted {len(df)} rows for {execution_date}")

        # save raw extract
        output_path = os.path.join(
            RAW_DIR,
            f"rentals_raw_{execution_date}.csv"
        )

        df.to_csv(output_path, index=False)

        return output_path

    except Exception as e:
        logger.error(f"Extract failed: {str(e)}")
        raise