import psycopg2
import pandas as pd
import os

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


def run_extract(execution_date):
    # Extract rental, customer, film, payment, and store data via multi-table join
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
            p.amount AS payment_amount,
            p.payment_date,
            s.store_id
        FROM rental r
        JOIN customer c ON r.customer_id = c.customer_id
        JOIN inventory i ON r.inventory_id = i.inventory_id
        JOIN film f ON i.film_id = f.film_id
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category cat ON fc.category_id = cat.category_id
        JOIN payment p ON r.rental_id = p.rental_id
        JOIN store s ON i.store_id = s.store_id;
    """

    # Execute query and load into DataFrame
    with psycopg2.connect(**DB_CONFIG) as conn:
        df = pd.read_sql(query, conn)

    # Save raw extract as CSV (date-partitioned)
    output_path = os.path.join(
        RAW_DIR,
        f"rentals_raw_{execution_date}.csv"
    )

    df.to_csv(output_path, index=False)

    return output_path