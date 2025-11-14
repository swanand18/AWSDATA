import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# --- Replace these with your actual values ---
DB_HOST = "43.205.78.69"
DB_PORT = "5432"
DB_NAME = "postgres"
DB_USER = "finalfunnelpga"
DB_PASSWORD = "FfpG@36$98Pl@y"

# Properly encode special characters
user = quote_plus(DB_USER)
password = quote_plus(DB_PASSWORD)

# PostgreSQL connection string
connection_string = f"postgresql+psycopg2://{user}:{password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# SQLAlchemy engine
engine = create_engine(connection_string)

def refresh_cached_contacts():
    # Step 1: Load data from the view
    query = "SELECT * FROM vw_full_contacts_data"
    df = pd.read_sql(query, engine)

    # Step 2: Add a last_updated column
    df['last_updated'] = datetime.now()

    # Step 3: Replace existing cached table
    with engine.begin() as conn:
        df.to_sql("cached_full_contacts_data", con=conn, index=False, if_exists="replace")

    print(f"✅ cached_full_contacts_data updated at {datetime.now()} with {len(df)} records.")

if __name__ == "__main__":
    refresh_cached_contacts()
