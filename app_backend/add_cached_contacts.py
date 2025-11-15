# app_backend/nightly_job.py

### Run with 'python -m app_backend.add_cached_contacts' in terminal ###

import pandas as pd
from datetime import datetime
from app_backend.database import engine  # Absolute import

def refresh_cached_contacts():
    # Step 1: Load data from the view
    query = "SELECT * FROM Vw_full_contacts_data"
    df = pd.read_sql(query, engine)

    # Step 2: Add a last_updated column
    df['last_updated'] = datetime.now()

    # Step 3: Replace existing cached table (use if_exists='replace' to avoid manually dropping the table)
    with engine.begin() as conn:
        df.to_sql("cached_full_contacts_data", con=conn, index=False, if_exists="replace")

    print(f"✅ cached_full_contacts_data updated at {datetime.now()} with {len(df)} records.")

if __name__ == "__main__":
    refresh_cached_contacts()
