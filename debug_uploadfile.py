import sys
import pandas as pd
from functions import (
    load_new_data,
    extract_lower_bound,
    extract_revenue_lower_bound,
    get_or_create_dim_ids,
    get_or_create_jobtitle_ids,
    get_existing_company_ids,
    prepare_unique_companies,
    insert_new_companies,
    get_contact_ids,
    update_matched_companies_if_different,
    update_matched_contacts_if_different
)

# Dummy DB session: prints SQL and skips commits
class DummyDB:
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        # you could clean up here if you need
        pass
    def execute(self, stmt, params=None):
        print("\n-- EXECUTE SQL --")
        print(stmt)
        if params:
            print("Params:", params)
        # Return empty result or dummy as needed
        class DummyResult:
            def fetchall(inner): return []
            def fetchone(inner): return None
            def scalar(inner): return None
        return DummyResult()
    def commit(self):
        print("[Skipping commit]")

# Step-by-step debug pipeline

def debug_pipeline(file_path):
    print(f"Loading data from: {file_path}\n")
    df = load_new_data(file_path)
    df.rename(columns={
    'comp_name':    'companyname',
    # if you ever run into similar issues you can chain more here:
    # 'other_upload_col': 'expected_col_name',
    }, inplace=True)
    print(f"Initial rows: {len(df)}, columns: {list(df.columns)}")
    print(df.head(), "\n")

    # Clean empsize and annrev
    print("Cleaning 'empsize' and 'annrev'...")
    df['empsize_clean'] = df['empsize'].apply(extract_lower_bound)
    df['annrev_clean'] = df['annrev'].apply(extract_revenue_lower_bound)
    print(df[['empsize', 'empsize_clean', 'annrev', 'annrev_clean']].head(), "\n")

    # Prepare for ID resolution
    df['index'] = df.index + 1

    with DummyDB() as db:
        # Resolve countries
        print("Resolving country IDs (no inserts)...")
        country_ids = get_or_create_dim_ids(
            df[['index','comp_country']],
            db,
            'dim_countries',
            'comp_country',
            'country_id',
            create_missing=False
        )
        print("Country ID mapping (first 5):", country_ids[:5], "\n")

        # Resolve manlevel and jobtitle
        print("Resolving management level IDs...")
        man_ids = get_or_create_dim_ids(df[['index','manlevel']], db, 'dim_manlevels', 'manlevel', 'manlevel_id', create_missing=False)
        print("Manlevel ID mapping:", man_ids[:5], "\n")

        print("Resolving jobtitle IDs...")
        df_job = (df[['index','jobtitle']].assign(manlevel_id=man_ids))
        job_ids = get_or_create_jobtitle_ids(df_job, db)
        print("Jobtitle ID mapping:", job_ids[:5], "\n")

        # Company matching
        print("Checking existing companies...")
        matched_map, df_unmatched = get_existing_company_ids(df[['index','companyname','comp_domain']], db)
        print("Matched count:", len(matched_map), "Unmatched count:", len(df_unmatched), "\n")

    # Prepare unmatched companies
    df_unique = prepare_unique_companies(df_unmatched)
    print("Unique unmatched companies (first 5):")
    print(df_unique.head(), "\n")

    with DummyDB() as db:
        print("Inserting new companies (dummy)...")
        company_id_map = insert_new_companies(df_unique, db)
        print("New company IDs:", company_id_map, "\n")

    # Contact matching
    df_new = df.copy()
    df_new['company_id'] = df_new['index'].map({**matched_map, **company_id_map})
    print("Company assignment complete. Sample:")
    print(df_new[['index','company_id']].head(), "\n")

    print("Resolving contact IDs...")
    with DummyDB() as db:
        df_contacts = df_new[['index','emplinkedin','empemail','company_id']].copy()
        contacts = get_contact_ids(df_contacts, db)
        print(contacts.head(), "\n")

        print("Checking and updating matched contacts...")
        df_matched = contacts[contacts['contact_id'].notna()]
        updated_contacts, num_updates = update_matched_contacts_if_different(df_matched)
        print(f"Would update {num_updates} contacts", updated_contacts.head(), "\n")

    print("Debug pipeline completed.")

if __name__ == "__main__":
    # hard-coded path (change this to point at your test file)
    file_path = "C:/Users/DaThabor/Downloads/debug_import.xlsx"
    debug_pipeline(file_path)
