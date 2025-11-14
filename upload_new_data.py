# main.py

import pandas as pd
from dotenv import load_dotenv
from app_backend.database import get_db
from sqlalchemy import text
import sys
from functions import (
    load_new_data,
    safe_exit,
    replace_nan_with_empty_string,
    convert_qa_disposition,
    convert_zipcode_to_string,
    truncate_linkedin_fields_with_log,
    clean_urls,
    enrich_and_merge_dim,
    enrich_and_merge_states,
    get_company_ids,
    compare_companies_to_db,
    upsert_companies,
    get_contact_ids,
    compare_contacts_to_db,
    upsert_contacts
)
from app_backend.database import get_db

# Load environment variables
load_dotenv()

# -----------------------------------------------------------------------------------------------------------------------------------------------------------
# ------------------------------- Load CSV  or Excel Mimicing the File Upload in Streamlit--------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------------------------------------

# Load File
file_path = "C:/Users/DaThabor/Downloads/test_ff.xlsx"
df = load_new_data(file_path)

# Define required columns
required_columns = [
    'name', 'firstname', 'lastname', 'emplinkedin', 'empemail', 'jobtitle',
    'qa_disposition', 'comp_name', 'comp_domain', 'comp_phone', 'comp_linkedin',
    'annrev', 'empsize', 'comp_street', 'comp_city', 'comp_country',
    'comp_state', 'comp_zipcode', 'comp_industry'
]

# Check if the DataFrame is not None and not empty
if df is None:
    safe_exit(f"❌ Import stopped. The file could not be loaded. Please check the file format and content.")
if df is not None and not df.empty:
    print("Start pre-processing the file...")
    # If required columns are missing
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        safe_exit(f"❌ Import stopped. Missing required columns: {', '.join(missing_columns)}")
    
    # Drop rows where email is missing
    skipped_df = df[df['empemail'].isna() | (df['empemail'].astype(str).str.strip() == "")]
    df = df[~(df['empemail'].isna() | (df['empemail'].astype(str).str.strip() == ""))]

    df.reset_index(drop=True, inplace=True)
    df['index'] = df.index + 1

    skipped_count = len(skipped_df)
    if skipped_count > 0:
        print(f"⚠️ {skipped_count} rows were skipped due to missing email addresses.")
    else:
        print(f"✅ No rows were skipped due to missing email addresses.")
    
    columns_to_clean = ['emplinkedin', 'jobtitle', 'comp_name', 'comp_domain', 'comp_phone', 'comp_linkedin']
    df = replace_nan_with_empty_string(df, columns_to_clean)
    print(f"✅ NaN values replaced with empty strings in specified columns.")
    df = convert_qa_disposition(df)
    print(f"✅ QA disposition column converted to disposition values")
    
    df = convert_zipcode_to_string(df)
    print(f"✅ Zipcode column converted to string")
    
    df = truncate_linkedin_fields_with_log(df, ['emplinkedin', 'comp_linkedin'])
    
    columns_to_clean = ['emplinkedin', 'comp_linkedin', 'comp_domain']
    df = clean_urls(df, columns_to_clean)
    print(f"✅ URLs cleaned in specified columns.")
    
    print(f"File pre-processing completed successfully.")
    print(f"Checking values for dimension tables...\n")

    columns_dims = {
        'jobtitle': 'dim_jobtitles',
        'comp_street': 'dim_addresses',
        'comp_country': 'dim_countries',
        'comp_zipcode': 'dim_postalcodes',
        'comp_industry': 'dim_industries',
        'comp_city': 'dim_cities'
    }

    with next(get_db()) as db:
        for col, dim in columns_dims.items():
            enriched_df, total_checked, inserted = enrich_and_merge_dim(df, col, dim, db)
            df = df.drop(columns=[col], errors='ignore')
            df = df.merge(enriched_df, left_index=True, right_index=True, how='left')
            print(f"📌 {dim}: {total_checked} unique values checked, {inserted} inserted")

        # comp_state (after comp_country is ready)
        enriched_states, total_checked, inserted = enrich_and_merge_states(df, 'comp_state', 'comp_country', db)
        df = df.drop(columns=['comp_state'], errors='ignore')
        df = df.merge(enriched_states, left_index=True, right_index=True, how='left')
        print(f"📌 dim_states: {total_checked} unique values checked, {inserted} inserted")
        columns_to_check = [
        'jobtitle_id', 'comp_street_id', 'comp_country_id', 'comp_zipcode_id',
        'comp_industry_id', 'comp_city_id', 'comp_state_id'
        ]
        for col in columns_to_check:
            if col in df.columns:
                null_count = df[col].isna().sum()
                empty_count = (df[col] == '').sum() if df[col].dtype == object else 0
                total_missing = null_count + empty_count
                if total_missing > 0:
                    print(f"⚠️ {col} has {total_missing} missing values ({null_count} null, {empty_count} empty).")
                else:
                    print(f"✅ {col} is fully populated.")
    print(f"Updating dimension tables completed successfully.")
    print("")
    print(f"Start procesing company names...")
    company_columns = {
    'index': 'index',
    'comp_name': 'name',
    'comp_domain': 'comp_domain',
    'comp_linkedin': 'comp_linkedin',
    'comp_phone': 'comp_phone',
    'annrev': 'annrev',
    'empsize': 'empsize',
    'comp_street_id': 'address_id',
    'comp_country_id': 'country_id',
    'comp_zipcode_id': 'postalcode_id',
    'comp_city_id': 'city_id',
    'comp_state_id': 'state_id',
    'comp_industry_id': 'industry_id'
    }

    df_companies = df[list(company_columns.keys())].rename(columns=company_columns)
    match_columns = ['name', 'comp_domain', 'comp_linkedin']
    for col in match_columns:
        df_companies[col] = df_companies[col].replace('', None)
        
    with next(get_db()) as db:
        df_companies = get_company_ids(df_companies, db)

    with next(get_db()) as db:
        df_companies = compare_companies_to_db(df_companies, db)
        
    with next(get_db()) as db:
        df_companies = upsert_companies(df_companies, db)
        
    # Ensure index is present in both DataFrames
    if 'index' not in df.columns:
        df.reset_index(inplace=True)
        df['index'] = df.index + 1  # fallback, in case it was dropped

    # Merge company_id from df_companies into df
    df = df.merge(df_companies[['index', 'company_id']], on='index', how='left')
    print(f"Company names processing completed successfully.")
    print("")
    print(f"Start processing contacts...")
    df_contacts = df[[
    'index', 'name', 'firstname', 'lastname',
    'emplinkedin', 'empemail', 'qa_disposition',
    'jobtitle_id', 'company_id'
    ]].copy()

    # Rename 'qa_disposition' to 'emailstatus_id'
    df_contacts = df_contacts.rename(columns={'qa_disposition': 'emailstatus_id'})

    # Add default dimension IDs
    df_contacts['address_id'] = 999999
    df_contacts['city_id'] = 999999
    df_contacts['state_id'] = 999999
    df_contacts['postalcode_id'] = 999999
    df_contacts['country_id'] = 999999   
    
    with next(get_db()) as db:
        df_contacts = get_contact_ids(df_contacts, db)
    
    with next(get_db()) as db:
        df_contacts = compare_contacts_to_db(df_contacts, db)
    
    with next(get_db()) as db:
        df_contacts = upsert_contacts(df_contacts, db)
    print(f"Contacts processing completed successfully.")
else:
    print(f"❌ Failed to load or empty file: {file_path}")
    