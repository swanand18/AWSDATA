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
    upsert_contacts,
    replace_blank_with_unknown,
    replace_blank_with_zero,
)
from app_backend.database import get_db

# Load environment variables
load_dotenv()

# -----------------------------------------------------------------------------------------------------------------------------------------------------------
# ------------------------------- Load CSV  or Excel Mimicing the File Upload in Streamlit--------------------------------------------------
# -----------------------------------------------------------------------------------------------------------------------------------------------------------

# Load File
file_path = "C:/Users/DaThabor/Downloads/input_tejas.xlsx"
df = load_new_data(file_path)

# Define required columns
required_columns = [
    'firstname', 'lastname', 'emplinkedin', 'empemail', 'jobtitle',
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

        df['name'] = df['firstname'].fillna('') + ' ' + df['lastname'].fillna('')
        
        for col in ['comp_street', 'comp_city', 'comp_state', 'comp_country', 'comp_zipcode', 'comp_industry','jobtitle' ]:
            df[col] = replace_blank_with_unknown(df[col])
        
        df['annrev'] = replace_blank_with_zero(df['annrev'])
        df['empsize'] = replace_blank_with_zero(df['empsize'])
        df['comp_phone'] = df['comp_phone'].fillna('').astype(str).str.strip()
        df = convert_qa_disposition(df)
        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].astype('string')
        df = truncate_linkedin_fields_with_log(df, ['emplinkedin', 'comp_linkedin'])
        df = clean_urls(df, ['emplinkedin', 'comp_linkedin', 'comp_domain'])
        
        columns_dims = {
            'jobtitle': 'dim_jobtitles',
            'comp_street': 'dim_addresses',
            'comp_country': 'dim_countries',
            'comp_zipcode': 'dim_postalcodes',
            'comp_industry': 'dim_industries',
            'comp_city': 'dim_cities',
            'comp_state': 'dim_states'
        }
        
        with next(get_db()) as db:
            df = enrich_and_merge_dim(df, 'comp_country', 'dim_countries', db)
            df = enrich_and_merge_dim(df, 'comp_street', 'dim_addresses', db)
            df = enrich_and_merge_dim(df, 'comp_zipcode', 'dim_postalcodes', db)
            df = enrich_and_merge_dim(df, 'comp_industry', 'dim_industries', db)
            df = enrich_and_merge_dim(df, 'comp_city', 'dim_cities', db)
            df = enrich_and_merge_dim(df, 'comp_state', 'dim_states', db)
            df = enrich_and_merge_dim(df, 'jobtitle', 'dim_jobtitles', db)

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
        for col in ['name', 'comp_domain', 'comp_linkedin']:
            df_companies[col] = df_companies[col].replace('', None)
        with next(get_db()) as db:
            df_companies = get_company_ids(df_companies, db)
            df_companies = compare_companies_to_db(df_companies, db)
            df_companies = upsert_companies(df_companies, db)

        df = df.merge(df_companies[['index', 'company_id']], on='index', how='left') 

        df_contacts = df[[
            'index', 'name', 'firstname', 'lastname',
            'emplinkedin', 'empemail', 'qa_disposition',
            'jobtitle_id', 'company_id'
        ]].copy()

        df_contacts = df_contacts.rename(columns={'qa_disposition': 'emailstatus_id'})
        df_contacts['address_id'] = 999999
        df_contacts['city_id'] = 999999
        df_contacts['state_id'] = 999999
        df_contacts['postalcode_id'] = 999999
        df_contacts['country_id'] = 999999

        with next(get_db()) as db:
            # Get contact id's from the database based on matching email and linkedin values
            df_contacts = get_contact_ids(df_contacts, db)
        
df_contacts