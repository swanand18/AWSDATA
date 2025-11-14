# main.py

import pandas as pd
from dotenv import load_dotenv
from functions import (
    get_or_create_dim_ids,
    get_existing_company_ids,
    prepare_unique_companies,
    load_new_data,
    get_or_create_jobtitle_ids,
    get_or_create_dim_id_value_pairs,
    insert_new_companies,
    get_or_create_state_ids
)
from database import get_db
from sqlalchemy import text

# Load environment variables
load_dotenv()

# ------------------------------- Load CSV --------------------------------------------------
df = load_new_data("D:/Repositories/FinaFunnel/FinalFunnel-data/import-data.csv")
df['index'] = df.index + 1

# ------------------------------- Extract base person/contact info --------------------------
df_new = df[['index', 'name', 'firstname', 'lastname', 'emplinkedin', 'empemail']].copy()

# ------------------------------- Enrich manlevel, jobtitle, and emailstatus ----------------
with next(get_db()) as db:
    df_lookup = df[['index', 'managementlevel', 'jobtitle', 'emailstatus']].copy()
    df_lookup['manlevel_id'] = get_or_create_dim_ids(df_lookup, db, "dim_manlevels", "managementlevel", "manlevel_id")
    df_lookup['jobtitle_id'] = get_or_create_jobtitle_ids(df_lookup, db)
    df_lookup['emailstatus_id'] = get_or_create_dim_ids(df_lookup, db, "dim_emailstatuses", "emailstatus", "emailstatus_id")

df_new = df_new.merge(df_lookup[['index', 'manlevel_id', 'jobtitle_id', 'emailstatus_id']], on='index', how='left')

# ------------------------------- Company Matching -------------------------------------------
df_companies = df[['index', 'companyname', 'comp_domain', 'comp_phone', 'comp_linkedin',
                   'annrev', 'empsize', 'address', 'city', 'country', 'compstate', 'postalcode', 'industry']].copy()
with next(get_db()) as db:
    matched_ids, df_unmatched_companies = get_existing_company_ids(df_companies, db)

df_new['company_id'] = df_new['index'].map(matched_ids)

# ------------------------------- Enrich and Insert New Companies ---------------------------
if not df_unmatched_companies.empty:
    df_unique_unmatched = prepare_unique_companies(df_unmatched_companies)

    with next(get_db()) as db:
        df_unique_unmatched['country_id'] = get_or_create_dim_id_value_pairs(
            df_unique_unmatched, db, 'dim_countries', 'country', 'name'
        )
        df_unique_unmatched['state_id'] = get_or_create_state_ids(df_unique_unmatched, db)
        df_unique_unmatched['address_id'] = get_or_create_dim_id_value_pairs(df_unique_unmatched, db, 'dim_addresses', 'address', 'name')
        df_unique_unmatched['city_id'] = get_or_create_dim_id_value_pairs(df_unique_unmatched, db, 'dim_cities', 'city', 'name')
        df_unique_unmatched['postalcode_id'] = get_or_create_dim_id_value_pairs(df_unique_unmatched, db, 'dim_postalcodes', 'postalcode', 'name')
        df_unique_unmatched['industry_id'] = get_or_create_dim_id_value_pairs(df_unique_unmatched, db, 'dim_industries', 'industry', 'name')

        company_id_map = insert_new_companies(df_unique_unmatched, db)

    df_new['company_id'] = df_new['company_id'].combine_first(df_new['index'].map(company_id_map))

# ------------------- Deduplicated Bulk Insert into fact_contacts --------------------------
with next(get_db()) as db:
    contacts_to_insert = df_new[df_new['company_id'].notna()].copy()

    if not contacts_to_insert.empty:
        existing = db.execute(text("""
            SELECT empemail, company_id FROM fact_contacts
            WHERE empemail IS NOT NULL
        """)).fetchall()

        existing_pairs = set((row.empemail.strip().lower(), row.company_id) for row in existing)

        def is_new_contact(row):
            email = str(row['empemail']).strip().lower()
            company_id = row['company_id']
            return (email, company_id) not in existing_pairs

        filtered_contacts = contacts_to_insert[contacts_to_insert.apply(is_new_contact, axis=1)]

        # Drop fields you don't want to insert
        final_contacts = filtered_contacts.drop(columns=['index'], errors='ignore')

        # Use only the columns that match fact_contacts schema
        records = final_contacts.to_dict(orient='records')

        if records:
            insert_stmt = text("""
                INSERT INTO fact_contacts (
                    name, firstname, lastname, emplinkedin, empemail,
                    jobtitle_id, emailstatus_id, company_id
                ) VALUES (
                    :name, :firstname, :lastname, :emplinkedin, :empemail,
                    :jobtitle_id, :emailstatus_id, :company_id
                )
            """)
            db.execute(insert_stmt, records)
            db.commit()
            print(f"✅ Inserted {len(records)} new contacts (after deduplication).")
        else:
            print("ℹ️ No new contacts to insert after deduplication.")
    else:
        print("ℹ️ No contacts with valid company_id to process.")