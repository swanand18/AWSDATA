# logic.py

import pandas as pd
from sqlalchemy import text
from app_backend.database import engine
import streamlit as st
import json
from functions import get_uploaded_filter_conditions

PAGE_SIZE = 100

@st.cache_data(ttl=86400)
def get_total_count(where_clause, params):
    with engine.connect() as conn:
        query = text(f"SELECT COUNT(*) FROM cached_full_contacts_data WHERE {where_clause}")
        return conn.execute(query, params).scalar()

@st.cache_data(ttl=86400)
# Function to get all filtered data (ignores pagination)
def get_all_filtered_data(where_clause, params):
    with engine.connect() as conn:
        query = text(f"""
            SELECT * FROM cached_full_contacts_data
            WHERE {where_clause}
            ORDER BY id
        """)
        # Don't use LIMIT and OFFSET for the full data export
        return pd.read_sql(query, conn, params=params)

@st.cache_data(ttl=86400)
def get_page_data(where_clause, params, offset):
    with engine.connect() as conn:
        query = text(f"""
            SELECT * FROM cached_full_contacts_data
            WHERE {where_clause}
            ORDER BY id
            OFFSET :offset LIMIT :limit
        """)
        params.update({"offset": offset, "limit": PAGE_SIZE})
        return pd.read_sql(query, conn, params=params)

def get_full_filtered_data(where_clause, params):
    query = f"""
        SELECT *
        FROM cached_full_contacts_data
        WHERE {where_clause}
        ORDER BY companyname
    """
    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df

def build_filter_conditions(filters, suppression_file, tal_file):
    conditions = []
    params = {}

    def add_in_condition(field, values, param_prefix):
        if values and values != ["All"]:
            placeholders = []
            for i, val in enumerate(values):
                key = f"{param_prefix}_{i}"
                placeholders.append(f":{key}")
                params[key] = val
            conditions.append(f"{field} IN ({', '.join(placeholders)})")

    add_in_condition("country", filters.get("country", []), "country")
    add_in_condition("compstate", filters.get("compstate", []), "compstate")
    add_in_condition("city", filters.get("city", []), "city")
    add_in_condition("industry", filters.get("industry", []), "industry")
    add_in_condition("emailstatus", filters.get("emailstatus", []), "emailstatus")

    # Job title text search
    if filters.get("jobtitle_text"):
        keywords = [kw.strip() for kw in filters["jobtitle_text"].split(",") if kw.strip()]
        if keywords:
            sub_conditions = []
            for i, kw in enumerate(keywords):
                key = f"jobtitle_text_{i}"
                sub_conditions.append(f"jobtitle ILIKE :{key}")
                params[key] = f"%{kw}%"
            conditions.append("(" + " OR ".join(sub_conditions) + ")")

    add_in_condition("companyname", filters.get("companyname", []), "company")
    add_in_condition("managementlevel", filters.get("managementlevel", []), "managementlevel")

    # Employee size
    if filters.get("empsize") and filters["empsize"] != ["All"]:
        size_conditions = []
        for i, val in enumerate(filters["empsize"]):
            key_min, key_max = f"empsize_min_{i}", f"empsize_max_{i}"
            if val == "10,000+":
                size_conditions.append(f"empsize >= :{key_min}")
                params[key_min] = 10000
            else:
                low, high = val.replace(",", "").split("-")
                size_conditions.append(f"(empsize >= :{key_min} AND empsize <= :{key_max})")
                params[key_min] = int(low)
                params[key_max] = int(high)
        conditions.append("(" + " OR ".join(size_conditions) + ")")

    # Revenue
    if filters.get("annrev") and filters["annrev"] != ["All"]:
        revenue_conditions = []
        for val in filters["annrev"]:
            ranges = {
                "0 - 1M": "annrev BETWEEN 0 AND 1000000",
                "1M - 10M": "annrev BETWEEN 1000000 AND 10000000",
                "10M - 100M": "annrev BETWEEN 10000000 AND 100000000",
                "100M - 500M": "annrev BETWEEN 100000000 AND 500000000",
                "500M - 1B": "annrev BETWEEN 500000000 AND 1000000000",
                "1B - 5B": "annrev BETWEEN 1000000000 AND 5000000000",
                "5B - 10B": "annrev BETWEEN 5000000000 AND 10000000000",
                "10B+": "annrev > 10000000000"
            }
            if val in ranges:
                revenue_conditions.append(ranges[val])
        if revenue_conditions:
            conditions.append(f"({' OR '.join(revenue_conditions)})")

    suppression_conditions, suppression_params = get_uploaded_filter_conditions(suppression_file, "exclude")
    tal_conditions, tal_params = get_uploaded_filter_conditions(tal_file, "include")

    conditions += suppression_conditions + tal_conditions
    params.update(suppression_params)
    params.update(tal_params)

    return " AND ".join(conditions) if conditions else "1=1", params

def update_campaign_query(campaign_name, filters):
    with engine.begin() as conn:
        query = text("""SELECT id FROM dim_savedqueries WHERE name = :campaign_name""")
        result = conn.execute(query, {"campaign_name": campaign_name}).fetchone()

        if result:
            campaign_id = result[0]
            update_query = text("""
                UPDATE dim_savedqueries
                SET filters = :filters
                WHERE id = :campaign_id
            """)
            conn.execute(update_query, {
                "filters": json.dumps(filters),
                "campaign_id": campaign_id
            })
            #st.success(f"Filters for campaign '{campaign_name}' have been updated successfully.")
        else:
            st.warning(f"Campaign '{campaign_name}' not found.")
