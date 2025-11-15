import streamlit as st
import pandas as pd
from io import BytesIO
from sqlalchemy import text
from streamlit_extras.switch_page_button import switch_page
import traceback

from functions import (
    check_uploaded_file_headers, copy_to_staging_table, clear_staging_table, log, remove_duplicates_from_staging,
    validate_and_clean_staging_data, normalize_and_enrich_dim, clean_staging_companies, clean_annrev_empsize,
    upsert_fact_companies_from_staging, clean_staging_contacts, upsert_fact_contacts_from_staging,
    refresh_cached_contacts_tables, get_filter_options_from_cache, validate_dataset, prepare_validation_results
)
from app_backend.database import get_db, DB_HOST, engine

st.set_page_config(page_title="Admin", layout="wide")

from styles.style import apply_custom_styles

apply_custom_styles()

# --------------------------- Authentication Check ----------------------------
if not st.session_state.get("authenticated"):
    switch_page("Home")
    
# --------------------------- Page Visit Log Clearing ------------------------
if st.session_state.get("page_last") != "Admin":
    if "import_log_df" in st.session_state:
        del st.session_state.import_log_df
    st.session_state["page_last"] = "Admin"

# --------------------------- Ensure log DataFrame ----------------------------
if "import_log_df" not in st.session_state:
    st.session_state.import_log_df = pd.DataFrame(columns=["timestamp", "level", "message"])

# --------------------------- Page Config ----------------------------
st.markdown("<h1 style='font-size: 24px;'>🔍 Admin</h1>", unsafe_allow_html=True)

# --------------------------- Tabs ----------------------------
tab1, tab2 = st.tabs(["📤 Upload New Data", "📊 Database Tables"])
if "import_status" not in st.session_state:
    st.session_state.import_status = ""
if "import_triggered" not in st.session_state:
    st.session_state.import_triggered = False

# --------------------------- Tab 1: Upload New Data ----------------------------
with tab1:
    template_columns = [
        'comp_name',  'comp_domain', 'annrev', 'comp_industry', 'comp_linkedin',
        'firstname', 'lastname', 'jobtitle', 'manlevel', 'empemail', 'emplinkedin', 'country_code', 'comp_phone',
        'comp_street', 'comp_city', 'comp_state', 'comp_country', 'comp_zipcode',
        'qa_disposition', 'empsize'
    ]

    template_df = pd.DataFrame(columns=template_columns)
    buffer = BytesIO()
    template_df.to_excel(buffer, index=False)
    buffer.seek(0)

    st.markdown(
        """
        <style>
        .grey-download-button button {
            background-color: #d3d3d3 !important;
            color: black !important;
            border-radius: 5px;
            border: 1px solid #aaa;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    with st.container():
        st.markdown('<div class="grey-download-button">', unsafe_allow_html=True)
        st.download_button(
            label="📥 Download Import Data Template",
            data=buffer,
            file_name="import_data_template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_template"
        )
        
    validation_file = st.file_uploader("Validate your dataset", type=["csv","xlsx"], key="validation_uploader")


    if validation_file is not None:
        res = validate_dataset(validation_file)

        # Quick headline messages
        if res["missing_columns"]:
            st.error(f"❌ Missing columns: {', '.join(res['missing_columns'])}")
        if res["unexpected_columns"]:
            st.warning(f"⚠️ Unexpected columns: {', '.join(res['unexpected_columns'])}")
        if not res["column_order_valid"]:
            st.warning("⚠️ Column order does not match the required template.")

        # Build output
        packaged, is_download = prepare_validation_results(res)
        if packaged is None and not is_download:
            st.success("✅ Validation passed.")
        elif is_download:
            st.download_button(
                "Download validation report (Excel)",
                data=packaged,
                file_name="validation_results.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        else:
            st.dataframe(packaged, use_container_width=True)

    uploaded_campaign_file = st.file_uploader(
        "Importing records from the uploaded file",
        type=["csv", "xlsx"], key="campaign_uploader"
    )

    info_placeholder = st.empty()  # Step 1: Create placeholder

    if uploaded_campaign_file is not None:
        info_placeholder.info("🚀 Import started. Please wait while your data is processed…") 
        st.session_state['import_log'] = []
        expected_columns = template_columns
        try:
            log("🧹 Step 1 Clearing staging table...")
            clear_staging_table(log, engine)
            log("📄 Step 2: Checking file headers…")
            check_uploaded_file_headers(uploaded_campaign_file, log, expected_columns)
            log("📄 Step 3: Copying data to staging table…")
            num_rows = copy_to_staging_table(uploaded_campaign_file, log, engine, expected_columns)
            log("📄 Step 4: Removing duplicates from Staging Table…")
            remove_duplicates_from_staging(log, engine)
            st.session_state.campaign_import_status = (
                f"✅ {num_rows} records copied to staging table."
            )
            info_placeholder.empty()

            # ---- CLEANER USER MESSAGING ----
            if num_rows < 5000:
                st.success(
                    f"Imported {num_rows} rows. 🟢 All processing will be handled directly in the app."
                )
            else:
                st.success(
                    f"Imported {num_rows} rows to staging table. 🟡 Large import: Please run the SQL script in your database (e.g., PGAdmin) to complete processing."
                )

            # -- <5000: in-app ETL --
            if num_rows < 5000:
                log("🚦 Running ETL steps in the app (for small file)...")
                validate_and_clean_staging_data(log, engine)
                for col, dim in [
                    ('comp_street', 'dim_addresses'),
                    ('comp_city', 'dim_cities'),
                    ('comp_industry', 'dim_industries'),
                    ('comp_zipcode', 'dim_postalcodes'),
                    ('manlevel', 'dim_manlevels'),
                    ('jobtitle', 'dim_jobtitles'),
                    ('comp_country', 'dim_countries'),
                    ('comp_state', 'dim_states'),
                ]:
                    normalize_and_enrich_dim(log, engine, col, dim)
                clean_staging_companies(engine)
                log("✅ Cleaned company fields in staging table.")
                clean_annrev_empsize(engine)
                log("✅ Cleaned annrev and empsize in staging table.")
                upsert_fact_companies_from_staging(log, engine)
                clean_staging_contacts(engine)
                upsert_fact_contacts_from_staging(log, engine)
                refresh_cached_contacts_tables(log, engine)
                log("🔄 App cache cleared. 1_Data_Explorer.py will now show latest data.")
                st.cache_data.clear()
                st.session_state["filter_options"] = get_filter_options_from_cache()
                log("✅ All ETL steps completed for <5000 records.")
                st.success("All records processed in the app. Data is ready.")
                st.cache_data.clear()

            # -- >5000: only cleaning and warning for SQL
            else:
                log("🧹 Cleaning data in staging table (no enrichment)...")
                validate_and_clean_staging_data(log, engine)
                log("✅ Staging data cleaned (no enrichment or upserts).")
                st.warning(
                    "You can now run the SQL script in PGAdmin to enrich and upsert the data into the main database tables."
                )
                if st.button("Refresh data after running SQL script"):
                    st.cache_data.clear()
                    st.session_state["filter_options"] = get_filter_options_from_cache()
                    st.success("Cache cleared and filter options reset. The explorer page will show the latest data.")

        except Exception as e:
            info_placeholder.empty()
            st.session_state.campaign_import_status = f"❌ Import failed:\n\n{e}"
            log(f"❌ Exception occurred: {e}", "ERROR")
            log(traceback.format_exc(), "ERROR")
            st.error(f"❌ Import failed. See downloadable log for details.")

    def clear_import_log():
        if "import_log_df" in st.session_state:
            del st.session_state.import_log_df

    if "import_log_df" in st.session_state and not st.session_state.import_log_df.empty:
        log_txt = st.session_state.import_log_df.to_string(index=False)
        st.download_button(
            label="📥 Download Import Log (TXT)",
            data=log_txt,
            file_name="import_log.txt",
            mime="text/plain",
            on_click=clear_import_log
        )

# --------------------------- Tab 2: Browse Tables ----------------------------
with tab2:
    table_options = [
        "dim_jobtitles", "dim_manlevels", "dim_emailstatuses",
        "dim_countries", "dim_states", "dim_cities",
        "dim_postalcodes", "dim_addresses", "dim_industries",
        "fact_companies"
    ]

    if "admin_page_number" not in st.session_state:
        st.session_state.admin_page_number = 0
    if "selected_admin_table" not in st.session_state:
        st.session_state.selected_admin_table = table_options[0]

    col1, col2 = st.columns([2, 3])

    with col1:
        st.markdown("**📋 Choose a Table**", unsafe_allow_html=True)
        selected_table = st.selectbox(
            "", table_options,
            index=table_options.index(st.session_state.selected_admin_table)
        )

    with col2:
        st.markdown("**🔍 Search in Name Column**", unsafe_allow_html=True)
        name_input = st.text_input("Filter by name", placeholder="Type to filter ...", label_visibility="collapsed")

    if selected_table != st.session_state.selected_admin_table:
        st.session_state.admin_page_number = 0
        st.session_state.selected_admin_table = selected_table

    with next(get_db()) as db:
        query = text(f"SELECT * FROM {selected_table}")
        result = db.execute(query)
        df_full = pd.DataFrame(result.fetchall(), columns=result.keys())

    df_filtered = df_full.copy()
    if "name" in df_filtered.columns and st.session_state.get("admin_name_search"):
        df_filtered = df_filtered[df_filtered["name"].astype(str).str.contains(
            st.session_state.admin_name_search, case=False, na=False)]

    rows_per_page = 50
    total_rows = len(df_filtered)
    total_pages = max(1, (total_rows - 1) // rows_per_page + 1)

    st.divider()
    st.write(f"**Total Results: {total_rows}**")
    st.write(f"Page **{st.session_state.admin_page_number + 1}** of **{total_pages}**")

    start_idx = st.session_state.admin_page_number * rows_per_page
    end_idx = start_idx + rows_per_page
    st.dataframe(df_filtered.iloc[start_idx:end_idx], use_container_width=True)

    pagination_row = st.columns([0.3, 0.3, 0.3, 0.3, 7])
    with pagination_row[0]:
        if st.session_state.admin_page_number > 0 and st.button("⏮️", key="admin_first"):
            st.session_state.admin_page_number = 0
            st.rerun()
    with pagination_row[1]:
        if st.session_state.admin_page_number > 0 and st.button("◀️", key="admin_prev"):
            st.session_state.admin_page_number -= 1
            st.rerun()
    with pagination_row[2]:
        if st.session_state.admin_page_number < total_pages - 1 and st.button("▶️", key="admin_next"):
            st.session_state.admin_page_number += 1
            st.rerun()
    with pagination_row[3]:
        if st.session_state.admin_page_number < total_pages - 1 and st.button("⏭️", key="admin_last"):
            st.session_state.admin_page_number = total_pages - 1
            st.rerun()

    st.download_button(
        "📥 Download Filtered CSV",
        data=df_filtered.to_csv(index=False).encode("utf-8"),
        file_name=f"{selected_table}_filtered.csv"
    )
