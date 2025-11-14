import streamlit as st
from streamlit_extras.switch_page_button import switch_page
from styles.style import apply_custom_styles
from functions import get_filter_options_from_cache
import json
from app_backend.database import get_db
from sqlalchemy import text
import io
import zipfile
from streamlit_extras.switch_page_button import switch_page
from styles.style import apply_custom_styles
from functions import get_filter_options_from_cache
import json
from app_backend.database import get_db
from sqlalchemy import text
import io
import zipfile

#------------------------------------ Set global UI ---------------------------------------------------
apply_custom_styles()
filter_options = get_filter_options_from_cache()
filter_options = get_filter_options_from_cache()

#------------------------------------ user authentication check ---------------------------------------------------
if not st.session_state.get("authenticated"):
    switch_page("Home")
else:
    st.markdown("<h1 style='font-size: 24px;'>🔍 Data Explorer</h1>", unsafe_allow_html=True)

    if 'save_query_visible' not in st.session_state:
        st.session_state['save_query_visible'] = False
    if 'load_query_visible' not in st.session_state:
        st.session_state['load_query_visible'] = False

    # --- Save Query Section ---
    if st.session_state['save_query_visible']:
        st.markdown("### Save Query")
        default_campaign_id = st.session_state.get("campaign_id", "")
        default_query_name = st.session_state.get("saved_query_name", "")

        campaign_id = st.text_input("Campaign ID (Optional)", value=default_campaign_id, key="campaign_id_input")
        query_name = st.text_input("Name", value=default_query_name, key="query_name_input")

        is_update = bool(st.session_state.get("saved_query_name"))
        action_button_label = "Update" if is_update else "Save"
        action_button = st.button(action_button_label)

        if action_button:
            applied_filters = {
                "country": st.session_state.get("country_filter", []),
                "compstate": st.session_state.get("compstate_filter", []),
                "city": st.session_state.get("city_filter", []),
                "companyname": st.session_state.get("company_filter", []),
                "industry": st.session_state.get("industry_filter", []),
                "empsize": st.session_state.get("empsize_filter", []),
                "annrev": st.session_state.get("revenue_filter", []),
                "jobtitle": st.session_state.get("job_title_filter", []),
                "managementlevel": st.session_state.get("management_level_filter", []),
                "emailstatus": st.session_state.get("email_status_filter", []),
            }
            applied_filters = {k: v for k, v in applied_filters.items() if v}
            filters_json = json.dumps(applied_filters) if applied_filters else ""

            db = next(get_db())
            if is_update:
                update_query = text("""
                    UPDATE dim_savedqueries
                    SET filters = :filters, campaign_id = :campaign_id
                    WHERE name = :name
                """)
                db.execute(update_query, {
                    "filters": filters_json,
                    "campaign_id": campaign_id if campaign_id else None,
                    "name": default_query_name
                })
                db.commit()
                st.success("✅ Query updated successfully!")
            else:
                insert_query = text("""
                    INSERT INTO dim_savedqueries (name, timestamp, filters, campaign_id)
                    VALUES (:name, now(), :filters, :campaign_id)
                """)
                db.execute(insert_query, {
                    "name": query_name,
                    "filters": filters_json,
                    "campaign_id": campaign_id if campaign_id else None
                })
                db.commit()
                st.success("✅ Query saved successfully!")

                st.session_state['saved_query_name'] = query_name

            st.session_state['save_query_visible'] = False
            st.rerun()

    # --- Load Query Section ---
    if st.session_state['load_query_visible']:
        st.markdown("### Load Saved Query")
        db = next(get_db())
        result = db.execute(text("SELECT id, name FROM dim_savedqueries ORDER BY name"))
        saved_queries = result.fetchall()

        if saved_queries:
            options = {row.name: row.id for row in saved_queries}
            selected_query_name = st.selectbox("Select a Query", options.keys(), key="selected_saved_query")

            load_button = st.button("Load")

            if load_button:
                selected_query_id = options[selected_query_name]
                query_result = db.execute(text("SELECT filters FROM dim_savedqueries WHERE id = :id"), {"id": selected_query_id}).fetchone()
                if query_result:
                    filters_json = query_result[0]

                    if filters_json:
                        loaded_filters = json.loads(filters_json)

                        st.session_state["country_filter"] = loaded_filters.get("country", [])
                        st.session_state["compstate_filter"] = loaded_filters.get("compstate", [])
                        st.session_state["city_filter"] = loaded_filters.get("city", [])
                        st.session_state["company_filter"] = loaded_filters.get("companyname", [])
                        st.session_state["industry_filter"] = loaded_filters.get("industry", [])
                        st.session_state["empsize_filter"] = loaded_filters.get("empsize", [])
                        st.session_state["revenue_filter"] = loaded_filters.get("annrev", [])
                        st.session_state["job_title_filter"] = loaded_filters.get("jobtitle", [])
                        st.session_state["management_level_filter"] = loaded_filters.get("managementlevel", [])
                        st.session_state["email_status_filter"] = loaded_filters.get("emailstatus", [])

                        st.session_state['saved_query_name'] = selected_query_name

                st.session_state['load_query_visible'] = False
                st.rerun()
        else:
            st.info("No saved queries found yet.")

    # --- Show Saved Query Name ---
    if 'saved_query_name' in st.session_state and st.session_state['saved_query_name']:
        st.markdown(f"**Saved Query Name:** {st.session_state['saved_query_name']}")
    else:
        st.markdown("**Saved Query Name:** New Query")

    PAGE_SIZE = 100

    if "page_number" not in st.session_state:
        st.session_state.page_number = 0
    if "reset_filters_requested" not in st.session_state:
        st.session_state.reset_filters_requested = False

            st.session_state.apply_filters_requested = True
            st.session_state.reset_filters_requested = True

    if st.session_state.get("reset_filters_requested", True):
        filter_keys = [
            "country_filter", "compstate_filter", "city_filter",
            "company_filter", "industry_filter", "empsize_filter", "revenue_filter",
            "job_title_filter", "management_level_filter", "email_status_filter",
            "suppression_file", "tal_file"
        ]
        for key in filter_keys:
            if key in st.session_state:
                st.session_state[key] = None if "file" in key else []
        st.session_state.reset_filters_requested = False

# --- Sidebar Filters ---
# --- Sidebar Filter Controls ---
if "page_number" not in st.session_state:
if "reset_filters_requested" not in st.session_state:
    st.session_state.reset_filters_requested = False
if "apply_filters_requested" not in st.session_state:
    st.session_state.apply_filters_requested = False

with st.sidebar:
    if st.button("♻️ Reset Filters"):
    if st.button("✅ Apply Filters"):

    with st.expander("🌍 Location Filters", expanded=False):
        country_filter = st.session_state.get("country_filter", [])
        compstate_filter = st.session_state.get("compstate_filter", [])
        city_filter = st.session_state.get("city_filter", [])

        country_filter = st.multiselect("🌍 Country", filter_options["country"], default=country_filter)
        compstate_filter = st.multiselect("🏛️ Company State", filter_options["compstate"], default=compstate_filter)
        city_filter = st.multiselect("🏙️ City", filter_options["city"], default=city_filter)

        st.session_state["country_filter"] = country_filter
        st.session_state["compstate_filter"] = compstate_filter
        st.session_state["city_filter"] = city_filter

    with st.expander("🏢 Company Filters", expanded=False):
        company_filter = st.session_state.get("company_filter", [])
        industry_filter = st.session_state.get("industry_filter", [])
        empsize_filter = st.session_state.get("empsize_filter", [])
        revenue_filter = st.session_state.get("revenue_filter", [])

        company_filter = st.multiselect("🏢 Company", filter_options["companyname"], default=company_filter)
        industry_filter = st.multiselect("🏢 Industry", filter_options["industry"], default=industry_filter)
        empsize_filter = st.multiselect("👥 Employee Size", filter_options["empsize"], default=empsize_filter)
        revenue_filter = st.multiselect("💰 Annual Revenue", filter_options["annrev"], default=revenue_filter)

        st.session_state["company_filter"] = company_filter
        st.session_state["industry_filter"] = industry_filter
        st.session_state["empsize_filter"] = empsize_filter
        st.session_state["revenue_filter"] = revenue_filter

    with st.expander("🧑‍💼 Contact Filters", expanded=False):
        jobtitle_filter = st.session_state.get("job_title_filter", [])
        managementlevel_filter = st.session_state.get("management_level_filter", [])
        email_status_filter = st.session_state.get("email_status_filter", [])

        jobtitle_filter = st.multiselect("🧑‍💼 Job Title", filter_options["jobtitle"], default=jobtitle_filter)
        managementlevel_filter = st.multiselect("🏷️ Management Level", filter_options["managementlevel"], default=managementlevel_filter)
        email_status_filter = st.multiselect("📬 Email Status", filter_options["emailstatus"], default=email_status_filter)

        st.session_state["job_title_filter"] = jobtitle_filter
        st.session_state["management_level_filter"] = managementlevel_filter
        st.session_state["email_status_filter"] = email_status_filter

    with st.expander("📂 Campaign Filters", expanded=False):
        suppression_file = st.file_uploader("Upload Suppression File", type=["csv"], label_visibility="visible", key="suppression_file_uploader")
        tal_file = st.file_uploader("Upload TAL File", type=["csv"], label_visibility="visible", key="tal_file_uploader")

        if suppression_file is not None:
            st.session_state["suppression_file"] = suppression_file
        if tal_file is not None:
            st.session_state["tal_file"] = tal_file
        with st.expander("🌍 Location Filters", expanded=False):
            country_filter = st.session_state.get("country_filter", [])
            compstate_filter = st.session_state.get("compstate_filter", [])
            city_filter = st.session_state.get("city_filter", [])

            country_filter = st.multiselect("🌍 Country", filter_options["country"]
            compstate_filter = st.multiselect("🏛️ Company State", filter_options["compstate"]
            city_filter = st.multiselect("🏙️ City", filter_options["city"]

            st.session_state["country_filter"] = country_filter
            st.session_state["compstate_filter"] = compstate_filter
            st.session_state["city_filter"] = city_filter

        with st.expander("🏢 Company Filters", expanded=False):
            company_filter = st.session_state.get("company_filter", [])
            industry_filter = st.session_state.get("industry_filter", [])
            empsize_filter = st.session_state.get("empsize_filter", [])
            revenue_filter = st.session_state.get("revenue_filter", [])

            company_filter = st.multiselect("🏢 Company", filter_options["companyname"]
            industry_filter = st.multiselect("🏢 Industry", filter_options["industry"]
            empsize_filter = st.multiselect("👥 Employee Size", ["2-10", "11-50", "51-200", "200-500", "500-1000", "1000-5000", "5000-10000", "10,000+"], default=empsize_filter)
            revenue_filter = st.multiselect("💰 Annual Revenue", ["0 - 1M", "1M - 10M", "10M - 100M", "100M - 500M", "500M - 1B", "1B - 5B", "5B - 10B", "10B+"], default=revenue_filter)

            st.session_state["company_filter"] = company_filter
            st.session_state["industry_filter"] = industry_filter
            st.session_state["empsize_filter"] = empsize_filter
            st.session_state["revenue_filter"] = revenue_filter

        with st.expander("🧑‍💼 Contact Filters", expanded=False):
            jobtitle_filter = st.session_state.get("job_title_filter", [])
            managementlevel_filter = st.session_state.get("management_level_filter", [])
            email_status_filter = st.session_state.get("email_status_filter", [])

            jobtitle_filter = st.multiselect("🧑‍💼 Job Title", filter_options["jobtitle"]
            managementlevel_filter = st.multiselect("🏷️ Management Level", filter_options["managementlevel"]
            email_status_filter = st.multiselect("📬 Email Status", filter_options["emailstatus"]

            st.session_state["job_title_filter"] = jobtitle_filter
            st.session_state["management_level_filter"] = managementlevel_filter
            st.session_state["email_status_filter"] = email_status_filter

        with st.expander("📂 Campaign Filters", expanded=False):
            suppression_file = st.file_uploader("Upload Suppression File", type=["csv"], label_visibility="visible", key="suppression_file_uploader")
            tal_file = st.file_uploader("Upload TAL File", type=["csv"], label_visibility="visible", key="tal_file_uploader")

            if suppression_file is not None:
                st.session_state["suppression_file"] = suppression_file
            if tal_file is not None:
                st.session_state["tal_file"] = tal_file

    # --- Filters and Query ---
    filters = {
        "companyname": company_filter,
        "industry": industry_filter,
        "empsize": empsize_filter,
        "annrev": revenue_filter,
        "country": country_filter,
        "compstate": compstate_filter,
        "city": city_filter,
        "jobtitle": jobtitle_filter,
        "managementlevel": managementlevel_filter,
        "emailstatus": email_status_filter,
    }

    suppression_file = st.session_state.get("suppression_file")
    tal_file = st.session_state.get("tal_file")

# Initialize default filter values if not set
if "apply_filters_requested" not in st.session_state:
    st.session_state.apply_filters_requested = False

if st.session_state.get("apply_filters_requested"):
    st.session_state["filters"] = {
        "companyname": st.session_state.get("company_filter", []),
        "industry": st.session_state.get("industry_filter", []),
        "empsize": st.session_state.get("empsize_filter", []),
        "annrev": st.session_state.get("revenue_filter", []),
        "country": st.session_state.get("country_filter", []),
        "compstate": st.session_state.get("compstate_filter", []),
        "city": st.session_state.get("city_filter", []),
        "jobtitle": st.session_state.get("job_title_filter", []),
        "managementlevel": st.session_state.get("management_level_filter", []),
        "emailstatus": st.session_state.get("email_status_filter", []),
    }
    st.session_state["suppression_file"] = st.session_state.get("suppression_file")
    st.session_state["tal_file"] = st.session_state.get("tal_file")
    st.session_state.apply_filters_requested = False

# Use stored filters (or empty if not yet applied)
filters = st.session_state.get("filters", {})
suppression_file = st.session_state.get("suppression_file", None)
tal_file = st.session_state.get("tal_file", None)


    # --- Sidebar Options ---
        st.markdown("---")
        with st.expander("⚙️ Options", expanded=False):
            save_query = st.button("💾 Save Query", use_container_width=True)
            load_query = st.button("📂 Load Query", use_container_width=True)
            export_data = st.button("📤 Export Data", use_container_width=True)
            new_query = st.button("➕ New Query", use_container_width=True)
            logout = st.button("🔒 Logout", use_container_width=True)

            if save_query:
                st.session_state['save_query_visible'] = True
                st.rerun()
            if load_query:
                st.session_state['load_query_visible'] = True
                st.rerun()
            if export_data:
                total_count = get_total_count(where_clause, params)
                st.write(f"**Filtered Total Count: {total_count}**")

                if total_count > 5000:
                    st.warning("Total Count is more than 5000, please add more filters.")
                else:
                    st.success("You're good to go!")
                    full_data = get_full_filtered_data(where_clause, params)

                    st.download_button(
                        label="📥 Download Filtered Data",
                        data=full_data.to_csv(index=False).encode("utf-8"),
                        file_name="filtered_data.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
            if new_query:
                st.session_state['saved_query_name'] = ""
                st.session_state['campaign_id'] = ""
                st.session_state['save_query_visible'] = False
                filter_keys = [
                    "country_filter", "compstate_filter", "city_filter",
                    "company_filter", "industry_filter", "empsize_filter", "revenue_filter",
                    "job_title_filter", "management_level_filter", "email_status_filter",
                    "suppression_file", "tal_file"
                ]
                for key in filter_keys:
                    if key in st.session_state:
                        st.session_state[key] = [] if "file" not in key else None
                st.success("✅ Started a new query!")
                st.rerun()
            if logout:
                for key in list(st.session_state.keys()):
                    if key not in ["authenticated"]:
                        del st.session_state[key]
                st.session_state.authenticated = False
                switch_page("Home")

    # --- Query and Paginated Results ---
    total_count = get_total_count(where_clause, params)
    total_pages = max(1, ((total_count - 1) // PAGE_SIZE) + 1)
    data = get_page_data(where_clause, params, st.session_state.page_number * PAGE_SIZE)

    st.write(f"**Total Results: {total_count}**")
    st.write(f"Page **{st.session_state.page_number + 1}** of **{total_pages}**")
    st.dataframe(data, height=500, use_container_width=True)

    # --- Pagination Buttons ---
    pagination_row = st.columns([0.25, 0.25, 0.25, 0.25, 7])
    with pagination_row[0]:
        if st.session_state.page_number > 0 and st.button("⏮️"):
            st.session_state.page_changed = True
    with pagination_row[1]:
        if st.session_state.page_number > 0 and st.button("◀️"):
            st.session_state.page_number -= 1
            st.session_state.page_changed = True
    with pagination_row[2]:
        if st.session_state.page_number < total_pages - 1 and st.button("▶️"):
            st.session_state.page_number += 1
            st.session_state.page_changed = True
    with pagination_row[3]:
        if st.session_state.page_number < total_pages - 1 and st.button("⏭️"):
            st.session_state.page_number = total_pages - 1
            st.session_state.page_changed = True

    if st.session_state.get("page_changed", False):
        del st.session_state["page_changed"]
        st.rerun()
    