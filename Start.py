#------------------------------------ import necessary libraries ---------------------------------------------------
import streamlit as st
from streamlit_extras.switch_page_button import switch_page
from sqlalchemy import create_engine, text
from app_backend.database import get_db  # Import the get_db function from your backend
import json
from styles.style import apply_custom_styles

#------------------------------------ Set global UI ---------------------------------------------------
apply_custom_styles()

#------------------------------------ user authentication check ---------------------------------------------------
# Ensure the user is authenticated before accessing this page
if not st.session_state.get("authenticated"):
    switch_page("Home")

#------------------------------------ page title -------------------------------------------------------------------
st.title("🚀 Start Searching")

#------------------------------------ database connection ----------------------------------------------------------
# Get a new database session instance using the generator from get_db()
db = next(get_db())

#------------------------------------ tab layout: new query, new campaign, or load saved query ------------------------
tab1, tab2, tab3 = st.tabs(["📝 Create New Query", "📢 Create New Campaign", "📂 Load Saved Query"])

#------------------------------------ tab 1: create a new query ----------------------------------------------------
with tab1:
    st.subheader("Create a New Query")
    
    # Input field for new query name
    new_query_name = st.text_input("Query Name", key="new_query_name")
    
    if st.button("Start Search", key="start_query_btn") and new_query_name:
        # Save values to Streamlit session state
        st.session_state.campaign_name = new_query_name
        st.session_state.mode = "new_query"
        st.session_state.campaign_ready = True
        st.session_state.query_id = None  # Not linked to any campaign

        # Insert new query into dim_savedqueries table (PostgreSQL compatible)
        with next(get_db()) as db:
            db.execute(
                text("INSERT INTO dim_savedqueries (name, timestamp, filters, campaign_id) VALUES (:name, CURRENT_TIMESTAMP, '', NULL)"),
                {"name": new_query_name}
            )
            db.commit()

        # Redirect to Search Data page
        switch_page("Search_Data")

#------------------------------------ tab 2: create a new campaign -------------------------------------------------
with tab2:
    st.subheader("Create a New Campaign")
    
    # Input fields for campaign name and ID
    new_campaign_name = st.text_input("Campaign Name", key="new_campaign_name")
    new_campaign_id = st.text_input("Campaign ID", key="new_campaign_id")

    if st.button("Start Search", key="start_campaign_btn") and new_campaign_name and new_campaign_id:
        # Save values to Streamlit session state
        st.session_state.campaign_name = new_campaign_name
        st.session_state.campaign_id = new_campaign_id
        st.session_state.mode = "new_campaign"
        st.session_state.campaign_ready = True

        # Insert campaign-related query into dim_savedqueries table (PostgreSQL compatible)
        with next(get_db()) as db:
            db.execute(
                text("INSERT INTO dim_savedqueries (name, timestamp, filters, campaign_id) VALUES (:name, CURRENT_TIMESTAMP, '', :campaign_id)"),
                {"name": new_campaign_name, "campaign_id": new_campaign_id}
            )
            db.commit()

        # Redirect to Search Data page
        switch_page("Search_Data")

#------------------------------------ tab 3: load saved query -----------------------------------------------------
with tab3:
    st.subheader("Load a Saved Query")

    # Retrieve saved queries from database
    results = db.execute(text("SELECT id, name FROM dim_savedqueries ORDER BY timestamp DESC")).fetchall()

    # Convert query results to a dictionary for the selectbox
    query_options = {row.name: row.id for row in results}
    selected_query = st.selectbox("Select a saved query", list(query_options.keys()))

    if st.button("Load Saved Query") and selected_query:
        selected_id = query_options[selected_query]

        # Get filters from database
        result = db.execute(text("SELECT filters FROM dim_savedqueries WHERE id = :id"), {"id": selected_id}).fetchone()
        filters = json.loads(result[0]) if result and result[0] else {}

        # Store everything in session state
        st.session_state.campaign_name = selected_query
        st.session_state.mode = "load"
        st.session_state.query_id = selected_id
        st.session_state.filters = filters  # ✅ Save filters
        st.session_state.campaign_ready = True

        # Redirect
        switch_page("Search_Data")
