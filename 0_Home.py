#------------------------------------ import necessary libraries ---------------------------------------------------
import streamlit as st
from streamlit_extras.switch_page_button import switch_page
import os
from dotenv import load_dotenv
from styles.style import apply_custom_styles

#------------------------------------ load environment variables ---------------------------------------------------
load_dotenv('/home/ubuntu/aws-ff-data/env_details.txt')

#------------------------------------ set up Streamlit page config -------------------------------------------------
st.set_page_config(page_title="Login", layout="wide")
apply_custom_styles()

#------------------------------------ login form UI ----------------------------------------------------------------
st.title("🔐 Login")

# User input fields
username = st.text_input("Username")
password = st.text_input("Password", type="password")

#------------------------------------ fetch credentials from environment -------------------------------------------
correct_username = os.getenv("APP_USERNAME")
correct_password = os.getenv("APP_PASSWORD")

#------------------------------------ validate login credentials ---------------------------------------------------
if st.button("Login"):
    if username == correct_username and password == correct_password:
        # Mark user as authenticated
        st.session_state["authenticated"] = True
        # Redirect to the Start page
        switch_page("Data_Explorer")
    else:
        # Show error message
        st.error("Invalid username or password")
