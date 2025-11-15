# app_backend/refresh_cached.py
from functions import update_cached_contacts

if update_cached_contacts():
    print("✅ Cache refreshed.")
else:
    print("❌ Cache update failed.")
