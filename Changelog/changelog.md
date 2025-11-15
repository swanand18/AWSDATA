# Change Log for FinalFunnel Data Explorer App

## Introcduction

This document describes the changes made to the app.

## Changes

| Date | Change | Changed By | Status |
| -- | -- | -- | --|
| 04/28/2025 | Add import functionality for new data | Thabor Walbeek | Under Development |
| 04/28/2025 | Add Export functionality | Thabor Walbeek | Under Development | 
| 04/28/2025 | Re-alligned the buttons for Save, Reset and Export | Thabor Walbeek | Under Development |
| 04/28/2025 | Moved Logout button to navigation | Thabor Walbeek | Deployed |
| 04/28/2025 | Applied global css to make everything a bit smaller | Thabor Walbeek | Deployed |
| 04/28/2025 | Accept xlsx files for upload and deliver template files | Thabor Walbeek | Deployed |

## Older Changes

| Date | Change | Changed By | Status |
| -- | -- | -- | --|
| 04/24/2025 | Change data type of campaign_id in table dim_savedqueries from int(4) to varchar. Change requested by Tejas, as Campaign ID's within Finalfunnel can have multiple characters | Thabor Walbeek | Change made on database |
| 04/25/2025 | Change the logic for passing on the saved query info to the next page, so it will be possible to re-save the query with any filters applied | Thabor Walbeek | Implemented |
| 04/25/2025 | Remove container for saved query and only show the button Save Query | Thabor Walbeek | Implemented |
| 04/25/2025 | When loading a query it doesn't pick up the filter state of the saved query (column filters)  | Thabor Walbeek | Implemented |
| 04/25/2025 | Using the reset filters doesn't reset the filters | Thabor Walbeek | Implemented |