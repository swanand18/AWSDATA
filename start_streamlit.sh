#!/bin/bash
cd /home/ubuntu/aws-ff-data
source aws-ff-data-env/bin/activate
streamlit run 0_Home.py --server.port 8501
