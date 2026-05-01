#!/bin/bash
# Ensure data directory and DB exist
mkdir -p /app/data
if [ ! -f /app/data/mvr_bulletins.db ] && [ -f ./mvr_bulletins.db ]; then
    cp ./mvr_bulletins.db /app/data/mvr_bulletins.db
    echo "Database copied to persistent volume"
fi
# Start streamlit
streamlit run app.py --server.port $PORT --server.headless true
