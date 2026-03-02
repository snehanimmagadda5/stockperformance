#!/bin/bash
cd "$(dirname "$0")"

echo "====================================="
echo "  Indian Stocks Dashboard"
echo "====================================="
echo ""
echo "Fetching latest financial data..."
python3 fetch_data.py LAURUSLABS
python3 fetch_data.py HDFCBANK
python3 fetch_data.py APOLLOHOSP
python3 fetch_data.py BAJFINANCE
python3 fetch_data.py CDSL
python3 fetch_data.py INFY
python3 fetch_data.py TCS
python3 fetch_data.py PERSISTENT

echo ""
echo "Starting local server..."
lsof -ti :8080 | xargs kill -9 2>/dev/null   # clear port if in use
python3 -m http.server 8080 &

sleep 1
open http://localhost:8080

echo "Dashboard running at http://localhost:8080"
echo "Close this window to stop the server."
wait
