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
python3 fetch_data.py BSE
python3 fetch_data.py CAMS
python3 fetch_data.py COFORGE
python3 fetch_data.py DIVISLAB
python3 fetch_data.py FRACTAL
python3 fetch_data.py GROWW
python3 fetch_data.py IGIL
python3 fetch_data.py KFINTECH
python3 fetch_data.py KPITTECH
python3 fetch_data.py MOTILALOFS
python3 fetch_data.py NETWEB
python3 fetch_data.py PGEL
python3 fetch_data.py RELIANCE
python3 fetch_data.py SETL
python3 fetch_data.py TATAPOWER
python3 fetch_data.py TATATECH

echo ""
echo "Starting local server..."
lsof -ti :8080 | xargs kill -9 2>/dev/null   # clear port if in use
python3 -m http.server 8080 &

sleep 1
open http://localhost:8080

echo "Dashboard running at http://localhost:8080"
echo "Close this window to stop the server."
wait
