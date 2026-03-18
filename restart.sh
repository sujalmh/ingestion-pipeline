#!/bin/bash

PID=$(sudo lsof -t -i:8073)

if [ ! -z "$PID" ]; then
  echo "Killing process $PID on port 8073"
  kill -9 $PID
fi

echo "Starting server on port 8073..."
source venv/bin/activate
nohup python -m uvicorn app.main:app --host 0.0.0.0 --port 8073 > server_8073.log 2>&1 &
