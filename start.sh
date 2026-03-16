#!/bin/bash
set -e
exec gunicorn --bind "0.0.0.0:${PORT:-8080}" --workers 1 --timeout 300 --log-level info app:app
