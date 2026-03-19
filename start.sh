#!/bin/bash
set -e
exec gunicorn --bind "0.0.0.0:${PORT:-8080}" --workers 1 --timeout 600 --log-level info --limit-request-fields 500 --limit-request-field_size 0 app:app
