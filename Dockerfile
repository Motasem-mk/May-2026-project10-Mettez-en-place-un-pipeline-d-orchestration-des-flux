FROM python:3.11-slim

RUN pip install --no-cache-dir \
    pandas \
    openpyxl \
    duckdb

WORKDIR /app

COPY scripts/ /app/scripts/