#!/bin/sh
set -e

echo "Checking raw source files..."

test -f /app/data/raw/erp.xlsx
test -f /app/data/raw/web.xlsx
test -f /app/data/raw/liaison.xlsx

echo "All raw files exist."

cp /app/data/raw/erp.xlsx erp.xlsx
cp /app/data/raw/web.xlsx web.xlsx
cp /app/data/raw/liaison.xlsx liaison.xlsx

echo "Files loaded into Kestra working directory:"
ls -lah erp.xlsx web.xlsx liaison.xlsx