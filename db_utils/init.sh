#!/bin/sh
set -e

echo "Running table creation script..."
python create_analysis_tables.py

echo "Running historical data population script..."
python historical_data_generator/populate_rds.py

echo "Database initialization complete." 