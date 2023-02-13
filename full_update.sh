#!/bin/bash

mkdir -p data/Census/states/
mkdir -p data/Congress/
mkdir -p data/NCES/

eval "$(conda shell.bash hook)"
conda activate geo

echo "Downloading all data"
echo "    NCES data"
python download_nces.py
echo "    Congress data"
python download_congress.py
echo "    Census data"
python download_census.py
echo "All data downloaded"
echo
echo "Generating universities table"
python make_udf.py
echo
echo "Uploading to Airtable"
python upload_to_airtable.py
echo "All done"
