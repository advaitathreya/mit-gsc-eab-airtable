#!/bin/bash

mkdir -p data/Census/states/
mkdir -p data/Congress/
mkdir -p data/NCES/

eval "$(conda shell.bash hook)"
conda activate geo

congress_session=118

echo "Downloading all data"
# echo "    NCES data"
# python download_nces.py
echo "    Congress data"
python download_congress.py $congress_session
# echo "    Census data"
# python download_census.py
# echo "All data downloaded"
# echo
# echo "Generating universities table"
# python make_udf.py
echo
echo "Uploading to Airtable"
python upload_to_airtable.py $congress_session
echo "All done"
