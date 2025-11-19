# !/bin/bash

mkdir -p data/clean_data data/raw_files
curl -L https://dst-de.s3.eu-west-3.amazonaws.com/airflow_avance_fr/eval/data.csv -o data/clean_data/data.csv
echo '[]' >> data/raw_files/null_file.json