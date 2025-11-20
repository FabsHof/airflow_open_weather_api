# !/bin/bash

# Initialize the UV environment
uv sync
uv pip freeze > requirements.txt

# Create necessary environment variables and directories for Airflow
export AIRFLOW_HOME=~/airflow
# Create necessary directories and .env file (otherwise there will be errors on permissions)
mkdir -p ./dags ./logs ./plugins ./config ./data/raw_files ./data/clean_data ./models
# Append AIRFLOW_UID to .env file if not already present
if [[ ! -f .env ]] || ! grep -q AIRFLOW_UID .env; then
    echo -e "AIRFLOW_UID=$(id -u)" >> .env
fi

# Build the Docker image and initialize the Airflow database
docker compose build
docker compose up airflow-init

echo -e "\n\n\n===\n🎉 Airflow setup is complete. You can now start Airflow with 'docker compose up -d'.\n==="