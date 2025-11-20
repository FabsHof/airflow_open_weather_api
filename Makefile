airflow-setup:
	# Set up Airflow environment
	bash scripts/airflow-setup.sh
fetch-example-data:
	# Fetch example data 
	bash scripts/fetch-example-data.sh
cleanup:
	# Clean up data, logs and models directories
	bash scripts/cleanup.sh