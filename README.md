# Airflow Open Weather API

![Dags Visualization](./docs/dag.png "Dags Visualization")

## Prerequisites

1. install [UV](https://docs.astral.sh/uv/getting-started/installation/).
2. install [Docker](https://docs.docker.com/get-docker/).
3. create the file `.env` and fill in the Open Weather Map API key in `OPEN_WEATHER_API_KEY`.

## Setup

Run the `airflow-setup` target in the Makefile to set up the environment:

```bash
make airflow-setup
```

## Development

After setting up the environment, you can start the Airflow services using Docker Compose:

```bash
docker-compose up -d
```

Clean up data, logs and models directories:

```bash
make cleanup
```