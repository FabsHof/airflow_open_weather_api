
import os
from glob import glob
import requests
import json
import joblib
import pandas as pd
from datetime import datetime, timedelta
from airflow.sdk import dag, task, setup, task_group, teardown, Variable
from airflow.sdk.bases.operator import chain
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.tree import DecisionTreeRegressor

def fetch_raw_data(city_name: str, api_key: str) -> dict:
    '''Fetch raw weather data from OpenWeather API for a given city.'''

    base_url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city_name,
        "appid": api_key,
        "units": "metric"
    }
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    return response.json()

def transform_data(raw_data: dict, date: str) -> dict:
    '''Transform raw weather data into a structured format.'''
    print(f'>>> Transforming data for city: {raw_data["name"]} on date: {date}\n{raw_data}')
    transformed = {
        'temperature': raw_data['main']['temp'],
        'city': raw_data['name'],
        'pressure': raw_data['main']['pressure'],
        'date': date
    }
    return transformed

def combine_and_transform(chunk_size: int = None) -> list[dict]:
    '''Combines all or the last n JSON files from the raw_files directory. Returns a list of transformed dictionaries.'''
    file_paths = sorted(
        glob('/app/raw_files/*.json'),
        reverse=True
    )
    if chunk_size:
        file_paths = file_paths[:chunk_size]
    print(f'>>> Combining and transforming {len(file_paths)} files.')
    all_data = []
    for file_path in file_paths:
        with open(file_path, 'r') as f:
            date = os.path.basename(file_path).replace('.json', '')
            raw_data = json.load(f)
            all_data.extend([transform_data(raw_entry, date) for raw_entry in raw_data])
    return all_data

def compute_model_score(model, X: pd.DataFrame, y: pd.Series) -> float:
    '''Compute and return the cross validation score for a given model.'''
    cross_validation = cross_val_score(model, X, y, cv=3, scoring='neg_mean_squared_error')
    print(f'>>> [{str(model)}] Cross validation scores: {cross_validation}')
    return cross_validation.mean()

def train_and_save_model(model, X: pd.DataFrame, y: pd.Series, model_path: str) -> str:
    '''Train the model and save it to the specified path. Returns the model path.'''
    model.fit(X, y)
    print(f'>>> [{str(model)}] Trained and saved to {model_path}.')
    joblib.dump(model, model_path)
    return model_path

def prepare_data_for_training(path: str) -> tuple[pd.DataFrame, pd.Series]:
    '''Prepare features and target variable from the cleaned data CSV file.'''
    df = pd.read_csv(path)
    # Sort and clean the data
    df.sort_values('date', inplace=True)
    print(f'>>> Preparing data from {path} for training. \n- total records: {len(df)}\n- columns: {df.columns.tolist()}\n- missing values (temperature):\n{df['temperature'].isnull().sum()}')
    df.dropna(subset=['temperature'], inplace=True)
    # One-hot encode the 'city' column
    cities_dummies = pd.get_dummies(df['city'], prefix='city')
    # Define target and features
    y = df['temperature']
    X = df.drop(columns=['temperature', 'city', 'date'])
    X = pd.concat([X, cities_dummies], axis=1)
    print(f'>>> Prepared features and target variable:\n- features columns: {X.columns.tolist()}\n- features shape: {X.shape}\n- target shape: {y.shape}')
    return X, y

@dag(
    dag_id="open_weather_dag",
    schedule=timedelta(minutes=1),
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    tags=['open_weather'],
)
def open_weather_dag():

    @setup
    def initialize():
        '''Initialization task for the DAG.'''
        print(f'>>> Initializing DAG run.')
        Variable.set('cities', json.dumps(['Paris', 'Madrid', 'Rome', 'Berlin', 'Lisbon']))
        Variable.set('OPEN_WEATHER_API_KEY', os.getenv('OPEN_WEATHER_API_KEY'))

    @task()
    def fetch_and_store_weather_data():
        '''Fetch weather data for a list of cities and store it in JSON files.'''
        api_key = Variable.get("OPEN_WEATHER_API_KEY")
        print(f'>>> Fetching weather data for cities: {json.loads(Variable.get("cities"))}')
        cities = json.loads(Variable.get('cities'))
        result = []
        for city in cities:
            raw_data = fetch_raw_data(city.lower(), api_key)
            result.append(raw_data)

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
        filename = f"/app/raw_files/{timestamp}.json"
        with open(filename, 'w') as f:
            json.dump(result, f, indent=4)

    @task(trigger_rule='all_done')
    def process_last_data_chunk():
        '''Process the last chunk of raw data files and store the cleaned data as csv.'''
        cleaned_data = combine_and_transform(chunk_size=20)
        df = pd.DataFrame(cleaned_data)
        output_file = '/app/clean_data/data.csv'
        # Remove file if it exists
        if os.path.exists(output_file):
            os.remove(output_file)
        df.to_csv(output_file, index=False)

    @task(trigger_rule='all_done')
    def process_all_data():
        '''Process all raw data files and store the cleaned data as csv.'''
        cleaned_data = combine_and_transform()
        df = pd.DataFrame(cleaned_data)
        output_file = '/app/clean_data/fulldata.csv'
        # Remove file if it exists
        if os.path.exists(output_file):
            os.remove(output_file)
        df.to_csv(output_file, index=False, mode='a', header=True)

    @task_group()
    def train_models():
        '''Train and save different regression models.'''

        @task()
        def _train_model_task(model_name: str) -> str:
            models = {
                'linear_regression': LinearRegression(),
                'decision_tree': DecisionTreeRegressor(),
                'random_forest': RandomForestRegressor(n_estimators=100),
            }
            model = models[model_name]
            X, y = prepare_data_for_training('/app/clean_data/fulldata.csv')
            model_path = f'/app/models/{model_name}.joblib'
            return train_and_save_model(model, X, y, model_path)

        linear_task = _train_model_task('linear_regression')
        decision_task = _train_model_task('decision_tree')
        random_task = _train_model_task('random_forest')
        
        return [linear_task, decision_task, random_task]

    @task(trigger_rule='all_done')
    def select_best_model(model_paths: list[str]):
        '''Select the best model based on cross-validation scores.'''
        X, y = prepare_data_for_training('/app/clean_data/fulldata.csv')
        best_model_name = None
        best_score = float('-inf')
        best_model_path = None

        model_names = ['linear_regression', 'decision_tree', 'random_forest']
        for model_name, model_path in zip(model_names, model_paths):
            model = joblib.load(model_path)
            score = compute_model_score(model, X, y)
            if score > best_score:
                best_score = score
                best_model_name = model_name
                best_model_path = model_path

        print(f'>>> Best model: {best_model_name} with score: {best_score}. Saved at: {best_model_path}.')

    @teardown()
    def finalize():
        '''Finalization task for the DAG.'''
        Variable.delete('cities')
        Variable.delete('OPEN_WEATHER_API_KEY')
        print('>>> DAG run completed.')

    initialize_task = initialize()
    fetch_and_store_weather_data_task = fetch_and_store_weather_data()
    process_last_data_chunk_task = process_last_data_chunk()
    process_all_data_task = process_all_data()
    train_models_task = train_models()
    select_best_model_task = select_best_model(train_models_task)
    finalize_task = finalize()
    
    chain(initialize_task, fetch_and_store_weather_data_task)
    chain(fetch_and_store_weather_data_task, process_last_data_chunk_task, train_models_task)
    chain(fetch_and_store_weather_data_task, process_all_data_task, train_models_task)
    chain(train_models_task, select_best_model_task, finalize_task)

open_weather_dag()