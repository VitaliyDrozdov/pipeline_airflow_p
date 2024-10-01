import datetime as dt
import logging

import pandas as pd

from airflow import DAG
from airflow.decorators import task
from funcs.aggregate import (
    analyze_age,
    analyze_income,
    analyze_investment_by_month,
    analyze_occupation,
    create_age_ranges,
)
from funcs.clean import clean

SAVE_PATH = "dags/data/test_cleaned.csv"
logger = logging.getLogger("airflow.task")

default_args = {
    "owner": "airflow",
    "start_date": dt.datetime(2024, 9, 20),
    "retries": 3,
    "retry_delay": dt.timedelta(seconds=30),
}

with DAG(
    dag_id="customer_analysis",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
) as dag:

    @task
    def clean_data_task():
        return clean(
            filepath="dags/data/test.csv",
            new_path=SAVE_PATH,
        )

    @task
    def analyze_age_task(path):
        df = pd.read_csv(
            filepath_or_buffer=path,
            header=0,
            delimiter=",",
            encoding="utf-8",
        )
        return analyze_age(df)

    @task
    def create_age_ranges_task(path):
        df = pd.read_csv(
            filepath_or_buffer=path,
            header=0,
            delimiter=",",
            encoding="utf-8",
        )
        return create_age_ranges(df)

    @task
    def analyze_investment_task(path):
        df = pd.read_csv(
            filepath_or_buffer=path,
            header=0,
            delimiter=",",
            encoding="utf-8",
        )
        return analyze_investment_by_month(df)

    @task
    def analyze_income_task(path):
        df = pd.read_csv(
            filepath_or_buffer=path,
            header=0,
            delimiter=",",
            encoding="utf-8",
        )
        return analyze_income(df)

    @task
    def analyze_occupation_task(path):
        df = pd.read_csv(
            filepath_or_buffer=path,
            header=0,
            delimiter=",",
            encoding="utf-8",
        )
        return analyze_occupation(df)

    @task
    def step_task(age_ranges_df, occupation_data):
        combined_results = {
            "age_ranges": age_ranges_df,
            "occupation_counts": occupation_data[0],
            "average_income_by_occupation": occupation_data[1],
        }

        return combined_results

    @task
    def task_4a():
        pass

    @task
    def task_5a():
        pass

    @task
    def task_4b():
        pass

    @task
    def task_5b():
        pass

    @task
    def end_task():
        pass

    path = clean_data_task()

    age = analyze_age_task(path)
    age_ranges = create_age_ranges_task(path)
    investment = analyze_investment_task(path)
    income = analyze_income_task(path)
    occupation = analyze_occupation_task(path)
    step = step_task(age_ranges, occupation)
    t_4a = task_4a()
    t_4b = task_4b()
    t_5a = task_5a()
    t_5b = task_5b()
    end = end_task()

    path >> [age, income, investment]
    age >> age_ranges
    income >> occupation
    [age_ranges, occupation] >> step
    step >> [t_4a, t_5a]
    t_4a >> t_4b
    t_5a >> t_5b
    [t_4b, t_5b, investment] >> end
