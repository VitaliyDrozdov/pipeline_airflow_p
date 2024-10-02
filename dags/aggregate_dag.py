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
    get_occupation_age_group_summary,
    get_age_income_summary,
    occupation_ration,
    get_age_occupation_summary,
)
from funcs.clean import clean

SAVE_PATH = "dags/data/test_cleaned"
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
            new_path=f"{SAVE_PATH}.csv",
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
        occupation_counts_df = pd.DataFrame(
            occupation_data[0].items(),
            columns=["Occupation", "Occupation_Count"],
        )

        average_income_df = pd.DataFrame(
            occupation_data[1].items(),
            columns=["Occupation", "Average_Income"],
        )

        occupation_summary_df = pd.merge(
            occupation_counts_df, average_income_df, on="Occupation"
        )

        combined_df = pd.merge(
            age_ranges_df, occupation_summary_df, how="cross"
        )  # Используем cross join для получения всех комбинаций
        path = f"{SAVE_PATH}_stepv3.csv"
        combined_df.to_csv(path, index=False)

        return path

    @task
    def task_4a(path):
        df = pd.read_csv(
            filepath_or_buffer=path,
            header=0,
            delimiter=",",
            encoding="utf-8",
        )
        return get_occupation_age_group_summary(df)

    @task
    def task_4b(path):
        df = pd.read_csv(
            filepath_or_buffer=path,
            header=0,
            delimiter=",",
            encoding="utf-8",
        )
        return get_age_income_summary(df)

    @task
    def task_5a(path):
        df = pd.read_csv(
            filepath_or_buffer=path,
            header=0,
            delimiter=",",
            encoding="utf-8",
        )
        return occupation_ration(df)

    @task
    def task_5b(path):
        df = pd.read_csv(
            filepath_or_buffer=path,
            header=0,
            delimiter=",",
            encoding="utf-8",
        )
        return get_age_occupation_summary(df)

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
    t_4a = task_4a(step)
    t_4b = task_4b(step)
    t_5a = task_5a(step)
    t_5b = task_5b(step)
    end = end_task()

    path >> [age, income, investment]
    age >> age_ranges
    income >> occupation
    [age_ranges, occupation] >> step
    step >> [t_4a, t_5a]
    t_4a >> t_4b
    t_5a >> t_5b
    [t_4b, t_5b, investment] >> end
