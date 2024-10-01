import datetime as dt
import logging

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
        return clean("dags/data/test.csv")

    @task
    def analyze_age_task(df):
        return analyze_age(df)

    @task
    def create_age_ranges_task(df):
        return create_age_ranges(df)

    @task
    def analyze_investment_task(df):
        return analyze_investment_by_month(df)

    @task
    def analyze_income_task(df):
        return analyze_income(df)

    @task
    def analyze_occupation_task(df):
        return analyze_occupation(df)

    @task
    def step_task(age_ranges_df, occupation_data):
        combined_results = {
            "age_ranges": age_ranges_df,
            "occupation_counts": occupation_data[0],
            "average_income_by_occupation": occupation_data[1],
        }
        age_occupation_summary = (
            age_ranges_df.groupby("Age_Group")
            .agg(
                {
                    "Occupation": lambda x: x.value_counts().index[
                        0
                    ],  # Наиболее распространенная профессия
                    "Annual_Income": "mean",
                }
            )
            .reset_index()
        )

        combined_results["age_occupation_summary"] = age_occupation_summary

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

    df = clean_data_task()
    age = analyze_age_task(df)
    age_ranges = create_age_ranges_task(df)
    investment = analyze_investment_task(df)
    income = analyze_income(df)
    occupation = analyze_occupation_task(df)
    step = step_task(age_ranges, occupation)
    t_4a = task_4a()
    t_4b = task_4b()
    t_5a = task_5a()
    t_5b = task_5b()
    end = end_task()

    df >> [age, income, investment]
    age >> age_ranges
    income >> occupation
    [age_ranges, occupation] >> step
    step >> [t_4a, t_5a]
    t_4a >> t_4b
    t_5a >> t_5b
    [t_4b, t_5b, investment] >> end
