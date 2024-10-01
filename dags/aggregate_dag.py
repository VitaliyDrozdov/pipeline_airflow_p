import datetime as dt

from airflow import DAG
from airflow.decorators import task
from funcs.aggregate import analyze_age, create_age_ranges
from funcs.clean import clean

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
        return clean("data/test.csv")

    @task
    def analyze_age_task(df):
        return analyze_age(df)

    @task
    def create_age_ranges_task(df):
        return create_age_ranges(df)

    df = clean_data_task()
    age = analyze_age_task(df)
    age_ranges = create_age_ranges_task(df)
    df >> age >> age_ranges

    # @task
    # def analyze_investment_task(df):
    #     return analyze_investment(df)

    # @task
    # def analyze_income_task(df):
    #     return analyze_income(df)
