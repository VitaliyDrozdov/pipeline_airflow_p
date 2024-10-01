import datetime as dt

from airflow import DAG

# from airflow.decorators import task
# from funcs.aggregate import (
#     analyze_age,
#     analyze_income,
#     analyze_investment,
#     analyze_months,
#     analyze_occupation,
#     credit_rating_by_age_range,
#     expenses_by_age_range,
#     investment_by_month,
#     load,
#     occupation_by_age_range,
#     step,
# )

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
) as dag:
    pass
