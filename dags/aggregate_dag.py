import datetime as dt
import logging
import zipfile
import os
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

SAVE_PATH = "dags/data/"
logger = logging.getLogger("airflow.task")


def archive_files(files, archive_name):
    """Архивирует файлы в ZIP."""
    with zipfile.ZipFile(archive_name, "w", zipfile.ZIP_DEFLATED) as archive:
        for file in files:
            if os.path.exists(file):
                archive.write(file, os.path.basename(file))
                logger.info(f"Файл {file} добавлен в архив.")
            else:
                logger.warning(f"Файл {file} не найден.")


def read(path):
    return pd.read_csv(
        filepath_or_buffer=path, header=0, delimiter=",", encoding="utf-8"
    )


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
            new_path=f"{SAVE_PATH}test_cleaned.csv",
        )

    @task
    def analyze_age_task(path):
        df = read(path)
        return analyze_age(df)

    @task
    def create_age_ranges_task(path):
        df = read(path)
        return create_age_ranges(df)

    @task
    def analyze_investment_task(path):
        df = read(path)
        return analyze_investment_by_month(
            df,
            f"{SAVE_PATH}analyze_investment_task.csv",
        )

    @task
    def analyze_income_task(path):
        df = read(path)
        return analyze_income(df)

    @task
    def analyze_occupation_task(path):
        df = read(path)
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
        )  # cross join для получения всех комбинаций
        path = f"{SAVE_PATH}step.csv"
        combined_df.to_csv(path, index=False)

        return path

    @task
    def task_4a(path):
        df = read(path)
        return get_occupation_age_group_summary(df)

    @task
    def task_4b(path):
        df = read(path)
        return get_age_income_summary(df, f"{SAVE_PATH}task_4b.csv")

    @task
    def task_5a(path):
        df = read(path)
        return occupation_ration(df)

    @task
    def task_5b(path):
        df = read(path)
        return get_age_occupation_summary(df, f"{SAVE_PATH}task_5b.csv")

    @task
    def end_task(*filepaths):
        archive_path = f"{SAVE_PATH}results.zip"
        archive_files(filepaths, archive_path)

    path = clean_data_task()

    age = analyze_age_task(path)
    age_ranges = create_age_ranges_task(path)
    investment = analyze_investment_task(path)
    income = analyze_income_task(path)
    occupation = analyze_occupation_task(path)
    step_path = step_task(age_ranges, occupation)
    t_4a = task_4a(step_path)
    t_4b = task_4b(step_path)
    t_5a = task_5a(step_path)
    t_5b = task_5b(step_path)
    end = end_task(
        step_path,
        f"{SAVE_PATH}task_4b.csv",
        f"{SAVE_PATH}task_5b.csv",
        f"{SAVE_PATH}analyze_investment_task.csv",
    )

    path >> [age, income, investment]
    age >> age_ranges
    income >> occupation
    [age_ranges, occupation] >> step_path
    step_path >> [t_4a, t_5a]
    t_4a >> t_4b
    t_5a >> t_5b
    [t_4b, t_5b, investment] >> end
