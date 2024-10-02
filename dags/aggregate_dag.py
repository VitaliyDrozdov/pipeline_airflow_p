import datetime as dt
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
from funcs.utils import archive_files, read
from consts import SAVE_PATH, INITIAL_FILENAME


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
        """
        Задача по очистке данных.

        Возвращает:
        str: Путь к очищенному файлу.
        """
        return clean(
            filepath=f"{SAVE_PATH}{INITIAL_FILENAME}.csv",
            new_path=f"{SAVE_PATH}{INITIAL_FILENAME}_cleaned.csv",
        )

    @task
    def analyze_age_task(path):
        """
        Задача по анализу возраста.

        Параметры:
        path (str): Путь к очищенному файлу.

        Возвращает:
        tuple: Минимальный, максимальный и средний возраст.
        """
        df = read(path)
        return analyze_age(df)

    @task
    def create_age_ranges_task(path):
        """
        Задача по созданию возрастных групп.

        Параметры:
        path (str): Путь к очищенному файлу.

        Возвращает:
        pd.DataFrame: DataFrame с количеством клиентов по возрастным группам.
        """
        df = read(path)
        return create_age_ranges(df)

    @task
    def analyze_investment_task(path):
        """
        Задача по анализу инвестиций.

        Параметры:
        path (str): Путь к очищенному файлу.

        Возвращает:
        dict: Средние инвестиции по месяцам.
        """
        df = read(path)
        return analyze_investment_by_month(
            df,
            f"{SAVE_PATH}analyze_investment_task.csv",
        )

    @task
    def analyze_income_task(path):
        """
        Задача по анализу доходов.

        Параметры:
        path (str): Путь к очищенному файлу.

        Возвращает:
        tuple: Мин и макс доход, словарь средних доходов по возрасту.
        """

        df = read(path)
        return analyze_income(df)

    @task
    def analyze_occupation_task(path):
        """
        Задача по анализу профессий.

        Параметры:
        path (str): Путь к очищенному файлу.

        Возвращает:
        tuple: Словарь с количеством клиентов по профессиям и
               словарь со средними доходами по профессиям.
        """
        df = read(path)
        return analyze_occupation(df)

    @task
    def step_task(age_ranges_df, occupation_data):
        """
        Задача по объединению данных возрастных групп и профессий.

        Параметры:
        age_ranges_df (pd.DataFrame): DataFrame с возрастными группами.
        occupation_data (tuple): Кортеж с данными о профессиях и доходах.

        Возвращает:
        str: Путь к результирующему CSV файлу.
        """
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
        """
        Задача для анализа распределения профессий по возрастным группам.

        Параметры:
        path (str): Путь к CSV файлу.

        Возвращает:
        pd.DataFrame: Сводная таблица с количеством клиентов по профессиям.
        """
        df = read(path)
        return get_occupation_age_group_summary(df)

    @task
    def task_4b(path):
        """
        Задача для анализа доходов по возрастным группам.

        Параметры:
        path (str): Путь к CSV файлу.

        Возвращает:
        pd.DataFrame: Сводная таблица со средним доходом по возрастным группам.
        """
        df = read(path)
        return get_age_income_summary(df, f"{SAVE_PATH}task_4b.csv")

    @task
    def task_5a(path):
        """
        Задача по анализу распределения профессий.

        Параметры:
        path (str): Путь к CSV файлу.

        Возвращает:
        dict: Процентное распределение профессий.
        """
        df = read(path)
        return occupation_ration(df)

    @task
    def task_5b(path):
        """
        Задача для анализа наиболее распространенной профессии по группам.

        Параметры:
        path (str): Путь к CSV файлу.

        Возвращает:
        pd.DataFrame: Сводная таблица.
        """
        df = read(path)
        return get_age_occupation_summary(df, f"{SAVE_PATH}task_5b.csv")

    @task
    def end_task(*filepaths):
        """

        Архивирует указанные файлы в ZIP архив.

        Параметры:
        *filepaths: Путь к файлам, которые нужно архивировать.
        """
        archive_path = f"{SAVE_PATH}results.zip"
        archive_files(filepaths, archive_path)

    # Основной поток выполнения
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
        t_4b,
        t_5b,
        investment,
    )

    # Определение зависимостей
    path >> [age, income, investment]
    age >> age_ranges
    income >> occupation
    [age_ranges, occupation] >> step_path
    step_path >> [t_4a, t_5a]
    t_4a >> t_4b
    t_5a >> t_5b
    [t_4b, t_5b, investment] >> end
