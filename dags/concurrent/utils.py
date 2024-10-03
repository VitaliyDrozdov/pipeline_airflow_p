import os
import sys

import pandas as pd

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(project_root)
SAVE_ABS_PATH = os.path.join(project_root, "data")


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
    path = f"{SAVE_ABS_PATH}\\step.csv"
    combined_df.to_csv(path, index=False)

    return path
