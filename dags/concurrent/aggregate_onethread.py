import os
import sys
import threading

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(project_root)

from queue import Queue

from consts import INITIAL_FILENAME
from funcs.aggregate import (
    analyze_age,
    analyze_income,
    analyze_investment_by_month,
    analyze_occupation,
    create_age_ranges,
    get_age_income_summary,
    get_age_occupation_summary,
    get_occupation_age_group_summary,
    occupation_ration,
)
from funcs.clean import clean
from funcs.utils import archive_files, read
from utils import step_task

SAVE_ABS_PATH = os.path.join(project_root, "data")


def run_in_thread(func, *args):
    """Запускает функцию в отдельном потоке и возвращает поток."""
    q = Queue()

    def wrapper():
        result = func(*args)
        q.put(result)

    thread = threading.Thread(target=wrapper)
    thread.start()
    return thread, q


FILEPATH = f"{SAVE_ABS_PATH}\\{INITIAL_FILENAME}.csv"
NEW_PATH = f"{SAVE_ABS_PATH}\\{INITIAL_FILENAME}_cleaned.csv"


def clean_data(path, newpath):
    return clean(
        filepath=path,
        new_path=newpath,
    )


def analyze_data():
    clean_data(FILEPATH, NEW_PATH)
    df = read(NEW_PATH)
    analyze_age(df)
    age_ranges_res = create_age_ranges(df)
    analyze_income(df)
    occupation_res = analyze_occupation(df)
    investment_res = analyze_investment_by_month(
        df, f"{SAVE_ABS_PATH}\\analyze_investment_task.csv"
    )
    step_path_result = step_task(age_ranges_res, occupation_res)
    df_2 = read(step_path_result)
    get_occupation_age_group_summary(df_2)
    t_4b = get_age_income_summary(df_2, f"{SAVE_ABS_PATH}\\task_4b.csv")
    occupation_ration(df_2)
    t_5b = get_age_occupation_summary(df_2, f"{SAVE_ABS_PATH}\\task_5b.csv")

    archive_files(
        [
            step_path_result,
            t_4b,
            t_5b,
            investment_res,
        ],
        f"{SAVE_ABS_PATH}\\results.zip",
    )


if __name__ == "__main__":
    analyze_data()
    print(f"Завершено. Результаты сохранены в {SAVE_ABS_PATH}.")
