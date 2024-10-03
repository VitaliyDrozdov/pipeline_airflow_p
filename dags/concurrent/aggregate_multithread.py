import threading
import sys
import os


from queue import Queue

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(project_root)

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

    age_thread, _ = run_in_thread(analyze_age, df)
    age_ranges_thread, q_1 = run_in_thread(create_age_ranges, df)
    income_thread, _ = run_in_thread(analyze_income, df)
    investment_thread, q_2 = run_in_thread(
        analyze_investment_by_month,
        df,
        f"{SAVE_ABS_PATH}\\analyze_investment_task.csv",
    )
    occupation_thread, q_3 = run_in_thread(analyze_occupation, df)

    threads = [
        age_thread,
        age_ranges_thread,
        income_thread,
        investment_thread,
        occupation_thread,
    ]

    for thread in threads:
        thread.join()

    age_ranges_res = q_1.get()
    investment_res = q_2.get()
    occupation_res = q_3.get()

    step_thread, step_q = run_in_thread(
        step_task, age_ranges_res, occupation_res
    )
    step_thread.join()
    step_path_result = step_q.get()

    df_2 = read(step_path_result)

    task_4a_thread, _ = run_in_thread(get_occupation_age_group_summary, df_2)
    task_4b_thread, task_4b_q = run_in_thread(
        get_age_income_summary,
        df_2,
        f"{SAVE_ABS_PATH}\\task_4b.csv",
    )
    task_5a_thread, _ = run_in_thread(occupation_ration, df_2)
    task_5b_thread, tas_5b_q = run_in_thread(
        get_age_occupation_summary,
        df_2,
        f"{SAVE_ABS_PATH}\\task_5b.csv",
    )

    task_4a_thread.join()
    task_4b_thread.join()
    task_5a_thread.join()
    task_5b_thread.join()

    task_threads = [
        task_4a_thread,
        task_4b_thread,
        task_5a_thread,
        task_5b_thread,
    ]
    for thread in task_threads:
        thread.join()

    t_4b = task_4b_q.get()
    t_5b = tas_5b_q.get()

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
