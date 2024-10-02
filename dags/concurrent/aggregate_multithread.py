import threading
from queue import Queue

from aggregate_dag import step_task
from consts import INITIAL_FILENAME, SAVE_PATH
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


def run_in_thread(func, *args):
    """Запускает функцию в отдельном потоке и возвращает поток."""
    q = Queue()

    def wrapper():
        result = func(*args)
        q.put(result)

    thread = threading.Thread(target=wrapper)
    thread.start()
    return thread, q


def clean_data():
    return clean(
        filepath=f"{SAVE_PATH}{INITIAL_FILENAME}.csv",
        new_path=f"{SAVE_PATH}{INITIAL_FILENAME}_cleaned.csv",
    )


def analyze_data(path):
    df = read(path)
    clean_data()

    age_thread, res = run_in_thread(analyze_age, df)
    age_thread.join()
    # age_res = res.get()

    age_ranges_thread, res = run_in_thread(create_age_ranges, df)
    age_ranges_thread.join()
    age_ranges_res = res.get()

    income_thread, res = run_in_thread(analyze_income, df)
    income_thread.join()
    # income_res = res.get()

    occupation_thread, res = run_in_thread(analyze_occupation, df)
    occupation_thread.join()
    occupation_res = res.get()

    investment_thread, res = run_in_thread(
        analyze_investment_by_month,
        df,
        f"{SAVE_PATH}analyze_investment_task.csv",
    )
    investment_thread.join()
    investment_res = res.get()

    step_thread, res = run_in_thread(step_task, age_ranges_res, occupation_res)
    step_thread.join()
    step_path_result = res.get()

    task_4a_thread, res = run_in_thread(
        get_occupation_age_group_summary, step_path_result
    )
    task_4a_thread.join()
    # task_4a_result = res.get()

    task_4b_thread, res = run_in_thread(
        get_age_income_summary, step_path_result, f"{SAVE_PATH}task_4b.csv"
    )
    task_4b_thread.join()
    # task_4b_result = res.get()

    task_5a_thread, res = run_in_thread(occupation_ration, step_path_result)
    task_5a_thread.join()
    # task_5a_result = res.get()

    task_5b_thread, res = run_in_thread(
        get_age_occupation_summary, step_path_result, f"{SAVE_PATH}task_5b.csv"
    )
    task_5b_thread.join()
    # task_5b_result = res.get()

    archive_files(
        [
            step_path_result,
            f"{SAVE_PATH}task_4b.csv",
            f"{SAVE_PATH}task_5b.csv",
            investment_res,
        ],
        f"{SAVE_PATH}results.zip",
    )


if __name__ == "__main__":
    analyze_data(f"{SAVE_PATH}{INITIAL_FILENAME}.csv")
    print(f"Завершено. Результаты сохранены в {SAVE_PATH}.")
