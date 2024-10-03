import logging
import multiprocessing
import os
import sys
import time

# Создание абсолютного пути для импорта:
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("aggregate_multiprocess.log", mode="w"),
        logging.StreamHandler(),
    ],
)
SAVE_ABS_PATH = os.path.join(project_root, "data")


def process_func(func, args, queue):
    """Функция-обертка для запуска в отдельном процессе."""
    result = func(*args)
    queue.put(result)


def run_in_process(func, *args, queue=None):
    """Запускает функцию в отдельном процессе ."""
    process_queue = multiprocessing.Queue() if queue is None else queue
    process = multiprocessing.Process(
        target=process_func, args=(func, args, process_queue)
    )
    process.start()
    return process


FILEPATH = f"{SAVE_ABS_PATH}\\{INITIAL_FILENAME}.csv"
NEW_PATH = f"{SAVE_ABS_PATH}\\{INITIAL_FILENAME}_cleaned.csv"


def clean_data(path, newpath):
    """Очищает данные.

    Args:
        path (str): Путь к исходному файлу.
        newpath (str): Путь для сохранения очищенного файла.

    Returns:
        DataFrame: Очищенный датафрейм.
    """

    return clean(
        filepath=path,
        new_path=newpath,
    )


def analyze_data():
    """Анализирует данные с использованием процессов."""
    start_time = time.time()
    logging.info("Начало скрипта...")
    logging.info("Начало очистки данных...")
    clean_data(FILEPATH, NEW_PATH)
    logging.info("Создание df...")
    df = read(NEW_PATH)

    logging.info("Запуск процессов...")
    age_queue = multiprocessing.Queue()
    age_process = run_in_process(analyze_age, df, queue=age_queue)
    age_process.join()
    # age_result = age_queue.get()

    age_ranges_queue = multiprocessing.Queue()
    age_ranges_process = run_in_process(
        create_age_ranges, df, queue=age_ranges_queue
    )
    age_ranges_process.join()
    age_ranges_result = age_ranges_queue.get()

    income_queue = multiprocessing.Queue()
    income_process = run_in_process(analyze_income, df, queue=income_queue)
    income_process.join()
    # income_result = income_queue.get()

    occupation_queue = multiprocessing.Queue()
    occupation_process = run_in_process(
        analyze_occupation, df, queue=occupation_queue
    )
    occupation_process.join()
    occupation_result = occupation_queue.get()

    investment_queue = multiprocessing.Queue()
    investment_process = run_in_process(
        analyze_investment_by_month,
        df,
        f"{SAVE_ABS_PATH}\\analyze_investment_task.csv",
        queue=investment_queue,
    )
    investment_process.join()
    investment_result = investment_queue.get()

    logging.info("Запуск step...")
    step_path_queue = multiprocessing.Queue()
    step_process = run_in_process(
        step_task, age_ranges_result, occupation_result, queue=step_path_queue
    )
    step_process.join()
    step_path = step_path_queue.get()

    logging.info("Создание df_2...")
    df_2 = read(step_path)

    # Запуск потоков
    logging.info("Запуск второй части потоков...")
    # Анализ распределения профессий по возрастным группам
    task_4a_queue = multiprocessing.Queue()
    task_4a_process = run_in_process(
        get_occupation_age_group_summary, df_2, queue=task_4a_queue
    )
    task_4a_process.join()
    # task_4a_result = task_4a_queue.get()

    task_4b_queue = multiprocessing.Queue()
    task_4b_process = run_in_process(
        get_age_income_summary,
        df_2,
        f"{SAVE_ABS_PATH}\\task_4b.csv",
        queue=task_4b_queue,
    )
    task_4b_process.join()
    # task_4b_result = task_4b_queue.get()

    task_5a_queue = multiprocessing.Queue()
    task_5a_process = run_in_process(
        occupation_ration, df_2, queue=task_5a_queue
    )
    task_5a_process.join()
    # task_5a_result = task_5a_queue.get()

    task_5b_queue = multiprocessing.Queue()
    task_5b_process = run_in_process(
        get_age_occupation_summary,
        df_2,
        f"{SAVE_ABS_PATH}\\task_5b.csv",
        queue=task_5b_queue,
    )
    task_5b_process.join()
    # task_5b_result = task_5b_queue.get()

    # Архивируем результаты
    end_queue = multiprocessing.Queue()
    logging.info("Архивируем результаты...")
    end_process = run_in_process(
        archive_files,
        [
            step_path,
            f"{SAVE_ABS_PATH}\\task_4b.csv",
            f"{SAVE_ABS_PATH}\\task_5b.csv",
            investment_result,
        ],
        f"{SAVE_ABS_PATH}\\results.zip",
        queue=end_queue,
    )
    end_process.join()
    end_time = time.time()
    elapsed_time = end_time - start_time
    logging.info(f" Общее время выполнения: {elapsed_time:.2f} секунд.")


if __name__ == "__main__":
    analyze_data()

    print(f"Завершено. Результаты сохранены в {SAVE_ABS_PATH}.")
