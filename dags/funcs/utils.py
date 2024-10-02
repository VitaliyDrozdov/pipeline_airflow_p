import logging
import os
import zipfile

import pandas as pd

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
