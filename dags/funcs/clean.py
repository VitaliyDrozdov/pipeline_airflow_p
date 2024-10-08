import pandas as pd


def to_numeric(df, col_name):
    """
    Преобразует указанный столбец в числовой формат.

    Параметры:
    df (pd.DataFrame): DataFrame.
    col_name (str): Имя столбца.
    """
    df[col_name] = pd.to_numeric(df[col_name], errors="coerce")


def clean(filepath, new_path):
    """
    Очищает данные из CSV файла и сохраняет их в новый CSV файл.

    Параметры:
    filepath (str): Путь к исходному файлу.
    new_path (str): Путь для очищенного файла.

    Возвращает:
    str: Путь к сохраненному очищенному файлу.
    """

    df = pd.read_csv(
        filepath_or_buffer=f"{filepath}",
        header=0,
        delimiter=",",
        encoding="utf-8",
    )
    numeric_cols = [
        "Amount_invested_monthly",
        "Monthly_Inhand_Salary",
        "Age",
        "Annual_Income",
    ]

    for col in numeric_cols:
        to_numeric(df, col)

    df["Age"] = df["Age"].round()
    df.loc[(df["Age"] < 18) | (df["Age"] > 90), "Age"] = pd.NA

    df["Occupation"] = df["Occupation"].apply(
        lambda x: (
            pd.NA if isinstance(x, str) and x.strip("_ ").strip() == "" else x
        )
    )

    correct_ssn_format = r"^\d{3}-\d{2}-\d{4}$"
    df["SSN"] = df["SSN"].where(df["SSN"].str.match(correct_ssn_format), pd.NA)
    df.to_csv(new_path, index=False)
    return new_path
