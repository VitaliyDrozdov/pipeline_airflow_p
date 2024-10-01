import pandas as pd


def clean_age(df):
    df["Age"] = pd.to_numeric(df["Age"], errors="coerce")
    df.loc[(df["Age"] < 18) | (df["Age"] > 90), "Age"] = pd.NA
    return df


def clean_ssn(df):
    correct_ssn_format = r"^\d{3}-\d{2}-\d{4}$"
    df["SSN"] = df["SSN"].where(df["SSN"].str.match(correct_ssn_format), pd.NA)
    return df


def clean_occupation_column(df):
    df["Occupation"] = df["Occupation"].apply(
        lambda x: (pd.NA if isinstance(x, str) and x.strip("_ ").strip() == "" else x)
    )
    return df


def clean_monthly_inhand_salary(df):
    df["Monthly_Inhand_Salary"] = df["Monthly_Inhand_Salary"].replace("", pd.NA)
    return df


def clean(filepath):
    df = pd.read_csv(
        filepath_or_buffer=f"{filepath}",
        header=0,
        delimiter=",",
        encoding="utf-8",
    )
    clean_age(df)
    clean_ssn(df)
    clean_occupation_column(df)
    clean_monthly_inhand_salary(df)
    return df
