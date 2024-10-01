import pandas as pd


def clean_age(df):
    df["Age"] = pd.to_numeric(df["Age"], errors="coerce")
    df.loc[(df["Age"] < 18) | (df["Age"] > 90), "Age"] = pd.NA
    return df


def clean():
    df = pd.read_csv(
        filepath_or_buffer="./data/test.csv",
        header=0,
        delimiter=",",
        encoding="utf-8",
    )
    # df["Age"] = pd.to_numeric(df["Age"], errors="coerce")
    # df.loc[(df["Age"] < 18) | (df["Age"] > 90), "Age"] = pd.NA
    clean_age(df)
    temp_filepath = "/tmp/test_data.csv"
    df.to_csv(temp_filepath, index=False)
    return temp_filepath


def analyze_age():
    pass


def create_age_ranges():
    pass


def analyze_income():
    pass


def analyze_occupation():
    pass


def step():
    pass


def credit_rating_by_age_range():
    pass


def occupation_by_age_range():
    pass


def expenses_by_age_range():
    pass


def analyze_investment():
    pass


def analyze_months():
    pass


def investment_by_month():
    pass
