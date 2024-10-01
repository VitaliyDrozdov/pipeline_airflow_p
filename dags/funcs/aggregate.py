import pandas as pd


def analyze_age(df):
    min_age = round(df["Age"].min())
    max_age = round(df["Age"].max())
    mean_age = round(df["Age"].mean())
    return min_age, max_age, mean_age


def create_age_ranges(df):
    bins = [18, 25, 35, 45, 55]
    labels = ["18-25", "26-35", "36-45", "46-55"]

    df["Age_Group"] = pd.cut(df["Age"], bins=bins, labels=labels, right=True)
    return df


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
