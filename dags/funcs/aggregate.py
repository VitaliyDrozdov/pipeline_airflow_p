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


def analyze_income(df):
    return (
        df["Annual_Income"].min(),
        df["Annual_Income"].max(),
        df.groupby("Age")["Annual_Income"].mean(),
    )


def analyze_occupation(df):
    # Количество клиентов по профессиям:
    occupation_counts = df["Occupation"].value_counts()
    # Средний доход по профессиям:
    average_income_by_occupation = df.groupby("Occupation")[
        "Annual_Income"
    ].mean()
    return occupation_counts, average_income_by_occupation


def analyze_investment_by_month(df):
    return (
        df.groupby("Month")["Amount_invested_monthly"].mean().sort_index(),
        df["Month"].value_counts(),
    )


def credit_rating_by_age_range():
    pass


def occupation_by_age_range():
    pass


def expenses_by_age_range():
    pass
