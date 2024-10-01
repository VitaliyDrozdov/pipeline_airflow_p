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
    age_group_counts = df["Age_Group"].value_counts().reset_index()
    age_group_counts.columns = ["Age_Group", "Count"]
    return age_group_counts


def analyze_income(df):
    return (
        df["Annual_Income"].min(),
        df["Annual_Income"].max(),
        df.groupby("Age")["Annual_Income"].mean().astype(int).to_dict(),
    )


def analyze_occupation(df):
    # Количество клиентов по профессиям:
    occupation_counts = df["Occupation"].value_counts().to_dict()
    # Средний доход по профессиям:
    average_income_by_occupation = (
        df.groupby("Occupation")["Annual_Income"].mean().to_dict()
    )
    return occupation_counts, average_income_by_occupation


def analyze_investment_by_month(df):
    monthly_average = (
        df.groupby("Month")["Amount_invested_monthly"]
        .mean()
        .sort_index()
        .to_dict()
    )
    monthly_counts = df["Month"].value_counts().to_dict()
    return monthly_average, monthly_counts


def occupation_by_age_range(df):
    age_ranges_df = df["age_ranges"]
    # occupation_counts = df["occupation_counts"]
    occupation_by_age_range = (
        age_ranges_df.groupby("Age_Group")["Occupation"]
        .value_counts()
        .unstack(fill_value=0)
    )
    return occupation_by_age_range


def analyze_5a(df):
    occupation_counts = df["Occupation_Count"]
    total_clients = occupation_counts.sum()
    occupation_ratio = {
        occ: count / total_clients * 100
        for occ, count in zip(df["Occupation"], occupation_counts)
    }

    return occupation_ratio


def analyze_5b(df):
    # average_income_by_occupation = df["Average_Income"]
    age_occupation_summary = (
        df.groupby("Age_Group")
        .agg(
            Most_Common_Occupation=(
                "Occupation",
                lambda x: x.value_counts().idxmax(),
            ),  # Наиболее распространенная профессия
        )
        .reset_index()
    )
    return age_occupation_summary
