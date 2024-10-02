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


def analyze_investment_by_month(df, path):
    monthly_average = (
        df.groupby("Month")["Amount_invested_monthly"]
        .mean()
        .sort_index()
        .to_dict()
    )
    monthly_counts = df["Month"].value_counts().to_dict()
    investment_df = pd.DataFrame(
        {
            "Month": monthly_average.keys(),
            "Average_Investment": monthly_average.values(),
            "Client_Count": [
                monthly_counts.get(month, 0)
                for month in monthly_average.keys()
            ],
        }
    )
    investment_df.to_csv(path, index=False)
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


def occupation_ration(df):
    occupation_counts = df["Occupation_Count"]
    total_clients = occupation_counts.sum()
    occupation_ratio = {
        occ: count / total_clients * 100
        for occ, count in zip(df["Occupation"], occupation_counts)
    }

    return occupation_ratio


def get_age_occupation_summary(df, path):
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

    age_occupation_summary.to_csv(path, index=False)
    return age_occupation_summary


def get_occupation_age_group_summary(df):
    occupation_age_group_summary = (
        df.groupby(["Age_Group", "Occupation"])
        .agg(Count=("Occupation_Count", "sum"))
        .reset_index()
    )
    return occupation_age_group_summary


def get_age_income_summary(df, path):
    age_income_summary = (
        df.groupby("Age_Group")
        .agg(Average_Income=("Average_Income", "mean"))
        .reset_index()
    )
    age_income_summary.to_csv(path, index=False)
    return age_income_summary
