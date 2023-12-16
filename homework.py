import pandas as pd
from typing import Union

hourly_rates = {
    pd.Timestamp("2022-06-14 14:00"): 1500,
    pd.Timestamp("2022-06-14 15:00"): 1800,
    pd.Timestamp("2022-06-14 16:00"): 3000,
    pd.Timestamp("2022-06-14 17:00"): 780,
}


def construct_df():
    """Constructs a skeleton solution dataframe, indexed by site number and containing the given metadata (profit share rates and MISO baselines)

    Args:
        None
    Returns:
        pd.DataFrame: a skeleton dataframe for populating with solutions"""
    # This is more easily done reading from a json or pre-made csv
    miso_baselines = {1: 10700, 2: 5400, 3: 850, 5: 9000, 6: 350}
    customer_profit_share = {1: 0.64, 2: 0.62, 3: 0.56, 5: 0.65, 6: 0.51}
    df = pd.DataFrame([miso_baselines, customer_profit_share]).transpose()
    df["10 of 10 baseline"] = ""
    df["Average Performance (10 of 10)"] = ""
    df["Average Performance (FSL)"] = ""
    df["Revenue"] = ""
    df["Customer Share"] = ""
    df["Voltus Share"] = ""
    return df.rename(columns={0: "MISO FSL Baseline", 1: "Profit Share%"})


def get_site_data(
    file_path: str = "files/site_1.csv", index_header="Interval Beginning (EST)"
) -> pd.DataFrame:
    """Given a file path to a site's CSV and the expected header, returns a dataframe indexed
    with pandas timestamps and the time series data. We opt for a single column dataframe as
    opposed to a series for ease of filtering and appending additional columns.

    Args:
        file_path (str, optional): Relative file path to a site's data. Defaults to "files/site_1.csv".
        index_header (str, optional): Expected header for the data column. Defaults to "Interval Beginning (EST)".

    Returns:
        pd.DataFrame: Single column df, indexed by timestamps.
    """
    series = pd.read_csv(file_path)
    series["Interval Beginning (EST)"] = series["Interval Beginning (EST)"].apply(
        pd.Timestamp
    )
    return series.set_index("Interval Beginning (EST)")


def get_10of10_baselines(
    data: pd.Series = get_site_data(),
    event_start=pd.Timestamp("2022-06-14 14:00:00"),
    event_end=pd.Timestamp("2022-06-14 17:00:00"),
) -> pd.Series:
    """Given a customer's site data, the event time range, returns the customer's baseline performance for that date using the MISO 10of10 methodology

    The 10of10 calculation metholody uses the mean of the 10 proceeding, non event, non weekend dates' daily average performance.

    Args:
        data (pd.DataFrame, optional): A customer's site data as a df. Defaults to site 1 data.
        event_start (pd.Timestamp, optional): starting timestamp of event. Defaults to pd.Timestamp("2022-06-14 14:00:00").
        event_end (pd.Timestamp, optional): ending timestamp of event. Defaults to pd.Timestamp("2022-06-14 14:00:00").

    Returns:
        float: The calculated baseline
    """

    data = data.loc[
        event_start - pd.offsets.BusinessDay(10) : event_end - pd.offsets.BusinessDay(1)
    ]  # Get the past 10 business days worth of data
    data = data[data.index.dayofweek < 5]  # Eliminate any weekend data
    data = data.groupby(
        data.index.floor("h")
    ).sum()  # Aggregate hourly performance across the whole series
    return pd.Series(
        {
            i: data[data.index.hour == i.hour].mean()
            for i in pd.date_range(
                event_start, event_end, freq="h"
            )  # Filter for measurements for each hour (2 P.M - 6 P.M), and get the mean()
        }
    )


def customer_performance_from_baseline(
    customer_series, baseline: Union[pd.Series, float]
) -> float:
    """Given a slice of the customer's 15 minute interval data and baseline kW measurement, returns the average performance relative to the given baseline over the course of the event. Assumes 15 minute intervals

    Args:
        customer_series (pd.Series): 15 minute interval series (indexed with timestamps, EST) of customer performance
        baseline (float or pd.Series): float (For FSL baseline) or series (for hourly 10of10 baselines) (kW)

    Returns:
        float: The average hourly performance
    """
    return (
        baseline - customer_series.groupby(customer_series.index.floor("h")).sum()
    ).mean()


def calculate_payouts(
    customer_series: pd.Series,
    baseline: pd.Series = get_10of10_baselines(),
    customer_profit_share: float = 0.64,
    hourly_payout_rates: dict = hourly_rates,
):
    df = customer_series.groupby(customer_series.index.floor("h")).sum()

    df = pd.DataFrame((baseline - df), columns=["Performance"])
    df["Revenue"] = (df["Performance"] * pd.Series(hourly_payout_rates) / 1000).apply(
        lambda x: max(x, 0)
    )
    df["Customer Share"] = df["Revenue"] * customer_profit_share
    df["Voltus Share"] = df["Revenue"] - df["Customer Share"]
    print(df)
    return df


def main():
    df = construct_df()

    for i in [1, 2, 3, 5, 6]:
        site_data = get_site_data("files/site_" + str(i) + ".csv")
        df.loc[i, "Average Performance (FSL)"] = customer_performance_from_baseline(
            site_data.loc[
                pd.Timestamp("2022-06-14 14:00:00") : pd.Timestamp(
                    "2022-06-14 17:00:00"
                )
            ]["kWh"],
            df.loc[i, "MISO FSL Baseline"],
        )

        df.loc[
            i, "Average Performance (10 of 10)"
        ] = customer_performance_from_baseline(
            site_data.loc[
                pd.Timestamp("2022-06-14 14:00:00") : pd.Timestamp(
                    "2022-06-14 17:00:00"
                )
            ]["kWh"],
            get_10of10_baselines(site_data["kWh"]),
        )

        payouts = calculate_payouts(
            site_data["kWh"].loc[
                pd.Timestamp("2022-06-14 14:00:00") : pd.Timestamp(
                    "2022-06-14 17:00:00"
                )
            ],
            get_10of10_baselines(site_data["kWh"]),
            df["Profit Share%"].loc[i],
        ).sum()
        df.loc[i, "Revenue"] = payouts["Revenue"]
        df.loc[i, "Customer Share"] = payouts["Customer Share"]
        df.loc[i, "Voltus Share"] = payouts["Voltus Share"]
    return df


print(main())
