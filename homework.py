import pandas as pd

hourly_rates = {
    pd.Timestamp("2022-06-14 14:00"): 1500,
    pd.Timestamp("2022-06-14 15:00"): 1800,
    pd.Timestamp("2022-06-14 16:00"): 3000,
    pd.Timestamp("2022-06-14 17:00"): 780,
}


def construct_df():
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


def get_site_data_as_series(
    file_path: str = "files/site_1.csv", index_header="Interval Beginning (EST)"
):
    series = pd.read_csv(file_path)
    series["Interval Beginning (EST)"] = series["Interval Beginning (EST)"].apply(
        pd.Timestamp
    )
    return series.set_index("Interval Beginning (EST)")


def customer_performance_from_baseline(customer_series: pd.Series, baseline: float):
    return (
        customer_series.rolling(window=4)
        .apply(sum)  # takes a rolling sum of the prior 4 intervals
        .dropna()  # eliminates the leading 3 NA values
        .iloc[
            ::4
        ]  # filters every 4th entry, isolating the 45 minute mark (This could also be done with groupby by grouping on the :45)
        .apply(
            lambda x: baseline - x
        )  # subtract the baseline from the sum of each hour
        .mean()
        .iloc[0]  # return floating point value
    )


def get_10of10_baseline(
    data: pd.Series = get_site_data_as_series(),
    event_date: pd.Timestamp = pd.Timestamp("2022-06-14"),
) -> float:
    hourly_aggregates = data.groupby(
        data.index.floor("h")
    ).sum()  # groups the total recorded performance hourly (adding up all values within an hour interval)
    daily_average = hourly_aggregates.groupby(
        hourly_aggregates.index.floor("d")
    ).mean()  # calculate the average hourly recorded performance over the course of each day
    return (
        daily_average[daily_average.index.dayofweek < 5]
        .loc[
            event_date
            - (pd.offsets.BusinessDay(10)) : event_date
            - (pd.offsets.BusinessDay(1))
        ]
        .mean()
        .iloc[0]
    )  # filters for weekends, takes the average of the 10 days prior to the event date


def calculate_payouts(
    customer_series: pd.Series,
    baseline: float,
    customer_profit_share: float = 0.64,
    hourly_payout_rates: dict = hourly_rates,
):
    hourly_performance = (
        customer_series.rolling(window=4)
        .apply(sum)
        .dropna()
        .iloc[::4]
        .apply(lambda x: baseline - x)
    )
    hourly_performance = hourly_performance.set_index(
        hourly_performance.index.floor("h")
    )
    hourly_performance["Revenue"] = (
        hourly_performance["kWh"] / 1000 * pd.Series(hourly_payout_rates)
    ).apply(
        lambda x: max(x, 0)
    )  # .mean()
    hourly_performance["Customer Share"] = (
        hourly_performance["Revenue"] * customer_profit_share
    )
    hourly_performance["Voltus Share"] = (
        hourly_performance["Revenue"] - hourly_performance["Customer Share"]
    )
    return hourly_performance


def main():
    df = construct_df()

    for i in [1, 2, 3, 5, 6]:
        site_data = get_site_data_as_series("files/site_" + str(i) + ".csv")
        ten_of_ten_baseline = get_10of10_baseline(
            data=site_data, event_date=pd.Timestamp("2022-06-14")
        )
        df.loc[
            i,
            "10 of 10 baseline",
        ] = ten_of_ten_baseline
        df.loc[i, "Average Performance (FSL)"] = customer_performance_from_baseline(
            site_data.loc[
                pd.Timestamp("2022-06-14 14:00:00") : pd.Timestamp(
                    "2022-06-14 18:00:00"
                )
            ],
            ten_of_ten_baseline,
        )
        df.loc[
            i, "Average Performance (10 of 10)"
        ] = customer_performance_from_baseline(
            site_data.loc[
                pd.Timestamp("2022-06-14 14:00:00") : pd.Timestamp(
                    "2022-06-14 18:00:00"
                )
            ],
            df.loc[i, "MISO FSL Baseline"],
        )

        payouts = calculate_payouts(
            site_data.loc[
                pd.Timestamp("2022-06-14 14:00:00") : pd.Timestamp(
                    "2022-06-14 18:00:00"
                )
            ],
            ten_of_ten_baseline,
            df["Profit Share%"].loc[i],
        )
        df.loc[i, "Revenue"] = payouts["Revenue"].sum()
        df.loc[i, "Customer Share"] = payouts["Customer Share"].sum()
        df.loc[i, "Voltus Share"] = payouts["Voltus Share"].sum()

    return df


print(main())
