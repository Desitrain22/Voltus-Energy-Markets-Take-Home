import pandas as pd

miso_baselines = {1: 10700, 2: 5400, 3: 850, 5: 9000, 6: 350}


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
        .apply(sum) #takes a rolling sum of the prior 4 intervals
        .dropna() #eliminates the leading 3 NA values
        .iloc[::4] #filters every 4th entry, isolating the 45 minute mark (This could also be done with groupby by grouping on the :45)
        .apply(lambda x: baseline - x) #subtract the baseline from the sum of each hour
        .mean()
        .iloc[0] #return floating point value
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


for i in [1, 2, 3, 5, 6]:
    site_data = get_site_data_as_series("files/site_" + str(i) + ".csv")
    fsl = customer_performance_from_baseline(
        site_data.loc[
            pd.Timestamp("2022-06-14 14:00:00") : pd.Timestamp("2022-06-14 18:00:00")
        ],
        get_10of10_baseline(data=site_data, event_date=pd.Timestamp("2022-06-14")),
    )
    ten_of_ten = customer_performance_from_baseline(
        site_data.loc[
            pd.Timestamp("2022-06-14 14:00:00") : pd.Timestamp("2022-06-14 18:00:00")
        ],
        miso_baselines[i],
    )
    print("Site " + str(i) + ":\nFSL: ", fsl, "\n10of10: ", ten_of_ten)
