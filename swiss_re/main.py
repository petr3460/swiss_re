import pandas
from functools import reduce
import json
from glob import glob
import os


schema = [
    "ts",
    "resp_header_size",
    "client_ip",
    "resp_code",
    "resp_size",
    "method",
    "url",
    "username",
    "dest_ip",
    "resp_type",
]


def get_df(filename):
    df = pandas.read_csv(
        filename,
        delim_whitespace=True,
        on_bad_lines="skip",
        names=schema,
    )
    return df


def get_most_least_frequent_ip(dfs):
    def _merge_dfs(df1, df2):
        merged = pandas.merge(df1, df2, how="outer", on=["client_ip"]).fillna(0)
        merged["count"] = merged["count_x"] + merged["count_y"]
        return merged.drop(columns=["count_x", "count_y"])
    
    def _get_count_by_ip(df):
        res = (
            df.groupby(["client_ip"])["client_ip"]
            .count()
            .reset_index(name="count")
            .sort_values(["count"], ascending=False)
        )
        return res

    count_by_ip = map(_get_count_by_ip, dfs)
    total_count = reduce(_merge_dfs, count_by_ip)
    ip_field = "client_ip"
    max_index = total_count["count"].idxmax()
    min_index = total_count["count"].idxmin()
    return (total_count.loc[max_index][ip_field], total_count.loc[min_index][ip_field])


def get_events_per_second(dfs):
    def _get_events_per_second(df):
        size = len(df)
        start_time = df.loc[0]["ts"]
        end_time = df.loc[size - 1]["ts"]
        period = end_time - start_time
        return size / period

    events_per_sec_by_df = list(map(_get_events_per_second, dfs))
    result = sum(events_per_sec_by_df) / len(events_per_sec_by_df)
    return result


def get_total_bytes_exchanged(dfs):
    def _get_bytes_exchanged(df):
        return df["resp_size"].sum() + df["resp_header_size"].sum()

    bytes_exchanged_by_df = map(_get_bytes_exchanged, dfs)
    return sum(bytes_exchanged_by_df)


def get_filenames():
    log_dir = "/tmp/logs/"
    file_format = "*.log"
    return glob(os.path.join(log_dir, file_format))


def run():
    files = get_filenames()
    if not files:
        raise Exception("Files not found.")
    dataframes = [get_df(f) for f in files]

    events_per_second = get_events_per_second(dataframes)
    total_bytes_exchanged = get_total_bytes_exchanged(dataframes)

    most_frequent_ip, least_frequent_ip = get_most_least_frequent_ip(dataframes)
    result = {
        "Most frequent IP": most_frequent_ip,
        "Least frequent IP": least_frequent_ip,
        "Events per second": str(events_per_second),
        "Total amount of bytes exchanged": str(total_bytes_exchanged),
    }
    with open("result/result.json", "w") as file:
        json.dump(result, file)
        print("result saved in 'result.json'")


if __name__ == "__main__":
    run()
