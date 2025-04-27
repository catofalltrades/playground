import requests
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone


def post_data(data):
    post_url = "https://candidate.hubteam.com/candidateTest/v3/problem/result?userKey=27eb057210b742f2dad3a27f0ff9"
    # post_url = "https://candidate.hubteam.com/candidateTest/v3/problem/test-result?userKey=27eb057210b742f2dad3a27f0ff9"
    results = {"results": data}
    response = requests.post(post_url, json=results)
    if response.status_code == 200:
        print("Data posted successfully.")
    else:
        print("Failed to post data. Status code:", response.status_code)
        print("Error message:", response.text)


def get_data():
    get_url = "https://candidate.hubteam.com/candidateTest/v3/problem/dataset?userKey=27eb057210b742f2dad3a27f0ff9"
    # get_url = "https://candidate.hubteam.com/candidateTest/v3/problem/test-dataset?userKey=27eb057210b742f2dad3a27f0ff9"
    response = requests.get(get_url)
    if response.status_code == 200:
        data = response.json()
        print("Data retrieved successfully: ", len(data["callRecords"]))
    else:
        print("Failed to retrieve data. Status code:", response.status_code)
        print("Error message:", response.text)
    return data["callRecords"]


def sort_data_by_customers_dates(data):
    data_by_customers_dates = defaultdict(lambda: defaultdict(list))
    for record in data:
        customer_id = record["customerId"]
        start_date = datetime.fromtimestamp(record["startTimestamp"]/1000, tz=timezone.utc)
        start_date_str = start_date.strftime("%Y-%m-%d")
        start_date_truncated = datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0, tzinfo=timezone.utc)
        data_by_customers_dates[customer_id][start_date_str].append(record)
        end_date = datetime.fromtimestamp((record["endTimestamp"]-1)/1000, tz=timezone.utc)
        end_date_truncated = datetime(end_date.year, end_date.month, end_date.day, 0, 0, 0, tzinfo=timezone.utc)
        record["prettyTime"] = "{} - {}".format(
            datetime.fromtimestamp(record["startTimestamp"]/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            datetime.fromtimestamp((record["endTimestamp"]-1)/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        )
        delta_days = end_date_truncated - start_date_truncated
        if delta_days.days > 0:
            for index in range(1, delta_days.days+1):
                date = start_date_truncated + timedelta(days=index)
                date_str = date.strftime("%Y-%m-%d")
                data_by_customers_dates[customer_id][date_str].append(record)
    return data_by_customers_dates


def get_max_concurrent_calls(data_by_customers_dates):
    max_concurrent_calls_by_customers_dates = []
    for customer_id, dates in data_by_customers_dates.items():
        for date_str, records in dates.items():
            records.sort(key=lambda x: x["startTimestamp"])
            concurrent_calls = []
            max_concurrent_calls = []
            num_concurrent_calls = 0
            max_num_concurrent_calls = 0
            date = datetime.strptime(date_str, "%Y-%m-%d")
            timestamp = date.replace(tzinfo=timezone.utc).timestamp()
            max_concurrent_timestamp = int(timestamp*1000)
            call_timestamps = []
            for record in records:
                start_timestamp = record["startTimestamp"]
                end_timestamp = record["endTimestamp"]
                call_timestamps.append((start_timestamp, 1, record["callId"]))
                call_timestamps.append((end_timestamp, -1, record["callId"]))
            call_timestamps.sort(key=lambda x: (x[0], x[1]))
            for timestamp, delta, call_id in call_timestamps:
                num_concurrent_calls += delta
                if delta > 0:
                    concurrent_calls.append(call_id)
                else:
                    concurrent_calls.remove(call_id)
                if num_concurrent_calls > max_num_concurrent_calls:
                    max_num_concurrent_calls = num_concurrent_calls
                    if timestamp > max_concurrent_timestamp:
                        max_concurrent_timestamp = timestamp
                    max_concurrent_calls = concurrent_calls.copy()
            max_concurrent_calls_by_customers_dates.append({
                "customerId": customer_id,
                "date": date_str,
                "maxConcurrentCalls": max_num_concurrent_calls,
                "callIds": max_concurrent_calls,
                "timestamp": max_concurrent_timestamp
            })
                
    return max_concurrent_calls_by_customers_dates

def main():
    data = get_data()
    data_by_customers_dates = sort_data_by_customers_dates(data)
    max_concurrent_calls_by_customers_dates = get_max_concurrent_calls(data_by_customers_dates)
    post_data(max_concurrent_calls_by_customers_dates)


if __name__ == "__main__":
    main()