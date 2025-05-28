import csv
import os
import sys
import time
import h5py
import argparse
import remfile
import urllib3
import multiprocessing

def fetch_hdf5_files(api_url):
    sensors = []
    # fetch hdf5 file list for project
    resp = urllib3.request("GET", api_url)
    data = resp.json()["data"]["items"]
    document: dict
    for document in data:
        if document.get("file_content_type", "") == "application/x-hdf":
            sensors.append(document)
    return sensors

def fetch_days(doc):
    url = doc["url"]
    doc_size = doc["size_in_bytes"]
    print(f"Connect to {url}")
    start = time.time()
    remote_f = remfile.File(url)
    if hasattr(remote_f, "open"):
        remote_f = remote_f.open()
    day_occurrences = {}
    with h5py.File(remote_f, 'r') as f:
        for year_month in f.keys():
            for day in f[year_month]:
                key = f"{year_month}_{day}"
                day_occurrences[key] = 1 + day_occurrences.get(key, 0)
    print(f"Fetch in {round(time.time() - start, 2)} seconds (document is {round(doc_size / 1024 ** 2, 2)} MB)")
    return day_occurrences

def fetch_sensors_per_day(documents):
    day_occurrences = {}
    try:
        doc_count = len(documents)
        doc_id = 0
        with multiprocessing.Pool(12) as p:
            for res in p.imap_unordered(fetch_days, documents):
                for date, day_occurrence in res.items():
                    day_occurrences[date] = day_occurrences[date] = 1 + day_occurrences.get(date, 0)
                print(f"Done {doc_id + 1}/{doc_count}")
                doc_id += 1

    finally:
        print(f"day_occurrences: {day_occurrences}")
    return day_occurrences

def fetch_slow_day_data(doc):
    rows_written = 0
    try:
        url = doc["url"]
        print(f"Connect to {url}")
        start = time.time()
        remote_f = remfile.File(url)
        if hasattr(remote_f, "open"):
            remote_f = remote_f.open()
        year_month = doc["year_month"]
        day = doc["day"]
        with h5py.File(remote_f, 'r') as f:
            lat = f.attrs["lat"]
            lon = f.attrs["long"]
            mac = f.attrs["mac"].replace(":", "")
            file_name = f"{year_month}_{day}_{mac}_{lat}_{lon}.csv"
            if year_month in f:
                year_month = f[year_month]
                if day in year_month:
                    day_group = year_month[day]
                    columns = [column_name for column_name, column_type in day_group["slow_1s"].dtype.descr]
                    with open(file_name, 'w', newline='') as csvfile:
                        writer = csv.DictWriter(csvfile, fieldnames=columns)
                        writer.writeheader()
                        for row in day_group["slow_1s"]:
                            writer.writerow(dict(zip(columns, row)))
                            rows_written += 1
            if rows_written == 0:
                # delete file if the file is empty
                if os.path.exists(file_name):
                    os.remove(file_name)
    except Exception as e:
        print(e, file=sys.stderr)
    return rows_written



def fetch_all_sensors_slow(documents, year_month, day):
    for doc in documents:
        doc["year_month"] = year_month
        doc["day"] = day
    doc_id = 0
    doc_count = len(documents)
    with multiprocessing.Pool(12) as p:
        for res in p.imap_unordered(fetch_slow_day_data, documents):
            print(f"Done {doc_id + 1}/{doc_count}")
            doc_id += 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api_url", default="https://entrepot.recherche.data.gouv.fr")
    parser.add_argument("--project_name", default="cense")
    args = parser.parse_args()
    search_url = f"{args.api_url}/api/search?q={args.project_name}&per_page=1000"
    print(f"search_url: {search_url}")
    documents = fetch_hdf5_files(search_url)
    #day_occurrences = fetch_sensors_per_day(documents)
    year_month = "2020_01"
    day = "20"
    fetch_all_sensors_slow(documents, year_month, day)

if __name__ == "__main__":
    main()

