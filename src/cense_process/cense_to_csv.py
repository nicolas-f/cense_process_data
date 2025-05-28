import sys
import json
import time

import h5py
import fsspec
import argparse

def fetch_hdf5_files(api_url):
    sensors = []
    # fetch hdf5 file list for project
    with fsspec.open(api_url, mode="rb") as f:
        data = json.load(f)["data"]["items"]
        document: dict
        for document in data:
            if document.get("file_content_type", "") == "application/x-hdf":
                sensors.append(document)
    return sensors

fsspec_caching = {
    "cache_type": "blockcache",  # block cache stores blocks of fixed size and uses eviction using a LRU strategy.
    "block_size": 8
    * 1024
    * 1024,  # size in bytes per block, adjust depends on the file size but the recommended size is in the MB
}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api_url", default="https://entrepot.recherche.data.gouv.fr")
    parser.add_argument("--project_name", default="cense")
    args = parser.parse_args()
    search_url = f"{args.api_url}/api/search?q={args.project_name}&per_page=1000"
    print(f"search_url: {search_url}")
    documents = fetch_hdf5_files(search_url)
    day_occurrences = dict()
    doc_count = len(documents)
    try:
        for doc_id, doc in enumerate(documents):
            url = doc["url"]
            print(doc)
            doc_size = doc["size_in_bytes"]
            print(f"Connect to {url} {doc_id + 1}/{doc_count}")
            start = time.time()
            with fsspec.open(url, mode="rb", **fsspec_caching) as remote_f:
                f = h5py.File(remote_f)
                for year_month in f.keys():
                    for day in f[year_month]:
                        key = f"{year_month}_{day}"
                        day_occurrences[key] = 1 + day_occurrences.get(key, 0)
            print(f"Fetch in {round(time.time() - start, 2)} seconds (document is {round(doc_size / 1024 ** 2, 2)} MB)")
    finally:
        print(f"day_occurrences: {day_occurrences}")

if __name__ == "__main__":
    main()

