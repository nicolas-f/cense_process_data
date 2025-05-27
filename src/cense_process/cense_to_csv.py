import sys
import json
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
    for id, doc in enumerate(documents):
        url = doc["url"]
        print(f"Connect to {url} {id+1}/{doc_count}")
        with fsspec.open(url, mode="rb") as remote_f:
            if hasattr(remote_f, "open"):
                remote_f = remote_f.open()

            f = h5py.File(remote_f)
            for day in f.keys():
                day_occurrences[day] = 1 + day_occurrences.get(day, 0)
    print(f"day_occurrences: {day_occurrences}")

if __name__ == "__main__":
    main()

