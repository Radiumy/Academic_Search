
import numpy as np
from qdrant_client import QdrantClient, models
from tqdm import tqdm
from typing import List, Dict, Tuple, Any
import os, time, json


def build_author_filter_cond(author: str) -> models.FieldCondition:
    return models.FieldCondition(key='authors[]', match=models.MatchValue(value=author.strip()))

def build_author_filter(authors: List[str]) -> models.Filter:
    conds = [build_author_filter_cond(author) for author in authors]
    return models.Filter(must=conds)

def count_papers(client: QdrantClient, author: str) -> int:
    res = client.scroll(
        collection_name="papers_v5",
        scroll_filter=build_author_filter([author]),
        with_payload=['authors'],
        limit=200
    )
    first_cnt = 0
    last_cnt = 0
    for paper in res[0]:
        auths = paper.payload['authors']
        if auths[0] == author:
            first_cnt += 1
        if auths[-1] == author:
            last_cnt += 1
    return len(res[0]), first_cnt, last_cnt

def process_school(client: QdrantClient, data_path: str):
    print(f"Processing school: {data_path}")
    with open(data_path) as f:
        data = json.load(f)
    filtered_data = []
    school_cnt = 0
    for k in tqdm(data):
        cnt, first, last = count_papers(client, k)
        if first > 3 or last > 1:
            filtered_data.append({
                'name': k,
                'school': data_path,
                'data': data[k]
            })
            school_cnt += 1
    print(f"School count: {school_cnt}")
    return filtered_data

if __name__ == "__main__":
    client = QdrantClient(url="http://100.89.198.70:6333")
    # print(count_papers(client, "Florian Tramer"))

    # exit(0)

    json_lists = os.listdir('C:\\advisor-crawler\\data')
    print(json_lists)
    json_lists = [os.path.join('C:\\advisor-crawler\\data', f) for f in json_lists]
    json_lists = [f for f in json_lists if os.path.isfile(f) and f.endswith('.json')]
    all_filtered_data = []
    for json_path in json_lists:
        filtered_data = process_school(client, json_path)
        all_filtered_data.extend(filtered_data)
    with open('all_filtered_data.json', 'w') as f:
        json.dump(all_filtered_data, f, indent=4)
