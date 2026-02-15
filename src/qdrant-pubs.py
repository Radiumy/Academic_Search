
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

def check_papers(client: QdrantClient, authors: List[str]) -> int:
    res = client.scroll(
        collection_name="papers_v5",
        scroll_filter=build_author_filter(authors),
        order_by='creation_time',
        with_payload=['title', 'authors', 'pdf_url'],
        limit=500
    )
    papers = [r.payload for r in res[0]]
    return papers


def accept_paper(paper: Dict, accept_threshold: int = 1) -> bool:
    accept_keywords = ['llm', 'language model', 'generative model', 'watermark', 'code', 'prompt', 'eval', 
                       'align', 'pretrain', 'tune', 'nlp', 'language', 'multilingual', 'advers', 'learning', 'ai', 
                       'benchmark', 'neural', 'robust', 'gpt', 'multimodal', 'transformer', 'ml', 'watermark', 
                       'software', 'learn', 'distil', 'train', 'llama', 'alpaca',
                       'lm', 'supervis', 'representation', 'sequence', 'seq2seq', 'rnn',
                       'semi', 'deep', 'embed', 'hallucin', 'encoder', 'token', 'vocab', 'layer', 'norm', 'asr', 'text'
                       ]
    title = paper['title']
    cnt = 0
    for kw in accept_keywords:
        if kw in title.lower():
            cnt += 1
            if cnt >= accept_threshold:
                return True
    return False

def accept_professor(client: QdrantClient, professor: str, accept_threshold: int = 2) -> bool:
    papers = check_papers(client, [professor])
    cnt = 0
    for paper in papers:
        if accept_paper(paper, accept_threshold):
            cnt += 1
            if cnt >= accept_threshold:
                return True, papers
    print(cnt)
    return False, papers




if __name__ == "__main__":
    client = QdrantClient(url="http://100.89.198.70:6333")
    # print(accept_professor(client, ['Florian Tramer']))
    data_path = 'filtered_peoples.json'
    with open(data_path) as f:
        data = json.load(f)
    outputs = {}
    for k in tqdm(data):
        acc, papers = accept_professor(client, k)
        print(k, acc)
        if acc:
            outputs[k] = {
                'name': k,
                'school': data[k]['school'],
                'papers': papers
            }
    with open('filtered_data_accepted.json', 'w') as f:
        json.dump(outputs, f, indent=4)
