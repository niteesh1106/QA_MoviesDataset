import re
import psutil
import socket
import logging
import warnings
import pandas as pd
from tqdm import tqdm
from pymongo import MongoClient
from stanza.server import CoreNLPClient


logging.getLogger("stanza").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger().setLevel(logging.ERROR)
warnings.filterwarnings("ignore")

# MongoDB Connection
client = MongoClient("mongodb://localhost:27017/")
db = client['MoviesDB']
collection = db['movies']
data = pd.DataFrame(list(collection.find()))

# CoreNLP Path
CORENLP_PATH = "C:/Users/nitee/stanza_corenlp/*"


def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]


def kill_corenlp_process(port=9000):
    for proc in psutil.process_iter():
        try:
            for conn in proc.connections(kind="inet"):
                if conn.laddr.port == port:
                    proc.terminate()
        except Exception:
            pass


def resolve_coreferences(annotated):
    if not annotated.corefChain:
        return annotated.text

    replacements = {}
    for chain in annotated.corefChain:
        representative = chain.mention[0]  # Representative mention
        rep_text = " ".join(
            token.word
            for token in annotated.sentence[representative.sentenceIndex].token[
                representative.beginIndex:representative.endIndex
            ]
        )

        for mention in chain.mention[1:]:
            sent_idx = mention.sentenceIndex
            token_range = range(mention.beginIndex, mention.endIndex)
            mention_text = " ".join(
                annotated.sentence[sent_idx].token[i].word for i in token_range
            )
            replacements[mention_text] = rep_text

    resolved_text = annotated.text
    for mention_text, rep_text in replacements.items():
        resolved_text = re.sub(r"\b" + re.escape(mention_text) + r"\b", rep_text, resolved_text)

    return resolved_text


def process_overview(overview, movie_id, client):
    triples = []
    ann = client.annotate(overview)
    resolved_text = resolve_coreferences(ann)
    resolved_ann = client.annotate(resolved_text)

    # Extract OpenIE triples
    for sentence in resolved_ann.sentence:
        for triple in sentence.openieTriple:
            triples.append({
                "movie_id": movie_id,
                "subject": triple.subject,
                "predicate": triple.relation,
                "object": triple.object,
            })

    return triples


def main():
    kill_corenlp_process(port=9000)
    port = 9000 if not psutil.net_connections(kind="inet") else find_free_port()

    # CoreNLPClient
    client = CoreNLPClient(
        annotators=["tokenize", "ssplit", "pos", "lemma", "ner", "depparse", "coref", "openie"],
        timeout=60000,
        memory="8G",
        classpath=CORENLP_PATH,
        port=port,
        be_quiet=True
    )

    try:
        client.start()
        all_triples = []
        for _, row in tqdm(data.iterrows(), total=len(data)):
            if pd.notna(row.get("overview")):
                extracted_triples = process_overview(row["overview"], row["id"], client)
                all_triples.extend(extracted_triples)

        # Save to MongoDB
        triples_collection = db["triples"]
        triples_collection.drop()
        triples_collection.insert_many(all_triples)

        print("Triple extraction and storage completed.")
    finally:
        client.stop()


if __name__ == "__main__":
    main()
