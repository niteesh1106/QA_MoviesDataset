import os
import csv
import logging
from tqdm import tqdm
import pandas as pd
from pymongo import MongoClient

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

# MongoDB Setup
client = MongoClient("mongodb://localhost:27017/")
db = client['MoviesDB']
movies_collection = db['movies']
triples_collection = db['triples']

# Create output directory
output_dir = "neo4j_import"
os.makedirs(output_dir, exist_ok=True)

# Fetch movies data
logging.info("Fetching movies data from MongoDB...")
movies = list(movies_collection.find())
if not movies:
    logging.error("No movies found in MongoDB. Ensure the movies collection has data.")
    exit()
movies_df = pd.DataFrame(movies)

if 'release_date' in movies_df.columns:
    logging.info("Converting release_date to 'YYYY-MM-DD' format...")
    movies_df['release_date'] = pd.to_datetime(movies_df['release_date'], errors='coerce')
    movies_df['release_date'] = movies_df['release_date'].dt.strftime('%Y-%m-%d')
    movies_df['release_date'] = movies_df['release_date'].fillna('')

# Fix numeric fields
logging.info("Converting numeric fields to appropriate data types...")
numeric_fields = ['budget', 'runtime', 'vote_average', 'revenue', 'imdb_rating', 'imdb_votes', 'popularity']
for field in numeric_fields:
    if field in movies_df.columns:
        if field == 'imdb_votes':  # Convert to integer
            movies_df[field] = pd.to_numeric(movies_df[field], errors='coerce').fillna(0).astype(int)
        else:  # Convert to float
            movies_df[field] = pd.to_numeric(movies_df[field], errors='coerce').fillna(0).astype(float)

# Generate unique IDs for entities
entity_id_counter = 1
entity_name_to_id = {}


def normalize_name(name):
    if isinstance(name, list):
        cleaned = ', '.join([n.strip().replace('[', '').replace(']', '').replace("'", "").lower() for n in name if isinstance(n, str)])
    elif isinstance(name, str):
        cleaned = name.strip().replace('[', '').replace(']', '').replace("'", "").lower()
    else:
        cleaned = str(name).strip().replace('[', '').replace(']', '').replace("'", "").lower()
    return cleaned.strip()


def get_entity_id(name, source):
    global entity_id_counter
    name = normalize_name(name)
    key = f"{source}:{name}"
    if key not in entity_name_to_id:
        entity_name_to_id[key] = entity_id_counter
        entity_id_counter += 1
    return entity_name_to_id[key]


# Prepare Nodes CSV Files
movies_nodes_file = os.path.join(output_dir, 'movies_nodes.csv')
entities_nodes_file = os.path.join(output_dir, 'entities_nodes.csv')
triples_entities_nodes_file = os.path.join(output_dir, 'triples_entities_nodes.csv')

# Prepare Relationships CSV Files
movies_entities_rels_file = os.path.join(output_dir, 'movies_entities_rels.csv')
triples_rels_file = os.path.join(output_dir, 'triples_rels.csv')
movies_triples_rels_file = os.path.join(output_dir, 'movies_triples_rels.csv')

# 1. Export Movies Nodes
logging.info("Exporting movies nodes...")
with open(movies_nodes_file, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['movieId:ID(Movie)', 'title', 'release_date', 'budget:float', 'runtime:float',
                  'vote_average:float', 'status', 'revenue:float', 'original_title',
                  'imdb_rating:float', 'imdb_votes:int', 'original_language', 'popularity:float']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for _, movie in tqdm(movies_df.iterrows(), total=movies_df.shape[0]):
        writer.writerow({
            'movieId:ID(Movie)': movie['id'],
            'title': movie.get('title', ''),
            'release_date': movie.get('release_date', ''),
            'budget:float': movie.get('budget', 0.0),
            'runtime:float': movie.get('runtime', 0.0),
            'vote_average:float': movie.get('vote_average', 0.0),
            'status': movie.get('status', ''),
            'revenue:float': movie.get('revenue', 0.0),
            'original_title': movie.get('original_title', ''),
            'imdb_rating:float': movie.get('imdb_rating', 0.0),
            'imdb_votes:int': movie.get('imdb_votes', 0),
            'original_language': movie.get('original_language', ''),
            'popularity:float': movie.get('popularity', 0.0),
        })

# 2. Collect Entities and Relationships from Movies
logging.info("Collecting entities and relationships from movies...")
entity_labels = {
    'genres': 'Genre',
    'spoken_languages': 'Language',
    'production_companies': 'Company',
    'production_countries': 'Country',
    'cast': 'Person',
    'director': 'Person',
    'writers': 'Person',
    'producers': 'Person',
    'music_composer': 'Person',
    'director_of_photography': 'Person'
}
relationship_types = {
    'genres': 'BELONGS_TO_GENRE',
    'spoken_languages': 'SPOKEN_IN',
    'production_companies': 'PRODUCTION_COMPANY',
    'production_countries': 'PRODUCED_IN',
    'cast': 'ACTED_IN',
    'director': 'DIRECTED_BY',
    'writers': 'WRITTEN_BY',
    'producers': 'PRODUCED_BY',
    'music_composer': 'COMPOSED_BY',
    'director_of_photography': 'DOP_BY'
}

entities_movies = []
movies_entities_relationships = []

for _, movie in tqdm(movies_df.iterrows(), total=movies_df.shape[0]):
    movie_id = movie['id']
    for field, label in entity_labels.items():
        items = movie.get(field, [])
        if not isinstance(items, list):
            items = [items]
        for item in items:
            if item:
                normalized_name = normalize_name(item)
                entity_id = get_entity_id(normalized_name, source='movie')
                entities_movies.append({
                    'entityId:ID(Entity)': entity_id,
                    'name': normalized_name,
                    ':LABEL': label
                })
                movies_entities_relationships.append({
                    ':START_ID(Movie)': movie_id,
                    ':END_ID(Entity)': entity_id,
                    ':TYPE': relationship_types[field]
                })

# 3. Export Entities Nodes from Movies
logging.info("Exporting entities nodes from movies...")
entities_movies_df = pd.DataFrame(entities_movies).drop_duplicates(subset=['entityId:ID(Entity)'])
entities_movies_df.to_csv(entities_nodes_file, index=False, encoding='utf-8')

# 4. Export Movies-Entities Relationships
logging.info("Exporting movies-entities relationships...")
movies_entities_relationships_df = pd.DataFrame(movies_entities_relationships).drop_duplicates()
movies_entities_relationships_df.to_csv(movies_entities_rels_file, index=False, encoding='utf-8')

# 5. Process Triples Data
logging.info("Fetching triples data from MongoDB...")
triples = list(triples_collection.find({}, {"_id": 0, "subject": 1, "predicate": 1, "object": 1, "movie_id": 1}))
if not triples:
    logging.error("No triples found in MongoDB. Ensure the triples collection has data.")
    exit()

entities_triples = []
triples_relationships = []
movies_triples_relationships = []

for triple in tqdm(triples):
    subject = triple.get('subject')
    predicate = triple.get('predicate')
    obj = triple.get('object')
    movie_id = triple.get('movie_id')

    if not subject or not predicate or not obj:
        continue

    subject_normalized = normalize_name(subject)
    object_normalized = normalize_name(obj)

    subject_id = get_entity_id(subject_normalized, source='triple')
    object_id = get_entity_id(object_normalized, source='triple')

    entities_triples.append({
        'entityId:ID(Entity)': subject_id,
        'name': subject_normalized,
        ':LABEL': 'Entity'
    })
    entities_triples.append({
        'entityId:ID(Entity)': object_id,
        'name': object_normalized,
        ':LABEL': 'Entity'
    })

    triples_relationships.append({
        ':START_ID(Entity)': subject_id,
        ':END_ID(Entity)': object_id,
        ':TYPE': predicate
    })

    movies_triples_relationships.append({
        ':START_ID(Movie)': movie_id,
        ':END_ID(Entity)': subject_id,
        ':TYPE': 'HAS_SUBJECT'
    })
    movies_triples_relationships.append({
        ':START_ID(Movie)': movie_id,
        ':END_ID(Entity)': object_id,
        ':TYPE': 'HAS_OBJECT'
    })

# 6. Export Triples Entities Nodes
logging.info("Exporting triples entities nodes...")
entities_triples_df = pd.DataFrame(entities_triples).drop_duplicates(subset=['entityId:ID(Entity)'])
entities_triples_df.to_csv(triples_entities_nodes_file, index=False, encoding='utf-8')

# 7. Export Triples Relationships
logging.info("Exporting triples relationships...")
triples_relationships_df = pd.DataFrame(triples_relationships).drop_duplicates()
triples_relationships_df.to_csv(triples_rels_file, index=False, encoding='utf-8')

# 8. Export Movies-Triples Relationships
logging.info("Exporting movies-triples relationships...")
movies_triples_relationships_df = pd.DataFrame(movies_triples_relationships).drop_duplicates()
movies_triples_relationships_df.to_csv(movies_triples_rels_file, index=False, encoding='utf-8')

logging.info("Data export to CSV files completed.")