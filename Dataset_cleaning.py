import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm

tqdm.pandas()

file_path = 'TMDB_all_movies_post_2020(200k).csv'
new_data = pd.read_csv(file_path)

textual_columns = [
    'title', 'status', 'imdb_id', 'original_language', 'original_title',
    'overview', 'tagline', 'genres', 'production_companies',
    'production_countries', 'spoken_languages', 'cast', 'director',
    'director_of_photography', 'writers', 'producers', 'music_composer'
]


def handle_missing_values(df):
    df = df[df['title'].notna()]
    df = df[df['title'].str.strip() != ""]
    df['budget'] = df['budget'].mask(df['budget'] == 0, pd.NA)
    df['revenue'] = df['revenue'].mask(df['revenue'] == 0, pd.NA)
    df['runtime'] = df['runtime'].mask(df['runtime'] <= 0, pd.NA)
    textual_columns_to_fill = ['genres', 'director', 'imdb_id', 'spoken_languages',
                               'cast', 'production_companies', 'production_countries', 'writers',
                               'director_of_photography', 'producers', 'music_composer']
    for col in textual_columns_to_fill:
        if col in df.columns:
            df[col] = df[col].fillna("unknown")
    if 'overview' in df.columns:
        df['overview'] = df['overview'].str.strip().replace(["", "n/a", "none"], "unknown")
    return df


def standardize_text_columns(df, columns):
    for col in tqdm(columns, desc="Standardizing text columns"):
        df[col] = df[col].astype(str).str.strip().str.lower()
    return df


def correct_inconsistent_data(df):
    if 'release_date' in df.columns:
        df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
    df = df[(df['budget'].isna()) | (df['budget'] >= 0)]
    df = df[(df['revenue'].isna()) | (df['revenue'] >= 0)]
    df = df[(df['runtime'].isna()) | (df['runtime'] >= 0)]
    return df


def normalize_columns(df):
    columns_to_split = [
        'genres', 'spoken_languages', 'production_companies', 'production_countries',
        'cast', 'director', 'director_of_photography', 'writers', 'producers', 'music_composer'
    ]
    for col in tqdm(columns_to_split, desc="Normalizing columns"):
        if col in df.columns:
            df[col] = df[col].str.strip().str.split(",")
    return df


def remove_duplicates(df):
    df = df.drop_duplicates(subset=['title', 'release_date'], keep='first')
    return df


cleaned_data = (
    new_data
    .pipe(lambda df: tqdm.pandas(desc="Handling missing values") or handle_missing_values(df))
    .pipe(lambda df: tqdm.pandas(desc="Standardizing text") or standardize_text_columns(df, textual_columns))
    .pipe(lambda df: tqdm.pandas(desc="Correcting inconsistent data") or correct_inconsistent_data(df))
    .pipe(lambda df: tqdm.pandas(desc="Normalizing columns") or normalize_columns(df))
    .pipe(lambda df: tqdm.pandas(desc="Removing duplicates") or remove_duplicates(df))
)

cleaned_data = cleaned_data.drop(columns=['poster_path'], errors='ignore')

client = MongoClient('mongodb://localhost:27017/')
db = client['MoviesDB']
collection = db['movies']
collection.drop()

records = cleaned_data.to_dict(orient='records')

batch_size = 1000
for i in tqdm(range(0, len(records), batch_size), desc="Inserting records to MongoDB"):
    batch = records[i:i+batch_size]
    collection.insert_many(batch)

print(f"Data successfully imported to MongoDB.")
