from pymongo import MongoClient
from neo4j import GraphDatabase

# Neo4j Configuration
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "bigdata612"

# MongoDB Setup
client = MongoClient("mongodb://localhost:27017/")
db = client['MoviesDB']
movies_collection = db['movies']
triples_collection = db['triples']


# Neo4j Setup
class Neo4jManager:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def execute_query(self, query, parameters=None):
        with self.driver.session() as session:
            return session.run(query, parameters)


neo4j_manager = Neo4jManager(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

# Fetch movies and triples data from MongoDB
movies = list(movies_collection.find())
triples = list(triples_collection.find({}, {"_id": 0, "subject": 1, "predicate": 1, "object": 1, "movie_id": 1}))

# Check if data exists
if not movies:
    print("No movies found in MongoDB. Ensure the movies collection has data.")
    exit()

if not triples:
    print("No triples found in MongoDB. Ensure the triples collection has data.")
    exit()


# Import Movies and Relationships into Neo4j
def import_movies_and_relationships(movies, neo4j_manager):
    print("Importing movies and relationships into Neo4j...")

    for movie in movies:
        # Movie Node
        movie_id = movie.get("id")
        title = movie.get("title")
        release_date = movie.get("release_date")
        budget = movie.get("budget")
        runtime = movie.get("runtime")
        vote_average = movie.get("vote_average")
        status = movie.get("status")
        revenue = movie.get("revenue")
        original_title = movie.get("original_title")
        imdb_rating = movie.get("imdb_rating")
        imdb_votes = movie.get("imdb_votes")
        original_language = movie.get("original_language")
        popularity = movie.get("popularity")

        # Create Movie Node
        query_movie = """
        MERGE (m:Movie {id: $movie_id})
        SET m.title = $title,
            m.release_date = $release_date,
            m.budget = $budget,
            m.runtime = $runtime,
            m.vote_average = $vote_average,
            m.status = $status,
            m.revenue = $revenue,
            m.original_title = $original_title,
            m.imdb_rating = $imdb_rating,
            m.imdb_votes = $imdb_votes,
            m.original_language = $original_language,
            m.popularity = $popularity
        """
        neo4j_manager.execute_query(query_movie, parameters={
            "movie_id": movie_id,
            "title": title,
            "release_date": release_date,
            "budget": budget,
            "runtime": runtime,
            "vote_average": vote_average,
            "status": status,
            "revenue": revenue,
            "original_title": original_title,
            "imdb_rating": imdb_rating,
            "imdb_votes": imdb_votes,
            "original_language": original_language,
            "popularity": popularity
        })

        # Process Relationships
        relationships = [
            ("genres", "Genre", "BELONGS_TO_GENRE"),
            ("spoken_languages", "Language", "SPOKEN_IN"),
            ("production_companies", "Company", "PRODUCTION_COMPANY"),
            ("production_countries", "Country", "PRODUCED_IN"),
            ("cast", "Person", "ACTED_IN"),
            ("director", "Person", "DIRECTED_BY"),
            ("writers", "Person", "WRITTEN_BY"),
            ("producers", "Person", "PRODUCED_BY"),
            ("music_composer", "Person", "COMPOSED_BY"),
            ("director_of_photography", "Person", "DOP_BY")
        ]

        for field, label, rel_type in relationships:
            entities = movie.get(field, [])
            if not isinstance(entities, list):
                entities = [entities]

            for entity in entities:
                if not entity:
                    continue

                query_entity = f"""
                MERGE (n:{label} {{name: $entity_name}})
                MERGE (m:Movie {{id: $movie_id}})
                MERGE (m)-[:{rel_type}]->(n)
                """
                neo4j_manager.execute_query(query_entity, parameters={
                    "entity_name": entity,
                    "movie_id": movie_id
                })

    print("Movies and relationships successfully imported into Neo4j.")


# Import Triples into Neo4j using APOC
def import_triples(triples, neo4j_manager):
    print("Importing triples into Neo4j...")
    query = """
    CALL apoc.periodic.iterate(
      'UNWIND $triples AS triple RETURN triple',
      'MERGE (s:Entity {name: triple.subject})
       MERGE (o:Entity {name: triple.object}) 
       MERGE (m:Movie {id: triple.movie_id})
       MERGE (s)-[r:RELATION {type: triple.predicate}]->(o)
       MERGE (m)-[:HAS_SUBJECT]->(s)
       MERGE (m)-[:HAS_OBJECT]->(o)',
      {batchSize: 1000, parallel: true, params: {triples: $triples}}
    )
    """
    neo4j_manager.execute_query(query, parameters={"triples": triples})
    print("Triples successfully imported into Neo4j.")


# Execute the import process
try:
    import_movies_and_relationships(movies, neo4j_manager)
    import_triples(triples, neo4j_manager)
finally:
    neo4j_manager.close()
