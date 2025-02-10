# Question_parser.py
import spacy
import re
import warnings

warnings.filterwarnings("ignore", category=FutureWarning, module="thinc.shims.pytorch")


# Load the transformer-based English model
nlp = spacy.load('en_core_web_trf')

# Define intent patterns
INTENT_PATTERNS = [
    {
        'intent': 'FindDirector',
        'pattern': r'who\s+(directed|is the director of)\s+(?P<Movie>.+)',
        'entities': ['Movie']
    },
    {
        'intent': 'FindActors',
        'pattern': r'who\s+(acted in|starred in|are the actors in)\s+(?P<Movie>.+)',
        'entities': ['Movie']
    },
    {
        'intent': 'FindMoviesByGenre',
        'pattern': r'(which|what)\s+movies\s+(are|belong to|fall under|classified as)\s+(?P<Genre>.+)\s+genre',
        'entities': ['Genre']
    },
    {
        'intent': 'FindMoviesByDirector',
        'pattern': r'which movies\s+(did|were)\s+(?P<Person>.+)\s+(direct|directed)',
        'entities': ['Person']
    },
    {
        'intent': 'FindMoviesByLanguage',
        'pattern': r'which movies\s+(are in|speak|use the language)\s+(?P<Language>.+)',
        'entities': ['Language']
    },
    {
        'intent': 'FindMoviesByCompany',
        'pattern': r'which movies\s+(are produced by|were made by|are from)\s+(?P<Company>.+)',
        'entities': ['Company']
    },
    {
        'intent': 'FindMoviesByCountry',
        'pattern': r'which movies\s+(were produced in|are from the country of|originated in)\s+(?P<Country>.+)',
        'entities': ['Country']
    },
    {
        'intent': 'FindMusicComposer',
        'pattern': r'who\s+(composed the music for|is the music composer of)\s+(?P<Movie>.+)',
        'entities': ['Movie']
    },
    {
        'intent': 'FindDOP',
        'pattern': r'who\s+(was the director of photography for|is the DOP of)\s+(?P<Movie>.+)',
        'entities': ['Movie']
    },
    {
        'intent': 'FindRevenue',
        'pattern': r'what\s+is the revenue of\s+(?P<Movie>.+)',
        'entities': ['Movie']
    },
    {
        'intent': 'FindMoviesByActor',
        'pattern': r'which movies\s+(did|were)\s+(?P<Person>.+)\s+(act in|star in)',
        'entities': ['Person']
    },
    {
        'intent': 'FindLanguagesOfMovie',
        'pattern': r'what\s+languages\s+does\s+(?P<Movie>.+)\s+have',
        'entities': ['Movie']
    },
    {
        'intent': 'FindCompanyOfMovie',
        'pattern': r'which\s+company\s+produced\s+(?P<Movie>.+)',
        'entities': ['Movie']
    },
    {
        'intent': 'FindCountryOfMovie',
        'pattern': r'which\s+country\s+produced\s+(?P<Movie>.+)',
        'entities': ['Movie']
    },
    {
        'intent': 'FindAllDetails',
        'pattern': r'give\s+all\s+details\s+of\s+(?P<Movie>.+)',
        'entities': ['Movie']
    },
    {
        'intent': 'TopMoviesByGenre',
        'pattern': r'what are the top 20 movies in each genre',
        'entities': []
    },
    {
        'intent': 'TopSuccessfulActors',
        'pattern': r'who are the top 5 most successful actors',
        'entities': []
    },
    {
        'intent': 'SuccessfulGenresByYear',
        'pattern': r'which genres were most successful in (?P<Year>\d{4})',
        'entities': ['Year']
    },
    {
        'intent': 'TopDirectorsByRating',
        'pattern': r'who are the top 10 directors by average rating',
        'entities': []
    },
    {
        'intent': 'LanguageSuccess',
        'pattern': r'which languages have the highest-rated movies',
        'entities': []
    },
    {
        'intent': 'TopCompaniesBySuccess',
        'pattern': r'which production companies have the most successful movies',
        'entities': []
    },
    {
        'intent': 'RevenueTopGenres',
        'pattern': r'which genres have generated the most revenue',
        'entities': []
    },
    {
        'intent': 'TopMoviesByCountry',
        'pattern': r'what are the highest-grossing movies by country',
        'entities': []
    },
    {
        'intent': 'YearlyRevenueTrend',
        'pattern': r'what is the yearly box office trend',
        'entities': []
    },
    {
        'intent': 'GenrePopularityTrend',
        'pattern': r'how has genre popularity changed over time',
        'entities': []
    }
]

def parse_question(question):
    question = question.lower().strip('?').strip()
    for pattern_info in INTENT_PATTERNS:
        match = re.match(pattern_info['pattern'], question)
        if match:
            intent = pattern_info['intent']
            entities = {entity: match.group(entity).strip().lower() for entity in pattern_info['entities']}
            return intent, entities

    # Fallback to spaCy NER
    doc = nlp(question)
    entities = {}
    for ent in doc.ents:
        if ent.label_ == 'PERSON':
            entities['Person'] = ent.text
        elif ent.label_ in ['WORK_OF_ART', 'MOVIE']:
            entities['Movie'] = ent.text
        elif ent.label_ == 'LANGUAGE':
            entities['Language'] = ent.text
        elif ent.label_ == 'ORG':
            entities['Company'] = ent.text
        elif ent.label_ == 'GPE':
            entities['Country'] = ent.text
        elif ent.label_ == 'NORP':
            entities['Genre'] = ent.text
    intent = 'Unknown' if not entities else 'FindInformation'
    return intent, entities


# Example usage
if __name__ == '__main__':
    question = input("Enter your question: ")
    intent, entities = parse_question(question)
    print(f"Intent: {intent}")
    print(f"Entities: {entities}")
