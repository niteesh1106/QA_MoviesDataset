from neo4j import GraphDatabase


class QueryGenerator:
    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="bigdata612"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def execute_query(self, query, parameters):
        with self.driver.session() as session:
            result = session.run(query, parameters)
            return [record for record in result]

    def generate_query(self, intent, entities):
        if intent == 'FindDirector':
            movie_title = entities.get('Movie')
            query = """
            MATCH (m:Movie)<-[:DIRECTED_BY]->(d:Person)
            WHERE toLower(m.title) = toLower($movie_title)
            RETURN d.name AS director
            """
            parameters = {'movie_title': movie_title}
            return query, parameters

        elif intent == 'FindActors':
            movie_title = entities.get('Movie')
            query = """
            MATCH (a:Person)<-[:ACTED_IN]->(m:Movie)
            WHERE toLower(m.title) = toLower($movie_title)
            RETURN a.name AS actor
            """
            parameters = {'movie_title': movie_title}
            return query, parameters

        elif intent == 'FindMoviesByGenre':
            genre_name = entities.get('Genre')
            query = """
            MATCH (m:Movie)<-[:BELONGS_TO_GENRE]->(g:Genre)
            WHERE toLower(g.name) = toLower($genre_name)
            RETURN m.title AS movie_title
            """
            parameters = {'genre_name': genre_name}
            return query, parameters

        elif intent == 'FindMoviesByDirector':
            director_name = entities.get('Person')
            query = """
            MATCH (d:Person)<-[:DIRECTED_BY]-(m:Movie)
            WHERE toLower(d.name) = toLower($director_name)
            RETURN m.title AS movie_title
            """
            parameters = {'director_name': director_name}
            return query, parameters

        elif intent == 'FindMoviesByLanguage':
            language_name = entities.get('Language')
            query = """
            MATCH (m:Movie)<-[:SPOKEN_IN]->(l:Language)
            WHERE toLower(l.name) = toLower($language_name)
            RETURN m.title AS movie_title
            """
            parameters = {'language_name': language_name}
            return query, parameters

        elif intent == 'FindMoviesByCompany':
            company_name = entities.get('Company')
            query = """
            MATCH (m:Movie)<-[:PRODUCTION_COMPANY]->(c:Company)
            WHERE toLower(c.name) = toLower($company_name)
            RETURN m.title AS movie_title
            """
            parameters = {'company_name': company_name}
            return query, parameters

        elif intent == 'FindMoviesByCountry':
            country_name = entities.get('Country')
            query = """
            MATCH (m:Movie)<-[:PRODUCED_IN]->(co:Country)
            WHERE toLower(co.name) = toLower($country_name)
            RETURN m.title AS movie_title
            """
            parameters = {'country_name': country_name}
            return query, parameters

        elif intent == 'FindMusicComposer':
            movie_title = entities.get('Movie')
            query = """
            MATCH (m:Movie)<-[:COMPOSED_BY]->(mc:Person)
            WHERE toLower(m.title) = toLower($movie_title)
            RETURN mc.name AS music_composer
            """
            parameters = {'movie_title': movie_title}
            return query, parameters

        elif intent == 'FindDOP':
            movie_title = entities.get('Movie')
            query = """
            MATCH (m:Movie)<-[:DOP_BY]->(dop:Person)
            WHERE toLower(m.title) = toLower($movie_title)
            RETURN dop.name AS director_of_photography
            """
            parameters = {'movie_title': movie_title}
            return query, parameters

        elif intent == 'FindRevenue':
            movie_title = entities.get('Movie')
            query = """
            MATCH (m:Movie)
            WHERE toLower(m.title) = toLower($movie_title)
            RETURN m.revenue AS revenue
            """
            parameters = {'movie_title': movie_title}
            return query, parameters

        elif intent == 'FindMoviesByActor':
            actor_name = entities.get('Person')
            query = """
            MATCH (p:Person)<-[:ACTED_IN]->(m:Movie)
            WHERE toLower(p.name) = toLower($actor_name)
            RETURN m.title AS movie_title
            """
            parameters = {'actor_name': actor_name}
            return query, parameters

        elif intent == 'FindLanguagesOfMovie':
            movie_title = entities.get('Movie')
            query = """
            MATCH (m:Movie)<-[:SPOKEN_IN]->(l:Language)
            WHERE toLower(m.title) = toLower($movie_title)
            RETURN l.name AS language
            """
            parameters = {'movie_title': movie_title}
            return query, parameters

        elif intent == 'FindCompanyOfMovie':
            movie_title = entities.get('Movie')
            query = """
            MATCH (m:Movie)<-[:PRODUCTION_COMPANY]->(c:Company)
            WHERE toLower(m.title) = toLower($movie_title)
            RETURN c.name AS company
            """
            parameters = {'movie_title': movie_title}
            return query, parameters

        elif intent == 'FindCountryOfMovie':
            movie_title = entities.get('Movie')
            query = """
            MATCH (m:Movie)<-[:PRODUCED_IN]->(co:Country)
            WHERE toLower(m.title) = toLower($movie_title)
            RETURN co.name AS country
            """
            parameters = {'movie_title': movie_title}
            return query, parameters

        elif intent == 'FindAllDetails':
            movie_title = entities.get('Movie')
            query = """
            MATCH (m:Movie)
            WHERE toLower(m.title) = toLower($movie_title)
            RETURN m.title AS title, m.release_date AS release_date, m.budget AS budget, 
                    m.runtime AS runtime, m.vote_average AS vote_average, m.status AS status, 
                    m.revenue AS revenue, m.original_language AS original_language
            """
            parameters = {'movie_title': movie_title}
            return query, parameters

        elif intent == 'TopMoviesByGenre':
            query = """
                MATCH (m:Movie)-[:BELONGS_TO_GENRE]->(g:Genre)
                RETURN g.name AS genre, m.title AS movie, m.vote_average AS rating
                ORDER BY g.name, m.vote_average DESC
                LIMIT 20
                """
            parameters = {}
            return query, parameters

        elif intent == 'TopSuccessfulActors':
            query = """
                MATCH (a:Person)-[:ACTED_IN]->(m:Movie)
                WHERE m.vote_average > 7.5
                RETURN a.name AS actor, COUNT(m) AS successful_movies
                ORDER BY successful_movies DESC
                LIMIT 5
                """
            parameters = {}
            return query, parameters

        elif intent == 'SuccessfulGenresByYear':
            year = entities.get('Year')
            query = """
                MATCH (m:Movie)-[:BELONGS_TO_GENRE]->(g:Genre)
                WHERE m.release_date STARTS WITH $year
                RETURN g.name AS genre, AVG(m.vote_average) AS average_rating
                ORDER BY average_rating DESC
                """
            parameters = {'year': year}
            return query, parameters

        elif intent == 'TopDirectorsByRating':
            query = """
                MATCH (d:Person)-[:DIRECTED_BY]->(m:Movie)
                RETURN d.name AS director, AVG(m.vote_average) AS avg_rating
                ORDER BY avg_rating DESC
                LIMIT 10
                """
            parameters = {}
            return query, parameters

        elif intent == 'LanguageSuccess':
            query = """
                MATCH (m:Movie)-[:SPOKEN_IN]->(l:Language)
                RETURN l.name AS language, AVG(m.vote_average) AS average_rating
                ORDER BY average_rating DESC
                """
            parameters = {}
            return query, parameters

        elif intent == 'TopCompaniesBySuccess':
            query = """
                MATCH (m:Movie)-[:PRODUCTION_COMPANY]->(c:Company)
                WHERE m.vote_average > 7.5
                RETURN c.name AS company, COUNT(m) AS successful_movies
                ORDER BY successful_movies DESC
                """
            parameters = {}
            return query, parameters

        elif intent == 'RevenueTopGenres':
            query = """
                MATCH (m:Movie)-[:BELONGS_TO_GENRE]->(g:Genre)
                RETURN g.name AS genre, SUM(m.revenue) AS total_revenue
                ORDER BY total_revenue DESC
                """
            parameters = {}
            return query, parameters

        elif intent == 'TopMoviesByCountry':
            query = """
                MATCH (m:Movie)-[:PRODUCED_IN]->(co:Country)
                RETURN co.name AS country, m.title AS movie, m.revenue AS revenue
                ORDER BY co.name, revenue DESC
                """
            parameters = {}
            return query, parameters

        elif intent == 'YearlyRevenueTrend':
            query = """
                MATCH (m:Movie)
                RETURN substring(m.release_date, 0, 4) AS year, SUM(m.revenue) AS total_revenue
                ORDER BY year ASC
                """
            parameters = {}
            return query, parameters

        elif intent == 'GenrePopularityTrend':
            query = """
                MATCH (m:Movie)-[:BELONGS_TO_GENRE]->(g:Genre)
                RETURN g.name AS genre, substring(m.release_date, 0, 4) AS year, COUNT(m) AS movie_count
                ORDER BY year ASC, movie_count DESC
                """
            parameters = {}
            return query, parameters

        else:
            return None, None

    def get_response(self, intent, entities):
        query, parameters = self.generate_query(intent, entities)
        if not query:
            return "I'm sorry, I couldn't understand your question."

        results = self.execute_query(query, parameters)
        if not results:
            return "I'm sorry, I couldn't find any results."

        if intent == 'FindDirector':
            directors = [record['director'] for record in results]
            movie_title = entities.get('Movie').title()
            directors_list = ', '.join(director.title() for director in directors)
            return f"The director of '{movie_title}' is {directors_list}."

        elif intent == 'FindActors':
            actors = [record['actor'] for record in results]
            movie_title = entities.get('Movie').title()
            actors_list = ', '.join(actor.title() for actor in actors)
            return f"The actors in '{movie_title}' are: {actors_list}."

        elif intent == 'FindMoviesByGenre':
            movies = [record['movie_title'] for record in results]
            genre_name = entities.get('Genre').title()
            movies_list = ', '.join(movies[:10])  # Limit to first 10 movies
            return f"Movies in the '{genre_name}' genre include: {movies_list}."

        elif intent == 'FindMoviesByDirector':
            movies = [record['movie_title'] for record in results]
            director_name = entities.get('Person').title()
            movies_list = ', '.join(movies[:10])  # Limit to first 10 movies
            return f"Movies directed by {director_name} include: {movies_list}."

        elif intent == 'FindMoviesByLanguage':
            movies = [record['movie_title'] for record in results]
            language_name = entities.get('Language').title()
            movies_list = ', '.join(movies)
            return f"Movies in the language '{language_name}' include: {movies_list}."

        elif intent == 'FindMoviesByCompany':
            movies = [record['movie_title'] for record in results]
            company_name = entities.get('Company').title()
            movies_list = ', '.join(movies)
            return f"Movies produced by '{company_name}' include: {movies_list}."

        elif intent == 'FindMoviesByCountry':
            movies = [record['movie_title'] for record in results]
            country_name = entities.get('Country').title()
            movies_list = ', '.join(movies)
            return f"Movies produced in '{country_name}' include: {movies_list}."

        elif intent == 'FindMusicComposer':
            composers = [record['music_composer'] for record in results]
            movie_title = entities.get('Movie').title()
            composers_list = ', '.join(composer.title() for composer in composers)
            return f"The music composer(s) for '{movie_title}' is/are {composers_list}."

        elif intent == 'FindDOP':
            dops = [record['director_of_photography'] for record in results]
            movie_title = entities.get('Movie').title()
            dops_list = ', '.join(dop.title() for dop in dops)
            return f"The director(s) of photography for '{movie_title}' is/are {dops_list}."

        elif intent == 'FindRevenue':
            revenues = [record['revenue'] for record in results]
            movie_title = entities.get('Movie').title()
            if revenues and revenues[0] > 0.0:
                return f"The revenue of '{movie_title}' is {revenues[0]}."
            else:
                return f"The revenue of '{movie_title}' is Unknown."

        elif intent == 'FindMoviesByActor':
            movies = [record['movie_title'] for record in results]
            actor_name = entities.get('Person').title()
            movies_list = ', '.join(movies)
            return f"Movies acted in by {actor_name} include: {movies_list}."

        elif intent == 'FindLanguagesOfMovie':
            languages = [record['language'] for record in results]
            movie_title = entities.get('Movie').title()
            languages_list = ', '.join(language.title() for language in languages)
            return f"The languages spoken in '{movie_title}' are: {languages_list}."

        elif intent == 'FindCompanyOfMovie':
            companies = [record['company'] for record in results]
            movie_title = entities.get('Movie').title()
            companies_list = ', '.join(company.title() for company in companies)
            return f"The production company/companies for '{movie_title}' include: {companies_list}."

        elif intent == 'FindCountryOfMovie':
            countries = [record['country'] for record in results]
            movie_title = entities.get('Movie').title()
            countries_list = ', '.join(country.title() for country in countries)
            return f"The country/countries that produced '{movie_title}' include: {countries_list}."

        elif intent == 'FindAllDetails':
            details = results[0]
            return (
                f"Details of '{details['title']}':\n"
                f"- Release Date: {details['release_date']}\n"
                f"- Budget: {details['budget']}\n"
                f"- Runtime: {details['runtime']} minutes\n"
                f"- Vote Average: {details['vote_average']}\n"
                f"- Status: {details['status']}\n"
                f"- Revenue: {details['revenue'] if details['revenue'] > 0 else 'Unknown'}\n"
                f"- Original Language: {details['original_language']}"
            )

        elif intent == 'TopMoviesByGenre':
            genres = {}
            for record in results:
                genre = record['genre']
                movie = record['movie']
                rating = record['rating']
                if genre not in genres:
                    genres[genre] = []
                genres[genre].append(f"{movie} (Rating: {rating})")
            response = "\n".join([f"{genre}: {', '.join(movies)}" for genre, movies in genres.items()])
            return f"Top 20 movies by each genre:\n{response}"

        elif intent == 'TopSuccessfulActors':
            actors = [f"{record['actor']} ({record['successful_movies']} movies)" for record in results]
            return f"Top 5 most successful actors:\n" + "\n".join(actors)

        elif intent == 'SuccessfulGenresByYear':
            year = entities.get('Year')
            genres = [f"{record['genre']} (Avg. Rating: {record['average_rating']:.2f})" for record in results]
            return f"Most successful genres in {year}:\n" + "\n".join(genres)

        elif intent == 'TopDirectorsByRating':
            directors = [f"{record['director']} (Avg. Rating: {record['avg_rating']:.2f})" for record in results]
            return f"Top 10 directors by average movie rating:\n" + "\n".join(directors)

        elif intent == 'LanguageSuccess':
            languages = [f"{record['language']} (Avg. Rating: {record['average_rating']:.2f})" for record in results]
            return f"Languages with the highest-rated movies:\n" + "\n".join(languages)

        elif intent == 'TopCompaniesBySuccess':
            companies = [f"{record['company']} ({record['successful_movies']} successful movies)" for record in results]
            return f"Production companies with the most successful movies:\n" + "\n".join(companies)

        elif intent == 'RevenueTopGenres':
            genres = [f"{record['genre']} (Total Revenue: ${record['total_revenue']:,.2f})" for record in results]
            return f"Genres with the highest total revenue:\n" + "\n".join(genres)

        elif intent == 'TopMoviesByCountry':
            movies_by_country = {}
            for record in results:
                country = record['country']
                movie = f"{record['movie']} (${record['revenue']:,.2f})"
                if country not in movies_by_country:
                    movies_by_country[country] = []
                movies_by_country[country].append(movie)
            response = "\n".join([f"{country}: {', '.join(movies)}" for country, movies in movies_by_country.items()])
            return f"Highest-grossing movies by country:\n{response}"

        elif intent == 'YearlyRevenueTrend':
            trends = [f"{record['year']}: ${record['total_revenue']:,.2f}" for record in results]
            return f"Yearly box office revenue trend:\n" + "\n".join(trends)

        elif intent == 'GenrePopularityTrend':
            trends = {}
            for record in results:
                year = record['year']
                genre = record['genre']
                count = record['movie_count']
                if year not in trends:
                    trends[year] = []
                trends[year].append(f"{genre} ({count} movies)")
            response = "\n".join([f"{year}: {', '.join(genres)}" for year, genres in trends.items()])
            return f"Genre popularity over time:\n{response}"

        else:
            return "I'm sorry, I couldn't generate a response."


# Example usage
if __name__ == '__main__':
    generator = QueryGenerator(password="bigdata612")
    intent = 'FindMoviesByLanguage'
    entities = {'Language': 'spanish'}
    response = generator.get_response(intent, entities)
    print(response)
    generator.close()
