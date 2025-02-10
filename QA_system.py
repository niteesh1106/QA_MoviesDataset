from Question_parser import parse_question
from Query_generator import QueryGenerator


def main():
    generator = QueryGenerator(password="bigdata612")
    print("Welcome to the Movie QA System! Type 'exit' to quit.\n")

    while True:
        question = input("You: ")
        if question.lower() in ['exit', 'quit']:
            break

        intent, entities = parse_question(question)
        if intent == 'Unknown':
            print("Bot: I'm sorry, I couldn't understand your question.")
            continue

        response = generator.get_response(intent, entities)
        print(f"Bot: {response}")

    generator.close()
    print("Goodbye!")


if __name__ == '__main__':
    main()
