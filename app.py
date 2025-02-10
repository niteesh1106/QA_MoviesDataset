from flask import Flask, request, render_template
from Question_parser import parse_question
from Query_generator import QueryGenerator

app = Flask(__name__)
generator = QueryGenerator(password="bigdata612")


@app.route('/', methods=['GET', 'POST'])
def home():
    if request.method == 'POST':
        question = request.form['question']
        intent, entities = parse_question(question)
        if intent == 'Unknown':
            response = "I'm sorry, I couldn't understand your question."
        else:
            response = generator.get_response(intent, entities)
        return render_template('index.html', question=question, response=response)
    return render_template('index.html')


@app.route('/shutdown')
def shutdown():
    generator.close()
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()
    return 'Server shutting down...'


if __name__ == '__main__':
    app.run(debug=True, port=8081)

