from flask import Flask, request, jsonify
from langchain_experimental.agents.agent_toolkits.csv.base import create_csv_agent
from langchain.llms import OpenAI
import pandas as pd
import os
import tempfile
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Load DataFrame from CSV
df = pd.read_csv('text_to_pandas.csv')
df1 = pd.read_csv('Users_by_system_20240306.csv')

# Set OpenAI API key
os.environ["OPENAI_API_KEY"] = os.environ.get("OPENAI_API_KEY")
api_key = os.environ.get("OPENAI_API_KEY")

# Define function to load agent
def load_agent(df):
    """Initialize and return the agent with the DataFrame."""
    # Save the DataFrame to a temporary CSV file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp_file:
        df.to_csv(temp_file.name, index=False)
        temp_file_path = temp_file.name

    # Create agent with the dangerous code flag enabled
    agent = create_csv_agent(
        OpenAI(api_key=api_key, temperature=0),
        temp_file_path,
        verbose=False,
        allow_dangerous_code=True  # Enable potentially dangerous code execution
    )
    return agent

# Load the agents
agent = load_agent(df)
agent2 = load_agent(df1)

@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route('/ask', methods=['POST'])
def ask_question():
    print('function called')
    request_data = request.get_json()
    api_key = request_data.get('api_key')
    question = request_data.get('question')

    if api_key != 'SAA':
        return jsonify({'error': 'Invalid API key'}), 401

    answer = agent.run(question)
    print(answer)
    return jsonify({'answer': answer})

@app.route('/ask2', methods=['POST'])
def ask_question_route():
    print('function called2')
    request_data = request.get_json()
    api_key = request_data.get('api_key')
    question = request_data.get('question')

    if api_key != 'SAA':
        return jsonify({'error': 'Invalid API key'}), 401

    answer = agent2.run(question)
    print(answer)
    return jsonify({'answer': answer})

# if __name__ == '__main__':
#     app.run()
