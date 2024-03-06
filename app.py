from flask import Flask, request, jsonify
from langchain_experimental.agents.agent_toolkits.csv.base import create_csv_agent
from langchain.llms import OpenAI
import pandas as pd
import os
import tempfile
from flask_cors import CORS, cross_origin

app = Flask(__name__)
CORS(app)

# Load DataFrame from CSV
df = pd.read_csv('text_to_pandas.csv')

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
    
    # Now you can pass the CSV file path to create_csv_agent
    agent = create_csv_agent(OpenAI(api_key=api_key, temperature=0), temp_file_path, verbose=False)
    
    return agent

# Load the agent
agent = load_agent(df)

@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route('/ask', methods=['POST'])
def ask_question():
    print('function called')
    # Get data from the request
    request_data = request.get_json()
    api_key = request_data.get('api_key')
    question = request_data.get('question')
    question = question+'. Give me descriptive answer.'

    # # Check if API key is valid
    if api_key != 'SAA':
        return jsonify({'error': 'Invalid API key'}), 401

    # Call the agent to get the answer
    answer = agent.run(question)
    print(answer)

    # Return the answer
    return jsonify({'answer': answer})

# if __name__ == '__main__':
#     app.run()
