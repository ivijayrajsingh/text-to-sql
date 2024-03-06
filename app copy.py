from flask import Flask
import langchain_experimental
import openai
import pandas
import pandas as pd
import ipywidgets as widgets
from langchain_experimental.agents.agent_toolkits.csv.base import create_csv_agent
from langchain.llms import OpenAI
from flask import Flask, request, jsonify
import langchain_experimental
import os
app = Flask(__name__)

import tempfile
df = pd.read_csv('text_to_pandas.csv')

os.environ["OPENAI_API_KEY"] = ""
def load_agent(df):
    """Initialize and return the agent with the DataFrame."""
    # Save the DataFrame to a temporary CSV file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp_file:
        df.to_csv(temp_file.name, index=False)
        temp_file_path = temp_file.name
    
    # Now you can pass the CSV file path to create_csv_agent
    agent = create_csv_agent(OpenAI(temperature=0), temp_file_path, verbose=False)
    
    return agent

agent = load_agent(df)

@app.route('/')
def hello_world():
    print("langchain_experimental version:", langchain_experimental.__version__)
    print("openai version:", openai.__version__)
    print("pandas version:", pandas.__version__)
    return f'Hello, World! { langchain_experimental.__version__, openai.__version__, pandas.__version__}'


@app.route('/ask', methods=['POST'])
def ask_question():
    # Get data from the request
    request_data = request.get_json()
    api_key = request_data.get('api_key')
    question = request_data.get('question')

    # Check if API key is valid (You may want to implement proper authentication logic here)
    if api_key != 'your_api_key':
        return jsonify({'error': 'Invalid API key'}), 401

    # Call your agent to get the answer (replace this with your actual agent call)
    answer = agent.run(question)


    # Return the answer
    return jsonify({'answer': answer})

# if __name__=='__main__':
#     app.run()