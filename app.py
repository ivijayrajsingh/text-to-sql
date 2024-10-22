import pandas as pd
import json
from flask import Flask, request, jsonify
from flask_cors import CORS
from pymongo import MongoClient
from bson.objectid import ObjectId  # Use bson from pymongo
from langchain.llms import OpenAI
import os

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# MongoDB Configuration
os.environ["MONGO_URI"] = os.environ.get("MONGO_URI")
MONGO_URI = os.environ.get("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client['GooseDB']

# OpenAI API Key Configuration
os.environ["OPENAI_API_KEY"] = os.environ.get("OPENAI_API_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

# Initialize OpenAI object with LangChain
llm = OpenAI(api_key=OPENAI_API_KEY)

def get_sql_operation_by_job_id(job_id):
    """Fetch the SQL operation corresponding to a given job ID."""
    collection = db['JobActivityDetails']
    job_id = ObjectId(job_id)
    try:
        # Query to find the matching job record
        record = collection.find_one({"JobId": job_id}, {"_id": 0, "Code": 1})

        if record and "Code" in record:
            return record["Code"], None
        else:
            return None, "No SQL operation found for the given job ID."

    except Exception as e:
        return None, f"Error retrieving SQL operation: {e}"

@app.route('/generate-lineage', methods=['POST'])
def generate_lineage():
    """Handle user requests to generate data lineage."""
    request_data = request.get_json()

    job_id = request_data.get('job_id')

    if not job_id:
        return jsonify({"error": "job_id is required"}), 400

    # Fetch the SQL operation using the job ID
    sql_operation, error = get_sql_operation_by_job_id(job_id)

    if error:
        return jsonify({"error": error}), 404

    try:
        # Create a prompt for generating data lineage
        prompt = f"Generate a data lineage table based on the following query:\n\n{sql_operation}. Make sure you provide the detail of source column, source database, target column, target database and transformation done."
        raw_response = llm.predict(prompt)

        # Parse the response into a structured JSON format
        table_data = parse_lineage_response(raw_response)

        return jsonify({"lineage": table_data}), 200

    except Exception as e:
        return jsonify({"error": f"Error generating lineage: {str(e)}"}), 500

def parse_lineage_response(response):
    """Parse the response from OpenAI into structured JSON."""
    lines = [line.strip() for line in response.split('\n') if line.strip()]

    headers = [header.strip() for header in lines[0].split('|') if header.strip()]

    rows = []
    for line in lines[2:]:  # Skip the header and separator line
        values = [value.strip() for value in line.split('|') if value.strip()]
        row = dict(zip(headers, values))
        rows.append(row)

    return rows

@app.route('/')
def home():
    return 'API is up and running!'

# if __name__ == '__main__':
#     app.run(debug=True, host='0.0.0.0', port=5000)
