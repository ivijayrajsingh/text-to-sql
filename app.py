import pandas as pd
import json
import re
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from pymongo import MongoClient
from bson.objectid import ObjectId
import os
from openai import OpenAI
import tiktoken

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
llm = OpenAI(api_key=OPENAI_API_KEY)
encoding = tiktoken.encoding_for_model("gpt-4")

# Function to load data from MongoDB
def load_data_from_mongo(collection_name, code_id):
    """Load specific code from MongoDB collection."""
    collection = db[collection_name]
    result = collection.find_one({"CodeId": ObjectId(code_id)})
    return result

# Function to extract JSON from OpenAI response
def extract_json_to_table(response):
    """
    Extract JSON content from the OpenAI response and convert it into proper format.
    """
    try:
        # Use regex to find the JSON content
        json_match = re.search(r'```json\n(.*?)\n```', response, re.DOTALL)
        if json_match:
            raw_json_str = json_match.group(1)
            parsed_json = json.loads(raw_json_str)
            return parsed_json
        else:
            print("No JSON found in the response.")
            return None
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None

# Function to save response to MongoDB
def save_to_mongo(collection_name, code_id, response_json):
    """Save the extracted details to MongoDB."""
    # Check if the collection exists; if not, create it
    if collection_name not in db.list_collection_names():
        db.create_collection(collection_name)
        print(f"Collection '{collection_name}' created.")
    
    collection = db[collection_name]
    
    # Prepare the document to be inserted
    document = {
        "code_id": code_id,
        "response": response_json,
        "created_at": datetime.utcnow()
    }
    
    result = collection.insert_one(document)
    return result.inserted_id

def extract_and_save_data_lineage(code_id, save=False, retries=3):
    """
    Extracts the data lineage from the provided code_id and optionally saves it to MongoDB.
    Uses responses from OpenAI for large outputs and retries in case of transient errors.

    Parameters:
        code_id (str): The unique identifier for the SQL code in MongoDB.
        save (bool): Whether to save the extracted data lineage to MongoDB (default: False).
        retries (int): Number of retry attempts in case of a timeout.

    Returns:
        dict: A status dictionary with the result of the operation.
    """
    job_activity = load_data_from_mongo('JobActivityDetails', code_id)
    if not job_activity:
        return {"status": "error", "message": "Code not found"}

    sql_statement = job_activity.get("Code")
    if not sql_statement:
        return {"status": "error", "message": "No SQL statement found"}

    sql_prompt = f"""
    Generate a JSON data lineage for the following SQL query. Include the following fields for each column:
    - SourceDatabaseName
    - SourceTableName
    - SourceColumnName
    - TargetDatabaseName
    - TargetTableName
    - TargetColumnName
    
    SQL query:
    ```sql
    {sql_statement}
    ```
    """
    
    # print(f"Processing SQL Statement: {sql_statement}")

    attempts = 0
    while attempts < retries:
        try:
            # Send request to OpenAI API
            response = llm.chat.completions.create(
                model="gpt-4",
                messages=[{"role": "user", "content": sql_prompt}],
                temperature=0,
                timeout=120,  # Increased timeout
                max_tokens=4096  # Adjust token limit for larger responses
            )

            # Extract the response content
            response_content = response.choices[0].message.content

            # Attempt to parse JSON from response
            try:
                parsed_json = extract_json_to_table(response_content)
            except Exception as e:
                print(f"Error parsing JSON: {e}")
                parsed_json = None

            if not parsed_json:
                return {"status": "error", "message": "Failed to parse response"}

            if save:
                # Save the parsed JSON to MongoDB if `save=True`
                document_id = save_to_mongo('DataLineage', code_id, parsed_json)
                return {
                    "status": "success",
                    "message": "Data lineage saved successfully",
                    "document_id": str(document_id),
                    "formatted_json": parsed_json
                }

            return {
                "status": "success",
                "message": "Data lineage extracted successfully",
                "formatted_json": parsed_json,
                "raw_response": response_content
            }

        except Exception as e:
            print(f"Attempt {attempts + 1} failed: {e}")
            if "timeout" in str(e).lower():
                attempts += 1
                time.sleep(2)  # Wait before retrying
            else:
                return {"status": "error", "message": str(e)}

    return {"status": "error", "message": "Request timed out after multiple attempts."}


# Flask route for getDetails
@app.route('/getDetails', methods=['POST'])
def get_details():
    """Extracts details from the provided code_id and saves it to MongoDB."""
    data = request.json
    code_id = data.get("code_id")
    
    if not code_id:
        return jsonify({"status": "error", "message": "code_id is required"}), 400
    
    # Load SQL statement from MongoDB
    job_activity = load_data_from_mongo('JobActivityDetails', code_id)
    if not job_activity:
        return jsonify({"status": "error", "message": "Code not found"}), 404

    sql_statement = job_activity.get("Code")
    if not sql_statement:
        return jsonify({"status": "error", "message": "No SQL statement found"}), 404

    # Create the prompt for OpenAI
    sql_prompt = f"""
    I have an SQL statement, and I would like to extract specific details from it. Can you provide the following information in JSON format:

    1. **Full Table Names**: Include schema and table name (e.g., `schema_name.table_name`) if available.
    2. **Column Names**: List all the column names used in the statement.
    3. **Database Name**: Extract the database name if specified in the statement.

    Format the output in the following JSON structure:

    {{
      "database_name": "database_name_here",
      "tables": [
        {{
          "table_name": "schema_name.table_name",
          "columns": ["column1", "column2", "column3"]
        }}
      ]
    }}

    If a particular detail (e.g., database name or schema) is not specified in the statement, set its value to `null`.
    **SQL Statement**:
    {sql_statement}
    """

    try:
        # Query OpenAI
        response = llm.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "user", "content": sql_prompt}],
            temperature=0,
            timeout=30
        )
        response_content = response.choices[0].message.content
        parsed_json = extract_json_to_table(response_content)
        
        if not parsed_json:
            return jsonify({"status": "error", "message": "Failed to parse response"}), 500
        
        # Save the extracted data to MongoDB
        document_id = save_to_mongo('CodeSummary', code_id, parsed_json)
        
        return jsonify({
            "status": "success",
            "message": "Data saved successfully",
            "document_id": str(document_id),
            "formatted_json": parsed_json
        }), 200

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/get_data_lineage', methods=['POST'])
def get_data_lineage():
    """
    Extracts data lineage from the provided code_id and optionally saves it to MongoDB.
    """
    data = request.json
    code_id = data.get("code_id")
    save = data.get("save", False)  # Optional parameter, defaults to False

    if not code_id:
        return jsonify({"status": "error", "message": "code_id is required"}), 400

    # Call the extract_and_save_data_lineage function
    try:
        result = extract_and_save_data_lineage(code_id=code_id, save=save)
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/')
def home():
    return 'API is up and running!'

# if __name__ == '__main__':
#     app.run(debug=True, host='0.0.0.0', port=5000)
