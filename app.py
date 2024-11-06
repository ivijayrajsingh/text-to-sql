import pandas as pd
import json
from flask import Flask, request, jsonify
from flask_cors import CORS
from pymongo import MongoClient
from bson.objectid import ObjectId  # Use bson from pymongo
import os
from openai import OpenAI
import tiktoken
import json

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

# Load DataFrame from MongoDB
# Load DataFrame from MongoDB
def load_data_from_mongo(collection_name):
    """Load data from MongoDB collection into a DataFrame."""
    collection = db[collection_name]
    data = list(collection.find({}, {"_id": 0}))  # Exclude MongoDB's default _id field
    return pd.DataFrame(data)



def generate_prompt(sql_chunk):
    """Generate prompt for data lineage with transformation handling."""
    example_output = '''{
        "lineage": [
            {
                "Source_Column": "[Patient_Text]", 
                "Source_Table": "[EDW].[PAS].[Input_Patient]", 
                "Target_Column": "[Patient_Text]", 
                "Target_Table": "[EDW].[dbo].[Patient]", 
                "Transformation": "TRIM"
            },
            {
                "Source_Column": "[Active_Flag]",
                "Source_Table": "[EDW].[PAS].[Input_Patient]",
                "Target_Column": "[Active_Flag]",
                "Target_Table": "[EDW].[dbo].[Patient]",
                "Transformation": "UPPER"
            }
        ]
    }'''
    
    return (
        f"Given the SQL operations:\n{sql_chunk}\n"
        "Generate data lineage **strictly in valid JSON format** matching this structure:\n\n"
        f"{example_output}\n\n"
        "Make sure to correctly infer transformations from SQL operations such as `TRIM`, `UPPER`, `LOWER`, `CAST`, etc., "
        "and reflect those transformations accurately in the JSON output."
        "Output only a single valid JSON object with no extra characters."
    )


@app.route('/generate-lineage', methods=['POST'])
def generate_lineage():
    """Handle user requests to generate data lineage."""
    request_data = request.get_json()

    job_id = request_data.get('job_id')

    if not job_id:
        return jsonify({"error": "job_id is required"}), 400

        # Load data into DataFrame
    df_code_collection = load_data_from_mongo('JobActivityDetails')

    # Filter by JobId and sort by CodeExecutionOrder
    sorted_df = df_code_collection[df_code_collection['JobId'] == ObjectId(job_id)].sort_values(by=['CodeEexecutionOrder'])

# Concatenate all 'Code' values into a single string
    concatenated_codes = sorted_df['Code'].str.cat(sep=' ')

    prompt = generate_prompt(concatenated_codes)

    client = OpenAI(api_key=OPENAI_API_KEY)
    
    # Example of counting tokens in the input prompt
    encoding = tiktoken.encoding_for_model("gpt-4")
    num_tokens = len(encoding.encode(concatenated_codes))
    print("generation response")
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=num_tokens,
        temperature=0
    )
    print("got response")
    response = response.choices[0].message.content
    # response = clean_json_lineage(response)
    # print(response)
    return jsonify({"lineage": response}), 200


def clean_json_lineage(data):
    # Load the JSON data
    parsed_data = json.loads(data)
    
    # Extract lineage data
    lineage_data = parsed_data.get("lineage", [])

    # Format each entry in the lineage list for readability
    cleaned_lineage = []
    for entry in lineage_data:
        cleaned_entry = {
            "Source Column": entry.get("Source_Column", ""),
            "Source Table": entry.get("Source_Table", ""),
            "Target Column": entry.get("Target_Column", ""),
            "Target Table": entry.get("Target_Table", ""),
            "Transformation": entry.get("Transformation", "")
        }
        cleaned_lineage.append(cleaned_entry)
    
    return cleaned_lineage

@app.route('/')
def home():
    return 'API is up and running!'

# if __name__ == '__main__':
#     app.run(debug=True, host='0.0.0.0', port=5000)
