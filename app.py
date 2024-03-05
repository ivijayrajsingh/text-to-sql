from flask import Flask
import langchain_experimental
import openai
import pandas

app = Flask(__name__)

@app.route('/')
def hello_world():
    print("langchain_experimental version:", langchain_experimental.__version__)
    print("openai version:", openai.__version__)
    print("pandas version:", pandas.__version__)
    return f'Hello, World! { langchain_experimental.__version__, openai.__version__, pandas.__version__}'

# if __name__=='__main__':
#     app.run()