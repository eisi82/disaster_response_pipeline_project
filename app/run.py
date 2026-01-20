import json
import plotly
import pandas as pd
import numpy as np

import re
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
import nltk

from flask import Flask
from flask import render_template, request, jsonify
import joblib
from sqlalchemy import create_engine


app = Flask(__name__)

def tokenize(text):
    """
    Normalize, tokenize, and lemmatize text using NLTK.
    
    Removes special characters, converts to lowercase, and lemmatizes tokens
    to their base form for consistency in text classification.
    
    Parameters:
        text (str): Raw text string to be processed
    
    Returns:
        list: List of cleaned, lemmatized tokens (lowercase strings)
    """
    text = re.sub(r"[^a-zA-Z0-9]", " ", text)
    tokens = word_tokenize(text)
    lemmatizer = WordNetLemmatizer()

    clean_tokens = []
    for tok in tokens:
        clean_tok = lemmatizer.lemmatize(tok).lower().strip()
        clean_tokens.append(clean_tok)

    return clean_tokens

# Initialize database engine and load preprocessed disaster messages dataset
engine = create_engine('sqlite:///../data/DisasterResponse.db')
df = pd.read_sql_table('disaster_messages', engine)

# Load the trained multi-output classifier (TF-IDF + Random Forest)
model = joblib.load("../models/classifier.pkl")

# Extract 36 category column names from dataset (exclude metadata columns)
category_names = [col for col in df.columns if col not in ['id', 'message', 'original', 'genre']]


# index webpage displays cool visuals and receives user input text for model
@app.route('/')
@app.route('/index')
def index():
    """
    Render the home page with training dataset visualizations.
    
    Loads preprocessed disaster response data from the database and creates
    three interactive Plotly visualizations:
    1. Genre distribution (bar chart)
    2. Top 15 message categories by frequency (bar chart)
    3. Related vs. non-related messages distribution (pie chart)
    
    Returns:
        str: Rendered HTML template with embedded Plotly graphs
    """
    # Aggregate statistics from training dataset for visualization
    genre_counts = df.groupby('genre').count()['message']
    genre_names = list(genre_counts.index)
    genre_values = list(genre_counts.values)
    
    # Count messages per category to identify most common disaster types
    category_counts = df[category_names].sum().sort_values(ascending=False).head(15)
    category_labels = list(category_counts.index)
    category_values = list(category_counts.values)
    
    # Calculate distribution of related vs. non-related messages
    related_distribution = df['related'].value_counts().sort_index()
    
    # Create visuals as dictionaries (not Plotly objects)
    graphs = [
        # Graph 1: Genre distribution
        {
            'data': [
                {
                    'x': genre_names,
                    'y': [int(v) for v in genre_values],
                    'type': 'bar',
                    'marker': {'color': 'rgb(55, 83, 109)'}
                }
            ],
            'layout': {
                'title': 'Distribution of Message Genres',
                'yaxis': {'title': "Count"},
                'xaxis': {'title': "Genre"}
            }
        },
        
        # Graph 2: Top 15 message categories
        {
            'data': [
                {
                    'x': category_labels,
                    'y': [int(v) for v in category_values],
                    'type': 'bar',
                    'marker': {'color': 'rgb(26, 118, 255)'}
                }
            ],
            'layout': {
                'title': 'Top 15 Message Categories by Frequency',
                'yaxis': {'title': "Count"},
                'xaxis': {'title': "Category", 'tickangle': -45}
            }
        },
        
        # Graph 3: Related vs Non-Related pie chart
        {
            'data': [
                {
                    'labels': ['Related', 'Not Related'] if len(related_distribution) > 1 else ['Related'],
                    'values': [int(v) for v in related_distribution.values],
                    'type': 'pie',
                    'marker': {'colors': ['rgb(255, 99, 71)', 'rgb(99, 255, 132)']}
                }
            ],
            'layout': {
                'title': 'Distribution of Related Messages'
            }
        }
    ]
    
    # encode plotly graphs in JSON
    ids = ["graph-{}".format(i) for i, _ in enumerate(graphs)]
    """
    Classify a user-submitted message across 36 disaster categories.
    
    Takes a message query from the user, preprocesses it using the same
    tokenization method as training, and passes it through the trained
    MultiOutputClassifier to predict applicable categories.
    
    Query Parameters:
        query (str): User-provided disaster message to classify
    
    Returns:
        str: Rendered results page with matched categories highlighted
    """
    graphJSON = json.dumps(graphs, cls=plotly.utils.PlotlyJSONEncoder)
    
    # render web page with plotly graphs
    return render_template('master.html', ids=ids, graphJSON=graphJSON)


# web page that handles user query and displays model results
@app.route('/go')
def go():
    # save user input in query
    query = request.args.get('query', '') 

    # use model to predict classification for query
    classification_labels = model.predict([query])[0]
    classification_results = dict(zip(category_names, classification_labels))
    
    # Sort by category name for display
    classification_results = dict(sorted(classification_results.items()))

    # This will render the go.html Please see that file. 
    return render_template(
        'go.html',
        query=query,
        classification_result=classification_results
    )


def main():
    app.run(host='0.0.0.0', port=3001, debug=True)


if __name__ == '__main__':
    main()