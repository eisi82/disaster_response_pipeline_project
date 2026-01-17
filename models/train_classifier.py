import sys
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

import re
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
import nltk

from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.multioutput import MultiOutputClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, precision_score, recall_score, f1_score

import joblib

# Download NLTK data if not already present
try:
    nltk.data.find('tokenizers/punkt')
    nltk.data.find('corpora/wordnet')
except LookupError:
    nltk.download('punkt')
    nltk.download('wordnet')


def load_data(database_filepath):
    """
    Load data from SQLite database.
    
    Args:
        database_filepath (str): Path to SQLite database file
    
    Returns:
        tuple: (X, Y, category_names) where X is messages, Y is categories, category_names is list of category column names
    """
    engine = create_engine(f'sqlite:///{database_filepath}')
    df = pd.read_sql_table('disaster_messages', engine)
    
    X = df['message']
    cols = list(df.columns)
    category_cols = [c for c in cols if c not in ['id', 'message', 'original', 'genre']]
    Y = df[category_cols]
    
    return X, Y, category_cols


def tokenize(text):
    """
    Normalize, tokenize, and lemmatize text.
    
    Args:
        text (str): Text to tokenize
    
    Returns:
        list: List of cleaned tokens
    """
    # Normalize
    text = re.sub(r"[^a-zA-Z0-9]", " ", text)
    
    # Tokenize
    tokens = word_tokenize(text)
    
    # Lemmatize
    lemmatizer = WordNetLemmatizer()
    clean_tokens = []
    for tok in tokens:
        clean_tok = lemmatizer.lemmatize(tok).lower().strip()
        clean_tokens.append(clean_tok)
    
    return clean_tokens


def build_model():
    """
    Build ML pipeline with TF-IDF vectorizer and MultiOutputClassifier.
    
    Returns:
        Pipeline: Configured scikit-learn Pipeline
    """
    pipeline = Pipeline([
        ('tfidf', TfidfVectorizer(tokenizer=tokenize, lowercase=True, stop_words='english')),
        ('clf', MultiOutputClassifier(RandomForestClassifier(n_jobs=-1, random_state=42)))
    ])
    
    return pipeline


def evaluate_model(model, X_test, Y_test, category_names):
    """
    Evaluate the model and print classification reports and summary metrics.
    
    Args:
        model: Trained model
        X_test (pd.Series): Test messages
        Y_test (pd.DataFrame): Test target variables
        category_names (list): List of category column names
    """
    Y_pred = model.predict(X_test)
    
    print("\n" + "=" * 80)
    print("MODEL EVALUATION RESULTS")
    print("=" * 80)
    
    # Per-category classification reports
    print("\nDetailed per-category metrics:")
    print("-" * 80)
    for i, col in enumerate(category_names):
        print(f"\nCategory: {col}")
        print(classification_report(Y_test.iloc[:, i], Y_pred[:, i], zero_division=0))
    
    # Summary metrics
    precisions = []
    recalls = []
    f1s = []
    
    for i in range(Y_test.shape[1]):
        precision = precision_score(Y_test.iloc[:, i], Y_pred[:, i], average='macro', zero_division=0)
        recall = recall_score(Y_test.iloc[:, i], Y_pred[:, i], average='macro', zero_division=0)
        f1 = f1_score(Y_test.iloc[:, i], Y_pred[:, i], average='macro', zero_division=0)
        
        precisions.append(precision)
        recalls.append(recall)
        f1s.append(f1)
    
    print("\n" + "=" * 80)
    print("SUMMARY METRICS (macro average across all labels)")
    print("=" * 80)
    print(f"Precision: {np.mean(precisions):.4f}")
    print(f"Recall: {np.mean(recalls):.4f}")
    print(f"F1-Score: {np.mean(f1s):.4f}")
    
    # Subset accuracy
    subset_accuracy = (Y_pred == Y_test.values).all(axis=1).mean()
    print(f"Subset Accuracy (exact match): {subset_accuracy:.4f}")
    print("=" * 80 + "\n")


def save_model(model, model_filepath):
    """
    Save trained model to a pickle file.
    
    Args:
        model: Trained model
        model_filepath (str): Path to save the model
    """
    joblib.dump(model, model_filepath)


def main():
    if len(sys.argv) == 3:
        database_filepath, model_filepath = sys.argv[1:]
        print('Loading data...\n    DATABASE: {}'.format(database_filepath))
        X, Y, category_names = load_data(database_filepath)
        X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2)
        
        print('Building model...')
        model = build_model()
        
        print('Training model...')
        model.fit(X_train, Y_train)
        
        print('Evaluating model...')
        evaluate_model(model, X_test, Y_test, category_names)

        print('Saving model...\n    MODEL: {}'.format(model_filepath))
        save_model(model, model_filepath)

        print('Trained model saved!')

    else:
        print('Please provide the filepath of the disaster messages database '\
              'as the first argument and the filepath of the pickle file to '\
              'save the model to as the second argument. \n\nExample: python '\
              'train_classifier.py ../data/DisasterResponse.db classifier.pkl')


if __name__ == '__main__':
    main()