import sys
import pandas as pd
from sqlalchemy import create_engine


def load_data(messages_filepath, categories_filepath):
    """
    Load messages and categories CSV files and merge them.
    
    Args:
        messages_filepath (str): Path to messages CSV file
        categories_filepath (str): Path to categories CSV file
    
    Returns:
        pd.DataFrame: Merged dataframe with id, message, genre, and categories columns
    """
    messages = pd.read_csv(messages_filepath)
    categories = pd.read_csv(categories_filepath)
    df = messages.merge(categories, how='inner', on='id')
    return df


def clean_data(df):
    """
    Clean and process the dataframe:
    - Split categories column into individual category columns
    - Convert category values to binary (0 or 1)
    - Remove duplicates
    
    Args:
        df (pd.DataFrame): Raw merged dataframe
    
    Returns:
        pd.DataFrame: Cleaned dataframe with individual category columns
    """
    # Split categories column into individual columns
    categories = df['categories'].str.split(';', expand=True)
    
    # Extract category names from first row
    row = categories.iloc[0]
    category_colnames = [col.split('-')[0] for col in row]
    
    # Rename columns
    categories.columns = category_colnames
    
    # Convert category values to binary (0 or 1)
    for column in categories:
        categories[column] = categories[column].astype(str).str[-1]
        categories[column] = pd.to_numeric(categories[column])
    
    # Replace original categories column with new category columns
    df = df.drop('categories', axis=1)
    df = pd.concat([df, categories], axis=1)
    
    # Remove duplicates
    df = df.drop_duplicates()
    
    return df


def save_data(df, database_filename):
    """
    Save cleaned dataframe to SQLite database.
    
    Args:
        df (pd.DataFrame): Cleaned dataframe
        database_filename (str): Path to SQLite database file
    """
    engine = create_engine(f'sqlite:///{database_filename}')
    df.to_sql('disaster_messages', engine, index=False, if_exists='replace')  


def main():
    if len(sys.argv) == 4:

        messages_filepath, categories_filepath, database_filepath = sys.argv[1:]

        print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
              .format(messages_filepath, categories_filepath))
        df = load_data(messages_filepath, categories_filepath)

        print('Cleaning data...')
        df = clean_data(df)
        
        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(df, database_filepath)
        
        print('Cleaned data saved to database!')
    
    else:
        print('Please provide the filepaths of the messages and categories '\
              'datasets as the first and second argument respectively, as '\
              'well as the filepath of the database to save the cleaned data '\
              'to as the third argument. \n\nExample: python process_data.py '\
              'disaster_messages.csv disaster_categories.csv '\
              'DisasterResponse.db')


if __name__ == '__main__':
    main()