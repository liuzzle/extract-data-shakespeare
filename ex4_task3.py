from dotenv import load_dotenv
import os
import sqlite3
import pandas as pd

# call variables
#host = os.getenv("DB_HOST")

#load environment variables
load_dotenv()

# Cursor pointing to your database
connection = sqlite3.connect("example.db")
cursor = connection.cursor()

# Drop tables if they exist
cursor.execute('DROP TABLE IF EXISTS works;')
cursor.execute('DROP TABLE IF EXISTS characters;')

# create works table
cursor.execute('''
    CREATE TABLE works (
        id INT PRIMARY KEY,
        short_title TEXT NOT NULL,
        long_title TEXT NOT NULL,
        year INT,
        genre TEXT,
        num_words INT
    );
''')

# create characters table
cursor.execute('''
    CREATE TABLE characters (
    	id INT PRIMARY KEY,
        name TEXT NOT NULL,
        work_id INT,
    	description TEXT,
        num_work INT,
        num_words INT, -- added to store num of words spoken by the char
        FOREIGN KEY(work_id) REFERENCES works(id)
    );
''')
#connection.commit()

# load the data from CSV files into tables
works_df = pd.read_csv(r"data/works.csv")
char_df = pd.read_csv(r"data/characters.csv")

# Print column names to debug
#print("Works Columns:", works_df.columns)
#print("Characters Columns:", char_df.columns)

# Filter the relevant columns to match the table schema before inserting
works_df = works_df[['id', 'short_title', 'long_title', 'year', 'genre', 'num_words']]
char_df = char_df[['id', 'name', 'work_id', 'description', 'num_words']]

# Insert data into the database
char_df.to_sql('characters', connection, if_exists='append', index=False)
works_df.to_sql('works', connection, if_exists='append', index=False)

#### Database Queries #####
# 1: Which characters say only one word?
query1 = '''
    SELECT name FROM characters WHERE num_words = 1;
'''
one_word_chars = pd.read_sql_query(query1, connection)
print("One word characters:", one_word_chars)

# 2: In which play does a fairy named Mustardseed appear?
query2 = '''
    SELECT works.long_title FROM characters 
    JOIN works ON characters.work_id = works.id
    WHERE characters.name = "Mustardseed";
'''
mustardseed_fairy = pd.read_sql_query(query2, connection)
print("Mustardseed Fairy:", mustardseed_fairy)

# 3: Do clowns appear more often in comedies or in tragedies?
query3 = '''
    SELECT works.genre, COUNT(characters.id) AS count FROM characters 
    JOIN works ON characters.work_id = works.id
    WHERE characters.name LIKE "%Clown%"
    GROUP BY works.genre
    ORDER BY count DESC;
'''
clown_appearances = pd.read_sql_query(query3, connection)
print("Clown Appearances:", clown_appearances)

connection.commit()
# Close the database connection
connection.close()