import psycopg2
import csv
from datetime import datetime
import logging

#Setup Logging
logging.basicConfig(filename='/home/test/logs/ETL.log', level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

#Connect to Database
conn = psycopg2.connect(host="192.168.56.1",
                        user="postgres",
                        password="postgres123",
                        port="5432")
cursor = conn.cursor()
conn.autocommit=True

#Create a Database
cursor.execute('CREATE DATABASE covid')
conn.commit()
logging.info("Database covid created.")


conn = psycopg2.connect(
			database="covid",
			host="192.168.56.1",
            user="postgres",
            password="postgres",
            port="5432")
cursor = conn.cursor()
logging.info("Database Connected.")

# Read headers from CSV to dynamically create table columns, and copy CSV
with open('/home/aseem/aseem_dam/assignment4_dam/transformed_data/transformed.csv') as file:
    reader_obj = csv.reader(file)
    headers = next(reader_obj)

    # Create table if not exists based on CSV headers
    create_table_query = f"CREATE TABLE IF NOT EXISTS covid ({', '.join([f'{header} varchar(255)' if header != 'total_cases' 
                                                                         else f'{header} decimal' for header in headers])})"
    cursor.execute(create_table_query)
    logging.info("Covid table created.")

    # Build the COPY command with explicit column names and data types
    copy_query = f"COPY covid ({', '.join(headers)}) FROM STDIN WITH CSV HEADER DELIMITER ','"

    # Insert content using copy_expert with explicit column names and data types
    i = 0
    for read in reader_obj:
        i += 1
    file.seek(0)
    cursor.copy_expert(copy_query, file)
    logging.info('Data Insertion in table Covid completed')
    logging.info('Rows copied %s' % (i))
    

#Commit changes to database\
conn.commit()
logging.info('Dataset committed to table Covid Completed')