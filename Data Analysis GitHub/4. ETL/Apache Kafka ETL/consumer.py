from kafka import KafkaConsumer
import logging
import json
import configparser
import psycopg2
from datetime import datetime

#Setup logging
logging.basicConfig(filename='./logs/consumer.log', 
                    level=logging.INFO,
                    filemode='a',
                    format='[%(asctime)s]-[%(levelname)s]-#%(message)s')
logging.info('Consumer Script Started')

#Setup Consumer Configuration
try:
    consumer = KafkaConsumer('weather-data',
                         bootstrap_servers=['localhost:9092'])
    if consumer.bootstrap_connected() == True:
        logging.info('Consumer connected to Server')
        print('Consumer connected to Server')
    else:
        logging.info('Consumer not connected to Server')
        print('Consumer not connected to Server')
except Exception as err:
    logging.error(err)
    print('Error', err)


def connect_db(config_file='config.ini', section='postgresql'):
    config = configparser.ConfigParser()
    config.read(config_file)
    db_config = {
        'host': config.get(section, 'host'),
        'user': config.get(section, 'user'),
        'password': config.get(section, 'password'),
        'database' : 'Assignment-6'
    }

    try:
        # Establish a connection to the PostgreSQL server
        conn = psycopg2.connect(**db_config)
        print("Connected to the PostgreSQL server successfully.")
        logging.info("Connected to the PostgreSQL server successfully.")
        return conn
    except psycopg2.Error as e:
        logging.info(f"Error: Unable to connect to the PostgreSQL server.\n{e}")
        print(f"Error: Unable to connect to the PostgreSQL server.\n{e}")
        return None
    

def create_table():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS weather_data (
        city VARCHAR,
        temperature FLOAT,
        windspeed FLOAT,
        humidity FLOAT,
        received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
    """)
    conn.commit()
    logging.info(("Last Executed Query",cursor.query))
    return None

def insert_weather_data():
    conn = connect_db()
    cursor = conn.cursor()
    insert_statment = """INSERT INTO weather_data (
            city, temperature, windspeed, humidity, received_at
        )   VALUES (%s, %s, %s, %s, %s) """
    for message in consumer:
        received_at = datetime.now()
        data = json.loads(message.value.decode('utf-8'))
        cursor.execute(insert_statment, (
                    data[0]['city'],
                    data[0]['temperature'],
                    data[0]['wind_speed'],
                    data[0]['humidity'],
                    received_at))
        cursor.execute(insert_statment, (
                    data[1]['city'],
                    data[1]['temperature'],
                    data[1]['wind_speed'],
                    data[1]['humidity'],
                    received_at))
        conn.commit()
        logging.info(('Data successfully added to table received at', str(received_at)))


create_table()
insert_weather_data()