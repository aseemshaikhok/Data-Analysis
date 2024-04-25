from kafka import KafkaProducer
import random
import time
import logging
import json
import os


#Setup logging
os.makedirs('./logs', exist_ok=True)
logging.basicConfig(filename='./logs/producer.log', 
                    level=logging.INFO,
                    filemode='a',
                    format='[%(asctime)s]-[%(levelname)s]-#%(message)s')
logging.info('Producer Script Started')

start_time = time.time() #timing the process

#Setup Producer Configuration
try:
    producer = KafkaProducer(bootstrap_servers='localhost:9092', 
                             value_serializer=lambda m: json.dumps(m).encode('utf-8'))
    if producer.bootstrap_connected() == True:
        logging.info('Producer connected to Server')
        print('Producer connected to Server')
    else: 
        logging.info('Producer not connected to Server')
        print('Producer not connected to Server')
except Exception as err:
    logging.error(err)
    print('Error', err)


#Function to generate data for winnipeg and Vancover
def generate_weather_data():
    data = [
        {
        'city' : 'Winnipeg',
        'temperature': round(random.uniform(-30, 30), 2),
        'wind_speed' : round(random.uniform(0, 50), 2),
        'humidity' :round(random.uniform(20, 80), 2)
        },
        {
        'city' : 'Vancouver',
        'temperature': round(random.uniform(-10, 25), 2),
        'wind_speed' : round(random.uniform(0, 50), 2),
        'humidity' :round(random.uniform(30,99), 2)
        }
    ]  
    return data

while True:
    time.sleep(5) #Sleep for 5 Seconds
    try:
        logging.info(generate_weather_data()) # log generated data
        producer.send('weather-data', generate_weather_data())
        print('Elapsed Time:',round(((time.time()-start_time)/60),2),'mins')
    except Exception as err:
        logging.error(err)
    except KeyboardInterrupt:
        logging.critical('Producer stopped by user')
        exit()