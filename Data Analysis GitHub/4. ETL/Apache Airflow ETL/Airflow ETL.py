from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import logging

#logging setup
logging.basicConfig(filename='/home/test/logs/ETL.log', level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

#Merge CSV files
def merge():
    unit_path = '/home/test/extracted_data/'
    df1 = pd.read_csv(unit_path+'csv_d.csv', header=None)
    df2 = pd.read_csv(unit_path+'tsv_d.csv', header=None)
    df3 = pd.read_csv(unit_path+'fixed_width_d.csv', header=None)
    df3 = df3[[10,12]]
    merge_df = pd.concat([df1, df2, df3], ignore_index=True, axis=1)
    merge_df.to_csv('/home/test/transformed_data/merged_table.csv', index=False, header=None)
    logging.info('3 CSV files merged to merged_table.csv')

def raise_upper_case():
    unit_path = '/home/test/transformed_data/'
    upper_case_df = pd.read_csv(unit_path+'merged_table.csv', header=None)
    upper_case_df[3] = upper_case_df[3].str.upper()
    upper_case_df.to_csv('/home/test/transformed_data/upper_case_table.csv', index=False, header=None)
    logging.info('Column 4 converted to Upper Case')


default_args = {
    'owner': 'test',
    'retries': 1
}


with DAG(
    dag_id="traffic_data_ETL",
    start_date=datetime(2024, 1, 30),
    default_args=default_args,
    schedule='@daily',
    catchup=False
) as dag:

    start = BashOperator(
        task_id="bash_task",
        bash_command='''mkdir -p /home/test/
            mkdir -p /home/test/logs
            touch /home/test/logs/ETL.log
            mkdir -p /home/test/raw_data
            mkdir -p /home/test/unzipped_raw
            mkdir -p /home/test/extracted_data
            mkdir -p /home/test/transformed_data
            echo "$(date +'%Y-%m-%d %H:%M:%S') ETL Process Started" >> /home/test/logs/ETL.log
            echo "$(date +'%Y-%m-%d %H:%M:%S') Directory for ETL Created" >> /home/test/logs/ETL.log'''
    )

    download = BashOperator(
        task_id = "download",
        bash_command='''curl https://elasticbeanstalk-us-east-2-340729127361.s3.us-east-2.amazonaws.com/trafficdata.tgz -o /home/test/raw_data/trafficdata.tgz >> /home/test/logs/ETL.log 2>&1
            echo "$(date +'%Y-%m-%d %H:%M:%S') Download Completed" >> /home/test/logs/ETL.log'''
        )

    unzip_tar = BashOperator(
        task_id="unzip_tar",
        bash_command='''
            tar -xvf /home/test/raw_data/trafficdata.tgz -C /home/test/unzipped_raw/ >> /home/test/logs/ETL.log 2>&1
            echo "$(date +'%Y-%m-%d %H:%M:%S') File Unzipped" >> /home/test/logs/ETL.log'''
    )

    extract_csv = BashOperator(
        task_id="extract_csv",
        bash_command=''' 
            cut -d',' -f1,2,3,4 /home/test/unzipped_raw/vehicle-data.csv > /home/test/extracted_data/csv_d.csv
            echo "$(date +'%Y-%m-%d %H:%M:%S') New file created with columns 1,2,3,4 as csv_d.csv">> /home/test/logs/ETL.log
        '''
    )   

    extract_tsv = BashOperator(
        task_id="extract_tsv",
        bash_command=''' 
            cut -f5,6,7 /home/test/unzipped_raw/tollplaza-data.tsv | tr '\t' ',' > /home/test/extracted_data/tsv_d.csv
            echo "$(date +'%Y-%m-%d %H:%M:%S') New file created with columns 5,6,7 as tsv_d.csv">> /home/test/logs/ETL.log
        '''
    )  

    extract_fixed_width = BashOperator(
        task_id="extract_fixed_width",
        bash_command=''' 
            awk 'BEGIN { FIELDWIDTHS = "6 1 24 1 7 4 4 1 9 1 3 1 5" } { print $1 "," $2 "," $3 "," $4 "," $5 "," $6 "," $7","$8","$9","$10","$11","$12","$13}' /home/test/unzipped_raw/payment-data.txt > /home/test/extracted_data/fixed_width_d.csv
            echo "$(date +'%Y-%m-%d %H:%M:%S') New file created with second last and last columns as fixed_width_d.csv">> /home/test/logs/ETL.log
        '''
    ) 

    merge = PythonOperator(
        task_id="merge",
        python_callable=merge,
    )
      
    raise_upper_case = PythonOperator(
        task_id="raise_upper_case",
        python_callable= raise_upper_case
    ) 


start>>download>>unzip_tar>>[extract_csv, extract_tsv, extract_fixed_width]>>merge>>raise_upper_case
