#! /bin/bash

#Setup Date and time variable
datetime=$(date '+%Y-%m-%d %H:%M:%S')

#Create directory for ETL process
set_directory="/home/test/"
mkdir "$set_directory"logs "$set_directory"downloaded_data "$set_directory"transformed_data 
echo $datetime " - INFO - ETL process started" >> "$set_directory"logs/ETL.log
echo  $datetime " - INFO - Folders for ETL process created" > "$set_directory"logs/ETL.log

#Download the file.
url="https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
output_file="$set_directory"downloaded_data/covid.csv
echo  $datetime " - INFO - Download started" >> "$set_directory"logs/ETL.log
curl $url -o $output_file >> "$set_directory"logs/ETL.log 2>&1
echo  $datetime " - INFO - Download finished" >> "$set_directory"logs/ETL.log


#transform the data
touch "$set_directory"transformed_data/transformed.csv
echo  $datetime " - INFO - Empty transformed csv file created" >> "$set_directory"logs/ETL.log
echo  $datetime " - INFO - Copying Column 1,2,4,5 from covid.csv to transformed.csv" >> "$set_directory"logs/ETL.log
cut -d ',' -f 1,2,4,5 "$output_file" >> "$set_directory"transformed_data/transformed.csv
echo  $datetime " - INFO - Copy finished" >> "$set_directory"logs/ETL.log

#Excute python script
echo $datetime " - INFO - Python script started" >> "$set_directory"logs/ETL.log
python3 /home/test/insert_to_database.py >> "$set_directory"logs/ETL.log
echo $datetime " - INFO - Python script finished" >> "$set_directory"logs/ETL.log
echo $datetime " - INFO - ETL process finished" >> "$set_directory"logs/ETL.log
echo "finished"
