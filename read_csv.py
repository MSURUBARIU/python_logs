import csv                     # Used to read CSV-formatted lines from the log file
from datetime import datetime  # Used to work with timestamps

tasks = {}

with open("logs1.log", "r") as file:
    # Opened the file and testing I can read it.
    # print(file.read())
    read_logs = csv.reader(file)

    #getting each row from the read logs
    for row in read_logs:
        #set variables for each column.
        timestamp, task_name, status, task_id = row
        #making sure the info prints properly
        print(row)




    