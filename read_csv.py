import csv                     # Used to read CSV-formatted lines from the log file
from datetime import datetime  # Used to work with timestamps
import mmap
import time
from collections import defaultdict

tasks = {}
''' ways to implement
-1 dictionary and open(file)
-2 for loop that searches contents every time
-3 dictionary and mmap.mmap()  Should improve performance, but we might have other drawback with really large files.
'''

with open("logs1.log", "r") as logs_file:
    # Opened the file and testing I can read it.
    # print(file.read())
    # read_logs = csv.reader(file)
    read_logs_mm = mmap.mmap(logs_file.fileno(), length=0, access=mmap.ACCESS_READ) #length=0 means "full line"

    for line in iter(read_logs_mm.readline, b""):  # Read line-by-line until EOF b"" = empty byte; .readline returns bytes not strings
        decoded_line = line.decode("utf-8").strip()
        if not decoded_line:
            continue

        parts = decoded_line.split(",") 
        if len(parts) != 4:
            continue  # Skip malformed lines

        timestamp_str, task_name, status, task_id = [p.strip() for p in parts]

        # for timestamp_str in parts:
            # set variables for each column.
            # timestamp, task_name, status, task_id = parts
            # making sure the info prints properly
        print(timestamp_str)
        print(task_name)
        print(status)
        print(f"{task_id} \n")




    #getting each row from the read logs
    # for row in read_logs:
    #     # set variables for each column.
    #     timestamp, task_name, status, task_id = row
    #     # making sure the info prints properly
    #     print(row)




    