import csv                     # Used to read CSV-formatted lines from the log file
from datetime import datetime  # Used to work with timestamps
import mmap
import time
from collections import defaultdict
import logging

# Configure logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')


started_tasks = {}
tasks = defaultdict(lambda: {"name": "", "runs": []})
started_tasks = {}
count_start = 0
count_finish = 0
count_errored = 0
incomplete_task_list = ""
completed_task_list = ""


with open("logs1.log", "r") as logs_file:
    # Opened the file and testing I can read it.
    read_logs_mm = mmap.mmap(logs_file.fileno(), length=0, access=mmap.ACCESS_READ) #length=0 means "full line"

    for line in iter(read_logs_mm.readline, b""):  # Read line-by-line until EOF b"" = empty byte; .readline returns bytes not strings
        decoded_line = line.decode("utf-8").strip()
        if not decoded_line:
            continue

        parts = decoded_line.split(",") 
        if len(parts) != 4:
            continue  # Skip malformed lines

        timestamp_str, task_name, status, task_id = [p.strip() for p in parts]
        timestamp = datetime.strptime(timestamp_str, "%H:%M:%S") #converting to useable timestamp
        tasks[task_id]["name"] = task_name

        if status == "START":
            started_tasks[task_id] = timestamp
            count_start=count_start+1

        elif status == "END":
            if task_id in started_tasks:
               start_time = started_tasks.pop(task_id) #removing the task from the started list, that way we can have a task star/end multiple times
               tasks[task_id]["runs"].append((start_time, timestamp))
               # Disabling for now to clean up the output               
               # print(f" Started and finished tasks {task_id} at {timestamp_str}")
               count_finish=count_finish+1

# Output task durations
for task_id, data in tasks.items():
    # get incomplete runs
    if not data["runs"]:
        incomplete_task_list+= str(data['name']) + "ID: "+ str(task_id) +" run #" + str(i) + " is incomplete\n"
        logging.error(f"There are no completed runs for {data['name']} (ID: {task_id})")
        continue
    # # get completed runs
    for i, (start, end) in enumerate(data["runs"], 1):
        duration = (end - start).total_seconds()
        #disabling in order to clean ooutput
        # print(f"{data['name']} (ID: {task_id}) run #{i} lasted {duration:.2f} seconds.")
        
        #creating warning for jobs that took longer than 5 minutes
        if duration > 300 and duration < 600:
            logging.warning(f"Task {data['name']} (ID: {task_id}) took longer than 5 minutes to complete {duration:.2f}")
        #creating error for jobs that took longer than 10 minutes
        elif duration > 600:
            logging.error(f"Task {data['name']} (ID: {task_id}) took longer than 10 minutes to complete {duration:.2f}")
        else:
            #I should probably find a cleaner way to save and display end results 
            completed_task_list += str(data['name']) + "ID: "+ str(task_id) +" run #" + str(i) + " lasted " + str(duration) +"seconds. \n"
            


#printing Completed jobs
print(f"\nCompleted tasks in suitable interval:\n{completed_task_list}")
#printing Incomplete jobs            
print(f"\nincomplete tasks:\n{incomplete_task_list}")





    