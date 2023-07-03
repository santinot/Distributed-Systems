import json
import time
from multiprocessing import Process, Manager
import os

# Create dictionary with films as key and 1 as value
def mapping(data):
    list_of_dictionary = []
    temp = {}
    for i in range(0, len(data)):
        for j in range(0, len(data[i]["films"])):
            temp = {data[i]["films"][j]["Title"]:1}
            list_of_dictionary.append(temp)
    return list_of_dictionary

# Count the number of occurrences of a film
def reducing(data, title):
    dictionary = {}
    for i in range(0, len(data)):
        if (list(data[i].keys())[0] in dictionary):
            dictionary[list(data[i].keys())[0]] += 1
        else:
            dictionary[list(data[i].keys())[0]] = 1
    return dictionary.get(title)

# Function executed by each process 
def MapReduce(data, i, result):
    affinity_mask = {i}                             # Set which core the process will use
    pid = os.getpid()                               # Get the process id
    os.sched_setaffinity(pid, affinity_mask)        # Set affinity between process and core
    mapped_data = mapping(data)
    reduced_data = reducing(mapped_data, title)
    result[i] = reduced_data

title = '300' #input("Please insert a title...\n")

# Opening JSON file with films to check if the title is valid and JSON file with data 
try:
    with open ('Film.json', 'r') as film:
        films_title = []
        film = json.load(film)
        for i in range(0, len(film)):
            films_title.append(film[i]["Title"])
        if title not in films_title:
            exit()

    with open('DataSet.json', 'r') as openfile:
        print("File DataSet.json found. Processing...")
        data = json.load(openfile)
        print("File DataSet.json loaded.")
except:
    print("Title not Found or Somethings wrong with the files opening.")
    exit()

num_thread = os.cpu_count() 

# Dividing the data in chunks for each thread
data = [data[x:x+len(data)//num_thread] for x in range(0, len(data), len(data)//num_thread)]
# List for result of each process
result = Manager().list(range(len(data)))

# Creating processes
p = [] 
for i in range(0, num_thread):
    p.append(Process(target=MapReduce, args=(data[i], i, result)))
start_time = time.time()
for i in range(0, num_thread):
    p[i].start()
for i in range(0, num_thread):    
    p[i].join()
end_time = time.time()

print("Il numero di occorrenze per il film " + title + " è: " + str(sum(result)))
print("Valutato in " + str(end_time - start_time) + " seconds.")



