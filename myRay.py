import json
import time
import argparse
import ray

# Arguments for the number of cluster's nodes
def arguments():
    parser = argparse.ArgumentParser(description='Commands')
    parser.add_argument('-n',
                        type=int,
                        dest='num_cluster',
                        required=True)
    return parser.parse_args()

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
@ray.remote
def MapReduce(data, title):
    mapped_data = mapping(data)
    reduced_data = reducing(mapped_data, title)
    return (reduced_data)

ray.init()

args = arguments()    

title = input("Please insert a title...\n")

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

cluster = args.num_cluster

# Dividing the data in chunks for each thread
data = [data[x:x+len(data)//cluster] for x in range(0, len(data), len(data)//cluster)]

start_time = time.time()
futures = [MapReduce.remote(ray.put(data[i]), ray.put(title)) for i in range(0, cluster)]
end_time = time.time()
result = [(ray.get(futures[i])) for i in range(0, cluster)]

print("Il numero di occorrenze per il film " + title + " è: " + str(sum(result)))
print("Valutato in " + str(end_time - start_time) + " seconds.")



