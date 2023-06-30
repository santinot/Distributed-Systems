import json
import time

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

def MapReduce(data):
    mapped_data = mapping(data)
    reduced_data = reducing(mapped_data, title)
    return reduced_data

#input("Please insert a title...")  Examples: 300, Vikings, Parasite, Narcos

title = '300'
 
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

start_time = time.time()
result = MapReduce(data)
end_time = time.time()
print("Il numero di occorrenze per il film " + title + " è: " + str(result))
print("Valutato in " + str(end_time - start_time) + " seconds.")


