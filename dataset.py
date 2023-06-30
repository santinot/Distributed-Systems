from faker import Faker
import random
import os
import json
import time

USERS = 200000

# Function to generate and return fake data
def generation():
    # Opening JSON file with films
    try:
        with open('Film.json', 'r') as openfile:
            films = json.load(openfile)
            print("File Film.json found. Processing...")
    except:
        print("No file named Film.json found.")
        exit()

    list_of_dictionary = []

    # Generating fake data and appending films to each user
    for _ in range(USERS):
        first_name = fake.first_name()
        last_name = fake.last_name()
        list_of_films = []
        for i in random.sample(range(0, len(films)), random.randint(3, len(films))): 
            list_of_films.append(films[i])
        temp = {
            "first_name": first_name,
            "last_name": last_name,
            "email": f"{first_name.lower()}.{last_name.lower()}@{fake.domain_name()}", #f-string, all the variables inside curly brackets are read and replaced by their value.
            "phonenumber": fake.phone_number(),
            "films": list_of_films
        }
        list_of_dictionary.append(temp)  

    # Serializing json
    json_object = json.dumps(list_of_dictionary, indent=4)
    return json_object

if __name__ == '__main__':

    fake = Faker()

    # Writing to Dataset.json
    try:
        if not os.path.isfile("DataSet.json"):
            open("DataSet.json", "x")     
        with open("DataSet.json", "w") as outfile:
            start_time = time.time()
            outfile.write(generation())
            end_time = time.time()
            print("File DataSet.json created in " + str(end_time - start_time) + " seconds.")
    except:
        print("Error in writing to file.")
        exit()
