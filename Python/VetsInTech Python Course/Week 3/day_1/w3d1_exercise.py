# ITP Week 3 Day 1 Exercise

# ENUMERATE!

# 1. Read all instructions first!
# 


users_list = ["Alex", "Bob", "Charlie", "Dexter", "Edgar", "Frank", "Gary"]

# Prompt: given a list of names, create a list of dictionaries with the index as the user_id and name
# example output    
# [{"user_id": 0, "name": "Alex"}, etc, etc]
users_dict_list = [{"user_id": index, "name": name} for index, name in enumerate(users_list)]

# 1a. Create a function that takes a single string value and returns the desired dictionary
def create_user_dict(name, user_id):
    return {"user_id": user_id, "name": name}

# 1b. Create a new empty list called users_dict_list
users_dict_list = []
# 1c. Loop through users_list that calls the function for each item and appends the return value to users_dict_list
for index, name in enumerate(users_list):
    users_dict_list.append(create_user_dict(name, index))

# 2. Prompt: Given a series of dictionaries and desired output (mock_data.py), can you provide the correct commands?
from w3d1_mock_data import mock_data
# 2a. retrieve the gender of Morty Smith
morty_smith = mock_data["results"][1]
print(morty_smith["name"] + " is " + morty_smith["gender"])

# 2b. retrieve the length of the Rick Sanchez episodes
print(len(mock_data["results"][0]["episode"]))

# 2c. retrieve the url of Summer Smith location
print(mock_data["results"][2]["location"]["url"])