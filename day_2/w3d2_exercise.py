# ITP Week 3 Day 2 Exercise

# import in the two modules for making API calls and parsing the data
import json
import requests

# set a url variable to "https://rickandmortyapi.com/api/character"
url = 'https://rickandmortyapi.com/api/character'

# set a variable response to the "get" request of the url
response = requests.get(url)

# print to verify we have a status code of 200
print(response) 

#Improving the code to automate the code verification
if response.status_code == 200:
    print("Success! Status Code:", response.status_code)
else:
    print("Failed to retrieve data. Status Code:", response.status_code)

# assign a variable json_data to the responses' json
json_data = response.json()

# print to verify a crazy body of strings!
print(json_data)

# lets make it into a python dictionary by using the appropriate json method
# Isn't json_data already a python dictionary?
#Otherwise this should convert it

# json_str = json.dumps(json_data)
# posts = json.loads(json_str)

# print the newly created python object
#json_data should already be a python dictionary so no new python object needs to be created. 
print(json_data)

#Otherwise run the below code
#print(posts)