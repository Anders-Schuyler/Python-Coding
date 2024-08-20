# ITP Week 3 Day 3 Practice

# import in the two modules for making API calls and parsing the data
import json
import requests

# Using our API call from the exercise yesterday (this is provided for you):

# UNCOMMENT BELOW AFTER YOUR IMPORTS
url = "https://rickandmortyapi.com/api/character"
response = requests.get(url)
json_data = response.text
data = json.loads(json_data)

# print it out to see how the data looks initially:
print(data)

# Convert the very hard to read data back into JSON using json.dumps (store into a serialized_json variable):
json_data = json.dumps(data)

# print the serialized_json
print(json_data)

# This doesn't look too different than our original print at a glance

# Add some appropriate indents to make it more readable reassigning serialized_json:
serialized_json = json.dumps(data, indent=4)

# uncomment below to see the changes!
print(serialized_json)

# This makes it look a lot more readable

# Now change all of the separators from (,  :)  into (:  =>) reassigning serialized_json
serialized_json = json.dumps(data, indent=4, separators=(": ","=> "))

# uncomment below to see the changes!
print(serialized_json)

# This one really has no purpose for this content,
# it can just give us a better looking separation of values.

# Finally, sort the data alphabetically reassigning serialized_json:
serialized_json = json.dumps(data, indent=4, separators=(": ","=> "), sort_keys=True)

# uncomment below to see the changes!
print(serialized_json)

# Again, this one is just for you the programmer to be able to easily
# sort through the data that you have gotten from the api alphabetically.