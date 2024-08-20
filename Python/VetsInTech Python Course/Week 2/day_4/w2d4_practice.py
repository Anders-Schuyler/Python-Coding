# ITP Week 2 Day 4 (In-Class) Practice

# Dictionary

# create a new dictionary named my_person with one key value pair of name : [YOUR_NAME]
my_person = {
    "YOUR_NAME": "Anders"
}
# verify the type of my_person to be a dictionary by using type
print(type(my_person))
# Next we will use a pre-made dictionary:

person_1 = {
    "first_name": "Scooby",
    "favorite_snack": "Rare Candy",
    "wears_glasses": True
    }

# add a key value pair to person_1 with the last_name of Doo
person_1['last_name'] = "Doo"
print(person_1["last_name"])

# update person_1 favorite_snack to "Scooby Snacks"
person_1.update({"favorite_snack":"Scooby Snacks"})
print(person_1["favorite_snack"])

# Remove the "wears_glasses" key:value from person_1
person_1.pop('wears_glasses')
print(person_1)