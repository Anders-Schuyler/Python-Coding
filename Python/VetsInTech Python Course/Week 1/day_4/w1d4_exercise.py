# ITP Week 1 Day 4 Exercise

# EASY

lowercase = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']

# 1. loop through the lowercase and print each element
for letter in lowercase:
    print(letter)
# 2. loop through the lowercase and print the capitalization of each element
for letter in lowercase:
    print(letter.upper())

# MEDIUM

# 1. create a new variable called uppercase with an empty list
uppercase = []

# 2. loop through the lowercase list
for letter in lowercase:
    # 2a. append the capitalization of each element to the uppercase list
    uppercase.append(letter.upper())

print(uppercase)

# HARD

# A safe password has a minimum of (1) uppercase, (1) lowercase, (1) number, (1) special character.

password = "MySuperSafePassword!@34"

special_char = ['!', '@', '#', '$', '%', '^', '&', '*', '(', ')']

# 1. create the following variables and assign them Booleans as False
has_uppercase = False
has_lowercase = False
has_number = False
has_special_char = False

# 2. loop through the string password (same as a list)
# OR you can create a new list variable of the string password
# using list(string) NOTE: assign it a new variable as such:
# password_list = list(password) prior to looping.
password_list = list(password)

# 3. For each iteration of the loop, create a if statement
# check to see if it exists in any of the list by using IN
# if it does exist, update the appropriate variable and CONTINUE
# not break.

# NOTE: to see if it has a number, use range from 0 - 10!
for char in password_list:
    if char.isupper():
        has_uppercase = True
    elif char.islower():
        has_lowercase = True
    elif char.isdigit():
        has_number = True
    elif char in special_char:
        has_special_char = True
    elif char in [str(i) for i in range(10)]:
        has_number = True

print("Has uppercase:", has_uppercase)
print("Has lowercase:", has_lowercase)
print("Has number:", has_number)
print("Has special character:", has_special_char)

# 4. do a final check to see if all of your variables are TRUE
# by using the AND operator for all 4 conditions. (This is done for you, uncomment below)

final_result = has_uppercase == True and has_lowercase == True and has_number == True and has_special_char == True

# NOTE: we can shorthand this by just checking if the variable exists (returns True)
#final_result_shorthand = has_uppercase and has_lowercase and has_number and has_special_char
# this will fail the same if any one of them is False

# If the final_result is true, print "SAFE STRONG PASSWORD"
# else, print "Update password: too weak"
# NOTE: this must be done outside of the loop
password_list = list(password)
if final_result:
    print("SAFE STRONG PASSWORD")
else:
    print("Update password: too weak")

# BONUS: update the password variable to take in an user input!
new_password = input("Enter your password: ")

new_has_uppercase = False
new_has_lowercase = False
new_has_number = False
new_has_special_char = False

new_password_list = list(new_password)

# NIGHTMARE: in the final check, use another if statement to list why it isn't a strong password!
# check password for quality
for char in new_password_list:
    if char.isupper():
        new_has_uppercase = True
    elif char.islower():
        new_has_lowercase = True
    elif char.isdigit():
        new_has_number = True
    elif char in special_char:
        new_has_special_char = True
    elif char in [str(i) for i in range(10)]:
        new_has_number = True

new_final_result = new_has_uppercase == True and new_has_lowercase == True and new_has_number == True and new_has_special_char == True

if new_final_result:
    print("SAFE STRONG PASSWORD")
else:
    print("Update password: too weak")
if not new_has_uppercase:
    print(" - Your password needs at least one uppercase letter.")
if not new_has_lowercase:
    print(" - Your password needs at least one lowercase letter.")
if not new_has_number:
    print(" - Your password needs at least one number.")
if not new_has_special_char:
    print(" - Your password needs at least one special character.")