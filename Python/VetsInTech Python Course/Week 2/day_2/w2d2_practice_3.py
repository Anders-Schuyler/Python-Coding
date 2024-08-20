# ITP Week 2 Day 2 (In-Class) Practice 3

# Functions Parameters and Arguments

# Lets take those functions we built in practice_2 and make them more dynamic:

# Rewrite the functions from practice_2 using parameters:
# add_num
def add_num(x, y):
    print(x + y)

# subtract_num
def subtract_num(x, y):
    print(x - y)

# multiply_num
def multiply_num(x, y):
    print(x * y)

# divide_num
def divide_num(x, y):
    print(x / y)

# Don't forget to call your functions to make sure they work

#Uncomment to call your functions:
print("I should see the number 7 below from add_num: ")
add_num(3, 4)

print("I should see the number -2 below from subtract_num: ")
subtract_num(6, 8)

print("I should see the number 18 below from multiply_num: ")
multiply_num(2, 9)

print("I should see the number 2 below from divide_num: ")
divide_num(10, 5)

# Extra Time?

# Now take in 2 users inputs and pass them 
# in as arguments when calling the functions
a = float(input("Enter the first number: "))
b = float(input("Enter the second number: "))

# Call the functions with user inputs
print("Results:")
add_num(a, b)
subtract_num(a, b)
multiply_num(a, b)
divide_num(a, b)