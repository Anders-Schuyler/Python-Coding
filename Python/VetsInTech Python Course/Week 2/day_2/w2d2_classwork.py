#Step 1: Define the function with multiple parameters
def multi_parameter(first_name, last_name, location):
    print("My name is " + first_name + " " + last_name + ", and I live in " + location + ".")

#Step 2: Call the function with multiple arguments
multi_parameter("Daniel", "Kim", "San Diego")

#Global Variable
my_global_variable = "Joy to the world"

#Function that uses a global variable
def joy(some_variable):
    print(some_variable)

#Call the function
joy(my_global_variable) #This prints "Joy to the world"

#Function with a local variable
def sadness():
    my_local_variable = "John Wick's dog"
    print(my_local_variable)

#Call the function
sadness()

#This will cause an error if uncommented, because my_local_variable is not accessable outside of the sadness function
#joy(my_local_function)

#Global variable
president = "Joe Biden"

#Function that modifies the global variable
def my_house():
    global president
    president = "Daniel Kim"
    print(president)

#Call the function
my_house()

#Print the global variable to see the change
print(president)