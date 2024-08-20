# ITP Week 1 Day 3 (In-Class) Practice

# Take an user's input and assign it to a variable named "student_grade_string"
student_grade_string = input("What is your grade? ")
# The user input comes in as a string so we have to cast it to a Int to a variable named "student_grade_int"
student_grade_int = int(student_grade_string)
# Create an If statement with the appropriate Elif and Else statement for the following grading system.
if student_grade_int >= 90:
    print("A")
elif student_grade_int >= 80:
    print("B")
elif student_grade_int >= 70:
    print("C")
elif student_grade_int >= 60:
    print("D")
elif student_grade_int >= 0:
    print("F")
"""
A : 90 - 100
B : 80 - 89
C : 70 - 79
D : 60 - 69
F : 0 - 59
"""

# Within each "block" print out the appropriate letter grade.